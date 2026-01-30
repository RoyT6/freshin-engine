#!/usr/bin/env python3
"""
SCHIG Fresh Data Integration for FC-ALGO-80
============================================
Integrates fresh collection data into the Cranberry database
for use in FC-ALGO-80 Phase 2 (Data Loading) and Phase 3 (X-Feature Creation)

This script:
1. Loads the latest fresh_data collection
2. Matches to Cranberry titles by TMDB ID / IMDB ID
3. Updates abstract signals (abs_social_buzz, abs_trend_velocity, etc.)
4. Outputs updated Abstract Data files for Phase 2

Usage:
    python integrate_fresh_to_algo80.py
    python integrate_fresh_to_algo80.py --date 20260113
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import argparse
import logging

# Paths
FRESH_DIR = Path(r"C:\Users\RoyT6\Downloads\Fresh In!")
ABSTRACT_DIR = Path(r"C:\Users\RoyT6\Downloads\Abstract Data")
CRANBERRY_PATH = Path(r"C:\Users\RoyT6\Downloads\Cranberry_Titles_V4.10.parquet")

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def find_latest_collection(date_str: str = None) -> Path:
    """Find the latest fresh_data JSON file"""
    if date_str:
        pattern = f"fresh_data_{date_str}*.json"
    else:
        pattern = "fresh_data_*.json"

    files = list(FRESH_DIR.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No fresh_data files found matching {pattern}")

    # Sort by modification time, newest first
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0]


def load_fresh_data(filepath: Path) -> dict:
    """Load fresh data JSON"""
    logger.info(f"Loading fresh data from: {filepath.name}")
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
    logger.info(f"  Collection date: {data.get('collection_date', 'unknown')}")
    logger.info(f"  Total results: {len(data.get('results', []))}")
    return data


def extract_tmdb_data(fresh_data: dict) -> pd.DataFrame:
    """Extract TMDB trending/popular data"""
    records = []

    for result in fresh_data.get('results', []):
        if result.get('source') == 'tmdb':
            data = result.get('data', {})
            items = data.get('results', [])

            data_type = result.get('type', 'unknown')

            for item in items:
                record = {
                    'tmdb_id': item.get('id'),
                    'title': item.get('title', item.get('name', '')),
                    'media_type': item.get('media_type', 'movie' if 'title' in item else 'tv'),
                    'popularity': item.get('popularity', 0),
                    'vote_average': item.get('vote_average', 0),
                    'vote_count': item.get('vote_count', 0),
                    'data_type': data_type,
                    'release_date': item.get('release_date', item.get('first_air_date', '')),
                }
                records.append(record)

    df = pd.DataFrame(records)
    if not df.empty:
        # Deduplicate, keeping highest popularity
        df = df.sort_values('popularity', ascending=False).drop_duplicates(subset=['tmdb_id'], keep='first')
    logger.info(f"  Extracted {len(df)} TMDB titles")
    return df


def extract_flixpatrol_data(fresh_data: dict) -> pd.DataFrame:
    """Extract FlixPatrol ranking data"""
    records = []

    for result in fresh_data.get('results', []):
        if result.get('source') == 'flixpatrol':
            data = result.get('data', {})
            items = data.get('data', [])

            for item in items:
                record = {
                    'flixpatrol_id': item.get('id'),
                    'title': item.get('title', ''),
                    'tmdb_id': item.get('tmdb_id'),
                    'imdb_id': item.get('imdb_id'),
                    'type': item.get('type', ''),
                    'points': item.get('points', 0),
                    'rank': item.get('rank'),
                }
                records.append(record)

    df = pd.DataFrame(records)
    logger.info(f"  Extracted {len(df)} FlixPatrol titles")
    return df


def extract_streaming_changes(fresh_data: dict) -> pd.DataFrame:
    """Extract streaming availability changes"""
    records = []

    for result in fresh_data.get('results', []):
        if result.get('source') == 'streaming_availability' and result.get('type') == 'new_shows':
            country = result.get('country', 'unknown')
            data = result.get('data', {})
            changes = data.get('changes', [])

            for change in changes:
                show = change.get('show', {})
                service = change.get('service')
                # Handle service being a dict or string
                if isinstance(service, dict):
                    service_name = service.get('id', service.get('name', 'unknown'))
                else:
                    service_name = str(service) if service else 'unknown'

                record = {
                    'country': country,
                    'tmdb_id': show.get('tmdbId'),
                    'imdb_id': show.get('imdbId'),
                    'title': show.get('title', ''),
                    'show_type': show.get('showType', ''),
                    'change_type': change.get('changeType', 'new'),
                    'service': service_name,
                }
                records.append(record)

    df = pd.DataFrame(records)
    logger.info(f"  Extracted {len(df)} streaming changes")
    return df


def calculate_social_buzz(tmdb_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate abs_social_buzz from TMDB popularity"""
    if tmdb_df.empty:
        return pd.DataFrame()

    # Normalize popularity to 0-100 scale
    # TMDB popularity typically ranges from 0-1000+
    df = tmdb_df.copy()
    df['abs_social_buzz'] = np.clip(df['popularity'] / 10, 0, 100)

    # Calculate trend velocity based on position in trending lists
    trending_types = ['trending_movies_week', 'trending_tv_week']
    df['is_trending'] = df['data_type'].isin(trending_types)
    df['abs_trend_velocity'] = np.where(df['is_trending'], 1.5, 0.5)

    return df[['tmdb_id', 'title', 'abs_social_buzz', 'abs_trend_velocity', 'vote_average', 'vote_count']]


def calculate_platform_signals(streaming_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate platform availability signals"""
    if streaming_df.empty:
        return pd.DataFrame()

    # Count platforms per title
    df = streaming_df.groupby('tmdb_id').agg({
        'country': 'nunique',
        'service': 'nunique',
        'title': 'first',
    }).reset_index()

    df.columns = ['tmdb_id', 'country_count', 'platform_count', 'title']

    # Create abs_availability_score (more platforms/countries = higher score)
    df['abs_availability_score'] = np.clip(
        (df['platform_count'] * 5 + df['country_count'] * 2), 0, 100
    )

    return df


def calculate_ranking_signals(flixpatrol_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate ranking-based signals from FlixPatrol"""
    if flixpatrol_df.empty:
        return pd.DataFrame()

    df = flixpatrol_df.copy()

    # Points-based buzz score
    max_points = df['points'].max() if df['points'].max() > 0 else 1
    df['abs_ranking_buzz'] = np.clip(df['points'] / max_points * 100, 0, 100)

    # Rank-based decay position (lower rank = higher score)
    df['abs_decay_position'] = np.where(
        df['rank'].notna(),
        np.clip(100 - df['rank'] * 2, 0, 100),
        50
    )

    return df[['tmdb_id', 'imdb_id', 'title', 'abs_ranking_buzz', 'abs_decay_position', 'points']]


def save_abstract_signals(signals_df: pd.DataFrame, timestamp: str):
    """Save computed abstract signals to Abstract Data folder"""
    if signals_df.empty:
        logger.warning("No signals to save")
        return

    # Save as parquet
    output_file = ABSTRACT_DIR / f"fresh_signals_{timestamp}.parquet"
    signals_df.to_parquet(output_file, index=False)
    logger.info(f"Saved abstract signals: {output_file.name} ({len(signals_df)} records)")

    # Save as JSON for inspection
    json_file = ABSTRACT_DIR / f"fresh_signals_{timestamp}.json"
    signals_df.to_json(json_file, orient='records', indent=2)
    logger.info(f"Saved JSON copy: {json_file.name}")

    return output_file


def create_integration_report(tmdb_df, flixpatrol_df, streaming_df, signals_df, timestamp):
    """Create integration report"""
    report = {
        "integration_timestamp": datetime.now().isoformat(),
        "source_file_timestamp": timestamp,
        "source_counts": {
            "tmdb_titles": len(tmdb_df),
            "flixpatrol_titles": len(flixpatrol_df),
            "streaming_changes": len(streaming_df),
        },
        "output_signals": {
            "total_records": len(signals_df),
            "columns": list(signals_df.columns) if not signals_df.empty else [],
            "signal_stats": {}
        }
    }

    if not signals_df.empty:
        for col in ['abs_social_buzz', 'abs_trend_velocity', 'abs_ranking_buzz', 'abs_availability_score']:
            if col in signals_df.columns:
                report["output_signals"]["signal_stats"][col] = {
                    "mean": float(signals_df[col].mean()),
                    "min": float(signals_df[col].min()),
                    "max": float(signals_df[col].max()),
                    "non_zero": int((signals_df[col] > 0).sum())
                }

    report_file = ABSTRACT_DIR / f"fresh_integration_report_{timestamp}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    logger.info(f"Saved integration report: {report_file.name}")

    return report


def main():
    parser = argparse.ArgumentParser(description="Integrate SCHIG data into FC-ALGO-80")
    parser.add_argument("--date", help="Collection date (YYYYMMDD format)")
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("SCHIG Fresh Data Integration for FC-ALGO-80")
    logger.info("=" * 70)

    # Find and load fresh data
    fresh_file = find_latest_collection(args.date)
    timestamp = fresh_file.stem.replace('fresh_data_', '')
    fresh_data = load_fresh_data(fresh_file)

    # Extract data from each source
    logger.info("\nExtracting source data...")
    tmdb_df = extract_tmdb_data(fresh_data)
    flixpatrol_df = extract_flixpatrol_data(fresh_data)
    streaming_df = extract_streaming_changes(fresh_data)

    # Calculate abstract signals
    logger.info("\nCalculating abstract signals...")
    social_signals = calculate_social_buzz(tmdb_df)
    platform_signals = calculate_platform_signals(streaming_df)
    ranking_signals = calculate_ranking_signals(flixpatrol_df)

    # Merge all signals
    logger.info("\nMerging signals...")
    all_signals = pd.DataFrame()

    # Convert tmdb_id to string in all dataframes for consistent merging
    if not social_signals.empty:
        social_signals['tmdb_id'] = social_signals['tmdb_id'].astype(str)
        all_signals = social_signals

    if not ranking_signals.empty:
        ranking_signals['tmdb_id'] = ranking_signals['tmdb_id'].astype(str)
        if all_signals.empty:
            all_signals = ranking_signals
        else:
            # Merge on tmdb_id where available
            all_signals = pd.merge(
                all_signals, ranking_signals,
                on='tmdb_id', how='outer', suffixes=('', '_rank')
            )
            # Consolidate title columns
            if 'title_rank' in all_signals.columns:
                all_signals['title'] = all_signals['title'].fillna(all_signals['title_rank'])
                all_signals.drop(columns=['title_rank'], inplace=True)

    if not platform_signals.empty:
        platform_signals['tmdb_id'] = platform_signals['tmdb_id'].astype(str)
        if all_signals.empty:
            all_signals = platform_signals
        else:
            all_signals = pd.merge(
                all_signals, platform_signals[['tmdb_id', 'abs_availability_score']],
                on='tmdb_id', how='left'
            )

    # Fill NaN signals with defaults
    signal_cols = ['abs_social_buzz', 'abs_trend_velocity', 'abs_ranking_buzz',
                   'abs_decay_position', 'abs_availability_score']
    for col in signal_cols:
        if col in all_signals.columns:
            all_signals[col] = all_signals[col].fillna(0)

    logger.info(f"  Total merged signals: {len(all_signals)} records")

    # Save outputs
    logger.info("\nSaving outputs...")
    save_abstract_signals(all_signals, timestamp)
    report = create_integration_report(tmdb_df, flixpatrol_df, streaming_df, all_signals, timestamp)

    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("INTEGRATION COMPLETE")
    logger.info("=" * 70)
    logger.info(f"Source: {fresh_file.name}")
    logger.info(f"TMDB titles: {len(tmdb_df)}")
    logger.info(f"FlixPatrol titles: {len(flixpatrol_df)}")
    logger.info(f"Streaming changes: {len(streaming_df)}")
    logger.info(f"Output signals: {len(all_signals)}")
    logger.info(f"Output location: {ABSTRACT_DIR}")
    logger.info("=" * 70)

    print("\nFor FC-ALGO-80 Phase 2, load:")
    print(f"  {ABSTRACT_DIR / f'fresh_signals_{timestamp}.parquet'}")


if __name__ == "__main__":
    main()
