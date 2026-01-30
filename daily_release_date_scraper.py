#!/usr/bin/env python3
"""
Daily Release Date Scraper
==========================
Checks for newly announced release dates and updates the BFD database.

Schedule: Daily at 06:00 UTC
Sources: IMDB, TMDB, TVDB

Usage:
    python daily_release_date_scraper.py [--dry-run] [--limit N]

Author: BFD Pipeline Team
Version: 1.0.0
Created: 2026-01-20
"""

import pandas as pd
import numpy as np
import requests
import logging
import json
import os
import re
from datetime import datetime, date
from typing import Optional, Dict, Any, Tuple
from pathlib import Path
import time
import argparse

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    # Database paths
    'BFD_PATH': r'C:\Users\RoyT6\Downloads\BFD_V27.54.parquet',
    'STAR_SCHEMA_PATH': r'C:\Users\RoyT6\Downloads\BFD_Star_Schema_V27.54.parquet',

    # API Keys (load from environment)
    'TMDB_API_KEY': os.environ.get('TMDB_API_KEY', ''),
    'TVDB_API_KEY': os.environ.get('TVDB_API_KEY', ''),

    # Rate limiting
    'API_DELAY_SECONDS': 0.25,  # 4 requests per second
    'MAX_RETRIES': 3,

    # Sentinel values
    'SENTINEL_DATE': '9999-12-31',
    'UNKNOWN_DATE': '1900-01-01',

    # Logging
    'LOG_DIR': r'C:\Users\RoyT6\Downloads\scraper_logs',
    'LOG_LEVEL': logging.INFO,
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

def setup_logging() -> logging.Logger:
    """Configure logging with file and console handlers."""
    log_dir = Path(CONFIG['LOG_DIR'])
    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"release_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=CONFIG['LOG_LEVEL'],
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")
    return logger

# ============================================================================
# API CLIENTS
# ============================================================================

class TMDBClient:
    """Client for The Movie Database API."""

    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()

    def _request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """Make API request with retry logic."""
        if not self.api_key:
            return None

        params = params or {}
        params['api_key'] = self.api_key

        for attempt in range(CONFIG['MAX_RETRIES']):
            try:
                response = self.session.get(
                    f"{self.BASE_URL}/{endpoint}",
                    params=params,
                    timeout=10
                )
                response.raise_for_status()
                time.sleep(CONFIG['API_DELAY_SECONDS'])
                return response.json()
            except requests.RequestException as e:
                if attempt == CONFIG['MAX_RETRIES'] - 1:
                    logging.warning(f"TMDB API error for {endpoint}: {e}")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        return None

    def get_movie(self, tmdb_id: int) -> Optional[Dict]:
        """Get movie details including release date."""
        return self._request(f"movie/{tmdb_id}")

    def get_tv_show(self, tmdb_id: int) -> Optional[Dict]:
        """Get TV show details."""
        return self._request(f"tv/{tmdb_id}")

    def get_tv_season(self, tmdb_id: int, season_number: int) -> Optional[Dict]:
        """Get TV season details including air date."""
        return self._request(f"tv/{tmdb_id}/season/{season_number}")


class IMDBScraper:
    """Scraper for IMDB release dates (web scraping fallback)."""

    BASE_URL = "https://www.imdb.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
        })

    def get_release_date(self, imdb_id: str) -> Optional[str]:
        """
        Scrape release date from IMDB.
        Returns date string in YYYY-MM-DD format or None.
        """
        try:
            url = f"{self.BASE_URL}/title/{imdb_id}/releaseinfo"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            # Parse release dates from HTML
            # Look for US theatrical release or first premiere
            html = response.text

            # Pattern for release date table
            date_pattern = r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})'
            matches = re.findall(date_pattern, html)

            if matches:
                # Parse the first date found
                date_str = matches[0]
                parsed = datetime.strptime(date_str, '%d %B %Y')
                time.sleep(CONFIG['API_DELAY_SECONDS'])
                return parsed.strftime('%Y-%m-%d')

            time.sleep(CONFIG['API_DELAY_SECONDS'])
            return None

        except Exception as e:
            logging.warning(f"IMDB scrape error for {imdb_id}: {e}")
            return None


# ============================================================================
# RELEASE DATE RESOLVER
# ============================================================================

class ReleaseDateResolver:
    """Resolves release dates from multiple sources."""

    def __init__(self):
        self.tmdb = TMDBClient(CONFIG['TMDB_API_KEY'])
        self.imdb = IMDBScraper()
        self.logger = logging.getLogger(__name__)

    def resolve(self, row: pd.Series) -> Tuple[Optional[str], str]:
        """
        Attempt to resolve release date for a record.

        Returns:
            Tuple of (date_string or None, source)
        """
        title_type = row.get('title_type', '')
        tmdb_id = row.get('tmdb_id')
        imdb_id = row.get('imdb_id', row.get('fc_uid', '').split('_')[0])
        season_number = row.get('season_number')

        # Try TMDB first (most reliable API)
        if pd.notna(tmdb_id):
            tmdb_id = int(tmdb_id)

            if title_type == 'film':
                data = self.tmdb.get_movie(tmdb_id)
                if data and data.get('release_date'):
                    return data['release_date'], 'TMDB'

            elif title_type == 'tv_show' and pd.notna(season_number):
                data = self.tmdb.get_tv_season(tmdb_id, int(season_number))
                if data and data.get('air_date'):
                    return data['air_date'], 'TMDB'

        # Fallback to IMDB scraping
        if imdb_id and imdb_id.startswith('tt'):
            date_str = self.imdb.get_release_date(imdb_id)
            if date_str:
                return date_str, 'IMDB'

        return None, 'NOT_FOUND'


# ============================================================================
# DATABASE UPDATER
# ============================================================================

class DatabaseUpdater:
    """Handles database updates with audit logging."""

    def __init__(self, bfd_path: str, star_schema_path: str, dry_run: bool = False):
        self.bfd_path = bfd_path
        self.star_schema_path = star_schema_path
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        self.changes = []

    def load_unreleased_content(self) -> pd.DataFrame:
        """Load records that need release date checking."""
        self.logger.info(f"Loading BFD from {self.bfd_path}")
        df = pd.read_parquet(self.bfd_path)

        # Filter for unreleased content
        mask = (
            (df['premiere_date'] == CONFIG['SENTINEL_DATE']) |
            (df['premiere_date_confirmed'] == False) |
            (df['status'].isin(['Unknown', 'Upcoming']))
        )

        unreleased = df[mask].copy()
        self.logger.info(f"Found {len(unreleased):,} records to check")
        return unreleased

    def update_record(self, fc_uid: str, new_date: str, source: str) -> Dict:
        """
        Update a single record with new release date.

        Returns change record for audit log.
        """
        change = {
            'fc_uid': fc_uid,
            'new_premiere_date': new_date,
            'source': source,
            'timestamp': datetime.now().isoformat(),
            'applied': not self.dry_run
        }

        self.changes.append(change)
        return change

    def apply_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all pending changes to dataframe."""
        today = date.today().isoformat()

        for change in self.changes:
            if not change['applied']:
                continue

            fc_uid = change['fc_uid']
            new_date = change['new_premiere_date']

            mask = df['fc_uid'] == fc_uid

            # Update premiere_date
            df.loc[mask, 'premiere_date'] = new_date
            df.loc[mask, 'premiere_date_confirmed'] = True

            # Update status if release date has passed
            if new_date <= today:
                df.loc[mask, 'status'] = 'Released'

            self.logger.info(f"Updated {fc_uid}: premiere_date={new_date}")

        return df

    def save(self, df: pd.DataFrame):
        """Save updated dataframe to parquet."""
        if self.dry_run:
            self.logger.info("DRY RUN: Skipping save")
            return

        # Backup original
        backup_path = self.bfd_path.replace('.parquet', '_backup.parquet')

        self.logger.info(f"Saving to {self.bfd_path}")
        df.to_parquet(self.bfd_path, index=False)
        self.logger.info("Save complete")

    def save_audit_log(self):
        """Save audit log of all changes."""
        if not self.changes:
            return

        log_dir = Path(CONFIG['LOG_DIR'])
        log_dir.mkdir(exist_ok=True)

        audit_file = log_dir / f"audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(audit_file, 'w') as f:
            json.dump({
                'run_timestamp': datetime.now().isoformat(),
                'dry_run': self.dry_run,
                'total_changes': len(self.changes),
                'changes': self.changes
            }, f, indent=2)

        self.logger.info(f"Audit log saved: {audit_file}")


# ============================================================================
# MAIN SCRAPER
# ============================================================================

class DailyReleaseDateScraper:
    """Main scraper orchestrator."""

    def __init__(self, dry_run: bool = False, limit: int = None):
        self.dry_run = dry_run
        self.limit = limit
        self.logger = setup_logging()

        self.resolver = ReleaseDateResolver()
        self.updater = DatabaseUpdater(
            CONFIG['BFD_PATH'],
            CONFIG['STAR_SCHEMA_PATH'],
            dry_run=dry_run
        )

    def run(self):
        """Execute the daily scrape."""
        self.logger.info("=" * 60)
        self.logger.info("DAILY RELEASE DATE SCRAPER - Starting")
        self.logger.info(f"Dry run: {self.dry_run}")
        self.logger.info("=" * 60)

        # Load unreleased content
        unreleased = self.updater.load_unreleased_content()

        if self.limit:
            unreleased = unreleased.head(self.limit)
            self.logger.info(f"Limited to {self.limit} records")

        # Process each record
        found_count = 0
        not_found_count = 0
        error_count = 0

        total = len(unreleased)
        for idx, (_, row) in enumerate(unreleased.iterrows()):
            if idx % 100 == 0:
                self.logger.info(f"Progress: {idx}/{total} ({idx/total*100:.1f}%)")

            try:
                new_date, source = self.resolver.resolve(row)

                if new_date:
                    self.updater.update_record(row['fc_uid'], new_date, source)
                    found_count += 1
                    self.logger.debug(f"Found: {row['fc_uid']} -> {new_date} ({source})")
                else:
                    not_found_count += 1

            except Exception as e:
                error_count += 1
                self.logger.error(f"Error processing {row['fc_uid']}: {e}")

        # Apply changes
        if self.updater.changes:
            self.logger.info(f"Applying {len(self.updater.changes)} changes...")
            df = pd.read_parquet(CONFIG['BFD_PATH'])
            df = self.updater.apply_changes(df)
            self.updater.save(df)

        # Save audit log
        self.updater.save_audit_log()

        # Summary
        self.logger.info("=" * 60)
        self.logger.info("SUMMARY")
        self.logger.info(f"  Total checked: {total:,}")
        self.logger.info(f"  Dates found: {found_count:,}")
        self.logger.info(f"  Not found: {not_found_count:,}")
        self.logger.info(f"  Errors: {error_count:,}")
        self.logger.info("=" * 60)


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Daily Release Date Scraper for BFD Database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Run without saving changes'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of records to process'
    )

    args = parser.parse_args()

    scraper = DailyReleaseDateScraper(
        dry_run=args.dry_run,
        limit=args.limit
    )
    scraper.run()


if __name__ == '__main__':
    main()
