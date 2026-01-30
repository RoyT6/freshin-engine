"""
FreshIn Ingestion Engine for BFD V27.65+
==========================================
Schema V27.00 Compliant - NO NEW COLUMNS

Ingests fresh data from multiple sources:
- Disney+ catalog data
- FlixPatrol title metadata
- TMDB trending/popular data
- IMDb chart data

Rules:
- NEVER creates new columns
- Updates existing records (MERGE mode per schema)
- FlixPatrol-specific columns: flixpatrol_id, flixpatrol_points, flixpatrol_rank
- Source priority: TMDB > IMDb > FlixPatrol for metadata
- P0: NO views for periods that END BEFORE premiere_date

Created: 2026-01-26
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

# Paths
BASE_DIR = Path("C:/Users/RoyT6/Downloads")
FRESHIN_DIR = BASE_DIR / "FreshIn"
SCHEMA_PATH = BASE_DIR / "Schema" / "SCHEMA_V27.00.json"

# Input database
BFD_INPUT = BASE_DIR / "BFD_V27.65.parquet"
# Output database (versioned increment)
BFD_OUTPUT = BASE_DIR / "BFD_V27.66.parquet"
VIEWERDBX_INPUT = BASE_DIR / "VIEWERDBX_V1.parquet"
VIEWERDBX_OUTPUT = BASE_DIR / "VIEWERDBX_V1.1.parquet"

# Report
REPORT_PATH = BASE_DIR / "FreshIn_Merge_Report_V27.66.json"

# Empty value handling
EMPTY_VALUES = {'', 'None', 'none', 'nan', 'NaN', 'NULL', 'null', 'N/A', 'n/a', '<NA>'}

# 22 Countries per Schema V27.00
COUNTRIES_22 = [
    'us', 'cn', 'in', 'gb', 'br', 'de', 'jp', 'fr', 'ca', 'mx',
    'au', 'es', 'it', 'kr', 'nl', 'se', 'sg', 'hk', 'ie', 'ru',
    'tr', 'row'
]


def is_empty(val) -> bool:
    """Check if value is effectively empty/null."""
    if val is None or pd.isna(val):
        return True
    if isinstance(val, str):
        return val.strip() in EMPTY_VALUES
    return False


def normalize_imdb_id(val) -> Optional[str]:
    """Normalize IMDb ID to tt####### format."""
    if is_empty(val):
        return None
    s = str(val).strip()
    if s.isdigit():
        # Pad to 7 digits minimum
        return f"tt{s.zfill(7)}"
    if s.startswith('tt'):
        return s
    return None


class FreshInIngestionEngine:
    """
    Schema V27.00 compliant ingestion engine.

    CRITICAL RULES:
    - NEVER creates new columns
    - Updates via MERGE (fill NULL) or OVERWRITE (for FlixPatrol-specific)
    - Source priority: TMDB (1) > IMDb (2) > FlixPatrol (3)
    """

    def __init__(self):
        self.stats = {
            'bfd_rows_start': 0,
            'bfd_cols': 0,
            'disney_records_processed': 0,
            'disney_updates': 0,
            'flixpatrol_records_processed': 0,
            'flixpatrol_updates': 0,
            'tmdb_records_processed': 0,
            'tmdb_updates': 0,
            'imdb_records_processed': 0,
            'imdb_updates': 0,
            'total_values_updated': 0,
            'bfd_rows_end': 0,
            'processing_errors': [],
        }
        self.bfd = None
        self.bfd_columns: Set[str] = set()
        self.imdb_index: Dict[str, int] = {}
        self.tmdb_index: Dict[int, int] = {}

    def load_bfd(self):
        """Load base BFD database and build indexes."""
        print(f"\n[LOAD] Loading BFD from {BFD_INPUT}")
        self.bfd = pd.read_parquet(BFD_INPUT)
        self.stats['bfd_rows_start'] = len(self.bfd)
        self.stats['bfd_cols'] = len(self.bfd.columns)
        self.bfd_columns = set(self.bfd.columns)

        # Build IMDb ID index
        print("[INDEX] Building IMDb ID index...")
        for idx, row in self.bfd.iterrows():
            imdb_id = normalize_imdb_id(row.get('imdb_id'))
            if imdb_id:
                self.imdb_index[imdb_id] = idx

        # Build TMDB ID index
        print("[INDEX] Building TMDB ID index...")
        for idx, row in self.bfd.iterrows():
            tmdb_id = row.get('tmdb_id')
            if not is_empty(tmdb_id):
                try:
                    self.tmdb_index[int(tmdb_id)] = idx
                except (ValueError, TypeError):
                    pass

        print(f"[LOAD] BFD: {self.stats['bfd_rows_start']:,} rows x {self.stats['bfd_cols']:,} columns")
        print(f"[INDEX] IMDb index: {len(self.imdb_index):,} records")
        print(f"[INDEX] TMDB index: {len(self.tmdb_index):,} records")

    def _update_value(self, idx: int, column: str, value, mode: str = 'MERGE') -> bool:
        """
        Update a single value in BFD.

        Modes:
        - MERGE: Only update if existing value is NULL/empty
        - OVERWRITE: Always update with new value

        Returns True if value was updated.
        """
        if column not in self.bfd_columns:
            return False

        if is_empty(value):
            return False

        current = self.bfd.at[idx, column]

        if mode == 'MERGE':
            if is_empty(current):
                self.bfd.at[idx, column] = value
                return True
            return False
        elif mode == 'OVERWRITE':
            # Handle NA comparison safely
            try:
                if pd.isna(current) or current != value:
                    self.bfd.at[idx, column] = value
                    return True
            except (TypeError, ValueError):
                # Fallback: just update if there's any issue comparing
                self.bfd.at[idx, column] = value
                return True
            return False
        return False

    def process_disney_plus(self):
        """
        Process Disney+ FreshIn data.

        Maps:
        - imdb_id -> imdb_id (MERGE)
        - imdb_score -> imdb_score (MERGE)
        - imdb_votes -> imdb_vote_count (MERGE)
        - tmdb_score -> tmdb_score (MERGE)
        - tmdb_popularity -> tmdb_popularity (MERGE)
        - age_certification -> age_certification (MERGE)
        - runtime -> runtime_minutes (MERGE)
        - genres -> genres (MERGE, array to semicolon)
        - Platform marker: streaming_platform_us = "Disney+" (MERGE)
        """
        print("\n[DISNEY+] Processing Disney+ FreshIn data...")

        disney_files = sorted(FRESHIN_DIR.glob("disney_plus_*.json"), reverse=True)
        if not disney_files:
            print("[DISNEY+] No Disney+ files found")
            return

        print(f"[DISNEY+] Found {len(disney_files)} files")

        # Collect all unique titles from all files
        seen_imdb_ids = set()
        all_items = []

        for json_file in disney_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('type') == 'titles':
                        items = entry.get('data', {}).get('items', [])
                        for item in items:
                            imdb_id = item.get('imdb_id')
                            if imdb_id and imdb_id not in seen_imdb_ids:
                                seen_imdb_ids.add(imdb_id)
                                all_items.append(item)
                    elif entry.get('type') == 'random_title':
                        item = entry.get('data', {})
                        imdb_id = item.get('imdb_id')
                        if imdb_id and imdb_id not in seen_imdb_ids:
                            seen_imdb_ids.add(imdb_id)
                            all_items.append(item)
            except Exception as e:
                self.stats['processing_errors'].append(f"Disney+ {json_file.name}: {str(e)}")

        print(f"[DISNEY+] Loaded {len(all_items):,} unique titles")

        updates = 0
        values_updated = 0

        for item in all_items:
            self.stats['disney_records_processed'] += 1

            imdb_id = normalize_imdb_id(item.get('imdb_id'))
            if not imdb_id or imdb_id not in self.imdb_index:
                continue

            idx = self.imdb_index[imdb_id]
            record_updates = 0

            # MERGE: imdb_score
            if self._update_value(idx, 'imdb_score', item.get('imdb_score'), 'MERGE'):
                record_updates += 1

            # MERGE: imdb_vote_count
            if self._update_value(idx, 'imdb_vote_count', item.get('imdb_votes'), 'MERGE'):
                record_updates += 1

            # MERGE: tmdb_score
            if self._update_value(idx, 'tmdb_score', item.get('tmdb_score'), 'MERGE'):
                record_updates += 1

            # MERGE: tmdb_popularity
            if self._update_value(idx, 'tmdb_popularity', item.get('tmdb_popularity'), 'MERGE'):
                record_updates += 1

            # MERGE: age_certification
            if self._update_value(idx, 'age_certification', item.get('age_certification'), 'MERGE'):
                record_updates += 1

            # MERGE: runtime_minutes
            if self._update_value(idx, 'runtime_minutes', item.get('runtime'), 'MERGE'):
                record_updates += 1

            # MERGE: genres (convert array to semicolon-separated)
            genres = item.get('genres')
            if genres and isinstance(genres, list):
                genres_str = '; '.join(genres)
                if self._update_value(idx, 'genres', genres_str, 'MERGE'):
                    record_updates += 1

            # MERGE: streaming_platform_us (mark as Disney+)
            if self._update_value(idx, 'streaming_platform_us', 'Disney+', 'MERGE'):
                record_updates += 1

            if record_updates > 0:
                updates += 1
                values_updated += record_updates

        self.stats['disney_updates'] = updates
        self.stats['total_values_updated'] += values_updated
        print(f"[DISNEY+] Updates: {updates:,} records, {values_updated:,} values")

    def process_flixpatrol(self):
        """
        Process FlixPatrol FreshIn metadata.

        Schema V27.00 FlixPatrol Mappings:
        - id -> flixpatrol_id (OVERWRITE - FlixPatrol-specific)
        - imdbId -> imdb_id (MERGE, transform: prefix with 'tt')
        - tmdbId -> tmdb_id (MERGE)
        - title -> title (MERGE)
        - premiere -> premiere_date (MERGE)
        - description -> overview (MERGE)
        - length -> runtime_minutes (MERGE)
        - budget -> budget (MERGE)
        - countries -> production_country (MERGE)
        - seasons -> max_seasons (MERGE)
        """
        print("\n[FLIXPATROL] Processing FlixPatrol FreshIn data...")

        fp_files = sorted(FRESHIN_DIR.glob("flixpatrol_*.json"), reverse=True)
        if not fp_files:
            print("[FLIXPATROL] No FlixPatrol files found")
            return

        print(f"[FLIXPATROL] Found {len(fp_files)} files")

        # Collect unique titles (use most recent file first)
        seen_ids = set()
        all_items = []

        for json_file in fp_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('source') == 'flixpatrol' and entry.get('endpoint') == 'titles':
                        items = entry.get('data', {}).get('data', [])
                        for item in items:
                            item_data = item.get('data', {})
                            fp_id = item_data.get('id')
                            if fp_id and fp_id not in seen_ids:
                                seen_ids.add(fp_id)
                                all_items.append(item_data)
            except Exception as e:
                self.stats['processing_errors'].append(f"FlixPatrol {json_file.name}: {str(e)}")

        print(f"[FLIXPATROL] Loaded {len(all_items):,} unique titles")

        updates = 0
        values_updated = 0

        for item in all_items:
            self.stats['flixpatrol_records_processed'] += 1

            # Find matching BFD record via IMDb or TMDB ID
            imdb_raw = item.get('imdbId')
            tmdb_id = item.get('tmdbId')

            idx = None

            # Try IMDb match first (transform integer to tt format)
            if imdb_raw:
                imdb_id = normalize_imdb_id(imdb_raw)
                if imdb_id and imdb_id in self.imdb_index:
                    idx = self.imdb_index[imdb_id]

            # Fallback to TMDB match
            if idx is None and tmdb_id:
                try:
                    tmdb_int = int(tmdb_id)
                    if tmdb_int in self.tmdb_index:
                        idx = self.tmdb_index[tmdb_int]
                except (ValueError, TypeError):
                    pass

            if idx is None:
                continue

            record_updates = 0

            # OVERWRITE: flixpatrol_id (FlixPatrol-specific column)
            if self._update_value(idx, 'flixpatrol_id', item.get('id'), 'OVERWRITE'):
                record_updates += 1

            # MERGE: premiere_date
            if self._update_value(idx, 'premiere_date', item.get('premiere'), 'MERGE'):
                record_updates += 1

            # MERGE: overview (from description)
            # Note: Schema says 'overview' merged into 'tagline' in V27.03,
            # but check if tagline exists
            desc = item.get('description')
            if desc:
                if 'tagline' in self.bfd_columns:
                    if self._update_value(idx, 'tagline', desc, 'MERGE'):
                        record_updates += 1
                elif 'overview' in self.bfd_columns:
                    if self._update_value(idx, 'overview', desc, 'MERGE'):
                        record_updates += 1

            # MERGE: runtime_minutes
            if self._update_value(idx, 'runtime_minutes', item.get('length'), 'MERGE'):
                record_updates += 1

            # MERGE: budget
            if self._update_value(idx, 'budget', item.get('budget'), 'MERGE'):
                record_updates += 1

            # MERGE: max_seasons
            # Note: FlixPatrol type=1 is movies, not TV
            item_type = item.get('type')
            if item_type != 1:  # Not a movie
                # Would need seasons data from FlixPatrol - not in current files
                pass

            # MERGE: wikipedia_link
            wiki = item.get('linkWikipedia')
            if wiki and 'wikipedia_link' in self.bfd_columns:
                if self._update_value(idx, 'wikipedia_link', wiki, 'MERGE'):
                    record_updates += 1

            if record_updates > 0:
                updates += 1
                values_updated += record_updates

        self.stats['flixpatrol_updates'] = updates
        self.stats['total_values_updated'] += values_updated
        print(f"[FLIXPATROL] Updates: {updates:,} records, {values_updated:,} values")

    def process_tmdb(self):
        """
        Process TMDB FreshIn data (trending/popular).

        TMDB has PRIORITY 1 for metadata per schema.

        Maps:
        - id -> tmdb_id (MERGE)
        - popularity -> tmdb_popularity (OVERWRITE - fresh data)
        - vote_average -> tmdb_score (OVERWRITE - fresh data)
        - vote_count -> (no direct column in schema)
        - release_date/first_air_date -> premiere_date (MERGE)
        - original_language -> original_language (MERGE)
        """
        print("\n[TMDB] Processing TMDB FreshIn data...")

        tmdb_files = sorted(FRESHIN_DIR.glob("tmdb_*.json"), reverse=True)
        if not tmdb_files:
            print("[TMDB] No TMDB files found")
            return

        print(f"[TMDB] Found {len(tmdb_files)} files")

        # Collect unique titles
        seen_ids = set()
        all_items = []

        for json_file in tmdb_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('source') == 'tmdb':
                        entry_type = entry.get('type', '')
                        if 'genres' in entry_type:
                            continue  # Skip genre reference data

                        results = entry.get('data', {}).get('results', [])
                        for item in results:
                            tmdb_id = item.get('id')
                            if tmdb_id and tmdb_id not in seen_ids:
                                seen_ids.add(tmdb_id)
                                # Add media type info
                                if 'trending_tv' in entry_type or 'popular_tv' in entry_type or \
                                   'top_rated_tv' in entry_type or 'airing' in entry_type:
                                    item['_media_type'] = 'tv'
                                else:
                                    item['_media_type'] = 'movie'
                                all_items.append(item)
            except Exception as e:
                self.stats['processing_errors'].append(f"TMDB {json_file.name}: {str(e)}")

        print(f"[TMDB] Loaded {len(all_items):,} unique titles")

        updates = 0
        values_updated = 0

        for item in all_items:
            self.stats['tmdb_records_processed'] += 1

            tmdb_id = item.get('id')
            if not tmdb_id:
                continue

            try:
                tmdb_int = int(tmdb_id)
            except (ValueError, TypeError):
                continue

            if tmdb_int not in self.tmdb_index:
                continue

            idx = self.tmdb_index[tmdb_int]
            record_updates = 0

            # OVERWRITE: tmdb_popularity (always take fresh TMDB data - priority 1)
            pop = item.get('popularity')
            if pop is not None:
                if self._update_value(idx, 'tmdb_popularity', float(pop), 'OVERWRITE'):
                    record_updates += 1

            # OVERWRITE: tmdb_score (from vote_average)
            vote_avg = item.get('vote_average')
            if vote_avg is not None:
                if self._update_value(idx, 'tmdb_score', float(vote_avg), 'OVERWRITE'):
                    record_updates += 1

            # MERGE: premiere_date (release_date for movies, first_air_date for TV)
            release = item.get('release_date') or item.get('first_air_date')
            if release:
                if self._update_value(idx, 'premiere_date', str(release), 'MERGE'):
                    record_updates += 1

            # MERGE: original_language
            lang = item.get('original_language')
            if lang:
                if self._update_value(idx, 'original_language', str(lang), 'MERGE'):
                    record_updates += 1

            if record_updates > 0:
                updates += 1
                values_updated += record_updates

        self.stats['tmdb_updates'] = updates
        self.stats['total_values_updated'] += values_updated
        print(f"[TMDB] Updates: {updates:,} records, {values_updated:,} values")

    def process_imdb(self):
        """
        Process IMDb FreshIn data (chart scrapes).

        Limited data available - mainly chart positions.
        Genre reference files are skipped (static data).
        """
        print("\n[IMDB] Processing IMDb FreshIn data...")

        imdb_files = sorted(FRESHIN_DIR.glob("imdb_*.json"), reverse=True)
        if not imdb_files:
            print("[IMDB] No IMDb files found")
            return

        print(f"[IMDB] Found {len(imdb_files)} files")

        # Look for scrape files with actual title data
        scrape_files = [f for f in imdb_files if 'scrape' in f.name]
        if not scrape_files:
            # Check if any regular imdb files have title data
            for json_file in imdb_files[:1]:  # Check first file
                try:
                    with open(json_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    for entry in data:
                        if entry.get('type') == 'genres':
                            print("[IMDB] Files contain genre reference only - no title updates")
                            return
                except:
                    pass

        all_items = []

        for json_file in imdb_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('source') == 'imdb_scrape':
                        titles = entry.get('titles', [])
                        page_type = entry.get('page', '')
                        for title_info in titles:
                            # Extract IMDb ID from URL
                            url = title_info.get('url', '')
                            if '/title/tt' in url:
                                import re
                                match = re.search(r'tt(\d+)', url)
                                if match:
                                    imdb_id = f"tt{match.group(1)}"
                                    all_items.append({
                                        'imdb_id': imdb_id,
                                        'title': title_info.get('title'),
                                        'chart_type': page_type
                                    })
            except Exception as e:
                self.stats['processing_errors'].append(f"IMDb {json_file.name}: {str(e)}")

        if not all_items:
            print("[IMDB] No title data to process")
            return

        print(f"[IMDB] Found {len(all_items):,} chart entries")

        # IMDb chart data doesn't provide scores in these files
        # Just log for reference
        updates = 0
        for item in all_items:
            self.stats['imdb_records_processed'] += 1

            imdb_id = normalize_imdb_id(item.get('imdb_id'))
            if imdb_id and imdb_id in self.imdb_index:
                # Record exists in BFD - could track chart appearances
                # but no direct column for this in schema
                pass

        self.stats['imdb_updates'] = updates
        print(f"[IMDB] Chart entries matched to BFD: {self.stats['imdb_records_processed']:,}")
        print("[IMDB] Note: IMDb FreshIn files contain charts/genres only, not score data")

    def save(self):
        """Save updated BFD."""
        print(f"\n[SAVE] Saving to {BFD_OUTPUT}...")

        self.stats['bfd_rows_end'] = len(self.bfd)

        # Verify no new columns created
        final_columns = set(self.bfd.columns)
        new_columns = final_columns - self.bfd_columns
        if new_columns:
            print(f"[ERROR] Unauthorized new columns detected: {new_columns}")
            print("[ABORT] Not saving - unauthorized columns")
            return False

        # Save parquet
        self.bfd.to_parquet(BFD_OUTPUT, index=False, engine='pyarrow', compression='snappy')
        file_size = BFD_OUTPUT.stat().st_size / (1024**3)
        print(f"[SAVE] Saved: {self.stats['bfd_rows_end']:,} rows x {len(self.bfd.columns):,} columns")
        print(f"[SAVE] File size: {file_size:.2f} GB")

        # Save report
        report = {
            'version': 'V27.66',
            'schema': 'V27.00',
            'created_at': datetime.now().isoformat(),
            'input_file': str(BFD_INPUT),
            'output_file': str(BFD_OUTPUT),
            'statistics': self.stats,
            'data_sources': {
                'disney_plus': list(FRESHIN_DIR.glob("disney_plus_*.json")),
                'flixpatrol': list(FRESHIN_DIR.glob("flixpatrol_*.json")),
                'tmdb': list(FRESHIN_DIR.glob("tmdb_*.json")),
                'imdb': list(FRESHIN_DIR.glob("imdb_*.json")),
            }
        }

        with open(REPORT_PATH, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"[SAVE] Report: {REPORT_PATH}")

        return True

    def run(self):
        """Execute full ingestion pipeline."""
        print("=" * 70)
        print("    FRESHIN INGESTION ENGINE V27.66")
        print("    Schema V27.00 Compliant - NO NEW COLUMNS")
        print("=" * 70)

        start_time = datetime.now()

        # Load BFD
        self.load_bfd()

        # Process each source in priority order
        # TMDB first (priority 1), then others
        self.process_tmdb()
        self.process_disney_plus()
        self.process_flixpatrol()
        self.process_imdb()

        # Save
        success = self.save()

        elapsed = datetime.now() - start_time

        print("\n" + "=" * 70)
        print("    INGESTION COMPLETE" if success else "    INGESTION FAILED")
        print("=" * 70)
        print(f"Duration: {elapsed}")
        print(f"\nStatistics:")
        print(f"  BFD rows: {self.stats['bfd_rows_start']:,} -> {self.stats['bfd_rows_end']:,}")
        print(f"  TMDB: {self.stats['tmdb_records_processed']:,} processed, {self.stats['tmdb_updates']:,} records updated")
        print(f"  Disney+: {self.stats['disney_records_processed']:,} processed, {self.stats['disney_updates']:,} records updated")
        print(f"  FlixPatrol: {self.stats['flixpatrol_records_processed']:,} processed, {self.stats['flixpatrol_updates']:,} records updated")
        print(f"  IMDb: {self.stats['imdb_records_processed']:,} processed")
        print(f"  Total values updated: {self.stats['total_values_updated']:,}")

        if self.stats['processing_errors']:
            print(f"\nProcessing Errors ({len(self.stats['processing_errors'])}):")
            for err in self.stats['processing_errors'][:5]:
                print(f"  - {err}")

        return self.stats


if __name__ == '__main__':
    engine = FreshInIngestionEngine()
    engine.run()
