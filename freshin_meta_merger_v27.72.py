"""
FreshIn Metadata Merger for BFD_META V27.72
============================================
Schema V27.66 Compliant - NO NEW COLUMNS

Merges NEW metadata (only) from FreshIn sources into the master BFD_META database.
- Uses Translation Mapping Guide and Alias Guide for column header normalization
- Tracks what's new for daily email reports
- Sends completion notifications to warren@framecore.ai and roy@framecore.ai

Source Priority (per Schema V27.66):
1. TMDB API (most complete, most reliable)
2. IMDb datasets (authoritative for IDs, ratings)
3. FlixPatrol API (streaming-specific data)

Created: 2026-01-27
"""

import os
import sys
import json
import hashlib
import smtplib
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# =============================================================================
# CONFIGURATION
# =============================================================================

def get_base_path() -> Path:
    """Get base path, handling both Windows native and WSL environments."""
    windows_path = Path("C:/Users/RoyT6/Downloads")
    wsl_path = Path("/mnt/c/Users/RoyT6/Downloads")

    # Check if running in WSL
    if wsl_path.exists():
        return wsl_path
    elif windows_path.exists():
        return windows_path
    else:
        # Fallback - try to detect current script location
        script_dir = Path(__file__).parent
        if 'FreshIn' in str(script_dir):
            return script_dir.parent
        return windows_path

BASE_DIR = get_base_path()
FRESHIN_DIR = BASE_DIR / "FreshIn"
SCHEMA_DIR = BASE_DIR / "Schema"

# Database paths - auto-detect latest version
def get_latest_bfd_path() -> Path:
    """Find the latest BFD database file."""
    import glob
    patterns = [
        BASE_DIR / "BFD_VIEWS_V*.parquet",
        BASE_DIR / "BFD_META_V*.parquet",
    ]
    all_files = []
    for pattern in patterns:
        all_files.extend(glob.glob(str(pattern)))
    if all_files:
        # Sort by modification time, newest first
        all_files.sort(key=lambda x: Path(x).stat().st_mtime, reverse=True)
        return Path(all_files[0])
    # Fallback
    return BASE_DIR / "BFD_VIEWS_V27.76.parquet"

BFD_META_PATH = get_latest_bfd_path()
BFD_META_BACKUP = BFD_META_PATH.with_suffix('.parquet.backup')

# Schema/mapping files
SCHEMA_PATH = SCHEMA_DIR / "SCHEMA_V27.66.json"
TRANSLATION_GUIDE_PATH = SCHEMA_DIR / "Translation Mapping Guide.json"
ALIAS_GUIDE_PATH = SCHEMA_DIR / "Aliase Guide.json"

# Report output
REPORT_DIR = BASE_DIR / "FreshIn" / "Audit Reports"
LOG_FILE = FRESHIN_DIR / "merger_log.txt"

# Email configuration - Update sender_email to your actual Gmail address
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'badgersreal@gmail.com',
    'sender_password': 'wmojroihqxvngrqb',
    'recipients': ['warren@framecore.ai', 'roy@framecore.ai'],
}

# Empty value indicators
EMPTY_VALUES = {'', 'None', 'none', 'nan', 'NaN', 'NULL', 'null', 'N/A', 'n/a', '<NA>', 'undefined'}

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def log(msg: str, level: str = "INFO"):
    """Log message to console and file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] [{level}] {msg}"
    print(formatted)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(formatted + "\n")
    except:
        pass


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
        return f"tt{s.zfill(7)}"
    if s.startswith('tt'):
        return s
    return None


def compute_row_hash(row_data: Dict) -> str:
    """Compute a hash for record deduplication."""
    content = json.dumps(row_data, sort_keys=True, default=str)
    return hashlib.md5(content.encode()).hexdigest()[:16]


# =============================================================================
# COLUMN MAPPING ENGINE
# =============================================================================

class ColumnMapper:
    """
    Handles column name translation using:
    - Translation Mapping Guide (flat alias->canonical mapping)
    - Alias Guide (grouped by canonical column)
    """

    def __init__(self):
        self.translation_map: Dict[str, str] = {}
        self.aliases_by_canonical: Dict[str, List[str]] = {}
        self.canonical_columns: Set[str] = set()

    def load_mappings(self):
        """Load all mapping guides."""
        # Load Translation Mapping Guide
        if TRANSLATION_GUIDE_PATH.exists():
            with open(TRANSLATION_GUIDE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.translation_map = data
            log(f"Loaded Translation Mapping Guide: {len(self.translation_map)} aliases")

        # Load Alias Guide
        if ALIAS_GUIDE_PATH.exists():
            with open(ALIAS_GUIDE_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract flat mapping if available
            if 'flat_mapping' in data:
                self.translation_map.update(data['flat_mapping'])

            # Extract aliases by canonical
            if 'aliases_by_canonical' in data:
                self.aliases_by_canonical = data['aliases_by_canonical']
                self.canonical_columns = set(self.aliases_by_canonical.keys())

            log(f"Loaded Alias Guide: {len(self.canonical_columns)} canonical columns")

    def to_canonical(self, column_name: str) -> str:
        """Convert any alias to canonical column name."""
        if not column_name:
            return column_name

        # Lowercase for matching
        col_lower = column_name.lower().strip()

        # Direct match in translation map
        if col_lower in self.translation_map:
            return self.translation_map[col_lower]

        # Check if already canonical
        if col_lower in self.canonical_columns:
            return col_lower

        # Original case lookup
        if column_name in self.translation_map:
            return self.translation_map[column_name]

        # Return original if no mapping found
        return column_name

    def normalize_dataframe_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Rename all columns in a DataFrame to canonical names."""
        rename_map = {}
        for col in df.columns:
            canonical = self.to_canonical(col)
            if canonical != col:
                rename_map[col] = canonical

        if rename_map:
            df = df.rename(columns=rename_map)
            log(f"Renamed {len(rename_map)} columns to canonical names")

        return df


# =============================================================================
# FRESHIN DATA LOADER
# =============================================================================

class FreshInLoader:
    """Loads and normalizes data from FreshIn source files."""

    def __init__(self, mapper: ColumnMapper):
        self.mapper = mapper
        self.processed_files: Set[str] = set()
        self.data_by_source: Dict[str, List[Dict]] = {
            'tmdb': [],
            'flixpatrol': [],
            'disney_plus': [],
            'imdb': [],
            'streaming_availability': [],
        }

    def get_latest_files(self, pattern: str, limit: int = 1) -> List[Path]:
        """Get most recent files matching pattern."""
        files = sorted(FRESHIN_DIR.glob(pattern), reverse=True)
        return files[:limit]

    def load_all_sources(self) -> Dict[str, List[Dict]]:
        """Load data from all FreshIn sources."""
        log("=" * 60)
        log("LOADING FRESHIN DATA SOURCES")
        log("=" * 60)

        # Load each source (most recent files only for daily merge)
        self._load_tmdb()
        self._load_flixpatrol()
        self._load_disney_plus()
        self._load_streaming_availability()

        # Summary
        for source, data in self.data_by_source.items():
            log(f"  {source}: {len(data):,} records")

        return self.data_by_source

    def _load_tmdb(self):
        """Load TMDB trending/popular data."""
        files = self.get_latest_files("tmdb_*.json", limit=3)
        seen_ids = set()

        for json_file in files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('source') != 'tmdb':
                        continue
                    entry_type = entry.get('type', '')
                    if 'genres' in entry_type:
                        continue  # Skip genre reference data

                    results = entry.get('data', {}).get('results', [])
                    for item in results:
                        tmdb_id = item.get('id')
                        if tmdb_id and tmdb_id not in seen_ids:
                            seen_ids.add(tmdb_id)
                            # Determine media type
                            if any(x in entry_type for x in ['tv', 'airing']):
                                item['_media_type'] = 'tv'
                            else:
                                item['_media_type'] = 'movie'
                            item['_source_file'] = json_file.name
                            self.data_by_source['tmdb'].append(item)

            except Exception as e:
                log(f"Error loading {json_file.name}: {e}", "ERROR")

        log(f"[TMDB] Loaded {len(self.data_by_source['tmdb']):,} unique records")

    def _load_flixpatrol(self):
        """Load FlixPatrol title metadata."""
        files = self.get_latest_files("flixpatrol_*.json", limit=3)
        seen_ids = set()

        for json_file in files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('source') != 'flixpatrol':
                        continue
                    if entry.get('endpoint') != 'titles':
                        continue

                    items = entry.get('data', {}).get('data', [])
                    for item in items:
                        item_data = item.get('data', {})
                        fp_id = item_data.get('id')
                        if fp_id and fp_id not in seen_ids:
                            seen_ids.add(fp_id)
                            item_data['_source_file'] = json_file.name
                            self.data_by_source['flixpatrol'].append(item_data)

            except Exception as e:
                log(f"Error loading {json_file.name}: {e}", "ERROR")

        log(f"[FlixPatrol] Loaded {len(self.data_by_source['flixpatrol']):,} unique records")

    def _load_disney_plus(self):
        """Load Disney+ catalog data."""
        files = self.get_latest_files("disney_plus_*.json", limit=3)
        seen_ids = set()

        for json_file in files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for entry in data:
                    if entry.get('type') == 'titles':
                        items = entry.get('data', {}).get('items', [])
                        for item in items:
                            imdb_id = item.get('imdb_id')
                            if imdb_id and imdb_id not in seen_ids:
                                seen_ids.add(imdb_id)
                                item['_source_file'] = json_file.name
                                self.data_by_source['disney_plus'].append(item)
                    elif entry.get('type') == 'random_title':
                        item = entry.get('data', {})
                        imdb_id = item.get('imdb_id')
                        if imdb_id and imdb_id not in seen_ids:
                            seen_ids.add(imdb_id)
                            item['_source_file'] = json_file.name
                            self.data_by_source['disney_plus'].append(item)

            except Exception as e:
                log(f"Error loading {json_file.name}: {e}", "ERROR")

        log(f"[Disney+] Loaded {len(self.data_by_source['disney_plus']):,} unique records")

    def _load_streaming_availability(self):
        """Load streaming availability data."""
        files = self.get_latest_files("streaming_availability_*.json", limit=1)

        for json_file in files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # This typically contains show availability by country
                for entry in data:
                    if entry.get('source') == 'streaming_availability':
                        shows = entry.get('data', {}).get('shows', [])
                        for show in shows:
                            show['_source_file'] = json_file.name
                            self.data_by_source['streaming_availability'].append(show)

            except Exception as e:
                log(f"Error loading {json_file.name}: {e}", "ERROR")

        log(f"[Streaming] Loaded {len(self.data_by_source['streaming_availability']):,} records")


# =============================================================================
# METADATA MERGER ENGINE
# =============================================================================

class MetadataMerger:
    """
    Merges new metadata into BFD_META database.

    Rules:
    - NEVER creates new columns
    - Updates via MERGE (fill NULL) or OVERWRITE (source-specific columns)
    - Source priority: TMDB (1) > IMDb (2) > FlixPatrol (3)
    - Tracks all changes for reporting
    """

    def __init__(self, mapper: ColumnMapper):
        self.mapper = mapper
        self.bfd: pd.DataFrame = None
        self.bfd_columns: Set[str] = set()
        self.imdb_index: Dict[str, int] = {}
        self.tmdb_index: Dict[int, int] = {}

        # Tracking
        self.stats = {
            'start_time': None,
            'end_time': None,
            'bfd_rows': 0,
            'bfd_cols': 0,
            'records_matched': 0,
            'records_updated': 0,
            'values_updated': 0,
            'new_titles_found': 0,
            'updates_by_source': {},
            'updates_by_column': {},
            'sample_updates': [],
            'errors': [],
        }

        # Track newly updated titles for email
        self.updated_titles: List[Dict] = []

    def load_database(self):
        """Load BFD_META and build indexes."""
        log("=" * 60)
        log("LOADING BFD_META DATABASE")
        log("=" * 60)

        if not BFD_META_PATH.exists():
            raise FileNotFoundError(f"Database not found: {BFD_META_PATH}")

        # Create backup
        log(f"Creating backup: {BFD_META_BACKUP}")
        import shutil
        shutil.copy(BFD_META_PATH, BFD_META_BACKUP)

        # Load database
        log(f"Loading: {BFD_META_PATH}")
        self.bfd = pd.read_parquet(BFD_META_PATH)
        self.stats['bfd_rows'] = len(self.bfd)
        self.stats['bfd_cols'] = len(self.bfd.columns)
        self.bfd_columns = set(self.bfd.columns)

        log(f"Loaded: {self.stats['bfd_rows']:,} rows x {self.stats['bfd_cols']:,} columns")

        # Build IMDb ID index
        log("Building IMDb ID index...")
        for idx, row in self.bfd.iterrows():
            imdb_id = row.get('imdb_id')
            if not is_empty(imdb_id):
                self.imdb_index[str(imdb_id)] = idx
        log(f"IMDb index: {len(self.imdb_index):,} entries")

        # Build TMDB ID index
        log("Building TMDB ID index...")
        for idx, row in self.bfd.iterrows():
            tmdb_id = row.get('tmdb_id')
            if not is_empty(tmdb_id):
                try:
                    self.tmdb_index[int(tmdb_id)] = idx
                except (ValueError, TypeError):
                    pass
        log(f"TMDB index: {len(self.tmdb_index):,} entries")

    def _update_value(self, idx: int, column: str, value: Any, mode: str = 'MERGE') -> bool:
        """
        Update a single value in BFD.

        Modes:
        - MERGE: Only update if existing value is NULL/empty
        - OVERWRITE: Always update with new value

        Returns True if value was updated.
        """
        # Map column to canonical name
        canonical_col = self.mapper.to_canonical(column)

        if canonical_col not in self.bfd_columns:
            return False

        if is_empty(value):
            return False

        current = self.bfd.at[idx, canonical_col]

        if mode == 'MERGE':
            if is_empty(current):
                self.bfd.at[idx, canonical_col] = value
                return True
            return False
        elif mode == 'OVERWRITE':
            try:
                if pd.isna(current) or current != value:
                    self.bfd.at[idx, canonical_col] = value
                    return True
            except (TypeError, ValueError):
                self.bfd.at[idx, canonical_col] = value
                return True
        return False

    def merge_tmdb(self, data: List[Dict]):
        """Merge TMDB data (Priority 1)."""
        log("\n[TMDB] Merging TMDB data (Priority 1)...")

        updates = 0
        values_updated = 0

        for item in data:
            tmdb_id = item.get('id')
            if not tmdb_id:
                continue

            try:
                tmdb_int = int(tmdb_id)
            except (ValueError, TypeError):
                continue

            if tmdb_int not in self.tmdb_index:
                self.stats['new_titles_found'] += 1
                continue

            idx = self.tmdb_index[tmdb_int]
            record_updates = 0
            title = item.get('title') or item.get('name', 'Unknown')

            # OVERWRITE: tmdb_popularity (fresh data from authoritative source)
            pop = item.get('popularity')
            if pop is not None:
                if self._update_value(idx, 'tmdb_popularity', float(pop), 'OVERWRITE'):
                    record_updates += 1
                    self._track_column_update('tmdb_popularity')

            # OVERWRITE: tmdb_score
            vote_avg = item.get('vote_average')
            if vote_avg is not None:
                if self._update_value(idx, 'tmdb_score', float(vote_avg), 'OVERWRITE'):
                    record_updates += 1
                    self._track_column_update('tmdb_score')

            # MERGE: premiere_date
            release = item.get('release_date') or item.get('first_air_date')
            if release:
                if self._update_value(idx, 'premiere_date', str(release), 'MERGE'):
                    record_updates += 1
                    self._track_column_update('premiere_date')

            # MERGE: original_language
            lang = item.get('original_language')
            if lang:
                if self._update_value(idx, 'original_language', str(lang), 'MERGE'):
                    record_updates += 1
                    self._track_column_update('original_language')

            # MERGE: overview
            overview = item.get('overview')
            if overview:
                if self._update_value(idx, 'overview', str(overview), 'MERGE'):
                    record_updates += 1
                    self._track_column_update('overview')

            if record_updates > 0:
                updates += 1
                values_updated += record_updates
                # Get rich details for email
                overview = item.get('overview')
                genre_ids = item.get('genre_ids', [])
                release = item.get('release_date') or item.get('first_air_date')
                year = release[:4] if release else None
                self._track_title_update(
                    title, 'TMDB', record_updates,
                    overview=overview,
                    genres=', '.join(str(g) for g in genre_ids[:3]) if genre_ids else None,
                    year=year,
                    tmdb_score=item.get('vote_average'),
                    popularity=item.get('popularity')
                )

        self.stats['updates_by_source']['tmdb'] = {'records': updates, 'values': values_updated}
        self.stats['records_updated'] += updates
        self.stats['values_updated'] += values_updated
        log(f"[TMDB] Updated: {updates:,} records, {values_updated:,} values")

    def merge_flixpatrol(self, data: List[Dict]):
        """Merge FlixPatrol data (Priority 3)."""
        log("\n[FlixPatrol] Merging FlixPatrol data (Priority 3)...")

        updates = 0
        values_updated = 0

        for item in data:
            imdb_raw = item.get('imdbId')
            tmdb_id = item.get('tmdbId')

            idx = None

            # Try IMDb match first
            if imdb_raw:
                imdb_id = normalize_imdb_id(imdb_raw)
                if imdb_id and imdb_id in self.imdb_index:
                    idx = self.imdb_index[imdb_id]

            # Fallback to TMDB
            if idx is None and tmdb_id:
                try:
                    tmdb_int = int(tmdb_id)
                    if tmdb_int in self.tmdb_index:
                        idx = self.tmdb_index[tmdb_int]
                except (ValueError, TypeError):
                    pass

            if idx is None:
                self.stats['new_titles_found'] += 1
                continue

            record_updates = 0
            title = item.get('title', 'Unknown')

            # OVERWRITE: flixpatrol_id (FlixPatrol-specific)
            if self._update_value(idx, 'flixpatrol_id', item.get('id'), 'OVERWRITE'):
                record_updates += 1
                self._track_column_update('flixpatrol_id')

            # OVERWRITE: flixpatrol_points
            if self._update_value(idx, 'flixpatrol_points', item.get('points'), 'OVERWRITE'):
                record_updates += 1
                self._track_column_update('flixpatrol_points')

            # OVERWRITE: flixpatrol_rank
            if self._update_value(idx, 'flixpatrol_rank', item.get('rank'), 'OVERWRITE'):
                record_updates += 1
                self._track_column_update('flixpatrol_rank')

            # MERGE: premiere_date
            if self._update_value(idx, 'premiere_date', item.get('premiere'), 'MERGE'):
                record_updates += 1
                self._track_column_update('premiere_date')

            # MERGE: runtime_minutes
            if self._update_value(idx, 'runtime_minutes', item.get('length'), 'MERGE'):
                record_updates += 1
                self._track_column_update('runtime_minutes')

            # MERGE: budget
            if self._update_value(idx, 'budget', item.get('budget'), 'MERGE'):
                record_updates += 1
                self._track_column_update('budget')

            # MERGE: overview/tagline
            desc = item.get('description')
            if desc:
                if 'tagline' in self.bfd_columns:
                    if self._update_value(idx, 'tagline', desc, 'MERGE'):
                        record_updates += 1
                        self._track_column_update('tagline')
                elif 'overview' in self.bfd_columns:
                    if self._update_value(idx, 'overview', desc, 'MERGE'):
                        record_updates += 1
                        self._track_column_update('overview')

            if record_updates > 0:
                updates += 1
                values_updated += record_updates
                premiere = item.get('premiere')
                year = premiere[:4] if premiere else None
                self._track_title_update(
                    title, 'FlixPatrol', record_updates,
                    overview=item.get('description'),
                    year=year
                )

        self.stats['updates_by_source']['flixpatrol'] = {'records': updates, 'values': values_updated}
        self.stats['records_updated'] += updates
        self.stats['values_updated'] += values_updated
        log(f"[FlixPatrol] Updated: {updates:,} records, {values_updated:,} values")

    def merge_disney_plus(self, data: List[Dict]):
        """Merge Disney+ data."""
        log("\n[Disney+] Merging Disney+ data...")

        updates = 0
        values_updated = 0

        for item in data:
            imdb_id = normalize_imdb_id(item.get('imdb_id'))
            if not imdb_id or imdb_id not in self.imdb_index:
                continue

            idx = self.imdb_index[imdb_id]
            record_updates = 0
            title = item.get('title', 'Unknown')

            # MERGE: imdb_score
            if self._update_value(idx, 'imdb_score', item.get('imdb_score'), 'MERGE'):
                record_updates += 1
                self._track_column_update('imdb_score')

            # MERGE: imdb_vote_count
            if self._update_value(idx, 'imdb_vote_count', item.get('imdb_votes'), 'MERGE'):
                record_updates += 1
                self._track_column_update('imdb_vote_count')

            # MERGE: tmdb_score
            if self._update_value(idx, 'tmdb_score', item.get('tmdb_score'), 'MERGE'):
                record_updates += 1
                self._track_column_update('tmdb_score')

            # MERGE: tmdb_popularity
            if self._update_value(idx, 'tmdb_popularity', item.get('tmdb_popularity'), 'MERGE'):
                record_updates += 1
                self._track_column_update('tmdb_popularity')

            # MERGE: age_certification
            if self._update_value(idx, 'age_certification', item.get('age_certification'), 'MERGE'):
                record_updates += 1
                self._track_column_update('age_certification')

            # MERGE: runtime_minutes
            if self._update_value(idx, 'runtime_minutes', item.get('runtime'), 'MERGE'):
                record_updates += 1
                self._track_column_update('runtime_minutes')

            # MERGE: genres (array to semicolon-separated)
            genres = item.get('genres')
            if genres and isinstance(genres, list):
                genres_str = '; '.join(genres)
                if self._update_value(idx, 'genres', genres_str, 'MERGE'):
                    record_updates += 1
                    self._track_column_update('genres')

            # MERGE: streaming platform marker
            if self._update_value(idx, 'streaming_platform_us', 'Disney+', 'MERGE'):
                record_updates += 1
                self._track_column_update('streaming_platform_us')

            if record_updates > 0:
                updates += 1
                values_updated += record_updates
                genres = item.get('genres')
                genres_str = ', '.join(genres[:3]) if genres and isinstance(genres, list) else None
                self._track_title_update(
                    title, 'Disney+', record_updates,
                    overview=item.get('overview'),
                    genres=genres_str,
                    year=str(item.get('year')) if item.get('year') else None,
                    tmdb_score=item.get('tmdb_score')
                )

        self.stats['updates_by_source']['disney_plus'] = {'records': updates, 'values': values_updated}
        self.stats['records_updated'] += updates
        self.stats['values_updated'] += values_updated
        log(f"[Disney+] Updated: {updates:,} records, {values_updated:,} values")

    def _track_column_update(self, column: str):
        """Track updates per column."""
        if column not in self.stats['updates_by_column']:
            self.stats['updates_by_column'][column] = 0
        self.stats['updates_by_column'][column] += 1

    def _track_title_update(self, title: str, source: str, field_count: int,
                            overview: str = None, genres: str = None, year: str = None,
                            tmdb_score: float = None, popularity: float = None):
        """Track updated titles for reporting with rich details."""
        if len(self.updated_titles) < 100:  # Limit for email size
            self.updated_titles.append({
                'title': title,
                'source': source,
                'fields_updated': field_count,
                'overview': (overview[:200] + '...') if overview and len(overview) > 200 else overview,
                'genres': genres,
                'year': year,
                'tmdb_score': tmdb_score,
                'popularity': popularity,
            })

    def save_database(self):
        """Save updated BFD_META."""
        log("\n[SAVE] Saving updated database...")

        # Verify no new columns were created
        final_columns = set(self.bfd.columns)
        new_columns = final_columns - self.bfd_columns
        if new_columns:
            log(f"ERROR: Unauthorized new columns detected: {new_columns}", "ERROR")
            self.stats['errors'].append(f"Unauthorized columns: {new_columns}")
            return False

        # Save
        self.bfd.to_parquet(BFD_META_PATH, index=False, engine='pyarrow', compression='snappy')
        file_size = BFD_META_PATH.stat().st_size / (1024**3)
        log(f"Saved: {len(self.bfd):,} rows to {BFD_META_PATH}")
        log(f"File size: {file_size:.2f} GB")

        return True

    def generate_report(self) -> Dict:
        """Generate detailed merge report."""
        self.stats['end_time'] = datetime.now().isoformat()

        report = {
            'report_type': 'FreshIn Metadata Merge',
            'version': 'V27.72',
            'timestamp': self.stats['end_time'],
            'database': str(BFD_META_PATH),
            'statistics': {
                'database_rows': self.stats['bfd_rows'],
                'database_columns': self.stats['bfd_cols'],
                'records_updated': self.stats['records_updated'],
                'values_updated': self.stats['values_updated'],
                'new_titles_not_in_db': self.stats['new_titles_found'],
            },
            'updates_by_source': self.stats['updates_by_source'],
            'updates_by_column': self.stats['updates_by_column'],
            'sample_updated_titles': self.updated_titles[:25],
            'errors': self.stats['errors'],
        }

        # Save report
        REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_file = REPORT_DIR / f"merge_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, default=str)
        log(f"Report saved: {report_file}")

        return report


# =============================================================================
# EMAIL NOTIFICATION
# =============================================================================

def send_email_notification(report: Dict, success: bool):
    """Send email notification to warren@framecore.ai and roy@framecore.ai."""

    if not EMAIL_CONFIG['sender_email'] or not EMAIL_CONFIG['sender_password']:
        log("Email not configured - skipping notification", "WARN")
        log("Set FRESHIN_SENDER_EMAIL and FRESHIN_SENDER_PASSWORD environment variables")
        return False

    # Build email content
    status_emoji = "🎬" if success else "❌"
    status = "SUCCESS" if success else "FAILED"
    subject = f"{status_emoji} FreshIn Daily Digest - {datetime.now().strftime('%Y-%m-%d')} - {status}"

    # HTML body
    stats = report.get('statistics', {})
    updates_by_source = report.get('updates_by_source', {})
    updates_by_column = report.get('updates_by_column', {})
    sample_titles = report.get('sample_updated_titles', [])

    # Source emojis
    source_emoji = {'TMDB': '🎥', 'FlixPatrol': '📊', 'Disney+': '✨', 'tmdb': '🎥', 'flixpatrol': '📊', 'disney_plus': '✨'}

    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 0; padding: 20px; background: #f5f7fa; }}
            .container {{ max-width: 800px; margin: 0 auto; background: white; border-radius: 12px; box-shadow: 0 2px 12px rgba(0,0,0,0.1); overflow: hidden; }}
            .header {{ background: linear-gradient(135deg, {'#28a745' if success else '#dc3545'}, {'#20c997' if success else '#e74c3c'}); color: white; padding: 30px; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 28px; }}
            .header p {{ margin: 10px 0 0; opacity: 0.9; }}
            .content {{ padding: 30px; }}
            .stats-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 30px; }}
            .stat-card {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 20px; border-radius: 10px; text-align: center; }}
            .stat-card.green {{ background: linear-gradient(135deg, #11998e, #38ef7d); }}
            .stat-card.orange {{ background: linear-gradient(135deg, #f093fb, #f5576c); }}
            .stat-number {{ font-size: 32px; font-weight: bold; }}
            .stat-label {{ font-size: 12px; text-transform: uppercase; opacity: 0.9; margin-top: 5px; }}
            h2 {{ color: #333; border-bottom: 2px solid #667eea; padding-bottom: 10px; margin-top: 30px; }}
            h2 span {{ font-size: 24px; margin-right: 8px; }}
            table {{ width: 100%; border-collapse: collapse; margin: 15px 0; }}
            th {{ background: #667eea; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 12px; border-bottom: 1px solid #eee; }}
            tr:hover {{ background: #f8f9ff; }}
            .title-card {{ background: #f8f9ff; border-radius: 8px; padding: 15px; margin: 10px 0; border-left: 4px solid #667eea; }}
            .title-card h3 {{ margin: 0 0 8px; color: #333; }}
            .title-card .meta {{ color: #666; font-size: 13px; margin-bottom: 8px; }}
            .title-card .meta span {{ background: #e8eaff; padding: 2px 8px; border-radius: 12px; margin-right: 8px; }}
            .title-card .overview {{ color: #555; font-size: 14px; line-height: 1.5; font-style: italic; }}
            .source-badge {{ display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: bold; }}
            .source-tmdb {{ background: #01b4e4; color: white; }}
            .source-flixpatrol {{ background: #ff6b6b; color: white; }}
            .source-disney {{ background: #113ccf; color: white; }}
            .footer {{ background: #f5f7fa; padding: 20px; text-align: center; color: #666; font-size: 12px; }}
            .pill {{ display: inline-block; background: #e8eaff; color: #667eea; padding: 4px 10px; border-radius: 15px; font-size: 11px; margin: 2px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎬 FreshIn Daily Digest</h1>
                <p>{datetime.now().strftime('%A, %B %d, %Y at %I:%M %p')}</p>
            </div>

            <div class="content">
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-number">{stats.get('records_updated', 0):,}</div>
                        <div class="stat-label">Titles Updated</div>
                    </div>
                    <div class="stat-card green">
                        <div class="stat-number">{stats.get('values_updated', 0):,}</div>
                        <div class="stat-label">Values Merged</div>
                    </div>
                    <div class="stat-card orange">
                        <div class="stat-number">{stats.get('new_titles_not_in_db', 0):,}</div>
                        <div class="stat-label">New Discoveries</div>
                    </div>
                </div>

                <h2><span>📡</span> Data Sources</h2>
                <table>
                    <tr><th>Source</th><th>Records</th><th>Values</th></tr>
    """

    for source, data in updates_by_source.items():
        emoji = source_emoji.get(source, '📦')
        source_class = 'tmdb' if 'tmdb' in source.lower() else ('flixpatrol' if 'flix' in source.lower() else 'disney')
        html += f'<tr><td><span class="source-badge source-{source_class}">{emoji} {source.upper()}</span></td><td><strong>{data.get("records", 0):,}</strong></td><td>{data.get("values", 0):,}</td></tr>'

    html += """
                </table>

                <h2><span>🔥</span> What's Hot - Updated Titles</h2>
    """

    # Show detailed title cards
    for i, item in enumerate(sample_titles[:15]):
        title = item.get('title', 'Unknown')
        source = item.get('source', '')
        overview = item.get('overview', '')
        year = item.get('year', '')
        genres = item.get('genres', '')
        score = item.get('tmdb_score')
        popularity = item.get('popularity')
        fields = item.get('fields_updated', 0)

        source_class = 'tmdb' if 'tmdb' in source.lower() else ('flixpatrol' if 'flix' in source.lower() else 'disney')
        emoji = source_emoji.get(source, '📦')

        # Build meta line
        meta_parts = []
        if year:
            meta_parts.append(f'<span>📅 {year}</span>')
        if score:
            meta_parts.append(f'<span>⭐ {score:.1f}</span>')
        if genres:
            meta_parts.append(f'<span>🎭 {genres}</span>')
        meta_parts.append(f'<span>✏️ {fields} fields</span>')
        meta_line = ' '.join(meta_parts)

        html += f'''
                <div class="title-card">
                    <h3>{emoji} {title} <span class="source-badge source-{source_class}">{source}</span></h3>
                    <div class="meta">{meta_line}</div>
        '''
        if overview:
            html += f'<div class="overview">"{overview}"</div>'
        html += '</div>'

    # Columns updated section
    html += """
                <h2><span>📊</span> Columns Updated</h2>
                <div style="margin: 15px 0;">
    """
    for col, count in sorted(updates_by_column.items(), key=lambda x: x[1], reverse=True)[:12]:
        html += f'<span class="pill">{col}: {count:,}</span> '

    html += f"""
                </div>
            </div>

            <div class="footer">
                <p>🤖 Automated by FreshIn Metadata Merger V27.72</p>
                <p>Database: BFD_META_V27.72.parquet | {stats.get('database_rows', 0):,} total titles</p>
            </div>
        </div>
    </body>
    </html>
    """

    # Create message
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = EMAIL_CONFIG['sender_email']
    msg['To'] = ', '.join(EMAIL_CONFIG['recipients'])

    # Plain text fallback
    plain_text = f"""
FreshIn Daily Merge Report - {status}
Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Summary:
- Records Updated: {stats.get('records_updated', 0):,}
- Values Updated: {stats.get('values_updated', 0):,}
- New Titles Found: {stats.get('new_titles_not_in_db', 0):,}

See HTML version for full details.
    """

    msg.attach(MIMEText(plain_text, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    # Send
    try:
        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.sendmail(
                EMAIL_CONFIG['sender_email'],
                EMAIL_CONFIG['recipients'],
                msg.as_string()
            )
        log(f"Email sent to: {', '.join(EMAIL_CONFIG['recipients'])}")
        return True
    except Exception as e:
        log(f"Email failed: {e}", "ERROR")
        return False


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run_merge():
    """Execute the full FreshIn metadata merge pipeline."""
    log("=" * 70)
    log("    FRESHIN METADATA MERGER V27.72")
    log("    Schema V27.66 Compliant - NO NEW COLUMNS")
    log("=" * 70)

    start_time = datetime.now()
    success = False
    report = {}

    try:
        # Initialize column mapper
        mapper = ColumnMapper()
        mapper.load_mappings()

        # Load FreshIn data
        loader = FreshInLoader(mapper)
        source_data = loader.load_all_sources()

        # Initialize merger
        merger = MetadataMerger(mapper)
        merger.stats['start_time'] = start_time.isoformat()
        merger.load_database()

        # Merge in priority order
        # Priority 1: TMDB (most reliable)
        merger.merge_tmdb(source_data['tmdb'])

        # Priority 2: Disney+ (platform-specific)
        merger.merge_disney_plus(source_data['disney_plus'])

        # Priority 3: FlixPatrol
        merger.merge_flixpatrol(source_data['flixpatrol'])

        # Save
        success = merger.save_database()

        # Generate report
        report = merger.generate_report()

    except Exception as e:
        log(f"FATAL ERROR: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        report = {
            'error': str(e),
            'timestamp': datetime.now().isoformat(),
            'statistics': {'records_updated': 0, 'values_updated': 0}
        }

    elapsed = datetime.now() - start_time

    # Summary
    log("\n" + "=" * 70)
    log("    MERGE COMPLETE" if success else "    MERGE FAILED")
    log("=" * 70)
    log(f"Duration: {elapsed}")
    log(f"Records updated: {report.get('statistics', {}).get('records_updated', 0):,}")
    log(f"Values updated: {report.get('statistics', {}).get('values_updated', 0):,}")

    # Send email notification
    send_email_notification(report, success)

    return report


if __name__ == '__main__':
    run_merge()
