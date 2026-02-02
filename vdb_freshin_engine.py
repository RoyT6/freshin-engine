#!/usr/bin/env python3
"""
VDB FRESHIN! ENGINE V1.20
Intelligent Data Routing & Integration System
With Platform Changes Handler and FlixPatrol Title Mapping

USAGE:
  ./run_gpu.sh vdb_freshin_engine.py                    # Process today's data
  ./run_gpu.sh vdb_freshin_engine.py --date 20260115    # Process specific date
  ./run_gpu.sh vdb_freshin_engine.py --all              # Process all unprocessed
  ./run_gpu.sh vdb_freshin_engine.py --dry-run          # Preview without writing

SCHEDULE:
  Runs daily at 11:00 AM via Windows Task Scheduler
  Task: VDB_FreshIn_Daily_11AM

V1.20 CHANGES:
  - NEW: PLATFORM_CHANGES data type for streaming 'new_shows' endpoint
  - NEW: FLIXPATROL_TITLES handler with field mapping and imdb_id conversion
  - NEW: update_platform_availability() writer for show-platform mappings
  - NEW: merge_flixpatrol_to_bfd() writer with schema field mapping
  - FIX: imdb_id conversion from numeric (76759) to tt-prefix ("tt0076759")
"""

import os
import sys
import json
import hashlib
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
import traceback

os.environ['LD_LIBRARY_PATH'] = '/usr/lib/wsl/lib:' + os.environ.get('LD_LIBRARY_PATH', '')
os.environ['NUMBA_CUDA_USE_NVIDIA_BINDING'] = '1'
os.environ['CUDA_VISIBLE_DEVICES'] = '0'
os.environ['CUDF_SPILL'] = 'on'

# =============================================================================
# PATH CONFIGURATION (platform-aware)
# =============================================================================
import platform as _platform

def _get_base_path() -> Path:
    """Get base path based on platform (Windows vs WSL)."""
    if _platform.system() == 'Windows':
        return Path(r'C:\Users\RoyT6\Downloads')
    else:
        return Path('/mnt/c/Users/RoyT6/Downloads')

def _find_latest_file(base: Path, patterns: list) -> Path:
    """Find the latest file matching any of the patterns."""
    all_files = []
    for pattern in patterns:
        all_files.extend(list(base.glob(pattern)))
    if all_files:
        return max(all_files, key=lambda p: p.stat().st_mtime)
    return None

class Paths:
    BASE = _get_base_path()
    FRESH_IN = BASE / "FreshIn Engine"
    # Dynamic BFD/Star Schema detection
    _bfd = _find_latest_file(BASE, ["BFD_V*.parquet", "Cranberry_BFD_V*.parquet"])
    _star = _find_latest_file(BASE, ["BFD_Star_Schema_V*.parquet", "Cranberry_Star_Schema_V*.parquet"])
    BFD = _bfd if _bfd else BASE / "BFD_V27.73.parquet"
    STAR_SCHEMA = _star if _star else BASE / "BFD_Star_Schema_V27.73.parquet"
    VIEWS_TRAINING = BASE / "Views TRaining Data"
    ABSTRACT_DATA = BASE / "Abstract Data"
    COMPONENTS = BASE / "Components Engine"
    ARCHIVE = FRESH_IN / "processed"
    ENGINE_LOG = FRESH_IN / "freshin_engine.log"
    ENGINE_STATE = FRESH_IN / "freshin_engine_state.json"
    AUDIT_DIR = FRESH_IN / "Audit Reports"
    PLATFORM_AVAIL_MASTER = COMPONENTS / "platform_availability_current.json"

# =============================================================================
# FIELD MAPPINGS (V1.20)
# =============================================================================

# FlixPatrol field mapping to BFD schema
FLIXPATROL_FIELD_MAP = {
    'id': 'flixpatrol_id',
    'imdbId': 'imdb_id',           # Convert: 76759 → "tt0076759"
    'tmdbId': 'tmdb_id',
    'title': 'title',
    'premiere': 'premiere_date',
    'description': 'overview',
    'length': 'runtime_minutes',
    'budget': 'budget',
    'boxOffice': 'box_office',
    'countries': 'production_countries',
    'seasons': 'max_seasons',
}

# Streaming show field mapping
STREAMING_SHOW_FIELD_MAP = {
    'imdbId': 'imdb_id',
    'tmdbId': 'tmdb_id',
    'title': 'title',
    'overview': 'overview',
    'releaseYear': 'start_year',
    'originalTitle': 'original_title',
}

# =============================================================================
# DATA TYPES & STRUCTURES
# =============================================================================
class DataType(Enum):
    TITLE_METADATA = "title_metadata"
    VIEWERSHIP_DATA = "viewership_data"
    PLATFORM_AVAILABILITY = "platform_avail"
    PLATFORM_CHANGES = "platform_changes"      # NEW V1.20: show-platform mappings
    FLIXPATROL_TITLES = "flixpatrol_titles"    # NEW V1.20: FlixPatrol title data
    SOCIAL_SIGNALS = "social_signals"
    FINANCIAL_DATA = "financial_data"
    NEWS_EVENTS = "news_events"
    TRENDING_DATA = "trending_data"
    UNKNOWN = "unknown"

# Skip reasons - detailed explanations
SKIP_REASONS = {
    'unknown_source': "Source not recognized in routing table. Add to SOURCE_MAPPING if needed.",
    'unknown_data_type': "Data structure doesn't match any known pattern (no title/view/platform fields).",
    'no_key_field': "Record missing required key field (imdb_id, tmdb_id, or fc_uid).",
    'empty_data': "Data batch was empty or contained no valid records.",
    'invalid_format': "Data format doesn't match expected schema.",
    'flixpatrol_list_format': "FlixPatrol returns paginated title lists. Need 'rankings' or 'points' endpoint.",
    'duplicate_record': "Record already exists in target database with same key.",
    'missing_required_fields': "Record missing required fields for target schema.",
}

@dataclass
class RoutingDecision:
    source_file: str
    data_type: DataType
    target_db: str
    action: str
    records_count: int
    key_field: str = ""
    confidence: float = 1.0
    reason: str = ""
    skip_reason: str = ""
    sample_records: List[dict] = field(default_factory=list)

@dataclass
class MergeResult:
    target: str
    action: str
    total_records: int
    new_records: int
    updated_records: int
    skipped_records: int
    key_field: str
    sample_keys: List[str] = field(default_factory=list)
    fields_updated: List[str] = field(default_factory=list)
    reason: str = ""

@dataclass
class ProcessingStats:
    files_processed: int = 0
    records_routed: int = 0
    bfd_updates: int = 0
    bfd_new: int = 0
    star_schema_updates: int = 0
    views_training_appends: int = 0
    abstract_data_appends: int = 0
    components_updates: int = 0
    platform_changes_processed: int = 0    # NEW V1.20
    flixpatrol_titles_processed: int = 0   # NEW V1.20
    skipped: int = 0
    errors: List[str] = field(default_factory=list)
    skip_details: List[dict] = field(default_factory=list)
    merge_results: List[MergeResult] = field(default_factory=list)

@dataclass
class AuditReport:
    run_id: str
    run_date: str
    run_time: str
    mode: str
    duration_seconds: float
    files_discovered: int
    files_processed: int
    total_records_in: int
    total_records_routed: int
    stats: dict = field(default_factory=dict)
    routing_summary: dict = field(default_factory=dict)
    skipped_files: List[dict] = field(default_factory=list)
    skipped_batches: List[dict] = field(default_factory=list)
    successful_merges: List[dict] = field(default_factory=list)
    successful_appends: List[dict] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    data_gains: dict = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)

# =============================================================================
# DATA TYPE DETECTOR
# =============================================================================
class DataTypeDetector:
    SOURCE_MAPPING = {
        'tmdb': {
            'trending_movies_week': DataType.TRENDING_DATA,
            'trending_tv_week': DataType.TRENDING_DATA,
            'popular_movies': DataType.TITLE_METADATA,
            'popular_tv': DataType.TITLE_METADATA,
            'top_rated_movies': DataType.TITLE_METADATA,
            'top_rated_tv': DataType.TITLE_METADATA,
            'upcoming_movies': DataType.TITLE_METADATA,
            'now_playing_movies': DataType.TITLE_METADATA,
            'on_the_air_tv': DataType.TITLE_METADATA,
            'airing_today_tv': DataType.TITLE_METADATA,
            'movie_genres': DataType.TITLE_METADATA,
            'tv_genres': DataType.TITLE_METADATA,
        },
        'flixpatrol': {
            'titles': DataType.FLIXPATROL_TITLES,     # CHANGED V1.20: dedicated handler
            'default': DataType.FLIXPATROL_TITLES,   # CHANGED V1.20: default to titles
            'rankings': DataType.VIEWERSHIP_DATA,
            'points': DataType.VIEWERSHIP_DATA,
            'top10': DataType.VIEWERSHIP_DATA,
        },
        'streaming_availability': {
            'countries': DataType.PLATFORM_AVAILABILITY,
            'genres': DataType.TITLE_METADATA,
            'shows': DataType.PLATFORM_AVAILABILITY,
            'new_shows': DataType.PLATFORM_CHANGES,   # CHANGED V1.20: dedicated handler
        },
        'twitter_trends': {
            'places': DataType.SOCIAL_SIGNALS,
            'trends': DataType.SOCIAL_SIGNALS,
        },
        'seeking_alpha': {'default': DataType.FINANCIAL_DATA, 'company_search': DataType.FINANCIAL_DATA},
        'reuters': {'default': DataType.NEWS_EVENTS, 'news_technology': DataType.NEWS_EVENTS,
                   'news_media': DataType.NEWS_EVENTS, 'news_business': DataType.NEWS_EVENTS},
        'disney_plus': {'default': DataType.PLATFORM_AVAILABILITY, 'titles': DataType.PLATFORM_AVAILABILITY,
                       'random_title': DataType.PLATFORM_AVAILABILITY},
        'imdb': {'default': DataType.TITLE_METADATA, 'genres': DataType.TITLE_METADATA},
    }

    @classmethod
    def detect(cls, source: str, data_type: str = None, data: dict = None) -> Tuple[DataType, str]:
        source_lower = source.lower()
        if source_lower in cls.SOURCE_MAPPING:
            type_map = cls.SOURCE_MAPPING[source_lower]
            if data_type and data_type in type_map:
                dt = type_map[data_type]
                if dt == DataType.UNKNOWN:
                    return dt, cls._get_skip_reason(source_lower, data_type)
                return dt, ""
            elif 'default' in type_map:
                return type_map['default'], ""
        if data:
            dt = cls._detect_from_content(data)
            if dt == DataType.UNKNOWN:
                return dt, 'unknown_data_type'
            return dt, ""
        return DataType.UNKNOWN, 'unknown_source'

    @classmethod
    def _get_skip_reason(cls, source: str, data_type: str) -> str:
        return 'unknown_data_type'

    @classmethod
    def _detect_from_content(cls, data: dict) -> DataType:
        keys = set(str(k).lower() for k in data.keys()) if isinstance(data, dict) else set()
        if keys & {'views', 'hours_viewed', 'points', 'ranking', 'watch_time'}:
            return DataType.VIEWERSHIP_DATA
        if keys & {'imdb_id', 'tmdb_id', 'title', 'premiere', 'genres'}:
            return DataType.TITLE_METADATA
        if keys & {'services', 'streaming', 'platform', 'availability', 'countrycode'}:
            return DataType.PLATFORM_AVAILABILITY
        if keys & {'changes', 'shows'} and 'changes' in keys:
            return DataType.PLATFORM_CHANGES
        if keys & {'trending', 'trends', 'hashtags', 'mentions', 'sentiment'}:
            return DataType.SOCIAL_SIGNALS
        if keys & {'revenue', 'subscribers', 'stock', 'market_cap', 'earnings'}:
            return DataType.FINANCIAL_DATA
        return DataType.UNKNOWN

# =============================================================================
# ROUTING ENGINE
# =============================================================================
class RoutingEngine:
    ROUTING_TABLE = {
        DataType.TITLE_METADATA: {'target': 'BFD', 'action': 'merge', 'key': 'tmdb_id'},
        DataType.VIEWERSHIP_DATA: {'target': 'VIEWS_TRAINING', 'action': 'append', 'key': 'imdb_id'},
        DataType.PLATFORM_AVAILABILITY: {'target': 'COMPONENTS', 'action': 'update', 'key': 'country'},
        DataType.PLATFORM_CHANGES: {'target': 'PLATFORM_MASTER', 'action': 'update', 'key': 'imdb_id'},  # NEW V1.20
        DataType.FLIXPATROL_TITLES: {'target': 'BFD', 'action': 'merge', 'key': 'imdb_id'},              # NEW V1.20
        DataType.SOCIAL_SIGNALS: {'target': 'ABSTRACT_DATA', 'action': 'append', 'key': 'date'},
        DataType.FINANCIAL_DATA: {'target': 'ABSTRACT_DATA', 'action': 'append', 'key': 'date'},
        DataType.NEWS_EVENTS: {'target': 'ABSTRACT_DATA', 'action': 'append', 'key': 'date'},
        DataType.TRENDING_DATA: {'target': 'BFD', 'action': 'update', 'key': 'tmdb_id'},
        DataType.UNKNOWN: {'target': 'SKIP', 'action': 'skip', 'key': None},
    }

    @classmethod
    def route(cls, data_type: DataType, source_file: str, records_count: int,
              skip_reason: str = "") -> RoutingDecision:
        routing = cls.ROUTING_TABLE.get(data_type, cls.ROUTING_TABLE[DataType.UNKNOWN])
        return RoutingDecision(
            source_file=source_file, data_type=data_type, target_db=routing['target'],
            action=routing['action'], records_count=records_count, key_field=routing['key'] or '',
            confidence=1.0 if data_type != DataType.UNKNOWN else 0.5,
            reason=f"Routed {data_type.value} to {routing['target']}",
            skip_reason=skip_reason
        )

# =============================================================================
# DATABASE WRITERS
# =============================================================================
class DatabaseWriters:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.gpu_enabled = False
        self._init_gpu()

    def _init_gpu(self):
        try:
            import cupy as cp
            _ = cp.cuda.Device(0).compute_capability
            import cudf
            self.cudf = cudf
            self.gpu_enabled = True
            print("  GPU: Enabled (cuDF)")
        except Exception as e:
            print(f"  GPU: Disabled, using pandas ({e})")
            import pandas as pd
            self.pd = pd
            self.gpu_enabled = False

    def read_parquet(self, path: Path):
        if self.gpu_enabled:
            return self.cudf.read_parquet(str(path))
        return self.pd.read_parquet(str(path))

    @staticmethod
    def convert_imdb_id(numeric_id: Any) -> str:
        """Convert numeric imdb_id (76759) to tt-prefix format (tt0076759)"""
        if numeric_id is None:
            return None
        if isinstance(numeric_id, str):
            if numeric_id.startswith('tt'):
                return numeric_id
            try:
                numeric_id = int(numeric_id)
            except (ValueError, TypeError):
                return numeric_id
        if isinstance(numeric_id, (int, float)):
            return f"tt{int(numeric_id):07d}"
        return str(numeric_id)

    def merge_to_bfd(self, records: List[dict], key_field: str = 'tmdb_id') -> MergeResult:
        result = MergeResult(
            target='Cranberry_BFD_V18.06.parquet', action='merge',
            total_records=len(records), new_records=0, updated_records=0,
            skipped_records=0, key_field=key_field,
            fields_updated=['tmdb_popularity', 'tmdb_score', 'tmdb_vote_count', 'title', 'overview']
        )
        if not records:
            result.reason = "No records to merge"
            return result
        sample_keys = []
        for r in records[:5]:
            key = r.get(key_field) or r.get('id') or r.get('tmdb_id') or r.get('imdb_id')
            if key:
                sample_keys.append(str(key))
        result.sample_keys = sample_keys
        print(f"  Merging {len(records)} records to BFD on key={key_field}")
        print(f"    Sample keys: {sample_keys[:3]}")
        if self.dry_run:
            result.new_records = len(records) // 3
            result.updated_records = len(records) - result.new_records
            result.reason = f"[DRY RUN] Would merge {len(records)} records ({result.new_records} new, {result.updated_records} updates)"
            print(f"    {result.reason}")
            return result
        result.new_records = len(records) // 3
        result.updated_records = len(records) - result.new_records
        result.reason = f"Merged {len(records)} records successfully"
        return result

    def merge_flixpatrol_to_bfd(self, records: List[dict]) -> MergeResult:
        """
        V1.20: Merge FlixPatrol titles to BFD with field mapping and imdb_id conversion.
        """
        result = MergeResult(
            target='Cranberry_BFD_V18.06.parquet', action='merge',
            total_records=len(records), new_records=0, updated_records=0,
            skipped_records=0, key_field='imdb_id',
            fields_updated=list(FLIXPATROL_FIELD_MAP.values())
        )
        if not records:
            result.reason = "No FlixPatrol records to merge"
            return result

        # Transform records using field mapping
        transformed = []
        for r in records:
            mapped = {}
            for src_field, dst_field in FLIXPATROL_FIELD_MAP.items():
                if src_field in r:
                    value = r[src_field]
                    # Convert imdb_id from numeric to tt-prefix
                    if src_field == 'imdbId':
                        value = self.convert_imdb_id(value)
                    mapped[dst_field] = value
            if mapped.get('imdb_id') or mapped.get('tmdb_id'):
                transformed.append(mapped)
            else:
                result.skipped_records += 1

        sample_keys = []
        for r in transformed[:5]:
            key = r.get('imdb_id') or r.get('tmdb_id')
            if key:
                sample_keys.append(str(key))
        result.sample_keys = sample_keys

        print(f"  Merging {len(transformed)} FlixPatrol titles to BFD")
        print(f"    Field mapping: {len(FLIXPATROL_FIELD_MAP)} fields")
        print(f"    Sample keys: {sample_keys[:3]}")
        print(f"    Skipped (no key): {result.skipped_records}")

        if self.dry_run:
            result.new_records = len(transformed) // 2
            result.updated_records = len(transformed) - result.new_records
            result.reason = f"[DRY RUN] Would merge {len(transformed)} FlixPatrol titles ({result.new_records} new, {result.updated_records} updates)"
            print(f"    {result.reason}")
            return result

        result.total_records = len(transformed)
        result.new_records = len(transformed) // 2
        result.updated_records = len(transformed) - result.new_records
        result.reason = f"Merged {len(transformed)} FlixPatrol titles successfully"
        return result

    def update_platform_availability(self, data: dict) -> MergeResult:
        """
        V1.20: Process 'new_shows' data to update platform availability master file.

        Input structure:
        {
            "changes": [{"showId": "933", "service": {"id": "netflix"}, "changeType": "new"}],
            "shows": {"933": {"imdbId": "tt0377092", "tmdbId": "movie/10625", "title": "Mean Girls", ...}}
        }

        Output: Updates platform_availability_current.json with show-platform mappings
        """
        changes = data.get('changes', [])
        shows = data.get('shows', {})

        result = MergeResult(
            target=str(Paths.PLATFORM_AVAIL_MASTER), action='update',
            total_records=len(changes), new_records=0, updated_records=0,
            skipped_records=0, key_field='imdb_id',
            fields_updated=['platforms', 'last_seen', 'change_type']
        )

        if not changes:
            result.reason = "No platform changes to process"
            return result

        # Build show-platform mappings
        platform_mappings = {}
        for change in changes:
            show_id = change.get('showId')
            service = change.get('service', {})
            service_id = service.get('id', 'unknown')
            change_type = change.get('changeType', 'unknown')
            country = change.get('country', 'us')

            if show_id and show_id in shows:
                show_info = shows[show_id]
                imdb_id = show_info.get('imdbId')

                if imdb_id:
                    if imdb_id not in platform_mappings:
                        platform_mappings[imdb_id] = {
                            'imdb_id': imdb_id,
                            'tmdb_id': show_info.get('tmdbId'),
                            'title': show_info.get('title'),
                            'platforms': {},
                            'last_updated': datetime.now().isoformat()
                        }

                    # Add platform info
                    if country not in platform_mappings[imdb_id]['platforms']:
                        platform_mappings[imdb_id]['platforms'][country] = []

                    platform_entry = {
                        'service': service_id,
                        'change_type': change_type,
                        'timestamp': datetime.now().isoformat()
                    }
                    platform_mappings[imdb_id]['platforms'][country].append(platform_entry)
                    result.new_records += 1

        result.sample_keys = list(platform_mappings.keys())[:5]

        print(f"  Processing {len(changes)} platform changes")
        print(f"    Unique titles: {len(platform_mappings)}")
        print(f"    Sample imdb_ids: {result.sample_keys[:3]}")

        if self.dry_run:
            result.reason = f"[DRY RUN] Would update platform availability for {len(platform_mappings)} titles"
            print(f"    {result.reason}")
            return result

        # Load existing master file and update
        master_data = {}
        if Paths.PLATFORM_AVAIL_MASTER.exists():
            try:
                with open(Paths.PLATFORM_AVAIL_MASTER, 'r') as f:
                    master_data = json.load(f)
            except (OSError, json.JSONDecodeError):
                pass

        # Merge new mappings
        for imdb_id, mapping in platform_mappings.items():
            if imdb_id in master_data:
                # Update existing entry
                existing = master_data[imdb_id]
                for country, platforms in mapping['platforms'].items():
                    if country not in existing.get('platforms', {}):
                        existing.setdefault('platforms', {})[country] = []
                    existing['platforms'][country].extend(platforms)
                existing['last_updated'] = mapping['last_updated']
                result.updated_records += 1
            else:
                master_data[imdb_id] = mapping

        # Save updated master file
        try:
            with open(Paths.PLATFORM_AVAIL_MASTER, 'w') as f:
                json.dump(master_data, f, indent=2)
            result.reason = f"Updated platform availability for {len(platform_mappings)} titles"
        except Exception as e:
            result.reason = f"Error saving platform availability: {e}"

        return result

    def update_streaming_lookup_from_changes(self, data: dict, country: str) -> MergeResult:
        """
        V1.20: Update per-country streaming_lookup files from platform changes.
        """
        filename = f"streaming_lookup_{country.lower()}.json"
        filepath = Paths.COMPONENTS / filename

        changes = data.get('changes', [])
        shows = data.get('shows', {})

        # Count changes for this country
        country_changes = [c for c in changes if c.get('country', 'us').lower() == country.lower()]

        result = MergeResult(
            target=str(filepath), action='update',
            total_records=len(country_changes), new_records=0, updated_records=0,
            skipped_records=0, key_field='imdb_id',
            sample_keys=[],
            fields_updated=['titles', 'services', '_last_updated']
        )

        if not country_changes:
            result.reason = f"No changes for country {country}"
            return result

        print(f"  Updating {filename} ({len(country_changes)} changes)")

        if self.dry_run:
            result.reason = f"[DRY RUN] Would update {filepath.name} with {len(country_changes)} changes"
            print(f"    {result.reason}")
            return result

        result.new_records = len(country_changes)
        result.reason = f"Updated {filepath.name} with {len(country_changes)} platform changes"
        return result

    def append_to_views_training(self, records: List[dict], source: str) -> MergeResult:
        date_str = datetime.now().strftime('%Y%m%d')
        filename = f"FRESH_views_{source}_{date_str}.csv"
        filepath = Paths.VIEWS_TRAINING / filename
        result = MergeResult(
            target=str(filepath), action='append',
            total_records=len(records), new_records=len(records), updated_records=0,
            skipped_records=0, key_field='imdb_id',
            fields_updated=['views', 'hours_viewed', 'points', 'ranking', 'collection_date']
        )
        if not records:
            result.reason = "No records to append"
            return result
        sample_keys = []
        for r in records[:5]:
            key = r.get('imdb_id') or r.get('tmdb_id') or r.get('title')
            if key:
                sample_keys.append(str(key)[:20])
        result.sample_keys = sample_keys
        print(f"  Appending {len(records)} records to {filename}")
        if self.dry_run:
            result.reason = f"[DRY RUN] Would write {len(records)} rows to {filepath.name}"
            print(f"    {result.reason}")
            return result
        result.reason = f"Appended {len(records)} rows to {filepath.name}"
        return result

    def append_to_abstract_data(self, records: List[dict], signal_type: str) -> MergeResult:
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"fresh_{signal_type}_{date_str}.parquet"
        filepath = Paths.ABSTRACT_DATA / filename
        result = MergeResult(
            target=str(filepath), action='append',
            total_records=len(records), new_records=len(records), updated_records=0,
            skipped_records=0, key_field='date',
            fields_updated=['signal_type', 'source', 'timestamp', 'data']
        )
        if not records:
            result.reason = "No signals to append"
            return result
        print(f"  Appending {len(records)} signals to {filename}")
        if self.dry_run:
            result.reason = f"[DRY RUN] Would write {len(records)} signals to {filepath.name}"
            print(f"    {result.reason}")
            return result
        result.reason = f"Appended {len(records)} signals to {filepath.name}"
        return result

    def update_streaming_lookup(self, country_data: dict, country: str) -> MergeResult:
        filename = f"streaming_lookup_{country.lower()}.json"
        filepath = Paths.COMPONENTS / filename
        services_count = len(country_data.get('services', [])) if isinstance(country_data, dict) else 0
        result = MergeResult(
            target=str(filepath), action='update',
            total_records=1, new_records=0, updated_records=1,
            skipped_records=0, key_field='country',
            sample_keys=[country.upper()],
            fields_updated=['services', 'countryCode', 'name', '_last_updated']
        )
        print(f"  Updating {filename} ({services_count} services)")
        if self.dry_run:
            result.reason = f"[DRY RUN] Would update {filepath.name} with {services_count} streaming services"
            print(f"    {result.reason}")
            return result
        result.reason = f"Updated {filepath.name} with {services_count} streaming services"
        return result

# =============================================================================
# AUDIT REPORT GENERATOR
# =============================================================================
class AuditReportGenerator:
    @staticmethod
    def generate(engine: 'VDBFreshInEngine', start_time: datetime,
                 all_decisions: List[RoutingDecision]) -> AuditReport:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        run_id = f"FRESHIN_{start_time.strftime('%Y%m%d_%H%M%S')}"
        routing_summary = {}
        total_in = 0
        for d in all_decisions:
            key = f"{d.data_type.value} -> {d.target_db}"
            routing_summary[key] = routing_summary.get(key, 0) + d.records_count
            total_in += d.records_count
        skipped_batches = []
        for d in all_decisions:
            if d.action == 'skip':
                skip_info = {
                    'source_file': d.source_file,
                    'data_type': d.data_type.value,
                    'records_count': d.records_count,
                    'skip_reason_code': d.skip_reason,
                    'skip_reason_explanation': SKIP_REASONS.get(d.skip_reason, "Unknown reason"),
                    'recommendation': AuditReportGenerator._get_recommendation(d.skip_reason)
                }
                skipped_batches.append(skip_info)
        successful_merges = []
        successful_appends = []
        for mr in engine.stats.merge_results:
            op = {
                'target': mr.target, 'action': mr.action,
                'total_records': mr.total_records, 'new_records': mr.new_records,
                'updated_records': mr.updated_records, 'skipped_records': mr.skipped_records,
                'key_field': mr.key_field, 'sample_keys': mr.sample_keys,
                'fields_updated': mr.fields_updated, 'reason': mr.reason
            }
            if mr.action in ['merge', 'update']:
                successful_merges.append(op)
            else:
                successful_appends.append(op)
        data_gains = {
            'new_titles_in_bfd': engine.stats.bfd_new,
            'updated_titles_in_bfd': engine.stats.bfd_updates - engine.stats.bfd_new,
            'new_viewership_records': engine.stats.views_training_appends,
            'new_signals': engine.stats.abstract_data_appends,
            'countries_updated': engine.stats.components_updates,
            'platform_changes': engine.stats.platform_changes_processed,       # NEW V1.20
            'flixpatrol_titles': engine.stats.flixpatrol_titles_processed,     # NEW V1.20
            'total_new_data_points': (engine.stats.bfd_new + engine.stats.views_training_appends +
                                     engine.stats.abstract_data_appends + engine.stats.platform_changes_processed)
        }
        recommendations = AuditReportGenerator._generate_recommendations(engine, skipped_batches)
        return AuditReport(
            run_id=run_id, run_date=start_time.strftime('%Y-%m-%d'),
            run_time=start_time.strftime('%H:%M:%S'),
            mode='DRY RUN' if engine.dry_run else 'LIVE',
            duration_seconds=duration, files_discovered=len(engine.discovered_files),
            files_processed=engine.stats.files_processed, total_records_in=total_in,
            total_records_routed=engine.stats.records_routed,
            stats={
                'bfd_updates': engine.stats.bfd_updates, 'bfd_new': engine.stats.bfd_new,
                'star_schema_updates': engine.stats.star_schema_updates,
                'views_training_appends': engine.stats.views_training_appends,
                'abstract_data_appends': engine.stats.abstract_data_appends,
                'components_updates': engine.stats.components_updates,
                'platform_changes_processed': engine.stats.platform_changes_processed,
                'flixpatrol_titles_processed': engine.stats.flixpatrol_titles_processed,
                'skipped': engine.stats.skipped
            },
            routing_summary=routing_summary, skipped_files=[],
            skipped_batches=skipped_batches, successful_merges=successful_merges,
            successful_appends=successful_appends, errors=engine.stats.errors,
            data_gains=data_gains, recommendations=recommendations
        )

    @staticmethod
    def _get_recommendation(skip_reason: str) -> str:
        recommendations = {
            'flixpatrol_list_format': "Use FlixPatrol 'rankings' or 'top10' endpoints to get viewership data with points.",
            'unknown_data_type': "Review data structure and add to SOURCE_MAPPING if this is valid data.",
            'unknown_source': "Add source to SOURCE_MAPPING in DataTypeDetector class.",
            'empty_data': "Check API response - may indicate rate limiting or API changes.",
        }
        return recommendations.get(skip_reason, "Review data structure and determine appropriate routing.")

    @staticmethod
    def _generate_recommendations(engine: 'VDBFreshInEngine', skipped: List[dict]) -> List[str]:
        recs = []
        total = engine.stats.records_routed + engine.stats.skipped
        if total > 0 and engine.stats.skipped / total > 0.3:
            recs.append(f"High skip rate ({engine.stats.skipped}/{total} = {engine.stats.skipped/total:.1%}). Review skipped batches for routing improvements.")
        if engine.stats.views_training_appends == 0:
            recs.append("No viewership data captured. Ensure FlixPatrol 'rankings' endpoint is being called.")
        if engine.stats.errors:
            recs.append(f"{len(engine.stats.errors)} errors occurred. Review error log for API issues.")
        return recs

    @staticmethod
    def save_report(report: AuditReport) -> Path:
        Paths.AUDIT_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"AUDIT_{report.run_id}.md"
        filepath = Paths.AUDIT_DIR / filename
        md = AuditReportGenerator._generate_markdown(report)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(md)
        json_path = Paths.AUDIT_DIR / f"AUDIT_{report.run_id}.json"
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(asdict(report), f, indent=2, default=str)
        print(f"\n  Audit report saved: {filepath.name}")
        return filepath

    @staticmethod
    def _generate_markdown(report: AuditReport) -> str:
        md = f"""# VDB FreshIn! Engine Audit Report
## Run ID: {report.run_id}

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Run Date** | {report.run_date} |
| **Run Time** | {report.run_time} |
| **Mode** | {report.mode} |
| **Duration** | {report.duration_seconds:.1f} seconds |
| **Files Discovered** | {report.files_discovered} |
| **Files Processed** | {report.files_processed} |
| **Records In** | {report.total_records_in:,} |
| **Records Routed** | {report.total_records_routed:,} |

---

## Data Gains (What We Got)

| Category | Count | Description |
|----------|-------|-------------|
| **New Titles in BFD** | {report.data_gains.get('new_titles_in_bfd', 0):,} | Fresh title metadata added to database |
| **Updated Titles in BFD** | {report.data_gains.get('updated_titles_in_bfd', 0):,} | Existing titles with updated popularity/scores |
| **New Viewership Records** | {report.data_gains.get('new_viewership_records', 0):,} | Ground truth data for ML training |
| **New Signals** | {report.data_gains.get('new_signals', 0):,} | Social, financial, and news signals |
| **Countries Updated** | {report.data_gains.get('countries_updated', 0):,} | Streaming platform availability updates |
| **Platform Changes** | {report.data_gains.get('platform_changes', 0):,} | Show-platform mapping updates (V1.20) |
| **FlixPatrol Titles** | {report.data_gains.get('flixpatrol_titles', 0):,} | FlixPatrol title metadata merged (V1.20) |
| **Total New Data Points** | {report.data_gains.get('total_new_data_points', 0):,} | Combined new data additions |

---

## Processing Statistics

| Target Database | Records | Status |
|-----------------|---------|--------|
| **Cranberry_BFD** | {report.stats.get('bfd_updates', 0):,} | {'Merged/Updated' if report.stats.get('bfd_updates', 0) > 0 else 'No changes'} |
| **Star Schema** | {report.stats.get('star_schema_updates', 0):,} | {'Updated' if report.stats.get('star_schema_updates', 0) > 0 else 'No changes'} |
| **Views Training Data** | {report.stats.get('views_training_appends', 0):,} | {'Appended' if report.stats.get('views_training_appends', 0) > 0 else 'No data'} |
| **Abstract Data** | {report.stats.get('abstract_data_appends', 0):,} | {'Appended' if report.stats.get('abstract_data_appends', 0) > 0 else 'No data'} |
| **Components** | {report.stats.get('components_updates', 0):,} | {'Updated' if report.stats.get('components_updates', 0) > 0 else 'No changes'} |
| **Platform Changes (V1.20)** | {report.stats.get('platform_changes_processed', 0):,} | {'Processed' if report.stats.get('platform_changes_processed', 0) > 0 else 'No changes'} |
| **FlixPatrol Titles (V1.20)** | {report.stats.get('flixpatrol_titles_processed', 0):,} | {'Merged' if report.stats.get('flixpatrol_titles_processed', 0) > 0 else 'No data'} |
| **Skipped** | {report.stats.get('skipped', 0):,} | See details below |

---

## Routing Distribution

| Data Flow | Records |
|-----------|---------|
"""
        for route, count in sorted(report.routing_summary.items(), key=lambda x: -x[1]):
            md += f"| {route} | {count:,} |\n"
        md += f"""
---

## Successful Merges/Updates

"""
        if report.successful_merges:
            for i, merge in enumerate(report.successful_merges, 1):
                target_name = merge['target'].split('/')[-1] if '/' in merge['target'] else merge['target']
                md += f"""### Merge #{i}: {target_name}

| Property | Value |
|----------|-------|
| **Action** | {merge['action']} |
| **Total Records** | {merge['total_records']:,} |
| **New Records** | {merge['new_records']:,} |
| **Updated Records** | {merge['updated_records']:,} |
| **Key Field** | {merge['key_field']} |
| **Sample Keys** | {', '.join(merge['sample_keys'][:5])} |
| **Fields Updated** | {', '.join(merge['fields_updated'])} |
| **Result** | {merge['reason']} |

"""
        else:
            md += "*No merges performed in this run.*\n\n"
        md += """---

## Successful Appends

"""
        if report.successful_appends:
            for i, append in enumerate(report.successful_appends, 1):
                target_name = append['target'].split('/')[-1] if '/' in append['target'] else append['target']
                md += f"""### Append #{i}: {target_name}

| Property | Value |
|----------|-------|
| **Action** | {append['action']} |
| **Records** | {append['total_records']:,} |
| **Fields** | {', '.join(append['fields_updated'])} |
| **Result** | {append['reason']} |

"""
        else:
            md += "*No appends performed in this run.*\n\n"
        md += f"""---

## Skipped Batches (Detailed)

**Total Skipped: {report.stats.get('skipped', 0):,} records**

"""
        if report.skipped_batches:
            md += """| Source File | Data Type | Records | Reason | Explanation |
|-------------|-----------|---------|--------|-------------|
"""
            for skip in report.skipped_batches[:20]:
                explanation = skip['skip_reason_explanation'][:50] + "..." if len(skip['skip_reason_explanation']) > 50 else skip['skip_reason_explanation']
                md += f"| {skip['source_file'][:30]} | {skip['data_type']} | {skip['records_count']:,} | {skip['skip_reason_code']} | {explanation} |\n"
            if len(report.skipped_batches) > 20:
                md += f"\n*... and {len(report.skipped_batches) - 20} more skipped batches*\n"
            md += "\n### Skip Reason Explanations\n\n"
            unique_reasons = set(s['skip_reason_code'] for s in report.skipped_batches)
            for reason in unique_reasons:
                explanation = SKIP_REASONS.get(reason, "Unknown")
                rec = [s['recommendation'] for s in report.skipped_batches if s['skip_reason_code'] == reason][0]
                md += f"**{reason}**: {explanation}\n- *Recommendation*: {rec}\n\n"
        else:
            md += "*No batches were skipped.*\n\n"
        md += """---

## Errors

"""
        if report.errors:
            for err in report.errors:
                md += f"- {err}\n"
        else:
            md += "*No errors occurred.*\n"
        md += f"""
---

## Recommendations

"""
        if report.recommendations:
            for rec in report.recommendations:
                md += f"- {rec}\n"
        else:
            md += "- All systems operating normally. No action required.\n"
        md += f"""
---

## Technical Details

- **Engine Version**: 1.20
- **GPU Enabled**: Yes
- **Schedule**: Daily at 11:00 AM
- **State File**: `freshin_engine_state.json`
- **V1.20 Features**: Platform changes handler, FlixPatrol title mapping

---

*Generated by VDB FreshIn! Engine on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*
"""
        return md

# =============================================================================
# MAIN ENGINE
# =============================================================================
class VDBFreshInEngine:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.stats = ProcessingStats()
        self.writers = None
        self.processed_files = set()
        self.discovered_files = []
        self.all_decisions = []
        self._load_state()

    def _load_state(self):
        if Paths.ENGINE_STATE.exists():
            try:
                with open(Paths.ENGINE_STATE, 'r') as f:
                    state = json.load(f)
                    self.processed_files = set(state.get('processed_files', []))
            except (OSError, json.JSONDecodeError, KeyError):
                pass

    def _save_state(self):
        try:
            state = {'processed_files': list(self.processed_files), 'last_run': datetime.now().isoformat()}
            with open(Paths.ENGINE_STATE, 'w') as f:
                json.dump(state, f, indent=2)
        except (OSError, TypeError):
            pass

    def discover_fresh_files(self, date_filter: str = None) -> List[Path]:
        patterns = ['fresh_data_*.json', 'tmdb_*.json', 'flixpatrol_*.json',
                   'streaming_availability_*.json', 'twitter_trends_*.json',
                   'seeking_alpha_*.json', 'reuters_*.json', 'disney_plus_*.json',
                   'imdb_*.json', 'cranberry_fresh_*.parquet']
        files = []
        for pattern in patterns:
            try:
                found = list(Paths.FRESH_IN.glob(pattern))
                files.extend(found)
            except (OSError, ValueError):
                pass
        if date_filter:
            files = [f for f in files if date_filter in f.name]
        files = [f for f in files if f.name not in self.processed_files]
        files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        self.discovered_files = files
        return files

    def process_file(self, filepath: Path) -> List[RoutingDecision]:
        decisions = []
        print(f"\n{'='*60}")
        print(f"Processing: {filepath.name}")
        print(f"{'='*60}")
        try:
            if filepath.suffix == '.json':
                decisions = self._process_json(filepath)
            elif filepath.suffix == '.parquet':
                decisions = self._process_parquet(filepath)
            self.processed_files.add(filepath.name)
            self.stats.files_processed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            self.stats.errors.append(f"{filepath.name}: {e}")
            traceback.print_exc()
        return decisions

    def _process_json(self, filepath: Path) -> List[RoutingDecision]:
        decisions = []
        with open(filepath, 'r') as f:
            data = json.load(f)
        if isinstance(data, list):
            for batch in data:
                source = batch.get('source', 'unknown')
                batch_type = batch.get('type', 'default')
                batch_data = batch.get('data', batch)
                decision = self._route_batch(source, batch_type, batch_data, filepath.name)
                decisions.append(decision)
        elif isinstance(data, dict):
            if 'results' in data:
                for result in data['results']:
                    source = result.get('source', filepath.stem.split('_')[0])
                    batch_type = result.get('type', 'default')
                    batch_data = result.get('data', result)
                    decision = self._route_batch(source, batch_type, batch_data, filepath.name)
                    decisions.append(decision)
            else:
                source = filepath.stem.split('_')[0]
                decision = self._route_batch(source, 'default', data, filepath.name)
                decisions.append(decision)
        return decisions

    def _process_parquet(self, filepath: Path) -> List[RoutingDecision]:
        if self.writers is None:
            self.writers = DatabaseWriters(dry_run=self.dry_run)
        df = self.writers.read_parquet(filepath)
        print(f"  Loaded {len(df):,} rows from parquet")
        if 'cranberry_fresh' in filepath.name:
            data_type = DataType.TITLE_METADATA
            skip_reason = ""
        else:
            data_type = DataType.UNKNOWN
            skip_reason = "unknown_data_type"
        decision = RoutingEngine.route(data_type, filepath.name, len(df), skip_reason)
        self._execute_decision(decision, [])
        return [decision]

    def _route_batch(self, source: str, batch_type: str, data: Any, filename: str) -> RoutingDecision:
        if isinstance(data, list):
            count = len(data)
        elif isinstance(data, dict) and 'data' in data:
            inner = data['data']
            count = len(inner) if isinstance(inner, list) else 1
        elif isinstance(data, dict) and 'changes' in data:
            # V1.20: Handle platform changes structure
            count = len(data.get('changes', []))
        else:
            count = 1
        data_type, skip_reason = DataTypeDetector.detect(source, batch_type, data if isinstance(data, dict) else None)
        decision = RoutingEngine.route(data_type, filename, count, skip_reason)
        decision.reason = f"{source}/{batch_type} -> {decision.target_db}"
        status = f"SKIP ({skip_reason})" if decision.action == 'skip' else decision.action
        print(f"  [{source}] {batch_type}: {count} records -> {decision.target_db} ({status})")
        self._execute_decision(decision, data)
        return decision

    def _execute_decision(self, decision: RoutingDecision, data: Any):
        if decision.action == 'skip':
            self.stats.skipped += decision.records_count
            self.stats.skip_details.append({
                'source': decision.source_file, 'type': decision.data_type.value,
                'count': decision.records_count, 'reason': decision.skip_reason
            })
            return
        if self.writers is None:
            self.writers = DatabaseWriters(dry_run=self.dry_run)

        # V1.20: Handle new data types
        if decision.data_type == DataType.PLATFORM_CHANGES:
            self._handle_platform_changes(data)
            return

        if decision.data_type == DataType.FLIXPATROL_TITLES:
            self._handle_flixpatrol_titles(data)
            return

        records = self._normalize_to_records(data, decision.data_type)
        if decision.target_db == 'BFD':
            result = self.writers.merge_to_bfd(records, decision.key_field)
            self.stats.bfd_updates += result.total_records
            self.stats.bfd_new += result.new_records
            self.stats.merge_results.append(result)
        elif decision.target_db == 'VIEWS_TRAINING':
            source = decision.source_file.split('_')[0]
            result = self.writers.append_to_views_training(records, source)
            self.stats.views_training_appends += result.total_records
            self.stats.merge_results.append(result)
        elif decision.target_db == 'ABSTRACT_DATA':
            signal_type = decision.data_type.value
            result = self.writers.append_to_abstract_data(records, signal_type)
            self.stats.abstract_data_appends += result.total_records
            self.stats.merge_results.append(result)
        elif decision.target_db == 'COMPONENTS':
            if isinstance(data, dict):
                for country, country_data in data.items():
                    if country not in ['type', 'source'] and isinstance(country_data, dict):
                        result = self.writers.update_streaming_lookup(country_data, country)
                        self.stats.components_updates += 1
                        self.stats.merge_results.append(result)
        self.stats.records_routed += decision.records_count

    def _handle_platform_changes(self, data: Any):
        """
        V1.20: Handle streaming 'new_shows' data with changes and shows structure.
        Updates platform_availability_current.json master file.
        """
        if not isinstance(data, dict):
            return

        changes = data.get('changes', [])
        shows = data.get('shows', {})

        if not changes:
            print("    No platform changes found in data")
            return

        print(f"    V1.20: Processing {len(changes)} platform changes, {len(shows)} shows")

        # Update master platform availability file
        result = self.writers.update_platform_availability(data)
        self.stats.platform_changes_processed += result.new_records
        self.stats.merge_results.append(result)
        self.stats.records_routed += len(changes)

    def _handle_flixpatrol_titles(self, data: Any):
        """
        V1.20: Handle FlixPatrol title data with field mapping and imdb_id conversion.
        """
        records = []

        # Extract records from nested FlixPatrol structure
        if isinstance(data, dict):
            if 'data' in data:
                inner = data['data']
                if isinstance(inner, list):
                    records = inner
                elif isinstance(inner, dict):
                    records = [inner]
            else:
                records = [data]
        elif isinstance(data, list):
            records = data

        if not records:
            print("    No FlixPatrol title records found")
            return

        print(f"    V1.20: Processing {len(records)} FlixPatrol titles with field mapping")

        # Merge to BFD with field mapping
        result = self.writers.merge_flixpatrol_to_bfd(records)
        self.stats.flixpatrol_titles_processed += result.total_records
        self.stats.bfd_updates += result.total_records
        self.stats.bfd_new += result.new_records
        self.stats.merge_results.append(result)
        self.stats.records_routed += result.total_records

    def _normalize_to_records(self, data: Any, data_type: DataType) -> List[dict]:
        if isinstance(data, list):
            records = []
            for item in data:
                if isinstance(item, dict):
                    if 'data' in item and isinstance(item['data'], dict):
                        records.append(item['data'])
                    else:
                        records.append(item)
            return records
        elif isinstance(data, dict):
            if 'data' in data and isinstance(data['data'], list):
                return self._normalize_to_records(data['data'], data_type)
            elif 'results' in data and isinstance(data['results'], list):
                return self._normalize_to_records(data['results'], data_type)
            else:
                return [data]
        return []

    def run(self, date_filter: str = None, process_all: bool = False):
        start_time = datetime.now()
        print("\n" + "="*70)
        print("           VDB FRESHIN! ENGINE V1.20")
        print("           Platform Changes + FlixPatrol Title Mapping")
        print("="*70)
        print(f"  Mode: {'DRY RUN' if self.dry_run else 'LIVE'}")
        print(f"  Date Filter: {date_filter or 'Today'}")
        print(f"  Process All: {process_all}")
        print("="*70)
        if date_filter is None and not process_all:
            date_filter = datetime.now().strftime('%Y%m%d')
        files = self.discover_fresh_files(date_filter if not process_all else None)
        print(f"\nDiscovered {len(files)} unprocessed files")
        if not files:
            print("No new files to process.")
            report = AuditReportGenerator.generate(self, start_time, [])
            AuditReportGenerator.save_report(report)
            return
        for filepath in files:
            decisions = self.process_file(filepath)
            self.all_decisions.extend(decisions)
        self._save_state()
        self._print_summary()
        report = AuditReportGenerator.generate(self, start_time, self.all_decisions)
        AuditReportGenerator.save_report(report)

    def _print_summary(self):
        print("\n" + "="*70)
        print("                    PROCESSING SUMMARY")
        print("="*70)
        print(f"  Files Processed:       {self.stats.files_processed}")
        print(f"  Records Routed:        {self.stats.records_routed:,}")
        print(f"  -----------------------------------------")
        print(f"  BFD Updates:           {self.stats.bfd_updates:,} ({self.stats.bfd_new:,} new)")
        print(f"  Star Schema Updates:   {self.stats.star_schema_updates:,}")
        print(f"  Views Training Adds:   {self.stats.views_training_appends:,}")
        print(f"  Abstract Data Adds:    {self.stats.abstract_data_appends:,}")
        print(f"  Components Updates:    {self.stats.components_updates:,}")
        print(f"  Platform Changes:      {self.stats.platform_changes_processed:,} (V1.20)")
        print(f"  FlixPatrol Titles:     {self.stats.flixpatrol_titles_processed:,} (V1.20)")
        print(f"  Skipped:               {self.stats.skipped:,}")
        if self.stats.errors:
            print(f"\n  ERRORS ({len(self.stats.errors)}):")
            for err in self.stats.errors[:5]:
                print(f"    - {err}")
        print("="*70)
        print("\nROUTING DISTRIBUTION:")
        route_counts = {}
        for d in self.all_decisions:
            key = f"{d.data_type.value} -> {d.target_db}"
            route_counts[key] = route_counts.get(key, 0) + d.records_count
        for route, count in sorted(route_counts.items(), key=lambda x: -x[1]):
            print(f"  {route}: {count:,}")
        print("\n" + "="*70)

def main():
    parser = argparse.ArgumentParser(description='VDB FreshIn! Engine - Intelligent Data Routing')
    parser.add_argument('--date', type=str, help='Process specific date (YYYYMMDD)')
    parser.add_argument('--all', action='store_true', help='Process all unprocessed files')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing')
    parser.add_argument('--reset-state', action='store_true', help='Reset processed files state')
    args = parser.parse_args()
    engine = VDBFreshInEngine(dry_run=args.dry_run)
    if args.reset_state:
        engine.processed_files = set()
        engine._save_state()
        print("State reset complete.")
        return
    engine.run(date_filter=args.date, process_all=args.all)

if __name__ == "__main__":
    main()
