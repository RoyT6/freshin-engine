# VDB FreshIn! Engine V1.00
## Intelligent Data Routing & Integration System

---

## Overview

The VDB FreshIn! Engine automatically processes incoming daily data from the SCHIG collector and routes it intelligently to the appropriate target databases:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     VDB FRESHIN! ENGINE                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  FRESH IN!/                     ┌─────────────────────────────────┐     │
│  ├── tmdb_*.json          ────▶ │ Cranberry_BFD_V18.02.parquet   │     │
│  ├── flixpatrol_*.json    ────▶ │ (Title Metadata)                │     │
│  │                              └─────────────────────────────────┘     │
│  │                                                                       │
│  ├── flixpatrol_*.json    ────▶ ┌─────────────────────────────────┐     │
│  │   (rankings/points)          │ Views Training Data/            │     │
│  │                              │ (Ground Truth Viewership)        │     │
│  │                              └─────────────────────────────────┘     │
│  │                                                                       │
│  ├── streaming_avail_*.json ──▶ ┌─────────────────────────────────┐     │
│  │                              │ Components/streaming_lookup_*   │     │
│  │                              │ (Platform Availability)          │     │
│  │                              └─────────────────────────────────┘     │
│  │                                                                       │
│  ├── twitter_trends_*.json ───▶ ┌─────────────────────────────────┐     │
│  ├── seeking_alpha_*.json  ───▶ │ Abstract Data/                  │     │
│  └── reuters_*.json        ───▶ │ (Signals, Trends, Financials)   │     │
│                                 └─────────────────────────────────┘     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Run on Today's Data
```bash
./run_gpu.sh vdb_freshin_engine.py
```

### Dry Run (Preview Only)
```bash
./run_gpu.sh vdb_freshin_engine.py --dry-run
```

### Process Specific Date
```bash
./run_gpu.sh vdb_freshin_engine.py --date 20260115
```

### Process All Unprocessed Files
```bash
./run_gpu.sh vdb_freshin_engine.py --all
```

### Reset Processing State
```bash
./run_gpu.sh vdb_freshin_engine.py --reset-state
```

---

## Data Routing Rules

### 1. Title Metadata → BFD

| Source | Data Types | Key Field | Action |
|--------|------------|-----------|--------|
| TMDB | trending_*, popular_*, top_rated_*, upcoming_*, now_playing_*, on_the_air_*, airing_today_* | tmdb_id | MERGE/UPDATE |
| FlixPatrol | titles | imdb_id | MERGE |
| IMDB | default | imdb_id | MERGE |

**Behavior:**
- New titles: Appended to BFD (requires full schema alignment)
- Existing titles: Update specific columns (popularity, scores, vote counts)

### 2. Viewership Data → Views Training Data

| Source | Data Types | Key Field | Action |
|--------|------------|-----------|--------|
| FlixPatrol | rankings, points, top10 | imdb_id | APPEND |
| cranberry_fresh_*.parquet | views | fc_uid | APPEND |

**Output Format:**
```
Views Training Data/FRESH_views_flixpatrol_20260115.csv
Views Training Data/FRESH_views_netflix_20260115.csv
```

### 3. Platform Availability → Components

| Source | Data Types | Key Field | Action |
|--------|------------|-----------|--------|
| Streaming Availability | countries, shows | country | UPDATE |
| Disney Plus | catalog | country | UPDATE |

**Output Format:**
```
Components/streaming_lookup_us.json
Components/streaming_lookup_gb.json
...
```

### 4. Signals → Abstract Data

| Source | Data Types | Signal Type | Action |
|--------|------------|-------------|--------|
| Twitter Trends | places, trends | social_signals | APPEND |
| Seeking Alpha | financial | financial_data | APPEND |
| Reuters | news | news_events | APPEND |

**Output Format:**
```
Abstract Data/fresh_social_signals_20260115_080324.parquet
Abstract Data/fresh_financial_data_20260115_080324.parquet
Abstract Data/fresh_news_events_20260115_080324.parquet
```

---

## Data Type Detection

The engine uses intelligent detection to classify incoming data:

```python
# Detection priority:
1. Source name (tmdb, flixpatrol, etc.)
2. Data type within source (trending_movies_week, rankings, etc.)
3. Content analysis (field names like 'views', 'imdb_id', 'trending')
```

### Detection Keywords

| Data Type | Indicator Fields |
|-----------|------------------|
| TITLE_METADATA | imdb_id, tmdb_id, title, premiere, genres |
| VIEWERSHIP_DATA | views, hours_viewed, points, ranking, watch_time |
| PLATFORM_AVAILABILITY | services, streaming, platform, availability |
| SOCIAL_SIGNALS | trending, trends, hashtags, mentions, sentiment |
| FINANCIAL_DATA | revenue, subscribers, stock, market_cap, earnings |

---

## Field Transformations

### TMDB → BFD

| TMDB Field | BFD Field |
|------------|-----------|
| id | tmdb_id |
| title / name | title |
| original_title / original_name | original_title |
| popularity | tmdb_popularity |
| vote_average | tmdb_score |
| vote_count | tmdb_vote_count |
| release_date | premiere_date |
| genre_ids | genres |
| adult | is_adult |

### FlixPatrol → BFD

| FlixPatrol Field | BFD Field |
|------------------|-----------|
| imdbId | imdb_id (with tt prefix) |
| tmdbId | tmdb_id |
| id | flixpatrol_id |
| title | title |
| premiere | premiere_date |
| length | runtime_minutes |
| description | overview |

---

## Processing State

The engine maintains a state file to track processed files:

```json
// freshin_engine_state.json
{
  "processed_files": [
    "tmdb_20260114_090916.json",
    "flixpatrol_20260114_090916.json",
    ...
  ],
  "last_run": "2026-01-15T08:30:00"
}
```

Files already in `processed_files` are skipped on subsequent runs.

---

## Output Summary

After each run, the engine prints a summary:

```
══════════════════════════════════════════════════════════════════════
                    PROCESSING SUMMARY
══════════════════════════════════════════════════════════════════════
  Files Processed:       12
  Records Routed:        1,847
  ─────────────────────────────────────────
  BFD Updates:           523
  Star Schema Updates:   0
  Views Training Adds:   812
  Abstract Data Adds:    512
  Components Updates:    18
  Skipped:               0
══════════════════════════════════════════════════════════════════════

ROUTING DISTRIBUTION:
  title_metadata -> BFD: 523
  viewership_data -> VIEWS_TRAINING: 812
  social_signals -> ABSTRACT_DATA: 312
  financial_data -> ABSTRACT_DATA: 100
  news_events -> ABSTRACT_DATA: 100
  platform_avail -> COMPONENTS: 18
══════════════════════════════════════════════════════════════════════
```

---

## Integration with ALGO-80

After processing fresh data, run the ALGO-80 pipeline:

```bash
# 1. Process fresh data
./run_gpu.sh vdb_freshin_engine.py

# 2. Run ALGO-80 ML pipeline (if views data was added)
./run_gpu.sh algo80_pipeline.py
```

**Note:** The engine prepares data for ALGO-80 Phase 2 (Data Loading). Fresh viewership data in `Views Training Data/` will be incorporated into the next ML training run.

---

## Configuration

See `VDB_FRESHIN_CONFIG.json` for:
- Target paths and schemas
- Source routing rules
- Field mappings
- Validation rules
- Scheduling recommendations

---

## Troubleshooting

### "No new files to process"
- Files may already be in processed state
- Run with `--reset-state` to clear processed files list

### GPU not available
- Engine falls back to pandas (slower but functional)
- Ensure RAPIDS environment is activated

### Key field not found
- Check incoming data structure matches expected schema
- Review field mappings in config

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.00 | 2026-01-15 | Initial release |

---

**Created for FrameCore ViewerDBX System**
**Integrates with SCHIG Daily Collector and ALGO-80 Pipeline**
