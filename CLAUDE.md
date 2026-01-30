# FreshIn Engine - ALGO 95.66 Compliance
## VERSION 27.66 | ALGO 95.66 | ViewerDBX Bus v1.0 | SCHEMA V27.00

---

## P0 CANON: Studio vs Production Company Classification (2026-01-25)

**CRITICAL RULE:** Classification is by PRIMARY FUNCTION, NOT by fixed lookup list.

### Studios (Creative/On-Set)
| Roles | Activities |
|-------|------------|
| Directors, Writers | Filming, Blocking |
| Cinematographers, Cameramen | Location, Direction |
| Set Designers, Crew | Content Creation, Capture |
| Actors, Casting Directors | Casting, Extras |

### Production Companies (Business/Post)
| Roles | Activities |
|-------|------------|
| Lawyers, Finance | Financing, Legal |
| Post-Production, Final Cut | Editing, Mixing |
| Producers, Editors | Distribution, Marketing |
| Music, Sound, Executives | Promotion, Post-Production |

**Ingestion Rules:**
- P0: Classify by FUNCTION, not name matching
- P1: Accept ANY entity meeting criteria (no lookup required)
- P2: Metadata columns OPTIONAL (NULL acceptable)

**Rule File:** `Schema/RULES/STUDIO_VS_PRODUCTION_COMPANY_RULES.json`

---

## ROLE: AGENT (Does Work)

Fresh In! contains SCHIG collection output and new data ingestion pipeline.

---

## VIEWERDBX BUS INTEGRATION

```python
from viewerdbx_bus import get_bus

bus = get_bus()
fresh_data = bus.resolve_path("fresh_data")
# Subscribe to SCHIG signals
bus.subscribe("SCHIG", on_fresh_data)
```

### Component Registration
- **Name**: Fresh In!
- **Role**: AGENT
- **Provides**: routing, integration, data_freshness
- **Requires**: fresh_data

---

## MANDATORY EXECUTION CONSTRAINTS

### GPU Enforcement
- GPU execution is **MANDATORY** for data processing
- Use `cudf` not `pandas`

### Data Ingestion
- 45+ files from SCHIG collection
- TMDB file processing
- Metadata updates for existing titles

---

## FOUR RULES ENFORCEMENT

| Rule | Requirement |
|------|-------------|
| V1 | Ingestion work verified |
| V2 | Data checksums |
| V3 | Ingestion logged |
| V4 | Machine-ingested |

---

## ANTI-CHEAT RULES

| FORBIDDEN | WHY |
|-----------|-----|
| `views_*` in features | Data leakage |
| `pd.read_parquet()` | CPU fallback |

---

## RALPH INTEGRATION

Uses Ralph's autonomous development loop with ViewerDBX Bus.

---

**Synced with**: `ALGO Engine/ALGO_95.4_SPEC.md`
**Bus Version**: 1.0

---

## SESSION LOG: 2026-01-22

### Issue Fixed: Scheduled Task Not Running

**Root Cause**: `run_daily_collection.bat` was calling `schig.py` which did not exist. The actual script is `sacred_child_gatherer.py`.

### Changes Made

1. **Directory Renamed**: `Fresh In!` -> `FreshIn`
   - Exclamation mark caused path issues with WSL/GPU execution

2. **Files Modified**:
   - `run_daily_collection.bat`: Fixed script reference and all paths
   - `sacred_child_gatherer.py`: Added WSL path auto-detection for GPU execution
   - Updated Task Scheduler `SCHIG_DailyCollection` to new path

3. **Path Auto-Detection** (in `sacred_child_gatherer.py`):
   ```python
   OUTPUT_DIR: str = "/mnt/c/Users/RoyT6/Downloads/FreshIn" if 'microsoft' in __import__('platform').uname().release.lower() else r"C:\Users\RoyT6\Downloads\FreshIn"
   ```

### Verified Working
- JSON data files created successfully (Jan 22 collection)
- GPU execution via WSL confirmed (RTX 3080 Ti)
- Scheduled task updated and enabled

### Pending Issue
- Parquet conversion fails due to mixed data types in 'id' column
- JSON files save correctly; only parquet/CSV affected
- Fix needed in `create_parquet()` method - cast 'id' column to string

### API Status (as of 2026-01-22)
| API | Status |
|-----|--------|
| TMDB | Working |
| Streaming Availability | Working |
| Disney+ | Working |
| Seeking Alpha | Working |
| FlixPatrol | Working |
| Reuters | Rate Limited |
| IMDB | 400 Bad Request |
| Rotten Tomatoes | 403 Forbidden |
| HBO Max | Rate Limited |
| Twitter Trends | 404 Not Found |
| Sports Highlights | 404 Not Found |
