# FreshIn Metadata Merger V27.72

Daily automated metadata merger for BFD_META database with email notifications.

## Files Created

| File | Purpose |
|------|---------|
| `freshin_meta_merger_v27.72.py` | Main merger script - processes FreshIn data |
| `FreshIn_Daily_9AM.xml` | Windows Task Scheduler configuration |
| `setup_daily_merge.bat` | One-click setup for scheduled task (run as admin) |
| `run_merge_now.bat` | Run merge immediately |

## Quick Start

### 1. Run Initial Merge (Already Done)

The initial merge has been completed:
- **408 records updated**
- **800 values merged**
- **59 new titles found** (not in database yet)

### 2. Set Up Daily Schedule

**Option A: Use Setup Script (Recommended)**
1. Right-click `setup_daily_merge.bat`
2. Select "Run as administrator"
3. The task will be created automatically

**Option B: Manual Setup**
1. Open Task Scheduler (`taskschd.msc`)
2. Click "Import Task..."
3. Select `FreshIn_Daily_9AM.xml`
4. Adjust credentials if needed

### 3. Configure Email Notifications

The merger sends completion emails to:
- warren@framecore.ai
- roy@framecore.ai

**Set Environment Variables:**

```powershell
# PowerShell (Run as Administrator)
[Environment]::SetEnvironmentVariable("FRESHIN_SMTP_SERVER", "smtp.gmail.com", "Machine")
[Environment]::SetEnvironmentVariable("FRESHIN_SMTP_PORT", "587", "Machine")
[Environment]::SetEnvironmentVariable("FRESHIN_SENDER_EMAIL", "your-email@gmail.com", "Machine")
[Environment]::SetEnvironmentVariable("FRESHIN_SENDER_PASSWORD", "your-app-password", "Machine")
```

**For Gmail:** Use an [App Password](https://support.google.com/accounts/answer/185833), not your regular password.

## What Gets Merged

The merger processes data from FreshIn sources in priority order:

| Priority | Source | Columns Updated |
|----------|--------|-----------------|
| 1 | TMDB | tmdb_popularity, tmdb_score, premiere_date, original_language, overview |
| 2 | Disney+ | imdb_score, imdb_vote_count, tmdb_score, genres, age_certification |
| 3 | FlixPatrol | flixpatrol_id, flixpatrol_points, flixpatrol_rank, budget, tagline |

### Merge Rules

- **MERGE mode**: Only fills NULL/empty values (preserves existing data)
- **OVERWRITE mode**: Updates FlixPatrol-specific columns and TMDB popularity/scores
- **NO NEW COLUMNS**: Schema V27.66 compliant - never creates unauthorized columns

## Column Mapping

The merger uses these schema files for column translation:
- `Schema/Translation Mapping Guide.json` - Flat alias-to-canonical mapping
- `Schema/Aliase Guide.json` - Grouped aliases by canonical column

Example translations:
- `release_date` → `premiere_date`
- `description` → `overview`
- `imdb_votes` → `imdb_vote_count`

## Output Reports

Reports are saved to: `FreshIn/Audit Reports/`

Each report includes:
- Summary statistics (records/values updated)
- Updates by source
- Updates by column
- Sample of updated titles
- Any errors encountered

## Troubleshooting

### Task Not Running
1. Check Task Scheduler history
2. Verify Python is in PATH
3. Check `merger_log.txt` for errors

### Email Not Sending
1. Verify environment variables are set correctly
2. For Gmail, ensure "Less secure app access" or use App Password
3. Check firewall allows outbound SMTP (port 587)

### Database Not Found
The script auto-detects Windows vs WSL paths. If issues persist, check:
- `C:\Users\RoyT6\Downloads\BFD_META_V27.72.parquet` exists
- Script has read/write permissions

## Log Files

- `merger_log.txt` - Detailed execution log
- `Audit Reports/merge_report_*.json` - Structured merge reports

## Manual Execution

```bash
# Windows
python "C:\Users\RoyT6\Downloads\FreshIn\freshin_meta_merger_v27.72.py"

# WSL
python3 "/mnt/c/Users/RoyT6/Downloads/FreshIn/freshin_meta_merger_v27.72.py"
```

---
Created: 2026-01-27
Schema: V27.66
Database: BFD_META_V27.72
