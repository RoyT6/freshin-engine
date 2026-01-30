#!/usr/bin/env python3
"""
SCHIG Monitor - Check status of scheduled collection runs
"""

import subprocess
import os
from datetime import datetime
from pathlib import Path

OUTPUT_DIR = Path(r"C:\Users\RoyT6\Downloads\Fresh In!")
LOG_FILE = OUTPUT_DIR / "collection_log.txt"

def get_task_status():
    """Get Windows Task Scheduler status for SCHIG"""
    try:
        result = subprocess.run(
            ['powershell.exe', '-Command',
             "Get-ScheduledTask -TaskName 'SCHIG_DailyCollection' | Get-ScheduledTaskInfo | Format-List"],
            capture_output=True, text=True
        )
        return result.stdout
    except Exception as e:
        return f"Error getting task status: {e}"

def get_recent_logs(n_lines=20):
    """Get last N lines from collection log"""
    if LOG_FILE.exists():
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            return ''.join(lines[-n_lines:])
    return "No log file found"

def get_recent_collections():
    """List recent collection files"""
    files = []
    patterns = ['fresh_data_*.json', 'cranberry_fresh_*.parquet']

    for pattern in patterns:
        for f in OUTPUT_DIR.glob(pattern):
            stat = f.stat()
            files.append({
                'name': f.name,
                'size': stat.st_size,
                'modified': datetime.fromtimestamp(stat.st_mtime)
            })

    # Sort by modified time, newest first
    files.sort(key=lambda x: x['modified'], reverse=True)
    return files[:10]

def main():
    print("=" * 70)
    print("SCHIG MONITOR - Scared Child Gatherer Status")
    print("=" * 70)
    print(f"Check Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Task Scheduler Status
    print("-" * 70)
    print("TASK SCHEDULER STATUS:")
    print("-" * 70)
    print(get_task_status())

    # Recent Collections
    print("-" * 70)
    print("RECENT COLLECTIONS:")
    print("-" * 70)
    collections = get_recent_collections()
    if collections:
        for f in collections:
            size_kb = f['size'] / 1024
            print(f"  {f['name']}")
            print(f"    Size: {size_kb:.1f} KB | Modified: {f['modified'].strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print("  No collection files found")
    print()

    # Recent Logs
    print("-" * 70)
    print("RECENT LOG ENTRIES:")
    print("-" * 70)
    print(get_recent_logs(15))

    print("=" * 70)

if __name__ == "__main__":
    main()
