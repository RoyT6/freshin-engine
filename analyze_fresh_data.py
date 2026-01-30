#!/usr/bin/env python3
"""
Analyze Fresh In! data files and cross-reference with BFD V22
Identifies NEW titles to add vs EXISTING titles to update
"""

import json
import os
import pandas as pd
from collections import defaultdict

def main():
    # Load BFD existing IDs
    print("Loading BFD V22.00...")
    bfd = pd.read_parquet('C:/Users/RoyT6/Downloads/BFD_V22.00.parquet',
                          columns=['fc_uid','tmdb_id','imdb_id','title','title_type','start_year'])

    existing_tmdb = set(bfd['tmdb_id'].dropna().astype(int).tolist())
    existing_imdb = set(bfd['imdb_id'].dropna().tolist())

    print(f"BFD has {len(bfd):,} rows")
    print(f"BFD has {len(existing_tmdb):,} unique tmdb_ids")
    print(f"BFD has {len(existing_imdb):,} unique imdb_ids")

    # Extract titles from Fresh In!
    fresh_in_path = r'C:\Users\RoyT6\Downloads\Fresh In!'
    new_titles = []
    existing_updates = []

    # Process TMDB files
    print("\nProcessing TMDB files...")
    tmdb_files = [f for f in os.listdir(fresh_in_path) if f.startswith('tmdb_') and f.endswith('.json')]

    for f in tmdb_files:
        filepath = os.path.join(fresh_in_path, f)
        with open(filepath, 'r', encoding='utf-8') as file:
            try:
                data = json.load(file)
                for item in data:
                    if 'data' in item and 'results' in item['data']:
                        for r in item['data']['results']:
                            tmdb_id = r.get('id')
                            if tmdb_id:
                                title_info = {
                                    'tmdb_id': tmdb_id,
                                    'title': r.get('title') or r.get('name'),
                                    'media_type': r.get('media_type', 'unknown'),
                                    'release_date': r.get('release_date') or r.get('first_air_date'),
                                    'overview': r.get('overview', '')[:100] + '...' if r.get('overview') else '',
                                    'genre_ids': r.get('genre_ids', []),
                                    'popularity': r.get('popularity', 0),
                                    'vote_average': r.get('vote_average', 0),
                                    'source_file': f
                                }

                                if tmdb_id in existing_tmdb:
                                    existing_updates.append(title_info)
                                else:
                                    new_titles.append(title_info)
            except json.JSONDecodeError:
                print(f"  Error parsing {f}")

    # Dedupe new titles
    seen = set()
    unique_new = []
    for t in new_titles:
        if t['tmdb_id'] not in seen:
            seen.add(t['tmdb_id'])
            unique_new.append(t)

    # Dedupe existing updates
    seen_existing = set()
    unique_existing = []
    for t in existing_updates:
        if t['tmdb_id'] not in seen_existing:
            seen_existing.add(t['tmdb_id'])
            unique_existing.append(t)

    print(f"\n{'='*60}")
    print("ANALYSIS RESULTS")
    print(f"{'='*60}")
    print(f"TMDB files processed: {len(tmdb_files)}")
    print(f"Titles ALREADY in BFD (metadata update only): {len(unique_existing)}")
    print(f"NEW titles to ADD to BFD: {len(unique_new)}")

    if unique_new:
        print(f"\n{'='*60}")
        print("NEW TITLES TO ADD")
        print(f"{'='*60}")
        for i, t in enumerate(unique_new, 1):
            media = t['media_type'] or 'unknown'
            print(f"{i:3}. TMDB {t['tmdb_id']:>8}: {t['title'][:50]:<50} ({t['release_date']}) [{media}]")

    if unique_existing:
        print(f"\n{'='*60}")
        print("EXISTING TITLES (metadata update)")
        print(f"{'='*60}")
        for i, t in enumerate(unique_existing[:20], 1):
            print(f"{i:3}. TMDB {t['tmdb_id']:>8}: {t['title'][:50]:<50}")
        if len(unique_existing) > 20:
            print(f"  ... and {len(unique_existing) - 20} more")

    # Return data for further processing
    return {
        'new_titles': unique_new,
        'existing_updates': unique_existing,
        'bfd_shape': bfd.shape
    }

if __name__ == "__main__":
    results = main()
