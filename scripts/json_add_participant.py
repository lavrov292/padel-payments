#!/usr/bin/env python3
"""
Скрипт для добавления участника в JSON файл турниров (локальный тест импорта).

Использование:
    export LUNDA_JSON_PATH="/path/to/tournaments_database.json"
    python scripts/json_add_participant.py \
        --tournament_starts_at "2026-01-05 15:00" \
        --location "K5 Padel | Санкт-Петербург" \
        --full_name "Player With TG"
"""

from dotenv import load_dotenv
load_dotenv()

import os
import json
import argparse
import shutil
from datetime import datetime
from pathlib import Path

def parse_datetime(date_str):
    """Parse datetime from various formats. Returns naive datetime."""
    if isinstance(date_str, datetime):
        # If already datetime, return as-is (make naive if needed)
        if date_str.tzinfo:
            return date_str.replace(tzinfo=None)
        return date_str
    
    if not isinstance(date_str, str):
        raise ValueError(f"Expected string or datetime, got {type(date_str)}")
    
    # Try ISO format first
    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M:%S.%f",  # With microseconds
        "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO with microseconds and Z
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Make naive (remove timezone info for comparison)
            if dt.tzinfo:
                dt = dt.replace(tzinfo=None)
            return dt
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse datetime: {date_str}")

def find_tournament(data, starts_at, location):
    """Find tournament in JSON by starts_at and location. Returns (tournament_data, tournament_info, tournaments_raw)."""
    tournaments_raw = data.get('tournaments', {})
    
    # Extract tournaments list from dict or use as-is if list
    if isinstance(tournaments_raw, dict):
        tournaments_list = list(tournaments_raw.values())
        tournaments_is_dict = True
    elif isinstance(tournaments_raw, list):
        tournaments_list = tournaments_raw
        tournaments_is_dict = False
    else:
        return None, None, None
    
    # Normalize location for comparison
    location_normalized = (location or '').strip()
    starts_at_normalized = starts_at.replace(microsecond=0)
    
    # Debug: print first few tournaments for comparison
    print(f"DEBUG: Searching for location='{location_normalized}', starts_at={starts_at_normalized}")
    print(f"DEBUG: Found {len(tournaments_list)} tournaments in JSON")
    
    # Also search in title if location doesn't match (sometimes location is in title)
    search_in_title = False
    
    for idx, tournament_data in enumerate(tournaments_list):
        tournament_info = tournament_data.get('tournament', {})
        tournament_location = (tournament_info.get('location', '') or '').strip()
        tournament_title = (tournament_info.get('title', '') or '').strip()
        tournament_starts_at_str = tournament_data.get('start_datetime')
        
        # Debug: print first 5 tournaments and any that match location partially
        if idx < 5 or location_normalized.lower() in tournament_location.lower() or location_normalized.lower() in tournament_title.lower():
            print(f"DEBUG: Tournament {idx}: title='{tournament_title}', location='{tournament_location}', start='{tournament_starts_at_str}'")
        
        if not tournament_starts_at_str:
            continue
        
        try:
            tournament_starts_at = parse_datetime(tournament_starts_at_str)
            # Normalize for comparison (remove microseconds if any)
            tournament_starts_at = tournament_starts_at.replace(microsecond=0)
        except (ValueError, TypeError) as e:
            if idx < 5:
                print(f"DEBUG: Failed to parse datetime '{tournament_starts_at_str}': {e}")
            continue
        
        # Compare: location can match either tournament.location OR tournament.title
        # (sometimes location is stored in title field)
        location_match = (tournament_location == location_normalized or 
                         tournament_title == location_normalized)
        datetime_match = tournament_starts_at == starts_at_normalized
        
        if location_match or datetime_match:
            print(f"DEBUG: Match check: location_match={location_match} (loc='{tournament_location}' == '{location_normalized}' or title='{tournament_title}' == '{location_normalized}'), datetime_match={datetime_match}")
        
        if location_match and datetime_match:
            return tournament_data, tournament_info, tournaments_raw
    
    return None, None, None

def add_participant(tournament_data, full_name):
    """Add participant to tournament if not already present. Returns (added, was_present)."""
    participants = tournament_data.get('participants', [])
    
    # Normalize full_name for comparison
    full_name_normalized = full_name.strip()
    
    # Check if already present
    for p in participants:
        if p and p.strip() == full_name_normalized:
            return False, True  # was_present = True
    
    # Add participant
    if not participants:
        tournament_data['participants'] = []
    
    tournament_data['participants'].append(full_name_normalized)
    return True, False  # added = True, was_present = False

def main():
    parser = argparse.ArgumentParser(description='Add participant to tournament JSON')
    parser.add_argument('--tournament_starts_at', required=True,
                        help='Tournament start datetime (e.g., "2026-01-05 15:00" or ISO format)')
    parser.add_argument('--location', required=True,
                        help='Tournament location or title (searches in both fields)')
    parser.add_argument('--full_name', required=True,
                        help='Full name of participant to add')
    
    args = parser.parse_args()
    
    # Get JSON path from env
    json_path = os.getenv("LUNDA_JSON_PATH")
    if not json_path:
        print("ERROR: LUNDA_JSON_PATH not set")
        return 1
    
    json_path = Path(json_path)
    if not json_path.exists():
        print(f"ERROR: File not found: {json_path}")
        return 1
    
    # Parse datetime
    try:
        starts_at = parse_datetime(args.tournament_starts_at)
    except ValueError as e:
        print(f"ERROR: {e}")
        return 1
    
    # Load JSON
    print(f"Loading JSON from: {json_path}")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return 1
    
    # Find tournament
    tournament_data, tournament_info, tournaments_raw = find_tournament(data, starts_at, args.location)
    
    if not tournament_data:
        print(f"ERROR: Tournament not found")
        print(f"  Location: {args.location}")
        print(f"  Starts at: {starts_at}")
        print(f"\nHint: Try searching by title instead of location if location doesn't match.")
        print(f"Or check the exact format of start_datetime in JSON.")
        return 1
    
    tournament_title = tournament_info.get('title', 'Unknown')
    print(f"Found tournament: {tournament_title}")
    
    # Add participant
    added, was_present = add_participant(tournament_data, args.full_name)
    
    if was_present:
        print(f"OK: already present")
        print(f"Tournament: {tournament_title}")
        return 0
    
    # Create backup
    backup_path = json_path.with_suffix(json_path.suffix + '.bak')
    print(f"Creating backup: {backup_path}")
    shutil.copy2(json_path, backup_path)
    
    # Save JSON (preserve original structure)
    print(f"Saving JSON to: {json_path}")
    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"ERROR: Failed to save JSON: {e}")
        # Restore from backup
        shutil.copy2(backup_path, json_path)
        return 1
    
    print(f"OK: added")
    print(f"Tournament: {tournament_title}")
    print(f"Participant: {args.full_name}")
    
    return 0

if __name__ == "__main__":
    exit(main())

