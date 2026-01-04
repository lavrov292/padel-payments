#!/usr/bin/env python3
"""
Импорт турниров и участников из JSON (Lunda) в Supabase БД.

Использование:
    export DATABASE_URL="postgresql://..."
    export LUNDA_JSON_PATH="/path/to/tournaments_database.json"
    python scripts/import_lunda.py
"""

from dotenv import load_dotenv
load_dotenv()

import os
import json
import re
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values

def get_db_conn():
    """Get database connection with SSL."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    return psycopg2.connect(database_url, sslmode="require")

def parse_price(price_str):
    """Parse price from string like '6000 Р за пару' -> 6000. Returns 0 if not found."""
    if not price_str:
        return 0
    # Extract first number from string
    match = re.search(r'(\d+)', price_str.replace(' ', ''))
    if match:
        return int(match.group(1))
    return 0

def upsert_tournament(conn, tournament_data, global_last_updated=None):
    """UPSERT tournament by (location, starts_at). Returns tournament_id."""
    cur = conn.cursor()
    
    # Extract tournament info from nested structure
    tournament_info = tournament_data.get('tournament', {})
    
    location = tournament_info.get('location', '') or ''
    starts_at = tournament_data.get('start_datetime')
    ends_at = tournament_data.get('end_datetime')
    organizer = tournament_info.get('organizer', '')
    title = tournament_info.get('title', '')
    price_rub = parse_price(tournament_info.get('price', ''))
    source_last_updated = tournament_data.get('last_updated') or global_last_updated
    
    # UPSERT tournament
    cur.execute("""
        INSERT INTO tournaments (location, starts_at, ends_at, organizer, title, price_rub, source_last_updated)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (location, starts_at)
        DO UPDATE SET
            ends_at = EXCLUDED.ends_at,
            organizer = EXCLUDED.organizer,
            title = EXCLUDED.title,
            price_rub = EXCLUDED.price_rub,
            source_last_updated = EXCLUDED.source_last_updated
        RETURNING id
    """, (location, starts_at, ends_at, organizer, title, price_rub, source_last_updated))
    
    tournament_id = cur.fetchone()[0]
    conn.commit()
    return tournament_id

def upsert_player(conn, full_name):
    """UPSERT player by full_name. Returns player_id."""
    cur = conn.cursor()
    
    cur.execute("""
        INSERT INTO players (full_name)
        VALUES (%s)
        ON CONFLICT (full_name) DO NOTHING
        RETURNING id
    """, (full_name,))
    
    row = cur.fetchone()
    if row:
        player_id = row[0]
    else:
        # Player already exists, get its id
        cur.execute("SELECT id FROM players WHERE full_name = %s", (full_name,))
        player_id = cur.fetchone()[0]
    
    conn.commit()
    return player_id

def upsert_entry(conn, tournament_id, player_id):
    """UPSERT entry by (tournament_id, player_id). Returns (entry_id, was_new)."""
    cur = conn.cursor()
    
    # Check if entry exists
    cur.execute("""
        SELECT id, payment_status, manual_paid
        FROM entries
        WHERE tournament_id = %s AND player_id = %s
    """, (tournament_id, player_id))
    
    existing = cur.fetchone()
    
    if existing:
        # Entry exists - update active and last_seen_in_source
        entry_id = existing[0]
        cur.execute("""
            UPDATE entries
            SET active = true, last_seen_in_source = NOW()
            WHERE id = %s
        """, (entry_id,))
        conn.commit()
        return (entry_id, False)
    else:
        # New entry - create with payment_status='pending', active=true
        cur.execute("""
            INSERT INTO entries (tournament_id, player_id, payment_status, active, last_seen_in_source)
            VALUES (%s, %s, 'pending', true, NOW())
            RETURNING id
        """, (tournament_id, player_id))
        entry_id = cur.fetchone()[0]
        conn.commit()
        return (entry_id, True)

def process_tournament(conn, tournament_data, stats, global_last_updated=None):
    """Process single tournament: upsert tournament, participants, and handle removed entries."""
    # 1. UPSERT tournament
    tournament_info = tournament_data.get('tournament', {})
    tournament_id = upsert_tournament(conn, tournament_data, global_last_updated)
    stats['tournaments_upsert'] += 1
    
    # 2. Get participants list
    participants = tournament_data.get('participants', [])
    participant_names = [p for p in participants if p]  # Filter empty strings
    
    # 3. Get current player_ids for this tournament
    cur = conn.cursor()
    cur.execute("""
        SELECT e.id, e.player_id, e.payment_status, e.manual_paid, p.full_name
        FROM entries e
        JOIN players p ON e.player_id = p.id
        WHERE e.tournament_id = %s
    """, (tournament_id,))
    current_entries = cur.fetchall()
    
    # 4. Process participants
    processed_player_ids = set()
    for participant_name in participant_names:
        # UPSERT player
        player_id = upsert_player(conn, participant_name)
        if player_id not in processed_player_ids:
            stats['players_upsert'] += 1
            processed_player_ids.add(player_id)
        
        # UPSERT entry
        entry_id, was_new = upsert_entry(conn, tournament_id, player_id)
        if was_new:
            stats['entries_upsert_new'] += 1
        else:
            stats['entries_upsert_existing'] += 1
    
    # 5. Handle entries that are no longer in participants
    # Get player_ids for new participants
    new_player_ids = set()
    for participant_name in participant_names:
        cur.execute("SELECT id FROM players WHERE full_name = %s", (participant_name,))
        row = cur.fetchone()
        if row:
            new_player_ids.add(row[0])
    
    # Find entries that are not in new participants list
    for entry_row in current_entries:
        entry_id, player_id, payment_status, manual_paid, full_name = entry_row
        if player_id not in new_player_ids:
            # This entry is no longer in participants
            if payment_status == 'paid' or manual_paid:
                # Mark as inactive (preserve paid entries)
                cur.execute("""
                    UPDATE entries
                    SET active = false
                    WHERE id = %s
                """, (entry_id,))
                stats['entries_marked_inactive'] += 1
            else:
                # Delete entry (not paid, safe to delete)
                cur.execute("DELETE FROM entries WHERE id = %s", (entry_id,))
                stats['entries_deleted'] += 1
    
    conn.commit()

def main():
    """Main import function."""
    # Get paths from env
    database_url = os.getenv("DATABASE_URL")
    json_path = os.getenv("LUNDA_JSON_PATH")
    
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        return 1
    
    if not json_path:
        print("ERROR: LUNDA_JSON_PATH not set")
        return 1
    
    # Load JSON
    print(f"Loading JSON from: {json_path}")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found: {json_path}")
        return 1
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return 1
    
    # Get tournaments dict/list
    tournaments_raw = data.get('tournaments', {})
    if not tournaments_raw:
        print("ERROR: No 'tournaments' key in JSON")
        return 1
    
    # Debug output
    print(f"DEBUG: data['tournaments'] type: {type(tournaments_raw).__name__}")
    
    # Extract tournaments list from dict or use as-is if list
    if isinstance(tournaments_raw, dict):
        tournaments_list = list(tournaments_raw.values())
    elif isinstance(tournaments_raw, list):
        tournaments_list = tournaments_raw
    else:
        print(f"ERROR: Unexpected type for tournaments: {type(tournaments_raw)}")
        return 1
    
    print(f"Found {len(tournaments_list)} tournaments in JSON")
    
    # Get global last_updated if available
    global_last_updated = data.get('last_updated')
    
    # Connect to DB
    try:
        conn = get_db_conn()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        return 1
    
    # Statistics
    stats = {
        'tournaments_upsert': 0,
        'players_upsert': 0,
        'entries_upsert_new': 0,
        'entries_upsert_existing': 0,
        'entries_marked_inactive': 0,
        'entries_deleted': 0
    }
    
    # Process each tournament
    try:
        for tournament_data in tournaments_list:
            tournament_info = tournament_data.get('tournament', {})
            title = tournament_info.get('title', 'Unknown')
            location = tournament_info.get('location', 'Unknown')
            print(f"Processing tournament: {title} at {location}")
            process_tournament(conn, tournament_data, stats, global_last_updated)
    except Exception as e:
        print(f"ERROR during processing: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return 1
    finally:
        conn.close()
    
    # Print statistics
    print("\n" + "="*50)
    print("IMPORT STATISTICS")
    print("="*50)
    print(f"Tournaments UPSERT: {stats['tournaments_upsert']}")
    print(f"Players UPSERT: {stats['players_upsert']}")
    print(f"Entries UPSERT (new): {stats['entries_upsert_new']}")
    print(f"Entries UPSERT (existing): {stats['entries_upsert_existing']}")
    print(f"Entries marked inactive: {stats['entries_marked_inactive']}")
    print(f"Entries deleted: {stats['entries_deleted']}")
    print("="*50)
    
    return 0

if __name__ == "__main__":
    exit(main())

