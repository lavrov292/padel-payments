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
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
import requests
import sys
import re

# MSK timezone offset: UTC+3
MSK_TZ = timezone(timedelta(hours=3))

def normalize_name(s):
    """
    Normalize name for comparison/searching.
    - strip and lowercase
    - replace 'ё' with 'е'
    - collapse whitespace
    Returns normalized string (NEVER show to users).
    """
    if not s:
        return ""
    # Strip and lowercase
    s = s.strip().lower()
    # Replace ё with е
    s = s.replace('ё', 'е')
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s)
    return s

def get_db_conn():
    """Get database connection with SSL."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    return psycopg2.connect(database_url, sslmode="require")

def ensure_conn(conn):
    """Ensure database connection is alive. Reconnect if needed."""
    if conn is None or getattr(conn, "closed", 1) != 0:
        print("DB RECONNECT: Connection closed, reconnecting...")
        return get_db_conn()
    return conn

def safe_rollback(conn):
    """Safely rollback transaction if connection is alive."""
    if conn and getattr(conn, "closed", 1) == 0:
        try:
            conn.rollback()
        except Exception as e:
            print(f"WARNING: Rollback failed: {e}")
    else:
        print("WARNING: Cannot rollback - connection is closed")

def parse_price(price_str):
    """Parse price from string like '6000 Р за пару' -> 6000. Returns 0 if not found."""
    if not price_str:
        return 0
    # Extract first number from string
    match = re.search(r'(\d+)', price_str.replace(' ', ''))
    if match:
        return int(match.group(1))
    return 0

def normalize_msk(dt_str):
    """
    Normalize datetime string to MSK timezone (+03:00).
    If timezone is missing, assumes MSK.
    If timezone exists, converts to MSK.
    Returns datetime object with tzinfo=+03:00.
    """
    if not dt_str:
        return None
    
    # If already a datetime object
    if isinstance(dt_str, datetime):
        if dt_str.tzinfo is None:
            # Naive datetime - assume it's MSK
            return dt_str.replace(tzinfo=MSK_TZ)
        else:
            # Convert to MSK
            return dt_str.astimezone(MSK_TZ)
    
    # Parse string
    if isinstance(dt_str, str):
        # Try parsing various formats
        formats = [
            "%Y-%m-%dT%H:%M:%S%z",  # ISO with timezone
            "%Y-%m-%dT%H:%M:%S.%f%z",  # ISO with microseconds and timezone
            "%Y-%m-%dT%H:%M:%S",  # ISO without timezone
            "%Y-%m-%dT%H:%M:%S.%f",  # ISO with microseconds without timezone
            "%Y-%m-%d %H:%M:%S",  # Space separator
        ]
        
        dt = None
        for fmt in formats:
            try:
                dt = datetime.strptime(dt_str, fmt)
                break
            except ValueError:
                continue
        
        if dt is None:
            # Fallback: try parsing with dateutil or return None
            try:
                dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            except:
                print(f"WARNING: Could not parse datetime: {dt_str}")
                return None
        
        # If timezone is missing, assume MSK
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=MSK_TZ)
        else:
            # Convert to MSK
            dt = dt.astimezone(MSK_TZ)
        
        return dt
    
    return None

def check_column_exists(conn, table_name, column_name):
    """Check if column exists in table."""
    cur = conn.cursor()
    cur.execute("""
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = %s AND column_name = %s
    """, (table_name, column_name))
    exists = cur.fetchone() is not None
    cur.close()
    return exists

def upsert_tournament(conn, tournament_data, global_last_updated=None):
    """UPSERT tournament by (location, starts_at). Returns (tournament_id, was_new)."""
    cur = conn.cursor()
    
    # Extract tournament info from nested structure
    tournament_info = tournament_data.get('tournament', {})
    
    location = tournament_info.get('location', '') or ''
    starts_at_raw = tournament_data.get('start_datetime')
    ends_at_raw = tournament_data.get('end_datetime')
    
    # Normalize datetime to MSK timezone
    starts_at = normalize_msk(starts_at_raw)
    ends_at = normalize_msk(ends_at_raw) if ends_at_raw else None
    
    organizer = tournament_info.get('organizer', '')
    title = tournament_info.get('title', '')
    price_rub = parse_price(tournament_info.get('price', ''))
    source_last_updated = tournament_data.get('last_updated') or global_last_updated
    
    # Get tournament_type from tournament_data (not from nested tournament object)
    tournament_type = tournament_data.get('tournament_type', '').lower()
    if tournament_type not in ['personal', 'team']:
        tournament_type = 'personal'  # Default to personal if not specified or invalid
    
    # Log tournament import
    print(f"IMPORT TOURNAMENT: title={title}, location={location}, starts_at={starts_at} (MSK), type={tournament_type}")
    
    # Check if new columns exist
    has_active = check_column_exists(conn, 'tournaments', 'active')
    has_archived_at = check_column_exists(conn, 'tournaments', 'archived_at')
    has_first_seen = check_column_exists(conn, 'tournaments', 'first_seen_in_source')
    has_last_seen = check_column_exists(conn, 'tournaments', 'last_seen_in_source')
    has_source = check_column_exists(conn, 'tournaments', 'source')
    
    # Check if tournament exists (compare as timestamptz)
    # Note: PostgreSQL will compare timestamptz correctly even if timezone differs
    cur.execute("""
        SELECT id FROM tournaments 
        WHERE location = %s AND starts_at = %s::timestamptz
    """, (location, starts_at))
    
    existing = cur.fetchone()
    
    if existing:
        # Tournament exists - update fields but preserve first_seen_in_source
        tournament_id = existing[0]
        
        # Build UPDATE query dynamically based on available columns
        update_fields = [
            "ends_at = %s",
            "organizer = %s",
            "title = %s",
            "price_rub = %s",
            "source_last_updated = %s",
            "tournament_type = %s"
        ]
        update_values = [ends_at, organizer, title, price_rub, source_last_updated, tournament_type]
        
        # Always set last_seen_in_source = NOW() and archived_at = NULL when tournament is seen in JSON
        if has_last_seen:
            update_fields.append("last_seen_in_source = NOW()")
        if has_archived_at:
            update_fields.append("archived_at = NULL")
        if has_active:
            update_fields.append("active = true")
        if has_source:
            update_fields.append("source = 'lunda'")
        
        update_query = f"""
            UPDATE tournaments
            SET {', '.join(update_fields)}
            WHERE id = %s
        """
        update_values.append(tournament_id)
        
        cur.execute(update_query, tuple(update_values))
        conn.commit()
        return (tournament_id, False)
    else:
        # New tournament - create with all timestamps
        insert_fields = ["location", "starts_at", "ends_at", "organizer", "title", "price_rub", "source_last_updated", "tournament_type"]
        insert_values = [location, starts_at, ends_at, organizer, title, price_rub, source_last_updated, tournament_type]
        insert_placeholders = ["%s"] * len(insert_fields)
        
        # Always set timestamps for new tournaments
        if has_first_seen:
            insert_fields.append("first_seen_in_source")
            insert_values.append("NOW()")
        if has_last_seen:
            insert_fields.append("last_seen_in_source")
            insert_values.append("NOW()")
        if has_archived_at:
            insert_fields.append("archived_at")
            insert_values.append(None)  # Explicitly NULL for new tournaments
        if has_active:
            insert_fields.append("active")
            insert_values.append(True)
        if has_source:
            insert_fields.append("source")
            insert_values.append("lunda")
        
        # Build placeholders and values separately
        placeholders = []
        sql_values = []
        for val in insert_values:
            if val == "NOW()":
                placeholders.append("NOW()")
            elif val is None:
                placeholders.append("NULL")
            else:
                placeholders.append("%s")
                sql_values.append(val)
        
        insert_query = f"""
            INSERT INTO tournaments ({', '.join(insert_fields)})
            VALUES ({', '.join(placeholders)})
            RETURNING id
        """
        
        cur.execute(insert_query, tuple(sql_values))
        tournament_id = cur.fetchone()[0]
        conn.commit()
        return (tournament_id, True)

def get_levenshtein_threshold(normalized_name_len):
    """
    Get dynamic threshold for Levenshtein distance based on name length.
    - len <= 12 -> 1
    - 13..20 -> 2
    - > 20 -> 3
    """
    if normalized_name_len <= 12:
        return 1
    elif normalized_name_len <= 20:
        return 2
    else:
        return 3

def resolve_player(conn, raw_name):
    """
    Resolve player by name using hybrid logic:
    1. Try alias
    2. Try exact normalized match
    3. Find candidates by Levenshtein
    4. Check threshold
    
    Returns:
    - (player_id, None) if exact match found
    - (None, candidates) if ambiguous (best_dist <= threshold)
    - (None, []) if new player (no candidates or best_dist > threshold)
    """
    if not raw_name:
        return (None, [])
    
    norm = normalize_name(raw_name)
    norm_len = len(norm)
    cur = conn.cursor()
    
    # 1. Try alias first
    cur.execute("""
        SELECT player_id FROM player_aliases 
        WHERE normalized_alias = %s
        LIMIT 1
    """, (norm,))
    row = cur.fetchone()
    if row:
        cur.close()
        return (row[0], None)  # Exact match via alias
    
    # 2. Try exact normalized match
    cur.execute("""
        SELECT id FROM players 
        WHERE normalized_name = %s
        LIMIT 1
    """, (norm,))
    row = cur.fetchone()
    if row:
        cur.close()
        return (row[0], None)  # Exact match
    
    # 3. Find candidates by Levenshtein
    threshold = get_levenshtein_threshold(norm_len)
    candidates = []
    best_dist = None
    
    try:
        cur.execute("""
            SELECT id, full_name, 
                   levenshtein(normalized_name, %s::text) AS dist
            FROM players
            WHERE normalized_name IS NOT NULL
            ORDER BY dist ASC
            LIMIT 6
        """, (norm,))
        rows = cur.fetchall()
        
        for player_id, name, dist in rows:
            candidates.append({
                'player_id': player_id,
                'name': name,
                'dist': dist
            })
            if best_dist is None:
                best_dist = dist
        
        print(f"FUZZY MATCH: input=\"{raw_name}\", candidates={len(candidates)}, threshold={threshold}")
    except psycopg2.Error as e:
        error_msg = str(e).lower()
        if 'levenshtein' in error_msg or 'fuzzystrmatch' in error_msg or 'does not exist' in error_msg:
            print(f"LEVENSHTEIN DISABLED: {e}")
            print(f"FUZZY MATCH FALLBACK: disabled")
            candidates = []
            best_dist = None
        else:
            # Re-raise if it's a different error
            raise
    
    cur.close()
    
    # 4. Check threshold
    if candidates and best_dist is not None:
        if best_dist <= threshold:
            return (None, candidates)  # Ambiguous - needs admin decision
    
    # 5. New player (no candidates or best_dist > threshold)
    return (None, [])

def find_candidate_players(conn, normalized_name, limit=6):
    """
    Find candidate players by Levenshtein distance.
    Returns list of {player_id, name, dist}.
    Falls back to empty list if Levenshtein extension is not available.
    """
    cur = conn.cursor()
    candidates = []
    
    try:
        cur.execute("""
            SELECT id, full_name, 
                   levenshtein(normalized_name, %s::text) AS dist
            FROM players
            WHERE normalized_name IS NOT NULL
            ORDER BY dist ASC
            LIMIT %s
        """, (normalized_name, limit))
        rows = cur.fetchall()
        
        for player_id, name, dist in rows:
            candidates.append({
                'player_id': player_id,
                'name': name,
                'dist': dist
            })
    except psycopg2.Error as e:
        error_msg = str(e).lower()
        if 'levenshtein' in error_msg or 'fuzzystrmatch' in error_msg or 'does not exist' in error_msg:
            print(f"LEVENSHTEIN DISABLED: {e}")
            print(f"FUZZY MATCH FALLBACK: disabled")
            candidates = []
        else:
            # Re-raise if it's a different error
            raise
    
    cur.close()
    return candidates

def upsert_player(conn, full_name):
    """
    UPSERT player by full_name.
    Also updates normalized_name if missing.
    Returns player_id.
    """
    cur = conn.cursor()
    
    # Check if normalized_name column exists
    cur.execute("""
        SELECT column_name FROM information_schema.columns 
        WHERE table_name = 'players' AND column_name = 'normalized_name'
    """)
    has_normalized = cur.fetchone() is not None
    
    norm = normalize_name(full_name) if has_normalized else None
    
    # Insert or get existing
    if has_normalized:
        cur.execute("""
            INSERT INTO players (full_name, normalized_name)
            VALUES (%s, %s)
            ON CONFLICT (full_name) DO UPDATE 
            SET normalized_name = COALESCE(players.normalized_name, EXCLUDED.normalized_name)
            RETURNING id
        """, (full_name, norm))
    else:
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
        # Update normalized_name if missing
        if has_normalized and norm:
            cur.execute("""
                UPDATE players 
                SET normalized_name = %s 
                WHERE id = %s AND normalized_name IS NULL
            """, (norm, player_id))
    
    conn.commit()
    cur.close()
    return player_id

def upsert_entry(conn, tournament_id, player_id):
    """UPSERT entry by (tournament_id, player_id). Returns (entry_id, was_new)."""
    cur = conn.cursor()
    
    # Check if new columns exist
    has_entry_active = check_column_exists(conn, 'entries', 'active')
    has_first_seen = check_column_exists(conn, 'entries', 'first_seen_in_source')
    has_last_seen = check_column_exists(conn, 'entries', 'last_seen_in_source')
    
    # Check if entry exists
    cur.execute("""
        SELECT id, payment_status, manual_paid
        FROM entries
        WHERE tournament_id = %s AND player_id = %s
    """, (tournament_id, player_id))
    
    existing = cur.fetchone()
    
    if existing:
        # Entry exists - update active and last_seen_in_source (don't touch first_seen_in_source)
        entry_id = existing[0]
        
        # Build UPDATE query dynamically
        update_fields = []
        if has_entry_active:
            update_fields.append("active = true")
        if has_last_seen:
            update_fields.append("last_seen_in_source = NOW()")
        
        if update_fields:
            update_query = f"""
                UPDATE entries
                SET {', '.join(update_fields)}
                WHERE id = %s
            """
            cur.execute(update_query, (entry_id,))
        conn.commit()
        return (entry_id, False)
    else:
        # New entry - create with payment_status='pending', active=true, both timestamps = now()
        insert_fields = ["tournament_id", "player_id", "payment_status"]
        insert_values = [tournament_id, player_id, 'pending']
        placeholders = ["%s", "%s", "%s"]
        
        if has_entry_active:
            insert_fields.append("active")
            insert_values.append(True)
            placeholders.append("%s")
        if has_first_seen:
            insert_fields.append("first_seen_in_source")
            placeholders.append("NOW()")
        if has_last_seen:
            insert_fields.append("last_seen_in_source")
            placeholders.append("NOW()")
        
        insert_query = f"""
            INSERT INTO entries ({', '.join(insert_fields)})
            VALUES ({', '.join(placeholders)})
            RETURNING id
        """
        
        cur.execute(insert_query, tuple(insert_values))
        entry_id = cur.fetchone()[0]
        conn.commit()
        return (entry_id, True)

def create_pending_entry(conn, sync_run_id, tournament_id, raw_player_name, normalized_name, payload, candidates):
    """
    Create or update pending_entry.
    Returns pending_entry_id.
    """
    cur = conn.cursor()
    import json
    
    # Try to find existing pending entry
    cur.execute("""
        SELECT id FROM pending_entries
        WHERE sync_run_id = %s 
          AND tournament_id = %s 
          AND normalized_name = %s
          AND raw_player_name = %s
        LIMIT 1
    """, (sync_run_id, tournament_id, normalized_name, raw_player_name))
    
    row = cur.fetchone()
    if row:
        # Update existing
        pending_id = row[0]
        cur.execute("""
            UPDATE pending_entries
            SET candidates = %s, status = 'pending', created_at = NOW()
            WHERE id = %s
        """, (json.dumps(candidates), pending_id))
    else:
        # Create new
        cur.execute("""
            INSERT INTO pending_entries 
            (sync_run_id, tournament_id, raw_player_name, normalized_name, payload, candidates, status)
            VALUES (%s, %s, %s, %s, %s, %s, 'pending')
            RETURNING id
        """, (
            sync_run_id,
            tournament_id,
            raw_player_name,
            normalized_name,
            json.dumps(payload),
            json.dumps(candidates)
        ))
        row = cur.fetchone()
        pending_id = row[0] if row else None
    
    conn.commit()
    cur.close()
    return pending_id

def send_pending_notification_to_admin(bot_token, admin_chat_id, pending_id, tournament_title, starts_at, raw_player_name, candidates):
    """
    Send Telegram notification to admin about pending entry.
    Returns message_id or None.
    """
    if not bot_token or not admin_chat_id:
        return None
    
    try:
        from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
        
        bot = Bot(token=bot_token)
        
        # Format starts_at
        if starts_at:
            if isinstance(starts_at, datetime):
                starts_at_str = starts_at.strftime("%d.%m.%Y %H:%M")
            else:
                starts_at_str = str(starts_at)
        else:
            starts_at_str = "Не указано"
        
        # Build message
        message = f"""⚠️ Имя из Lunda не совпало с базой.

Турнир: {tournament_title} ({starts_at_str})
Имя из Lunda: {raw_player_name}

Выбери кто это:"""
        
        # Build buttons
        buttons = []
        for cand in candidates[:6]:  # Max 6 candidates
            name = cand['name']
            dist = cand['dist']
            player_id = cand['player_id']
            button_text = f"{name} (dist={dist})"
            buttons.append([InlineKeyboardButton(
                button_text,
                callback_data=f"pending_approve:{pending_id}:{player_id}"
            )])
        
        buttons.append([InlineKeyboardButton(
            "➕ Это новый игрок",
            callback_data=f"pending_new_player:{pending_id}"
        )])
        buttons.append([InlineKeyboardButton(
            "❌ Игнор",
            callback_data=f"pending_reject:{pending_id}"
        )])
        
        keyboard = InlineKeyboardMarkup(buttons)
        
        # Send message using async
        try:
            import asyncio
            # Create new event loop if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            if loop.is_running():
                # If loop is already running, we can't use asyncio.run
                # Use a thread instead
                import threading
                result_container = {'result': None, 'error': None}
                
                def send_in_thread():
                    try:
                        new_loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(new_loop)
                        result = new_loop.run_until_complete(bot.send_message(
                            chat_id=admin_chat_id,
                            text=message,
                            reply_markup=keyboard
                        ))
                        result_container['result'] = result
                        new_loop.close()
                    except Exception as e:
                        result_container['error'] = e
                
                thread = threading.Thread(target=send_in_thread)
                thread.start()
                thread.join(timeout=10)
                
                if result_container['error']:
                    raise result_container['error']
                result = result_container['result']
            else:
                result = loop.run_until_complete(bot.send_message(
                    chat_id=admin_chat_id,
                    text=message,
                    reply_markup=keyboard
                ))
            
            return result.message_id if result else None
        except Exception as e:
            print(f"ERROR sending pending notification: {e}")
            import traceback
            traceback.print_exc()
            return None
    except Exception as e:
        print(f"ERROR sending pending notification: {e}")
        return None

def process_tournament(conn, tournament_data, stats, global_last_updated=None, processed_tournament_ids=None, sync_run_id=None):
    """Process single tournament: upsert tournament, participants, and handle removed entries."""
    # 1. UPSERT tournament
    tournament_info = tournament_data.get('tournament', {})
    tournament_id, was_new = upsert_tournament(conn, tournament_data, global_last_updated)
    stats['tournaments_upsert'] += 1
    if processed_tournament_ids is not None:
        processed_tournament_ids.add(tournament_id)
    
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
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    admin_chat_id = os.getenv("ADMIN_CHAT_ID")
    
    # Get tournament info for pending notifications
    cur.execute("SELECT title, starts_at FROM tournaments WHERE id = %s", (tournament_id,))
    tour_row = cur.fetchone()
    tournament_title = tour_row[0] if tour_row else "Неизвестный турнир"
    tournament_starts_at = tour_row[1] if tour_row else None
    
    for participant_name in participant_names:
        if not participant_name:
            continue
        
        # Resolve player using hybrid logic
        player_id, candidates = resolve_player(conn, participant_name)
        
        if player_id:
            # Exact match found - create/update entry
            if player_id not in processed_player_ids:
                stats['players_upsert'] += 1
                processed_player_ids.add(player_id)
            
            entry_id, was_new = upsert_entry(conn, tournament_id, player_id)
            if was_new:
                stats['entries_new'] += 1
            else:
                stats['entries_existing'] += 1
        elif candidates:
            # Ambiguous case - best_dist <= threshold
            # Create pending entry for admin decision
            norm = normalize_name(participant_name)
            payload = {
                'tournament_id': tournament_id,
                'raw_player_name': participant_name,
                'normalized_name': norm
            }
            
            pending_id = create_pending_entry(
                conn, sync_run_id, tournament_id, 
                participant_name, norm, payload, candidates
            )
            
            if pending_id:
                # Send notification to admin
                message_id = send_pending_notification_to_admin(
                    bot_token, admin_chat_id, pending_id,
                    tournament_title, tournament_starts_at,
                    participant_name, candidates
                )
                
                # Update admin_message_id
                if message_id:
                    cur.execute("""
                        UPDATE pending_entries 
                        SET admin_message_id = %s 
                        WHERE id = %s
                    """, (message_id, pending_id))
                    conn.commit()
                
                print(f"PENDING: {participant_name} -> {len(candidates)} candidates, pending_id={pending_id}")
        else:
            # New player - create player and entry
            norm = normalize_name(participant_name)
            player_id = upsert_player(conn, participant_name)
            
            if player_id not in processed_player_ids:
                stats['players_upsert'] += 1
                processed_player_ids.add(player_id)
            
            entry_id, was_new = upsert_entry(conn, tournament_id, player_id)
            if was_new:
                stats['entries_new'] += 1
            else:
                stats['entries_existing'] += 1
            
            # Optional: send info to admin about new player
            if bot_token and admin_chat_id:
                try:
                    import asyncio
                    from telegram import Bot
                    bot = Bot(token=bot_token)
                    info_msg = f"ℹ️ Добавлен новый игрок: {participant_name}"
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            import threading
                            def send_info():
                                new_loop = asyncio.new_event_loop()
                                asyncio.set_event_loop(new_loop)
                                new_loop.run_until_complete(bot.send_message(
                                    chat_id=admin_chat_id,
                                    text=info_msg
                                ))
                                new_loop.close()
                            thread = threading.Thread(target=send_info)
                            thread.start()
                            thread.join(timeout=5)
                        else:
                            loop.run_until_complete(bot.send_message(
                                chat_id=admin_chat_id,
                                text=info_msg
                            ))
                    except Exception as e:
                        print(f"INFO: Failed to send new player notification: {e}")
                except Exception as e:
                    pass  # Ignore errors in optional notification
    
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
                # Mark as inactive (preserve paid entries) - only if column exists
                has_entry_active = check_column_exists(conn, 'entries', 'active')
                if has_entry_active:
                    cur.execute("""
                        UPDATE entries
                        SET active = false
                        WHERE id = %s
                    """, (entry_id,))
                stats['entries_inactivated'] += 1
            else:
                # Delete entry (not paid, safe to delete)
                cur.execute("DELETE FROM entries WHERE id = %s", (entry_id,))
                stats['entries_deleted'] += 1
    
    conn.commit()

def create_sync_run(conn, json_path):
    """Create sync_runs record and return sync_run_id."""
    cur = conn.cursor()
    
    # Get JSON file mtime
    json_mtime = None
    if json_path and os.path.exists(json_path):
        json_mtime = datetime.fromtimestamp(os.path.getmtime(json_path))
    
    cur.execute("""
        INSERT INTO sync_runs (source, started_at, json_path, json_mtime)
        VALUES ('lunda', NOW(), %s, %s)
        RETURNING id
    """, (json_path, json_mtime))
    
    sync_run_id = cur.fetchone()[0]
    conn.commit()
    return sync_run_id

def update_sync_run(conn, sync_run_id, stats, error=None):
    """Update sync_runs record with statistics."""
    cur = conn.cursor()
    
    cur.execute("""
        UPDATE sync_runs
        SET finished_at = NOW(),
            tournaments_upsert = %s,
            players_upsert = %s,
            entries_new = %s,
            entries_existing = %s,
            entries_deleted = %s,
            entries_inactivated = %s,
            error = %s
        WHERE id = %s
    """, (
        stats.get('tournaments_upsert', 0),
        stats.get('players_upsert', 0),
        stats.get('entries_new', 0),
        stats.get('entries_existing', 0),
        stats.get('entries_deleted', 0),
        stats.get('entries_inactivated', 0),
        error,
        sync_run_id
    ))
    
    conn.commit()

def archive_past_tournaments(conn, cutoff_time, stats):
    """Archive tournaments that are past (starts_at < cutoff_time). One-time operation."""
    # Ensure connection is alive
    conn = ensure_conn(conn)
    cur = conn.cursor()
    
    try:
        # Check if archived_at column exists
        has_archived_at = check_column_exists(conn, 'tournaments', 'archived_at')
        
        if not has_archived_at:
            print("WARNING: archived_at column not found. Skipping past tournaments archiving.")
            return
        
        # Find past tournaments that are not yet archived
        # Convert cutoff_time to UTC for database comparison (PostgreSQL stores timestamptz in UTC)
        cutoff_utc = cutoff_time.astimezone(timezone.utc)
        
        query = """
            SELECT id, title, location, starts_at
            FROM tournaments
            WHERE archived_at IS NULL
              AND starts_at < %s
            ORDER BY starts_at DESC
        """
        
        cur.execute(query, (cutoff_utc,))
        past_tournaments = cur.fetchall()
        
        archived_count = 0
        for tournament_id, title, location, starts_at in past_tournaments:
            cur.execute("""
                UPDATE tournaments
                SET archived_at = NOW()
                WHERE id = %s AND archived_at IS NULL
            """, (tournament_id,))
            
            if cur.rowcount > 0:
                archived_count += 1
                stats['tournaments_archived'] += 1
        
        conn.commit()
        
        if archived_count > 0:
            print(f"Archived {archived_count} past tournaments (starts_at < {cutoff_time.strftime('%Y-%m-%d %H:%M')} MSK)")
        else:
            print("No past tournaments to archive")
    finally:
        cur.close()

def process_missing_tournaments(conn, processed_tournament_ids, run_started_at, stats):
    """Archive tournaments that are not in current JSON (JSON is source of truth). Returns list of archived tournament IDs."""
    # Ensure connection is alive
    conn = ensure_conn(conn)
    cur = conn.cursor()
    
    try:
        # Check if required columns exist
        has_last_seen = check_column_exists(conn, 'tournaments', 'last_seen_in_source')
        has_archived_at = check_column_exists(conn, 'tournaments', 'archived_at')
        
        # If columns don't exist, skip this functionality
        if not has_last_seen or not has_archived_at:
            print("WARNING: Required columns (last_seen_in_source, archived_at) not found.")
            print("Please run migration 004_fix_tournament_archiving.sql first.")
            return []
        
        # Find tournaments that were NOT seen in this run
        # Archive tournaments where:
        # - archived_at IS NULL (not already archived)
        # - last_seen_in_source IS NULL OR last_seen_in_source < run_started_at (not seen in this run)
        # - id NOT IN processed_tournament_ids (not in current JSON)
        if processed_tournament_ids:
            # Use NOT IN for list of IDs
            placeholders = ','.join(['%s'] * len(processed_tournament_ids))
            query = f"""
                SELECT t.id, t.title, t.location, t.starts_at
                FROM tournaments t
                WHERE t.archived_at IS NULL
                  AND (t.last_seen_in_source IS NULL OR t.last_seen_in_source < %s)
                  AND t.id NOT IN ({placeholders})
            """
            params = [run_started_at] + list(processed_tournament_ids)
        else:
            # If no processed tournaments, archive all that weren't seen
            query = """
                SELECT t.id, t.title, t.location, t.starts_at
                FROM tournaments t
                WHERE t.archived_at IS NULL
                  AND (t.last_seen_in_source IS NULL OR t.last_seen_in_source < %s)
            """
            params = [run_started_at]
        
        cur.execute(query, params)
        missing_tournaments = cur.fetchall()
        
        archived_ids = []
        for tournament_id, title, location, starts_at in missing_tournaments:
            # Archive tournament (don't delete - preserve history)
            cur.execute("""
                UPDATE tournaments
                SET archived_at = NOW()
                WHERE id = %s AND archived_at IS NULL
            """, (tournament_id,))
            
            # Check if tournament was actually updated
            if cur.rowcount > 0:
                stats['tournaments_archived'] += 1
                archived_ids.append(tournament_id)
                print(f"ARCHIVED tournament: id={tournament_id}, title={title}, location={location}")
        
        conn.commit()
        
        # Log summary
        if archived_ids:
            print(f"\nARCHIVING SUMMARY: {len(archived_ids)} tournaments archived")
            print(f"Example archived IDs: {archived_ids[:3]}")
        else:
            print("\nARCHIVING SUMMARY: No tournaments archived (all present in JSON)")
        
        return archived_ids
    finally:
        cur.close()

def main():
    """Main import function."""
    # Log run start
    pid = os.getpid()
    now_local = datetime.now(MSK_TZ)
    print("="*50)
    print(f"RUN START: PID={pid}, Time={now_local.strftime('%Y-%m-%d %H:%M:%S %z')} MSK")
    print("="*50)
    
    # Get paths from env
    database_url = os.getenv("DATABASE_URL")
    json_path = os.getenv("LUNDA_JSON_PATH")
    
    if not database_url:
        print("ERROR: DATABASE_URL not set")
        return 0  # Exit 0 for launchd
    
    if not json_path:
        print("ERROR: LUNDA_JSON_PATH not set")
        return 0  # Exit 0 for launchd
    
    # Load JSON
    print(f"Loading JSON from: {json_path}")
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: File not found: {json_path}")
        return 0  # Exit 0 for launchd
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        return 0  # Exit 0 for launchd
    
    # Get tournaments dict/list
    tournaments_raw = data.get('tournaments', {})
    if not tournaments_raw:
        print("ERROR: No 'tournaments' key in JSON")
        return 0  # Exit 0 for launchd
    
    # Debug output
    print(f"DEBUG: data['tournaments'] type: {type(tournaments_raw).__name__}")
    
    # Extract tournaments list from dict or use as-is if list
    if isinstance(tournaments_raw, dict):
        tournaments_list = list(tournaments_raw.values())
    elif isinstance(tournaments_raw, list):
        tournaments_list = tournaments_raw
    else:
        print(f"ERROR: Unexpected type for tournaments: {type(tournaments_raw)}")
        return 0  # Exit 0 for launchd
    
    print(f"Found {len(tournaments_list)} tournaments in JSON")
    
    # Get global last_updated if available
    global_last_updated = data.get('last_updated')
    
    # Connect to DB
    conn = None
    try:
        conn = get_db_conn()
    except Exception as e:
        print(f"ERROR: Failed to connect to database: {e}")
        return 0  # Exit 0 for launchd
    
    # Create sync run record
    sync_run_id = None
    run_started_at = None
    try:
        sync_run_id = create_sync_run(conn, json_path)
        print(f"Created sync run: id={sync_run_id}")
        
        # Get run_started_at for processing missing tournaments
        cur = conn.cursor()
        cur.execute("SELECT started_at FROM sync_runs WHERE id = %s", (sync_run_id,))
        run_started_at = cur.fetchone()[0]
        cur.close()
    except Exception as e:
        print(f"WARNING: Failed to create sync run: {e}")
        # Continue anyway
        run_started_at = datetime.now()
    
    # Calculate cutoff time: now_msk - grace (grace = 6 hours)
    now_msk = datetime.now(MSK_TZ)
    grace_hours = 6
    cutoff_time = now_msk - timedelta(hours=grace_hours)
    print(f"Cutoff time (MSK): {cutoff_time.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"Tournaments with starts_at < {cutoff_time.strftime('%Y-%m-%d %H:%M')} MSK will be skipped")
    
    # Statistics
    stats = {
        'tournaments_upsert': 0,
        'players_upsert': 0,
        'entries_new': 0,
        'entries_existing': 0,
        'entries_deleted': 0,
        'entries_inactivated': 0,
        'tournaments_archived': 0,
        'tournaments_skipped_past': 0,
        'tournaments_deleted': 0
    }
    
    # Track processed tournament IDs
    processed_tournament_ids = set()
    archived_tournament_ids = []
    
    # Process each tournament (only future tournaments)
    # Each tournament in separate transaction for resilience
    error_occurred = None
    tournament_errors = []
    
    for tournament_data in tournaments_list:
        tournament_info = tournament_data.get('tournament', {})
        title = tournament_info.get('title', 'Unknown')
        location = tournament_info.get('location', 'Unknown')
        
        # Get and normalize starts_at
        starts_at_raw = tournament_data.get('start_datetime')
        starts_at = normalize_msk(starts_at_raw)
        
        # Skip past tournaments (before cutoff)
        if starts_at is None:
            print(f"SKIPPED tournament (no starts_at): {title} at {location}")
            stats['tournaments_skipped_past'] += 1
            continue
        
        if starts_at < cutoff_time:
            print(f"SKIPPED past tournament: {title} at {location} (starts_at={starts_at.strftime('%Y-%m-%d %H:%M')} MSK)")
            stats['tournaments_skipped_past'] += 1
            continue
        
        # Process only future tournaments - each in separate transaction
        print(f"Processing tournament: {title} at {location} (starts_at={starts_at.strftime('%Y-%m-%d %H:%M')} MSK)")
        
        # Ensure connection is alive before processing
        try:
            conn = ensure_conn(conn)
            process_tournament(conn, tournament_data, stats, global_last_updated, processed_tournament_ids, sync_run_id)
            conn.commit()
        except Exception as e:
            # Tournament error - log and continue
            err_msg = str(e)
            tournament_errors.append(f"{title} ({starts_at.strftime('%Y-%m-%d %H:%M') if starts_at else 'no date'}): {err_msg}")
            print(f"TOURNAMENT ERROR: title={title}, starts_at={starts_at.strftime('%Y-%m-%d %H:%M') if starts_at else 'None'}, err={err_msg}")
            safe_rollback(conn)
            # Continue with next tournament
            continue
    
    # Archive past tournaments (one-time, based on cutoff)
    try:
        print("\nArchiving past tournaments (starts_at < cutoff)...")
        conn = ensure_conn(conn)
        archive_past_tournaments(conn, cutoff_time, stats)
    except Exception as e:
        print(f"ERROR archiving past tournaments: {e}")
        if not error_occurred:
            error_occurred = f"Archive error: {str(e)}"
    
    # Process tournaments that are missing from JSON
    archived_tournament_ids = []
    if run_started_at:
        try:
            print("\nProcessing missing tournaments (not in JSON)...")
            conn = ensure_conn(conn)
            archived_tournament_ids = process_missing_tournaments(conn, processed_tournament_ids, run_started_at, stats)
        except Exception as e:
            print(f"ERROR processing missing tournaments: {e}")
            if not error_occurred:
                error_occurred = f"Missing tournaments error: {str(e)}"
    
    # Collect tournament errors into main error_occurred
    if tournament_errors:
        if error_occurred:
            error_occurred += f"; Tournament errors: {len(tournament_errors)}"
        else:
            error_occurred = f"Tournament errors: {len(tournament_errors)}"
        print(f"\nTotal tournament errors: {len(tournament_errors)}")
        if len(tournament_errors) <= 5:
            for err in tournament_errors:
                print(f"  - {err}")
    # Update sync run with statistics
    # Expire old pending entries
    if sync_run_id:
        try:
            conn = ensure_conn(conn)
            cur = conn.cursor()
            cur.execute("""
                UPDATE pending_entries 
                SET status = 'expired'
                WHERE status = 'pending' AND sync_run_id <> %s
            """, (sync_run_id,))
            expired_count = cur.rowcount
            conn.commit()
            cur.close()
            if expired_count > 0:
                print(f"Expired {expired_count} old pending entries")
        except Exception as e:
            print(f"WARNING: Failed to expire old pending entries: {e}")
    
    if sync_run_id:
        try:
            conn = ensure_conn(conn)
            update_sync_run(conn, sync_run_id, stats, error_occurred)
            print(f"Updated sync run: id={sync_run_id}")
        except Exception as e:
            print(f"WARNING: Failed to update sync run: {e}")
    
    # Close connection safely
    if conn and getattr(conn, "closed", 1) == 0:
        try:
            conn.close()
        except Exception as e:
            print(f"WARNING: Failed to close connection: {e}")
    
    # Print statistics
    print("\n" + "="*50)
    print("IMPORT STATISTICS")
    print("="*50)
    print(f"Tournaments UPSERT: {stats['tournaments_upsert']}")
    print(f"Skipped past tournaments: {stats['tournaments_skipped_past']}")
    print(f"Tournaments ARCHIVED: {stats['tournaments_archived']}")
    if stats['tournaments_archived'] > 0:
        print(f"  -> {stats['tournaments_archived']} tournaments were archived (past or not in current JSON)")
        if archived_tournament_ids:
            print(f"  -> Example archived tournament IDs: {archived_tournament_ids[:3]}")
    if stats['tournaments_deleted'] > 0:
        print(f"Tournaments DELETED: {stats['tournaments_deleted']}")
    print(f"Players UPSERT: {stats['players_upsert']}")
    print(f"Entries NEW: {stats['entries_new']}")
    print(f"Entries EXISTING (confirmed): {stats['entries_existing']}")
    print(f"Entries INACTIVATED: {stats['entries_inactivated']}")
    print(f"Entries DELETED: {stats['entries_deleted']}")
    if error_occurred:
        print(f"ERROR: {error_occurred}")
    print("="*50)
    
    # Auto-trigger Telegram notifications in batches
    # Wrap in try/except to not break import
    print("\n" + "="*50)
    print("AUTO-TRIGGERING TELEGRAM NOTIFICATIONS")
    print("="*50)
    
    backend_base_url = os.getenv("BACKEND_BASE_URL", "https://padel-payments.onrender.com")
    batch_limit = 500
    max_iterations = 50
    total_processed = 0
    total_notified = 0
    iteration = 0
    
    print(f"AUTO TG: start batching, limit={batch_limit}")
    
    try:
        while iteration < max_iterations:
            iteration += 1
            endpoint_url = f"{backend_base_url}/admin/process-new-entries?limit={batch_limit}"
            
            try:
                response = requests.post(endpoint_url, timeout=120)
                
                if response.status_code == 200:
                    result = response.json()
                    processed = result.get('processed', 0)
                    notified = result.get('notified', 0)
                    total_processed += processed
                    total_notified += notified
                    
                    print(f"AUTO TG: iter={iteration} status={response.status_code} processed={processed} notified={notified}")
                    
                    # If no entries processed, we're done
                    if processed == 0:
                        break
                else:
                    print(f"AUTO TG: iter={iteration} status={response.status_code} error={response.text[:100]}")
                    # On error, stop batching (but don't fail import)
                    break
                    
            except requests.exceptions.Timeout:
                print(f"AUTO TG: iter={iteration} ERROR: Request timeout (120s)")
                # Stop batching on timeout
                break
            except requests.exceptions.RequestException as e:
                print(f"AUTO TG: iter={iteration} ERROR: Request failed: {e}")
                # Stop batching on request error
                break
            except Exception as e:
                print(f"AUTO TG: iter={iteration} ERROR: Unexpected error: {e}")
                import traceback
                traceback.print_exc()
                # Stop batching on unexpected error
                break
        
        if iteration >= max_iterations:
            print(f"AUTO TG: WARNING: Reached max_iterations={max_iterations}, stopping")
        
        print(f"AUTO TG: done, total_processed={total_processed}, total_notified={total_notified}, iters={iteration}")
        
    except Exception as e:
        print(f"ERROR: Unexpected error in notification batching: {e}")
        import traceback
        traceback.print_exc()
    
    print("="*50)
    
    # Always return 0 (success) for launchd - errors are logged but don't fail the import
    return 0

if __name__ == "__main__":
    exit(main())

