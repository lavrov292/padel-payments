from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from clubs import normalize_club_name
from date_parser import MSK, iso_or_empty, parse_tournament_datetime
from player_matcher import normalize_name, resolve_player
from visible_cards import build_merge_key


DEFAULT_DB_PATH = Path("work/lunda_collector.sqlite3")


def connect(db_path: str | Path = DEFAULT_DB_PATH) -> sqlite3.Connection:
    path = Path(db_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sync_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL DEFAULT 'running',
            stats_json TEXT NOT NULL DEFAULT '{}',
            error TEXT
        );

        CREATE TABLE IF NOT EXISTS tournaments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            identity_key TEXT NOT NULL UNIQUE,
            title TEXT,
            organizer TEXT,
            date_label TEXT,
            time_label TEXT,
            tournament_date TEXT,
            starts_at TEXT,
            ends_at TEXT,
            location TEXT,
            skill_level TEXT,
            format TEXT,
            price_label TEXT,
            price_value INTEGER,
            participants_current INTEGER,
            participants_capacity INTEGER,
            participants_unit TEXT,
            source_status TEXT NOT NULL DEFAULT 'active',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            finalized_at TEXT,
            cancelled_at TEXT,
            raw_json TEXT NOT NULL DEFAULT '{}'
        );

        CREATE TABLE IF NOT EXISTS tournament_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
            observed_at TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT 'list',
            is_complete INTEGER NOT NULL DEFAULT 0,
            card_json TEXT NOT NULL,
            screen_index INTEGER,
            FOREIGN KEY(run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS participant_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
            observed_at TEXT NOT NULL,
            participant_count INTEGER NOT NULL,
            participants_json TEXT NOT NULL,
            raw_json TEXT NOT NULL DEFAULT '{}',
            FOREIGN KEY(run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            display_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL UNIQUE,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS player_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL REFERENCES players(id) ON DELETE CASCADE,
            alias_name TEXT NOT NULL,
            normalized_alias TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pending_players (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER,
            tournament_id INTEGER REFERENCES tournaments(id) ON DELETE CASCADE,
            raw_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            candidates_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            resolved_player_id INTEGER REFERENCES players(id) ON DELETE SET NULL,
            FOREIGN KEY(run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS current_participants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
            participant_key TEXT NOT NULL,
            player_id INTEGER REFERENCES players(id) ON DELETE SET NULL,
            raw_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            resolve_status TEXT NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            last_run_id INTEGER,
            UNIQUE(tournament_id, participant_key),
            FOREIGN KEY(last_run_id) REFERENCES sync_runs(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS final_participations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tournament_id INTEGER NOT NULL REFERENCES tournaments(id) ON DELETE CASCADE,
            participant_key TEXT NOT NULL,
            player_id INTEGER REFERENCES players(id) ON DELETE SET NULL,
            raw_name TEXT NOT NULL,
            normalized_name TEXT NOT NULL,
            resolve_status TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            finalized_at TEXT NOT NULL,
            UNIQUE(tournament_id, participant_key)
        );

        CREATE INDEX IF NOT EXISTS idx_tournaments_starts_at ON tournaments(starts_at);
        CREATE INDEX IF NOT EXISTS idx_tournaments_date ON tournaments(tournament_date);
        CREATE INDEX IF NOT EXISTS idx_tournaments_filters ON tournaments(organizer, location, skill_level, format);
        CREATE INDEX IF NOT EXISTS idx_current_active ON current_participants(tournament_id, active);
        CREATE INDEX IF NOT EXISTS idx_final_player ON final_participations(player_id);
        CREATE INDEX IF NOT EXISTS idx_pending_status ON pending_players(status);
        """
    )
    conn.commit()


def start_run(conn: sqlite3.Connection, kind: str, *, now_iso: str | None = None) -> int:
    now_iso = now_iso or _now_iso()
    conn.execute(
        """
        INSERT INTO sync_runs (kind, started_at, status)
        VALUES (?, ?, 'running')
        """,
        (kind, now_iso),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def finish_run(
    conn: sqlite3.Connection,
    run_id: int,
    *,
    status: str = "ok",
    stats: dict[str, Any] | None = None,
    error: str | None = None,
    now_iso: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE sync_runs
        SET finished_at = ?, status = ?, stats_json = ?, error = ?
        WHERE id = ?
        """,
        (now_iso or _now_iso(), status, json.dumps(stats or {}, ensure_ascii=False), error, run_id),
    )
    conn.commit()


def upsert_tournament_from_card(
    conn: sqlite3.Connection,
    card: dict[str, Any],
    *,
    run_id: int | None = None,
    observed_at: str | None = None,
    source: str = "list",
) -> int:
    observed_at = observed_at or _now_iso()
    prepared = prepare_tournament_card(card, now_iso=observed_at)
    identity_key = prepared["identity_key"]

    existing = conn.execute(
        "SELECT id FROM tournaments WHERE identity_key = ?",
        (identity_key,),
    ).fetchone()

    if existing:
        tournament_id = int(existing["id"])
        conn.execute(
            """
            UPDATE tournaments
            SET title = COALESCE(NULLIF(?, ''), title),
                organizer = COALESCE(NULLIF(?, ''), organizer),
                date_label = COALESCE(NULLIF(?, ''), date_label),
                time_label = COALESCE(NULLIF(?, ''), time_label),
                tournament_date = COALESCE(NULLIF(?, ''), tournament_date),
                starts_at = COALESCE(NULLIF(?, ''), starts_at),
                ends_at = COALESCE(NULLIF(?, ''), ends_at),
                location = COALESCE(NULLIF(?, ''), location),
                skill_level = COALESCE(NULLIF(?, ''), skill_level),
                format = COALESCE(NULLIF(?, ''), format),
                price_label = COALESCE(NULLIF(?, ''), price_label),
                price_value = COALESCE(?, price_value),
                participants_current = COALESCE(?, participants_current),
                participants_capacity = COALESCE(?, participants_capacity),
                participants_unit = COALESCE(NULLIF(?, ''), participants_unit),
                source_status = CASE
                    WHEN source_status = 'finalized' THEN source_status
                    ELSE 'active'
                END,
                last_seen_at = ?,
                raw_json = ?
            WHERE id = ?
            """,
            (
                prepared["title"],
                prepared["organizer"],
                prepared["date_label"],
                prepared["time_label"],
                prepared["tournament_date"],
                prepared["starts_at"],
                prepared["ends_at"],
                prepared["location"],
                prepared["skill_level"],
                prepared["format"],
                prepared["price_label"],
                prepared["price_value"],
                prepared["participants_current"],
                prepared["participants_capacity"],
                prepared["participants_unit"],
                observed_at,
                json.dumps(card, ensure_ascii=False),
                tournament_id,
            ),
        )
    else:
        conn.execute(
            """
            INSERT INTO tournaments (
                identity_key, title, organizer, date_label, time_label,
                tournament_date, starts_at, ends_at, location, skill_level,
                format, price_label, price_value, participants_current,
                participants_capacity, participants_unit, first_seen_at,
                last_seen_at, raw_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                identity_key,
                prepared["title"],
                prepared["organizer"],
                prepared["date_label"],
                prepared["time_label"],
                prepared["tournament_date"],
                prepared["starts_at"],
                prepared["ends_at"],
                prepared["location"],
                prepared["skill_level"],
                prepared["format"],
                prepared["price_label"],
                prepared["price_value"],
                prepared["participants_current"],
                prepared["participants_capacity"],
                prepared["participants_unit"],
                observed_at,
                observed_at,
                json.dumps(card, ensure_ascii=False),
            ),
        )
        tournament_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

    conn.execute(
        """
        INSERT INTO tournament_observations (
            run_id, tournament_id, observed_at, source, is_complete,
            card_json, screen_index
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            tournament_id,
            observed_at,
            source,
            1 if card.get("is_complete") else 0,
            json.dumps(card, ensure_ascii=False),
            card.get("screen_index"),
        ),
    )
    conn.commit()
    return tournament_id


def prepare_tournament_card(card: dict[str, Any], *, now_iso: str | None = None) -> dict[str, Any]:
    now = _parse_iso_datetime(now_iso) if now_iso else datetime.now(MSK)
    parsed_time = parse_tournament_datetime(str(card.get("date", "")), str(card.get("time", "")), now=now)

    title = _clean_text(card.get("title", ""))
    organizer = _clean_text(card.get("organizer", ""))
    location = normalize_club_name(_clean_text(card.get("location", "")))
    skill_level = _clean_text(card.get("skill_level", ""))
    format_value = _clean_text(card.get("format", ""))
    price_label = _clean_text(card.get("price", ""))

    prepared = {
        "identity_key": build_tournament_identity_key(card, parsed_time.starts_at),
        "title": title,
        "organizer": organizer,
        "date_label": _clean_text(card.get("date", "")),
        "time_label": _clean_text(card.get("time", "")),
        "tournament_date": iso_or_empty(parsed_time.tournament_date),
        "starts_at": iso_or_empty(parsed_time.starts_at),
        "ends_at": iso_or_empty(parsed_time.ends_at),
        "location": location,
        "skill_level": skill_level,
        "format": format_value,
        "price_label": price_label,
        "price_value": parse_price_value(price_label),
        "participants_current": _int_or_none(card.get("participants_current")),
        "participants_capacity": _int_or_none(card.get("participants_capacity")),
        "participants_unit": _clean_text(card.get("participants_unit", "")),
    }
    return prepared


def is_persistable_tournament_card(card: dict[str, Any]) -> bool:
    return bool(card.get("is_complete"))


def is_persistable_schedule_card(card: dict[str, Any]) -> bool:
    required_fields = ("title", "organizer", "date", "time", "location")
    return all(str(card.get(field, "")).strip() for field in required_fields)


def build_tournament_identity_key(card: dict[str, Any], starts_at: datetime | None = None) -> str:
    organizer = normalize_identity_part(card.get("organizer", ""))
    location = normalize_identity_part(card.get("location", ""))
    title = normalize_identity_part(card.get("title", ""))
    start_key = starts_at.isoformat() if starts_at else ""

    if organizer and start_key and location:
        return "|".join(["v1", organizer, start_key, location])
    if organizer and start_key:
        return "|".join(["v1", organizer, start_key, title])

    old_key = build_merge_key(card)
    if old_key:
        return f"v1|legacy|{old_key}"

    fallback = "|".join(
        normalize_identity_part(card.get(field, ""))
        for field in ("title", "organizer", "date", "time", "location")
    )
    return f"v1|fallback|{fallback}"


def record_participant_snapshot(
    conn: sqlite3.Connection,
    tournament_id: int,
    participant_names: list[str],
    *,
    run_id: int | None = None,
    observed_at: str | None = None,
    raw: dict[str, Any] | None = None,
) -> dict[str, int]:
    observed_at = observed_at or _now_iso()
    cleaned_names = _unique_clean_names(participant_names)

    conn.execute(
        """
        INSERT INTO participant_snapshots (
            run_id, tournament_id, observed_at, participant_count,
            participants_json, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            tournament_id,
            observed_at,
            len(cleaned_names),
            json.dumps(cleaned_names, ensure_ascii=False),
            json.dumps(raw or {}, ensure_ascii=False),
        ),
    )

    seen_keys: set[str] = set()
    stats = {"seen": len(cleaned_names), "resolved": 0, "pending": 0, "new": 0}

    for raw_name in cleaned_names:
        resolution = resolve_player(
            conn,
            raw_name,
            run_id=run_id,
            tournament_id=tournament_id,
            now_iso=observed_at,
        )
        participant_key = f"player:{resolution.player_id}" if resolution.player_id else f"pending:{resolution.normalized_name}"
        seen_keys.add(participant_key)
        if resolution.status == "new_player":
            stats["new"] += 1
        elif resolution.status == "fuzzy_pending":
            stats["pending"] += 1
        elif resolution.player_id:
            stats["resolved"] += 1

        existing = conn.execute(
            """
            SELECT id
            FROM current_participants
            WHERE tournament_id = ? AND participant_key = ?
            """,
            (tournament_id, participant_key),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE current_participants
                SET player_id = ?,
                    raw_name = ?,
                    normalized_name = ?,
                    resolve_status = ?,
                    active = 1,
                    last_seen_at = ?,
                    last_run_id = ?
                WHERE id = ?
                """,
                (
                    resolution.player_id,
                    raw_name,
                    resolution.normalized_name,
                    resolution.status,
                    observed_at,
                    run_id,
                    int(existing["id"]),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO current_participants (
                    tournament_id, participant_key, player_id, raw_name,
                    normalized_name, resolve_status, active, first_seen_at,
                    last_seen_at, last_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
                """,
                (
                    tournament_id,
                    participant_key,
                    resolution.player_id,
                    raw_name,
                    resolution.normalized_name,
                    resolution.status,
                    observed_at,
                    observed_at,
                    run_id,
                ),
            )

    if seen_keys:
        placeholders = ",".join("?" for _ in seen_keys)
        conn.execute(
            f"""
            UPDATE current_participants
            SET active = 0,
                last_run_id = ?
            WHERE tournament_id = ?
              AND active = 1
              AND participant_key NOT IN ({placeholders})
            """,
            [run_id, tournament_id, *sorted(seen_keys)],
        )

    conn.commit()
    return stats


def mark_tournament_cancelled(
    conn: sqlite3.Connection,
    tournament_id: int,
    *,
    observed_at: str | None = None,
) -> None:
    observed_at = observed_at or _now_iso()
    conn.execute(
        """
        UPDATE tournaments
        SET source_status = 'cancelled',
            cancelled_at = ?,
            last_seen_at = ?
        WHERE id = ?
        """,
        (observed_at, observed_at, tournament_id),
    )
    conn.commit()


def mark_tournament_missing(
    conn: sqlite3.Connection,
    tournament_id: int,
    *,
    observed_at: str | None = None,
) -> None:
    observed_at = observed_at or _now_iso()
    conn.execute(
        """
        UPDATE tournaments
        SET source_status = 'missing',
            last_seen_at = ?
        WHERE id = ?
        """,
        (observed_at, tournament_id),
    )
    conn.commit()


def finalize_due_tournaments(
    conn: sqlite3.Connection,
    *,
    now: datetime | None = None,
    grace_minutes: int = 0,
) -> dict[str, int]:
    current = now or datetime.now(MSK)
    if current.tzinfo is None:
        current = current.replace(tzinfo=MSK)
    cutoff = current - timedelta(minutes=grace_minutes)
    cutoff_iso = cutoff.isoformat()
    finalized_at = current.isoformat(timespec="seconds")

    rows = conn.execute(
        """
        SELECT id
        FROM tournaments
        WHERE source_status = 'active'
          AND starts_at IS NOT NULL
          AND starts_at != ''
          AND starts_at <= ?
          AND EXISTS (
              SELECT 1
              FROM current_participants cp
              WHERE cp.tournament_id = tournaments.id
                AND cp.active = 1
          )
        ORDER BY starts_at
        """,
        (cutoff_iso,),
    ).fetchall()

    stats = {"tournaments_finalized": 0, "participants_finalized": 0}
    for row in rows:
        tournament_id = int(row["id"])
        participants = conn.execute(
            """
            SELECT participant_key, player_id, raw_name, normalized_name,
                   resolve_status, first_seen_at, last_seen_at
            FROM current_participants
            WHERE tournament_id = ? AND active = 1
            """,
            (tournament_id,),
        ).fetchall()
        for participant in participants:
            conn.execute(
                """
                INSERT INTO final_participations (
                    tournament_id, participant_key, player_id, raw_name,
                    normalized_name, resolve_status, first_seen_at,
                    last_seen_at, finalized_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tournament_id, participant_key) DO UPDATE SET
                    player_id = excluded.player_id,
                    raw_name = excluded.raw_name,
                    normalized_name = excluded.normalized_name,
                    resolve_status = excluded.resolve_status,
                    last_seen_at = excluded.last_seen_at,
                    finalized_at = excluded.finalized_at
                """,
                (
                    tournament_id,
                    participant["participant_key"],
                    participant["player_id"],
                    participant["raw_name"],
                    participant["normalized_name"],
                    participant["resolve_status"],
                    participant["first_seen_at"],
                    participant["last_seen_at"],
                    finalized_at,
                ),
            )
            stats["participants_finalized"] += 1

        conn.execute(
            """
            UPDATE tournaments
            SET source_status = 'finalized',
                finalized_at = ?
            WHERE id = ?
            """,
            (finalized_at, tournament_id),
        )
        stats["tournaments_finalized"] += 1

    conn.commit()
    return stats


def tournament_summary(conn: sqlite3.Connection) -> dict[str, int]:
    def count(sql: str, params: tuple[Any, ...] = ()) -> int:
        return int(conn.execute(sql, params).fetchone()[0])

    return {
        "tournaments": count("SELECT COUNT(*) FROM tournaments"),
        "active_tournaments": count("SELECT COUNT(*) FROM tournaments WHERE source_status = 'active'"),
        "finalized_tournaments": count("SELECT COUNT(*) FROM tournaments WHERE source_status = 'finalized'"),
        "cancelled_tournaments": count("SELECT COUNT(*) FROM tournaments WHERE source_status = 'cancelled'"),
        "missing_tournaments": count("SELECT COUNT(*) FROM tournaments WHERE source_status = 'missing'"),
        "players": count("SELECT COUNT(*) FROM players"),
        "current_participants": count("SELECT COUNT(*) FROM current_participants WHERE active = 1"),
        "final_participations": count("SELECT COUNT(*) FROM final_participations"),
        "pending_players": count("SELECT COUNT(*) FROM pending_players WHERE status = 'pending'"),
    }


def player_rankings(
    conn: sqlite3.Connection,
    *,
    date_from: str = "",
    date_to: str = "",
    organizer: str = "",
    location: str = "",
    skill_level: str = "",
    format_value: str = "",
    limit: int = 200,
) -> list[sqlite3.Row]:
    filters = []
    params: list[Any] = []
    if date_from:
        filters.append("t.tournament_date >= ?")
        params.append(date_from)
    if date_to:
        filters.append("t.tournament_date <= ?")
        params.append(date_to)
    if organizer:
        filters.append("LOWER(t.organizer) LIKE ?")
        params.append(f"%{organizer.lower()}%")
    if location:
        filters.append("LOWER(t.location) LIKE ?")
        params.append(f"%{location.lower()}%")
    if skill_level:
        filters.append("LOWER(t.skill_level) LIKE ?")
        params.append(f"%{skill_level.lower()}%")
    if format_value:
        filters.append("LOWER(t.format) LIKE ?")
        params.append(f"%{format_value.lower()}%")

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    params.append(limit)
    return conn.execute(
        f"""
        SELECT
            COALESCE(p.display_name, fp.raw_name) AS player_name,
            fp.normalized_name,
            COUNT(*) AS tournament_count,
            GROUP_CONCAT(DISTINCT t.location) AS locations,
            GROUP_CONCAT(DISTINCT t.organizer) AS organizers,
            GROUP_CONCAT(DISTINCT t.skill_level) AS levels
        FROM final_participations fp
        JOIN tournaments t ON t.id = fp.tournament_id
        LEFT JOIN players p ON p.id = fp.player_id
        {where_sql}
        GROUP BY COALESCE(fp.player_id, fp.participant_key), fp.normalized_name
        ORDER BY tournament_count DESC, player_name ASC
        LIMIT ?
        """,
        params,
    ).fetchall()


def schedule_rows(
    conn: sqlite3.Connection,
    *,
    date_from: str = "",
    date_to: str = "",
    organizer: str = "",
    location: str = "",
    skill_level: str = "",
    format_value: str = "",
    include_cancelled: bool = False,
) -> list[sqlite3.Row]:
    filters = []
    params: list[Any] = []
    if not include_cancelled:
        filters.append("source_status != 'cancelled'")
    if date_from:
        filters.append("tournament_date >= ?")
        params.append(date_from)
    if date_to:
        filters.append("tournament_date <= ?")
        params.append(date_to)
    if organizer:
        filters.append("LOWER(organizer) LIKE ?")
        params.append(f"%{organizer.lower()}%")
    if location:
        filters.append("LOWER(location) LIKE ?")
        params.append(f"%{location.lower()}%")
    if skill_level:
        filters.append("LOWER(skill_level) LIKE ?")
        params.append(f"%{skill_level.lower()}%")
    if format_value:
        filters.append("LOWER(format) LIKE ?")
        params.append(f"%{format_value.lower()}%")

    where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
    return conn.execute(
        f"""
        SELECT *
        FROM tournaments
        {where_sql}
        ORDER BY COALESCE(starts_at, tournament_date, date_label), organizer, location
        """,
        params,
    ).fetchall()


def find_tournament_id(
    conn: sqlite3.Connection,
    *,
    identity_key: str = "",
    title: str = "",
    organizer: str = "",
    date_label: str = "",
    time_label: str = "",
) -> int | None:
    if identity_key:
        row = conn.execute("SELECT id FROM tournaments WHERE identity_key = ?", (identity_key,)).fetchone()
        return int(row["id"]) if row else None

    filters = []
    params: list[Any] = []
    if title:
        filters.append("LOWER(title) LIKE ?")
        params.append(f"%{title.lower()}%")
    if organizer:
        filters.append("LOWER(organizer) LIKE ?")
        params.append(f"%{organizer.lower()}%")
    if date_label:
        filters.append("LOWER(date_label) LIKE ?")
        params.append(f"%{date_label.lower()}%")
    if time_label:
        filters.append("LOWER(time_label) LIKE ?")
        params.append(f"%{time_label.lower()}%")
    if not filters:
        return None

    row = conn.execute(
        f"""
        SELECT id
        FROM tournaments
        WHERE {' AND '.join(filters)}
        ORDER BY starts_at DESC, last_seen_at DESC
        LIMIT 1
        """,
        params,
    ).fetchone()
    return int(row["id"]) if row else None


def parse_price_value(price_label: str) -> int | None:
    if not price_label:
        return None
    match = re.search(r"(\d[\d\s]*)", price_label)
    if not match:
        return None
    try:
        return int(match.group(1).replace(" ", ""))
    except ValueError:
        return None


def normalize_identity_part(value: Any) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", normalize_name(str(value or "")))


def _unique_clean_names(values: list[str]) -> list[str]:
    seen: set[str] = set()
    cleaned: list[str] = []
    for value in values:
        text = _clean_text(value)
        key = normalize_name(text)
        if not text or not key or key in seen:
            continue
        seen.add(key)
        cleaned.append(text)
    return cleaned


def _clean_text(value: Any) -> str:
    text = str(value or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _now_iso() -> str:
    return datetime.now(MSK).isoformat(timespec="seconds")


def _parse_iso_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(MSK)
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.now(MSK)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=MSK)
    return parsed.astimezone(MSK)
