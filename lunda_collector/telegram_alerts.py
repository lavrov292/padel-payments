from __future__ import annotations

import json
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Any

from date_parser import MSK
from player_matcher import normalize_name


DEFAULT_ADMIN_ID = "416485678"


def telegram_configured() -> bool:
    return bool(bot_token() and admin_chat_id())


def bot_token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def admin_chat_id() -> str:
    return (
        os.environ.get("ADMIN_TELEGRAM_ID", "").strip()
        or os.environ.get("ADMIN_CHAT_ID", "").strip()
        or DEFAULT_ADMIN_ID
    )


def send_admin_message(text: str, *, reply_markup: dict[str, Any] | None = None) -> int | None:
    token = bot_token()
    chat_id = admin_chat_id()
    if not token or not chat_id:
        return None
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    result = telegram_request("sendMessage", payload)
    try:
        return int(result["result"]["message_id"])
    except (KeyError, TypeError, ValueError):
        return None


def answer_callback(callback_query_id: str, text: str = "") -> None:
    if callback_query_id:
        telegram_request("answerCallbackQuery", {"callback_query_id": callback_query_id, "text": text})


def edit_message_text(chat_id: int | str, message_id: int, text: str) -> None:
    telegram_request(
        "editMessageText",
        {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "disable_web_page_preview": True,
        },
    )


def telegram_request(method: str, payload: dict[str, Any]) -> dict[str, Any]:
    token = bot_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        parsed = json.loads(response.read().decode("utf-8"))
    if not parsed.get("ok"):
        raise RuntimeError(f"Telegram API error: {parsed}")
    return parsed


def telegram_get_updates(offset: int | None, *, timeout: int = 25) -> list[dict[str, Any]]:
    token = bot_token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not configured")
    params = {"timeout": str(timeout), "allowed_updates": json.dumps(["message", "callback_query"])}
    if offset is not None:
        params["offset"] = str(offset)
    url = f"https://api.telegram.org/bot{token}/getUpdates?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=timeout + 10) as response:
        parsed = json.loads(response.read().decode("utf-8"))
    if not parsed.get("ok"):
        raise RuntimeError(f"Telegram API error: {parsed}")
    return list(parsed.get("result") or [])


def ensure_notification_schema(conn: sqlite3.Connection) -> None:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(pending_players)").fetchall()}
    migrations = {
        "telegram_notified_at": "ALTER TABLE pending_players ADD COLUMN telegram_notified_at TEXT",
        "telegram_message_id": "ALTER TABLE pending_players ADD COLUMN telegram_message_id INTEGER",
        "resolved_at": "ALTER TABLE pending_players ADD COLUMN resolved_at TEXT",
        "resolved_by_telegram_id": "ALTER TABLE pending_players ADD COLUMN resolved_by_telegram_id TEXT",
        "resolution_note": "ALTER TABLE pending_players ADD COLUMN resolution_note TEXT",
    }
    for column, sql in migrations.items():
        if column not in columns:
            conn.execute(sql)
    conn.commit()


def notify_cycle_problem(*, title: str, details: str) -> None:
    if not telegram_configured():
        return
    try:
        send_admin_message(f"⚠️ Lunda collector\n\n{title}\n\n{details}")
    except Exception as exc:
        print(f"telegram notification failed: {exc}", file=sys.stderr, flush=True)


def notify_pending_players(conn: sqlite3.Connection, *, limit: int = 20) -> int:
    ensure_notification_schema(conn)
    if not telegram_configured():
        return 0
    rows = conn.execute(
        """
        SELECT
            pp.id,
            pp.raw_name,
            pp.normalized_name,
            pp.candidates_json,
            pp.created_at,
            t.title,
            t.organizer,
            t.date_label,
            t.time_label,
            t.location
        FROM pending_players pp
        LEFT JOIN tournaments t ON t.id = pp.tournament_id
        WHERE pp.status = 'pending'
          AND pp.telegram_message_id IS NULL
        ORDER BY pp.created_at
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    sent = 0
    for row in rows:
        candidates = parse_candidates(row["candidates_json"])
        try:
            message_id = send_admin_message(
                pending_message(row, candidates),
                reply_markup=pending_keyboard(row["id"], candidates),
            )
        except Exception as exc:
            print(f"telegram pending notification failed: {exc}", file=sys.stderr, flush=True)
            break
        if message_id:
            conn.execute(
                """
                UPDATE pending_players
                SET telegram_message_id = ?,
                    telegram_notified_at = ?
                WHERE id = ?
                """,
                (message_id, now_iso(), row["id"]),
            )
            sent += 1
    conn.commit()
    return sent


def wait_for_pending_resolution(
    conn: sqlite3.Connection,
    *,
    poll_seconds: int = 30,
    log: Any | None = None,
) -> None:
    ensure_notification_schema(conn)
    if not telegram_configured():
        return
    while pending_count(conn) > 0:
        notify_pending_players(conn)
        if log:
            print(f"{now_iso()} waiting for Telegram pending resolution: {pending_count(conn)}", file=log, flush=True)
        time.sleep(max(5, poll_seconds))


def pending_count(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT COUNT(*) FROM pending_players WHERE status = 'pending'").fetchone()
    return int(row[0])


def parse_candidates(raw_json: str | None) -> list[dict[str, Any]]:
    try:
        values = json.loads(raw_json or "[]")
    except json.JSONDecodeError:
        return []
    return [value for value in values if isinstance(value, dict)]


def pending_message(row: sqlite3.Row, candidates: list[dict[str, Any]]) -> str:
    lines = [
        "⚠️ Спорное имя Lunda",
        "",
        f"Имя OCR: {row['raw_name']}",
        f"Турнир: {row['title'] or 'не указан'}",
        f"Когда: {(row['date_label'] or '').strip()} {(row['time_label'] or '').strip()}".strip(),
        f"Организатор: {row['organizer'] or 'не указан'}",
        f"Место: {row['location'] or 'не указано'}",
        "",
        "Выбери, кто это:",
    ]
    if candidates:
        lines.append("")
        for idx, candidate in enumerate(candidates[:5], 1):
            lines.append(f"{idx}. {candidate.get('name')} (dist={candidate.get('dist')})")
    return "\n".join(lines)


def pending_keyboard(pending_id: int, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    rows: list[list[dict[str, str]]] = []
    for candidate in candidates[:5]:
        player_id = candidate.get("player_id")
        name = str(candidate.get("name") or f"player #{player_id}")
        if player_id:
            rows.append(
                [
                    {
                        "text": name[:60],
                        "callback_data": f"lunda:pending:{pending_id}:player:{player_id}",
                    }
                ]
            )
    rows.append([{"text": "🆕 Это новый игрок", "callback_data": f"lunda:pending:{pending_id}:new"}])
    rows.append([{"text": "⏸ Отложить и продолжить", "callback_data": f"lunda:pending:{pending_id}:skip"}])
    return {"inline_keyboard": rows}


def resolve_pending(
    conn: sqlite3.Connection,
    *,
    pending_id: int,
    action: str,
    player_id: int | None = None,
    resolved_by: str = "",
) -> str:
    ensure_notification_schema(conn)
    pending = conn.execute(
        """
        SELECT pp.*, t.title
        FROM pending_players pp
        LEFT JOIN tournaments t ON t.id = pp.tournament_id
        WHERE pp.id = ?
        """,
        (pending_id,),
    ).fetchone()
    if not pending:
        return "pending not found"
    if pending["status"] != "pending":
        return f"already {pending['status']}"

    if action == "player":
        if not player_id:
            raise ValueError("player_id is required")
        player = conn.execute("SELECT id, display_name FROM players WHERE id = ?", (player_id,)).fetchone()
        if not player:
            raise ValueError(f"player #{player_id} not found")
        apply_player_resolution(conn, pending, int(player["id"]), "manual_existing")
        add_alias(conn, int(player["id"]), pending["raw_name"], pending["normalized_name"])
        status = "resolved"
        note = f"linked to {player['display_name']}"
    elif action == "new":
        player_id = create_player_if_needed(conn, pending["raw_name"], pending["normalized_name"])
        apply_player_resolution(conn, pending, player_id, "manual_new")
        status = "resolved"
        note = "created new player"
    elif action == "skip":
        status = "snoozed"
        note = "snoozed by admin"
    else:
        raise ValueError(f"unknown action: {action}")

    conn.execute(
        """
        UPDATE pending_players
        SET status = ?,
            resolved_player_id = COALESCE(?, resolved_player_id),
            resolved_at = ?,
            resolved_by_telegram_id = ?,
            resolution_note = ?
        WHERE id = ?
        """,
        (status, player_id, now_iso(), resolved_by, note, pending_id),
    )
    conn.commit()
    return note


def apply_player_resolution(conn: sqlite3.Connection, pending: sqlite3.Row, player_id: int, status: str) -> None:
    tournament_id = pending["tournament_id"]
    normalized_name = pending["normalized_name"]
    raw_name = pending["raw_name"]
    participant_key = f"player:{player_id}"
    for table in ("current_participants", "final_participations"):
        existing = conn.execute(
            f"SELECT id FROM {table} WHERE tournament_id = ? AND participant_key = ?",
            (tournament_id, participant_key),
        ).fetchone()
        if existing:
            conn.execute(
                f"""
                DELETE FROM {table}
                WHERE tournament_id = ?
                  AND normalized_name = ?
                  AND participant_key LIKE 'pending:%'
                """,
                (tournament_id, normalized_name),
            )
        else:
            conn.execute(
                f"""
                UPDATE {table}
                SET participant_key = ?,
                    player_id = ?,
                    raw_name = ?,
                    normalized_name = ?,
                    resolve_status = ?
                WHERE tournament_id = ?
                  AND normalized_name = ?
                  AND participant_key LIKE 'pending:%'
                """,
                (participant_key, player_id, raw_name, normalized_name, status, tournament_id, normalized_name),
            )


def add_alias(conn: sqlite3.Connection, player_id: int, raw_name: str, normalized_name: str) -> None:
    conn.execute(
        """
        INSERT INTO player_aliases (player_id, alias_name, normalized_alias, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(normalized_alias) DO UPDATE SET
            player_id = excluded.player_id,
            alias_name = excluded.alias_name
        """,
        (player_id, raw_name.strip(), normalized_name, now_iso()),
    )


def create_player_if_needed(conn: sqlite3.Connection, raw_name: str, normalized_name: str) -> int:
    normalized = normalized_name or normalize_name(raw_name)
    existing = conn.execute("SELECT id FROM players WHERE normalized_name = ?", (normalized,)).fetchone()
    if existing:
        return int(existing["id"])
    conn.execute(
        """
        INSERT INTO players (display_name, normalized_name, first_seen_at, last_seen_at)
        VALUES (?, ?, ?, ?)
        """,
        (raw_name.strip(), normalized, now_iso(), now_iso()),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def now_iso() -> str:
    return datetime.now(MSK).isoformat(timespec="seconds")
