#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from invite_players import (
    create_invite_job,
    init_invite_db,
    list_active_my_tournaments,
    read_player_names_from_xlsx,
)
from player_audit import (
    add_player_name_blacklist,
    audit_item_candidates,
    cleanup_blacklisted_players,
    init_player_audit_db,
    load_open_audit_items,
)
from storage import connect, init_db
from telegram_alerts import (
    admin_chat_id,
    answer_callback,
    edit_message_text,
    notify_cycle_problem,
    resolve_pending,
    send_admin_message,
    telegram_configured,
    telegram_get_updates,
    telegram_request,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram admin bot for Lunda collector")
    parser.add_argument("--db", default=os.environ.get("LUNDA_DB_PATH", ""))
    parser.add_argument("--env-file", default="/opt/lunda-collector/app/.env")
    args = parser.parse_args()

    load_env_file(Path(args.env_file))
    if not args.db:
        print("LUNDA_DB_PATH or --db is required", file=sys.stderr)
        return 2
    if not telegram_configured():
        print("TELEGRAM_BOT_TOKEN is required", file=sys.stderr)
        return 2

    conn = connect(args.db)
    init_db(conn)
    init_invite_db(conn)
    init_invite_bot_db(conn)
    init_player_audit_db(conn)
    telegram_request("deleteWebhook", {"drop_pending_updates": False})
    offset: int | None = None
    print(f"Telegram admin bot started for admin {admin_chat_id()}", flush=True)
    notify_cycle_problem(title="Telegram admin bot started", details="Бот готов принимать решения по спорным именам.")

    while True:
        try:
            updates = telegram_get_updates(offset, timeout=25)
            for update in updates:
                offset = int(update["update_id"]) + 1
                handle_update(conn, update)
        except Exception as exc:
            print(f"telegram bot error: {exc}", file=sys.stderr, flush=True)
            time.sleep(5)


def handle_update(conn, update: dict) -> None:
    callback = update.get("callback_query")
    if callback:
        handle_callback(conn, callback)
        return
    message = update.get("message") or {}
    text = str(message.get("text") or "")
    chat = message.get("chat") or {}
    user = message.get("from") or {}
    if str(user.get("id") or chat.get("id") or "") != str(admin_chat_id()):
        return
    if text.startswith("/status"):
        rows = conn.execute("SELECT COUNT(*) FROM pending_players WHERE status = 'pending'").fetchone()[0]
        audit_rows = conn.execute("SELECT COUNT(*) FROM player_name_audit WHERE status = 'open'").fetchone()[0]
        notify_cycle_problem(title="Lunda status", details=f"Pending names: {rows}\nAudit names: {audit_rows}")
        return
    if text.startswith("/audit"):
        removed = cleanup_blacklisted_players(conn)
        sent = notify_audit_items(conn)
        notify_cycle_problem(title="Lunda audit", details=f"Audit messages sent: {sent}\nBlacklisted removed: {removed}")
        return
    if text.startswith("/invite"):
        send_invite_tournament_picker(conn)
        return

    document = message.get("document") or {}
    if document:
        handle_invite_document(conn, str(user.get("id") or chat.get("id") or ""), document)


def init_invite_bot_db(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS invite_bot_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            my_tournament_id INTEGER NOT NULL,
            upload_path TEXT,
            names_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'waiting_file',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            job_id INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_bot_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_ids_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            telegram_message_id INTEGER,
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            resolved_by_telegram_id TEXT,
            resolution_note TEXT
        )
        """
    )
    conn.commit()


def handle_callback(conn, callback: dict) -> None:
    user = callback.get("from") or {}
    user_id = str(user.get("id") or "")
    callback_id = str(callback.get("id") or "")
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    data = str(callback.get("data") or "")

    if user_id != str(admin_chat_id()):
        safe_answer_callback(callback_id, "Недоступно")
        return

    if data.startswith("lunda:invite:tournament:"):
        tournament_id = int(data.rsplit(":", 1)[-1])
        session_id = create_invite_session(conn, user_id=user_id, tournament_id=tournament_id)
        safe_answer_callback(callback_id, "Жду Excel")
        tournament_label = invite_tournament_label(conn, tournament_id)
        send_admin_message(
            f"✅ Турнир выбран\n{tournament_label}\n\nТеперь отправь Excel-файл с именами игроков в первом столбце."
        )
        print(f"invite session {session_id}: tournament {tournament_id} selected", flush=True)
        return

    if data.startswith("lunda:invite:start:"):
        session_id = int(data.rsplit(":", 1)[-1])
        session = get_invite_session(conn, session_id=session_id, user_id=user_id)
        if not session or session["status"] != "ready":
            safe_answer_callback(callback_id, "Задача уже неактуальна")
            return
        names = json.loads(session["names_json"] or "[]")
        job_id = create_invite_job(conn, my_tournament_id=int(session["my_tournament_id"]), player_names=names)
        conn.execute(
            "UPDATE invite_bot_sessions SET status='queued', job_id=?, updated_at=? WHERE id=?",
            (job_id, now_text(), session_id),
        )
        conn.commit()
        safe_answer_callback(callback_id, "В очереди")
        text = f"✅ Хорошо, принял.\nЗадача #{job_id} в очереди.\nИгроков: {len(names)}.\nОтправка начнется в 23:30."
        send_admin_message(text)
        print(f"invite session {session_id}: job {job_id} queued", flush=True)
        return

    if data.startswith("lunda:invite:cancel:"):
        session_id = int(data.rsplit(":", 1)[-1])
        conn.execute(
            "UPDATE invite_bot_sessions SET status='cancelled', updated_at=? WHERE id=? AND user_id=?",
            (now_text(), session_id, user_id),
        )
        conn.commit()
        safe_answer_callback(callback_id, "Отменено")
        send_admin_message("❌ Приглашение отменено.")
        return

    if data.startswith("lunda:audit:"):
        handle_audit_callback(conn, callback_id=callback_id, user_id=user_id, chat_id=chat_id, message_id=message_id, data=data)
        return

    if data.startswith("lunda:auditbulk:"):
        handle_audit_bulk_callback(conn, callback_id=callback_id, user_id=user_id, chat_id=chat_id, message_id=message_id, data=data)
        return

    parts = data.split(":")
    if len(parts) < 4 or parts[:2] != ["lunda", "pending"]:
        safe_answer_callback(callback_id, "Неизвестная команда")
        return

    pending_id = int(parts[2])
    action = parts[3]
    player_id = int(parts[4]) if action == "player" and len(parts) > 4 else None
    note = resolve_pending(conn, pending_id=pending_id, action=action, player_id=player_id, resolved_by=user_id)
    safe_answer_callback(callback_id, "Готово")
    if chat_id and message_id:
        edit_message_text(chat_id, int(message_id), f"✅ Решено: pending #{pending_id}\n{note}")


def send_invite_tournament_picker(conn) -> None:
    rows = list_active_my_tournaments(conn, limit=30)
    if not rows:
        notify_cycle_problem(
            title="Приглашения",
            details="Пока нет сохраненных моих турниров. Сначала нужно запустить collect-my-tournaments.",
        )
        return
    keyboard = []
    lines = ["Выбери турнир для приглашений:"]
    for row in rows:
        label = f"{_short_datetime(row['starts_at'], row['time_label'])} | {row['location']} | {row['participants_label'] or ''}".strip()
        lines.append(f"#{row['id']} {label}")
        keyboard.append([{"text": label[:60], "callback_data": f"lunda:invite:tournament:{row['id']}"}])
    telegram_request(
        "sendMessage",
        {
            "chat_id": admin_chat_id(),
            "text": "\n".join(lines),
            "reply_markup": {"inline_keyboard": keyboard},
            "disable_web_page_preview": True,
        },
    )


def safe_answer_callback(callback_id: str, text: str = "") -> None:
    try:
        answer_callback(callback_id, text)
    except Exception as exc:
        print(f"telegram answerCallbackQuery failed: {exc}", file=sys.stderr, flush=True)


def notify_audit_items(conn, *, limit: int = 20) -> int:
    rows = load_open_audit_items(conn, limit=limit)
    confident, review = split_audit_rows(rows)
    sent = 0
    if confident:
        message_id = send_admin_message(
            audit_bulk_message(confident),
            reply_markup=audit_bulk_keyboard(conn, confident),
        )
        if message_id:
            ids = [int(row["id"]) for row in confident]
            conn.execute(
                "UPDATE audit_bot_batches SET telegram_message_id=? WHERE id=(SELECT MAX(id) FROM audit_bot_batches)",
                (message_id,),
            )
            for audit_id in ids:
                conn.execute("UPDATE player_name_audit SET telegram_message_id = ? WHERE id = ?", (message_id, audit_id))
            sent += 1
    for row in review:
        message_id = send_admin_message(
            audit_message(conn, row),
            reply_markup=audit_keyboard(int(row["id"]), audit_item_candidates(conn, row)),
        )
        if message_id:
            conn.execute(
                "UPDATE player_name_audit SET telegram_message_id = ? WHERE id = ?",
                (message_id, int(row["id"])),
            )
            sent += 1
    conn.commit()
    return sent


def split_audit_rows(rows) -> tuple[list, list]:
    confident = []
    review = []
    for row in rows:
        if audit_row_is_confident_garbage(row):
            confident.append(row)
        else:
            review.append(row)
    return confident, review


def audit_row_is_confident_garbage(row) -> bool:
    reasons = set(json.loads(row["heuristic_reasons"] or "[]"))
    if reasons & {"known_ui_or_description_phrase", "normalized_known_bad_phrase", "too_many_words", "contains_digit"}:
        return True
    if "contains_connector_word_without_rating" in reasons and row["latest_rating"] is None:
        return True
    if "short_all_caps_without_rating" in reasons and row["latest_rating"] is None:
        return True
    return False


def audit_bulk_message(rows) -> str:
    lines = [
        "🧹 OCR-аудит: похоже, это явный мусор",
        "",
        "Можно удалить пачкой:",
    ]
    for row in rows:
        reasons = ", ".join(json.loads(row["heuristic_reasons"] or "[]"))
        lines.append(f"- #{row['id']} / player {row['player_id']}: {row['display_name']} ({reasons})")
    lines.append("")
    lines.append("Удаление уберет эти имена из статистики игроков, а участия пометит как ocr_garbage.")
    return "\n".join(lines)


def audit_bulk_keyboard(conn, rows) -> dict:
    item_ids = [int(row["id"]) for row in rows]
    now = now_text()
    conn.execute(
        """
        INSERT INTO audit_bot_batches (item_ids_json, status, created_at)
        VALUES (?, 'open', ?)
        """,
        (json.dumps(item_ids), now),
    )
    batch_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    return {
        "inline_keyboard": [
            [{"text": f"🗑 Удалить все ({len(item_ids)})", "callback_data": f"lunda:auditbulk:{batch_id}:delete"}],
            [{"text": "✅ Оставить все", "callback_data": f"lunda:auditbulk:{batch_id}:keep"}],
            [{"text": "⏸ Потом", "callback_data": f"lunda:auditbulk:{batch_id}:later"}],
        ]
    }


def audit_message(conn, row) -> str:
    reasons = ", ".join(json.loads(row["heuristic_reasons"] or "[]"))
    candidates = audit_item_candidates(conn, row)
    lines = [
        "🧹 Проверка OCR-имени",
        "",
        f"Имя: {row['display_name']}",
        f"player_id: {row['player_id']}",
        f"Рейтинг: {row['latest_rating'] if row['latest_rating'] is not None else 'нет'}",
        f"Участий: final={row['final_count'] or 0}, current={row['current_count'] or 0}",
        f"Причины: {reasons}",
    ]
    if row["locations"]:
        lines.append(f"Клубы: {row['locations']}")
    if row["titles"]:
        lines.append(f"Турниры: {row['titles']}")
    if candidates:
        lines.append("")
        lines.append("Похожие игроки:")
        for candidate in candidates[:5]:
            lines.append(f"- #{candidate['player_id']} {candidate['name']} (dist={candidate['dist']})")
    return "\n".join(lines)


def audit_keyboard(audit_id: int, candidates: list[dict]) -> dict:
    rows = [
        [{"text": "🗑 Удалить мусор", "callback_data": f"lunda:audit:{audit_id}:delete"}],
        [{"text": "✅ Это игрок, оставить", "callback_data": f"lunda:audit:{audit_id}:keep"}],
    ]
    for candidate in candidates[:3]:
        rows.append(
            [
                {
                    "text": f"🔗 Склеить с {candidate['name']}"[:60],
                    "callback_data": f"lunda:audit:{audit_id}:merge:{candidate['player_id']}",
                }
            ]
        )
    rows.append([{"text": "⏸ Потом", "callback_data": f"lunda:audit:{audit_id}:later"}])
    return {"inline_keyboard": rows}


def handle_audit_callback(conn, *, callback_id: str, user_id: str, chat_id, message_id, data: str) -> None:
    parts = data.split(":")
    if len(parts) < 4:
        safe_answer_callback(callback_id, "Не понял")
        return
    audit_id = int(parts[2])
    action = parts[3]
    merge_player_id = int(parts[4]) if action == "merge" and len(parts) > 4 else None
    try:
        note = resolve_audit_item(conn, audit_id=audit_id, action=action, merge_player_id=merge_player_id, user_id=user_id)
        safe_answer_callback(callback_id, "Готово")
        if chat_id and message_id:
            edit_message_text(chat_id, int(message_id), f"✅ OCR-аудит решен #{audit_id}\n{note}")
    except Exception as exc:
        safe_answer_callback(callback_id, "Ошибка")
        send_admin_message(f"Ошибка OCR-аудита #{audit_id}\n\n{exc}")


def handle_audit_bulk_callback(conn, *, callback_id: str, user_id: str, chat_id, message_id, data: str) -> None:
    parts = data.split(":")
    if len(parts) < 4:
        safe_answer_callback(callback_id, "Не понял")
        return
    batch_id = int(parts[2])
    action = parts[3]
    row = conn.execute("SELECT * FROM audit_bot_batches WHERE id=?", (batch_id,)).fetchone()
    if not row and message_id:
        row = conn.execute(
            """
            SELECT *
            FROM audit_bot_batches
            WHERE telegram_message_id = ?
              AND status = 'open'
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(message_id),),
        ).fetchone()
    if not row:
        safe_answer_callback(callback_id, "Пачка не найдена")
        return
    batch_id = int(row["id"])
    if row["status"] != "open":
        safe_answer_callback(callback_id, "Уже обработано")
        return
    item_ids = json.loads(row["item_ids_json"] or "[]")
    notes: list[str] = []
    try:
        if action == "later":
            for audit_id in item_ids:
                conn.execute("UPDATE player_name_audit SET telegram_message_id=NULL WHERE id=? AND status='open'", (audit_id,))
            status = "later"
            note = "Отложено."
        elif action == "keep":
            for audit_id in item_ids:
                notes.append(resolve_audit_item(conn, audit_id=int(audit_id), action="keep", merge_player_id=None, user_id=user_id))
            status = "kept"
            note = f"Оставлено: {len(notes)}"
        elif action == "delete":
            for audit_id in item_ids:
                notes.append(resolve_audit_item(conn, audit_id=int(audit_id), action="delete", merge_player_id=None, user_id=user_id))
            status = "deleted"
            note = f"Удалено: {len(notes)}"
        else:
            raise RuntimeError(f"unknown bulk action: {action}")
        conn.execute(
            """
            UPDATE audit_bot_batches
            SET status=?, resolved_at=?, resolved_by_telegram_id=?, resolution_note=?
            WHERE id=?
            """,
            (status, now_text(), user_id, note, batch_id),
        )
        conn.commit()
        safe_answer_callback(callback_id, "Готово")
        if chat_id and message_id:
            edit_message_text(chat_id, int(message_id), f"✅ OCR-аудит пачкой решен #{batch_id}\n{note}")
    except Exception as exc:
        safe_answer_callback(callback_id, "Ошибка")
        send_admin_message(f"Ошибка OCR-аудита пачкой #{batch_id}\n\n{exc}")


def resolve_audit_item(conn, *, audit_id: int, action: str, merge_player_id: int | None, user_id: str) -> str:
    row = conn.execute(
        """
        SELECT pna.*, p.normalized_name
        FROM player_name_audit pna
        LEFT JOIN players p ON p.id = pna.player_id
        WHERE pna.id = ?
        """,
        (audit_id,),
    ).fetchone()
    if not row:
        raise RuntimeError("audit item not found")
    if row["status"] != "open":
        return f"Уже обработано: {row['status']}"
    player_id = int(row["player_id"])
    name = str(row["display_name"])
    if row["normalized_name"] is None and action in {"delete", "merge"}:
        conn.execute(
            """
            UPDATE player_name_audit
            SET status='missing_player',
                resolved_at=?,
                resolved_by_telegram_id=?,
                resolution_note='player row already missing'
            WHERE id=?
            """,
            (now_text(), user_id, audit_id),
        )
        conn.commit()
        return "Игрок уже отсутствует в базе."
    now = now_text()
    if action == "later":
        conn.execute("UPDATE player_name_audit SET telegram_message_id=NULL WHERE id=?", (audit_id,))
        conn.commit()
        return "Отложено."
    if action == "keep":
        status = "kept"
        note = "Оставлено как реальный игрок."
    elif action == "delete":
        add_player_name_blacklist(
            conn,
            display_name=name,
            normalized_name=str(row["normalized_name"] or ""),
            source="telegram_audit_delete",
            created_by_telegram_id=user_id,
            note=f"deleted by player audit #{audit_id}",
        )
        mark_player_as_ocr_garbage(conn, player_id=player_id, audit_id=audit_id)
        status = "deleted"
        note = f"Помечено как OCR-мусор и убрано из участий: {name}"
    elif action == "merge" and merge_player_id:
        merge_player(conn, source_player_id=player_id, target_player_id=merge_player_id)
        status = "merged"
        note = f"Склеено: {name} -> player #{merge_player_id}"
    else:
        raise RuntimeError(f"unknown audit action: {action}")
    conn.execute(
        """
        UPDATE player_name_audit
        SET status=?,
            resolved_at=?,
            resolved_by_telegram_id=?,
            resolution_note=?
        WHERE id=?
        """,
        (status, now, user_id, note, audit_id),
    )
    conn.commit()
    return note


def mark_player_as_ocr_garbage(conn, *, player_id: int, audit_id: int) -> None:
    key = f"ocr_garbage:{player_id}"
    for table in ("current_participants", "final_participations"):
        conn.execute(
            f"""
            UPDATE {table}
            SET player_id = NULL,
                participant_key = ?,
                resolve_status = 'ocr_garbage'
            WHERE player_id = ?
            """,
            (key, player_id),
        )
    conn.execute(
        """
        UPDATE pending_players
        SET status='discarded',
            resolution_note=COALESCE(resolution_note, ?) 
        WHERE resolved_player_id = ? OR normalized_name = (
            SELECT normalized_name FROM players WHERE id = ?
        )
        """,
        (f"discarded by player audit #{audit_id}", player_id, player_id),
    )
    conn.execute("DELETE FROM player_aliases WHERE player_id = ?", (player_id,))
    conn.execute("DELETE FROM players WHERE id = ?", (player_id,))


def merge_player(conn, *, source_player_id: int, target_player_id: int) -> None:
    if source_player_id == target_player_id:
        raise RuntimeError("source and target are the same")
    source = conn.execute("SELECT * FROM players WHERE id=?", (source_player_id,)).fetchone()
    target = conn.execute("SELECT * FROM players WHERE id=?", (target_player_id,)).fetchone()
    if not source or not target:
        raise RuntimeError("source or target player not found")
    for table in ("current_participants", "final_participations"):
        conn.execute(
            f"""
            UPDATE {table}
            SET player_id = ?,
                participant_key = ?,
                resolve_status = 'audit_merged'
            WHERE player_id = ?
            """,
            (target_player_id, f"player:{target_player_id}", source_player_id),
        )
    conn.execute(
        """
        INSERT INTO player_aliases (player_id, alias_name, normalized_alias, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(normalized_alias) DO UPDATE SET player_id=excluded.player_id
        """,
        (target_player_id, source["display_name"], source["normalized_name"], now_text()),
    )
    conn.execute("DELETE FROM players WHERE id = ?", (source_player_id,))


def handle_invite_document(conn, user_id: str, document: dict) -> None:
    session = latest_waiting_invite_session(conn, user_id=user_id)
    if not session:
        send_admin_message("Сначала выбери турнир через /invite, потом отправь Excel-файл.")
        return
    tournament_id = int(session["my_tournament_id"])
    file_name = str(document.get("file_name") or "")
    if not file_name.lower().endswith((".xlsx", ".xlsm")):
        send_admin_message("Нужен Excel .xlsx/.xlsm с именами в первом столбце.")
        return
    file_id = str(document.get("file_id") or "")
    try:
        path = download_telegram_file(file_id, suffix=Path(file_name).suffix or ".xlsx")
        names = read_player_names_from_xlsx(path)
        if not names:
            send_admin_message("В Excel не нашел имен в первом столбце.")
            return
        conn.execute(
            """
            UPDATE invite_bot_sessions
            SET upload_path=?, names_json=?, status='ready', updated_at=?
            WHERE id=?
            """,
            (str(path), json.dumps(names, ensure_ascii=False), now_text(), session["id"]),
        )
        conn.commit()
        send_admin_message(
            invite_confirmation_message(conn, tournament_id=tournament_id, names=names),
            reply_markup={
                "inline_keyboard": [
                    [{"text": "▶️ Начать отправку приглашений", "callback_data": f"lunda:invite:start:{session['id']}"}],
                    [{"text": "❌ Отмена", "callback_data": f"lunda:invite:cancel:{session['id']}"}],
                ]
            },
        )
        print(f"invite session {session['id']}: excel received; names={len(names)}", flush=True)
    except Exception as exc:
        send_admin_message(f"Приглашения: ошибка Excel\n\n{exc}")


def create_invite_session(conn, *, user_id: str, tournament_id: int) -> int:
    now = now_text()
    conn.execute(
        "UPDATE invite_bot_sessions SET status='cancelled', updated_at=? WHERE user_id=? AND status IN ('waiting_file', 'ready')",
        (now, user_id),
    )
    conn.execute(
        """
        INSERT INTO invite_bot_sessions (user_id, my_tournament_id, status, created_at, updated_at)
        VALUES (?, ?, 'waiting_file', ?, ?)
        """,
        (user_id, tournament_id, now, now),
    )
    conn.commit()
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def latest_waiting_invite_session(conn, *, user_id: str):
    return conn.execute(
        """
        SELECT *
        FROM invite_bot_sessions
        WHERE user_id = ? AND status = 'waiting_file'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_id,),
    ).fetchone()


def get_invite_session(conn, *, session_id: int, user_id: str):
    return conn.execute(
        "SELECT * FROM invite_bot_sessions WHERE id = ? AND user_id = ?",
        (session_id, user_id),
    ).fetchone()


def invite_confirmation_message(conn, *, tournament_id: int, names: list[str]) -> str:
    return (
        "Excel получил.\n\n"
        f"Турнир:\n{invite_tournament_label(conn, tournament_id)}\n\n"
        f"Игроков в файле: {len(names)}\n"
        "Начать отправку приглашений?"
    )


def invite_tournament_label(conn, tournament_id: int) -> str:
    row = conn.execute("SELECT * FROM my_tournaments WHERE id = ?", (tournament_id,)).fetchone()
    if not row:
        return f"#{tournament_id}"
    return (
        f"#{row['id']} {_short_datetime(row['starts_at'], row['time_label'])} | "
        f"{row['location']} | {row['participants_label'] or ''}"
    ).strip()


def download_telegram_file(file_id: str, *, suffix: str = ".xlsx") -> Path:
    info = telegram_request("getFile", {"file_id": file_id})
    file_path = str((info.get("result") or {}).get("file_path") or "")
    if not file_path:
        raise RuntimeError("Telegram did not return file_path")
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    url = f"https://api.telegram.org/file/bot{token}/{file_path}"
    out_dir = Path(os.environ.get("LUNDA_INVITE_UPLOAD_DIR", "/opt/lunda-collector/work/invite_uploads"))
    out_dir.mkdir(parents=True, exist_ok=True)
    output = out_dir / f"invite_{int(time.time())}{suffix}"
    urllib.request.urlretrieve(url, output)
    return output


def _short_datetime(starts_at: str, fallback_time: str) -> str:
    if not starts_at:
        return fallback_time
    try:
        from datetime import datetime

        dt = datetime.fromisoformat(starts_at)
    except ValueError:
        return fallback_time
    return dt.strftime("%d.%m %H:%M")


def now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


if __name__ == "__main__":
    raise SystemExit(main())
