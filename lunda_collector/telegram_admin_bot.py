#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from storage import connect, init_db
from telegram_alerts import (
    admin_chat_id,
    answer_callback,
    edit_message_text,
    notify_cycle_problem,
    resolve_pending,
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
        notify_cycle_problem(title="Lunda status", details=f"Pending names: {rows}")


def handle_callback(conn, callback: dict) -> None:
    user = callback.get("from") or {}
    user_id = str(user.get("id") or "")
    callback_id = str(callback.get("id") or "")
    message = callback.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    message_id = message.get("message_id")
    data = str(callback.get("data") or "")

    if user_id != str(admin_chat_id()):
        answer_callback(callback_id, "Недоступно")
        return

    parts = data.split(":")
    if len(parts) < 4 or parts[:2] != ["lunda", "pending"]:
        answer_callback(callback_id, "Неизвестная команда")
        return

    pending_id = int(parts[2])
    action = parts[3]
    player_id = int(parts[4]) if action == "player" and len(parts) > 4 else None
    note = resolve_pending(conn, pending_id=pending_id, action=action, player_id=player_id, resolved_by=user_id)
    answer_callback(callback_id, "Готово")
    if chat_id and message_id:
        edit_message_text(chat_id, int(message_id), f"✅ Решено: pending #{pending_id}\n{note}")


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
