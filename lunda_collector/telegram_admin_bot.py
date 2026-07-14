#!/usr/bin/env python3
from __future__ import annotations

import argparse
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


INVITE_UPLOAD_STATE: dict[str, int] = {}


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
        return
    if text.startswith("/invite"):
        send_invite_tournament_picker(conn)
        return

    document = message.get("document") or {}
    if document:
        handle_invite_document(conn, str(user.get("id") or chat.get("id") or ""), document)


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

    if data.startswith("lunda:invite:tournament:"):
        tournament_id = int(data.rsplit(":", 1)[-1])
        INVITE_UPLOAD_STATE[user_id] = tournament_id
        answer_callback(callback_id, "Жду Excel")
        if chat_id and message_id:
            edit_message_text(
                chat_id,
                int(message_id),
                f"✅ Турнир выбран: #{tournament_id}\nТеперь отправь Excel-файл с именами игроков в первом столбце.",
            )
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


def handle_invite_document(conn, user_id: str, document: dict) -> None:
    tournament_id = INVITE_UPLOAD_STATE.get(user_id)
    if not tournament_id:
        return
    file_name = str(document.get("file_name") or "")
    if not file_name.lower().endswith((".xlsx", ".xlsm")):
        notify_cycle_problem(title="Приглашения", details="Нужен Excel .xlsx/.xlsm с именами в первом столбце.")
        return
    file_id = str(document.get("file_id") or "")
    try:
        path = download_telegram_file(file_id, suffix=Path(file_name).suffix or ".xlsx")
        names = read_player_names_from_xlsx(path)
        if not names:
            notify_cycle_problem(title="Приглашения", details="В Excel не нашел имен в первом столбце.")
            return
        job_id = create_invite_job(conn, my_tournament_id=tournament_id, player_names=names)
        INVITE_UPLOAD_STATE.pop(user_id, None)
        notify_cycle_problem(
            title="Приглашения",
            details=f"Создана задача #{job_id}. Игроков в файле: {len(names)}. Выполню ее отдельным invite-runner.",
        )
    except Exception as exc:
        notify_cycle_problem(title="Приглашения: ошибка Excel", details=str(exc))


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
