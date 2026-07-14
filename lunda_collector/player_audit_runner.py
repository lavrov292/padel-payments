#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

from player_audit import init_player_audit_db, load_env_file
from storage import connect, init_db
from telegram_admin_bot import init_invite_bot_db, notify_audit_items
from telegram_alerts import notify_cycle_problem


def main() -> int:
    parser = argparse.ArgumentParser(description="Run daily OCR player audit and notify Telegram admin")
    parser.add_argument("--db", required=True)
    parser.add_argument("--env-file", default="/opt/lunda-collector/app/.env")
    parser.add_argument("--out-dir", default="/opt/lunda-collector/outputs/player_audit")
    parser.add_argument("--limit", type=int, default=50)
    args = parser.parse_args()

    load_env_file(Path(args.env_file))
    out_dir = Path(args.out_dir).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"player_audit_{time.strftime('%Y%m%d_%H%M%S')}.xlsx"

    command = [
        sys.executable,
        "-u",
        str(Path(__file__).resolve().parent / "player_audit.py"),
        "--db",
        args.db,
        "--env-file",
        args.env_file,
        "--out",
        str(out_path),
        "--limit",
        str(args.limit),
    ]
    result = subprocess.run(
        command,
        cwd=str(Path(__file__).resolve().parent),
        text=True,
        capture_output=True,
        timeout=20 * 60,
    )

    conn = connect(args.db)
    init_db(conn)
    init_invite_bot_db(conn)
    init_player_audit_db(conn)
    sent = notify_audit_items(conn, limit=args.limit)

    details = [
        f"player_audit returncode={result.returncode}",
        f"telegram messages sent={sent}",
        f"output={out_path}",
    ]
    if result.stdout.strip():
        details.append("")
        details.append(result.stdout.strip()[-2500:])
    if result.stderr.strip():
        details.append("")
        details.append(result.stderr.strip()[-2500:])

    title = "Daily player OCR audit" if result.returncode == 0 else "Daily player OCR audit failed"
    notify_cycle_problem(title=title, details="\n".join(details))
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
