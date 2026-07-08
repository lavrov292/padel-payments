#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from date_parser import MSK
from exports import export_workbook
from storage import connect, init_db, tournament_summary
from telegram_alerts import notify_cycle_problem


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect future Lunda tournament schedule")
    parser.add_argument("--db", default="")
    parser.add_argument("--device", default="100.77.174.30:5555")
    parser.add_argument("--session-dir", default="")
    parser.add_argument("--env-file", default="/opt/lunda-collector/app/.env")
    parser.add_argument("--days", type=int, default=21)
    parser.add_argument("--screens", type=int, default=80)
    parser.add_argument("--scroll-pixels", type=int, default=880)
    parser.add_argument("--cycle-timeout-minutes", type=int, default=90)
    parser.add_argument("--launch", action="store_true")
    args = parser.parse_args()

    load_env_file(Path(args.env_file))

    today = datetime.now(MSK).date()
    session_dir = Path(args.session_dir or f"/opt/lunda-collector/work/schedule_{today.isoformat()}").expanduser()
    session_dir.mkdir(parents=True, exist_ok=True)
    output_dir = Path("/opt/lunda-collector/outputs") / f"schedule_{today.isoformat()}"
    output_dir.mkdir(parents=True, exist_ok=True)
    db_path = Path(args.db or os.environ.get("LUNDA_DB_PATH") or (session_dir / "schedule.sqlite3")).expanduser()
    log_path = session_dir / "runner.log"
    state_path = session_dir / "state.json"
    cycle_dir = session_dir / f"run_{datetime.now(MSK).strftime('%H%M%S')}"
    cycle_dir.mkdir(parents=True, exist_ok=True)
    cycle_log = cycle_dir / "cycle.log"

    command = build_schedule_command(args=args, db_path=db_path, cycle_dir=cycle_dir)

    with log_path.open("a", encoding="utf-8", buffering=1) as log:
        log_line(log, f"schedule runner started session={session_dir} db={db_path}")
        log_line(log, f"command={' '.join(command)}")
        result = run_cycle(command, cycle_log, timeout_minutes=args.cycle_timeout_minutes)
        if result != 0:
            notify_cycle_problem(
                title="Schedule collection failed",
                details=f"returncode={result}\nlog={cycle_log}",
            )
            write_state(
                state_path,
                args,
                session_dir,
                db_path,
                output_dir,
                status="error",
                summary={"returncode": result, "cycle_log": str(cycle_log)},
            )
            return result

        summary = postprocess_schedule(db_path, output_dir, days=args.days)
        summary["cycle_log"] = str(cycle_log)
        write_state(state_path, args, session_dir, db_path, output_dir, status="ok", summary=summary)
        log_line(log, f"schedule runner finished summary={summary}")
    return 0


def build_schedule_command(*, args: argparse.Namespace, db_path: Path, cycle_dir: Path) -> list[str]:
    script_dir = Path(__file__).resolve().parent
    command = [
        sys.executable,
        "-u",
        str(script_dir / "live_phone_cycle.py"),
        "--db",
        str(db_path),
        "--device",
        args.device,
        "--out-dir",
        str(cycle_dir),
        "schedule",
        "--days",
        str(args.days),
        "--screens",
        str(args.screens),
        "--scroll-pixels",
        str(args.scroll_pixels),
    ]
    if args.launch:
        command.append("--launch")
    return command


def run_cycle(command: list[str], cycle_log: Path, *, timeout_minutes: int) -> int:
    with cycle_log.open("w", encoding="utf-8") as log:
        try:
            result = subprocess.run(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=timeout_minutes * 60,
                cwd=str(Path(__file__).resolve().parent),
            )
            return int(result.returncode)
        except subprocess.TimeoutExpired:
            log.write(f"\nTIMEOUT after {timeout_minutes} minutes\n")
            return 124


def postprocess_schedule(db_path: Path, output_dir: Path, *, days: int) -> dict[str, Any]:
    now = datetime.now(MSK)
    horizon = now.date() + timedelta(days=days)
    conn = connect(db_path)
    init_db(conn)
    missing_stats = mark_missing_future_tournaments(conn, now=now, horizon=horizon)
    export_path = output_dir / "schedule_latest.xlsx"
    export_workbook(conn, export_path, date_from=now.date().isoformat(), date_to=horizon.isoformat())
    summary = tournament_summary(conn)
    summary.update(missing_stats)
    summary["date_from"] = now.date().isoformat()
    summary["date_to"] = horizon.isoformat()
    summary["export_path"] = str(export_path)
    return summary


def mark_missing_future_tournaments(conn, *, now: datetime, horizon) -> dict[str, Any]:
    run = conn.execute(
        """
        SELECT id, status, stats_json
        FROM sync_runs
        WHERE kind = 'schedule_live'
        ORDER BY id DESC
        LIMIT 1
        """
    ).fetchone()
    if not run or run["status"] != "ok":
        return {"schedule_missing_marked": 0, "schedule_missing_skipped": 1}

    observed = [
        int(row["tournament_id"])
        for row in conn.execute(
            "SELECT DISTINCT tournament_id FROM tournament_observations WHERE run_id = ?",
            (run["id"],),
        ).fetchall()
    ]
    if not observed:
        return {"schedule_missing_marked": 0, "schedule_missing_skipped": 1}

    placeholders = ",".join("?" for _ in observed)
    params: list[Any] = [now.isoformat(timespec="seconds"), horizon.isoformat(), *observed]
    cursor = conn.execute(
        f"""
        UPDATE tournaments
        SET source_status = 'missing',
            last_seen_at = last_seen_at
        WHERE source_status = 'active'
          AND starts_at >= ?
          AND tournament_date <= ?
          AND id NOT IN ({placeholders})
        """,
        params,
    )
    conn.commit()
    return {"schedule_missing_marked": int(cursor.rowcount or 0), "schedule_missing_skipped": 0}


def write_state(
    state_path: Path,
    args: argparse.Namespace,
    session_dir: Path,
    db_path: Path,
    output_dir: Path,
    *,
    status: str,
    summary: dict[str, Any],
) -> None:
    state = {
        "status": status,
        "updated_at": datetime.now(MSK).isoformat(timespec="seconds"),
        "session_dir": str(session_dir),
        "db_path": str(db_path),
        "output_dir": str(output_dir),
        "days": args.days,
        "screens": args.screens,
        "scroll_pixels": args.scroll_pixels,
        "summary": summary,
    }
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def log_line(log: Any, message: str) -> None:
    print(f"{datetime.now(MSK).isoformat(timespec='seconds')} {message}", file=log, flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
