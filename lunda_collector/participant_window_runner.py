#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any

from date_parser import MSK
from exports import export_workbook
from storage import connect, finalize_due_tournaments, init_db, tournament_summary
from telegram_alerts import notify_cycle_problem, notify_pending_players


def main() -> int:
    parser = argparse.ArgumentParser(description="Run participant collection every N minutes for nearby tournaments")
    parser.add_argument("--db", default="")
    parser.add_argument("--device", default="100.77.174.30:5555")
    parser.add_argument("--session-dir", default="")
    parser.add_argument("--env-file", default="/opt/lunda-collector/app/.env")
    parser.add_argument("--interval-minutes", type=int, default=30)
    parser.add_argument("--lookahead-minutes", type=int, default=150)
    parser.add_argument("--active-from", default="06:00")
    parser.add_argument("--active-until", default="22:00")
    parser.add_argument("--cycle-timeout-minutes", type=int, default=29)
    parser.add_argument("--screens", type=int, default=16)
    parser.add_argument("--scroll-pixels", type=int, default=440)
    parser.add_argument("--participants-screens", type=int, default=10)
    parser.add_argument("--participants-scroll-pixels", type=int, default=420)
    parser.add_argument("--launch", action="store_true")
    parser.add_argument("--exit-after-active-until", action="store_true")
    parser.add_argument("--pending-check-seconds", type=int, default=30, help=argparse.SUPPRESS)
    parser.add_argument("--disable-pending-wait", action="store_true")
    args = parser.parse_args()

    load_env_file(Path(args.env_file))

    today = datetime.now(MSK).date().isoformat()
    session_dir = Path(args.session_dir or f"/opt/lunda-collector/work/participant_window_{today}").expanduser()
    session_dir.mkdir(parents=True, exist_ok=True)
    output_dir = Path("/opt/lunda-collector/outputs") / f"participant_window_{today}"
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path = Path(args.db or (session_dir / "participant_window.sqlite3")).expanduser()
    log_path = session_dir / "runner.log"
    state_path = session_dir / "state.json"

    active_from = parse_hhmm(args.active_from)
    active_until = parse_hhmm(args.active_until)
    cycle_no = 0
    first_cycle = True

    with log_path.open("a", encoding="utf-8", buffering=1) as log:
        log_line(log, f"runner started session={session_dir} db={db_path}")
        while True:
            now = datetime.now(MSK)
            if now.time() < active_from:
                sleep_seconds = seconds_until_today(now, active_from)
                log_line(log, f"sleeping until active window: {sleep_seconds:.0f}s")
                time.sleep(min(sleep_seconds, 3600))
                continue

            if now.time() >= active_until:
                summary = finalize_and_export(db_path, output_dir, today)
                write_state(state_path, args, session_dir, db_path, output_dir, summary, status="sleeping")
                log_line(log, f"active window closed; finalized/exported summary={summary}")
                if args.exit_after_active_until:
                    log_line(log, "runner exiting after active window")
                    return 0
                time.sleep(seconds_until_tomorrow(now, active_from))
                continue

            cycle_no += 1
            cycle_started = datetime.now(MSK)
            cycle_dir = session_dir / f"cycle_{cycle_no:03d}_{cycle_started.strftime('%H%M%S')}"
            cycle_dir.mkdir(parents=True, exist_ok=True)
            cycle_log = cycle_dir / "cycle.log"
            command = build_cycle_command(
                args=args,
                db_path=db_path,
                cycle_dir=cycle_dir,
                target_date=today,
                launch=args.launch or first_cycle,
            )
            first_cycle = False
            log_line(log, f"cycle {cycle_no} start command={' '.join(command)}")
            result = run_cycle(command, cycle_log, timeout_minutes=args.cycle_timeout_minutes)
            if result != 0:
                notify_cycle_problem(
                    title=f"Cycle {cycle_no} failed",
                    details=f"returncode={result}\nlog={cycle_log}",
                )
            if not args.disable_pending_wait:
                with connect(db_path) as pending_conn:
                    init_db(pending_conn)
                    sent = notify_pending_players(pending_conn)
                    if sent:
                        log_line(log, f"telegram pending notifications sent: {sent}")
            summary = finalize_and_export(db_path, output_dir, today)
            write_state(
                state_path,
                args,
                session_dir,
                db_path,
                output_dir,
                summary,
                status="running",
                last_cycle={"number": cycle_no, "returncode": result, "dir": str(cycle_dir)},
            )
            log_line(log, f"cycle {cycle_no} done returncode={result} summary={summary}")

            sleep_seconds = seconds_until_next_interval(datetime.now(MSK), args.interval_minutes)
            log_line(log, f"sleeping until next cycle: {sleep_seconds:.0f}s")
            time.sleep(max(5, sleep_seconds))


def build_cycle_command(
    *,
    args: argparse.Namespace,
    db_path: Path,
    cycle_dir: Path,
    target_date: str,
    launch: bool,
) -> list[str]:
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
        "today-participants",
        "--target-date",
        target_date,
        "--screens",
        str(args.screens),
        "--scroll-pixels",
        str(args.scroll_pixels),
        "--participants-screens",
        str(args.participants_screens),
        "--participants-scroll-pixels",
        str(args.participants_scroll_pixels),
        "--lookahead-minutes",
        str(args.lookahead_minutes),
    ]
    if launch:
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


def finalize_and_export(db_path: Path, output_dir: Path, target_date: str) -> dict[str, Any]:
    conn = connect(db_path)
    init_db(conn)
    finalize_stats = finalize_due_tournaments(conn)
    summary = tournament_summary(conn)
    export_path = output_dir / "participant_window_latest.xlsx"
    export_workbook(conn, export_path, date_from=target_date, date_to=target_date)
    summary.update({f"finalize_{key}": value for key, value in finalize_stats.items()})
    summary["export_path"] = str(export_path)
    return summary


def write_state(
    state_path: Path,
    args: argparse.Namespace,
    session_dir: Path,
    db_path: Path,
    output_dir: Path,
    summary: dict[str, Any],
    *,
    status: str,
    last_cycle: dict[str, Any] | None = None,
) -> None:
    state = {
        "status": status,
        "updated_at": datetime.now(MSK).isoformat(timespec="seconds"),
        "session_dir": str(session_dir),
        "db_path": str(db_path),
        "output_dir": str(output_dir),
        "interval_minutes": args.interval_minutes,
        "lookahead_minutes": args.lookahead_minutes,
        "active_from": args.active_from,
        "active_until": args.active_until,
        "summary": summary,
        "last_cycle": last_cycle or {},
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


def parse_hhmm(value: str) -> dt_time:
    hour, minute = value.split(":", 1)
    return dt_time(int(hour), int(minute))


def seconds_until_today(now: datetime, target: dt_time) -> float:
    target_dt = datetime.combine(now.date(), target, tzinfo=MSK)
    return max(0.0, (target_dt - now).total_seconds())


def seconds_until_tomorrow(now: datetime, target: dt_time) -> float:
    target_dt = datetime.combine(now.date() + timedelta(days=1), target, tzinfo=MSK)
    return max(0.0, (target_dt - now).total_seconds())


def seconds_until_next_interval(now: datetime, interval_minutes: int) -> float:
    interval_seconds = max(1, interval_minutes) * 60
    timestamp = int(now.timestamp())
    next_timestamp = ((timestamp // interval_seconds) + 1) * interval_seconds
    return float(next_timestamp - timestamp)


def log_line(log: Any, message: str) -> None:
    print(f"{datetime.now(MSK).isoformat(timespec='seconds')} {message}", file=log, flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
