#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from date_parser import MSK
from exports import export_workbook
from player_matcher import normalize_name
from participants_parser import (
    append_unique_participant,
    is_departed_section,
    is_departed_status,
    parse_participants_from_ocr,
)
from storage import (
    DEFAULT_DB_PATH,
    connect,
    finalize_due_tournaments,
    find_tournament_id,
    finish_run,
    init_db,
    is_persistable_tournament_card,
    player_rankings,
    record_participant_snapshot,
    schedule_rows,
    start_run,
    tournament_summary,
    upsert_tournament_from_card,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lunda collector local database CLI")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH), help="SQLite database path")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init-db", help="Create or migrate local SQLite database")

    ingest_list = subparsers.add_parser("ingest-list", help="Ingest parsed tournament cards JSON")
    ingest_list.add_argument("--json", required=True, help="Path to tournaments.json or observations.json")
    ingest_list.add_argument("--kind", default="schedule", choices=["schedule", "today_participants", "test"])
    ingest_list.add_argument("--source", default="list")
    ingest_list.add_argument("--include-incomplete", action="store_true")

    ingest_participants = subparsers.add_parser("ingest-participants", help="Ingest participants JSON for one tournament")
    ingest_participants.add_argument("--json", default="", help="Path to participants.json")
    ingest_participants.add_argument("--ocr-dir", default="", help="Directory with scan_tournament_participants *_ocr.json files")
    ingest_participants.add_argument("--type", choices=["auto", "team", "personal"], default="auto")
    ingest_participants.add_argument("--tournament-id", type=int, default=0)
    ingest_participants.add_argument("--identity-key", default="")
    ingest_participants.add_argument("--title", default="")
    ingest_participants.add_argument("--organizer", default="")
    ingest_participants.add_argument("--date", default="")
    ingest_participants.add_argument("--time", default="")
    ingest_participants.add_argument("--kind", default="participants")

    finalize = subparsers.add_parser("finalize-due", help="Finalize active tournaments whose start time has passed")
    finalize.add_argument("--now", default="", help="ISO datetime, defaults to now in MSK")
    finalize.add_argument("--grace-minutes", type=int, default=0)

    subparsers.add_parser("summary", help="Print database counters")

    list_tournaments = subparsers.add_parser("list-tournaments", help="Print known tournaments")
    list_tournaments.add_argument("--date-from", default="")
    list_tournaments.add_argument("--date-to", default="")
    list_tournaments.add_argument("--organizer", default="")
    list_tournaments.add_argument("--location", default="")
    list_tournaments.add_argument("--level", default="")
    list_tournaments.add_argument("--format", default="")
    list_tournaments.add_argument("--include-cancelled", action="store_true")

    rankings = subparsers.add_parser("rankings", help="Print final player rankings")
    rankings.add_argument("--date-from", default="")
    rankings.add_argument("--date-to", default="")
    rankings.add_argument("--organizer", default="")
    rankings.add_argument("--location", default="")
    rankings.add_argument("--level", default="")
    rankings.add_argument("--format", default="")
    rankings.add_argument("--limit", type=int, default=50)

    export = subparsers.add_parser("export", help="Export database views to Excel")
    export.add_argument("--out", default="")
    export.add_argument("--date-from", default="")
    export.add_argument("--date-to", default="")
    export.add_argument("--organizer", default="")
    export.add_argument("--location", default="")
    export.add_argument("--level", default="")
    export.add_argument("--format", default="")

    alias = subparsers.add_parser("add-alias", help="Attach OCR spelling variant to an existing player")
    alias.add_argument("--player-id", type=int, required=True)
    alias.add_argument("--alias", required=True)

    args = parser.parse_args()
    conn = connect(args.db)
    init_db(conn)

    if args.command == "init-db":
        print(f"Database ready: {Path(args.db).expanduser().resolve()}")
        return 0

    if args.command == "ingest-list":
        return cmd_ingest_list(conn, args)
    if args.command == "ingest-participants":
        return cmd_ingest_participants(conn, args)
    if args.command == "finalize-due":
        return cmd_finalize_due(conn, args)
    if args.command == "summary":
        return cmd_summary(conn)
    if args.command == "list-tournaments":
        return cmd_list_tournaments(conn, args)
    if args.command == "rankings":
        return cmd_rankings(conn, args)
    if args.command == "export":
        return cmd_export(conn, args)
    if args.command == "add-alias":
        return cmd_add_alias(conn, args)

    parser.print_help()
    return 2


def cmd_ingest_list(conn, args: argparse.Namespace) -> int:
    cards = _load_cards(Path(args.json))
    run_id = start_run(conn, args.kind)
    stats = {"cards": len(cards), "skipped_incomplete": 0, "tournaments_upserted": 0}
    try:
        for card in cards:
            if not args.include_incomplete and not is_persistable_tournament_card(card):
                stats["skipped_incomplete"] += 1
                continue
            upsert_tournament_from_card(conn, card, run_id=run_id, source=args.source)
            stats["tournaments_upserted"] += 1
        finish_run(conn, run_id, stats=stats)
    except Exception as exc:
        finish_run(conn, run_id, status="error", stats=stats, error=str(exc))
        raise

    print(f"Ingested cards: {stats['cards']}")
    print(f"Skipped incomplete: {stats['skipped_incomplete']}")
    print(f"Upserted tournaments: {stats['tournaments_upserted']}")
    print(f"Run id: {run_id}")
    return 0


def cmd_ingest_participants(conn, args: argparse.Namespace) -> int:
    tournament_id = args.tournament_id or find_tournament_id(
        conn,
        identity_key=args.identity_key,
        title=args.title,
        organizer=args.organizer,
        date_label=args.date,
        time_label=args.time,
    )
    if not tournament_id:
        print("Tournament was not found. Use list-tournaments and pass --tournament-id.", file=sys.stderr)
        return 2

    if args.ocr_dir:
        participants = _load_participants_from_ocr_dir(Path(args.ocr_dir), tournament_type=args.type)
    elif args.json:
        participants = _load_participants(Path(args.json))
    else:
        print("Pass --json or --ocr-dir.", file=sys.stderr)
        return 2
    run_id = start_run(conn, args.kind)
    try:
        stats = record_participant_snapshot(
            conn,
            int(tournament_id),
            participants,
            run_id=run_id,
            raw={"source_file": str(Path(args.json).expanduser())},
        )
        finish_run(conn, run_id, stats=stats)
    except Exception as exc:
        finish_run(conn, run_id, status="error", error=str(exc))
        raise

    print(f"Tournament id: {tournament_id}")
    print(f"Participants seen: {stats['seen']}")
    print(f"Resolved: {stats['resolved']}")
    print(f"New players: {stats['new']}")
    print(f"Pending: {stats['pending']}")
    print(f"Run id: {run_id}")
    return 0


def cmd_finalize_due(conn, args: argparse.Namespace) -> int:
    now = _parse_now(args.now)
    stats = finalize_due_tournaments(conn, now=now, grace_minutes=args.grace_minutes)
    print(f"Finalized tournaments: {stats['tournaments_finalized']}")
    print(f"Finalized participants: {stats['participants_finalized']}")
    return 0


def cmd_summary(conn) -> int:
    for key, value in tournament_summary(conn).items():
        print(f"{key}: {value}")
    return 0


def cmd_list_tournaments(conn, args: argparse.Namespace) -> int:
    rows = schedule_rows(
        conn,
        date_from=args.date_from,
        date_to=args.date_to,
        organizer=args.organizer,
        location=args.location,
        skill_level=args.level,
        format_value=args.format,
        include_cancelled=args.include_cancelled,
    )
    for row in rows:
        participants = _participants_label(row)
        print(
            f"#{row['id']} | {row['tournament_date'] or row['date_label']} | "
            f"{row['time_label'] or ''} | {row['organizer'] or ''} | "
            f"{row['location'] or ''} | {row['skill_level'] or ''} | "
            f"{participants} | {row['title'] or ''}"
        )
    print(f"Total: {len(rows)}")
    return 0


def cmd_rankings(conn, args: argparse.Namespace) -> int:
    rows = player_rankings(
        conn,
        date_from=args.date_from,
        date_to=args.date_to,
        organizer=args.organizer,
        location=args.location,
        skill_level=args.level,
        format_value=args.format,
        limit=args.limit,
    )
    for idx, row in enumerate(rows, 1):
        print(
            f"{idx}. {row['player_name']} | {row['tournament_count']} | "
            f"{row['locations'] or ''} | {row['organizers'] or ''}"
        )
    print(f"Total: {len(rows)}")
    return 0


def cmd_export(conn, args: argparse.Namespace) -> int:
    output = args.out or f"work/lunda_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    export_path = export_workbook(
        conn,
        output,
        date_from=args.date_from,
        date_to=args.date_to,
        organizer=args.organizer,
        location=args.location,
        skill_level=args.level,
        format_value=args.format,
    )
    print(f"Export: {export_path}")
    return 0


def cmd_add_alias(conn, args: argparse.Namespace) -> int:
    row = conn.execute("SELECT id FROM players WHERE id = ?", (args.player_id,)).fetchone()
    if not row:
        print(f"Player not found: {args.player_id}", file=sys.stderr)
        return 2
    normalized = normalize_name(args.alias)
    now_iso = datetime.now(MSK).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO player_aliases (player_id, alias_name, normalized_alias, created_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(normalized_alias) DO UPDATE SET
            player_id = excluded.player_id,
            alias_name = excluded.alias_name
        """,
        (args.player_id, args.alias, normalized, now_iso),
    )
    conn.commit()
    print(f"Alias saved: {args.alias} -> player #{args.player_id}")
    return 0


def _load_cards(path: Path) -> list[dict[str, Any]]:
    data = json.loads(path.expanduser().read_text(encoding="utf-8"))
    if isinstance(data, dict):
        if isinstance(data.get("tournaments"), list):
            data = data["tournaments"]
        elif isinstance(data.get("cards"), list):
            data = data["cards"]
    if not isinstance(data, list):
        raise ValueError(f"Expected list in {path}")
    return [item for item in data if isinstance(item, dict)]


def _load_participants(path: Path) -> list[str]:
    data = json.loads(path.expanduser().read_text(encoding="utf-8"))
    if isinstance(data, dict):
        data = data.get("participants", [])
    if not isinstance(data, list):
        raise ValueError(f"Expected participants list in {path}")
    return [str(item).strip() for item in data if str(item).strip()]


def _load_participants_from_ocr_dir(path: Path, *, tournament_type: str = "auto") -> list[str]:
    participants: list[str] = []
    for ocr_path in sorted(path.expanduser().glob("*_ocr.json")):
        ocr_result = json.loads(ocr_path.read_text(encoding="utf-8"))
        text = extract_text(ocr_result)
        departed_marker_seen = any(is_departed_section(line) for line in text.splitlines())
        departed_status_seen = any(is_departed_status(line) for line in text.splitlines())

        if departed_status_seen and not departed_marker_seen:
            break

        found = parse_participants_from_ocr(ocr_result, tournament_type=tournament_type)
        for participant in found:
            append_unique_participant(participants, participant)

        if departed_marker_seen:
            break

    return participants


def extract_text(ocr_result: dict[str, Any]) -> str:
    lines: list[str] = []
    for result in ocr_result.get("results", []):
        for res in result.get("results", []):
            text_detection = res.get("textDetection", {})
            for page in text_detection.get("pages", []):
                for block in page.get("blocks", []):
                    for line in block.get("lines", []):
                        words = line.get("words", [])
                        text = " ".join(word.get("text", "") for word in words).strip()
                        if text:
                            lines.append(text)
    return "\n".join(lines)


def _parse_now(value: str) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=MSK)
    return parsed.astimezone(MSK)


def _participants_label(row) -> str:
    current = row["participants_current"]
    capacity = row["participants_capacity"]
    unit = row["participants_unit"] or ""
    if current is None or capacity is None:
        return ""
    return f"{current}/{capacity} {unit}".strip()


if __name__ == "__main__":
    raise SystemExit(main())
