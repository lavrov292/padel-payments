#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from date_parser import MSK, participant_target_date, parse_tournament_datetime
from device_backend import ADBDevice, YandexOCRClient
from participants_parser import (
    append_unique_participant,
    is_departed_section,
    is_departed_status,
    parse_participant_records_from_ocr,
    parse_participants_from_ocr,
)
from screen_nav import (
    detect_screen,
    find_home_button_center,
    find_participants_entry_center,
    has_lunda_bottom_nav,
    list_header_is_expanded,
)
from storage import (
    DEFAULT_DB_PATH,
    connect,
    finalize_due_tournaments,
    init_db,
    is_persistable_schedule_card,
    is_persistable_tournament_card,
    mark_tournament_cancelled,
    mark_tournament_missing,
    record_participant_snapshot,
    start_run,
    finish_run,
    upsert_tournament_from_card,
)
from visible_cards import merge_visible_cards, parse_visible_tournament_cards


DEFAULT_OLD_PARSER_DIR = Path(
    os.environ.get(
        "LUNDA_OLD_PARSER_DIR",
        "/Users/kirill/android_parser_service/parser_russian_version",
    )
)

LUNDA_PACKAGE = "lunda.padel.app"


class LiveContext:
    def __init__(self, android: Any, ocr: Any, out_dir: Path):
        self.android = android
        self.ocr = ocr
        self.out_dir = out_dir
        self.capture_index = 0

    def capture(self, label: str) -> tuple[dict[str, Any], str, Path] | None:
        self.capture_index += 1
        safe_label = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in label)[:40]
        screenshot_path = self.out_dir / f"{self.capture_index:04d}_{safe_label}.png"
        screenshot = self.android.take_screenshot(str(screenshot_path))
        if not screenshot:
            return None
        ocr_result = self.ocr.recognize_text(image_path=str(screenshot_path))
        if not ocr_result:
            return None
        ocr_path = screenshot_path.with_name(f"{screenshot_path.stem}_ocr.json")
        text_path = screenshot_path.with_name(f"{screenshot_path.stem}_text.txt")
        ocr_path.write_text(json.dumps(ocr_result, ensure_ascii=False, indent=2), encoding="utf-8")
        text = self.ocr.extract_text_from_result(ocr_result)
        text_path.write_text(text, encoding="utf-8")
        return ocr_result, text, screenshot_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Live Lunda phone collector")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--parser-dir", default=str(DEFAULT_OLD_PARSER_DIR))
    parser.add_argument("--device", default="")
    parser.add_argument("--out-dir", default="")
    subparsers = parser.add_subparsers(dest="command", required=True)

    schedule = subparsers.add_parser("schedule", help="Collect visible tournament schedule into DB")
    schedule.add_argument("--screens", type=int, default=30)
    schedule.add_argument("--scroll-pixels", type=int, default=440)
    schedule.add_argument("--days", type=int, default=21)
    schedule.add_argument("--target-date", default="")
    schedule.add_argument("--launch", action="store_true")
    schedule.add_argument("--no-refresh", action="store_true")

    today = subparsers.add_parser("today-participants", help="Collect participants for target day tournaments")
    today.add_argument("--screens", type=int, default=40)
    today.add_argument("--scroll-pixels", type=int, default=440)
    today.add_argument("--participants-screens", type=int, default=10)
    today.add_argument("--participants-scroll-pixels", type=int, default=420)
    today.add_argument("--target-date", default="")
    today.add_argument("--launch", action="store_true")
    today.add_argument("--no-refresh", action="store_true")
    today.add_argument("--finalize-grace-minutes", type=int, default=0)
    today.add_argument("--max-tournaments", type=int, default=0)
    today.add_argument("--lookahead-minutes", type=int, default=0)
    today.add_argument("--window-start-minutes", type=int, default=0)

    args = parser.parse_args()
    selected_device = args.device or select_adb_device()
    if not selected_device:
        print("ADB devices: none", file=sys.stderr)
        return 2

    android, ocr = create_backend(Path(args.parser_dir).expanduser().resolve(), selected_device)

    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else Path.cwd() / "work" / f"live_{args.command}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    out_dir.mkdir(parents=True, exist_ok=True)
    ctx = LiveContext(android, ocr, out_dir)

    conn = connect(args.db)
    init_db(conn)

    if args.launch:
        launch_lunda(selected_device)
        time.sleep(3)

    if args.command == "schedule":
        return collect_schedule(
            conn,
            ctx,
            screens=args.screens,
            scroll_pixels=args.scroll_pixels,
            days=args.days,
            target_date=date.fromisoformat(args.target_date) if args.target_date else None,
            refresh=not args.no_refresh,
        )
    if args.command == "today-participants":
        target = date.fromisoformat(args.target_date) if args.target_date else participant_target_date()
        return collect_today_participants(
            conn,
            ctx,
            target_date=target,
            screens=args.screens,
            scroll_pixels=args.scroll_pixels,
            participants_screens=args.participants_screens,
            participants_scroll_pixels=args.participants_scroll_pixels,
            finalize_grace_minutes=args.finalize_grace_minutes,
            max_tournaments=args.max_tournaments,
            lookahead_minutes=args.lookahead_minutes,
            window_start_minutes=args.window_start_minutes,
            refresh=not args.no_refresh,
        )

    return 2


def collect_schedule(
    conn,
    ctx: LiveContext,
    *,
    screens: int,
    scroll_pixels: int,
    days: int,
    target_date: date | None,
    refresh: bool,
) -> int:
    run_id = start_run(conn, "schedule_live")
    stats = {"screens": 0, "observations": 0, "merged_cards": 0, "upserted": 0}
    horizon = target_date or (datetime.now(MSK).date() + timedelta(days=days))
    observations: list[dict[str, Any]] = []
    consecutive_after_horizon = 0
    consecutive_unknown_screens = 0
    previous_signature = ""
    repeated_signature_count = 0
    try:
        if not ensure_tournament_list(ctx, refresh=refresh):
            stats["error"] = "tournament_list_not_reached"
            finish_run(conn, run_id, status="error", stats=stats, error="tournament_list_not_reached")
            return 3
        for screen_idx in range(screens):
            captured = ctx.capture(f"schedule_{screen_idx + 1:02d}")
            if not captured:
                continue
            ocr_result, text, _ = captured
            screen = detect_screen(text)
            if screen != "tournament_list":
                if screen == "unknown" and consecutive_unknown_screens < 2:
                    consecutive_unknown_screens += 1
                    print(f"Screen {screen_idx + 1}: screen=unknown, skipping")
                    scroll_tournament_list(ctx)
                    time.sleep(1.2)
                    continue
                print(f"Screen {screen_idx + 1}: screen={screen}, stopping")
                break
            consecutive_unknown_screens = 0

            cards = parse_visible_tournament_cards(ocr_result)
            signature = cards_signature(cards)
            if signature and signature == previous_signature:
                repeated_signature_count += 1
            else:
                repeated_signature_count = 0
                previous_signature = signature
            if repeated_signature_count >= 4:
                stats["error"] = "tournament_list_did_not_scroll"
                finish_run(conn, run_id, status="error", stats=stats, error="tournament_list_did_not_scroll")
                print("Tournament list did not scroll for 4 consecutive OCR screens; stopping")
                return 4

            list_stale = False
            for card in cards:
                card["screen_index"] = screen_idx + 1
            observations.extend(cards)
            stats["screens"] += 1
            stats["observations"] += len(cards)
            print(f"Screen {screen_idx + 1}: cards={len(cards)}")

            for card in cards:
                parsed = parse_tournament_datetime(str(card.get("date", "")), str(card.get("time", "")))
                if not parsed.tournament_date:
                    continue
                if parsed.tournament_date > horizon:
                    consecutive_after_horizon += 1
                else:
                    consecutive_after_horizon = 0

            if consecutive_after_horizon >= 3:
                print(f"Next date confirmed after {horizon.isoformat()}: {consecutive_after_horizon} cards")
                break

            merged_now = merge_visible_cards(observations)
            if _all_cards_after_horizon(merged_now, horizon):
                print(f"Horizon reached: {horizon.isoformat()}")
                break

            scroll_tournament_list(ctx)
            time.sleep(1.2)

        merged = merge_visible_cards(observations)
        stats["merged_cards"] = len(merged)
        for card in merged:
            if target_date:
                parsed = parse_tournament_datetime(str(card.get("date", "")), str(card.get("time", "")))
                if parsed.tournament_date != target_date:
                    continue
            if not is_persistable_schedule_card(card):
                continue
            upsert_tournament_from_card(conn, card, run_id=run_id, source="live_schedule")
            stats["upserted"] += 1

        (ctx.out_dir / "observations.json").write_text(json.dumps(observations, ensure_ascii=False, indent=2), encoding="utf-8")
        (ctx.out_dir / "tournaments.json").write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        finish_run(conn, run_id, stats=stats)
    except Exception as exc:
        finish_run(conn, run_id, status="error", stats=stats, error=str(exc))
        raise

    print(f"Merged tournaments: {stats['merged_cards']}")
    print(f"Upserted: {stats['upserted']}")
    print(f"Artifacts: {ctx.out_dir}")
    return 0


def cards_signature(cards: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for card in cards[:3]:
        values = [
            str(card.get("title", "")),
            str(card.get("organizer", "")),
            str(card.get("date", "")),
            str(card.get("time", "")),
            str(card.get("location", "")),
        ]
        parts.append("|".join(value.lower().strip() for value in values))
    return "||".join(parts)


def scroll_tournament_list(ctx: LiveContext) -> bool:
    width, height = get_screen_size(ctx)
    x = width // 2
    start_y = int(height * 0.76)
    end_y = int(height * 0.23)
    adb_path = getattr(ctx.android, "adb_path", "adb")
    device_id = getattr(ctx.android, "device_id", "") or select_adb_device()
    result = subprocess.run(
        [
            adb_path,
            "-s",
            device_id,
            "shell",
            "input",
            "swipe",
            str(x),
            str(start_y),
            str(x),
            str(end_y),
            "600",
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=8,
    )
    time.sleep(0.5)
    return result.returncode == 0


def collect_today_participants(
    conn,
    ctx: LiveContext,
    *,
    target_date: date,
    screens: int,
    scroll_pixels: int,
    participants_screens: int,
    participants_scroll_pixels: int,
    finalize_grace_minutes: int,
    max_tournaments: int,
    lookahead_minutes: int,
    window_start_minutes: int,
    refresh: bool,
) -> int:
    run_id = start_run(conn, "today_participants_live")
    window_start = datetime.now(MSK) + timedelta(minutes=window_start_minutes)
    window_end = window_start + timedelta(minutes=lookahead_minutes) if lookahead_minutes > 0 else None
    stats = {
        "target_date": target_date.isoformat(),
        "window_start": window_start.isoformat(timespec="seconds"),
        "window_end": window_end.isoformat(timespec="seconds") if window_end else "",
        "screens": 0,
        "cards_seen": 0,
        "cards_in_window": 0,
        "tournaments_opened": 0,
        "cancelled": 0,
        "missing": 0,
        "participant_snapshots": 0,
        "participants_seen": 0,
    }
    processed_keys: set[str] = set()
    consecutive_after_window = 0

    try:
        finalize_stats = finalize_due_tournaments(conn, grace_minutes=finalize_grace_minutes)
        stats.update({f"finalize_{key}": value for key, value in finalize_stats.items()})

        if not ensure_tournament_list(ctx, refresh=refresh):
            stats["error"] = "tournament_list_not_reached"
            finish_run(conn, run_id, status="error", stats=stats, error="tournament_list_not_reached")
            return 3
        for screen_idx in range(screens):
            captured = ctx.capture(f"today_list_{screen_idx + 1:02d}")
            if not captured:
                continue
            ocr_result, text, _ = captured
            if detect_screen(text) != "tournament_list":
                print(f"Screen {screen_idx + 1}: not tournament list, stopping")
                break

            cards = parse_visible_tournament_cards(ocr_result)
            list_stale = False
            stats["screens"] += 1
            stats["cards_seen"] += len(cards)
            print(f"Screen {screen_idx + 1}: cards={len(cards)}")

            if _first_definitely_after_target(cards, target_date):
                print(f"Next day reached after target {target_date.isoformat()}")
                break

            for card in cards:
                parsed = parse_tournament_datetime(str(card.get("date", "")), str(card.get("time", "")))
                if parsed.tournament_date != target_date:
                    continue
                if window_end and parsed.starts_at:
                    if parsed.starts_at < window_start:
                        continue
                    if parsed.starts_at > window_end:
                        consecutive_after_window += 1
                        continue
                    consecutive_after_window = 0
                    stats["cards_in_window"] += 1
                if not _card_is_safe_to_open(card):
                    continue
                tournament_key = "|".join(str(card.get(field, "")) for field in ("organizer", "date", "time", "title"))
                if tournament_key in processed_keys:
                    continue

                processed_keys.add(tournament_key)
                tournament_id = upsert_tournament_from_card(conn, card, run_id=run_id, source="live_today_list")
                open_result = open_card_and_collect_participants(
                    conn,
                    ctx,
                    tournament_id=tournament_id,
                    card=card,
                    run_id=run_id,
                    participants_screens=participants_screens,
                    participants_scroll_pixels=participants_scroll_pixels,
                    stats=stats,
                )
                if open_result == "stale":
                    list_stale = True
                    break
                if open_result:
                    ensure_tournament_list(ctx)
                    if max_tournaments and stats["tournaments_opened"] >= max_tournaments:
                        print(f"Max tournaments reached: {max_tournaments}")
                        finish_run(conn, run_id, stats=stats)
                        print(json.dumps(stats, ensure_ascii=False, indent=2))
                        print(f"Artifacts: {ctx.out_dir}")
                        return 0

            if window_end and consecutive_after_window >= 3:
                print(f"Window end reached after {window_end.isoformat(timespec='seconds')}: {consecutive_after_window} cards")
                break

            if list_stale:
                continue

            ctx.android.scroll_down(pixels=scroll_pixels)
            time.sleep(1.2)

        finish_run(conn, run_id, stats=stats)
    except Exception as exc:
        finish_run(conn, run_id, status="error", stats=stats, error=str(exc))
        raise

    print(json.dumps(stats, ensure_ascii=False, indent=2))
    print(f"Artifacts: {ctx.out_dir}")
    return 0


def open_card_and_collect_participants(
    conn,
    ctx: LiveContext,
    *,
    tournament_id: int,
    card: dict[str, Any],
    run_id: int,
    participants_screens: int,
    participants_scroll_pixels: int,
    stats: dict[str, Any],
) -> bool | str:
    tap_x = int(card.get("tap_x") or 360)
    tap_y = int(card.get("tap_y") or ((card.get("card_y_min", 500) + card.get("card_y_max", 900)) // 2))
    print(f"Opening tournament #{tournament_id}: tap={tap_x},{tap_y} {card.get('title', '')[:60]}")
    ctx.android.tap(tap_x, tap_y)
    time.sleep(2.0)

    detail = ctx.capture(f"detail_{tournament_id}")
    if not detail:
        return False
    _, detail_text, _ = detail
    detail_screen = detect_screen(detail_text)
    if "турнир отменен" in detail_text.lower():
        mark_tournament_cancelled(conn, tournament_id)
        stats["cancelled"] += 1
        guarded_back_to_list(ctx)
        return True
    if detail_screen != "tournament_detail":
        if detail_screen == "tournament_missing":
            print(f"Tournament #{tournament_id}: missing in app, skipping")
            mark_tournament_missing(conn, tournament_id)
            stats["missing"] += 1
            reset_from_missing_tournament(ctx)
            return "stale"
        print(f"Tournament #{tournament_id}: expected detail, got {detail_screen}")
        guarded_back_to_list(ctx)
        return False

    if not open_participants_section(ctx):
        print(f"Tournament #{tournament_id}: participants section not found")
        guarded_back_to_list(ctx)
        return False

    expected_count = expected_people_count(card)
    participant_records = scan_open_participants(
        ctx,
        tournament_type="team" if str(card.get("participants_unit", "")).startswith("команд") else "auto",
        expected_count=expected_count,
        max_screens=participants_screens,
        scroll_pixels=participants_scroll_pixels,
    )
    snapshot_stats = record_participant_snapshot(
        conn,
        tournament_id,
        participant_records,
        run_id=run_id,
        raw={"card": card},
    )
    stats["tournaments_opened"] += 1
    stats["participant_snapshots"] += 1
    stats["participants_seen"] += snapshot_stats["seen"]
    guarded_back_to_list(ctx)
    return True


def scan_open_participants(
    ctx: LiveContext,
    *,
    tournament_type: str,
    expected_count: int,
    max_screens: int,
    scroll_pixels: int,
) -> list[dict[str, Any]]:
    participants: list[dict[str, Any]] = []
    no_new_count = 0
    for screen_idx in range(max_screens):
        captured = ctx.capture(f"participants_{screen_idx + 1:02d}")
        if not captured:
            continue
        ocr_result, text, _ = captured
        departed_marker_seen = any(is_departed_section(line) for line in text.splitlines())
        departed_status_seen = any(is_departed_status(line) for line in text.splitlines())
        if departed_status_seen and not departed_marker_seen:
            break

        found = parse_participant_records_from_ocr(ocr_result, tournament_type=tournament_type)
        before = len(participants)
        for participant in found:
            append_unique_participant_record(participants, participant.name, participant.rating)
        added = len(participants) - before
        ratings_found = sum(1 for participant in found if participant.rating is not None)
        print(
            f"Participants screen {screen_idx + 1}: found={len(found)} ratings={ratings_found} "
            f"added={added} total={len(participants)}"
        )

        if expected_count and len(participants) >= expected_count:
            return participants[:expected_count]
        if departed_marker_seen:
            break
        no_new_count = no_new_count + 1 if added == 0 else 0
        if no_new_count >= 2:
            break

        ctx.android.scroll_down(pixels=scroll_pixels)
        time.sleep(1.2)
    return participants


def append_unique_participant_record(participants: list[dict[str, Any]], name: str, rating: float | None) -> None:
    normalized_name = " ".join(name.split())
    if not normalized_name:
        return
    for participant in participants:
        existing_name = str(participant.get("name", ""))
        existing_rating = participant.get("rating")
        if _same_rating(existing_rating, rating) and _single_token_matches_full_name(existing_name, normalized_name):
            if len(normalized_name.split()) > len(existing_name.split()):
                participant["name"] = normalized_name
            if participant.get("rating") is None and rating is not None:
                participant["rating"] = rating
            return

        merged_name = [existing_name]
        append_unique_participant(merged_name, normalized_name)
        if len(merged_name) == 1:
            participant["name"] = merged_name[0]
            if participant.get("rating") is None and rating is not None:
                participant["rating"] = rating
            return
    participants.append({"name": normalized_name, "rating": rating})


def _same_rating(left: object, right: object) -> bool:
    if left is None or right is None:
        return False
    try:
        return abs(float(left) - float(right)) < 0.005
    except (TypeError, ValueError):
        return False


def _single_token_matches_full_name(left: str, right: str) -> bool:
    left_parts = left.split()
    right_parts = right.split()
    if len(left_parts) == 1 and len(right_parts) >= 2:
        return left_parts[0].lower().replace("ё", "е") in {part.lower().replace("ё", "е") for part in right_parts}
    if len(right_parts) == 1 and len(left_parts) >= 2:
        return right_parts[0].lower().replace("ё", "е") in {part.lower().replace("ё", "е") for part in left_parts}
    return False


def open_participants_section(ctx: LiveContext, *, max_scrolls: int = 12) -> bool:
    for attempt in range(max_scrolls + 1):
        captured = ctx.capture(f"open_participants_{attempt + 1}")
        if not captured:
            continue
        ocr_result, text, _ = captured
        screen = detect_screen(text)
        if screen == "participants":
            return True

        coords = find_participants_entry_center(ocr_result)
        if coords:
            ctx.android.tap(coords["x"], coords["y"])
            time.sleep(2.0)
            check = ctx.capture("after_participants_tap")
            if not check:
                return False
            _, check_text, _ = check
            return detect_screen(check_text) == "participants"

        if screen in {"home", "tournament_list", "login", "android_recents", "tournament_missing"}:
            return False

        if attempt < max_scrolls:
            ctx.android.scroll_down(pixels=360)
            time.sleep(1.0)
    return False


def guarded_back_to_list(ctx: LiveContext, *, max_actions: int = 4) -> bool:
    for _ in range(max_actions + 1):
        captured = ctx.capture("guarded_back")
        if not captured:
            continue
        _, text, _ = captured
        screen = detect_screen(text)
        if screen == "tournament_list":
            return True
        if screen == "home":
            tap_lunda_bottom_nav(ctx, "играть")
            time.sleep(1.5)
            continue
        if screen in {"participants", "tournament_detail", "tournament_missing", "invite_players"}:
            ctx.android.go_back()
            time.sleep(1.5)
            continue
        return False
    return False


def reset_from_missing_tournament(ctx: LiveContext) -> bool:
    captured = ctx.capture("missing_tournament")
    if captured:
        ocr_result, text, _ = captured
        if detect_screen(text) == "tournament_missing":
            coords = find_home_button_center(ocr_result)
            if coords:
                ctx.android.tap(coords["x"], coords["y"])
                time.sleep(3.0)
            else:
                ctx.android.go_back()
                time.sleep(1.5)

    for attempt in range(4):
        captured = ctx.capture(f"after_missing_reset_{attempt + 1}")
        if not captured:
            continue
        _, text, _ = captured
        screen = detect_screen(text)
        if has_lunda_bottom_nav(text):
            if screen == "home":
                tap_lunda_bottom_nav(ctx, "играть")
                time.sleep(2.0)
                hide_expanded_list_header(ctx)
                return True
            if screen == "tournament_list":
                tap_lunda_bottom_nav(ctx, "главная")
                time.sleep(1.5)
                tap_lunda_bottom_nav(ctx, "играть")
                time.sleep(2.0)
                hide_expanded_list_header(ctx)
                return True
        ctx.android.go_back()
        time.sleep(1.5)

    return guarded_back_to_list(ctx)


def ensure_tournament_list(ctx: LiveContext, *, refresh: bool = False) -> bool:
    captured = ctx.capture("ensure_list")
    if not captured:
        return False
    _, text, _ = captured
    screen = detect_screen(text)
    if screen == "tournament_list":
        if refresh:
            return refresh_tournament_list(ctx)
        return True
    if screen == "home":
        tap_lunda_bottom_nav(ctx, "играть")
        time.sleep(2.0)
        hide_expanded_list_header(ctx)
        return True
    if has_lunda_bottom_nav(text):
        tap_lunda_bottom_nav(ctx, "играть")
        time.sleep(2.0)
        hide_expanded_list_header(ctx)
        if refresh:
            return refresh_tournament_list(ctx)
        return True
    if screen == "tournament_missing":
        return reset_from_missing_tournament(ctx)
    if screen in {"participants", "tournament_detail", "invite_players"}:
        if not guarded_back_to_list(ctx):
            return recover_to_tournament_list(ctx)
        if refresh:
            return refresh_tournament_list(ctx)
        return True
    return recover_to_tournament_list(ctx)


def expected_people_count(card: dict[str, Any]) -> int:
    capacity = _int_or_zero(card.get("participants_capacity"))
    current = _int_or_zero(card.get("participants_current"))
    count = capacity or current
    if not count:
        return 0
    unit = str(card.get("participants_unit", "")).lower()
    if unit.startswith("команд"):
        return count * 2
    return count


def import_old_parser_modules(parser_dir: Path):
    load_env_file(parser_dir / ".env")
    sys.path.insert(0, str(parser_dir))
    from android_parser import AndroidParser  # type: ignore
    from yandex_ocr import YandexOCR  # type: ignore

    return AndroidParser, YandexOCR


def create_backend(parser_dir: Path, selected_device: str):
    if (parser_dir / "android_parser.py").exists() and (parser_dir / "yandex_ocr.py").exists():
        AndroidParser, YandexOCR = import_old_parser_modules(parser_dir)
        android = AndroidParser(device_id=selected_device)
        android.get_devices = lambda: [selected_device]
        return android, YandexOCR()
    return ADBDevice(device_id=selected_device), YandexOCRClient()


def load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def select_adb_device() -> str:
    result = subprocess.run(["adb", "devices", "-l"], capture_output=True, text=True, check=False)
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2 and parts[1] == "device":
            return parts[0]
    return ""


def launch_lunda(device: str, adb_path: str = "adb") -> None:
    subprocess.run(
        [adb_path, "-s", device, "shell", "monkey", "-p", LUNDA_PACKAGE, "-c", "android.intent.category.LAUNCHER", "1"],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


def force_stop_lunda(device: str, adb_path: str = "adb") -> None:
    subprocess.run(
        [adb_path, "-s", device, "shell", "am", "force-stop", LUNDA_PACKAGE],
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


def restart_lunda_to_tournament_list(ctx: LiveContext) -> bool:
    device = getattr(ctx.android, "device_id", "") or select_adb_device()
    adb_path = getattr(ctx.android, "adb_path", "adb")
    if not device:
        return False
    force_stop_lunda(device, adb_path=adb_path)
    time.sleep(1.0)
    launch_lunda(device, adb_path=adb_path)
    time.sleep(3.0)
    captured = ctx.capture("after_app_restart")
    if not captured:
        return False
    _, text, _ = captured
    if detect_screen(text) == "tournament_list":
        hide_expanded_list_header(ctx)
        return True
    if has_lunda_bottom_nav(text):
        tap_lunda_bottom_nav(ctx, "играть")
        time.sleep(2.0)
        hide_expanded_list_header(ctx)
        return True
    return False


def recover_to_tournament_list(ctx: LiveContext) -> bool:
    if refresh_tournament_list(ctx):
        return True
    if guarded_back_to_list(ctx, max_actions=6):
        return refresh_tournament_list(ctx)
    return restart_lunda_to_tournament_list(ctx)


def tap_lunda_bottom_nav(ctx: LiveContext, button_name: str) -> bool:
    positions = {
        "главная": 1,
        "играть": 3,
        "рейтинг": 5,
        "чаты": 7,
        "профиль": 9,
    }
    key = button_name.lower()
    if key not in positions:
        return False
    width, height = get_screen_size(ctx)
    x = int(width * positions[key] / 10)
    y = int(height * 0.895)
    return ctx.android.tap(x, y)


def hide_expanded_list_header(ctx: LiveContext) -> None:
    captured = ctx.capture("after_refresh")
    if not captured:
        return
    _, text, _ = captured
    if detect_screen(text) == "tournament_list" and list_header_is_expanded(text):
        ctx.android.scroll_down(pixels=220)
        time.sleep(1.2)


def refresh_tournament_list(ctx: LiveContext) -> bool:
    captured = ctx.capture("before_list_refresh")
    if not captured:
        return False
    _, text, _ = captured

    if not has_lunda_bottom_nav(text):
        for _ in range(4):
            ctx.android.go_back()
            time.sleep(1.2)
            captured = ctx.capture("before_list_refresh_back")
            if not captured:
                continue
            _, text, _ = captured
            if has_lunda_bottom_nav(text):
                break
        else:
            return False

    tap_lunda_bottom_nav(ctx, "главная")
    time.sleep(3.0)
    tap_lunda_bottom_nav(ctx, "играть")
    time.sleep(2.0)
    hide_expanded_list_header(ctx)
    return True


def scroll_list_towards_top(ctx: LiveContext, *, attempts: int = 4) -> None:
    for _ in range(attempts):
        ctx.android.scroll_down(pixels=-850)
        time.sleep(0.7)


def get_screen_size(ctx: LiveContext) -> tuple[int, int]:
    target_device = ctx.android.device_id
    result = subprocess.run(
        [ctx.android.adb_path, "-s", target_device, "shell", "wm", "size"],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    match = None
    for line in result.stdout.splitlines():
        if "x" in line:
            match = line.strip().split()[-1]
    if match and "x" in match:
        try:
            width_text, height_text = match.split("x", 1)
            return int(width_text), int(height_text)
        except ValueError:
            pass
    return 720, 1520


def _card_is_safe_to_open(card: dict[str, Any]) -> bool:
    required_for_open = ("title", "organizer", "date", "time", "location", "format", "participants")
    if any(not str(card.get(field, "")).strip() for field in required_for_open):
        return False
    if card.get("near_bottom_obstruction"):
        return False
    tap_y = int(card.get("tap_y") or 0)
    return 360 <= tap_y <= 1120


def _all_cards_after_horizon(cards: list[dict[str, Any]], horizon: date) -> bool:
    dated_cards = []
    for card in cards:
        parsed = parse_tournament_datetime(str(card.get("date", "")), str(card.get("time", "")))
        if parsed.tournament_date:
            dated_cards.append(parsed.tournament_date)
    return bool(dated_cards) and min(dated_cards) > horizon


def _first_definitely_after_target(cards: list[dict[str, Any]], target_date: date) -> bool:
    dates = []
    for card in cards:
        parsed = parse_tournament_datetime(str(card.get("date", "")), str(card.get("time", "")))
        if parsed.tournament_date:
            dates.append(parsed.tournament_date)
    return bool(dates) and min(dates) > target_date


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
