#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from visible_cards import merge_visible_cards, parse_visible_tournament_cards


DEFAULT_OLD_PARSER_DIR = Path(
    os.environ.get(
        "LUNDA_OLD_PARSER_DIR",
        "/Users/kirill/android_parser_service/parser_russian_version",
    )
)

EXPORT_HEADERS = [
    "title",
    "organizer",
    "date",
    "time",
    "location",
    "skill_level",
    "format",
    "price",
    "participants",
    "participants_current",
    "participants_capacity",
    "participants_unit",
    "is_complete",
    "missing_fields",
    "observation_count",
    "near_top_obstruction",
    "near_bottom_obstruction",
    "tap_x",
    "tap_y",
]


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


def import_old_parser_modules(parser_dir: Path):
    if not parser_dir.exists():
        raise FileNotFoundError(f"Old parser directory not found: {parser_dir}")

    load_env_file(parser_dir / ".env")
    sys.path.insert(0, str(parser_dir))

    from android_parser import AndroidParser  # type: ignore
    from yandex_ocr import YandexOCR  # type: ignore

    return AndroidParser, YandexOCR


def list_adb_devices() -> list[str]:
    result = subprocess.run(
        ["adb", "devices", "-l"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return []

    devices: list[str] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2 and parts[1] == "device":
            devices.append(parts[0])
    return devices


def export_cards(cards: list[dict[str, Any]], output_path: Path) -> Path:
    try:
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "Tournaments"
        ws.append(EXPORT_HEADERS)

        for card in cards:
            ws.append([_export_value(card.get(header, "")) for header in EXPORT_HEADERS])

        for column_cells in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = min(max_len + 2, 70)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(output_path)
        return output_path
    except ImportError:
        csv_path = output_path.with_suffix(".csv")
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=EXPORT_HEADERS)
            writer.writeheader()
            for card in cards:
                writer.writerow({header: _export_value(card.get(header, "")) for header in EXPORT_HEADERS})
        return csv_path


def _export_value(value: Any) -> Any:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan visible Lunda tournament list screens")
    parser.add_argument("--parser-dir", default=str(DEFAULT_OLD_PARSER_DIR))
    parser.add_argument("--device", default=None)
    parser.add_argument("--screens", type=int, default=3)
    parser.add_argument("--scroll-pixels", type=int, default=180)
    parser.add_argument("--no-scroll", action="store_true")
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()

    parser_dir = Path(args.parser_dir).expanduser().resolve()
    AndroidParser, YandexOCR = import_old_parser_modules(parser_dir)

    devices = list_adb_devices()
    if not devices:
        print("ADB devices: none")
        return 2

    selected_device = args.device or devices[0]
    if selected_device not in devices:
        print(f"Requested ADB device is not ready: {selected_device}")
        return 2

    android = AndroidParser(device_id=selected_device)
    android.get_devices = lambda: [selected_device]
    ocr = YandexOCR()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else Path.cwd() / "work" / f"list_scan_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    observations: list[dict[str, Any]] = []

    for screen_idx in range(args.screens):
        screenshot_path = out_dir / f"screen_{screen_idx + 1:02d}.png"
        screenshot = android.take_screenshot(str(screenshot_path))
        if not screenshot:
            print(f"Screen {screen_idx + 1}: screenshot failed")
            continue

        ocr_result = ocr.recognize_text(image_path=str(screenshot_path))
        if not ocr_result:
            print(f"Screen {screen_idx + 1}: OCR failed")
            continue

        (out_dir / f"screen_{screen_idx + 1:02d}_ocr.json").write_text(
            json.dumps(ocr_result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        cards = parse_visible_tournament_cards(ocr_result)
        for card in cards:
            card["screen_index"] = screen_idx + 1
        observations.extend(cards)
        print(f"Screen {screen_idx + 1}: cards={len(cards)}")

        if args.no_scroll or screen_idx == args.screens - 1:
            continue

        android.scroll_down(pixels=args.scroll_pixels)
        time.sleep(1.2)

    merged = merge_visible_cards(observations)

    observations_path = out_dir / "observations.json"
    observations_path.write_text(json.dumps(observations, ensure_ascii=False, indent=2), encoding="utf-8")

    merged_path = out_dir / "tournaments.json"
    merged_path.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    export_path = export_cards(merged, out_dir / "tournaments.xlsx")

    complete_count = sum(1 for card in merged if card.get("is_complete"))
    print(f"Observations: {len(observations)}")
    print(f"Merged tournaments: {len(merged)}")
    print(f"Complete tournaments: {complete_count}")
    print(f"JSON: {merged_path}")
    print(f"Export: {export_path}")

    for idx, card in enumerate(merged[:10], 1):
        missing = ",".join(card.get("missing_fields", []))
        print(
            f"{idx}. {card.get('title') or '[no title]'} | "
            f"{card.get('organizer')} | {card.get('date')} {card.get('time')} | "
            f"{card.get('participants') or 'no participants'} | missing={missing}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
