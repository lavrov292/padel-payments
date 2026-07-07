#!/usr/bin/env python3
"""
Smoke test for a real Android phone running Lunda Padel.

It reuses the proven Russian parser modules from the old local project:
  /Users/kirill/android_parser_service/parser_russian_version

What it does:
  1. Checks that ADB can see a device.
  2. Takes a screenshot.
  3. Sends the screenshot to Yandex OCR.
  4. Saves OCR text/raw JSON.
  5. Parses visible tournament cards.
  6. Exports visible tournament metadata to XLSX, or CSV if openpyxl is unavailable.

This script intentionally does not tap or scroll. It is a safe first test.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from visible_cards import parse_visible_tournament_cards


DEFAULT_OLD_PARSER_DIR = Path(
    os.environ.get(
        "LUNDA_OLD_PARSER_DIR",
        "/Users/kirill/android_parser_service/parser_russian_version",
    )
)


def load_env_file(env_path: Path) -> None:
    """Load simple KEY=VALUE lines without overriding existing environment."""
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
    """Import old parser modules after loading its local .env."""
    if not parser_dir.exists():
        raise FileNotFoundError(f"Old parser directory not found: {parser_dir}")

    load_env_file(parser_dir / ".env")
    sys.path.insert(0, str(parser_dir))

    from android_parser import AndroidParser  # type: ignore
    from yandex_ocr import YandexOCR  # type: ignore

    return AndroidParser, YandexOCR


def list_adb_devices() -> list[str]:
    """Return device ids that are currently ready in ADB."""
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


def export_tournaments(tournaments: list[dict[str, Any]], output_path: Path) -> Path:
    headers = [
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
        "tap_x",
        "tap_y",
        "card_y_min",
        "card_y_max",
    ]

    try:
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "Visible tournaments"
        ws.append(headers)

        for tournament in tournaments:
            ws.append([format_export_value(tournament.get(header, "")) for header in headers])

        for column_cells in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in column_cells)
            ws.column_dimensions[column_cells[0].column_letter].width = min(max_len + 2, 60)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(output_path)
        return output_path
    except ImportError:
        csv_path = output_path.with_suffix(".csv")
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for tournament in tournaments:
                writer.writerow({header: format_export_value(tournament.get(header, "")) for header in headers})
        return csv_path


def format_export_value(value: Any) -> Any:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value)
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Safe phone/OCR smoke test for Lunda Padel")
    parser.add_argument(
        "--parser-dir",
        default=str(DEFAULT_OLD_PARSER_DIR),
        help="Path to old parser_russian_version directory",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="ADB device id. Optional if only one device is connected.",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output directory. Defaults to ./work/phone_smoke_YYYYMMDD_HHMMSS",
    )
    args = parser.parse_args()

    parser_dir = Path(args.parser_dir).expanduser().resolve()
    AndroidParser, YandexOCR = import_old_parser_modules(parser_dir)

    devices = list_adb_devices()
    if not devices:
        print("ADB devices: none")
        print("Connect the phone, enable USB debugging, and approve the RSA prompt.")
        return 2

    print(f"ADB devices: {', '.join(devices)}")
    selected_device = args.device or devices[0]
    if selected_device not in devices:
        print(f"Requested ADB device is not ready: {selected_device}")
        return 2

    android = AndroidParser(device_id=selected_device)
    android.get_devices = lambda: [selected_device]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else Path.cwd() / "work" / f"phone_smoke_{timestamp}"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    screenshot_path = out_dir / "screen.png"
    screenshot = android.take_screenshot(str(screenshot_path))
    if not screenshot:
        print("Failed to take screenshot")
        return 3
    print(f"Screenshot: {screenshot_path}")

    ocr = YandexOCR()
    ocr_result = ocr.recognize_text(image_path=str(screenshot_path))
    if not ocr_result:
        print("OCR failed")
        return 4

    raw_json_path = out_dir / "ocr_raw.json"
    raw_json_path.write_text(json.dumps(ocr_result, ensure_ascii=False, indent=2), encoding="utf-8")

    text = ocr.extract_text_from_result(ocr_result)
    text_path = out_dir / "ocr_text.txt"
    text_path.write_text(text, encoding="utf-8")

    tournaments = parse_visible_tournament_cards(ocr_result)

    tournaments_json_path = out_dir / "visible_tournaments.json"
    tournaments_json_path.write_text(
        json.dumps(tournaments, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    export_path = export_tournaments(tournaments, out_dir / "visible_tournaments.xlsx")

    print(f"OCR text: {text_path}")
    print(f"Visible tournaments parsed: {len(tournaments)}")
    print(f"Tournaments JSON: {tournaments_json_path}")
    print(f"Tournaments export: {export_path}")

    if tournaments:
        print("\nFirst visible tournaments:")
        for idx, tournament in enumerate(tournaments[:5], 1):
            title = tournament.get("title", "")
            organizer = tournament.get("organizer", "")
            date = tournament.get("date", "")
            time_value = tournament.get("time", "")
            print(f"{idx}. {title} | {organizer} | {date} {time_value}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
