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

from participants_parser import is_departed_section, is_departed_status, parse_participants_from_ocr


DEFAULT_OLD_PARSER_DIR = Path(
    os.environ.get(
        "LUNDA_OLD_PARSER_DIR",
        "/Users/kirill/android_parser_service/parser_russian_version",
    )
)


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


def export_participants(participants: list[str], output_path: Path) -> Path:
    try:
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "Participants"
        ws.append(["#", "participant"])
        for idx, participant in enumerate(participants, 1):
            ws.append([idx, participant])
        ws.column_dimensions["A"].width = 6
        ws.column_dimensions["B"].width = 40
        output_path.parent.mkdir(parents=True, exist_ok=True)
        wb.save(output_path)
        return output_path
    except ImportError:
        csv_path = output_path.with_suffix(".csv")
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["#", "participant"])
            for idx, participant in enumerate(participants, 1):
                writer.writerow([idx, participant])
        return csv_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Scan participants from opened Lunda tournament detail")
    parser.add_argument("--parser-dir", default=str(DEFAULT_OLD_PARSER_DIR))
    parser.add_argument("--device", default=None)
    parser.add_argument("--type", choices=["auto", "team", "personal"], default="auto")
    parser.add_argument("--screens", type=int, default=8)
    parser.add_argument("--scroll-pixels", type=int, default=420)
    parser.add_argument("--expected-count", type=int, default=0)
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
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else Path.cwd() / "work" / f"participants_scan_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    participants: list[str] = []
    no_new_count = 0

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
        text = ocr.extract_text_from_result(ocr_result)
        (out_dir / f"screen_{screen_idx + 1:02d}_text.txt").write_text(text, encoding="utf-8")
        departed_marker_seen = any(is_departed_section(line) for line in text.splitlines())
        departed_status_seen = any(is_departed_status(line) for line in text.splitlines())

        if departed_status_seen and not departed_marker_seen:
            print(f"Screen {screen_idx + 1}: departed players continuation detected; stopping")
            break

        found = parse_participants_from_ocr(ocr_result, tournament_type=args.type)
        before_count = len(participants)
        for participant in found:
            if participant not in participants:
                participants.append(participant)
        added = len(participants) - before_count
        print(f"Screen {screen_idx + 1}: found={len(found)} added={added} total={len(participants)}")

        if added == 0:
            no_new_count += 1
        else:
            no_new_count = 0
        if args.expected_count and len(participants) >= args.expected_count:
            participants = participants[: args.expected_count]
            print(f"Expected count reached: {args.expected_count}")
            break
        if departed_marker_seen:
            print("Departed players section reached; stopping active participants scan")
            break
        if no_new_count >= 2:
            break

        android.scroll_down(pixels=args.scroll_pixels)
        time.sleep(1.2)

    participants_path = out_dir / "participants.json"
    participants_path.write_text(json.dumps(participants, ensure_ascii=False, indent=2), encoding="utf-8")
    export_path = export_participants(participants, out_dir / "participants.xlsx")

    print(f"Participants: {len(participants)}")
    print(f"JSON: {participants_path}")
    print(f"Export: {export_path}")
    for idx, participant in enumerate(participants, 1):
        print(f"{idx}. {participant}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
