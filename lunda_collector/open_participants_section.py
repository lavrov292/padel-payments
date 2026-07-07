#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from screen_nav import detect_screen, find_participants_entry_center


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
    load_env_file(parser_dir / ".env")
    sys.path.insert(0, str(parser_dir))
    from android_parser import AndroidParser  # type: ignore
    from yandex_ocr import YandexOCR  # type: ignore

    return AndroidParser, YandexOCR


def list_adb_devices() -> list[str]:
    result = subprocess.run(["adb", "devices", "-l"], check=False, capture_output=True, text=True)
    if result.returncode != 0:
        return []
    devices: list[str] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2 and parts[1] == "device":
            devices.append(parts[0])
    return devices


def main() -> int:
    parser = argparse.ArgumentParser(description="Open Lunda participants/teams section from tournament detail")
    parser.add_argument("--parser-dir", default=str(DEFAULT_OLD_PARSER_DIR))
    parser.add_argument("--device", default=None)
    parser.add_argument("--max-scrolls", type=int, default=6)
    parser.add_argument("--scroll-pixels", type=int, default=360)
    parser.add_argument("--out-dir", default=None)
    args = parser.parse_args()

    AndroidParser, YandexOCR = import_old_parser_modules(Path(args.parser_dir).expanduser().resolve())
    devices = list_adb_devices()
    if not devices:
        print("ADB devices: none")
        return 2
    selected_device = args.device or devices[0]
    android = AndroidParser(device_id=selected_device)
    android.get_devices = lambda: [selected_device]
    ocr = YandexOCR()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else Path.cwd() / "work" / f"open_participants_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    for attempt in range(args.max_scrolls + 1):
        screenshot_path = out_dir / f"screen_{attempt + 1:02d}.png"
        screenshot = android.take_screenshot(str(screenshot_path))
        if not screenshot:
            continue

        ocr_result = ocr.recognize_text(image_path=str(screenshot_path))
        if not ocr_result:
            continue

        (out_dir / f"screen_{attempt + 1:02d}_ocr.json").write_text(
            json.dumps(ocr_result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        text = ocr.extract_text_from_result(ocr_result)
        (out_dir / f"screen_{attempt + 1:02d}_text.txt").write_text(text, encoding="utf-8")

        screen = detect_screen(text)
        print(f"Attempt {attempt + 1}: screen={screen}")
        if screen == "participants":
            print("Already on participants screen")
            return 0
        if screen != "tournament_detail":
            print("Not on tournament detail; cannot open participants section")
            return 3

        coords = find_participants_entry_center(ocr_result)
        if coords:
            print(f"Tap {coords['text']}: {coords['x']},{coords['y']}")
            android.tap(coords["x"], coords["y"])
            time.sleep(2.0)

            check_screenshot = android.take_screenshot(str(out_dir / "after_tap.png"))
            if not check_screenshot:
                return 4
            check_ocr = ocr.recognize_text(image_path=str(out_dir / "after_tap.png"))
            if not check_ocr:
                return 4
            check_text = ocr.extract_text_from_result(check_ocr)
            (out_dir / "after_tap_text.txt").write_text(check_text, encoding="utf-8")
            check_screen = detect_screen(check_text)
            print(f"After tap: screen={check_screen}")
            return 0 if check_screen == "participants" else 5

        if attempt < args.max_scrolls:
            android.scroll_down(pixels=args.scroll_pixels)
            time.sleep(1.0)

    print("Participants entry was not found")
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
