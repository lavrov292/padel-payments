#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from visible_cards import parse_visible_tournament_cards


YANDEX_OCR_URL = "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze"


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


def run_adb(args: list[str], device: str | None = None, timeout: int = 20) -> subprocess.CompletedProcess[str]:
    cmd = ["adb"]
    if device:
        cmd.extend(["-s", device])
    cmd.extend(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def adb_connect(adb_host: str | None) -> str:
    if adb_host:
        result = run_adb(["connect", adb_host], timeout=15)
        output = f"{result.stdout}\n{result.stderr}".strip()
        print(output)
        if result.returncode != 0 or "failed" in output.lower() or "unable" in output.lower():
            raise RuntimeError(f"ADB connect failed: {output}")
        return adb_host

    devices = list_ready_devices()
    if not devices:
        raise RuntimeError("No ready ADB devices. Pass --adb-host or connect USB.")
    return devices[0]


def list_ready_devices() -> list[str]:
    result = run_adb(["devices", "-l"], timeout=10)
    devices: list[str] = []
    for line in result.stdout.splitlines()[1:]:
        parts = line.split()
        if len(parts) >= 2 and parts[1] == "device":
            devices.append(parts[0])
    return devices


def take_screenshot(device: str, output_path: Path) -> None:
    remote_path = "/sdcard/lunda_remote_smoke.png"

    result = run_adb(["shell", "screencap", "-p", remote_path], device=device, timeout=20)
    if result.returncode != 0:
        raise RuntimeError(f"screencap failed: {result.stderr}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    result = run_adb(["pull", remote_path, str(output_path)], device=device, timeout=30)
    if result.returncode != 0:
        raise RuntimeError(f"adb pull failed: {result.stderr}")

    run_adb(["shell", "rm", remote_path], device=device, timeout=10)


def recognize_text(image_path: Path) -> dict[str, Any]:
    api_key = os.getenv("YANDEX_OCR_API_KEY", "")
    folder_id = os.getenv("YANDEX_OCR_FOLDER_ID", "")
    if not api_key or not folder_id:
        raise RuntimeError("YANDEX_OCR_API_KEY and YANDEX_OCR_FOLDER_ID are required")

    image_base64 = base64.b64encode(image_path.read_bytes()).decode("utf-8")
    response = requests.post(
        YANDEX_OCR_URL,
        headers={
            "Authorization": f"Api-Key {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "folderId": folder_id,
            "analyzeSpecs": [
                {
                    "content": image_base64,
                    "features": [
                        {
                            "type": "TEXT_DETECTION",
                            "textDetectionConfig": {"languageCodes": ["ru", "en"]},
                        }
                    ],
                }
            ],
        },
        timeout=40,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Yandex OCR failed: {response.status_code} {response.text}")
    return response.json()


def extract_text(ocr_result: dict[str, Any]) -> str:
    text_blocks: list[str] = []
    for result in ocr_result.get("results", []):
        for res in result.get("results", []):
            text_detection = res.get("textDetection", {})
            for page in text_detection.get("pages", []):
                for block in page.get("blocks", []):
                    for line in block.get("lines", []):
                        words = line.get("words", [])
                        line_text = " ".join(word.get("text", "") for word in words).strip()
                        if line_text:
                            text_blocks.append(line_text)
    return "\n".join(text_blocks)


def main() -> int:
    parser = argparse.ArgumentParser(description="Remote ADB smoke test for phone/VPS setup")
    parser.add_argument("--adb-host", default=os.getenv("PHONE_ADB_HOST", ""))
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--out-dir", default="")
    args = parser.parse_args()

    load_env_file(Path(args.env_file).expanduser())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else Path.cwd() / "work" / f"remote_adb_smoke_{timestamp}"
    out_dir.mkdir(parents=True, exist_ok=True)

    device = adb_connect(args.adb_host or None)
    print(f"ADB device: {device}")

    screenshot_path = out_dir / "screen.png"
    take_screenshot(device, screenshot_path)
    print(f"Screenshot: {screenshot_path}")

    ocr_result = recognize_text(screenshot_path)
    (out_dir / "ocr_raw.json").write_text(json.dumps(ocr_result, ensure_ascii=False, indent=2), encoding="utf-8")

    text = extract_text(ocr_result)
    (out_dir / "ocr_text.txt").write_text(text, encoding="utf-8")

    cards = parse_visible_tournament_cards(ocr_result)
    (out_dir / "visible_tournaments.json").write_text(json.dumps(cards, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OCR text: {out_dir / 'ocr_text.txt'}")
    print(f"Visible tournaments parsed: {len(cards)}")
    for idx, card in enumerate(cards[:5], 1):
        print(
            f"{idx}. {card.get('title', '')} | {card.get('organizer', '')} | "
            f"{card.get('date', '')} {card.get('time', '')} | {card.get('participants', '')}"
        )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
