from __future__ import annotations

import base64
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import requests


YANDEX_OCR_URL = "https://vision.api.cloud.yandex.net/vision/v1/batchAnalyze"


class ADBDevice:
    def __init__(self, device_id: str | None = None, adb_path: str = "adb"):
        self.device_id = device_id
        self.adb_path = adb_path

    def get_devices(self) -> list[str]:
        result = self._run(["devices", "-l"], timeout=10)
        devices: list[str] = []
        for line in result.stdout.splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 2 and parts[1] == "device":
                devices.append(parts[0])
        return devices

    def take_screenshot(self, output_path: str) -> str | None:
        target = self._target_device()
        remote_path = "/sdcard/lunda_collector_screen.png"
        result = self._run(["shell", "screencap", "-p", remote_path], device=target, timeout=20)
        if result.returncode != 0:
            return None
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        result = self._run(["pull", remote_path, str(output)], device=target, timeout=30)
        self._run(["shell", "rm", remote_path], device=target, timeout=10)
        if result.returncode != 0 or not output.exists():
            return None
        return str(output)

    def scroll_down(self, pixels: int = 500) -> bool:
        width, height = self.screen_size()
        start_x = width // 2
        abs_pixels = abs(pixels)
        if pixels > 0:
            start_y = height // 2 + abs_pixels // 2
            end_y = start_y - abs_pixels
        else:
            start_y = height // 2 - abs_pixels // 2
            end_y = start_y + abs_pixels
        result = self._run(
            ["shell", "input", "swipe", str(start_x), str(start_y), str(start_x), str(end_y), "600"],
            device=self._target_device(),
            timeout=8,
        )
        time.sleep(0.5)
        return result.returncode == 0

    def tap(self, x: int, y: int) -> bool:
        result = self._run(["shell", "input", "tap", str(x), str(y)], device=self._target_device(), timeout=8)
        time.sleep(0.5)
        return result.returncode == 0

    def go_back(self) -> bool:
        result = self._run(["shell", "input", "keyevent", "KEYCODE_BACK"], device=self._target_device(), timeout=8)
        time.sleep(0.5)
        return result.returncode == 0

    def screen_size(self) -> tuple[int, int]:
        result = self._run(["shell", "wm", "size"], device=self._target_device(), timeout=8)
        for line in result.stdout.splitlines():
            if "x" not in line:
                continue
            value = line.strip().split()[-1]
            try:
                width_text, height_text = value.split("x", 1)
                return int(width_text), int(height_text)
            except ValueError:
                continue
        return 720, 1520

    def _target_device(self) -> str:
        if self.device_id:
            return self.device_id
        devices = self.get_devices()
        if not devices:
            raise RuntimeError("No ready ADB devices")
        return devices[0]

    def _run(
        self,
        args: list[str],
        *,
        device: str | None = None,
        timeout: int = 20,
    ) -> subprocess.CompletedProcess[str]:
        cmd = [self.adb_path]
        if device:
            cmd.extend(["-s", device])
        cmd.extend(args)
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


class YandexOCRClient:
    def __init__(self, api_key: str | None = None, folder_id: str | None = None):
        self.api_key = api_key or os.getenv("YANDEX_OCR_API_KEY", "")
        self.folder_id = folder_id or os.getenv("YANDEX_OCR_FOLDER_ID", "")

    def recognize_text(self, image_path: str, image: Any = None) -> dict[str, Any] | None:
        if image is not None:
            raise NotImplementedError("PIL image input is not supported in standalone OCR client")
        if not self.api_key or not self.folder_id:
            raise RuntimeError("YANDEX_OCR_API_KEY and YANDEX_OCR_FOLDER_ID are required")

        content = base64.b64encode(Path(image_path).read_bytes()).decode("utf-8")
        response = requests.post(
            YANDEX_OCR_URL,
            headers={
                "Authorization": f"Api-Key {self.api_key}",
                "Content-Type": "application/json",
            },
            json={
                "folderId": self.folder_id,
                "analyzeSpecs": [
                    {
                        "content": content,
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

    def extract_text_from_result(self, ocr_result: dict[str, Any]) -> str:
        text_blocks: list[str] = []
        for result in ocr_result.get("results", []):
            for res in result.get("results", []):
                text_detection = res.get("textDetection", {})
                for page in text_detection.get("pages", []):
                    for block in page.get("blocks", []):
                        for line in block.get("lines", []):
                            words = line.get("words", [])
                            text = " ".join(word.get("text", "") for word in words).strip()
                            if text:
                                text_blocks.append(text)
        return "\n".join(text_blocks)
