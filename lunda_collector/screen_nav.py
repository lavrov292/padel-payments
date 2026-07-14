from __future__ import annotations

from typing import Any, Callable
import re

from participants_parser import is_departed_section, is_departed_status
from visible_cards import OCRLine, extract_ocr_lines


def detect_screen(text: str) -> str:
    lower = text.lower()
    lines = [line.strip().lower() for line in text.splitlines() if line.strip()]
    top_text = "\n".join(lines[:8])
    has_bottom_nav = has_lunda_bottom_nav(text)

    if "закрыть все" in lower:
        return "android_recents"

    if "турнир не найден" in lower:
        return "tournament_missing"

    if "пригласить игроков" in lower and ("найти игрока" in lower or "только мои напарники" in lower):
        return "invite_players"

    if "игры/турниры" in lower and ("тренировки" in lower or "утро" in lower or "вечер" in lower):
        return "tournament_list"

    if has_bottom_nav and "турнир" in lower and "организатор" in lower:
        return "tournament_list"

    if has_bottom_nav and re.search(r"\b(?:пн|вт|ср|чт|пт|сб|вс)\s+\d{1,2}\s+[а-яё]+", lower) and re.search(r"\d{1,2}:\d{2}", lower):
        return "tournament_list"

    if has_bottom_nav and ("ваш город" in lower or "календарь" in lower or "приглашения" in lower):
        return "home"

    if "войти" in lower and "зарегистрироваться" in lower:
        return "login"

    if ("команды" in top_text or "участники" in top_text) and "организатор" not in lower:
        return "participants"

    if any(is_departed_section(line) or is_departed_status(line) for line in lines) and "организатор" not in lower:
        return "participants"

    if "турнир" in top_text and ("ответственный" in lower or "команды" in lower or "участники" in lower):
        return "tournament_detail"

    if "организатор" in lower and ("турнир" in lower or "уровень" in lower):
        return "tournament_detail"

    return "unknown"


def find_first_line_center(
    ocr_result: dict[str, Any],
    predicate: Callable[[OCRLine], bool],
) -> dict[str, int] | None:
    for line in extract_ocr_lines(ocr_result):
        if predicate(line):
            return {
                "x": (line.x_min + line.x_max) // 2,
                "y": (line.y_min + line.y_max) // 2,
                "text": line.text,
            }
    return None


def find_participants_entry_center(ocr_result: dict[str, Any]) -> dict[str, int] | None:
    def is_entry(line: OCRLine) -> bool:
        lower = line.text.lower().strip()
        return lower in {"команды", "участники"}

    return find_first_line_center(ocr_result, is_entry)


def find_home_button_center(ocr_result: dict[str, Any]) -> dict[str, int] | None:
    def is_home_button(line: OCRLine) -> bool:
        return "на главную" in line.text.lower()

    return find_first_line_center(ocr_result, is_home_button)


def list_header_is_expanded(text: str) -> bool:
    lower = text.lower()
    return "только в любимых" in lower or "рядом с" in lower or "изменить" in lower and "город" in lower


def has_lunda_bottom_nav(text: str) -> bool:
    lower = text.lower()
    return "главная" in lower and "играть" in lower and "рейтинг" in lower
