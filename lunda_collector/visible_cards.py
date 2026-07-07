from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class OCRLine:
    text: str
    y_min: int
    y_max: int
    x_min: int
    x_max: int
    words: list[dict[str, Any]]


FORMAT_KEYWORDS = (
    "americano",
    "американо",
    "mexicano",
    "мексикано",
    "round robin",
    "king",
    "escalera",
    "парный",
)

BOTTOM_NAV_LABELS = {
    "главная",
    "играть",
    "рейтинг",
    "чаты",
    "профиль",
}

REQUIRED_CARD_FIELDS = (
    "title",
    "organizer",
    "date",
    "time",
    "location",
    "format",
    "price",
    "participants",
)

BOTTOM_OBSTRUCTION_Y = 1130
TOP_OBSTRUCTION_Y = 430


def extract_ocr_lines(ocr_result: dict[str, Any]) -> list[OCRLine]:
    lines: list[OCRLine] = []

    for result in ocr_result.get("results", []):
        for res in result.get("results", []):
            text_detection = res.get("textDetection", {})
            for page in text_detection.get("pages", []):
                for block in page.get("blocks", []):
                    for line in block.get("lines", []):
                        words = line.get("words", [])
                        text = " ".join(word.get("text", "") for word in words).strip()
                        if not text:
                            continue

                        vertices = line.get("boundingBox", {}).get("vertices", [])
                        if not vertices and words:
                            vertices = words[0].get("boundingBox", {}).get("vertices", [])
                        if not vertices:
                            continue

                        x_coords: list[float] = []
                        y_coords: list[float] = []
                        for vertex in vertices:
                            try:
                                x_coords.append(float(vertex.get("x", 0)))
                                y_coords.append(float(vertex.get("y", 0)))
                            except (TypeError, ValueError):
                                continue

                        if not x_coords or not y_coords:
                            continue

                        lines.append(
                            OCRLine(
                                text=text,
                                x_min=int(min(x_coords)),
                                x_max=int(max(x_coords)),
                                y_min=int(min(y_coords)),
                                y_max=int(max(y_coords)),
                                words=words,
                            )
                        )

    return _sort_lines_in_reading_order(lines)


def _sort_lines_in_reading_order(lines: list[OCRLine]) -> list[OCRLine]:
    sorted_by_y = sorted(lines, key=lambda item: (item.y_min, item.x_min))
    groups: list[list[OCRLine]] = []

    for line in sorted_by_y:
        if not groups or abs(line.y_min - groups[-1][0].y_min) > 18:
            groups.append([line])
        else:
            groups[-1].append(line)

    ordered: list[OCRLine] = []
    for group in groups:
        ordered.extend(sorted(group, key=lambda item: item.x_min))

    return ordered


def parse_visible_tournament_cards(ocr_result: dict[str, Any]) -> list[dict[str, Any]]:
    lines = extract_ocr_lines(ocr_result)
    starts = _find_card_starts(lines)
    cards: list[dict[str, Any]] = []

    for pos, start_idx in enumerate(starts):
        next_start_idx = starts[pos + 1] if pos + 1 < len(starts) else len(lines)
        end_idx = next_start_idx

        for idx in range(start_idx + 1, next_start_idx):
            if _is_bottom_nav(lines[idx].text):
                end_idx = idx
                break

        card_lines = lines[start_idx:end_idx]
        parsed = _parse_card(card_lines)
        if parsed:
            cards.append(parsed)

    return cards


def _find_card_starts(lines: list[OCRLine]) -> list[int]:
    starts: list[int] = []

    for idx, line in enumerate(lines):
        if _is_tournament_start(line.text):
            starts.append(idx)
            continue

        if not _is_organizer_line(line.text):
            continue

        has_near_title = False
        for prev_idx in range(idx - 1, -1, -1):
            if line.y_min - lines[prev_idx].y_min > 260:
                break
            if _is_tournament_start(lines[prev_idx].text):
                has_near_title = True
                break

        if not has_near_title:
            starts.append(idx)

    return starts


def _parse_card(lines: list[OCRLine]) -> dict[str, Any] | None:
    if not lines:
        return None

    title_parts: list[str] = []
    organizer_parts: list[str] = []
    location_parts: list[str] = []
    date_value = ""
    time_value = ""
    category = ""
    format_value = ""
    price = ""
    participants = ""
    participants_current = ""
    participants_capacity = ""
    participants_unit = ""
    tap_x = 360
    tap_y = (lines[0].y_min + lines[-1].y_max) // 2

    mode = "title"
    for line in lines:
        text = line.text.strip()
        lower = text.lower()

        if not text or _is_bottom_nav(text):
            continue

        price_match = _price_match(text)
        participants_match = _participants_match(text)

        if _is_organizer_line(text):
            mode = "organizer"
            value = re.sub(r"(?i).*?организато\w*\s*", "", text).strip()
            if value:
                organizer_parts.append(value)
            continue

        if _is_datetime_line(text):
            date_value, time_value = _split_datetime(text)
            mode = "location"
            continue

        if mode == "title":
            title_parts.append(text)
            continue

        if participants_match:
            participants_current, participants_capacity, raw_unit = participants_match.groups()
            participants_unit = "команд" if raw_unit.lower().startswith("коман") else "игроков"
            participants = f"{participants_current}/{participants_capacity} {participants_unit}"
            continue

        if price_match:
            price = f"{price_match.group(1).replace(' ', '')} ₽"
            tap_x = (line.x_min + line.x_max) // 2
            tap_y = (line.y_min + line.y_max) // 2
            continue

        if _is_category_line(text):
            category = text
            mode = "after_category"
            continue

        if _is_format_line(text):
            format_value = text
            mode = "after_format"
            continue

        if mode == "title":
            title_parts.append(text)
        elif mode == "organizer":
            organizer_parts.append(text)
        elif mode == "location":
            location_parts.append(text)

    title = _join_wrapped(title_parts)
    organizer = _join_wrapped(organizer_parts)
    location = _join_wrapped(location_parts)

    if not title and not organizer:
        return None

    card_y_min = min(line.y_min for line in lines)
    card_y_max = max(line.y_max for line in lines)

    card = {
        "title": title,
        "organizer": organizer,
        "date": date_value,
        "time": time_value,
        "location": location,
        "skill_level": category,
        "format": format_value,
        "price": price,
        "participants": participants,
        "participants_current": participants_current,
        "participants_capacity": participants_capacity,
        "participants_unit": participants_unit,
        "tap_x": tap_x,
        "tap_y": tap_y,
        "card_y_min": card_y_min,
        "card_y_max": card_y_max,
        "near_top_obstruction": card_y_min < TOP_OBSTRUCTION_Y,
        "near_bottom_obstruction": card_y_max > BOTTOM_OBSTRUCTION_Y,
        "raw_lines": [asdict(line) for line in lines],
    }
    _add_card_status(card)
    return card


def merge_visible_cards(cards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged_by_key: dict[str, dict[str, Any]] = {}
    loose_cards: list[dict[str, Any]] = []

    for card in cards:
        key = build_merge_key(card)
        if not key:
            if card.get("is_complete"):
                loose_cards.append(card)
            continue

        existing = merged_by_key.get(key)
        if not existing:
            copy = dict(card)
            copy["observation_count"] = 1
            merged_by_key[key] = copy
            continue

        _merge_card_into(existing, card)

    merged = list(merged_by_key.values()) + loose_cards
    merged.sort(key=lambda item: (item.get("date", ""), item.get("time", ""), item.get("card_y_min", 0)))
    return merged


def build_merge_key(card: dict[str, Any]) -> str:
    organizer = str(card.get("organizer", "")).strip()
    date_value = str(card.get("date", "")).strip()
    time_value = str(card.get("time", "")).strip()
    if organizer and date_value and time_value:
        return _normalize_key("|".join([organizer, date_value, time_value]))

    primary_fields = ("organizer", "date", "time", "location", "price", "format")
    primary_values = [str(card.get(field, "")).strip() for field in primary_fields]
    if sum(bool(value) for value in primary_values) >= 3:
        return _normalize_key("|".join(primary_values))

    fallback_fields = ("title", "organizer", "date", "time")
    fallback_values = [str(card.get(field, "")).strip() for field in fallback_fields]
    if sum(bool(value) for value in fallback_values) >= 2:
        return _normalize_key("|".join(fallback_values))

    return ""


def _merge_card_into(existing: dict[str, Any], new_card: dict[str, Any]) -> None:
    for field in REQUIRED_CARD_FIELDS:
        if not existing.get(field) and new_card.get(field):
            existing[field] = new_card[field]

    for field in ("participants_current", "participants_capacity", "participants_unit"):
        if not existing.get(field) and new_card.get(field):
            existing[field] = new_card[field]

    if new_card.get("tap_x") and new_card.get("tap_y"):
        existing["tap_x"] = new_card["tap_x"]
        existing["tap_y"] = new_card["tap_y"]

    existing["card_y_min"] = min(existing.get("card_y_min", new_card.get("card_y_min", 0)), new_card.get("card_y_min", 0))
    existing["card_y_max"] = max(existing.get("card_y_max", new_card.get("card_y_max", 0)), new_card.get("card_y_max", 0))
    existing["near_top_obstruction"] = bool(existing.get("near_top_obstruction")) or bool(new_card.get("near_top_obstruction"))
    existing["near_bottom_obstruction"] = bool(existing.get("near_bottom_obstruction")) or bool(new_card.get("near_bottom_obstruction"))
    existing["observation_count"] = int(existing.get("observation_count", 1)) + 1
    existing.setdefault("raw_observations", [])
    existing["raw_observations"].append(new_card)
    _add_card_status(existing)


def _add_card_status(card: dict[str, Any]) -> None:
    missing_fields = [field for field in REQUIRED_CARD_FIELDS if not card.get(field)]
    card["missing_fields"] = missing_fields
    card["is_complete"] = not missing_fields


def _is_tournament_start(text: str) -> bool:
    lower = text.lower().strip()
    return (
        lower.startswith("турнир")
        or lower.startswith("женский турнир")
        or lower.startswith("мужской турнир")
    )


def _is_organizer_line(text: str) -> bool:
    return text.lower().strip().startswith("организато")


def _is_bottom_nav(text: str) -> bool:
    return text.lower().strip() in BOTTOM_NAV_LABELS


def _is_datetime_line(text: str) -> bool:
    return "|" in text and bool(re.search(r"\d{1,2}:\d{2}", text))


def _split_datetime(text: str) -> tuple[str, str]:
    left, _, right = text.partition("|")
    return left.strip(), right.strip()


def _is_category_line(text: str) -> bool:
    normalized = text.strip()
    return bool(
        re.fullmatch(r"[A-D][+-]?", normalized, re.IGNORECASE)
        or re.search(r"[A-D][+-]?\s*[.…]+\s*[A-D][+-]?", text, re.IGNORECASE)
        or re.search(r"[A-D][+-]?\s*[-–—]\s*[A-D][+-]?", text, re.IGNORECASE)
        or re.search(r"\(\d+[.,]\d+.*\d+[.,]\d+\)", text)
    )


def _is_format_line(text: str) -> bool:
    lower = text.lower()
    return any(keyword in lower for keyword in FORMAT_KEYWORDS)


def _price_match(text: str) -> re.Match[str] | None:
    return re.search(r"(\d[\d\s]*)\s*(?:₽|р|руб)", text, re.IGNORECASE)


def _participants_match(text: str) -> re.Match[str] | None:
    return re.search(r"(\d+)\s*/\s*(\d+)\s*(игр\w*|коман\w*)", text, re.IGNORECASE)


def _join_wrapped(parts: list[str]) -> str:
    text = " ".join(part.strip() for part in parts if part.strip())
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"-\s+", "-", text)
    text = text.replace("« ", "«").replace(" »", "»")
    return text


def _normalize_key(text: str) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", text.lower())
