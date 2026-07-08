from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from visible_cards import extract_ocr_lines


@dataclass
class OCRWord:
    text: str
    x_min: int
    x_max: int
    y_min: int
    y_max: int

    @property
    def x_center(self) -> int:
        return (self.x_min + self.x_max) // 2

    @property
    def y_center(self) -> int:
        return (self.y_min + self.y_max) // 2


STOP_WORDS = {
    "турнир",
    "организатор",
    "ответственный",
    "команды",
    "команда",
    "участники",
    "участник",
    "игроки",
    "покинувшие",
    "покинувший",
    "вышел",
    "исключен",
    "владельцем",
    "парный",
    "round",
    "robin",
    "americano",
    "mexicano",
    "king",
    "уровень",
    "онлайн",
    "оплата",
    "участия",
    "приложении",
    "лунда",
    "турнира",
}


def parse_participants_from_ocr(ocr_result: dict[str, Any], tournament_type: str = "auto") -> list[str]:
    lines = extract_ocr_lines(ocr_result)
    text_lower = "\n".join(line.text.lower() for line in lines)

    if tournament_type == "auto":
        tournament_type = "team" if "команды" in text_lower or "команд" in text_lower else "personal"

    header_y = _find_participants_header_y(lines, tournament_type)
    if header_y is None:
        return []

    return _parse_participant_rows(extract_words(ocr_result), header_y) or _parse_participant_lines(lines, header_y)


def extract_words(ocr_result: dict[str, Any]) -> list[OCRWord]:
    result_words: list[OCRWord] = []

    for result in ocr_result.get("results", []):
        for res in result.get("results", []):
            text_detection = res.get("textDetection", {})
            for page in text_detection.get("pages", []):
                for block in page.get("blocks", []):
                    for line in block.get("lines", []):
                        for word in line.get("words", []):
                            text = word.get("text", "").strip()
                            vertices = word.get("boundingBox", {}).get("vertices", [])
                            if not text or not vertices:
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

                            result_words.append(
                                OCRWord(
                                    text=text,
                                    x_min=int(min(x_coords)),
                                    x_max=int(max(x_coords)),
                                    y_min=int(min(y_coords)),
                                    y_max=int(max(y_coords)),
                                )
                            )

    result_words.sort(key=lambda item: (item.y_center, item.x_center))
    return result_words


def _find_participants_header_y(lines, tournament_type: str) -> int | None:
    header_patterns = ["команды"] if tournament_type == "team" else ["участники турнира", "участники"]

    for line in lines:
        lower = line.text.lower()
        if any(pattern in lower for pattern in header_patterns):
            return line.y_max

    if tournament_type == "team":
        for line in lines:
            if "команд" in line.text.lower():
                return line.y_max

    return None


def _parse_participant_lines(lines, header_y: int) -> list[str]:
    candidates: list[dict[str, Any]] = []
    participants: list[str] = []

    for line in lines:
        if line.y_min <= header_y + 20:
            continue

        if is_departed_section(line.text):
            break

        candidate = _line_to_name_candidate(line.text)
        if not candidate:
            continue
        tokens = candidate.split()
        if len(tokens) == 1 and line.y_min <= header_y + 100:
            continue

        candidates.append(
            {
                "name": candidate,
                "tokens": tokens,
                "y_min": line.y_min,
                "y_max": line.y_max,
                "x_min": line.x_min,
            }
        )

    idx = 0
    while idx < len(candidates):
        current = candidates[idx]
        current_tokens = current["tokens"]

        if len(current_tokens) >= 2:
            _append_unique(participants, current["name"])
            idx += 1
            continue

        if idx + 1 < len(candidates):
            next_item = candidates[idx + 1]
            next_tokens = next_item["tokens"]
            y_gap = next_item["y_min"] - current["y_max"]
            if len(next_tokens) == 1 and 0 <= y_gap <= 95:
                _append_unique(participants, f"{current['name']} {next_item['name']}")
                idx += 2
                continue

        _append_unique(participants, current["name"])
        idx += 1

    return participants


def _parse_participant_rows(words: list[OCRWord], header_y: int) -> list[str]:
    candidates: list[dict[str, Any]] = []
    participants: list[str] = []

    for row in _group_words_into_rows(words):
        if not row:
            continue
        y_min = min(word.y_min for word in row)
        y_max = max(word.y_max for word in row)
        if y_min <= header_y + 20:
            continue

        row_text = _participant_row_text(row)
        if is_departed_section(row_text):
            break

        candidate = _line_to_name_candidate(row_text)
        if not candidate:
            continue
        tokens = candidate.split()
        if len(tokens) == 1 and y_min <= header_y + 100:
            continue

        candidates.append(
            {
                "name": candidate,
                "tokens": tokens,
                "y_min": y_min,
                "y_max": y_max,
                "x_min": min(word.x_min for word in row),
            }
        )

    idx = 0
    while idx < len(candidates):
        current = candidates[idx]
        current_tokens = current["tokens"]

        if len(current_tokens) >= 2:
            _append_unique(participants, current["name"])
            idx += 1
            continue

        if idx + 1 < len(candidates):
            next_item = candidates[idx + 1]
            next_tokens = next_item["tokens"]
            y_gap = next_item["y_min"] - current["y_max"]
            if len(next_tokens) == 1 and 0 <= y_gap <= 95:
                _append_unique(participants, f"{current['name']} {next_item['name']}")
                idx += 2
                continue

        _append_unique(participants, current["name"])
        idx += 1

    return participants


def is_departed_section(text: str) -> bool:
    lower = text.lower()
    return "покинувш" in lower or ("игроки" in lower and "покин" in lower)


def is_departed_status(text: str) -> bool:
    lower = text.lower()
    return lower.startswith("вышел") or "исключен владельцем" in lower


def _line_to_name_candidate(text: str) -> str:
    if re.fullmatch(r"\s*[()LIl|]+\s*", text):
        return ""
    if is_departed_section(text):
        return ""
    if is_departed_status(text):
        return ""
    if re.search(r"\d{2}\.\d{2}\.\d{4}", text):
        return ""

    words = [word for word in re.findall(r"[A-Za-zА-Яа-яЁё-]+", text) if _is_name_word(word)]
    if len(words) > 1:
        words = [word for word in words if not _looks_like_multi_letter_avatar_initials(word)]
    if not words:
        return ""

    if len(words) == 1 and len(words[0]) <= 3 and any(char in text for char in "()"):
        return ""
    if len(words) == 1 and _looks_like_avatar_initials(words[0]):
        return ""

    return " ".join(words)


def _append_unique(participants: list[str], name: str) -> None:
    append_unique_participant(participants, name)


def append_unique_participant(participants: list[str], name: str) -> None:
    normalized = " ".join(name.split())
    if not normalized:
        return

    for idx, existing in enumerate(participants):
        if _same_participant_name(existing, normalized):
            if _participant_name_quality(normalized) > _participant_name_quality(existing):
                participants[idx] = normalized
            return

    participants.append(normalized)


def _group_words_into_rows(words: list[OCRWord]) -> list[list[OCRWord]]:
    rows: list[list[OCRWord]] = []

    for word in sorted(words, key=lambda item: (item.y_center, item.x_center)):
        if not rows or abs(word.y_center - _row_y_center(rows[-1])) > 24:
            rows.append([word])
        else:
            rows[-1].append(word)

    return [sorted(row, key=lambda item: item.x_center) for row in rows]


def _row_y_center(row: list[OCRWord]) -> int:
    return sum(word.y_center for word in row) // len(row)


def _participant_row_text(row: list[OCRWord]) -> str:
    useful_words = [
        word.text
        for word in row
        if not (word.x_max < 170 and _looks_like_left_avatar_badge(word.text))
    ]
    return " ".join(useful_words)


def _is_name_word(text: str) -> bool:
    normalized = text.strip(".,:;!?()[]{}«»\"'").lower()
    if normalized in STOP_WORDS:
        return False
    if re.search(r"\d|https?|t\.me|@", normalized):
        return False
    return bool(re.fullmatch(r"[A-Za-zА-Яа-яЁё-]{1,}", normalized))


def _looks_like_full_name(text: str) -> bool:
    parts = text.split()
    return len(parts) == 2 and all(_is_name_word(part) for part in parts)


def _looks_like_avatar_initials(text: str) -> bool:
    token = text.strip(".,:;!?()[]{}«»\"'")
    if len(token) > 3:
        return False
    if not token.isupper():
        return False
    return bool(re.fullmatch(r"[A-ZА-ЯЁ]{1,3}", token))


def _looks_like_multi_letter_avatar_initials(text: str) -> bool:
    token = text.strip(".,:;!?()[]{}«»\"'")
    return len(token) >= 2 and _looks_like_avatar_initials(token)


def _looks_like_left_avatar_badge(text: str) -> bool:
    token = re.sub(r"[^A-Za-zА-Яа-яЁё]", "", text.strip())
    if not token or len(token) > 3:
        return False
    return token.isupper()


def _same_participant_name(left: str, right: str) -> bool:
    left_key = _participant_compare_key(left)
    right_key = _participant_compare_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True

    left_parts = left_key.split()
    right_parts = right_key.split()
    if len(left_parts) < 2 or len(right_parts) < 2:
        return False

    surname_dist = _levenshtein_distance(left_parts[0], right_parts[0])
    name_dist = _levenshtein_distance(left_parts[1], right_parts[1])
    return surname_dist <= 1 and name_dist <= 1


def _participant_name_quality(name: str) -> int:
    key = _participant_compare_key(name)
    score = len(key)
    if len(key.split()) >= 2:
        score += 20
    return score


def _participant_compare_key(name: str) -> str:
    text = name.lower().replace("ё", "е")
    text = text.replace("і", "и").replace("ї", "и")
    text = re.sub(r"[^a-zа-я\\s-]", " ", text, flags=re.IGNORECASE)
    text = text.replace("-", " ")
    return re.sub(r"\s+", " ", text).strip()


def _levenshtein_distance(left: str, right: str) -> int:
    if len(left) < len(right):
        left, right = right, left

    previous_row = list(range(len(right) + 1))
    for i, left_char in enumerate(left, 1):
        current_row = [i]
        for j, right_char in enumerate(right, 1):
            insertions = previous_row[j] + 1
            deletions = current_row[j - 1] + 1
            substitutions = previous_row[j - 1] + (left_char != right_char)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row
    return previous_row[-1]
