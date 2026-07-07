from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone


MSK = timezone(timedelta(hours=3))


MONTHS_RU = {
    "января": 1,
    "январь": 1,
    "февраля": 2,
    "февраль": 2,
    "марта": 3,
    "март": 3,
    "апреля": 4,
    "апрель": 4,
    "мая": 5,
    "май": 5,
    "июня": 6,
    "июнь": 6,
    "июля": 7,
    "июль": 7,
    "августа": 8,
    "август": 8,
    "сентября": 9,
    "сентябрь": 9,
    "октября": 10,
    "октябрь": 10,
    "ноября": 11,
    "ноябрь": 11,
    "декабря": 12,
    "декабрь": 12,
}


@dataclass(frozen=True)
class ParsedTournamentTime:
    starts_at: datetime | None
    ends_at: datetime | None
    tournament_date: date | None
    start_time: str
    end_time: str


def now_msk() -> datetime:
    return datetime.now(MSK)


def participant_target_date(now: datetime | None = None) -> date:
    current = _as_msk(now or now_msk())
    if current.time() >= time(23, 30):
        return current.date() + timedelta(days=1)
    return current.date()


def parse_tournament_datetime(
    date_label: str,
    time_label: str,
    *,
    now: datetime | None = None,
) -> ParsedTournamentTime:
    base_now = _as_msk(now or now_msk())
    tournament_day = parse_russian_date_label(date_label, now=base_now)
    start_value, end_value = parse_time_range(time_label)

    starts_at = None
    ends_at = None
    if tournament_day and start_value:
        starts_at = datetime.combine(tournament_day, start_value, tzinfo=MSK)
    if tournament_day and end_value:
        ends_at = datetime.combine(tournament_day, end_value, tzinfo=MSK)
        if starts_at and ends_at < starts_at:
            ends_at += timedelta(days=1)

    return ParsedTournamentTime(
        starts_at=starts_at,
        ends_at=ends_at,
        tournament_date=tournament_day,
        start_time=_format_time(start_value),
        end_time=_format_time(end_value),
    )


def parse_russian_date_label(date_label: str, *, now: datetime | None = None) -> date | None:
    if not date_label:
        return None

    normalized = (
        date_label.lower()
        .replace(".", " ")
        .replace(",", " ")
        .replace("|", " ")
    )
    normalized = re.sub(r"\s+", " ", normalized).strip()

    match = re.search(r"(\d{1,2})\s+([а-яё]+)", normalized, re.IGNORECASE)
    if not match:
        return None

    day = int(match.group(1))
    month_text = match.group(2).replace("ё", "е")
    month = MONTHS_RU.get(month_text)
    if not month:
        return None

    base_now = _as_msk(now or now_msk())
    year = base_now.year
    try:
        parsed = date(year, month, day)
    except ValueError:
        return None

    if parsed < base_now.date() - timedelta(days=90):
        try:
            parsed = date(year + 1, month, day)
        except ValueError:
            return None

    return parsed


def parse_time_range(time_label: str) -> tuple[time | None, time | None]:
    if not time_label:
        return None, None

    matches = re.findall(r"(\d{1,2})[:.](\d{2})", time_label)
    parsed: list[time] = []
    for hour_text, minute_text in matches[:2]:
        try:
            parsed.append(time(int(hour_text), int(minute_text)))
        except ValueError:
            continue

    if not parsed:
        return None, None
    if len(parsed) == 1:
        return parsed[0], None
    return parsed[0], parsed[1]


def iso_or_empty(value: datetime | date | None) -> str:
    if not value:
        return ""
    return value.isoformat()


def _as_msk(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=MSK)
    return value.astimezone(MSK)


def _format_time(value: time | None) -> str:
    return value.strftime("%H:%M") if value else ""
