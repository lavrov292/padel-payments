from __future__ import annotations

import re


CANONICAL_CLUBS = (
    "Падел Клуб WIN WIN",
    "Ракета СПБ",
    "ВМЯЧ",
    "КультПадел",
    "Da Sport теннис и падел",
    "K5 Padel",
    "Астра Падел Клуб",
    "Vibora Padel Club",
    "Падел клуб Нева",
    "PADEL POINT",
    "PARI Padel Arsenal",
    "Комета",
    "Репино Падел Тайм",
    "Padel Pro",
    "PRIM-PADEL на Шаврова",
    "GAZPADEL",
    "Спорт-клуб Лесной Олень",
)


def normalize_club_name(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" |,-")
    if not text:
        return ""

    comparable = _comparable(text)
    for club in CANONICAL_CLUBS:
        if comparable == _comparable(club):
            return club

    if re.search(r"\bk\s*5\s*padel\b", text, re.IGNORECASE):
        return "K5 Padel"

    return text


def _comparable(value: str) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", value.lower())
