from __future__ import annotations

import re
from difflib import SequenceMatcher


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
        club_key = _comparable(club)
        if comparable == club_key or club_key in comparable or comparable in club_key:
            return club

    aliases = {
        "winwin": "Падел Клуб WIN WIN",
        "ракетаспб": "Ракета СПБ",
        "вмяч": "ВМЯЧ",
        "культпадел": "КультПадел",
        "культпад": "КультПадел",
        "dasport": "Da Sport теннис и падел",
        "тенниспадел": "Da Sport теннис и падел",
        "k5padel": "K5 Padel",
        "астрападел": "Астра Падел Клуб",
        "viborapadel": "Vibora Padel Club",
        "vibora20padel": "Vibora Padel Club",
        "паделклубнева": "Падел клуб Нева",
        "padelpoint": "PADEL POINT",
        "padelpo": "PADEL POINT",
        "paripadelarsenal": "PARI Padel Arsenal",
        "комета": "Комета",
        "репинопаделтайм": "Репино Падел Тайм",
        "padelpro": "Padel Pro",
        "primpadel": "PRIM-PADEL на Шаврова",
        "gazpadel": "GAZPADEL",
        "леснойолень": "Спорт-клуб Лесной Олень",
    }
    for alias, club in aliases.items():
        if alias in comparable:
            return club

    if re.search(r"\bk\s*5\s*padel\b", text, re.IGNORECASE):
        return "K5 Padel"

    best = max(CANONICAL_CLUBS, key=lambda club: SequenceMatcher(None, comparable, _comparable(club)).ratio())
    if SequenceMatcher(None, comparable, _comparable(best)).ratio() >= 0.78:
        return best

    return text


def _comparable(value: str) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", value.lower())
