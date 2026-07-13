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
    "Маршал Арена",
)


def normalize_club_name(value: str, *, context: str = "") -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip(" |,-")
    if not text:
        return ""

    comparable = _comparable(text)
    direct = _match_canonical(comparable)
    if direct:
        return direct

    aliases = {
        "winwin": "Падел Клуб WIN WIN",
        "паделклуnwin": "Падел Клуб WIN WIN",
        "паделклiwin": "Падел Клуб WIN WIN",
        "паделклwin": "Падел Клуб WIN WIN",
        "ракетаспб": "Ракета СПБ",
        "вмяч": "ВМЯЧ",
        "культпадел": "КультПадел",
        "культпад": "КультПадел",
        "dasport": "Da Sport теннис и падел",
        "тенниспадел": "Da Sport теннис и падел",
        "k5padel": "K5 Padel",
        "астрападел": "Астра Падел Клуб",
        "астрапад": "Астра Падел Клуб",
        "viborapadel": "Vibora Padel Club",
        "viborapa": "Vibora Padel Club",
        "viborapab": "Vibora Padel Club",
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
        "gazpade": "GAZPADEL",
        "леснойолень": "Спорт-клуб Лесной Олень",
        "маршаларена": "Маршал Арена",
        "pari": "PARI Padel Arsenal",
        "paripade": "PARI Padel Arsenal",
        "arsenal": "PARI Padel Arsenal",
        "ракета": "Ракета СПБ",
        "ракетасг": "Ракета СПБ",
        "паделкла": "Падел клуб Нева",
    }
    for alias, club in aliases.items():
        if alias in comparable:
            return club

    if re.search(r"\bk\s*5\s*padel\b", text, re.IGNORECASE):
        return "K5 Padel"

    context_match = _match_context_club(context)
    if context_match and _looks_like_truncated_location(comparable):
        return context_match

    best = max(CANONICAL_CLUBS, key=lambda club: SequenceMatcher(None, comparable, _comparable(club)).ratio())
    if SequenceMatcher(None, comparable, _comparable(best)).ratio() >= 0.78:
        return best

    return text


def is_canonical_club(value: str) -> bool:
    return value in CANONICAL_CLUBS


def _comparable(value: str) -> str:
    return re.sub(r"[^0-9a-zа-я]+", "", value.lower())


def _match_canonical(comparable: str) -> str:
    if not comparable:
        return ""
    for club in CANONICAL_CLUBS:
        club_key = _comparable(club)
        if comparable == club_key or club_key in comparable or comparable in club_key:
            return club
    return ""


def _match_context_club(context: str) -> str:
    comparable = _comparable(context)
    if not comparable:
        return ""
    context_aliases = {
        "astra": "Астра Падел Клуб",
        "астра": "Астра Падел Клуб",
        "gazpadel": "GAZPADEL",
        "газ": "GAZPADEL",
        "viborapadel": "Vibora Padel Club",
        "vibora": "Vibora Padel Club",
        "winwin": "Падел Клуб WIN WIN",
        "паделклwin": "Падел Клуб WIN WIN",
    }
    for alias, club in context_aliases.items():
        if alias in comparable:
            return club
    return ""


def _looks_like_truncated_location(comparable: str) -> bool:
    if not comparable:
        return False
    if len(comparable) <= 14:
        return True
    city_noise = ("санкт", "саитпет", "сенктпет", "ктпетербур")
    return any(noise in comparable for noise in city_noise)
