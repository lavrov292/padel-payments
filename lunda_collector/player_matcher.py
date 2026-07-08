from __future__ import annotations

import re
import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class Candidate:
    player_id: int
    name: str
    normalized_name: str
    dist: int
    score: int
    surname_dist: int | None
    name_dist: int | None


@dataclass(frozen=True)
class PlayerResolution:
    player_id: int | None
    status: str
    normalized_name: str
    candidates: list[Candidate]


def normalize_name(value: str) -> str:
    if not value:
        return ""

    text = unicodedata.normalize("NFKC", value)
    text = text.strip().lower().replace("ё", "е")
    text = _replace_latin_lookalikes(text)
    text = re.sub(r"[^\w\sа-яa-z-]", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"[_\d]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def comparison_key(value: str) -> str:
    normalized = normalize_name(value)
    normalized = normalized.replace("й", "и")
    normalized = normalized.replace("-", " ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def resolve_player(
    conn: sqlite3.Connection,
    raw_name: str,
    *,
    run_id: int | None = None,
    tournament_id: int | None = None,
    now_iso: str | None = None,
) -> PlayerResolution:
    normalized = normalize_name(raw_name)
    if not normalized:
        return PlayerResolution(None, "empty", "", [])

    now_iso = now_iso or datetime.now().isoformat(timespec="seconds")

    alias_row = conn.execute(
        """
        SELECT player_id
        FROM player_aliases
        WHERE normalized_alias = ?
        LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if alias_row:
        _touch_player(conn, int(alias_row["player_id"]), now_iso)
        return PlayerResolution(int(alias_row["player_id"]), "alias_hit", normalized, [])

    exact_row = conn.execute(
        """
        SELECT id
        FROM players
        WHERE normalized_name = ?
        LIMIT 1
        """,
        (normalized,),
    ).fetchone()
    if exact_row:
        _touch_player(conn, int(exact_row["id"]), now_iso)
        return PlayerResolution(int(exact_row["id"]), "exact_hit", normalized, [])

    candidates = find_candidate_players(conn, raw_name, normalized, limit=5)
    threshold = levenshtein_threshold(len(comparison_key(normalized)))
    close_candidates = [candidate for candidate in candidates if candidate.dist <= threshold]

    if close_candidates:
        if _raw_variant_wins_tournament_consensus(
            conn,
            normalized_name=normalized,
            candidate=close_candidates[0],
            tournament_id=tournament_id,
        ):
            player_id = _create_player(conn, raw_name, normalized, now_iso)
            return PlayerResolution(player_id, "new_player", normalized, close_candidates)

        auto_candidate = _auto_resolve_close_candidate(
            conn,
            normalized_name=normalized,
            candidates=close_candidates,
            tournament_id=tournament_id,
        )
        if auto_candidate:
            _touch_player(conn, auto_candidate.player_id, now_iso)
            _mark_pending_auto_resolved(
                conn,
                normalized_name=normalized,
                tournament_id=tournament_id,
                player_id=auto_candidate.player_id,
            )
            return PlayerResolution(
                auto_candidate.player_id,
                "fuzzy_auto_existing",
                normalized,
                close_candidates,
            )

        _upsert_pending_player(
            conn,
            raw_name=raw_name,
            normalized_name=normalized,
            candidates=close_candidates,
            run_id=run_id,
            tournament_id=tournament_id,
            now_iso=now_iso,
        )
        return PlayerResolution(None, "fuzzy_pending", normalized, close_candidates)

    player_id = _create_player(conn, raw_name, normalized, now_iso)
    return PlayerResolution(player_id, "new_player", normalized, [])


def find_candidate_players(
    conn: sqlite3.Connection,
    raw_name: str,
    normalized_name: str | None = None,
    *,
    limit: int = 5,
    pool_limit: int = 50,
) -> list[Candidate]:
    normalized = normalized_name or normalize_name(raw_name)
    key = comparison_key(normalized)
    if not key:
        return []

    input_surname, input_name = split_name_tokens(key)
    max_dist = levenshtein_threshold(len(key))

    rows = conn.execute(
        """
        SELECT id, display_name, normalized_name
        FROM players
        WHERE normalized_name IS NOT NULL AND normalized_name != ''
        """
    ).fetchall()

    pool: list[Candidate] = []
    for row in rows:
        candidate_norm = str(row["normalized_name"] or "")
        candidate_key = comparison_key(candidate_norm)
        if not candidate_key or candidate_key == key:
            continue

        full_dist = levenshtein_distance(key, candidate_key)
        if full_dist > max_dist + 2:
            continue

        candidate_surname, candidate_name = split_name_tokens(candidate_key)
        passes, surname_dist, name_dist = passes_similarity_filter(
            input_surname=input_surname,
            input_name=input_name,
            candidate_surname=candidate_surname,
            candidate_name=candidate_name,
            full_dist=full_dist,
            max_dist=max_dist,
        )
        if not passes:
            continue

        score = candidate_score(full_dist, surname_dist, name_dist)
        pool.append(
            Candidate(
                player_id=int(row["id"]),
                name=str(row["display_name"]),
                normalized_name=candidate_norm,
                dist=full_dist,
                score=score,
                surname_dist=surname_dist,
                name_dist=name_dist,
            )
        )

    pool.sort(key=lambda item: (item.score, item.dist, item.name))
    return pool[: min(limit, pool_limit)]


def levenshtein_threshold(normalized_name_len: int) -> int:
    if normalized_name_len <= 8:
        return 2
    if normalized_name_len <= 14:
        return 3
    if normalized_name_len <= 22:
        return 4
    return 5


def split_name_tokens(normalized_name: str) -> tuple[str | None, str | None]:
    tokens = normalized_name.split()
    if len(tokens) >= 2:
        return tokens[0], tokens[1]
    if len(tokens) == 1:
        return tokens[0], None
    return None, None


def candidate_score(full_dist: int, surname_dist: int | None, name_dist: int | None) -> int:
    surname_score = (surname_dist if surname_dist is not None else 0) * 3
    name_score = (name_dist if name_dist is not None else 0) * 3
    return full_dist * 10 + surname_score + name_score


def passes_similarity_filter(
    *,
    input_surname: str | None,
    input_name: str | None,
    candidate_surname: str | None,
    candidate_name: str | None,
    full_dist: int,
    max_dist: int,
) -> tuple[bool, int | None, int | None]:
    surname_dist = levenshtein_distance(input_surname, candidate_surname) if input_surname and candidate_surname else None
    name_dist = levenshtein_distance(input_name, candidate_name) if input_name and candidate_name else None

    if full_dist <= max_dist:
        if surname_dist is not None and name_dist is not None and surname_dist <= 1 and name_dist <= 1:
            return True, surname_dist, name_dist
        if _one_letter_surname_shift(input_surname, candidate_surname) and (name_dist is None or name_dist <= 1):
            return True, surname_dist, name_dist
        if input_surname and input_name and candidate_surname and candidate_name:
            return False, surname_dist, name_dist
        return True, surname_dist, name_dist

    if full_dist <= max_dist + 2:
        if surname_dist is not None and name_dist is not None and surname_dist <= 1 and name_dist <= 1:
            return True, surname_dist, name_dist
        if _one_letter_surname_shift(input_surname, candidate_surname) and (name_dist is None or name_dist <= 1):
            return True, surname_dist, name_dist

    return False, surname_dist, name_dist


def levenshtein_distance(left: str | None, right: str | None) -> int:
    if not left:
        return len(right or "")
    if not right:
        return len(left)

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


def candidate_to_dict(candidate: Candidate) -> dict[str, Any]:
    return {
        "player_id": candidate.player_id,
        "name": candidate.name,
        "normalized_name": candidate.normalized_name,
        "dist": candidate.dist,
        "score": candidate.score,
        "surname_dist": candidate.surname_dist,
        "name_dist": candidate.name_dist,
    }


def _create_player(conn: sqlite3.Connection, raw_name: str, normalized: str, now_iso: str) -> int:
    conn.execute(
        """
        INSERT INTO players (display_name, normalized_name, first_seen_at, last_seen_at)
        VALUES (?, ?, ?, ?)
        """,
        (raw_name.strip(), normalized, now_iso, now_iso),
    )
    return int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])


def _touch_player(conn: sqlite3.Connection, player_id: int, now_iso: str) -> None:
    conn.execute(
        """
        UPDATE players
        SET last_seen_at = ?
        WHERE id = ?
        """,
        (now_iso, player_id),
    )


def _upsert_pending_player(
    conn: sqlite3.Connection,
    *,
    raw_name: str,
    normalized_name: str,
    candidates: list[Candidate],
    run_id: int | None,
    tournament_id: int | None,
    now_iso: str,
) -> None:
    import json

    candidates_json = json.dumps([candidate_to_dict(candidate) for candidate in candidates], ensure_ascii=False)
    existing = conn.execute(
        """
        SELECT id
        FROM pending_players
        WHERE normalized_name = ?
          AND COALESCE(tournament_id, 0) = COALESCE(?, 0)
          AND status = 'pending'
        LIMIT 1
        """,
        (normalized_name, tournament_id),
    ).fetchone()

    if existing:
        conn.execute(
            """
            UPDATE pending_players
            SET raw_name = ?,
                candidates_json = ?,
                run_id = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (raw_name.strip(), candidates_json, run_id, now_iso, int(existing["id"])),
        )
        return

    conn.execute(
        """
        INSERT INTO pending_players (
            run_id, tournament_id, raw_name, normalized_name,
            candidates_json, status, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        """,
        (run_id, tournament_id, raw_name.strip(), normalized_name, candidates_json, now_iso, now_iso),
    )


def _auto_resolve_close_candidate(
    conn: sqlite3.Connection,
    *,
    normalized_name: str,
    candidates: list[Candidate],
    tournament_id: int | None,
) -> Candidate | None:
    if not candidates:
        return None

    best = candidates[0]
    if tournament_id:
        raw_tournament_count = _snapshot_variant_count(conn, tournament_id, normalized_name)
        best_tournament_count = _snapshot_variant_count(conn, tournament_id, best.normalized_name)
        if best_tournament_count > raw_tournament_count:
            return best
        if raw_tournament_count > best_tournament_count:
            return None
        if best_tournament_count > 0 and best.dist <= 1 and _player_observation_count(conn, best.player_id) > 1:
            return best

    raw_count = _normalized_name_observation_count(conn, normalized_name)
    best_count = _player_observation_count(conn, best.player_id)
    if best.dist <= 1 and best_count > raw_count:
        return best

    if best.dist <= 1 and raw_count == 0 and _candidate_is_unambiguous(best, candidates):
        return best

    return None


def _raw_variant_wins_tournament_consensus(
    conn: sqlite3.Connection,
    *,
    normalized_name: str,
    candidate: Candidate,
    tournament_id: int | None,
) -> bool:
    if not tournament_id:
        return False
    raw_tournament_count = _snapshot_variant_count(conn, tournament_id, normalized_name)
    candidate_tournament_count = _snapshot_variant_count(conn, tournament_id, candidate.normalized_name)
    return raw_tournament_count > candidate_tournament_count


def _candidate_is_unambiguous(best: Candidate, candidates: list[Candidate]) -> bool:
    if len(candidates) == 1:
        return True
    second = candidates[1]
    return best.dist < second.dist or best.score + 10 < second.score


def _candidate_seen_in_tournament(conn: sqlite3.Connection, player_id: int, tournament_id: int) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM current_participants
        WHERE tournament_id = ? AND player_id = ?
        LIMIT 1
        """,
        (tournament_id, player_id),
    ).fetchone()
    if row:
        return True

    row = conn.execute(
        """
        SELECT 1
        FROM final_participations
        WHERE tournament_id = ? AND player_id = ?
        LIMIT 1
        """,
        (tournament_id, player_id),
    ).fetchone()
    return bool(row)


def _snapshot_variant_count(conn: sqlite3.Connection, tournament_id: int, normalized_name: str) -> int:
    import json

    count = 0
    rows = conn.execute(
        """
        SELECT participants_json
        FROM participant_snapshots
        WHERE tournament_id = ?
        """,
        (tournament_id,),
    ).fetchall()
    for row in rows:
        try:
            values = json.loads(row["participants_json"] or "[]")
        except (TypeError, json.JSONDecodeError):
            continue
        for value in values:
            if normalize_name(str(value)) == normalized_name:
                count += 1
    return count


def _normalized_name_observation_count(conn: sqlite3.Connection, normalized_name: str) -> int:
    current_count = conn.execute(
        "SELECT COUNT(*) FROM current_participants WHERE normalized_name = ?",
        (normalized_name,),
    ).fetchone()[0]
    final_count = conn.execute(
        "SELECT COUNT(*) FROM final_participations WHERE normalized_name = ?",
        (normalized_name,),
    ).fetchone()[0]
    pending_count = conn.execute(
        "SELECT COUNT(*) FROM pending_players WHERE normalized_name = ?",
        (normalized_name,),
    ).fetchone()[0]
    return int(current_count) + int(final_count) + int(pending_count)


def _player_observation_count(conn: sqlite3.Connection, player_id: int) -> int:
    current_count = conn.execute(
        "SELECT COUNT(*) FROM current_participants WHERE player_id = ?",
        (player_id,),
    ).fetchone()[0]
    final_count = conn.execute(
        "SELECT COUNT(*) FROM final_participations WHERE player_id = ?",
        (player_id,),
    ).fetchone()[0]
    return int(current_count) + int(final_count)


def _mark_pending_auto_resolved(
    conn: sqlite3.Connection,
    *,
    normalized_name: str,
    tournament_id: int | None,
    player_id: int,
) -> None:
    conn.execute(
        """
        UPDATE pending_players
        SET status = 'auto_resolved',
            resolved_player_id = ?
        WHERE normalized_name = ?
          AND COALESCE(tournament_id, 0) = COALESCE(?, 0)
          AND status = 'pending'
        """,
        (player_id, normalized_name, tournament_id),
    )


def _one_letter_surname_shift(left: str | None, right: str | None) -> bool:
    if not left or not right or len(left) <= 1 or len(right) <= 1:
        return False
    return left[1:] == right or right[1:] == left or left[1:] == right[1:]


def _replace_latin_lookalikes(text: str) -> str:
    table = str.maketrans(
        {
            "a": "а",
            "c": "с",
            "e": "е",
            "o": "о",
            "p": "р",
            "x": "х",
            "y": "у",
        }
    )
    return text.translate(table)
