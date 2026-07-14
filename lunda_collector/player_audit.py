#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from openpyxl import Workbook

from storage import DEFAULT_DB_PATH, connect, init_db


BAD_PHRASES = {
    "а также",
    "а мы",
    "бесплатная парковка",
    "в выбранном городе",
    "вручаем",
    "доступно для приглашения",
    "игрок",
    "игроки",
    "игроков",
    "изменить город",
    "мойки",
    "акваматик",
    "парковка",
    "пригласить",
    "призеры",
    "призерам",
    "свободно",
    "турнир",
}

CONNECTOR_WORDS = {
    "а",
    "без",
    "в",
    "для",
    "и",
    "или",
    "к",
    "на",
    "не",
    "по",
    "с",
    "со",
    "у",
}


@dataclass
class PlayerAuditRow:
    player_id: int
    name: str
    normalized_name: str
    latest_rating: float | None
    final_count: int
    current_count: int
    participation_count: int
    locations: str
    titles: str
    reasons: list[str]
    heuristic_status: str
    llm_status: str = ""
    llm_confidence: float | None = None
    llm_reason: str = ""
    llm_merge_with: str = ""


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit Lunda player names for OCR garbage")
    parser.add_argument("--db", default=os.environ.get("LUNDA_DB_PATH", str(DEFAULT_DB_PATH)))
    parser.add_argument("--env-file", default="/opt/lunda-collector/app/.env")
    parser.add_argument("--out", default="")
    parser.add_argument("--all", action="store_true", help="Send/export all players, not only suspicious ones")
    parser.add_argument("--use-yandex-gpt", action="store_true", help="Call YandexGPT-compatible API for classification")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--batch-size", type=int, default=60)
    args = parser.parse_args()

    load_env_file(Path(args.env_file))
    conn = connect(args.db)
    init_db(conn)
    init_player_audit_db(conn)

    rows = load_player_rows(conn)
    audited = [audit_player(row) for row in rows]
    selected = audited if args.all else [row for row in audited if row.reasons]
    selected.sort(key=lambda row: (-risk_score(row), row.name.lower()))
    if args.limit:
        selected = selected[: args.limit]

    if args.use_yandex_gpt and selected:
        apply_yandex_gpt_audit(selected, batch_size=max(1, args.batch_size))

    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    save_audit_results(conn, selected, now)
    out_path = Path(args.out or f"/opt/lunda-collector/work/player_audit_{time.strftime('%Y%m%d_%H%M%S')}.xlsx")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_xlsx(selected, out_path)
    print(f"Audited players: {len(audited)}")
    print(f"Rows exported: {len(selected)}")
    print(f"Output: {out_path}")
    return 0


def init_player_audit_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS player_name_audit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id INTEGER NOT NULL,
            display_name TEXT NOT NULL,
            heuristic_status TEXT NOT NULL,
            heuristic_reasons TEXT NOT NULL,
            llm_status TEXT,
            llm_confidence REAL,
            llm_reason TEXT,
            llm_merge_with TEXT,
            status TEXT NOT NULL DEFAULT 'open',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def load_player_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT
            p.id,
            p.display_name,
            p.normalized_name,
            p.latest_rating,
            COUNT(DISTINCT fp.tournament_id) AS final_count,
            COUNT(DISTINCT cp.tournament_id) AS current_count,
            GROUP_CONCAT(DISTINCT COALESCE(tf.location, tc.location)) AS locations,
            GROUP_CONCAT(DISTINCT COALESCE(tf.title, tc.title)) AS titles
        FROM players p
        LEFT JOIN final_participations fp ON fp.player_id = p.id
        LEFT JOIN tournaments tf ON tf.id = fp.tournament_id
        LEFT JOIN current_participants cp ON cp.player_id = p.id
        LEFT JOIN tournaments tc ON tc.id = cp.tournament_id
        GROUP BY p.id
        ORDER BY p.display_name COLLATE NOCASE
        """
    ).fetchall()


def audit_player(row: sqlite3.Row) -> PlayerAuditRow:
    name = str(row["display_name"] or "").strip()
    normalized = str(row["normalized_name"] or "").strip()
    final_count = int(row["final_count"] or 0)
    current_count = int(row["current_count"] or 0)
    total_count = max(final_count, current_count, final_count + current_count)
    rating = row["latest_rating"]
    reasons = heuristic_reasons(name, normalized, total_count, rating)
    status = "suspicious" if reasons else "likely_player"
    return PlayerAuditRow(
        player_id=int(row["id"]),
        name=name,
        normalized_name=normalized,
        latest_rating=float(rating) if rating is not None else None,
        final_count=final_count,
        current_count=current_count,
        participation_count=total_count,
        locations=str(row["locations"] or ""),
        titles=str(row["titles"] or ""),
        reasons=reasons,
        heuristic_status=status,
    )


def heuristic_reasons(name: str, normalized: str, participation_count: int, rating: float | None) -> list[str]:
    reasons: list[str] = []
    lower = name.lower().strip()
    words = re.findall(r"[A-Za-zА-Яа-яЁё]+", name)
    lower_words = [word.lower() for word in words]
    if not name:
        reasons.append("empty_name")
    if any(phrase in lower for phrase in BAD_PHRASES):
        reasons.append("known_ui_or_description_phrase")
    if any(word in CONNECTOR_WORDS for word in lower_words) and rating is None:
        reasons.append("contains_connector_word_without_rating")
    if re.search(r"[а-яёa-z][А-ЯЁA-Z]", name):
        reasons.append("possible_concatenated_names")
    if re.search(r"\d", name):
        reasons.append("contains_digit")
    if len(words) >= 4:
        reasons.append("too_many_words")
    if len(name) <= 3 and rating is None:
        reasons.append("too_short_without_rating")
    if len(words) == 1 and rating is None and participation_count <= 1:
        reasons.append("single_word_without_rating_once")
    if name.isupper() and len(name) <= 6 and rating is None:
        reasons.append("short_all_caps_without_rating")
    if normalized in BAD_PHRASES:
        reasons.append("normalized_known_bad_phrase")
    return reasons


def risk_score(row: PlayerAuditRow) -> int:
    score = len(row.reasons) * 10
    if row.latest_rating is None:
        score += 5
    if row.participation_count <= 1:
        score += 3
    if "known_ui_or_description_phrase" in row.reasons:
        score += 20
    if "possible_concatenated_names" in row.reasons:
        score += 15
    return score


def apply_yandex_gpt_audit(rows: list[PlayerAuditRow], *, batch_size: int) -> None:
    api_key = (
        os.environ.get("YANDEXGPT_API_KEY")
        or os.environ.get("YANDEX_GPT_API_KEY")
        or os.environ.get("YANDEX_AI_STUDIO_API_KEY")
    )
    if not api_key:
        raise RuntimeError("Set YANDEXGPT_API_KEY or YANDEX_AI_STUDIO_API_KEY to use --use-yandex-gpt")
    url = os.environ.get("YANDEXGPT_OPENAI_URL", "https://llm.api.cloud.yandex.net/v1/chat/completions")
    model = os.environ.get("YANDEXGPT_MODEL", "gpt://b1g/yandexgpt-lite/latest")
    for start in range(0, len(rows), batch_size):
        batch = rows[start : start + batch_size]
        result = call_yandex_gpt(api_key=api_key, url=url, model=model, rows=batch)
        by_id = {int(item.get("player_id")): item for item in result if str(item.get("player_id", "")).isdigit()}
        for row in batch:
            item = by_id.get(row.player_id)
            if not item:
                continue
            row.llm_status = str(item.get("status") or "")
            row.llm_reason = str(item.get("reason") or "")
            row.llm_merge_with = str(item.get("merge_with_player_id") or "")
            try:
                row.llm_confidence = float(item.get("confidence"))
            except (TypeError, ValueError):
                row.llm_confidence = None


def call_yandex_gpt(*, api_key: str, url: str, model: str, rows: list[PlayerAuditRow]) -> list[dict[str, Any]]:
    payload_rows = [
        {
            "player_id": row.player_id,
            "name": row.name,
            "rating": row.latest_rating,
            "participations": row.participation_count,
            "locations": split_compact(row.locations),
            "tournament_titles": split_compact(row.titles, limit=5),
            "heuristic_reasons": row.reasons,
        }
        for row in rows
    ]
    prompt = (
        "Ты проверяешь OCR-имена игроков в падел-приложении. "
        "Нужно отличить реальных игроков от мусора интерфейса или описаний турнира. "
        "Важно: реальные игроки могут быть записаны одним словом, латиницей, с одной буквой фамилии "
        "(например 'K Александр'), с короткой фамилией, без рейтинга. Не отклоняй такие имена только из-за формата. "
        "Явный мусор: фразы интерфейса, рекламные/описательные фразы, склейки нескольких имен без пробела, "
        "слова вроде 'бесплатная парковка', 'призеры', 'свободно', 'игроки'. "
        "Верни только JSON-массив. Для каждой строки: "
        "player_id, status (valid_player|ocr_garbage|needs_manual_review), confidence 0..1, "
        "reason, merge_with_player_id null. Не добавляй markdown."
    )
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": json.dumps(payload_rows, ensure_ascii=False)},
        ],
        "temperature": 0,
    }
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": f"Api-Key {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            parsed = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"YandexGPT HTTP {exc.code}: {detail}") from exc
    content = parsed["choices"][0]["message"]["content"]
    return json.loads(strip_json_fence(content))


def strip_json_fence(value: str) -> str:
    text = value.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text


def split_compact(value: str, *, limit: int = 8) -> list[str]:
    if not value:
        return []
    items = []
    for part in value.split(","):
        text = part.strip()
        if text and text not in items:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def save_audit_results(conn: sqlite3.Connection, rows: list[PlayerAuditRow], created_at: str) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO player_name_audit (
                player_id, display_name, heuristic_status, heuristic_reasons,
                llm_status, llm_confidence, llm_reason, llm_merge_with,
                status, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)
            """,
            (
                row.player_id,
                row.name,
                row.heuristic_status,
                json.dumps(row.reasons, ensure_ascii=False),
                row.llm_status,
                row.llm_confidence,
                row.llm_reason,
                row.llm_merge_with,
                created_at,
            ),
        )
    conn.commit()


def write_xlsx(rows: list[PlayerAuditRow], path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "audit"
    ws.append(
        [
            "player_id",
            "name",
            "rating",
            "participations",
            "final_count",
            "current_count",
            "heuristic_status",
            "heuristic_reasons",
            "llm_status",
            "llm_confidence",
            "llm_reason",
            "llm_merge_with",
            "locations",
            "titles",
        ]
    )
    for row in rows:
        ws.append(
            [
                row.player_id,
                row.name,
                row.latest_rating if row.latest_rating is not None else "",
                row.participation_count,
                row.final_count,
                row.current_count,
                row.heuristic_status,
                ", ".join(row.reasons),
                row.llm_status,
                row.llm_confidence if row.llm_confidence is not None else "",
                row.llm_reason,
                row.llm_merge_with,
                row.locations,
                row.titles,
            ]
        )
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions
    for column in ws.columns:
        width = min(60, max(10, max(len(str(cell.value or "")) for cell in column) + 2))
        ws.column_dimensions[column[0].column_letter].width = width
    wb.save(path)


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


if __name__ == "__main__":
    raise SystemExit(main())
