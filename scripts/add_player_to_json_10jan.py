import json
from pathlib import Path

JSON_PATH = Path("/Users/kirill/android_parser_service/parser_russian_version/work/tournaments_database.json")

PLAYER_NAME = "Player With TG"
DATE_MARK = "10 января"   # ищем по ключу турнира (там есть "Сб 10 января")

def bump_participants_counter(t: dict):
    """
    Пытаемся обновить строку вида "1/12 игроков" -> "2/12 игроков".
    Если формат неожиданный — просто молча пропускаем.
    """
    try:
        s = (t.get("tournament") or {}).get("participants", "")
        left, rest = s.split("/", 1)
        x = int(left.strip())
        t["tournament"]["participants"] = f"{x+1}/{rest}"
    except Exception:
        pass

def main():
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    tournaments = data.get("tournaments", {})

    # Находим первый турнир на 10 января
    candidates = [k for k in tournaments.keys() if DATE_MARK in k]
    if not candidates:
        raise SystemExit(f"Не нашёл турниров с '{DATE_MARK}' в ключах. Проверь JSON.")

    # Чтобы выбор был стабильный — сортируем по ключу
    candidates.sort()
    key = candidates[0]

    t = tournaments[key]
    participants = t.get("participants") or []

    if PLAYER_NAME in participants:
        print(f"Уже есть: {PLAYER_NAME} в турнире:\n{key}")
        return

    participants.append(PLAYER_NAME)
    t["participants"] = participants
    bump_participants_counter(t)

    tournaments[key] = t
    data["tournaments"] = tournaments

    JSON_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print("OK: добавил игрока в турнир на 10 января:")
    print(key)

if __name__ == "__main__":
    main()