# Lunda Collector Data Model

Цель новой базы - разделить три разных состояния:

- расписание турниров вперед;
- текущий снимок участников будущего турнира;
- финальный состав после того, как турнир начался и исчез из будущего списка.

## Основные таблицы

- `sync_runs` - каждый запуск сборщика: расписание, сегодняшние участники, тест.
- `tournaments` - нормализованные турниры. Главный ключ строится из организатора, времени старта и места.
- `tournament_observations` - сырые OCR-наблюдения карточек. Нужны для отладки дублей и ошибок распознавания.
- `participant_snapshots` - полный список участников, увиденный в конкретном проходе.
- `current_participants` - актуальный состав будущего турнира: кто сейчас активен, кто исчез между проходами.
- `final_participations` - итоговый состав турнира после финализации.
- `players` - накопительная база людей.
- `player_aliases` - ручные привязки OCR-вариантов к человеку.
- `pending_players` - карантин для похожих имен, где автоматика не должна решать сама.

## Задача 1: Участники

Периодический проход:

1. Открыть Lunda -> `Играть`.
2. Идти по турнирам целевого дня.
3. Открывать только полностью видимые карточки.
4. Пропускать `Турнир отменен`.
5. Открывать `Команды` или `Участники`.
6. Читать активных игроков до `Игроки, покинувшие турнир`.
7. Записывать снимок в `participant_snapshots` и обновлять `current_participants`.
8. Когда текущее время позже старта турнира, переносить активных участников в `final_participations`.

## Задача 2: Расписание

Отдельный проход:

1. Открыть `Играть`.
2. Пролистать карточки на глубину планирования, например 21 день.
3. Записать только карточки, которые можно надежно идентифицировать.
4. Использовать `tournament_observations` для диагностики неполных OCR-скринов.

## Представление данных

Первый экран сайта для людей:

- фильтры: период, клуб, организатор, уровень, формат;
- таблица игроков, отсортированная по количеству финальных участий;
- в строке игрока: количество турниров, клубы, организаторы, уровни.

Экран расписания:

- день/неделя как календарная сетка;
- параллельные турниры в одно время показываются соседними блоками;
- фильтры: клуб, организатор, уровень, формат, цена, свободные места.

## Команды

Инициализация:

```bash
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 init-db
```

Живой сбор расписания:

```bash
/Users/kirill/android_parser_service/parser_russian_version/venv/bin/python \
  lunda_collector/live_phone_cycle.py \
  --db work/lunda.sqlite3 \
  schedule --launch --screens 40 --days 21
```

Живой сбор участников целевого дня:

```bash
/Users/kirill/android_parser_service/parser_russian_version/venv/bin/python \
  lunda_collector/live_phone_cycle.py \
  --db work/lunda.sqlite3 \
  today-participants --launch --screens 40 --participants-screens 12
```

Финализация прошедших турниров:

```bash
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 finalize-due
```

Рейтинг игроков:

```bash
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 rankings --limit 100
```

Excel-экспорт:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r lunda_collector/requirements.txt
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 export
```
