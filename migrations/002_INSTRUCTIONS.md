# Инструкция по применению миграции 002_tournament_archiving.sql

## Шаг 1: Выполнить SQL миграцию

1. Откройте Supabase Dashboard
2. Перейдите в SQL Editor (НЕ используйте "sandboxed queries")
3. Скопируйте содержимое файла `migrations/002_tournament_archiving.sql`
4. Вставьте в SQL Editor
5. Нажмите "Run"

**Или через psql:**
```bash
psql $DATABASE_URL -f migrations/002_tournament_archiving.sql
```

## Шаг 2: Проверка миграции

Выполните в Supabase SQL Editor:
```sql
-- Проверить наличие новых полей
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_name = 'tournaments' 
  AND column_name IN ('active', 'first_seen_in_source', 'last_seen_in_source', 'archived_at', 'source');

-- Проверить таблицу sync_runs (если еще не создана)
SELECT COUNT(*) FROM sync_runs;

-- Проверить view
SELECT * FROM admin_tournaments_view LIMIT 1;
```

## Шаг 3: Запустить импорт

```bash
export DATABASE_URL="postgresql://..."
export LUNDA_JSON_PATH="/path/to/tournaments_database.json"
python scripts/import_lunda.py
```

## Шаг 4: Проверка в админке

1. Откройте админку: `http://localhost:3000` (или ваш URL)
2. Проверьте:
   - Турниры отсортированы по дате (ближайшие сверху)
   - По умолчанию показываются только активные турниры
   - Включите "Показать архив" - должны появиться архивные турниры с пометкой "Архив"
   - Архивные турниры показываются ниже активных

## Шаг 5: Тест архивации

1. Удалите один турнир из JSON файла
2. Запустите импорт снова
3. Проверьте логи импорта:
   - Если у турнира были оплаченные записи → `tournaments_archived=1`
   - Если оплаченных записей не было → `tournaments_deleted=1`
4. В админке включите "Показать архив" - удаленный турнир должен быть в архиве (или исчезнуть, если был удален)

## Ожидаемый результат

После успешного импорта в логах должно быть:
```
IMPORT STATISTICS
==================================================
Tournaments UPSERT: N
Tournaments ARCHIVED: X
Tournaments DELETED: Y
...
```

В админке:
- Активные турниры показываются по умолчанию
- Архивные турниры скрыты, но доступны через переключатель
- Турниры отсортированы по дате (ближайшие сверху)






