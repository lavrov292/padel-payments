-- Тестовый запрос для проверки "Мои турниры" в боте
-- Замените 350 на нужный player_id

SELECT 
    e.id as entry_id,
    t.title,
    t.starts_at,
    t.price_rub,
    t.tournament_type,
    t.location,
    e.payment_status,
    t.active,
    t.archived_at,
    t.last_seen_in_source
FROM entries e
JOIN tournaments t ON e.tournament_id = t.id
WHERE e.player_id = 350  -- ЗАМЕНИТЕ НА НУЖНЫЙ player_id
  AND t.starts_at > NOW()
  AND t.active = true
  AND t.archived_at IS NULL
ORDER BY t.starts_at ASC;

-- Для сравнения: запрос БЕЗ фильтров (покажет все, включая неактуальные)
-- Раскомментируйте, чтобы увидеть разницу:
/*
SELECT 
    e.id as entry_id,
    t.title,
    t.starts_at,
    t.price_rub,
    t.tournament_type,
    t.location,
    e.payment_status,
    t.active,
    t.archived_at,
    t.last_seen_in_source
FROM entries e
JOIN tournaments t ON e.tournament_id = t.id
WHERE e.player_id = 350
  AND t.starts_at > NOW()
ORDER BY t.starts_at ASC;
*/



