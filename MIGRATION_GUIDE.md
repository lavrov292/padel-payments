# Миграция: tournament_type и вечные ссылки

## SQL Миграция для Supabase

Выполните SQL из файла `migrations/add_tournament_type.sql` в Supabase SQL Editor.

## Переменные окружения

Добавьте в `.env` (backend) и настройте на сервере:

```bash
PUBLIC_BASE_URL=https://padel-payments.onrender.com
```

Для локальной разработки:
```bash
PUBLIC_BASE_URL=http://127.0.0.1:8000
```

## Изменения в коде

### 1. scripts/import_lunda.py
- ✅ Импортирует `tournament_type` из JSON
- ✅ Сохраняет в `tournaments.tournament_type`
- ✅ Логирует импорт турниров

### 2. main.py
- ✅ `/p/e/{entry_id}` учитывает `tournament_type` (50% для team, 100% для personal)
- ✅ `process_new_entries` НЕ создает YooKassa payments массово
- ✅ Сохраняет вечные ссылки `${PUBLIC_BASE_URL}/p/e/{entry_id}` в `payment_url`
- ✅ Telegram уведомления используют вечные ссылки
- ✅ Время отображается в MSK (Europe/Moscow)

### 3. admin/app/page.tsx
- ✅ Добавлена колонка "Тип" (team/personal)
- ✅ Добавлена колонка "Обновлено" (source_last_updated в MSK)
- ✅ Время турнира отображается в MSK
- ✅ Кнопка "Скопировать ссылку" скрыта для paid entries

## Проверка

1. После импорта JSON проверьте `tournaments.tournament_type` в Supabase
2. В админке должны быть видны колонки "Тип" и "Обновлено"
3. При копировании ссылки должна быть вечная ссылка `/p/e/{id}`
4. YooKassa payment создается только при открытии `/p/e/{id}`
5. TooManyRequestsError должен исчезнуть






