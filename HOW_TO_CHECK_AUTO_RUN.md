# Как проверить автоматический запуск импорта

## Проблема

Ты заметил, что автоматический запуск может не работать. Теперь добавлена возможность различать автоматические и ручные запуски.

## Что изменилось

1. **Добавлена метка типа запуска** в логи:
   - `RUN TYPE: AUTOMATIC (launchd)` - автоматический запуск через launchd
   - `RUN TYPE: MANUAL` - ручной запуск

2. **Создан скрипт проверки** `check_import_status.sh`

## Как проверить последний автоматический запуск

### Вариант 1: Через скрипт (проще всего)

```bash
cd /Users/kirill/Documents/Padel\ Payment\ SaaS/padel-payments
./check_import_status.sh
```

Скрипт покажет:
- Статус launchd задачи
- Последние автоматические запуски
- Последние ручные запуски
- Время последнего изменения лога
- Последние ошибки

### Вариант 2: Через grep в логах

```bash
# Последние автоматические запуски
grep "RUN TYPE: AUTOMATIC" ~/lunda_import.log | tail -5

# Последние ручные запуски
grep "RUN TYPE: MANUAL" ~/lunda_import.log | tail -5

# Все запуски с временем
grep "RUN START:" ~/lunda_import.log | tail -10
```

### Вариант 3: Проверить статус launchd

```bash
# Статус задачи
launchctl list com.padel.lunda.import

# Если LastExitStatus != 0 - была ошибка
# Если LastExitStatus = 0 - последний запуск успешен
```

## Что делать если автоматический запуск не работает

### 1. Проверить ошибки

```bash
tail -50 ~/lunda_import_error.log
```

### 2. Перезагрузить launchd задачу

```bash
launchctl unload ~/Library/LaunchAgents/com.padel.lunda.import.plist
launchctl load ~/Library/LaunchAgents/com.padel.lunda.import.plist
```

### 3. Проверить, что задача загружена

```bash
launchctl list | grep lunda
```

Должна быть строка с `com.padel.lunda.import`

### 4. Проверить расписание

Задача настроена на запуск каждые 3600 секунд (1 час).

Если Mac спит - задача не выполнится. После пробуждения Mac запустится по расписанию.

### 5. Запустить вручную для теста

```bash
# С переменной окружения (будет помечен как AUTOMATIC)
LAUNCHD_AUTO_RUN=true python scripts/import_lunda.py

# Без переменной (будет помечен как MANUAL)
python scripts/import_lunda.py
```

## Интерпретация результатов

### Если в логах нет "RUN TYPE: AUTOMATIC"
- Это значит, что автоматический запуск не работал с момента добавления этой метки
- Нужно перезагрузить launchd задачу (см. выше)

### Если LastExitStatus != 0
- Последний запуск завершился с ошибкой
- Проверь `~/lunda_import_error.log`

### Если последний автоматический запуск был давно
- Mac мог спать
- Задача могла быть остановлена
- Перезагрузи задачу

## Пример вывода скрипта

```
==========================================
Проверка статуса автоматического импорта
==========================================

1. Статус launchd задачи:
   LastExitStatus = 0  ← успешно
   Label = com.padel.lunda.import

2. Последние автоматические запуски:
   RUN START: PID=12345, Time=2026-01-07 10:00:00 +0300 MSK
   RUN TYPE: AUTOMATIC (launchd)

3. Последние ручные запуски:
   RUN START: PID=12346, Time=2026-01-07 11:30:00 +0300 MSK
   RUN TYPE: MANUAL
```

## Важно

После изменений в `com.padel.lunda.import.plist` нужно перезагрузить задачу:
```bash
launchctl unload ~/Library/LaunchAgents/com.padel.lunda.import.plist
launchctl load ~/Library/LaunchAgents/com.padel.lunda.import.plist
```



