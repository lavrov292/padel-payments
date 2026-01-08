#!/bin/bash
# Скрипт для проверки статуса автоматического импорта

echo "=========================================="
echo "Проверка статуса автоматического импорта"
echo "=========================================="
echo ""

# Проверка статуса launchd задачи
echo "1. Статус launchd задачи:"
launchctl list com.padel.lunda.import 2>/dev/null | grep -E "(LastExitStatus|Label)" || echo "   Задача не найдена или не запущена"
echo ""

# Последние автоматические запуски
echo "2. Последние автоматические запуски (RUN TYPE: AUTOMATIC):"
grep "RUN TYPE: AUTOMATIC" ~/lunda_import.log 2>/dev/null | tail -5 || echo "   Не найдено автоматических запусков"
echo ""

# Последние ручные запуски
echo "3. Последние ручные запуски (RUN TYPE: MANUAL):"
grep "RUN TYPE: MANUAL" ~/lunda_import.log 2>/dev/null | tail -5 || echo "   Не найдено ручных запусков"
echo ""

# Последние 5 запусков вообще
echo "4. Последние 5 запусков (все):"
grep "RUN START:" ~/lunda_import.log 2>/dev/null | tail -5 || echo "   Нет записей о запусках"
echo ""

# Время последнего изменения лога
echo "5. Время последнего изменения лога:"
stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" ~/lunda_import.log 2>/dev/null || echo "   Файл лога не найден"
echo ""

# Проверка, когда должен быть следующий запуск (примерно)
echo "6. Расписание:"
echo "   Запуск каждые 3600 секунд (1 час)"
echo "   Последний автоматический запуск был:"
LAST_AUTO=$(grep "RUN TYPE: AUTOMATIC" ~/lunda_import.log 2>/dev/null | tail -1 | grep -o "Time=[0-9-]* [0-9:]*" | cut -d= -f2)
if [ -n "$LAST_AUTO" ]; then
    echo "   $LAST_AUTO"
    echo "   Следующий должен быть примерно через час после этого времени"
else
    echo "   Не найдено автоматических запусков"
fi
echo ""

# Проверка ошибок
echo "7. Последние ошибки:"
tail -10 ~/lunda_import_error.log 2>/dev/null | head -5 || echo "   Нет ошибок или файл не найден"
echo ""

echo "=========================================="
echo "Для перезагрузки задачи:"
echo "  launchctl unload ~/Library/LaunchAgents/com.padel.lunda.import.plist"
echo "  launchctl load ~/Library/LaunchAgents/com.padel.lunda.import.plist"
echo "=========================================="



