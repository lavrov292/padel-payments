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
if [ -f ~/lunda_import_error.log ]; then
    ERROR_FILE_TIME=$(stat -f "%Sm" -t "%Y-%m-%d %H:%M:%S" ~/lunda_import_error.log 2>/dev/null)
    ERROR_FILE_SIZE=$(wc -l < ~/lunda_import_error.log 2>/dev/null | xargs)
    
    if [ -n "$ERROR_FILE_TIME" ]; then
        echo "   Время последнего изменения файла ошибок: $ERROR_FILE_TIME"
        
        # Получаем время последнего запуска для сравнения
        LAST_RUN_TIME=$(grep "RUN START:" ~/lunda_import.log 2>/dev/null | tail -1 | grep -o "Time=[0-9-]* [0-9:]*" | cut -d= -f2)
        
        if [ -n "$LAST_RUN_TIME" ]; then
            # Конвертируем времена в секунды для сравнения
            ERROR_EPOCH=$(date -j -f "%Y-%m-%d %H:%M:%S" "$ERROR_FILE_TIME" "+%s" 2>/dev/null)
            LAST_RUN_EPOCH=$(date -j -f "%Y-%m-%d %H:%M:%S" "$LAST_RUN_TIME" "+%s" 2>/dev/null)
            
            if [ -n "$ERROR_EPOCH" ] && [ -n "$LAST_RUN_EPOCH" ]; then
                if [ "$ERROR_EPOCH" -ge "$LAST_RUN_EPOCH" ]; then
                    echo "   ⚠️  Ошибки из последнего или текущего запуска!"
                else
                    SECONDS_OLD=$((LAST_RUN_EPOCH - ERROR_EPOCH))
                    HOURS_OLD=$((SECONDS_OLD / 3600))
                    DAYS_OLD=$((HOURS_OLD / 24))
                    if [ "$DAYS_OLD" -gt 0 ]; then
                        echo "   ℹ️  Ошибки старые (около $DAYS_OLD дн. назад, до последнего запуска)"
                    elif [ "$HOURS_OLD" -gt 0 ]; then
                        echo "   ℹ️  Ошибки старые (около $HOURS_OLD ч. назад, до последнего запуска)"
                    else
                        echo "   ℹ️  Ошибки старые (до последнего запуска)"
                    fi
                fi
            fi
        fi
        
        if [ "$ERROR_FILE_SIZE" -gt 0 ]; then
            echo "   Строк в файле ошибок: $ERROR_FILE_SIZE"
            echo ""
            echo "   Последние строки ошибок:"
            tail -10 ~/lunda_import_error.log 2>/dev/null | head -5 | sed 's/^/   /'
        else
            echo "   Файл пустой (нет ошибок)"
        fi
    else
        tail -10 ~/lunda_import_error.log 2>/dev/null | head -5 | sed 's/^/   /' || echo "   Не удалось прочитать файл"
    fi
else
    echo "   ✅ Файл ошибок не найден (ошибок нет)"
fi
echo ""

# Проверка, запущен ли процесс сейчас
echo "8. Работает ли импорт прямо сейчас:"
CURRENT_PID=$(ps aux | grep "[i]mport_lunda.py" | awk '{print $2}' | head -1)
if [ -n "$CURRENT_PID" ]; then
    echo "   ✅ Импорт работает прямо сейчас (PID: $CURRENT_PID)"
    START_TIME=$(ps -p "$CURRENT_PID" -o lstart= 2>/dev/null | xargs)
    if [ -n "$START_TIME" ]; then
        echo "   Время запуска: $START_TIME"
    fi
    # Проверяем, сколько времени работает
    RUNTIME=$(ps -p "$CURRENT_PID" -o etime= 2>/dev/null | xargs)
    if [ -n "$RUNTIME" ]; then
        echo "   Время работы: $RUNTIME"
    fi
else
    echo "   ❌ Импорт не запущен"
fi
echo ""

echo "=========================================="
echo "Для перезагрузки задачи:"
echo "  launchctl unload ~/Library/LaunchAgents/com.padel.lunda.import.plist"
echo "  launchctl load ~/Library/LaunchAgents/com.padel.lunda.import.plist"
echo "=========================================="





