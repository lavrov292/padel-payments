#!/bin/bash
# Скрипт для проверки статуса launchd задачи и времени следующего запуска

echo "=========================================="
echo "Проверка launchd задачи импорта"
echo "=========================================="
echo ""

# 1. Проверка существования задачи
echo "1. Существует ли задача в launchd:"
if launchctl list | grep -q "com.padel.lunda.import"; then
    echo "   ✅ Задача найдена: com.padel.lunda.import"
    echo ""
    
    # Детальная информация
    echo "2. Детальная информация о задаче:"
    launchctl list com.padel.lunda.import 2>/dev/null | while IFS= read -r line; do
        echo "   $line"
    done
    echo ""
    
    # Проверка plist файла
    echo "3. Файл конфигурации:"
    if [ -f ~/Library/LaunchAgents/com.padel.lunda.import.plist ]; then
        echo "   ✅ Файл существует: ~/Library/LaunchAgents/com.padel.lunda.import.plist"
        
        # Извлечь StartInterval
        INTERVAL=$(grep -A1 "StartInterval" ~/Library/LaunchAgents/com.padel.lunda.import.plist | grep -o "[0-9]*" | head -1)
        if [ -n "$INTERVAL" ]; then
            HOURS=$((INTERVAL / 3600))
            MINUTES=$(((INTERVAL % 3600) / 60))
            echo "   📅 Интервал запуска: $INTERVAL секунд ($HOURS ч $MINUTES мин)"
        fi
    else
        echo "   ❌ Файл не найден"
    fi
    echo ""
    
    # 4. Последний запуск из логов
    echo "4. Последний запуск (из логов):"
    LAST_RUN=$(grep "RUN START:" ~/lunda_import.log 2>/dev/null | tail -1)
    if [ -n "$LAST_RUN" ]; then
        LAST_TIME=$(echo "$LAST_RUN" | grep -o "Time=[0-9-]* [0-9:]*" | cut -d= -f2)
        LAST_PID=$(echo "$LAST_RUN" | grep -o "PID=[0-9]*" | cut -d= -f2)
        echo "   Время: $LAST_TIME"
        echo "   PID: $LAST_PID"
        
        # Проверить, был ли это автоматический запуск
        LAST_TYPE=$(grep -A1 "RUN START: PID=$LAST_PID" ~/lunda_import.log 2>/dev/null | grep "RUN TYPE:" | cut -d: -f2 | xargs)
        if [ -n "$LAST_TYPE" ]; then
            echo "   Тип: $LAST_TYPE"
        fi
    else
        echo "   Не найдено записей о запусках"
    fi
    echo ""
    
    # 5. Примерное время следующего запуска
    echo "5. Примерное время следующего запуска:"
    if [ -n "$INTERVAL" ] && [ -n "$LAST_TIME" ]; then
        # Конвертируем время в секунды с начала эпохи
        LAST_EPOCH=$(date -j -f "%Y-%m-%d %H:%M:%S" "$LAST_TIME" "+%s" 2>/dev/null || date -j -f "%Y-%m-%d %H:%M" "$LAST_TIME" "+%s" 2>/dev/null)
        if [ -n "$LAST_EPOCH" ]; then
            NEXT_EPOCH=$((LAST_EPOCH + INTERVAL))
            NEXT_TIME=$(date -r "$NEXT_EPOCH" "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -j -f "%s" "$NEXT_EPOCH" "+%Y-%m-%d %H:%M:%S" 2>/dev/null)
            NOW_EPOCH=$(date +%s)
            
            if [ "$NEXT_EPOCH" -gt "$NOW_EPOCH" ]; then
                SECONDS_LEFT=$((NEXT_EPOCH - NOW_EPOCH))
                HOURS_LEFT=$((SECONDS_LEFT / 3600))
                MINUTES_LEFT=$(((SECONDS_LEFT % 3600) / 60))
                echo "   ⏰ Следующий запуск: $NEXT_TIME"
                echo "   ⏳ Осталось: ~$HOURS_LEFT ч $MINUTES_LEFT мин"
            else
                echo "   ⚠️  Время следующего запуска уже прошло"
                echo "   (Возможно, Mac спал или задача не запустилась)"
            fi
        else
            echo "   Не удалось вычислить время"
        fi
    else
        echo "   Недостаточно данных для вычисления"
    fi
    echo ""
    
    # 6. Проверка, запущен ли процесс сейчас
    echo "6. Запущен ли процесс сейчас:"
    CURRENT_PID=$(ps aux | grep "[i]mport_lunda.py" | awk '{print $2}' | head -1)
    if [ -n "$CURRENT_PID" ]; then
        echo "   ✅ Процесс запущен (PID: $CURRENT_PID)"
        START_TIME=$(ps -p "$CURRENT_PID" -o lstart= 2>/dev/null | xargs)
        if [ -n "$START_TIME" ]; then
            echo "   Время запуска: $START_TIME"
        fi
    else
        echo "   ❌ Процесс не запущен"
    fi
    echo ""
    
else
    echo "   ❌ Задача НЕ найдена в launchd"
    echo ""
    echo "   Попробуй загрузить задачу:"
    echo "   launchctl load ~/Library/LaunchAgents/com.padel.lunda.import.plist"
    echo ""
fi

# 7. Статус задачи (если доступно)
echo "7. Статус задачи:"
LAST_EXIT=$(launchctl list com.padel.lunda.import 2>/dev/null | grep "LastExitStatus" | grep -o "[0-9]*")
if [ -n "$LAST_EXIT" ]; then
    if [ "$LAST_EXIT" = "0" ]; then
        echo "   ✅ Последний запуск успешен (код: 0)"
    else
        echo "   ❌ Последний запуск завершился с ошибкой (код: $LAST_EXIT)"
        echo "   Проверь логи: ~/lunda_import_error.log"
    fi
fi
echo ""

echo "=========================================="
echo "Полезные команды:"
echo "=========================================="
echo "  Перезагрузить задачу:"
echo "    launchctl unload ~/Library/LaunchAgents/com.padel.lunda.import.plist"
echo "    launchctl load ~/Library/LaunchAgents/com.padel.lunda.import.plist"
echo ""
echo "  Запустить вручную:"
echo "    python scripts/import_lunda.py"
echo ""
echo "  Посмотреть логи:"
echo "    tail -f ~/lunda_import.log"
echo "    tail -f ~/lunda_import_error.log"
echo "=========================================="





