# Локальный vs Render: сравнение вариантов запуска импорта

## Текущая ситуация

У тебя настроен **локальный автозапуск** через macOS launchd:
- Файл: `~/Library/LaunchAgents/com.padel.lunda.import.plist`
- Логи: `/Users/kirill/lunda_import.log`
- JSON файл: локальный (путь в `LUNDA_JSON_PATH`)

---

## Сравнение вариантов

### Вариант 1: Оставить локально (текущий) ✅

**Плюсы:**
- ✅ JSON файл всегда доступен локально
- ✅ Не нужно настраивать доступ к файлу в Render
- ✅ Можно легко обновлять JSON вручную
- ✅ Не зависит от интернета для чтения JSON
- ✅ Бесплатно (твой Mac работает)

**Минусы:**
- ❌ Импорт работает только когда Mac включен
- ❌ Если Mac спит - импорт не выполняется
- ❌ Нужно держать Mac включенным 24/7
- ❌ Локальные логи (нужно заходить на Mac)

**Когда использовать:**
- Если JSON обновляется вручную на твоем Mac
- Если не критично, что импорт может пропуститься когда Mac спит
- Если хочешь полный контроль над процессом

---

### Вариант 2: Перенести в Render

**Плюсы:**
- ✅ Работает 24/7 независимо от твоего Mac
- ✅ Логи доступны в Render Dashboard
- ✅ Не зависит от состояния твоего компьютера
- ✅ Можно настроить мониторинг и алерты

**Минусы:**
- ❌ Нужно как-то доставлять JSON в Render
- ❌ Render может "засыпать" на бесплатном плане (нужно настроить keep-alive)
- ❌ Платно (если не бесплатный план)

**Варианты доставки JSON в Render:**

#### 2a. JSON через переменную окружения (для маленьких файлов)
```bash
# В Render Dashboard → Environment Variables
LUNDA_JSON_PATH=/tmp/tournaments.json
# И загрузить JSON через API или скрипт
```

#### 2b. JSON через URL (скачивать при каждом импорте)
Модифицировать `import_lunda.py`:
```python
json_url = os.getenv("LUNDA_JSON_URL")  # https://example.com/tournaments.json
response = requests.get(json_url)
data = json.loads(response.text)
```

#### 2c. JSON через S3/Cloud Storage
```python
import boto3
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='bucket', Key='tournaments.json')
data = json.loads(obj['Body'].read())
```

#### 2d. JSON через Git репозиторий
```bash
# В Render Cron Job
git pull && python scripts/import_lunda.py
# JSON файл в репозитории
```

---

## Рекомендация

**Оставить локально**, если:
- JSON обновляется вручную на Mac
- Mac обычно включен
- Не критично пропустить импорт когда Mac спит

**Перенести в Render**, если:
- Нужна гарантия работы 24/7
- JSON можно получать по URL или из облака
- Готов настроить доставку JSON в Render

---

## Гибридный вариант (лучший)

**Локально для разработки + Render для продакшена:**

1. **Локально:** оставить текущий launchd для тестирования
2. **Render:** настроить Cron Job, который:
   - Скачивает JSON по URL (если есть API Lunda)
   - Или читает из S3/облака
   - Или клонирует Git репозиторий с JSON

**Преимущества:**
- Тестируешь локально быстро
- Продакшен работает стабильно 24/7
- JSON обновляется автоматически (если есть API)

---

## Как проверить текущий запуск

```bash
# Проверить статус launchd задачи
launchctl list com.padel.lunda.import

# Посмотреть логи
tail -f ~/lunda_import.log

# Посмотреть ошибки
tail -f ~/lunda_import_error.log

# Проверить расписание в plist
cat ~/Library/LaunchAgents/com.padel.lunda.import.plist
```

---

## Если решишь перенести в Render

1. **Создать Cron Job в Render Dashboard**
2. **Настроить переменные окружения:**
   - `DATABASE_URL` (уже есть)
   - `LUNDA_JSON_PATH` или `LUNDA_JSON_URL`
   - `BACKEND_BASE_URL`
   - `ADMIN_TELEGRAM_ID`
   - `TELEGRAM_BOT_TOKEN`

3. **Выбрать способ доставки JSON** (URL/S3/Git)

4. **Отключить локальный launchd:**
   ```bash
   launchctl unload ~/Library/LaunchAgents/com.padel.lunda.import.plist
   ```





