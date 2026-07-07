# Lunda Collector Prototype

Новый прототип для сбора статистики Lunda на базе старого локального парсера.

## Что уже собрано в новом контуре

- Tailscale APK установлен на телефон через `adb`, потому что Play Market его не находил. Дальше нужен вход в аккаунт Tailscale на телефоне.
- Lunda Padel обновлена до `versionName=19.0`, старую версию приложение уже блокировало экраном обновления.
- Добавлен новый SQLite-контур: см. [DATA_MODEL.md](DATA_MODEL.md).
- Живой сборщик `live_phone_cycle.py` умеет читать расписание и открывать полностью видимые карточки для сбора участников.
- Парсер участников отфильтровывает аватарные инициалы вроде `LR`, `MD`, `РД`, `ТИ` и останавливается перед покинувшими турнир.

## Первый безопасный тест телефона

1. Подключить Android-телефон по USB.
2. На телефоне включить:
   - Developer options
   - USB debugging
3. Подтвердить RSA-запрос от Mac на экране телефона.
4. Проверить ADB:

```bash
adb devices -l
```

5. Открыть Lunda Padel на экране турниров.
6. Запустить smoke test:

```bash
cd "/Users/kirill/Documents/Lunda stats"
/Users/kirill/android_parser_service/parser_russian_version/venv/bin/python lunda_collector/phone_smoke_test.py
```

Короткий вариант:

```bash
./lunda_collector/run_phone_smoke.sh
```

Скрипт создаст папку `work/phone_smoke_YYYYMMDD_HHMMSS` и сохранит:

- `screen.png` - скриншот телефона
- `ocr_raw.json` - сырой ответ Yandex OCR
- `ocr_text.txt` - распознанный текст
- `visible_tournaments.json` - распарсенные видимые турниры
- `visible_tournaments.xlsx` - Excel с видимыми турнирами

Скрипт не нажимает кнопки и не скроллит.

## Мини-скан списка турниров

Когда Lunda открыта на вкладке "Игры/Турниры", можно собрать несколько экранов списка с мягкой прокруткой:

```bash
cd "/Users/kirill/Documents/Lunda stats"
./lunda_collector/run_phone_smoke.sh scan_tournament_list.py --screens 5 --scroll-pixels 180
```

Скрипт сохраняет `observations.json`, склеенный `tournaments.json`, `tournaments.xlsx` и скриншоты каждого шага.

Важная логика: карточка турнира может быть видна не полностью. Зеленая кнопка `+`, нижнее меню и верхняя шапка могут закрывать строки карточки. Поэтому скрипт сохраняет неполные наблюдения и склеивает их без дублей по организатору, дате и времени. В Excel есть поля `is_complete`, `missing_fields`, `near_top_obstruction`, `near_bottom_obstruction`.

## Скан участников открытого турнира

Когда открыта детальная карточка турнира, можно собрать видимый ниже список участников или команд:

```bash
./lunda_collector/run_phone_smoke.sh scan_tournament_participants.py --type auto --screens 8 --expected-count 16
```

Для командных турниров `expected-count` равен количеству команд умножить на 2. Парсер останавливается на разделе `Игроки, покинувшие турнир`, потому что эти люди уже не участвуют. Имена с переносом на две строки склеиваются обратно в одну запись.

Чтобы из детальной карточки открыть список команд/участников:

```bash
./lunda_collector/run_phone_smoke.sh open_participants_section.py
```

Чтобы из списка участников или детальной карточки вернуться к списку турниров:

```bash
./lunda_collector/run_phone_smoke.sh back_to_tournament_list.py
```

Навигация назад работает только с проверкой OCR:

- экран команд/участников -> Back в детальную карточку
- детальная карточка -> Back в список турниров
- список турниров -> стоп, Back больше не нажимается
- главная вкладка -> нажать нижнюю вкладку "Играть"
- экран входа или неизвестный экран -> стоп без Back

## Новая локальная БД

Инициализация:

```bash
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 init-db
```

Сбор расписания напрямую с телефона:

```bash
/Users/kirill/android_parser_service/parser_russian_version/venv/bin/python \
  lunda_collector/live_phone_cycle.py \
  --db work/lunda.sqlite3 \
  schedule --launch --screens 40 --days 21
```

Сбор участников ближайших турниров целевого дня:

```bash
/Users/kirill/android_parser_service/parser_russian_version/venv/bin/python \
  lunda_collector/live_phone_cycle.py \
  --db work/lunda.sqlite3 \
  today-participants --launch --screens 40 --participants-screens 12
```

Текущие итоги/рейтинг:

```bash
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 summary
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 rankings --limit 100
```

Excel:

```bash
pip install -r lunda_collector/requirements.txt
python3 lunda_collector/collector_cli.py --db work/lunda.sqlite3 export
```

## ADB по Wi-Fi для локального теста

Для подключения с VPS см. [REMOTE_ADB_VPS.md](REMOTE_ADB_VPS.md).

После успешного USB-подключения можно включить ADB по Wi-Fi:

```bash
adb tcpip 5555
adb shell ip route
adb connect PHONE_WIFI_IP:5555
adb devices -l
```

После этого USB можно отключить и проверить smoke test еще раз.

Для Android 12 также есть штатная "Отладка по Wi-Fi" с pairing code:

```bash
adb pair PHONE_WIFI_IP:PAIRING_PORT PAIRING_CODE
adb connect PHONE_WIFI_IP:CONNECT_PORT
```

Порт для pairing и порт для подключения обычно разные. Они показываются на телефоне в меню "Отладка по Wi-Fi".

Если `adb connect` пишет `No route to host`, проблема не в парсере и не в OCR: Mac не может открыть сетевое соединение к телефону. Проверки:

```bash
ping PHONE_WIFI_IP
adb shell ping -c 2 MAC_WIFI_IP
adb shell ss -lntp | grep 5555
```

Частые причины: Mac и телефон на разных Wi-Fi сегментах/диапазонах с изоляцией клиентов, гостевая сеть, VPN/kill switch на Mac, настройки роутера вроде AP isolation/client isolation.

Важно: такой ADB по Wi-Fi работает нормально в локальной сети. Для облачного сервера напрямую это не подходит, потому что телефон обычно находится за домашним роутером. Не стоит открывать порт `5555` в интернет.

## Варианты для постоянной работы без ноутбука

Надежные варианты:

- Маленький всегда включенный хост рядом с телефоном: Raspberry Pi, mini PC, старый Android TV box. Он держит ADB и запускает парсер.
- Облачный сервер + приватная сеть/VPN до телефона: Tailscale/ZeroTier или reverse tunnel. Это нужно тестировать на конкретном телефоне, потому что Android может ограничивать фоновые процессы.

Телефон сам по себе, без управляющего хоста или специальной accessibility-автоматизации, обычно не может надежно запускать Python-парсер, делать tap/swipe в другом приложении и отправлять скриншоты.
