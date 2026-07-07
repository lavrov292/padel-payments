# Remote ADB From VPS

Цель: VPS управляет физическим Android-телефоном через ADB и запускает парсер без эмулятора.

## Архитектура

Не открываем порт `5555` в публичный интернет.

Нормальная схема:

```text
VPS -- Tailscale/ZeroTier private network -- Android phone
```

Телефон остается дома в розетке и Wi-Fi. VPS подключается к приватному IP телефона в VPN-сети.

## Телефон

1. Включить Developer options.
2. Включить USB debugging.
3. Подключить телефон по USB к Mac один раз после перезагрузки.
4. Включить ADB TCP:

```bash
adb tcpip 5555
```

5. Установить Tailscale на Android, войти в тот же аккаунт/tailnet, отключить battery optimization для Tailscale.
6. Посмотреть Tailscale IP телефона в приложении. Обычно это `100.x.y.z`.

Важно: `adb tcpip 5555` обычно сбрасывается после перезагрузки телефона или перезапуска ADB debug. Тогда USB нужен снова.

На текущем телефоне Tailscale уже установлен вручную через официальный APK:

```text
com.tailscale.ipn
tailscale-android-universal-1.98.8.apk
```

Дальше требуется только интерактивный вход на телефоне: Google/GitHub/email или QR/code в Tailscale.

## VPS

Ubuntu/Debian:

```bash
sudo apt update
sudo apt install -y android-tools-adb python3 python3-venv git
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

После авторизации VPS должен появиться в той же Tailscale-сети, что и телефон.

Проверка связи:

```bash
tailscale status
ping PHONE_TAILSCALE_IP
adb connect PHONE_TAILSCALE_IP:5555
adb devices -l
```

На телефоне может появиться RSA prompt от VPS. Нужно разрешить и выбрать "Always allow".

## Запуск smoke-test на VPS

```bash
git clone https://github.com/lavrov292/padel-payments.git
cd padel-payments
python3 -m venv .venv
. .venv/bin/activate
pip install -r lunda_collector/requirements.txt
```

Создать `.env` на VPS:

```bash
YANDEX_OCR_API_KEY=...
YANDEX_OCR_FOLDER_ID=...
PHONE_ADB_HOST=PHONE_TAILSCALE_IP:5555
```

Запустить:

```bash
python3 lunda_collector/remote_adb_smoke.py
```

Скрипт сделает:

- `adb connect`
- скриншот телефона
- Yandex OCR
- `visible_tournaments.json`

## Локальная проверка без VPS

Если телефон подключен по USB:

```bash
python3 lunda_collector/remote_adb_smoke.py --env-file /path/to/.env
```

Если Mac может достучаться до телефона по Wi-Fi:

```bash
python3 lunda_collector/remote_adb_smoke.py --adb-host 192.168.0.216:5555 --env-file /path/to/.env
```

Если получаем `No route to host`, проблема в локальной сети/VPN, а не в парсере.
