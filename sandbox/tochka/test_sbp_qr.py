#!/usr/bin/env python3
"""
Тестовый скрипт для регистрации QR-кода СБП в песочнице Точка банк
Документация: https://developers.tochka.com/docs/tochka-api/api/register-qr-code-sbp-v-1-0-qr-code-merchant-merchant-id-account-id-post
"""
import os
import json
import requests
from dotenv import load_dotenv
import sys
from datetime import datetime
import base64

# Загружаем переменные окружения
load_dotenv()

# Настройки песочницы
RS_URL = "https://enter.tochka.com/sandbox/v2"
SANDBOX_TOKEN = "sandbox.jwt.token"

# Тестовые данные из песочницы (выгружены из терминала)
TEST_ACCOUNT_IDS = [
    "12345123451234512345/044525104",
    "12345678901234567890/044525104",
    "12345810901234567890/044525104",
]

TEST_MERCHANTS = [
    {"merchantId": "200000000001097", "terminalId": "20000097", "name": "OOO ALTERO"},
    {"merchantId": "200000000001098", "terminalId": "20000090", "name": "OOO LUCH"},
]

# По умолчанию используем первый merchant и первый account
TEST_ACCOUNT_ID = os.getenv("TOCHKA_TEST_ACCOUNT_ID", TEST_ACCOUNT_IDS[0])
TEST_MERCHANT_ID = os.getenv("TOCHKA_TEST_MERCHANT_ID", TEST_MERCHANTS[0]["merchantId"])
TEST_TERMINAL_ID = os.getenv("TOCHKA_TEST_TERMINAL_ID", TEST_MERCHANTS[0]["terminalId"])


def register_qr_code(merchant_id: str, account_id: str, amount_kopecks: int = None, 
                     purpose: str = "Тестовый QR-код", is_static: bool = False):
    """
    Регистрирует QR-код СБП в песочнице
    
    Args:
        merchant_id: ID мерчанта
        account_id: ID счёта (формат: номер/БИК)
        amount_kopecks: Сумма в копейках (для динамического QR-кода)
        purpose: Назначение платежа
        is_static: True для статического QR-кода, False для динамического
    
    Returns:
        dict: Ответ от API с данными QR-кода
    """
    # Эндпоинт согласно документации (правильный путь для песочницы)
    url = f"{RS_URL}/sbp/v1.0/qr-code/merchant/{merchant_id}/{account_id}"
    
    headers = {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Content-Type": "application/json"
    }
    
    # Формируем тело запроса (нужно обернуть в "data" согласно API)
    # qrc_type: '01' для статического, '02' для динамического
    data_payload = {
        "payment_purpose": purpose,  # Правильное название поля
        "qrc_type": "01" if is_static else "02"  # Тип QR-кода: '01' или '02'
    }
    
    # Для динамического QR-кода добавляем сумму
    if not is_static and amount_kopecks:
        data_payload["amount"] = amount_kopecks
    
    # Обёртываем в "data" согласно требованиям API
    payload = {
        "data": data_payload
    }
    
    print(f"🚀 Регистрирую QR-код СБП...")
    print(f"   URL: {url}")
    print(f"   Merchant ID: {merchant_id}")
    print(f"   Account ID: {account_id}")
    print(f"   Тип: {'Статический' if is_static else 'Динамический'}")
    if amount_kopecks:
        print(f"   Сумма: {amount_kopecks / 100:.2f} руб ({amount_kopecks} коп)")
    print(f"   Назначение: {purpose}")
    print()
    print(f"   Payload: {json.dumps(payload, indent=2, ensure_ascii=False)}")
    print()
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        print(f"   Статус: {response.status_code}")
        
        if response.status_code in [200, 201]:
            result = response.json()
            print(f"   ✅ QR-код зарегистрирован!")
            print()
            print("📋 Ответ от API:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
            print()
            
            # Извлекаем данные QR-кода
            qr_data = None
            if isinstance(result, dict):
                qr_data = result.get("Data") or result.get("data") or result
            
            if qr_data and isinstance(qr_data, dict):
                qr_id = qr_data.get("qrcId") or qr_data.get("qrId") or qr_data.get("id")
                qr_payload = qr_data.get("payload") or qr_data.get("qrString") or qr_data.get("qr_code") or qr_data.get("qr")
                qr_image = qr_data.get("image")
                
                if qr_id:
                    print(f"   📱 QR ID: {qr_id}")
                if qr_payload:
                    print(f"   🔗 QR Payload (URL): {qr_payload}")
                    print()
                    print("⚠️  ВАЖНО: URL может не работать напрямую в браузере!")
                    print("   Используйте ИЗОБРАЖЕНИЕ QR-кода для сканирования через приложение банка")
                if qr_image and isinstance(qr_image, dict):
                    image_content = qr_image.get('content', '')
                    print(f"   🖼️  QR Image: {qr_image.get('width')}x{qr_image.get('height')} ({qr_image.get('mediaType')})")
                    print(f"   💾 Изображение доступно в base64 (длина: {len(image_content)} символов)")
                    
                    # Сохраняем изображение QR-кода в файл
                    if image_content and qr_id:
                        try:
                            image_data = base64.b64decode(image_content)
                            image_filename = f"qr_code_{qr_id}.png"
                            with open(image_filename, "wb") as img_file:
                                img_file.write(image_data)
                            print(f"   💾 Изображение сохранено: {image_filename}")
                            print(f"   📱 Откройте этот файл и отсканируйте через приложение банка!")
                        except Exception as e:
                            print(f"   ⚠️  Не удалось сохранить изображение: {e}")
            
            return result
        else:
            print(f"   ❌ Ошибка: {response.text}")
            try:
                error_data = response.json()
                print(f"   Детали ошибки:")
                print(json.dumps(error_data, indent=2, ensure_ascii=False))
            except:
                pass
            return None
    
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Ошибка запроса: {e}")
        return None
    except Exception as e:
        print(f"   ❌ Неожиданная ошибка: {e}")
        return None


def get_qr_payment_status(merchant_id: str, account_id: str, qrc_id: str):
    """
    Получает статус оплаты QR-кода СБП
    
    Args:
        merchant_id: ID мерчанта
        account_id: ID счёта
        qrc_id: ID QR-кода (qrcId из ответа при создании)
    
    Returns:
        dict: Ответ от API со статусом оплаты
    """
    print(f"\n📊 Проверяю статус оплаты QR-кода...")
    print(f"   QR ID: {qrc_id}")
    
    # Пробуем разные варианты эндпоинтов для статуса
    endpoints = [
        f"/sbp/v1.0/qr-code/merchant/{merchant_id}/{account_id}/{qrc_id}/payment-status",
        f"/sbp/v1.0/qr-code/{qrc_id}/payment-status",
        f"/sbp/v1.0/qr-code/merchant/{merchant_id}/account/{account_id}/qrc/{qrc_id}/status",
    ]
    
    headers = {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Content-Type": "application/json"
    }
    
    for endpoint in endpoints:
        url = f"{RS_URL}{endpoint}"
        print(f"   Пробую: {endpoint}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            print(f"   Статус HTTP: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"   ✅ Статус получен!")
                print()
                print("📋 Ответ от API:")
                print(json.dumps(result, indent=2, ensure_ascii=False))
                print()
                
                # Извлекаем информацию о статусе
                status_data = result.get("Data") or result.get("data") or result
                if isinstance(status_data, dict):
                    status = status_data.get("status") or status_data.get("paymentStatus")
                    amount = status_data.get("amount")
                    payment_date = status_data.get("paymentDate") or status_data.get("date")
                    
                    if status:
                        print(f"   💳 Статус оплаты: {status}")
                    if amount:
                        print(f"   💰 Сумма: {amount / 100:.2f} руб ({amount} коп)" if isinstance(amount, (int, float)) else f"   💰 Сумма: {amount}")
                    if payment_date:
                        print(f"   📅 Дата оплаты: {payment_date}")
                
                return result
            elif response.status_code != 404:
                print(f"   Ответ: {response.text[:300]}")
        except Exception as e:
            print(f"   Ошибка: {e}")
            continue
    
    print("   ❌ Не удалось получить статус (возможно, эндпоинт отличается)")
    return None


def get_qr_codes_list(merchant_id: str, account_id: str):
    """Получает список QR-кодов"""
    print(f"\n📋 Получаю список QR-кодов...")
    
    url = f"{RS_URL}/sbp/v1.0/qr-code/merchant/{merchant_id}/{account_id}"
    
    headers = {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        print(f"   Статус: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"   ✅ Список получен:")
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return result
        else:
            print(f"   Ответ: {response.text[:300]}")
            return None
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return None


def main():
    """Основная функция"""
    # Проверяем, нужно ли проверить статус
    if len(sys.argv) > 1 and sys.argv[1] == "--status":
        if len(sys.argv) < 5:
            print("❌ Ошибка: для проверки статуса нужны параметры:")
            print("   python3 test_sbp_qr.py --status <qrc_id> <merchant_id> <account_id>")
            print()
            print("Или используйте сохранённый QR-код:")
            print("   python3 test_sbp_qr.py --status-last")
            return
        
        qrc_id = sys.argv[2]
        merchant_id = sys.argv[3]
        account_id = sys.argv[4]
        
        print("=" * 60)
        print("📊 Проверка статуса оплаты QR-кода СБП")
        print("=" * 60)
        print()
        
        get_qr_payment_status(merchant_id, account_id, qrc_id)
        return
    
    if len(sys.argv) > 1 and sys.argv[1] == "--status-last":
        # Загружаем последний созданный QR-код
        try:
            with open("last_qr_code.json", "r") as f:
                qr_info = json.load(f)
            
            print("=" * 60)
            print("📊 Проверка статуса последнего QR-кода")
            print("=" * 60)
            print()
            
            get_qr_payment_status(
                qr_info["merchantId"],
                qr_info["accountId"],
                qr_info["qrcId"]
            )
            return
        except FileNotFoundError:
            print("❌ Файл last_qr_code.json не найден. Сначала создайте QR-код.")
            return
        except Exception as e:
            print(f"❌ Ошибка при загрузке last_qr_code.json: {e}")
            return
    
    print("=" * 60)
    print("🧪 Регистрация QR-кода СБП в песочнице Точка банк")
    print("=" * 60)
    print()
    
    # Получаем параметры из аргументов командной строки или переменных окружения
    merchant_id = sys.argv[1] if len(sys.argv) > 1 else TEST_MERCHANT_ID
    account_id = sys.argv[2] if len(sys.argv) > 2 else TEST_ACCOUNT_ID
    terminal_id = sys.argv[3] if len(sys.argv) > 3 else TEST_TERMINAL_ID
    
    if not merchant_id:
        print("❌ Ошибка: не указан merchant_id")
        print()
        print("Использование:")
        print("  python3 test_sbp_qr.py <merchant_id> [account_id] [terminal_id]")
        print()
        print("Или установите переменные окружения:")
        print("  export TOCHKA_TEST_MERCHANT_ID=ваш_merchant_id")
        print("  export TOCHKA_TEST_ACCOUNT_ID=ваш_account_id")
        print()
        print(f"Текущие значения:")
        print(f"  Account ID: {TEST_ACCOUNT_ID}")
        print(f"  Merchant ID: {merchant_id or '(не указан)'}")
        print(f"  Terminal ID: {terminal_id or '(не указан)'}")
        return
    
    print(f"📝 Параметры:")
    print(f"   Merchant ID: {merchant_id}")
    print(f"   Account ID: {account_id}")
    if terminal_id:
        print(f"   Terminal ID: {terminal_id}")
    print()
    print(f"💡 Доступные тестовые данные:")
    print(f"   Accounts: {', '.join(TEST_ACCOUNT_IDS)}")
    for m in TEST_MERCHANTS:
        print(f"   Merchant: {m['merchantId']} (Terminal: {m['terminalId']}, Name: {m['name']})")
    print()
    
    # Регистрируем динамический QR-код на 100 рублей
    result = register_qr_code(
        merchant_id=merchant_id,
        account_id=account_id,
        amount_kopecks=10000,  # 100 рублей
        purpose="Тестовый платёж за турнир",
        is_static=False
    )
    
    if result:
        print("\n✅ QR-код создан успешно!")
        
        # Извлекаем qrcId для проверки статуса
        qr_data = None
        if isinstance(result, dict):
            qr_data = result.get("Data") or result.get("data") or result
        
        qrc_id = None
        qr_payload_url = None
        if qr_data and isinstance(qr_data, dict):
            qrc_id = qr_data.get("qrcId") or qr_data.get("qrId")
            qr_payload_url = qr_data.get("payload")
        
        if qrc_id:
            print(f"\n" + "=" * 60)
            print("⚠️  ВАЖНО: ОГРАНИЧЕНИЯ ПЕСОЧНИЦЫ")
            print("=" * 60)
            print()
            print("❌ В ПЕСОЧНИЦЕ НЕВОЗМОЖНО ПРОВЕСТИ РЕАЛЬНУЮ ОПЛАТУ!")
            print()
            print("Песочница Точка Банк предназначена только для:")
            print("  ✅ Тестирования API запросов (создание QR-кодов)")
            print("  ✅ Проверки формата ответов")
            print("  ✅ Интеграции с API")
            print()
            print("Песочница НЕ поддерживает:")
            print("  ❌ Реальные платежи через СБП")
            print("  ❌ Оплату через мобильное приложение банка")
            print("  ❌ Проверку статуса реальных платежей")
            print()
            print("=" * 60)
            print("📋 ЧТО МОЖНО ПРОТЕСТИРОВАТЬ:")
            print("=" * 60)
            print()
            print("1. ✅ QR-код успешно создан")
            print(f"   QR ID: {qrc_id}")
            if qr_payload_url:
                print(f"   URL: {qr_payload_url}")
            print()
            print("2. ✅ Изображение QR-кода сохранено:")
            print(f"   qr_code_{qrc_id}.png")
            print()
            print("3. ✅ Формат данных API проверен")
            print()
            print("=" * 60)
            print("🚀 ДЛЯ РЕАЛЬНОГО ТЕСТИРОВАНИЯ:")
            print("=" * 60)
            print()
            print("1. Зарегистрируйтесь в личном кабинете Точка Банк")
            print("2. Получите доступ к боевому контуру")
            print("3. Используйте реальные токены (не sandbox.jwt.token)")
            print("4. Измените URL с /sandbox/v2 на /uapi/v2")
            print("5. Создайте QR-код в боевом контуре")
            print("6. Тогда QR-код можно будет оплатить реально")
            print()
            print("=" * 60)
            
            # Сохраняем qrc_id в файл для удобства
            import json
            qr_info = {
                "qrcId": qrc_id,
                "merchantId": merchant_id,
                "accountId": account_id,
                "payload": qr_payload_url,
                "created_at": datetime.now().isoformat()
            }
            with open("last_qr_code.json", "w") as f:
                json.dump(qr_info, f, indent=2, ensure_ascii=False)
            print(f"💾 Информация о QR-коде сохранена в last_qr_code.json")
        
        # Пробуем получить список QR-кодов
        # get_qr_codes_list(merchant_id, account_id)
    else:
        print("\n❌ Тест завершён с ошибкой")
        print()
        print("💡 Проверьте:")
        print("   1. Правильность merchant_id и account_id")
        print("   2. Что используете правильный URL песочницы")
        print("   3. Что токен sandbox.jwt.token работает")
        print("   4. Документацию: https://developers.tochka.com/docs/tochka-api/api/register-qr-code-sbp-v-1-0-qr-code-merchant-merchant-id-account-id-post")


if __name__ == "__main__":
    main()

