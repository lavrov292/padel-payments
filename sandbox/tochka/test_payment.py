#!/usr/bin/env python3
"""
Тестовый скрипт для создания платёжной ссылки в песочнице Точка банк
"""
import os
import json
import requests
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройки песочницы
SANDBOX_BASE_URL = "https://enter.tochka.com/sandbox/v2"
SANDBOX_TOKEN = "sandbox.jwt.token"  # Специальный токен для песочницы

def test_api_connection():
    """Проверяет подключение к API"""
    print("🔍 Проверяю подключение к API...")
    
    # Пробуем простой GET запрос для проверки доступности
    test_endpoints = [
        "/",
        "/health",
        "/status",
    ]
    
    headers = {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Content-Type": "application/json"
    }
    
    for endpoint in test_endpoints:
        url = f"{SANDBOX_BASE_URL}{endpoint}"
        try:
            response = requests.get(url, headers=headers, timeout=5)
            print(f"   {endpoint}: статус {response.status_code}")
            if response.status_code != 404:
                print(f"   Ответ: {response.text[:200]}")
        except Exception as e:
            print(f"   {endpoint}: ошибка - {e}")
    
    print()


def create_payment_link(amount_rub: float, customer_code: str = None, purpose: str = "Тестовый платёж", ttl_minutes: int = 60):
    """
    Создаёт платёжную ссылку в песочнице Точка банк
    
    Args:
        amount_rub: Сумма в рублях (будет конвертирована в копейки)
        customer_code: Уникальный код клиента (опционально)
        purpose: Назначение платежа
        ttl_minutes: Время жизни ссылки в минутах
    
    Returns:
        dict: Ответ от API с данными платёжной ссылки
    """
    # Конвертируем рубли в копейки
    amount_kopecks = int(amount_rub * 100)
    
    # Если customer_code не указан, генерируем тестовый
    if not customer_code:
        customer_code = f"test_customer_{os.urandom(4).hex()}"
    
    # Пробуем разные варианты эндпоинтов
    endpoints = [
        "/acquiring/payment-links",
        "/payment-links",
        "/acquiring/payment_link",
    ]
    
    headers = {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "amount": amount_kopecks,
        "customerCode": customer_code,
        "purpose": purpose,
        "ttl": ttl_minutes
    }
    
    print(f"🚀 Создаю платёжную ссылку...")
    print(f"   Сумма: {amount_rub} руб ({amount_kopecks} коп)")
    print(f"   Назначение: {purpose}")
    print(f"   TTL: {ttl_minutes} минут")
    print(f"   Customer Code: {customer_code}")
    print()
    
    # Пробуем каждый эндпоинт
    for endpoint in endpoints:
        url = f"{SANDBOX_BASE_URL}{endpoint}"
        print(f"🔍 Пробую: {endpoint}")
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            
            print(f"   Статус: {response.status_code}")
            
            if response.status_code in [200, 201]:
                print(f"✅ Успех!")
                result = response.json()
                print()
                print("📋 Ответ от API:")
                print(json.dumps(result, indent=2, ensure_ascii=False))
                print()
                
                # Извлекаем URL платёжной ссылки
                payment_link = None
                if isinstance(result, dict):
                    if "data" in result and isinstance(result["data"], dict):
                        payment_link = result["data"].get("paymentLink") or result["data"].get("link") or result["data"].get("url")
                    else:
                        payment_link = result.get("paymentLink") or result.get("link") or result.get("url")
                
                if payment_link:
                    print(f"🔗 Платёжная ссылка: {payment_link}")
                    print()
                    print("💡 Перейдите по ссылке выше для тестовой оплаты")
                
                return result
            else:
                print(f"   Ответ: {response.text[:300]}")
                print()
        
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Ошибка: {e}")
            print()
            continue
    
    print("❌ Все эндпоинты вернули ошибку")
    print()
    print("💡 Возможные причины:")
    print("   1. Эндпоинт может быть недоступен в песочнице")
    print("   2. Может потребоваться регистрация в личном кабинете Точка банк")
    print("   3. Проверьте актуальную документацию: https://developers.tochka.com/docs/tochka-api/")
    print()
    
    return None


def main():
    """Основная функция для тестирования"""
    print("=" * 60)
    print("🧪 Тестовый платёж в песочнице Точка банк")
    print("=" * 60)
    print()
    
    # Проверяем подключение
    test_api_connection()
    
    # Создаём тестовый платёж на 100 рублей
    result = create_payment_link(
        amount_rub=100.0,
        purpose="Тестовый платёж за турнир",
        ttl_minutes=60
    )
    
    if result:
        print("✅ Тест завершён успешно!")
    else:
        print("❌ Тест завершён с ошибкой")
        print()
        print("📝 Примечание: Если эндпоинт возвращает 501, возможно:")
        print("   - Метод не реализован в текущей версии песочницы")
        print("   - Требуется регистрация и настройка в личном кабинете")
        print("   - Проверьте актуальную документацию API")


if __name__ == "__main__":
    main()
