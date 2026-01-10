#!/usr/bin/env python3
"""
Тестовый скрипт для создания платёжной ссылки в песочнице Точка банк
с использованием OAuth 2.0 авторизации (как в Postman окружении)
"""
import os
import json
import requests
from dotenv import load_dotenv
import base64

# Загружаем переменные окружения
load_dotenv()

# Настройки из Postman окружения для песочницы
AS_URL = "https://enter.tochka.com"  # Authorization Server URL
RS_URL = "https://enter.tochka.com/sandbox/v2"  # Resource Server URL
CLIENT_ID = "test_app"
CLIENT_SECRET = "test_secret"
SCOPE = "accounts balances customers statements sbp payments acquiring"
REDIRECT_URI = "http://localhost/"
TEST_ACCOUNT_ID = "12345810901234567890/044525104"  # Тестовый accountId из Postman

def get_oauth_token():
    """
    Получает OAuth 2.0 токен используя client credentials flow
    Пробует разные варианты эндпоинтов
    """
    print("🔐 Получаю OAuth 2.0 токен...")
    
    # Пробуем разные варианты эндпоинтов для получения токена
    token_endpoints = [
        f"{AS_URL}/oauth2/token",
        f"{AS_URL}/oauth/token",
        f"{AS_URL}/api/oauth2/token",
        f"{RS_URL}/oauth2/token",
    ]
    
    # Basic Auth для client credentials
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    data = {
        "grant_type": "client_credentials",
        "scope": SCOPE
    }
    
    for token_url in token_endpoints:
        print(f"   Пробую: {token_url}")
        try:
            response = requests.post(token_url, headers=headers, data=data, timeout=10)
            
            print(f"   Статус: {response.status_code}")
            
            if response.status_code == 200:
                token_data = response.json()
                access_token = token_data.get("access_token")
                print(f"   ✅ Токен получен: {access_token[:20]}...")
                return access_token
            else:
                print(f"   Ответ: {response.text[:200]}")
        except Exception as e:
            print(f"   Ошибка: {e}")
            continue
    
    # Если OAuth не работает, используем гибридный токен из Postman
    print("\n   ⚠️ OAuth не сработал, использую гибридный токен из Postman")
    hybrid_token = "sandbox.jwt.token"
    print(f"   ✅ Использую: {hybrid_token}")
    return hybrid_token


def get_accounts(access_token):
    """Получает список счетов"""
    print("\n📋 Получаю список счетов...")
    
    # Пробуем разные варианты эндпоинтов
    endpoints = [
        "/accounts",
        "/v2/accounts",
        "/api/accounts",
        "/api/v2/accounts",
    ]
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    for endpoint in endpoints:
        url = f"{RS_URL}{endpoint}"
        print(f"   Пробую: {endpoint}")
        try:
            response = requests.get(url, headers=headers, timeout=10)
            print(f"   Статус: {response.status_code}")
            
            if response.status_code == 200:
                accounts = response.json()
                print(f"   ✅ Счета получены:")
                print(json.dumps(accounts, indent=2, ensure_ascii=False)[:500])
                return accounts
            elif response.status_code != 404:
                print(f"   Ответ: {response.text[:300]}")
        except Exception as e:
            print(f"   Ошибка: {e}")
            continue
    
    print("   ❌ Не удалось получить счета")
    return None


def get_balance(access_token, account_id=None):
    """Получает баланс счёта"""
    if not account_id:
        account_id = TEST_ACCOUNT_ID
    
    print(f"\n💰 Получаю баланс счёта {account_id}...")
    
    url = f"{RS_URL}/balances"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    params = {
        "accountId": account_id
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        print(f"   Статус: {response.status_code}")
        
        if response.status_code == 200:
            balance = response.json()
            print(f"   ✅ Баланс получен:")
            print(json.dumps(balance, indent=2, ensure_ascii=False))
            return balance
        else:
            print(f"   Ответ: {response.text[:300]}")
            return None
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return None


def create_payment_link(access_token, amount_rub: float, customer_code: str = None, purpose: str = "Тестовый платёж", ttl_minutes: int = 60):
    """
    Создаёт платёжную ссылку в песочнице Точка банк
    """
    amount_kopecks = int(amount_rub * 100)
    
    if not customer_code:
        customer_code = f"test_customer_{os.urandom(4).hex()}"
    
    # Пробуем разные варианты эндпоинтов
    endpoints = [
        "/acquiring/payment-links",
        "/payment-links",
    ]
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "amount": amount_kopecks,
        "customerCode": customer_code,
        "purpose": purpose,
        "ttl": ttl_minutes
    }
    
    print(f"\n🚀 Создаю платёжную ссылку...")
    print(f"   Сумма: {amount_rub} руб ({amount_kopecks} коп)")
    print(f"   Назначение: {purpose}")
    print(f"   TTL: {ttl_minutes} минут")
    print(f"   Customer Code: {customer_code}")
    print()
    
    for endpoint in endpoints:
        url = f"{RS_URL}{endpoint}"
        print(f"🔍 Пробую: {endpoint}")
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=10)
            print(f"   Статус: {response.status_code}")
            
            if response.status_code in [200, 201]:
                print(f"   ✅ Успех!")
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
    return None


def main():
    """Основная функция для тестирования"""
    print("=" * 60)
    print("🧪 Тестовый платёж в песочнице Точка банк (OAuth 2.0)")
    print("=" * 60)
    print()
    print(f"📝 Настройки из Postman окружения:")
    print(f"   AS_URL: {AS_URL}")
    print(f"   RS_URL: {RS_URL}")
    print(f"   CLIENT_ID: {CLIENT_ID}")
    print(f"   SCOPE: {SCOPE}")
    print(f"   TEST_ACCOUNT_ID: {TEST_ACCOUNT_ID}")
    print()
    
    # Получаем OAuth токен
    access_token = get_oauth_token()
    
    if not access_token:
        print("\n❌ Не удалось получить токен. Проверьте настройки.")
        return
    
    # Тестируем получение счетов
    accounts = get_accounts(access_token)
    
    # Тестируем получение баланса
    balance = get_balance(access_token)
    
    # Пробуем создать платёжную ссылку
    result = create_payment_link(
        access_token=access_token,
        amount_rub=100.0,
        purpose="Тестовый платёж за турнир",
        ttl_minutes=60
    )
    
    if result:
        print("\n✅ Тест завершён успешно!")
    else:
        print("\n❌ Создание платёжной ссылки не удалось")
        print("\n💡 Возможные причины:")
        print("   - Метод может быть недоступен в песочнице")
        print("   - Может потребоваться дополнительная настройка в личном кабинете")
        print("   - Проверьте актуальную документацию: https://developers.tochka.com/docs/tochka-api/")


if __name__ == "__main__":
    main()

