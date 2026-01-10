#!/usr/bin/env python3
"""
Тестирование других методов API Точка банк в песочнице
"""
import json
import requests

SANDBOX_BASE_URL = "https://enter.tochka.com/sandbox/v2"
SANDBOX_TOKEN = "sandbox.jwt.token"

headers = {
    "Authorization": f"Bearer {SANDBOX_TOKEN}",
    "Content-Type": "application/json"
}

def test_endpoint(method, endpoint, payload=None):
    """Тестирует эндпоинт"""
    url = f"{SANDBOX_BASE_URL}{endpoint}"
    
    print(f"\n🔍 {method} {endpoint}")
    
    try:
        if method == "GET":
            response = requests.get(url, headers=headers, timeout=10)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=payload, timeout=10)
        else:
            print(f"   ❌ Неподдерживаемый метод: {method}")
            return
        
        print(f"   Статус: {response.status_code}")
        
        if response.status_code in [200, 201]:
            print(f"   ✅ Успех!")
            try:
                result = response.json()
                print(f"   Ответ: {json.dumps(result, indent=2, ensure_ascii=False)[:500]}")
            except:
                print(f"   Ответ: {response.text[:500]}")
        else:
            print(f"   Ответ: {response.text[:300]}")
    
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")

def main():
    print("=" * 60)
    print("🧪 Тестирование методов API Точка банк")
    print("=" * 60)
    
    # Список методов для тестирования
    endpoints = [
        # Работа со счетами
        ("GET", "/accounts"),
        ("GET", "/accounts/list"),
        ("GET", "/v2/accounts"),
        
        # Работа с балансами
        ("GET", "/balances"),
        ("GET", "/v2/balances"),
        
        # Работа с выписками
        ("GET", "/statements"),
        ("GET", "/v2/statements"),
        
        # Работа с платежами
        ("GET", "/payments"),
        ("GET", "/v2/payments"),
        
        # Работа с клиентами
        ("GET", "/customers"),
        ("GET", "/v2/customers"),
        
        # СБП - QR коды
        ("GET", "/sbp/qr-codes"),
        ("GET", "/v2/sbp/qr-codes"),
    ]
    
    for method, endpoint in endpoints:
        test_endpoint(method, endpoint)
    
    # POST методы
    print("\n" + "=" * 60)
    print("📝 Тестирование POST методов")
    print("=" * 60)
    
    post_endpoints = [
        # Создание QR кода
        ("POST", "/sbp/qr-codes", {
            "amount": 10000,
            "purpose": "Тестовый QR код"
        }),
        ("POST", "/v2/sbp/qr-codes", {
            "amount": 10000,
            "purpose": "Тестовый QR код"
        }),
    ]
    
    for method, endpoint, payload in post_endpoints:
        test_endpoint(method, endpoint, payload)
    
    print("\n" + "=" * 60)
    print("✅ Тестирование завершено")
    print("=" * 60)
    print("\n💡 Если все методы возвращают 404/501, возможно:")
    print("   - Требуется регистрация в личном кабинете Точка банк")
    print("   - Нужно настроить доступ к API в настройках аккаунта")
    print("   - Проверьте актуальную документацию: https://developers.tochka.com/docs/tochka-api/")

if __name__ == "__main__":
    main()


