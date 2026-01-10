#!/usr/bin/env python3
"""
Скрипт для проверки статуса оплаты QR-кода СБП
"""
import os
import json
import requests
from dotenv import load_dotenv
import sys
import time

load_dotenv()

RS_URL = "https://enter.tochka.com/sandbox/v2"
SANDBOX_TOKEN = "sandbox.jwt.token"


def get_qr_payment_status(merchant_id: str, account_id: str, qrc_id: str):
    """
    Получает статус оплаты QR-кода СБП
    """
    print(f"📊 Проверяю статус оплаты QR-кода {qrc_id}...")
    print()
    
    # Пробуем разные варианты эндпоинтов для статуса
    endpoints = [
        f"/sbp/v1.0/qr-code/merchant/{merchant_id}/{account_id}/{qrc_id}/payment-status",
        f"/sbp/v1.0/qr-code/{qrc_id}/payment-status",
        f"/sbp/v1.0/qr-code/merchant/{merchant_id}/account/{account_id}/qrc/{qrc_id}/status",
        f"/sbp/v1.0/qr-code/{qrc_id}/status",
    ]
    
    headers = {
        "Authorization": f"Bearer {SANDBOX_TOKEN}",
        "Content-Type": "application/json"
    }
    
    for endpoint in endpoints:
        url = f"{RS_URL}{endpoint}"
        print(f"🔍 Пробую: {endpoint}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            print(f"   HTTP статус: {response.status_code}")
            
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
                    status = status_data.get("status") or status_data.get("paymentStatus") or status_data.get("state")
                    amount = status_data.get("amount")
                    payment_date = status_data.get("paymentDate") or status_data.get("date") or status_data.get("paidAt")
                    transaction_id = status_data.get("transactionId") or status_data.get("id")
                    
                    print("=" * 60)
                    print("📊 ИНФОРМАЦИЯ О ПЛАТЕЖЕ:")
                    print("=" * 60)
                    if status:
                        print(f"   💳 Статус: {status}")
                    if amount:
                        amount_rub = amount / 100 if isinstance(amount, (int, float)) else amount
                        print(f"   💰 Сумма: {amount_rub:.2f} руб")
                    if payment_date:
                        print(f"   📅 Дата оплаты: {payment_date}")
                    if transaction_id:
                        print(f"   🔢 Transaction ID: {transaction_id}")
                    print("=" * 60)
                
                return result
            elif response.status_code == 404:
                print(f"   ⚠️  Эндпоинт не найден")
            else:
                print(f"   Ответ: {response.text[:300]}")
        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            continue
    
    print()
    print("❌ Не удалось получить статус")
    print("💡 Возможные причины:")
    print("   1. Эндпоинт может отличаться в документации")
    print("   2. Платёж ещё не был выполнен")
    print("   3. Проверьте документацию API")
    return None


def watch_status(merchant_id: str, account_id: str, qrc_id: str, interval: int = 5, max_attempts: int = 60):
    """
    Периодически проверяет статус оплаты (каждые interval секунд)
    """
    print(f"👀 Отслеживание статуса оплаты (каждые {interval} сек, максимум {max_attempts} попыток)")
    print(f"   Нажмите Ctrl+C для остановки")
    print()
    
    for attempt in range(1, max_attempts + 1):
        print(f"[{attempt}/{max_attempts}] Проверка статуса...")
        result = get_qr_payment_status(merchant_id, account_id, qrc_id)
        
        if result:
            status_data = result.get("Data") or result.get("data") or result
            if isinstance(status_data, dict):
                status = status_data.get("status") or status_data.get("paymentStatus") or status_data.get("state")
                # Если платёж завершён, можно остановить
                if status and status.lower() in ["paid", "completed", "success", "successful"]:
                    print("✅ Платёж успешно завершён!")
                    return result
        
        if attempt < max_attempts:
            print(f"⏳ Ожидание {interval} секунд...")
            time.sleep(interval)
            print()
    
    print("⏰ Достигнуто максимальное количество попыток")


def main():
    if len(sys.argv) < 2:
        print("Использование:")
        print("  python3 check_payment_status.py <qrc_id> [merchant_id] [account_id]")
        print("  python3 check_payment_status.py --last  # из last_qr_code.json")
        print("  python3 check_payment_status.py --watch <qrc_id> [merchant_id] [account_id]  # отслеживание")
        return
    
    if sys.argv[1] == "--last":
        try:
            with open("last_qr_code.json", "r") as f:
                qr_info = json.load(f)
            
            merchant_id = qr_info["merchantId"]
            account_id = qr_info["accountId"]
            qrc_id = qr_info["qrcId"]
        except FileNotFoundError:
            print("❌ Файл last_qr_code.json не найден")
            return
        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return
    elif sys.argv[1] == "--watch":
        if len(sys.argv) < 3:
            print("❌ Укажите qrc_id для отслеживания")
            return
        qrc_id = sys.argv[2]
        merchant_id = sys.argv[3] if len(sys.argv) > 3 else None
        account_id = sys.argv[4] if len(sys.argv) > 4 else None
        
        if not merchant_id or not account_id:
            try:
                with open("last_qr_code.json", "r") as f:
                    qr_info = json.load(f)
                merchant_id = merchant_id or qr_info["merchantId"]
                account_id = account_id or qr_info["accountId"]
            except:
                print("❌ Укажите merchant_id и account_id")
                return
        
        watch_status(merchant_id, account_id, qrc_id)
        return
    else:
        qrc_id = sys.argv[1]
        merchant_id = sys.argv[2] if len(sys.argv) > 2 else None
        account_id = sys.argv[3] if len(sys.argv) > 3 else None
        
        if not merchant_id or not account_id:
            try:
                with open("last_qr_code.json", "r") as f:
                    qr_info = json.load(f)
                merchant_id = merchant_id or qr_info["merchantId"]
                account_id = account_id or qr_info["accountId"]
            except:
                print("❌ Укажите merchant_id и account_id")
                return
    
    print("=" * 60)
    print("📊 Проверка статуса оплаты QR-кода СБП")
    print("=" * 60)
    print()
    
    get_qr_payment_status(merchant_id, account_id, qrc_id)


if __name__ == "__main__":
    main()


