# Сравнение API Точка Банк и YooKassa

## ✅ Да, API Точка Банк позволяет создавать уникальные платежи и получать вебхуки!

### Возможности API Точка Банк:

#### 1. **Уникальные идентификаторы платежей**

При создании платёжной ссылки можно указать уникальный идентификатор в поле `Data.paymentLinkId`:

```json
{
  "data": {
    "paymentLinkId": "unique-payment-id-12345",  // Ваш уникальный ID
    "amount": 10000,
    "purpose": "Оплата турнира",
    "customerCode": "customer-123"
  }
}
```

Если не указать, система автоматически присвоит порядковый номер.

#### 2. **Вебхуки (Webhooks) для уведомлений об оплате**

API Точка Банк отправляет вебхуки при успешной оплате:

**Событие:** `acquiringInternetPayment`

**Структура вебхука:**
```json
{
  "event": "acquiringInternetPayment",
  "data": {
    "paymentLinkId": "unique-payment-id-12345",  // Ваш уникальный ID
    "status": "SUCCESS",
    "amount": 10000,
    "transactionId": "transaction-abc123",
    "paymentDate": "2024-01-09T12:00:00Z"
  }
}
```

#### 3. **Платёжные ссылки для СБП и карт**

API Точка Банк поддерживает:
- ✅ Платёжные ссылки (payment links) - для оплаты картой и/или СБП
- ✅ QR-коды СБП - для оплаты через СБП
- ✅ Оба метода можно использовать с уникальными идентификаторами

## Сравнение с YooKassa

### YooKassa (текущая реализация):

```python
# Создание платежа
payment_data = {
    "amount": {"value": "100.00", "currency": "RUB"},
    "confirmation": {"type": "redirect", "return_url": "..."},
    "description": "Tournament payment",
    "metadata": {"entry_id": entry_id}  # Уникальный идентификатор
}

payment = Payment.create(payment_data, idempotence_key)
payment_id = payment.id  # Уникальный ID от YooKassa

# Вебхук
@app.post("/webhooks/yookassa")
async def yookassa_webhook(payload: dict):
    if payload.get("event") == "payment.succeeded":
        payment_id = payload.get("object", {}).get("id")
        # Обработка оплаты
```

### Точка Банк (аналогичная реализация):

```python
# Создание платёжной ссылки
payment_data = {
    "data": {
        "paymentLinkId": f"entry-{entry_id}-{uuid.uuid4()}",  # Уникальный ID
        "amount": 10000,  # в копейках
        "purpose": "Оплата турнира",
        "customerCode": f"customer-{entry_id}"
    }
}

response = requests.post(
    f"{API_URL}/acquiring/payment-links",
    headers={"Authorization": f"Bearer {token}"},
    json=payment_data
)

payment_link_id = response.json()["data"]["paymentLinkId"]
payment_url = response.json()["data"]["paymentLink"]

# Вебхук
@app.post("/webhooks/tochka")
async def tochka_webhook(payload: dict):
    if payload.get("event") == "acquiringInternetPayment":
        payment_link_id = payload.get("data", {}).get("paymentLinkId")
        status = payload.get("data", {}).get("status")
        # Обработка оплаты
```

## Ключевые отличия

| Функция | YooKassa | Точка Банк |
|---------|----------|------------|
| **Уникальный ID** | `payment.id` (автоматически) + `metadata` | `paymentLinkId` (можно указать свой) |
| **Вебхук событие** | `payment.succeeded` | `acquiringInternetPayment` |
| **Сумма** | В рублях с копейками (`"100.00"`) | В копейках (`10000`) |
| **СБП** | ✅ Поддерживается | ✅ Поддерживается |
| **Карты** | ✅ Поддерживается | ✅ Поддерживается |
| **QR-коды СБП** | ❌ Нет | ✅ Есть отдельный метод |

## Интеграция в ваш проект

### Вариант 1: Платёжные ссылки (аналог YooKassa)

```python
def create_tochka_payment_link(entry_id: int, amount_rub: float):
    """Создаёт платёжную ссылку Точка Банк (аналог YooKassa)"""
    
    unique_id = f"entry-{entry_id}-{uuid.uuid4()}"
    
    payload = {
        "data": {
            "paymentLinkId": unique_id,
            "amount": int(amount_rub * 100),  # в копейках
            "purpose": f"Оплата турнира (entry {entry_id})",
            "customerCode": f"entry-{entry_id}",
            "ttl": 3600  # время жизни ссылки в секундах
        }
    }
    
    response = requests.post(
        f"{TOCHKA_API_URL}/acquiring/payment-links",
        headers={"Authorization": f"Bearer {TOCHKA_TOKEN}"},
        json=payload
    )
    
    result = response.json()
    payment_url = result["data"]["paymentLink"]
    
    # Сохраняем unique_id в БД для связи с entry_id
    save_payment_link_id(entry_id, unique_id, payment_url)
    
    return payment_url
```

### Вариант 2: QR-коды СБП

```python
def create_tochka_qr_code(entry_id: int, amount_rub: float, merchant_id: str, account_id: str):
    """Создаёт QR-код СБП Точка Банк"""
    
    unique_id = f"entry-{entry_id}-{uuid.uuid4()}"
    
    payload = {
        "data": {
            "payment_purpose": f"Оплата турнира (entry {entry_id})",
            "qrc_type": "02",  # динамический
            "amount": int(amount_rub * 100),  # в копейках
            # Можно добавить metadata через дополнительные поля
        }
    }
    
    response = requests.post(
        f"{TOCHKA_API_URL}/sbp/v1.0/qr-code/merchant/{merchant_id}/{account_id}",
        headers={"Authorization": f"Bearer {TOCHKA_TOKEN}"},
        json=payload
    )
    
    result = response.json()
    qrc_id = result["Data"]["qrcId"]
    qr_image = result["Data"]["image"]["content"]  # base64
    
    # Сохраняем qrc_id в БД для связи с entry_id
    save_qr_code_id(entry_id, qrc_id, unique_id)
    
    return qrc_id, qr_image
```

### Обработка вебхуков

```python
@app.post("/webhooks/tochka")
async def tochka_webhook(payload: dict = Body(...)):
    """Обработка вебхуков от Точка Банк"""
    
    event = payload.get("event")
    
    if event == "acquiringInternetPayment":
        data = payload.get("data", {})
        payment_link_id = data.get("paymentLinkId")
        status = data.get("status")
        amount = data.get("amount")
        
        if status == "SUCCESS":
            # Находим entry_id по payment_link_id
            entry_id = get_entry_id_by_payment_link_id(payment_link_id)
            
            if entry_id:
                # Обновляем статус оплаты
                update_entry_payment_status(entry_id, "paid", amount)
                
                # Логика аналогична YooKassa webhook
                handle_payment_success(entry_id, amount)
    
    elif event == "sbpPayment":  # Для QR-кодов СБП
        data = payload.get("data", {})
        qrc_id = data.get("qrcId")
        status = data.get("status")
        
        if status == "PAID":
            # Находим entry_id по qrc_id
            entry_id = get_entry_id_by_qrc_id(qrc_id)
            
            if entry_id:
                update_entry_payment_status(entry_id, "paid")
                handle_payment_success(entry_id)
    
    return {"ok": True}
```

## Выводы

✅ **API Точка Банк полностью поддерживает:**
- Создание уникальных платежей с вашими идентификаторами
- Получение вебхуков об оплате
- Отслеживание статуса платежей
- Работу с СБП и картами

✅ **Можно реализовать аналогично YooKassa:**
- Уникальный ID для каждого платежа
- Сохранение связи `entry_id` ↔ `payment_link_id` в БД
- Обработка вебхуков для автоматического обновления статуса
- Проверка статуса платежа через API

⚠️ **Отличия:**
- Сумма в копейках (не в рублях с копейками)
- Другой формат вебхуков
- Дополнительная возможность создания QR-кодов СБП

## Рекомендации

1. **Для замены YooKassa:** Используйте платёжные ссылки (`/acquiring/payment-links`)
2. **Для СБП:** Используйте QR-коды (`/sbp/v1.0/qr-code/...`)
3. **Для вебхуков:** Настройте endpoint `/webhooks/tochka` аналогично `/webhooks/yookassa`
4. **Для уникальности:** Используйте `paymentLinkId` для связи с `entry_id`


