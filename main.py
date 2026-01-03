from dotenv import load_dotenv
load_dotenv()
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

import os
import uuid
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Body, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
import psycopg2
from yookassa import Configuration, Payment

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")

def get_db():
    return psycopg2.connect(DATABASE_URL)
# Configure YooKassa
shop_id = os.getenv("YOOKASSA_SHOP_ID")
secret_key = os.getenv("YOOKASSA_SECRET_KEY")
if shop_id and secret_key:
    Configuration.account_id = shop_id
    Configuration.secret_key = secret_key

app = FastAPI()

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://padel-payments.vercel.app",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/db-check")
def db_check():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"db": "error", "reason": "missing DATABASE_URL"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        conn.close()
        return {"db": "ok"}
    except Exception as e:
        return {"db": "error", "reason": str(e)}

@app.get("/p/e/{entry_id}")
def payment_entry_link(entry_id: int):
    """
    Вечная ссылка на оплату entry. Проверяет статус платежа и создает новый при необходимости.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return HTMLResponse(content="<html><body>Ошибка: база данных не настроена</body></html>", status_code=500)
    
    if not shop_id or not secret_key:
        return HTMLResponse(content="<html><body>Ошибка: YooKassa не настроен</body></html>", status_code=500)
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Читаем entry + tournament + player из БД
        query = """
            SELECT 
                e.payment_status,
                e.payment_id,
                e.payment_url,
                t.price_rub,
                t.title,
                t.starts_at,
                p.full_name
            FROM entries e
            JOIN tournaments t ON e.tournament_id = t.id
            JOIN players p ON e.player_id = p.id
            WHERE e.id = %s
        """
        
        cur.execute(query, (entry_id,))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body>Запись не найдена</body></html>", status_code=404)
        
        payment_status, payment_id, payment_url, price_rub, title, starts_at, full_name = row
        
        # Если уже оплачено
        if payment_status == 'paid':
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body><h1>✅ Уже оплачено</h1></body></html>")
        
        # Если есть payment_id, проверяем статус в YooKassa
        if payment_id:
            try:
                print(f"PAYMENT CHECK: entry_id={entry_id}, payment_id={payment_id}")
                payment = Payment.find_one(payment_id)
                print(f"PAYMENT STATUS: {payment.status}")
                
                # Если платеж pending и есть confirmation_url - редирект
                if payment.status == 'pending' and payment.confirmation and payment.confirmation.confirmation_url:
                    cur.close()
                    conn.close()
                    print(f"REDIRECT: using existing payment {payment_id}")
                    return RedirectResponse(url=payment.confirmation.confirmation_url, status_code=302)
                else:
                    # Платеж не pending (succeeded/canceled/expired) - считаем невалидным
                    print(f"PAYMENT INVALID: status={payment.status}, creating new")
                    payment_id = None
            except Exception as e:
                # Платеж не найден или ошибка - считаем невалидным
                print(f"PAYMENT ERROR: {str(e)}, creating new")
                payment_id = None
        
        # Если платеж невалиден или payment_id пустой - создаем новый
        print(f"CREATE NEW PAYMENT: entry_id={entry_id}")
        
        # Calculate expires_at
        now_utc = datetime.now(timezone.utc)
        if starts_at:
            if isinstance(starts_at, datetime):
                if starts_at.tzinfo is None:
                    starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                else:
                    starts_at_utc = starts_at.astimezone(timezone.utc)
                
                if starts_at_utc > now_utc:
                    expires_at = starts_at_utc + timedelta(hours=3)
                else:
                    expires_at = now_utc + timedelta(hours=24)
            else:
                expires_at = now_utc + timedelta(hours=24)
        else:
            expires_at = now_utc + timedelta(hours=24)
        
        expires_at_str = expires_at.isoformat().replace('+00:00', 'Z')
        
        return_url = os.getenv("PAYMENT_RETURN_URL", "https://example.com/paid")
        
        # Генерируем idempotence_key для предотвращения дублей
        idempotence_key = f"entry-{entry_id}-{uuid.uuid4()}"
        
        payment_data = {
            "amount": {
                "value": f"{price_rub:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url
            },
            "description": "Tournament payment",
            "capture": True,
            "expires_at": expires_at_str
        }
        
        print(f"PAYMENT CREATE PAYLOAD: entry_id={entry_id}, payload={payment_data}")
        payment = Payment.create(payment_data, idempotence_key)
        
        new_payment_id = payment.id
        new_confirmation_url = payment.confirmation.confirmation_url
        
        print(f"PAYMENT CREATED: payment_id={new_payment_id}, confirmation_url={new_confirmation_url}")
        
        # Сохраняем payment_id и payment_url в entries
        update_query = """
            UPDATE entries
            SET payment_id = %s,
                payment_url = %s
            WHERE id = %s
        """
        
        cur.execute(update_query, (new_payment_id, new_confirmation_url, entry_id))
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"REDIRECT: using new payment {new_payment_id}")
        return RedirectResponse(url=new_confirmation_url, status_code=302)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return HTMLResponse(content=f"<html><body>Ошибка: {str(e)}</body></html>", status_code=500)

@app.get("/tournaments/{tournament_id}")
def get_tournament(tournament_id: int):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"error": "missing DATABASE_URL"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        query = """
            SELECT 
                t.id, 
                t.title, 
                t.price_rub,
                p.full_name,
                e.payment_status,
                e.confirmation_url
            FROM tournaments t
            JOIN entries e ON t.id = e.tournament_id
            JOIN players p ON e.player_id = p.id
            WHERE t.id = %s
        """
        
        cur.execute(query, (tournament_id,))
        rows = cur.fetchall()
        
        if not rows:
            cur.close()
            conn.close()
            return {"error": "tournament not found"}
        
        # Get tournament info from first row
        tournament_id_result, title, price_rub, _, _, _ = rows[0]
        
        # Build players list
        players = []
        for row in rows:
            _, _, _, full_name, payment_status, confirmation_url = row
            players.append({
                "full_name": full_name,
                "payment_status": payment_status,
                "payment_url": confirmation_url
            })
        
        cur.close()
        conn.close()
        
        return {
            "id": tournament_id_result,
            "title": title,
            "price_rub": price_rub,
            "players": players
        }
    except Exception as e:
        return {"error": str(e)}

@app.post("/entries/{entry_id}/pay")
def pay_entry(entry_id: int):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"error": "missing DATABASE_URL"}
    
    if not shop_id or not secret_key:
        return {"error": "YooKassa not configured"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Load entry, tournament, player from DB
        query = """
            SELECT 
                e.id,
                e.tournament_id,
                e.player_id,
                t.price_rub,
                t.title,
                t.starts_at,
                p.full_name
            FROM entries e
            JOIN tournaments t ON e.tournament_id = t.id
            JOIN players p ON e.player_id = p.id
            WHERE e.id = %s
        """
        
        cur.execute(query, (entry_id,))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            conn.close()
            return {"error": "entry not found"}
        
        entry_id_result, tournament_id, player_id, price_rub, tournament_title, starts_at, player_name = row
        
        # Calculate expires_at
        now_utc = datetime.now(timezone.utc)
        if starts_at:
            if isinstance(starts_at, datetime):
                if starts_at.tzinfo is None:
                    starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                else:
                    starts_at_utc = starts_at.astimezone(timezone.utc)
                
                if starts_at_utc > now_utc:
                    expires_at = starts_at_utc + timedelta(hours=3)
                else:
                    expires_at = now_utc + timedelta(hours=24)
            else:
                expires_at = now_utc + timedelta(hours=24)
        else:
            expires_at = now_utc + timedelta(hours=24)
        
        expires_at_str = expires_at.isoformat().replace('+00:00', 'Z')
        
        # Get return URL from env or use default
        return_url = os.getenv("PAYMENT_RETURN_URL", "https://example.com/paid")
        
        # Create YooKassa payment
        payment_data = {
            "amount": {
                "value": f"{price_rub:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url
            },
            "description": "Tournament payment",
            "capture": True,
            "expires_at": expires_at_str
        }
        
        payment = Payment.create(payment_data)
        
        payment_id = payment.id
        confirmation_url = payment.confirmation.confirmation_url
        
        # Save payment_id and confirmation_url into entries table
        update_query = """
            UPDATE entries
            SET payment_id = %s, confirmation_url = %s
            WHERE id = %s
        """
        
        cur.execute(update_query, (payment_id, confirmation_url, entry_id))
        conn.commit()
        
        cur.close()
        conn.close()
        
        return {"payment_url": confirmation_url}
    except Exception as e:
        return {"error": str(e)}

def ensure_payment_url_for_entry(entry_id: int) -> str:
    """Ensure payment URL exists for entry, create if needed. Returns confirmation_url."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    
    if not shop_id or not secret_key:
        raise Exception("YOOKASSA_SHOP_ID / YOOKASSA_SECRET_KEY not set")

    conn = psycopg2.connect(database_url, sslmode="require")
    try:
        with conn.cursor() as cur:
            # 1) если ссылка уже есть — вернуть
            cur.execute("""
                select e.confirmation_url, t.price_rub, t.starts_at
                from entries e
                join tournaments t on t.id = e.tournament_id
                where e.id = %s
            """, (entry_id,))
            row = cur.fetchone()
            if not row:
                raise Exception(f"entry {entry_id} not found")

            confirmation_url, price_rub, starts_at = row
            if confirmation_url:
                return confirmation_url

            # 2) создать платеж в YooKassa
            # Calculate expires_at
            now_utc = datetime.now(timezone.utc)
            if starts_at:
                if isinstance(starts_at, datetime):
                    if starts_at.tzinfo is None:
                        starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                    else:
                        starts_at_utc = starts_at.astimezone(timezone.utc)
                    
                    if starts_at_utc > now_utc:
                        expires_at = starts_at_utc + timedelta(hours=3)
                    else:
                        expires_at = now_utc + timedelta(hours=24)
                else:
                    expires_at = now_utc + timedelta(hours=24)
            else:
                expires_at = now_utc + timedelta(hours=24)
            
            expires_at_str = expires_at.isoformat().replace('+00:00', 'Z')
            
            return_url = os.getenv("PAYMENT_RETURN_URL") or "https://example.com/paid"

            payment = Payment.create({
                "amount": {"value": f"{price_rub:.2f}", "currency": "RUB"},
                "confirmation": {"type": "redirect", "return_url": return_url},
                "capture": True,
                "description": "Tournament payment",
                "expires_at": expires_at_str
            })

            payment_id = payment.id
            new_url = payment.confirmation.confirmation_url

            # 3) сохранить в БД
            cur.execute("""
                update entries
                set payment_id=%s,
                    confirmation_url=%s
                where id=%s
            """, (payment_id, new_url, entry_id))
            conn.commit()

            return new_url
    finally:
        conn.close()


def save_player_telegram_id_for_entry(entry_id: int, telegram_user_id: int) -> None:
    """Save Telegram user ID for the player associated with the entry."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    
    conn = psycopg2.connect(database_url, sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                update players p
                set telegram_id = %s
                from entries e
                where e.player_id = p.id and e.id = %s
            """, (telegram_user_id, entry_id))
            conn.commit()
    finally:
        conn.close()

@app.post("/webhooks/yookassa")
async def yookassa_webhook(payload: dict = Body(...)):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"ok": False, "error": "missing DATABASE_URL"}
    
    try:
        if payload.get("event") == "payment.succeeded":
            payment_id = payload.get("object", {}).get("id")
            
            if payment_id:
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Update payment status
                update_query = """
                    UPDATE entries
                    SET payment_status = 'paid', paid_at = NOW()
                    WHERE payment_id = %s
                """
                
                cur.execute(update_query, (payment_id,))
                conn.commit()
                
                # Fetch player's telegram_id and tournament info
                fetch_query = """
                    SELECT 
                        p.telegram_id,
                        t.title,
                        t.starts_at,
                        t.price_rub
                    FROM entries e
                    JOIN players p ON e.player_id = p.id
                    JOIN tournaments t ON e.tournament_id = t.id
                    WHERE e.payment_id = %s
                """
                
                cur.execute(fetch_query, (payment_id,))
                row = cur.fetchone()
                
                cur.close()
                conn.close()
                
                # Send Telegram notification if telegram_id exists and bot is available
                if row and bot is not None:
                    telegram_id, tournament_title, starts_at, price_rub = row
                    if telegram_id:
                        try:
                            # Format starts_at if it exists
                            starts_at_str = starts_at.strftime("%Y-%m-%d %H:%M") if starts_at else "Не указано"
                            
                            message = f"""✅ Оплата получена!

Турнир: {tournament_title}
Время: {starts_at_str}
Сумма: {price_rub} ₽"""
                            
                            await bot.send_message(chat_id=telegram_id, text=message)
                        except Exception as telegram_error:
                            # Log error but don't fail the webhook
                            pass
        
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/admin/tournaments")
def get_admin_tournaments():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"error": "missing DATABASE_URL"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        query = """
            SELECT id, title, starts_at, price_rub
            FROM tournaments
            ORDER BY starts_at
        """
        
        cur.execute(query)
        rows = cur.fetchall()
        
        tournaments = []
        for row in rows:
            tournament_id, title, starts_at, price_rub = row
            tournaments.append({
                "id": tournament_id,
                "title": title,
                "starts_at": starts_at.isoformat() if starts_at else None,
                "price_rub": price_rub
            })
        
        cur.close()
        conn.close()
        
        return tournaments
    except Exception as e:
        return {"error": str(e)}

@app.post("/admin/entries/{entry_id}/mark-manual-paid")
async def mark_manual_paid(entry_id: int, body: dict = Body(...)):
    """
    Отмечает entry как оплаченное вручную.
    Body: { "note": "cash" } (note опционально)
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"ok": False, "error": "missing DATABASE_URL"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Получаем payment_id и payment_status
        cur.execute("""
            SELECT payment_id, payment_status
            FROM entries
            WHERE id = %s
        """, (entry_id,))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            conn.close()
            return {"ok": False, "error": "entry not found"}
        
        payment_id, payment_status = row
        
        # Если есть payment_id и payment_status='pending', отменяем платеж в YooKassa
        if payment_id and payment_status == 'pending':
            try:
                Payment.cancel(payment_id)
                print(f"Payment {payment_id} cancelled successfully")
            except Exception as cancel_error:
                # Если cancel не удался, логируем предупреждение, но продолжаем
                print(f"WARNING: Failed to cancel payment {payment_id}: {str(cancel_error)}")
        
        note = body.get("note")
        
        # Обновляем entry: помечаем как paid вручную и обнуляем payment_url и payment_id
        update_query = """
            UPDATE entries
            SET payment_status = 'paid',
                manual_paid = true,
                manual_note = %s,
                payment_url = NULL,
                payment_id = NULL
            WHERE id = %s
        """
        
        cur.execute(update_query, (note, entry_id))
        conn.commit()
        
        cur.close()
        conn.close()
        
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/admin/entries/{entry_id}/ensure-payment")
def ensure_entry_payment(entry_id: int):
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"error": "missing DATABASE_URL"}
    
    if not shop_id or not secret_key:
        return {"error": "YooKassa not configured"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Check if entry has confirmation_url
        check_query = """
            SELECT confirmation_url
            FROM entries
            WHERE id = %s
        """
        
        cur.execute(check_query, (entry_id,))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            conn.close()
            return {"error": "entry not found"}
        
        confirmation_url = row[0]
        
        # If confirmation_url exists, return it
        if confirmation_url:
            cur.close()
            conn.close()
            return {"payment_url": confirmation_url}
        
        # Otherwise, create payment (same as /entries/{id}/pay)
        # Load entry, tournament, player from DB
        query = """
            SELECT 
                e.id,
                e.tournament_id,
                e.player_id,
                t.price_rub,
                t.title,
                t.starts_at,
                p.full_name
            FROM entries e
            JOIN tournaments t ON e.tournament_id = t.id
            JOIN players p ON e.player_id = p.id
            WHERE e.id = %s
        """
        
        cur.execute(query, (entry_id,))
        row = cur.fetchone()
        
        if not row:
            cur.close()
            conn.close()
            return {"error": "entry not found"}
        
        entry_id_result, tournament_id, player_id, price_rub, tournament_title, starts_at, player_name = row
        
        # Calculate expires_at
        now_utc = datetime.now(timezone.utc)
        if starts_at:
            if isinstance(starts_at, datetime):
                if starts_at.tzinfo is None:
                    starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                else:
                    starts_at_utc = starts_at.astimezone(timezone.utc)
                
                if starts_at_utc > now_utc:
                    expires_at = starts_at_utc + timedelta(hours=3)
                else:
                    expires_at = now_utc + timedelta(hours=24)
            else:
                expires_at = now_utc + timedelta(hours=24)
        else:
            expires_at = now_utc + timedelta(hours=24)
        
        expires_at_str = expires_at.isoformat().replace('+00:00', 'Z')
        
        # Get return URL from env or use default
        return_url = os.getenv("PAYMENT_RETURN_URL", "https://example.com/paid")
        
        # Create YooKassa payment
        payment_data = {
            "amount": {
                "value": f"{price_rub:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url
            },
            "description": "Tournament payment",
            "capture": True,
            "expires_at": expires_at_str
        }
        
        payment = Payment.create(payment_data)
        
        payment_id = payment.id
        confirmation_url_new = payment.confirmation.confirmation_url
        
        # Save payment_id and confirmation_url into entries table
        update_query = """
            UPDATE entries
            SET payment_id = %s, confirmation_url = %s
            WHERE id = %s
        """
        
        cur.execute(update_query, (payment_id, confirmation_url_new, entry_id))
        conn.commit()
        
        cur.close()
        conn.close()
        
        return {"payment_url": confirmation_url_new}
    except Exception as e:
        return {"error": str(e)}

@app.post("/webhooks/telegram")
async def telegram_webhook(request: Request):
    if bot is None:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN is missing"}

    payload = await request.json()

    # 1) Сообщения
    message = payload.get("message")
    if message:
        text = (message.get("text") or "").strip()
        chat_id = message["chat"]["id"]
        from_user = message.get("from")

        # /start
        if text.startswith("/start"):
            # Get telegram_user_id
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = from_user["id"]
            
            if not telegram_user_id:
                await bot.send_message(chat_id=chat_id, text="Ошибка: не удалось определить ваш Telegram ID.")
                return {"ok": True}
            
            # Create reply keyboard (always show)
            keyboard = ReplyKeyboardMarkup(
                [
                    [KeyboardButton("Мои турниры"), KeyboardButton("Помощь")]
                ],
                resize_keyboard=True
            )
            
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.", reply_markup=keyboard)
                return {"ok": True}
            
            try:
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Check if player exists with this telegram_id
                cur.execute("SELECT full_name FROM players WHERE telegram_id = %s", (telegram_user_id,))
                row = cur.fetchone()
                
                if row:
                    # Player exists, greet them
                    player_name = row[0]
                    welcome_text = f"Привет, {player_name}!"
                    await bot.send_message(
                        chat_id=chat_id,
                        text=welcome_text,
                        reply_markup=keyboard
                    )
                else:
                    # Player not found, create session and ask for Lunda name
                    cur.execute("""
                        INSERT INTO telegram_sessions (telegram_id, state, temp_name)
                        VALUES (%s, 'awaiting_lunda_name', NULL)
                        ON CONFLICT (telegram_id) 
                        DO UPDATE SET state = 'awaiting_lunda_name', temp_name = NULL
                    """, (telegram_user_id,))
                    conn.commit()
                    
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Напиши, как ты называешься в Lunda (слово в слово). Например: Иван Иванов",
                        reply_markup=keyboard
                    )
                
                cur.close()
                conn.close()
            except Exception as e:
                await bot.send_message(chat_id=chat_id, text=f"Ошибка: {str(e)}", reply_markup=keyboard)
            
            return {"ok": True}
        
        # /whoami command
        if text.startswith("/whoami"):
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = from_user["id"]
            
            if telegram_user_id:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ваш Telegram ID: {telegram_user_id}"
                )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text="Не удалось определить ваш Telegram ID."
                )
            return {"ok": True}

        # Handle text messages when session state is "awaiting_lunda_name"
        # Skip if it's a known button or command
        if text not in ["Мои турниры", "Помощь"] and not text.startswith("/"):
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = from_user["id"]
            
            if telegram_user_id:
                database_url = os.getenv("DATABASE_URL")
                if database_url:
                    try:
                        conn = psycopg2.connect(database_url, sslmode="require")
                        cur = conn.cursor()
                        
                        # Check if there's an active session with awaiting_lunda_name state
                        cur.execute("""
                            SELECT state, temp_name 
                            FROM telegram_sessions 
                            WHERE telegram_id = %s AND state = 'awaiting_lunda_name'
                        """, (telegram_user_id,))
                        session_row = cur.fetchone()
                        
                        if session_row:
                            # User is in awaiting_lunda_name state, process the name
                            provided_name = text.strip()
                            
                            # Store name in temp_name
                            cur.execute("""
                                UPDATE telegram_sessions 
                                SET temp_name = %s 
                                WHERE telegram_id = %s
                            """, (provided_name, telegram_user_id))
                            conn.commit()
                            
                            # Try to find player by name (case-insensitive)
                            # Only consider players where telegram_id is null or empty
                            cur.execute("""
                                SELECT id, full_name, lunda_name 
                                FROM players 
                                WHERE (full_name ILIKE %s OR lunda_name ILIKE %s)
                                  AND (telegram_id IS NULL OR telegram_id = '')
                            """, (provided_name, provided_name))
                            matches = cur.fetchall()
                            
                            if len(matches) == 1:
                                # Exactly one match - link the player
                                player_id = matches[0][0]
                                cur.execute("""
                                    UPDATE players 
                                    SET telegram_id = %s, telegram_verified_at = NOW() 
                                    WHERE id = %s
                                """, (telegram_user_id, player_id))
                                
                                # Delete session
                                cur.execute("DELETE FROM telegram_sessions WHERE telegram_id = %s", (telegram_user_id,))
                                conn.commit()
                                
                                cur.close()
                                conn.close()
                                
                                await bot.send_message(
                                    chat_id=chat_id,
                                    text="✅ Готово! Теперь нажми «Мои турниры»."
                                )
                                return {"ok": True}
                            else:
                                # 0 or >1 matches - need manual linking
                                cur.execute("""
                                    UPDATE telegram_sessions 
                                    SET state = 'needs_manual_link' 
                                    WHERE telegram_id = %s
                                """, (telegram_user_id,))
                                conn.commit()
                                
                                # Get username if available
                                username = from_user.get("username")
                                username_str = f"@{username}" if username else "не указан"
                                
                                # Notify admin
                                admin_chat_id = os.getenv("ADMIN_CHAT_ID")
                                if admin_chat_id and bot:
                                    admin_message = f"""Требуется ручная привязка:

Telegram ID: {telegram_user_id}
Username: {username_str}
Указанное имя: {provided_name}
Найдено совпадений: {len(matches)}

Пожалуйста, свяжите вручную."""
                                    try:
                                        await bot.send_message(chat_id=admin_chat_id, text=admin_message)
                                    except Exception:
                                        pass  # Ignore errors sending to admin
                                
                                cur.close()
                                conn.close()
                                
                                await bot.send_message(
                                    chat_id=chat_id,
                                    text="Я не смог автоматически привязать. Я написал организатору, он свяжет вручную."
                                )
                                return {"ok": True}
                        
                        cur.close()
                        conn.close()
                    except Exception:
                        # Ignore errors
                        pass

        # "Мои турниры" button
        if text == "Мои турниры":
            # Get telegram_user_id
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = from_user["id"]
            
            if not telegram_user_id:
                await bot.send_message(
                    chat_id=chat_id,
                    text="Ошибка: не удалось определить ваш Telegram ID."
                )
                return {"ok": True}
            
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                await bot.send_message(
                    chat_id=chat_id,
                    text="Ошибка: база данных не настроена."
                )
                return {"ok": True}
            
            try:
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Find player by telegram_id
                cur.execute("SELECT id FROM players WHERE telegram_id = %s", (telegram_user_id,))
                player_row = cur.fetchone()
                
                if not player_row:
                    cur.close()
                    conn.close()
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Я не нашёл тебя в базе. Напиши организатору, чтобы он добавил твой Telegram ID."
                    )
                    return {"ok": True}
                
                player_id = player_row[0]
                
                # Query future tournaments
                query = """
                    SELECT 
                        e.id as entry_id,
                        t.title,
                        t.starts_at,
                        t.location,
                        t.price_rub,
                        e.payment_status
                    FROM entries e
                    JOIN tournaments t ON e.tournament_id = t.id
                    WHERE e.player_id = %s 
                      AND t.starts_at >= NOW()
                    ORDER BY t.starts_at
                """
                
                cur.execute(query, (player_id,))
                rows = cur.fetchall()
                
                cur.close()
                conn.close()
                
                if not rows:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="У тебя нет предстоящих турниров."
                    )
                    return {"ok": True}
                
                # Send message for each entry
                for row in rows:
                    entry_id, title, starts_at, location, price_rub, payment_status = row
                    
                    # Format starts_at
                    starts_at_str = starts_at.strftime("%d.%m.%Y %H:%M") if starts_at else "Не указано"
                    
                    # Format location
                    location_str = location if location else "Не указано"
                    
                    # Format payment status
                    status_emoji = "✅" if payment_status == "paid" else "⏳"
                    status_text = "Оплачено" if payment_status == "paid" else "Не оплачено"
                    
                    # Build message
                    message = f"""<b>{title}</b>

📅 Время: {starts_at_str}
📍 Место: {location_str}
💰 Сумма: {price_rub} ₽
{status_emoji} Статус: {status_text}"""
                    
                    # Create inline keyboard if not paid
                    keyboard = None
                    if payment_status != 'paid':
                        try:
                            # Используем вечную ссылку на наш сервис
                            payment_link = f"{API_BASE_URL}/p/e/{entry_id}"
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Оплатить", url=payment_link),
                                    InlineKeyboardButton("Получить ссылку", callback_data=f"get_link:{entry_id}")
                                ]
                            ])
                        except Exception as e:
                            # If payment URL creation fails, send message without buttons
                            pass
                    
                    await bot.send_message(
                        chat_id=chat_id,
                        text=message,
                        parse_mode="HTML",
                        reply_markup=keyboard
                    )
                
                return {"ok": True}
            except Exception as e:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ошибка при получении турниров: {str(e)}"
                )
                return {"ok": True}

        # /pay <entry_id>
        if text.startswith("/pay"):
            parts = text.split()
            if len(parts) < 2:
                await bot.send_message(chat_id=chat_id, text="Формат: /pay <entry_id>")
                return {"ok": True}

            try:
                entry_id = int(parts[1])
                
                # Parse telegram_user_id
                telegram_user_id = None
                if from_user and from_user.get("id"):
                    telegram_user_id = from_user["id"]
                    # Store Telegram user id
                    save_player_telegram_id_for_entry(entry_id, telegram_user_id)
                
                # Используем вечную ссылку на наш сервис
                payment_link = f"{API_BASE_URL}/p/e/{entry_id}"
                
                # Create inline keyboard
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("Оплатить", url=payment_link),
                        InlineKeyboardButton("Получить ссылку", callback_data=f"get_link:{entry_id}")
                    ]
                ])
                
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ссылка на оплату для entry_id={entry_id}:",
                    reply_markup=keyboard
                )
                return {"ok": True}
            except ValueError as e:
                await bot.send_message(chat_id=chat_id, text=f"Ошибка: {str(e)}")
                return {"ok": True}
            except Exception as e:
                await bot.send_message(chat_id=chat_id, text=f"Ошибка при создании платежа: {str(e)}")
                return {"ok": True}

        return {"ok": True}

    # 2) Callback queries
    callback_query = payload.get("callback_query")
    if callback_query:
        data = callback_query.get("data", "")
        chat_id = callback_query["message"]["chat"]["id"]
        message_id = callback_query["message"]["message_id"]
        
        if data.startswith("get_link:"):
            try:
                entry_id = int(data.split(":")[1])
                # Используем вечную ссылку на наш сервис
                payment_link = f"{API_BASE_URL}/p/e/{entry_id}"
                
                # Answer callback query first
                await bot.answer_callback_query(callback_query["id"])
                
                # Send plain text message with the link and instruction how to copy
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ссылка на оплату:\n\n{payment_link}\n\nЧтобы скопировать ссылку, нажмите на неё и удерживайте, затем выберите \"Копировать\"."
                )
                return {"ok": True}
            except ValueError as e:
                await bot.answer_callback_query(callback_query["id"], text=f"Ошибка: {str(e)}")
                return {"ok": True}
            except Exception as e:
                await bot.answer_callback_query(callback_query["id"], text=f"Ошибка: {str(e)}")
                return {"ok": True}

    return {"ok": True}

    from fastapi import Query
from datetime import datetime

@app.post("/admin/process-new-entries")
async def process_new_entries(limit: int = Query(50, ge=1, le=500)):
    """
    Находит entries, которым нужно создать ссылку оплаты, и создает платежи.
    Если у игрока есть telegram_id — отправляет сообщение.
    limit — защита от массовых ошибочных созданий.
    """
    conn = get_db()
    cur = conn.cursor()

    # Выбираем entries, которым нужно создать ссылку
    cur.execute("""
        select
          e.id as entry_id,
          t.title,
          t.starts_at,
          t.price_rub,
          p.full_name,
          p.telegram_id
        from entries e
        join tournaments t on t.id = e.tournament_id
        join players p on p.id = e.player_id
        where e.payment_status = 'pending'
          and e.payment_url IS NULL
          and coalesce(e.manual_paid, false) = false
          and (t.starts_at IS NULL OR t.starts_at > NOW() - INTERVAL '3 hours')
        order by e.created_at asc
        limit %s
    """, (limit,))
    rows = cur.fetchall()

    processed = 0
    notified = 0

    for (entry_id, title, starts_at, price_rub, full_name, telegram_id) in rows:
        print("PROCESS ENTRY", entry_id)
        
        # Вычисляем expires_at
        now_utc = datetime.now(timezone.utc)
        if starts_at:
            # Конвертируем starts_at в UTC datetime
            if isinstance(starts_at, datetime):
                if starts_at.tzinfo is None:
                    starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                else:
                    starts_at_utc = starts_at.astimezone(timezone.utc)
                
                # Если starts_at в будущем: expires_at = starts_at + 3 часа
                if starts_at_utc > now_utc:
                    expires_at = starts_at_utc + timedelta(hours=3)
                else:
                    # Если starts_at в прошлом: expires_at = now + 24 часа
                    expires_at = now_utc + timedelta(hours=24)
            else:
                # Если starts_at не datetime, используем now + 24 часа
                expires_at = now_utc + timedelta(hours=24)
        else:
            # Если starts_at NULL: expires_at = now + 24 часа
            expires_at = now_utc + timedelta(hours=24)
        
        # Преобразуем в ISO8601 UTC строку
        expires_at_str = expires_at.isoformat().replace('+00:00', 'Z')
        
        # создаем платеж
        payment = Payment.create({
            "amount": {"value": f"{float(price_rub):.2f}", "currency": "RUB"},
            "confirmation": {
                "type": "redirect",
                "return_url": "https://example.com/paid"
            },
            "capture": True,
            "description": f"Padel tournament: {title}",
            "metadata": {"entry_id": str(entry_id), "player": full_name},
            "expires_at": expires_at_str
        })

        payment_url = payment.confirmation.confirmation_url
        payment_id_new = payment.id

        # Записываем payment_id и payment_url в entries
        cur.execute("""
            update entries
            set payment_id = %s,
                payment_url = %s
            where id = %s
        """, (payment_id_new, payment_url, entry_id))
        conn.commit()
        processed += 1

        # уведомление в телеграм (если есть telegram_id)
        if telegram_id and bot is not None:
            try:
                chat_id = int(telegram_id)
                print("TG SEND", telegram_id)

                msg = (
                    "🎾 Ты записан на турнир!\n\n"
                    f"🏷️ {title}\n"
                    f"🕒 {starts_at}\n"
                    f"💳 {price_rub} ₽\n\n"
                    "Оплата по ссылке:"
                )

                # Вызываем асинхронную функцию
                await bot.send_message(chat_id=chat_id, text=msg)
                await bot.send_message(chat_id=chat_id, text=payment_url)

                # Обновляем telegram_notified после успешной отправки
                cur.execute("""
                    update entries
                    set telegram_notified = true,
                        telegram_notified_at = now()
                    where id = %s
                """, (entry_id,))
                conn.commit()

                print("TG OK", telegram_id)
                notified += 1
            except Exception as e:
                print("TG ERROR", str(e))

    cur.close()
    conn.close()

    return {"ok": True, "processed": processed, "notified": notified}