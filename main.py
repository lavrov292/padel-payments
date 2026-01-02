from dotenv import load_dotenv
load_dotenv()
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup

import os
from fastapi import FastAPI, Body, Request
import psycopg2
from yookassa import Configuration, Payment

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None

# Configure YooKassa
shop_id = os.getenv("YOOKASSA_SHOP_ID")
secret_key = os.getenv("YOOKASSA_SECRET_KEY")
if shop_id and secret_key:
    Configuration.account_id = shop_id
    Configuration.secret_key = secret_key

app = FastAPI()

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
        
        entry_id_result, tournament_id, player_id, price_rub, tournament_title, player_name = row
        
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
            "capture": True
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
                
                update_query = """
                    UPDATE entries
                    SET payment_status = 'paid', paid_at = NOW()
                    WHERE payment_id = %s
                """
                
                cur.execute(update_query, (payment_id,))
                conn.commit()
                
                cur.close()
                conn.close()
        
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
        
        entry_id_result, tournament_id, player_id, price_rub, tournament_title, player_name = row
        
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
            "capture": True
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

        # /start
        if text.startswith("/start"):
            await bot.send_message(
                chat_id=chat_id,
                text="Привет! Я бот оплат турниров. Если тебе пришлют команду /pay <id>, я дам кнопку оплаты."
            )
            return {"ok": True}

        # /pay <entry_id>
        if text.startswith("/pay"):
            parts = text.split()
            if len(parts) < 2:
                await bot.send_message(chat_id=chat_id, text="Формат: /pay <entry_id>")
                return {"ok": True}

            entry_id = int(parts[1])

            # TODO: на следующем шаге подцепим сюда твою готовую логику YooKassa,
            # чтобы тут создавалась/возвращалась ссылка оплаты.
            await bot.send_message(
                chat_id=chat_id,
                text=f"Принял. entry_id={entry_id}. Следующий шаг — подключить генерацию payment_url."
            )
            return {"ok": True}

        return {"ok": True}

    return {"ok": True}