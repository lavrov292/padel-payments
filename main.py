from dotenv import load_dotenv
load_dotenv()
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton

import os
import uuid
import traceback
import json
from datetime import datetime, timedelta, timezone, date
import pytz
from fastapi import FastAPI, Body, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
import psycopg2
from yookassa import Configuration, Payment

DATABASE_URL = os.getenv("DATABASE_URL")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
bot = Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:8000")
BOT_TZ = pytz.timezone(os.getenv("BOT_TZ", "Europe/Moscow"))

def get_db():
    return psycopg2.connect(DATABASE_URL)

# SQL миграция для Supabase (если таблицы telegram_sessions нет или нет поля support_mode):
# 
# ALTER TABLE telegram_sessions ADD COLUMN IF NOT EXISTS support_mode BOOLEAN NOT NULL DEFAULT false;
# ALTER TABLE telegram_sessions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
#
# Или если таблицы нет вообще:
# CREATE TABLE IF NOT EXISTS telegram_sessions (
#     telegram_id TEXT PRIMARY KEY,
#     state TEXT,
#     temp_name TEXT,
#     support_mode BOOLEAN NOT NULL DEFAULT false,
#     updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
# );

def get_db_conn():
    """Get database connection."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    return psycopg2.connect(database_url, sslmode="require")

def tg_id_str(from_user):
    """Extract telegram_id from from_user and convert to string."""
    if from_user and from_user.get("id"):
        return str(from_user["id"])
    return None

def fmt_date_ru(date_input) -> tuple[str, str]:
    """
    Format date for Russian locale display and ISO callback.
    Returns: (display_text: str, iso_date: str)
    - display_text: DD.MM.YYYY format for button text
    - iso_date: YYYY-MM-DD format for callback_data
    """
    if isinstance(date_input, (datetime, date)):
        display_text = date_input.strftime("%d.%m.%Y")
        iso_date = date_input.strftime("%Y-%m-%d")
        return (display_text, iso_date)
    elif isinstance(date_input, str):
        # Try to parse string date
        try:
            from dateutil import parser
            date_parsed = parser.parse(date_input)
            display_text = date_parsed.strftime("%d.%m.%Y")
            iso_date = date_parsed.strftime("%Y-%m-%d")
            return (display_text, iso_date)
        except:
            # If parsing fails, assume it's already in ISO format
            try:
                # Try to parse YYYY-MM-DD
                if len(date_input) == 10 and date_input.count('-') == 2:
                    parts = date_input.split('-')
                    if len(parts) == 3:
                        display_text = f"{parts[2]}.{parts[1]}.{parts[0]}"
                        iso_date = date_input
                        return (display_text, iso_date)
            except:
                pass
            # Fallback: return as-is
            return (str(date_input), str(date_input))
    else:
        # For other types, try to convert to string and parse
        try:
            from dateutil import parser
            date_parsed = parser.parse(str(date_input))
            display_text = date_parsed.strftime("%d.%m.%Y")
            iso_date = date_parsed.strftime("%Y-%m-%d")
            return (display_text, iso_date)
        except:
            return (str(date_input), str(date_input))

def set_support_mode(telegram_id, enabled):
    """Set support_mode for telegram_id."""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO telegram_sessions (telegram_id, support_mode, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (telegram_id)
            DO UPDATE SET support_mode = %s, updated_at = NOW()
        """, (telegram_id, enabled, enabled))
        conn.commit()
    finally:
        conn.close()

def get_support_mode(telegram_id):
    """Get support_mode for telegram_id. Returns False if not found."""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT support_mode FROM telegram_sessions
            WHERE telegram_id = %s
        """, (telegram_id,))
        row = cur.fetchone()
        return row[0] if row else False
    finally:
        conn.close()

def get_player_by_tg(telegram_id):
    """Get player by telegram_id. Returns (id, full_name) or None."""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, full_name FROM players
            WHERE telegram_id = %s
        """, (telegram_id,))
        row = cur.fetchone()
        return row if row else None
    finally:
        conn.close()

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
def payment_entry_link(
    entry_id: int, 
    pay: str = Query("default", description="Payment mode: 'half' for 50%, 'full' for 100%, 'default' for auto"),
    request: Request = None
):
    """
    Вечная ссылка на оплату entry. Проверяет статус платежа и создает новый при необходимости.
    Query param 'pay': 'half' (50%), 'full' (100%), 'default' (auto based on tournament_type)
    Query param 'partner_entry_id': ID партнёра для оплаты за пару (только при pay=full)
    """
    # Читаем partner_entry_id из query параметров
    partner_entry_id = None
    if request:
        partner_entry_id = request.query_params.get("partner_entry_id")
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return HTMLResponse(content="<html><body>Ошибка: база данных не настроена</body></html>", status_code=500)
    
    if not shop_id or not secret_key:
        return HTMLResponse(content="<html><body>Ошибка: YooKassa не настроен</body></html>", status_code=500)
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Читаем entry + tournament + player из БД (включая payment_scope и paid_for_entry_id)
        query = """
            SELECT 
                e.payment_status,
                e.payment_id,
                e.payment_url,
                e.payment_scope,
                e.paid_for_entry_id,
                t.price_rub,
                t.title,
                t.starts_at,
                t.tournament_type,
                p.full_name,
                e.tournament_id
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
        
        payment_status, payment_id, payment_url, existing_payment_scope, existing_paid_for_entry_id, price_rub, title, starts_at, tournament_type, full_name, tournament_id = row
        
        # Если уже оплачено
        if payment_status == 'paid':
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body><h1>✅ Уже оплачено</h1></body></html>")
        
        # Валидация partner_entry_id из query параметров (делаем ДО проверки существующего платежа)
        partner_entry_id_int = None
        partner_entry = None
        
        if partner_entry_id is not None:
            try:
                partner_entry_id_int = int(partner_entry_id)
            except (ValueError, TypeError):
                cur.close()
                conn.close()
                return HTMLResponse(content="<html><body>Ошибка: Некорректный partner_entry_id</body></html>", status_code=400)
            
            # Проверка, что partner_entry_id != entry_id
            if partner_entry_id_int == entry_id:
                cur.close()
                conn.close()
                return HTMLResponse(content="<html><body>Ошибка: нельзя оплатить за самого себя</body></html>", status_code=400)
            
            # Получаем entry партнёра из БД
            cur.execute("""
                SELECT id, payment_status, tournament_id
                FROM entries
                WHERE id = %s
            """, (partner_entry_id_int,))
            partner_row = cur.fetchone()
            
            if not partner_row:
                cur.close()
                conn.close()
                return HTMLResponse(content="<html><body>Ошибка: запись партнёра не найдена</body></html>", status_code=404)
            
            partner_id, partner_status, partner_tournament_id = partner_row
            
            # Проверка, что оба entry относятся к одному турниру
            if partner_tournament_id != tournament_id:
                cur.close()
                conn.close()
                return HTMLResponse(content="<html><body>Ошибка: записи относятся к разным турнирам</body></html>", status_code=400)
            
            # Проверка, что партнёр ещё не оплатил
            if partner_status == 'paid':
                cur.close()
                conn.close()
                return HTMLResponse(content="<html><body>Ошибка: партнёр уже оплатил</body></html>", status_code=400)
            
            # Всё ок - сохраняем информацию о партнёре
            partner_entry = {
                "id": partner_id,
                "payment_status": partner_status,
                "tournament_id": partner_tournament_id
            }
        
        # Рассчитываем желаемый контекст оплаты (desired_scope и amount)
        desired_scope = 'self'
        desired_paid_for_entry_id = None
        desired_amount = None
        
        if tournament_type == 'team' and pay == 'full' and partner_entry_id_int is not None and partner_entry is not None:
            # Pair payment: 100% (full pair payment)
            desired_scope = 'pair'
            desired_paid_for_entry_id = partner_entry_id_int
            desired_amount = float(price_rub)
        elif tournament_type == 'team':
            # Team tournament: 50% (single person payment)
            desired_scope = 'self'
            desired_paid_for_entry_id = None
            desired_amount = float(price_rub) / 2
        else:
            # Personal tournament: always 100%
            desired_scope = 'self'
            desired_paid_for_entry_id = None
            desired_amount = float(price_rub)
        
        # Проверяем существующий payment_id
        can_reuse_payment = False
        if payment_id:
            try:
                print(f"PAYMENT CHECK: entry_id={entry_id}, payment_id={payment_id}")
                payment = Payment.find_one(payment_id)
                print(f"PAYMENT STATUS: {payment.status}")
                
                # Если платеж pending - проверяем соответствие желаемому контексту
                if payment.status == 'pending' and payment.confirmation and payment.confirmation.confirmation_url:
                    # Проверяем соответствие scope
                    scope_match = (existing_payment_scope == desired_scope)
                    
                    # Если desired_scope == 'pair' - проверяем paid_for_entry_id
                    paid_for_match = True
                    if desired_scope == 'pair':
                        paid_for_match = (existing_paid_for_entry_id == desired_paid_for_entry_id)
                    
                    can_reuse_payment = scope_match and paid_for_match
                    
                    print("PAY REUSE CHECK", {
                        "entry_id": entry_id,
                        "existing_payment_id": payment_id,
                        "existing_status": payment.status,
                        "existing_scope": existing_payment_scope,
                        "existing_paid_for": existing_paid_for_entry_id,
                        "desired_scope": desired_scope,
                        "partner_entry_id": partner_entry_id_int,
                        "desired_paid_for": desired_paid_for_entry_id,
                        "amount": desired_amount,
                        "reuse": can_reuse_payment
                    })
                    
                    if can_reuse_payment:
                        cur.close()
                        conn.close()
                        print(f"REDIRECT: using existing payment {payment_id}")
                        return RedirectResponse(url=payment.confirmation.confirmation_url, status_code=302)
                    else:
                        # Не соответствует - очищаем старые поля и создадим новый
                        print(f"PAYMENT MISMATCH: existing scope={existing_payment_scope}, desired scope={desired_scope}, clearing old payment")
                        cur.execute("""
                            UPDATE entries
                            SET payment_id = NULL,
                                payment_url = NULL,
                                payment_status = 'pending',
                                paid_for_entry_id = NULL,
                                payment_scope = 'self'
                            WHERE id = %s
                        """, (entry_id,))
                        conn.commit()
                        payment_id = None
                else:
                    # Платеж не pending (succeeded/canceled/expired) - считаем невалидным
                    print(f"PAYMENT INVALID: status={payment.status}, creating new")
                    payment_id = None
            except Exception as e:
                # Платеж не найден или ошибка - считаем невалидным
                print(f"PAYMENT ERROR: {str(e)}, creating new")
                payment_id = None
        
        # Если платеж невалиден или payment_id пустой - создаем новый
        print(f"CREATE NEW PAYMENT: entry_id={entry_id}, tournament_type={tournament_type}, pay={pay}, partner_entry_id={partner_entry_id_int}, desired_scope={desired_scope}")
        
        # Используем рассчитанные значения
        payment_scope = desired_scope
        paid_for_entry_id_to_save = desired_paid_for_entry_id
        payment_amount = desired_amount
        
        print(f"PAYMENT SCOPE DETERMINED: payment_scope={payment_scope}, paid_for_entry_id={paid_for_entry_id_to_save}, payment_amount={payment_amount}")
        
        # Round to 2 decimal places (YooKassa requires .2f format)
        payment_amount = round(payment_amount, 2)
        
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
                "value": f"{payment_amount:.2f}",
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
        
        print("PAY CREATE:", {
            "entry_id": entry_id,
            "pay": pay,
            "partner_entry_id": partner_entry_id_int,
            "scope": payment_scope,
            "paid_for": paid_for_entry_id_to_save
        })
        print(f"PAYMENT CREATE PAYLOAD: entry_id={entry_id}, tournament_type={tournament_type}, amount={payment_amount:.2f}, payload={payment_data}")
        payment = Payment.create(payment_data, idempotence_key)
        
        new_payment_id = payment.id
        new_confirmation_url = payment.confirmation.confirmation_url
        
        print(f"PAYMENT CREATED: payment_id={new_payment_id}, confirmation_url={new_confirmation_url}, payment_scope={payment_scope}, paid_for_entry_id={paid_for_entry_id_to_save}")
        
        # Сохраняем payment_id, payment_url, payment_scope и paid_for_entry_id в entries
        update_query = """
            UPDATE entries
            SET payment_id = %s,
                payment_url = %s,
                payment_scope = %s,
                paid_for_entry_id = %s
            WHERE id = %s
        """
        
        print(f"UPDATING ENTRY: entry_id={entry_id}, payment_scope={payment_scope}, paid_for_entry_id={paid_for_entry_id_to_save}")
        cur.execute(update_query, (new_payment_id, new_confirmation_url, payment_scope, paid_for_entry_id_to_save, entry_id))
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"REDIRECT: using new payment {new_payment_id}")
        return RedirectResponse(url=new_confirmation_url, status_code=302)
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return HTMLResponse(content=f"<html><body>Ошибка: {str(e)}</body></html>", status_code=500)

@app.get("/p/team")
def payment_team_link(payer_entry_id: int = Query(...), partner_entry_id: int = Query(...)):
    """
    Командная оплата за пару. Создает один платеж на полную стоимость для двух entries.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return HTMLResponse(content="<html><body>Ошибка: база данных не настроена</body></html>", status_code=500)
    
    if not shop_id or not secret_key:
        return HTMLResponse(content="<html><body>Ошибка: YooKassa не настроен</body></html>", status_code=500)
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Проверяем оба entries
        query = """
            SELECT 
                e.id,
                e.payment_status,
                e.tournament_id,
                t.tournament_type,
                t.price_rub,
                t.starts_at
            FROM entries e
            JOIN tournaments t ON e.tournament_id = t.id
            WHERE e.id IN (%s, %s)
        """
        
        cur.execute(query, (payer_entry_id, partner_entry_id))
        rows = cur.fetchall()
        
        if len(rows) != 2:
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body>Ошибка: одна или обе записи не найдены</body></html>", status_code=404)
        
        # Проверяем условия
        entry1_id, status1, tournament_id1, type1, price1, starts_at1 = rows[0]
        entry2_id, status2, tournament_id2, type2, price2, starts_at2 = rows[1]
        
        # Проверка: один tournament_id
        if tournament_id1 != tournament_id2:
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body>Ошибка: записи относятся к разным турнирам</body></html>", status_code=400)
        
        # Проверка: tournament_type = 'team'
        if type1 != 'team':
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body>Ошибка: это не командный турнир</body></html>", status_code=400)
        
        # Проверка: оба payment_status = 'pending'
        if status1 != 'pending' or status2 != 'pending':
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body>Один из игроков уже оплатил. Используйте оплату за себя.</body></html>", status_code=400)
        
        # Проверка: оба entry_id присутствуют
        if payer_entry_id not in [entry1_id, entry2_id] or partner_entry_id not in [entry1_id, entry2_id]:
            cur.close()
            conn.close()
            return HTMLResponse(content="<html><body>Ошибка: неверные entry_id</body></html>", status_code=400)
        
        # Создаем платеж на полную стоимость
        price_rub = price1
        starts_at = starts_at1
        
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
        
        # Генерируем idempotence_key
        idempotence_key = f"team-{payer_entry_id}-{partner_entry_id}-{uuid.uuid4()}"
        
        payment_data = {
            "amount": {
                "value": f"{price_rub:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": return_url
            },
            "description": "Team tournament payment (pair)",
            "capture": True,
            "expires_at": expires_at_str,
            "idempotence_key": idempotence_key
        }
        
        print(f"TEAM PAYMENT CREATE: payer_entry_id={payer_entry_id}, partner_entry_id={partner_entry_id}, payload={payment_data}")
        payment = Payment.create(payment_data, idempotence_key)
        
        payment_id = payment.id
        confirmation_url = payment.confirmation.confirmation_url
        
        # Сохраняем payment_id и payment_url в оба entries
        update_query = """
            UPDATE entries
            SET payment_id = %s,
                payment_url = %s
            WHERE id IN (%s, %s)
        """
        
        cur.execute(update_query, (payment_id, confirmation_url, payer_entry_id, partner_entry_id))
        conn.commit()
        
        cur.close()
        conn.close()
        
        print(f"TEAM PAYMENT CREATED: payment_id={payment_id}, entries={payer_entry_id},{partner_entry_id}")
        return RedirectResponse(url=confirmation_url, status_code=302)
        
    except Exception as e:
        print(f"TEAM PAYMENT ERROR: {str(e)}")
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


def save_player_telegram_id_for_entry(entry_id: int, telegram_user_id) -> None:
    """Save Telegram user ID for the player associated with the entry. telegram_user_id should be a string."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL not set")
    
    # Ensure telegram_user_id is a string
    telegram_user_id_str = str(telegram_user_id)
    
    conn = psycopg2.connect(database_url, sslmode="require")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                update players p
                set telegram_id = %s
                from entries e
                where e.player_id = p.id and e.id = %s
            """, (telegram_user_id_str, entry_id))
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
            payment_object = payload.get("object", {})
            
            # Get actual payment amount from YooKassa
            amount_value = None
            if payment_object.get("amount"):
                amount_value = payment_object["amount"].get("value")
                if amount_value:
                    try:
                        amount_value = float(amount_value)
                    except (ValueError, TypeError):
                        amount_value = None
            
            if payment_id:
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # First, get payer entry info (entry with this payment_id)
                fetch_payer_query = """
                    SELECT 
                        id,
                        payment_scope,
                        paid_for_entry_id,
                        payment_status
                    FROM entries
                    WHERE payment_id = %s
                    LIMIT 1
                """
                cur.execute(fetch_payer_query, (payment_id,))
                payer_row = cur.fetchone()
                
                if not payer_row:
                    cur.close()
                    conn.close()
                    print(f"WARNING: No entry found with payment_id={payment_id}")
                    return {"ok": True}
                
                payer_entry_id, payment_scope, paid_for_entry_id, payer_status = payer_row
                
                # If both entries already paid, just return ok (idempotent)
                if payer_status == 'paid' and paid_for_entry_id:
                    cur.execute("SELECT payment_status FROM entries WHERE id = %s", (paid_for_entry_id,))
                    partner_status_row = cur.fetchone()
                    if partner_status_row and partner_status_row[0] == 'paid':
                        cur.close()
                        conn.close()
                        print(f"INFO: Both entries already paid for payment_id={payment_id}")
                        return {"ok": True}
                
                # Update payer entry
                if amount_value is not None:
                    update_payer_query = """
                        UPDATE entries
                        SET payment_status = 'paid', 
                            paid_at = NOW(),
                            paid_amount_rub = %s
                        WHERE id = %s AND payment_status != 'paid'
                    """
                    cur.execute(update_payer_query, (amount_value, payer_entry_id))
                else:
                    update_payer_query = """
                        UPDATE entries
                        SET payment_status = 'paid', 
                            paid_at = NOW()
                        WHERE id = %s AND payment_status != 'paid'
                    """
                    cur.execute(update_payer_query, (payer_entry_id,))
                
                # If this is a pair payment, update partner entry
                if payment_scope == 'pair' and paid_for_entry_id:
                    # Check partner entry exists and is not already paid
                    cur.execute("""
                        SELECT id, payment_status
                        FROM entries
                        WHERE id = %s
                    """, (paid_for_entry_id,))
                    partner_row = cur.fetchone()
                    
                    if partner_row:
                        partner_id, partner_status = partner_row
                        if partner_status != 'paid':
                            # Update partner entry
                            if amount_value is not None:
                                # For pair payment, partner gets half amount (or full, depending on logic)
                                # We'll use the same amount for now, but mark it as paid by payer
                                update_partner_query = """
                                    UPDATE entries
                                    SET payment_status = 'paid',
                                        paid_at = NOW(),
                                        paid_by_entry_id = %s,
                                        paid_amount_rub = %s
                                    WHERE id = %s
                                """
                                cur.execute(update_partner_query, (payer_entry_id, amount_value, partner_id))
                            else:
                                update_partner_query = """
                                    UPDATE entries
                                    SET payment_status = 'paid',
                                        paid_at = NOW(),
                                        paid_by_entry_id = %s
                                    WHERE id = %s
                                """
                                cur.execute(update_partner_query, (payer_entry_id, partner_id))
                        else:
                            print(f"WARNING: Partner entry {partner_id} already paid, skipping update")
                    else:
                        print(f"WARNING: Partner entry {paid_for_entry_id} not found, payer entry still marked as paid")
                
                conn.commit()
                
                # Fetch all entries that should be notified:
                # 1. Entry with this payment_id (payer)
                # 2. Partner entry if this is a pair payment (via paid_for_entry_id)
                fetch_query = """
                    SELECT DISTINCT
                        e.id,
                        p.telegram_id,
                        t.title,
                        t.starts_at,
                        t.price_rub,
                        t.tournament_type,
                        t.location,
                        e.paid_amount_rub,
                        e.paid_by_entry_id,
                        e.paid_for_entry_id
                    FROM entries e
                    JOIN players p ON e.player_id = p.id
                    JOIN tournaments t ON e.tournament_id = t.id
                    WHERE e.payment_id = %s
                       OR (e.paid_by_entry_id IN (
                           SELECT id FROM entries WHERE payment_id = %s
                       ))
                """
                
                cur.execute(fetch_query, (payment_id, payment_id))
                rows = cur.fetchall()
                
                cur.close()
                conn.close()
                
                # Send Telegram notifications to all players whose status became paid
                if rows and bot is not None:
                    for row in rows:
                        entry_id, telegram_id, tournament_title, starts_at, price_rub, tournament_type, location, paid_amount_rub, paid_by_entry_id, paid_for_entry_id = row
                        if telegram_id:
                            try:
                                # Format starts_at (without MSK suffix)
                                if starts_at:
                                    if isinstance(starts_at, datetime):
                                        if starts_at.tzinfo is None:
                                            starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                                        else:
                                            starts_at_utc = starts_at.astimezone(timezone.utc)
                                        starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                                        starts_at_str = starts_at_msk.strftime("%d.%m.%Y %H:%M")
                                    else:
                                        starts_at_str = str(starts_at)
                                else:
                                    starts_at_str = "Не указано"
                                
                                # Determine actual payment amount
                                # Priority: paid_amount_rub > calculated from tournament type
                                if paid_amount_rub is not None:
                                    actual_amount = int(paid_amount_rub)
                                elif tournament_type == 'team' and not paid_by_entry_id:
                                    # Single team payment (half)
                                    actual_amount = int(price_rub / 2)
                                else:
                                    # Personal or full team payment
                                    actual_amount = int(price_rub)
                                
                                # Check if this is a pair payment
                                if paid_by_entry_id:
                                    # Partner entry - someone paid for them
                                    # Get payer name
                                    conn2 = psycopg2.connect(database_url, sslmode="require")
                                    cur2 = conn2.cursor()
                                    cur2.execute("""
                                        SELECT p2.full_name
                                        FROM entries e2
                                        JOIN players p2 ON e2.player_id = p2.id
                                        WHERE e2.id = %s
                                    """, (paid_by_entry_id,))
                                    payer_row = cur2.fetchone()
                                    payer_name = payer_row[0] if payer_row else "партнер"
                                    cur2.close()
                                    conn2.close()
                                    
                                    message = f"""✅ Оплата получена!

Турнир: {tournament_title}
Место: {location or 'Не указано'}
Время: {starts_at_str}

Партнер {payer_name} оплатил за пару."""
                                elif paid_for_entry_id:
                                    # Payer entry - they paid for partner
                                    # Get partner name
                                    conn2 = psycopg2.connect(database_url, sslmode="require")
                                    cur2 = conn2.cursor()
                                    cur2.execute("""
                                        SELECT p2.full_name
                                        FROM entries e2
                                        JOIN players p2 ON e2.player_id = p2.id
                                        WHERE e2.id = %s
                                    """, (paid_for_entry_id,))
                                    partner_row = cur2.fetchone()
                                    partner_name = partner_row[0] if partner_row else "партнер"
                                    cur2.close()
                                    conn2.close()
                                    
                                    message = f"""✅ Оплата получена!

Турнир: {tournament_title}
Место: {location or 'Не указано'}
Время: {starts_at_str}
Сумма: {actual_amount} ₽

Вы оплатили за пару (партнер: {partner_name})."""
                                else:
                                    # Personal payment or single team payment
                                    message = f"""✅ Оплата получена!

Турнир: {tournament_title}
Место: {location or 'Не указано'}
Время: {starts_at_str}
Сумма: {actual_amount} ₽"""
                                
                                await bot.send_message(chat_id=telegram_id, text=message)
                            except Exception as telegram_error:
                                # Log error but don't fail the webhook
                                print(f"Telegram notification error: {telegram_error}")
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

# Helper functions for Telegram bot
def get_entry_info(entry_id: int):
    """Get entry info: tournament_type, title, starts_at, price_rub, tournament_id, player_id"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None
    
    conn = psycopg2.connect(database_url, sslmode="require")
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT 
                t.tournament_type,
                t.title,
                t.starts_at,
                t.price_rub,
                t.id as tournament_id,
                e.player_id
            FROM entries e
            JOIN tournaments t ON e.tournament_id = t.id
            WHERE e.id = %s
        """, (entry_id,))
        row = cur.fetchone()
        
        if row:
            return {
                "tournament_type": row[0],
                "title": row[1],
                "starts_at": row[2],
                "price_rub": row[3],
                "tournament_id": row[4],
                "player_id": row[5]
            }
        return None
    finally:
        cur.close()
        conn.close()

def get_player_id_by_telegram(telegram_id_text: str):
    """Get player_id by telegram_id (TEXT)"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return None
    
    conn = psycopg2.connect(database_url, sslmode="require")
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT id FROM players WHERE telegram_id = %s
        """, (telegram_id_text,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        cur.close()
        conn.close()

def get_partners_for_tournament(tournament_id: int, exclude_player_id: int):
    """Get list of partners for tournament: list of {entry_id, full_name}"""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return []
    
    conn = psycopg2.connect(database_url, sslmode="require")
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT e.id, p.full_name
            FROM entries e
            JOIN players p ON e.player_id = p.id
            WHERE e.tournament_id = %s
              AND e.player_id != %s
              AND e.active = true
              AND e.payment_status = 'pending'
            ORDER BY p.full_name
        """, (tournament_id, exclude_player_id))
        return [{"entry_id": row[0], "full_name": row[1]} for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()

def format_dt_msk(dt):
    """Format datetime in MSK timezone: DD.MM.YYYY HH:MM (without MSK suffix)"""
    if not dt:
        return "Не указано"
    
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt_utc = dt.replace(tzinfo=timezone.utc)
        else:
            dt_utc = dt.astimezone(timezone.utc)
        dt_msk = dt_utc.astimezone(BOT_TZ)
        return dt_msk.strftime("%d.%m.%Y %H:%M")
    return str(dt)

@app.post("/webhooks/telegram")
async def telegram_webhook(request: Request):
    if bot is None:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN is missing"}

    payload = await request.json()
    print("TG UPDATE:", payload)

    # 1) Сообщения
    message = payload.get("message")
    if message:
        text = (message.get("text") or "").strip()
        print("TG TEXT:", text)
        chat_id = message["chat"]["id"]
        from_user = message.get("from")

        # /start
        if text.startswith("/start"):
            # Get telegram_user_id (always convert to string)
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = str(from_user["id"])
            
            if not telegram_user_id:
                await bot.send_message(chat_id=chat_id, text="Ошибка: не удалось определить ваш Telegram ID.")
                return {"ok": True}
            
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                return {"ok": True}
            
            try:
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Check if player exists with this telegram_id
                cur.execute("SELECT full_name FROM players WHERE telegram_id = %s", (telegram_user_id,))
                row = cur.fetchone()
                
                if row:
                    # Player exists, show menu with "Мои турниры" and "Помощь"
                    player_name = row[0]
                    welcome_text = f"Привет, {player_name}!"
                    keyboard = ReplyKeyboardMarkup(
                        [
                            [KeyboardButton("Мои турниры"), KeyboardButton("Помощь")]
                        ],
                        resize_keyboard=True
                    )
                    await bot.send_message(
                        chat_id=chat_id,
                        text=welcome_text,
                        reply_markup=keyboard
                    )
                else:
                    # Player not found, show menu with "Привязать аккаунт" and "Помощь"
                    keyboard = ReplyKeyboardMarkup(
                        [
                            [KeyboardButton("Привязать аккаунт"), KeyboardButton("Помощь")]
                        ],
                        resize_keyboard=True
                    )
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Привет! Чтобы начать, нужно привязать аккаунт.",
                        reply_markup=keyboard
                    )
                
                cur.close()
                conn.close()
            except Exception as e:
                await bot.send_message(chat_id=chat_id, text=f"Ошибка: {str(e)}")
            
            return {"ok": True}
        
        # /whoami command
        if text.startswith("/whoami"):
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = str(from_user["id"])
            
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

        # Check support_mode BEFORE other handlers (except /start, /whoami, /pay, buttons)
        telegram_user_id = tg_id_str(from_user)
        if telegram_user_id:
            try:
                support_mode = get_support_mode(telegram_user_id)
                if support_mode and text not in ["Мои турниры", "Помощь"] and not text.startswith("/start") and not text.startswith("/pay") and not text.startswith("/whoami"):
                    # User is in support mode, process help request
                    admin_chat_id = os.getenv("ADMIN_CHAT_ID")
                    
                    if not admin_chat_id:
                        print("WARNING: ADMIN_CHAT_ID not set, support mode unavailable")
                        await bot.send_message(
                            chat_id=chat_id,
                            text="Сервис помощи временно недоступен."
                        )
                        set_support_mode(telegram_user_id, False)
                        return {"ok": True}
                    
                    # Get player info
                    player_info = get_player_by_tg(telegram_user_id)
                    player_name = player_info[1] if player_info else "не найден в базе"
                    
                    # Get username
                    username = from_user.get("username") if from_user else None
                    username_str = f"@{username}" if username else "—"
                    
                    # Form admin message
                    admin_message = f"""🆘 Help request

Player: {player_name}
TG: {username_str}
chat_id: {telegram_user_id}
Text: {text}"""
                    
                    # Send to admin
                    if bot:
                        try:
                            await bot.send_message(chat_id=admin_chat_id, text=admin_message)
                        except Exception as e:
                            print(f"ERROR sending to admin: {str(e)}")
                    
                    # Reset support mode
                    set_support_mode(telegram_user_id, False)
                    
                    # Reply to user
                    await bot.send_message(
                        chat_id=chat_id,
                        text="✅ Принято! Спасибо. Мы разберёмся."
                    )
                    return {"ok": True}
            except Exception as e:
                print(f"ERROR in support_mode check: {str(e)}")
                # Continue with normal processing if error

        # Handle text messages when session state is "awaiting_lunda_name"
        # Skip if it's a known button or command
        if text not in ["Мои турниры", "Помощь"] and not text.startswith("/"):
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = str(from_user["id"])
            
            if telegram_user_id:
                database_url = os.getenv("DATABASE_URL")
                if database_url:
                    try:
                        conn = psycopg2.connect(database_url, sslmode="require")
                        cur = conn.cursor()
                        
                        # Check if there's an active session with awaiting_lunda_name state
                        print("TG DEBUG session check telegram_user_id=", telegram_user_id, "type=", type(telegram_user_id))
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
                            print("TG DEBUG update session telegram_user_id=", telegram_user_id, "type=", type(telegram_user_id))
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
                                print("TG DEBUG delete session telegram_user_id=", telegram_user_id, "type=", type(telegram_user_id))
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
                                print("TG DEBUG manual link telegram_user_id=", telegram_user_id, "type=", type(telegram_user_id))
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
        print("TG CHECK my_tournaments branch, text=", text)
        if text == "Мои турниры":
            print("TG ENTERED my_tournaments branch")
            # Get telegram_user_id (always convert to string)
            telegram_user_id = None
            if from_user and from_user.get("id"):
                telegram_user_id = str(from_user["id"])
            
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
                # Гарантированно приводим к строке перед запросом
                telegram_user_id = str(telegram_user_id)
                print("DEBUG my_tournaments telegram_user_id=", telegram_user_id, "type=", type(telegram_user_id))
                cur.execute("SELECT id FROM players WHERE telegram_id = %s", (telegram_user_id,))
                player_row = cur.fetchone()
                
                if not player_row:
                    cur.close()
                    conn.close()
                    # Show menu with "Привязать аккаунт"
                    keyboard = ReplyKeyboardMarkup(
                        [
                            [KeyboardButton("Привязать аккаунт"), KeyboardButton("Помощь")]
                        ],
                        resize_keyboard=True
                    )
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Я тебя не нашёл в базе. Нажми «Привязать аккаунт», чтобы привязаться через выбор турнира.",
                        reply_markup=keyboard
                    )
                    return {"ok": True}
                
                player_id = player_row[0]
                
                # Query future tournaments (starts_at > now(), strictly future)
                query = """
                    SELECT 
                        e.id as entry_id,
                        t.title,
                        t.starts_at,
                        t.price_rub,
                        t.tournament_type,
                        t.location,
                        e.payment_status
                    FROM entries e
                    JOIN tournaments t ON e.tournament_id = t.id
                    WHERE e.player_id = %s 
                      AND t.starts_at > NOW()
                    ORDER BY t.starts_at ASC
                """
                
                cur.execute(query, (player_id,))
                rows = cur.fetchall()
                
                cur.close()
                conn.close()
                
                if not rows:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="У тебя пока нет ближайших турниров."
                    )
                    return {"ok": True}
                
                # Send message for each entry
                for row in rows:
                    entry_id, title, starts_at, price_rub, tournament_type, location, payment_status = row
                    
                    # Format starts_at (without MSK suffix)
                    if starts_at:
                        # Convert to UTC if timezone-naive
                        if starts_at.tzinfo is None:
                            starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                        else:
                            starts_at_utc = starts_at.astimezone(timezone.utc)
                        
                        # Convert to MSK
                        starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                        starts_at_str = starts_at_msk.strftime("%d.%m.%Y %H:%M")
                    else:
                        starts_at_str = "Не указано"
                    
                    # Format payment status
                    status_emoji = "✅" if payment_status == "paid" else "⏳"
                    status_text = "Оплачено" if payment_status == "paid" else "Не оплачено"
                    
                    # Build message with location
                    location_str = location or "Не указано"
                    message = f"""<b>{title}</b>
Место: {location_str}
Время: {starts_at_str}
{status_emoji} {status_text}"""
                    
                    # Create inline keyboard if not paid
                    keyboard = None
                    if payment_status != 'paid':
                        if tournament_type == 'team':
                            # Team tournament - show choice button
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Оплатить", callback_data=f"pay:{entry_id}")
                                ]
                            ])
                        else:
                            # Personal tournament - direct payment link
                            payment_link = f"{PUBLIC_BASE_URL}/p/e/{entry_id}"
                            keyboard = InlineKeyboardMarkup([
                                [
                                    InlineKeyboardButton("Оплатить", url=payment_link)
                                ]
                            ])
                    
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

        # "Помощь" button
        if text == "Помощь":
            telegram_user_id = tg_id_str(from_user)
            if not telegram_user_id:
                await bot.send_message(
                    chat_id=chat_id,
                    text="Ошибка: не удалось определить ваш Telegram ID."
                )
                return {"ok": True}
            
            try:
                set_support_mode(telegram_user_id, True)
                await bot.send_message(
                    chat_id=chat_id,
                    text="Опиши проблему одним сообщением. Я отправлю её администратору."
                )
                return {"ok": True}
            except Exception as e:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ошибка: {str(e)}"
                )
                return {"ok": True}

        # "Привязать аккаунт" button
        if text == "Привязать аккаунт":
            telegram_user_id = tg_id_str(from_user)
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
                
                # Get distinct dates of future tournaments
                cur.execute("""
                    SELECT DISTINCT DATE(starts_at AT TIME ZONE 'Europe/Moscow') AS tournament_date
                    FROM tournaments
                    WHERE archived_at IS NULL
                      AND starts_at >= NOW()
                    ORDER BY tournament_date ASC
                    LIMIT 10
                """)
                date_rows = cur.fetchall()
                
                if not date_rows:
                    cur.close()
                    conn.close()
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Пока не вижу ближайших турниров в базе. Запишись на турнир в Lunda и попробуй снова 🙂"
                    )
                    return {"ok": True}
                
                # Create buttons for dates
                buttons = []
                for (date_obj,) in date_rows:
                    date_display, date_iso = fmt_date_ru(date_obj)
                    buttons.append([InlineKeyboardButton(date_display, callback_data=f"bind_date:{date_iso}")])
                
                # Add Cancel button
                buttons.append([InlineKeyboardButton("Отмена", callback_data="bind_back:menu")])
                
                keyboard = InlineKeyboardMarkup(buttons)
                
                await bot.send_message(
                    chat_id=chat_id,
                    text="Чтобы привязать аккаунт, выбери дату турнира, на который ты УЖЕ записан в Lunda. Это нужно только один раз.",
                    reply_markup=keyboard
                )
                
                cur.close()
                conn.close()
                return {"ok": True}
            except Exception as e:
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ошибка: {str(e)}"
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
                
                # Parse telegram_user_id (always convert to string)
                telegram_user_id = None
                if from_user and from_user.get("id"):
                    telegram_user_id = str(from_user["id"])
                    # Store Telegram user id
                    save_player_telegram_id_for_entry(entry_id, telegram_user_id)
                
                # Используем вечную ссылку на наш сервис
                payment_link = f"{PUBLIC_BASE_URL}/p/e/{entry_id}"
                
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
        
        # Main payment handler: pay:<entry_id>
        if data.startswith("pay:"):
            try:
                entry_id = int(data.split(":")[1])
                await bot.answer_callback_query(callback_query["id"])
                
                print(f"PAY CALLBACK: entry_id={entry_id}")
                
                entry_info = get_entry_info(entry_id)
                if not entry_info:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: запись не найдена.")
                    return {"ok": True}
                
                tournament_type = entry_info["tournament_type"]
                print(f"PAY CALLBACK: tournament_type={tournament_type}")
                
                public_base_url = os.getenv("PUBLIC_BASE_URL")
                if not public_base_url:
                    print("ERROR: PUBLIC_BASE_URL not set")
                    await bot.send_message(chat_id=chat_id, text="Ошибка: сервис временно недоступен.")
                    return {"ok": True}
                
                if tournament_type == 'personal':
                    # Personal tournament: сразу ссылка на оплату
                    payment_link = f"{public_base_url}/p/e/{entry_id}"
                    
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Оплатить", url=payment_link)
                        ]
                    ])
                    
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Ссылка на оплату:",
                        reply_markup=keyboard
                    )
                else:
                    # Team tournament: показать выбор 50% или 100%
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Оплатить за себя (50%)", callback_data=f"pay_half:{entry_id}")
                        ],
                        [
                            InlineKeyboardButton("Оплатить за пару (100%)", callback_data=f"pay_full_choose:{entry_id}")
                        ],
                        [
                            InlineKeyboardButton("Отмена", callback_data=f"pay_cancel:{entry_id}")
                        ]
                    ])
                    
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Как вы хотите оплатить?",
                        reply_markup=keyboard
                    )
                
                return {"ok": True}
            except Exception as e:
                print(f"PAY CALLBACK ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
        
        # Pay half (50%): pay_half:<entry_id>
        if data.startswith("pay_half:"):
            try:
                entry_id = int(data.split(":")[1])
                await bot.answer_callback_query(callback_query["id"])
                
                print(f"PAY_HALF CALLBACK: entry_id={entry_id}")
                
                public_base_url = os.getenv("PUBLIC_BASE_URL")
                if not public_base_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: сервис временно недоступен.")
                    return {"ok": True}
                
                payment_link = f"{public_base_url}/p/e/{entry_id}?pay=half"
                
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("Оплатить", url=payment_link)
                    ]
                ])
                
                await bot.send_message(
                    chat_id=chat_id,
                    text="Оплата за себя (50%). После оплаты статус обновится автоматически.",
                    reply_markup=keyboard
                )
                return {"ok": True}
            except Exception as e:
                print(f"PAY_HALF ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
        
        # Pay full choose partner: pay_full_choose:<entry_id>
        if data.startswith("pay_full_choose:"):
            try:
                entry_id = int(data.split(":")[1])
                await bot.answer_callback_query(callback_query["id"])
                
                print(f"PAY_FULL_CHOOSE CALLBACK: entry_id={entry_id}")
                
                entry_info = get_entry_info(entry_id)
                if not entry_info:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: запись не найдена.")
                    return {"ok": True}
                
                tournament_id = entry_info["tournament_id"]
                player_id = entry_info["player_id"]
                
                # Get telegram_id from callback to find current player
                from_user = callback_query.get("from", {})
                telegram_id = str(from_user.get("id", ""))
                
                if not telegram_id:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: не удалось определить пользователя.")
                    return {"ok": True}
                
                # Get partners for tournament
                partners = get_partners_for_tournament(tournament_id, player_id)
                print(f"PAY_FULL_CHOOSE: found {len(partners)} partners")
                
                if not partners:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Нет доступных партнеров для оплаты. Все участники уже оплатили или запись не найдена."
                    )
                    return {"ok": True}
                
                # Create inline buttons for each partner (1-2 per row)
                buttons = []
                for i in range(0, len(partners), 2):
                    row = []
                    row.append(InlineKeyboardButton(
                        partners[i]["full_name"],
                        callback_data=f"pay_full_partner:{entry_id}:{partners[i]['entry_id']}"
                    ))
                    if i + 1 < len(partners):
                        row.append(InlineKeyboardButton(
                            partners[i + 1]["full_name"],
                            callback_data=f"pay_full_partner:{entry_id}:{partners[i + 1]['entry_id']}"
                        ))
                    buttons.append(row)
                
                # Add Back and Cancel buttons
                buttons.append([
                    InlineKeyboardButton("Назад", callback_data=f"pay:{entry_id}")
                ])
                buttons.append([
                    InlineKeyboardButton("Отмена", callback_data=f"pay_cancel:{entry_id}")
                ])
                
                keyboard = InlineKeyboardMarkup(buttons)
                
                await bot.send_message(
                    chat_id=chat_id,
                    text="За кого вы хотите оплатить?",
                    reply_markup=keyboard
                )
                return {"ok": True}
            except Exception as e:
                print(f"PAY_FULL_CHOOSE ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
        
        # Pay full partner: pay_full_partner:<entry_id>:<partner_entry_id>
        if data.startswith("pay_full_partner:"):
            try:
                parts = data.split(":")
                entry_id = int(parts[1])
                partner_entry_id = int(parts[2])
                await bot.answer_callback_query(callback_query["id"])
                
                print(f"PAY_FULL_PARTNER CALLBACK: entry_id={entry_id}, partner_entry_id={partner_entry_id}")
                
                public_base_url = os.getenv("PUBLIC_BASE_URL")
                if not public_base_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: сервис временно недоступен.")
                    return {"ok": True}
                
                # Get partner name
                database_url = os.getenv("DATABASE_URL")
                if database_url:
                    conn = psycopg2.connect(database_url, sslmode="require")
                    cur = conn.cursor()
                    try:
                        cur.execute("""
                            SELECT p.full_name
                            FROM entries e
                            JOIN players p ON e.player_id = p.id
                            WHERE e.id = %s
                        """, (partner_entry_id,))
                        row = cur.fetchone()
                        partner_name = row[0] if row else "Партнер"
                    finally:
                        cur.close()
                        conn.close()
                else:
                    partner_name = "Партнер"
                
                # Include partner_entry_id in payment link for pair payment
                payment_link = f"{public_base_url}/p/e/{entry_id}?pay=full&partner_entry_id={partner_entry_id}"
                
                keyboard = InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("Оплатить", url=payment_link)
                    ]
                ])
                
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ты оплачиваешь за пару. Партнер: {partner_name}",
                    reply_markup=keyboard
                )
                return {"ok": True}
            except Exception as e:
                print(f"PAY_FULL_PARTNER ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
        
        # Pay cancel: pay_cancel:<entry_id>
        if data.startswith("pay_cancel:"):
            try:
                await bot.answer_callback_query(callback_query["id"], text="Отменено")
                return {"ok": True}
            except Exception as e:
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
        
        if data.startswith("get_link:"):
            try:
                entry_id = int(data.split(":")[1])
                # Используем вечную ссылку на наш сервис
                payment_link = f"{PUBLIC_BASE_URL}/p/e/{entry_id}"
                
                # Answer callback query first
                await bot.answer_callback_query(callback_query["id"])
                
                # Send plain text message with the link and instruction how to copy
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ссылка на оплату:\n\n{payment_link}\n\nЧтобы скопировать ссылку, нажмите на неё и удерживайте, затем выберите \"Копировать\"."
                )
                return {"ok": True}
            except ValueError as e:
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
            except Exception as e:
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
                return {"ok": True}
        
        # Bind account flow callbacks
        from_user = callback_query.get("from", {})
        telegram_user_id = str(from_user.get("id", "")) if from_user.get("id") else None
        
        # bind_date:<date> - выбор даты
        if data.startswith("bind_date:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                date_str = data.split(":", 1)[1]
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Parse date_str - может быть в формате YYYY-MM-DD или datetime object
                try:
                    # Попробуем распарсить как дату
                    if isinstance(date_str, str):
                        # Если это строка вида "2026-01-10", используем как есть
                        # Если это datetime object string, парсим
                        from dateutil import parser
                        date_parsed = parser.parse(date_str)
                        date_str = date_parsed.strftime("%Y-%m-%d")
                except:
                    # Если не парсится, используем как есть (уже в формате YYYY-MM-DD)
                    pass
                
                # Get tournaments for this date (using MSK timezone to match date selection)
                cur.execute("""
                    SELECT id, title, starts_at, location
                    FROM tournaments
                    WHERE DATE(starts_at AT TIME ZONE 'Europe/Moscow') = %s::date
                      AND starts_at >= NOW()
                      AND archived_at IS NULL
                    ORDER BY starts_at ASC
                """, (date_str,))
                tournaments = cur.fetchall()
                
                if not tournaments:
                    await bot.send_message(chat_id=chat_id, text="На эту дату нет доступных турниров.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Update session
                cur.execute("""
                    UPDATE telegram_sessions
                    SET state = 'bind_pick_tournament'
                    WHERE telegram_id = %s
                """, (telegram_user_id,))
                conn.commit()
                
                # Create buttons for tournaments
                buttons = []
                for tournament_id, title, starts_at, location in tournaments:
                    # Format time
                    if starts_at:
                        if isinstance(starts_at, datetime):
                            if starts_at.tzinfo is None:
                                starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                            else:
                                starts_at_utc = starts_at.astimezone(timezone.utc)
                            starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                            time_str = starts_at_msk.strftime("%H:%M")
                        else:
                            time_str = str(starts_at)
                    else:
                        time_str = "??:??"
                    
                    location_str = location or ""
                    button_text = f"{title[:30]} — {time_str}" if len(title) <= 30 else f"{title[:27]}... — {time_str}"
                    buttons.append([InlineKeyboardButton(button_text, callback_data=f"bind_pick_tournament:{tournament_id}")])
                
                buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="bind_back:date")])
                
                keyboard = InlineKeyboardMarkup(buttons)
                
                await bot.send_message(
                    chat_id=chat_id,
                    text="Выбери турнир:",
                    reply_markup=keyboard
                )
                
                print(f"BIND: выбранная дата={date_str}")
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"BIND DATE ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # bind_pick_tournament:<tournament_id> - выбор турнира
        if data.startswith("bind_pick_tournament:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                tournament_id = int(data.split(":")[1])
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get tournament info
                cur.execute("""
                    SELECT title, location, starts_at
                    FROM tournaments
                    WHERE id = %s
                """, (tournament_id,))
                tournament_row = cur.fetchone()
                
                if not tournament_row:
                    await bot.send_message(chat_id=chat_id, text="Турнир не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                tournament_title, location, starts_at = tournament_row
                
                # Get players for this tournament (paginated)
                cur.execute("""
                    SELECT p.id, p.full_name
                    FROM entries e
                    JOIN players p ON e.player_id = p.id
                    WHERE e.tournament_id = %s
                      AND e.active = true
                    ORDER BY p.full_name ASC
                """, (tournament_id,))
                players = cur.fetchall()
                
                if not players:
                    await bot.send_message(chat_id=chat_id, text="В этом турнире нет участников.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Update session
                cur.execute("""
                    UPDATE telegram_sessions
                    SET state = 'bind_pick_player'
                    WHERE telegram_id = %s
                """, (telegram_user_id,))
                conn.commit()
                
                # Show first page of players
                page = 0
                players_per_page = 10
                start_idx = page * players_per_page
                end_idx = start_idx + players_per_page
                page_players = players[start_idx:end_idx]
                
                buttons = []
                for player_id, full_name in page_players:
                    buttons.append([InlineKeyboardButton(full_name, callback_data=f"bind_pick_player:{tournament_id}:{player_id}:{page}")])
                
                # Navigation buttons
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"bind_player_page:{tournament_id}:{page-1}"))
                if end_idx < len(players):
                    nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"bind_player_page:{tournament_id}:{page+1}"))
                if nav_buttons:
                    buttons.append(nav_buttons)
                
                buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="bind_back:tournament")])
                buttons.append([InlineKeyboardButton("Отмена", callback_data="bind_back:menu")])
                
                keyboard = InlineKeyboardMarkup(buttons)
                
                await bot.send_message(
                    chat_id=chat_id,
                    text="Выбери себя из списка участников:",
                    reply_markup=keyboard
                )
                
                print(f"BIND: выбранный турнир={tournament_id}, title={tournament_title}")
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"BIND TOURNAMENT ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # bind_player_page:<tournament_id>:<page> - пагинация участников
        if data.startswith("bind_player_page:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                parts = data.split(":")
                tournament_id = int(parts[1])
                page = int(parts[2])
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get players
                cur.execute("""
                    SELECT p.id, p.full_name
                    FROM entries e
                    JOIN players p ON e.player_id = p.id
                    WHERE e.tournament_id = %s
                      AND e.active = true
                    ORDER BY p.full_name ASC
                """, (tournament_id,))
                players = cur.fetchall()
                
                players_per_page = 10
                start_idx = page * players_per_page
                end_idx = start_idx + players_per_page
                page_players = players[start_idx:end_idx]
                
                buttons = []
                for player_id, full_name in page_players:
                    buttons.append([InlineKeyboardButton(full_name, callback_data=f"bind_pick_player:{tournament_id}:{player_id}:{page}")])
                
                # Navigation buttons
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"bind_player_page:{tournament_id}:{page-1}"))
                if end_idx < len(players):
                    nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"bind_player_page:{tournament_id}:{page+1}"))
                if nav_buttons:
                    buttons.append(nav_buttons)
                
                buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="bind_back:tournament")])
                buttons.append([InlineKeyboardButton("Отмена", callback_data="bind_back:menu")])
                
                keyboard = InlineKeyboardMarkup(buttons)
                
                # Edit message
                await bot.edit_message_reply_markup(
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=keyboard
                )
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"BIND PAGE ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # bind_pick_player:<tournament_id>:<player_id>:<page> - выбор участника
        if data.startswith("bind_pick_player:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                parts = data.split(":")
                tournament_id = int(parts[1])
                player_id = int(parts[2])
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get player and tournament info
                cur.execute("""
                    SELECT p.full_name, t.title, t.location, t.starts_at
                    FROM players p, tournaments t
                    WHERE p.id = %s AND t.id = %s
                """, (player_id, tournament_id))
                row = cur.fetchone()
                
                if not row:
                    await bot.send_message(chat_id=chat_id, text="Данные не найдены.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                player_name, tournament_title, location, starts_at = row
                
                # Format starts_at
                if starts_at:
                    if isinstance(starts_at, datetime):
                        if starts_at.tzinfo is None:
                            starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                        else:
                            starts_at_utc = starts_at.astimezone(timezone.utc)
                        starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                        starts_at_str = starts_at_msk.strftime("%d.%m.%Y %H:%M")
                    else:
                        starts_at_str = str(starts_at)
                else:
                    starts_at_str = "Не указано"
                
                # Update session
                cur.execute("""
                    UPDATE telegram_sessions
                    SET state = 'bind_confirm'
                    WHERE telegram_id = %s
                """, (telegram_user_id,))
                conn.commit()
                
                location_str = location or "Не указано"
                
                keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton("✅ Да, это я", callback_data=f"bind_confirm:{player_id}")],
                    [InlineKeyboardButton("↩️ Назад", callback_data=f"bind_back:player:{tournament_id}")]
                ])
                
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"Ты — {player_name}?\n\nТурнир: {tournament_title}\nМесто: {location_str}\nВремя: {starts_at_str}",
                    reply_markup=keyboard
                )
                
                print(f"BIND: выбранный player={player_id}, name={player_name}")
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"BIND PLAYER ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # bind_confirm:<player_id> - подтверждение привязки
        if data.startswith("bind_confirm:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                player_id = int(data.split(":")[1])
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Check if this telegram_id is already bound to another player
                cur.execute("""
                    SELECT id, full_name FROM players WHERE telegram_id = %s AND id != %s
                """, (telegram_user_id, player_id))
                other_player = cur.fetchone()
                
                if other_player:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Этот Telegram уже привязан, напишите админу."
                    )
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Check if selected player already has telegram_id
                cur.execute("SELECT telegram_id FROM players WHERE id = %s", (player_id,))
                player_row = cur.fetchone()
                
                if player_row and player_row[0] and player_row[0] != telegram_user_id:
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Этот игрок уже привязан к другому Telegram."
                    )
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Bind player
                cur.execute("""
                    UPDATE players
                    SET telegram_id = %s
                    WHERE id = %s
                """, (telegram_user_id, player_id))
                conn.commit()
                
                print(f"BIND CONFIRM: player_id={player_id}, telegram_id={telegram_user_id}, result=success")
                
                # Clear session
                cur.execute("DELETE FROM telegram_sessions WHERE telegram_id = %s", (telegram_user_id,))
                conn.commit()
                
                # Send confirmation
                await bot.send_message(chat_id=chat_id, text="Готово! Аккаунт привязан.")
                
                # Show new menu
                keyboard = ReplyKeyboardMarkup(
                    [
                        [KeyboardButton("Мои турниры"), KeyboardButton("Помощь")]
                    ],
                    resize_keyboard=True
                )
                await bot.send_message(chat_id=chat_id, text="Теперь ты можешь использовать все функции бота.", reply_markup=keyboard)
                
                # Send notifications for future entries
                cur.execute("""
                    SELECT 
                        e.id,
                        t.title,
                        t.starts_at,
                        t.price_rub,
                        t.tournament_type,
                        t.location,
                        p.full_name
                    FROM entries e
                    JOIN tournaments t ON e.tournament_id = t.id
                    JOIN players p ON e.player_id = p.id
                    WHERE e.player_id = %s
                      AND e.telegram_notified = false
                      AND t.starts_at > NOW()
                      AND t.archived_at IS NULL
                    ORDER BY t.starts_at ASC
                """, (player_id,))
                future_entries = cur.fetchall()
                
                public_base_url = os.getenv("PUBLIC_BASE_URL")
                
                for entry_id, title, starts_at, price_rub, tournament_type, location, full_name in future_entries:
                    try:
                        # Format starts_at
                        if starts_at:
                            if isinstance(starts_at, datetime):
                                if starts_at.tzinfo is None:
                                    starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                                else:
                                    starts_at_utc = starts_at.astimezone(timezone.utc)
                                starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                                starts_at_str = starts_at_msk.strftime("%d.%m.%Y %H:%M")
                            else:
                                starts_at_str = str(starts_at)
                        else:
                            starts_at_str = "Не указано"
                        
                        location_str = location or "Не указано"
                        
                        if tournament_type == 'team':
                            msg = (
                                "🎾 Ты записан на турнир!\n\n"
                                f"🏷️ {title}\n"
                                f"📍 {location_str}\n"
                                f"🕒 {starts_at_str}\n"
                                f"💳 Цена: {price_rub} ₽ за пару\n"
                            )
                            keyboard_entry = InlineKeyboardMarkup([
                                [InlineKeyboardButton("Оплатить", callback_data=f"pay:{entry_id}")]
                            ])
                        else:
                            msg = (
                                "🎾 Ты записан на турнир!\n\n"
                                f"🏷️ {title}\n"
                                f"📍 {location_str}\n"
                                f"🕒 {starts_at_str}\n"
                                f"💳 {price_rub} ₽\n\n"
                            )
                            keyboard_entry = InlineKeyboardMarkup([
                                [InlineKeyboardButton("Оплатить", callback_data=f"pay:{entry_id}")]
                            ])
                        
                        await bot.send_message(chat_id=chat_id, text=msg, reply_markup=keyboard_entry)
                        
                        # Mark as notified
                        cur.execute("""
                            UPDATE entries
                            SET telegram_notified = true, telegram_notified_at = NOW()
                            WHERE id = %s
                        """, (entry_id,))
                        conn.commit()
                    except Exception as e:
                        print(f"BIND NOTIFICATION ERROR for entry {entry_id}: {str(e)}")
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"BIND CONFIRM ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # bind_back:* - навигация назад
        if data.startswith("bind_back:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                back_type = data.split(":", 1)[1]
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                if back_type == "menu":
                    # Back to menu - clear session
                    cur.execute("DELETE FROM telegram_sessions WHERE telegram_id = %s", (telegram_user_id,))
                    conn.commit()
                    
                    keyboard = ReplyKeyboardMarkup(
                        [
                            [KeyboardButton("Привязать аккаунт"), KeyboardButton("Помощь")]
                        ],
                        resize_keyboard=True
                    )
                    await bot.send_message(chat_id=chat_id, text="Привязка отменена.", reply_markup=keyboard)
                elif back_type == "date":
                    # Back to date selection
                    cur.execute("""
                        UPDATE telegram_sessions
                        SET state = 'bind_pick_date'
                        WHERE telegram_id = %s
                    """, (telegram_user_id,))
                    conn.commit()
                    
                    # Get dates again (using MSK timezone to match date selection)
                    cur.execute("""
                        SELECT DISTINCT DATE(starts_at AT TIME ZONE 'Europe/Moscow') AS tournament_date
                        FROM tournaments
                        WHERE archived_at IS NULL
                          AND starts_at >= NOW()
                        ORDER BY tournament_date ASC
                        LIMIT 10
                    """)
                    date_rows = cur.fetchall()
                    
                    buttons = []
                    for (date_obj,) in date_rows:
                        date_display, date_iso = fmt_date_ru(date_obj)
                        buttons.append([InlineKeyboardButton(date_display, callback_data=f"bind_date:{date_iso}")])
                    
                    buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="bind_back:menu")])
                    
                    keyboard = InlineKeyboardMarkup(buttons)
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Чтобы привязать аккаунт, выбери дату турнира, на который ты УЖЕ записан в Lunda. Это нужно только один раз.",
                        reply_markup=keyboard
                    )
                elif back_type == "tournament":
                    # Back to tournament selection - redirect to date selection since we don't store date in session
                    # User needs to select date again
                    cur.execute("""
                        SELECT DISTINCT DATE(starts_at AT TIME ZONE 'Europe/Moscow') AS tournament_date
                        FROM tournaments
                        WHERE archived_at IS NULL
                          AND starts_at >= NOW()
                        ORDER BY tournament_date ASC
                        LIMIT 10
                    """)
                    date_rows = cur.fetchall()
                    
                    if date_rows:
                        buttons = []
                        for (date_obj,) in date_rows:
                            date_display, date_iso = fmt_date_ru(date_obj)
                            buttons.append([InlineKeyboardButton(date_display, callback_data=f"bind_date:{date_iso}")])
                        
                        buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="bind_back:menu")])
                        
                        keyboard = InlineKeyboardMarkup(buttons)
                        await bot.send_message(
                            chat_id=chat_id,
                            text="Чтобы привязать аккаунт, выбери дату турнира, на который ты УЖЕ записан в Lunda. Это нужно только один раз.",
                            reply_markup=keyboard
                        )
                elif back_type.startswith("player:"):
                    # Back to player selection
                    tournament_id = int(back_type.split(":")[1])
                    
                    cur.execute("""
                        SELECT p.id, p.full_name
                        FROM entries e
                        JOIN players p ON e.player_id = p.id
                        WHERE e.tournament_id = %s
                          AND e.active = true
                        ORDER BY p.full_name ASC
                    """, (tournament_id,))
                    players = cur.fetchall()
                    
                    page = 0
                    players_per_page = 10
                    start_idx = page * players_per_page
                    end_idx = start_idx + players_per_page
                    page_players = players[start_idx:end_idx]
                    
                    buttons = []
                    for player_id, full_name in page_players:
                        buttons.append([InlineKeyboardButton(full_name, callback_data=f"bind_pick_player:{tournament_id}:{player_id}:{page}")])
                    
                    nav_buttons = []
                    if page > 0:
                        nav_buttons.append(InlineKeyboardButton("◀️", callback_data=f"bind_player_page:{tournament_id}:{page-1}"))
                    if end_idx < len(players):
                        nav_buttons.append(InlineKeyboardButton("▶️", callback_data=f"bind_player_page:{tournament_id}:{page+1}"))
                    if nav_buttons:
                        buttons.append(nav_buttons)
                    
                    buttons.append([InlineKeyboardButton("↩️ Назад", callback_data="bind_back:tournament")])
                    buttons.append([InlineKeyboardButton("Отмена", callback_data="bind_back:menu")])
                    
                    keyboard = InlineKeyboardMarkup(buttons)
                    await bot.send_message(
                        chat_id=chat_id,
                        text="Выбери себя из списка участников:",
                        reply_markup=keyboard
                    )
                
                cur.close()
                conn.close()
            except Exception as e:
                print(f"BIND BACK ERROR: {str(e)}")
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # pending_approve:<pending_id>:<player_id> - approve pending entry
        if data.startswith("pending_approve:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                parts = data.split(":")
                pending_id = int(parts[1])
                player_id = int(parts[2])
                
                from_user = callback_query.get("from", {})
                admin_telegram_id = str(from_user.get("id", "")) if from_user.get("id") else None
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get pending entry
                cur.execute("""
                    SELECT status, raw_player_name, normalized_name, payload
                    FROM pending_entries
                    WHERE id = %s
                """, (pending_id,))
                pending_row = cur.fetchone()
                
                if not pending_row:
                    await bot.send_message(chat_id=chat_id, text="Запрос не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                pending_status, raw_player_name, normalized_name, payload_json = pending_row
                
                if pending_status != 'pending':
                    if pending_status == 'expired':
                        await bot.answer_callback_query(callback_query["id"], text="Запрос устарел")
                    else:
                        await bot.answer_callback_query(callback_query["id"], text="Уже обработан")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Import normalize_name
                import sys
                import importlib.util
                spec = importlib.util.spec_from_file_location("import_lunda", "scripts/import_lunda.py")
                if spec and spec.loader:
                    import_lunda = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(import_lunda)
                    normalize_name = import_lunda.normalize_name
                else:
                    def normalize_name(s):
                        if not s:
                            return ""
                        import re
                        s = s.strip().lower().replace('ё', 'е')
                        s = re.sub(r'\s+', ' ', s)
                        return s
                
                # Create alias
                cur.execute("""
                    INSERT INTO player_aliases (alias_name, normalized_alias, player_id, created_by_telegram_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (normalized_alias) 
                    DO UPDATE SET player_id = EXCLUDED.player_id
                """, (raw_player_name, normalize_name(raw_player_name), player_id, admin_telegram_id))
                
                # Create entry from payload
                payload = json.loads(payload_json)
                tournament_id = payload['tournament_id']
                
                # Use upsert_entry logic (simplified)
                cur.execute("""
                    SELECT id FROM entries
                    WHERE tournament_id = %s AND player_id = %s
                """, (tournament_id, player_id))
                existing = cur.fetchone()
                
                if not existing:
                    # Create new entry
                    cur.execute("""
                        INSERT INTO entries (tournament_id, player_id, payment_status, active, first_seen_in_source, last_seen_in_source)
                        VALUES (%s, %s, 'pending', true, NOW(), NOW())
                        RETURNING id
                    """, (tournament_id, player_id))
                    entry_id = cur.fetchone()[0]
                else:
                    entry_id = existing[0]
                    # Update existing
                    cur.execute("""
                        UPDATE entries
                        SET active = true, last_seen_in_source = NOW()
                        WHERE id = %s
                    """, (entry_id,))
                
                # Update pending status
                cur.execute("""
                    UPDATE pending_entries
                    SET status = 'approved'
                    WHERE id = %s
                """, (pending_id,))
                
                conn.commit()
                cur.close()
                conn.close()
                
                await bot.send_message(chat_id=chat_id, text="✅ Привязал. Участие добавлено.")
                return {"ok": True}
            except Exception as e:
                print(f"PENDING APPROVE ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Попробуй ещё раз или нажми Отмена.")
            return {"ok": True}
        
        # pending_new_player:<pending_id> - create new player from pending entry
        if data.startswith("pending_new_player:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                pending_id = int(data.split(":")[1])
                
                from_user = callback_query.get("from", {})
                admin_telegram_id = str(from_user.get("id", "")) if from_user.get("id") else None
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get pending entry
                cur.execute("""
                    SELECT status, raw_player_name, normalized_name, payload
                    FROM pending_entries
                    WHERE id = %s
                """, (pending_id,))
                pending_row = cur.fetchone()
                
                if not pending_row:
                    await bot.send_message(chat_id=chat_id, text="Запрос не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                pending_status, raw_player_name, normalized_name, payload_json = pending_row
                
                if pending_status != 'pending':
                    if pending_status == 'expired':
                        await bot.answer_callback_query(callback_query["id"], text="Запрос устарел")
                    else:
                        await bot.answer_callback_query(callback_query["id"], text="Уже обработан")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Import normalize_name
                import sys
                import importlib.util
                spec = importlib.util.spec_from_file_location("import_lunda", "scripts/import_lunda.py")
                if spec and spec.loader:
                    import_lunda = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(import_lunda)
                    normalize_name = import_lunda.normalize_name
                else:
                    def normalize_name(s):
                        if not s:
                            return ""
                        import re
                        s = s.strip().lower().replace('ё', 'е')
                        s = re.sub(r'\s+', ' ', s)
                        return s
                
                # Create new player
                cur.execute("""
                    INSERT INTO players (full_name, normalized_name)
                    VALUES (%s, %s)
                    RETURNING id
                """, (raw_player_name, normalized_name))
                new_player_id = cur.fetchone()[0]
                
                # Create entry from payload
                payload = json.loads(payload_json)
                tournament_id = payload['tournament_id']
                
                cur.execute("""
                    INSERT INTO entries (tournament_id, player_id, payment_status, active, first_seen_in_source, last_seen_in_source)
                    VALUES (%s, %s, 'pending', true, NOW(), NOW())
                    RETURNING id
                """, (tournament_id, new_player_id))
                entry_id = cur.fetchone()[0]
                
                # Update pending status and save created_player_id
                cur.execute("""
                    UPDATE pending_entries
                    SET status = 'approved',
                        payload = jsonb_set(payload, '{created_player_id}', %s::text::jsonb)
                    WHERE id = %s
                """, (json.dumps(new_player_id), pending_id))
                
                conn.commit()
                cur.close()
                conn.close()
                
                await bot.send_message(chat_id=chat_id, text=f"✅ Создан новый игрок: {raw_player_name}. Участие добавлено.")
                return {"ok": True}
            except Exception as e:
                print(f"PENDING NEW PLAYER ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Смотри логи.")
                return {"ok": True}
        
        # pending_reject:<pending_id> - reject pending entry
        if data.startswith("pending_reject:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                pending_id = int(data.split(":")[1])
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Check status
                cur.execute("SELECT status FROM pending_entries WHERE id = %s", (pending_id,))
                row = cur.fetchone()
                
                if not row:
                    await bot.send_message(chat_id=chat_id, text="Запрос не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                if row[0] != 'pending':
                    if row[0] == 'expired':
                        await bot.answer_callback_query(callback_query["id"], text="Запрос устарел")
                    else:
                        await bot.answer_callback_query(callback_query["id"], text="Уже обработан")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Update status
                cur.execute("""
                    UPDATE pending_entries
                    SET status = 'rejected'
                    WHERE id = %s
                """, (pending_id,))
                
                conn.commit()
                cur.close()
                conn.close()
                
                await bot.send_message(chat_id=chat_id, text="Ок, пропустил.")
                return {"ok": True}
            except Exception as e:
                print(f"PENDING REJECT ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Смотри логи.")
                return {"ok": True}
        
        # bind_resolve_pending:<pending_id>:<player_id> - resolve pending by choosing existing player
        if data.startswith("bind_resolve_pending:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                parts = data.split(":")
                pending_id = int(parts[1])
                player_id = int(parts[2])
                
                from_user = callback_query.get("from", {})
                admin_telegram_id = str(from_user.get("id", "")) if from_user.get("id") else None
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get pending entry
                cur.execute("""
                    SELECT status, raw_player_name, normalized_name, payload, tournament_id
                    FROM pending_entries
                    WHERE id = %s
                """, (pending_id,))
                pending_row = cur.fetchone()
                
                if not pending_row:
                    await bot.send_message(chat_id=chat_id, text="Запрос не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                pending_status, raw_player_name, normalized_name, payload_json, tournament_id = pending_row
                
                if pending_status != 'pending':
                    await bot.answer_callback_query(callback_query["id"], text="Уже обработан")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Import normalize_name
                import sys
                import importlib.util
                spec = importlib.util.spec_from_file_location("import_lunda", "scripts/import_lunda.py")
                if spec and spec.loader:
                    import_lunda = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(import_lunda)
                    normalize_name = import_lunda.normalize_name
                else:
                    def normalize_name(s):
                        if not s:
                            return ""
                        import re
                        s = s.strip().lower().replace('ё', 'е')
                        s = re.sub(r'\s+', ' ', s)
                        return s
                
                # Create alias
                cur.execute("""
                    INSERT INTO player_aliases (alias_name, normalized_alias, player_id, created_by_telegram_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (normalized_alias) 
                    DO UPDATE SET player_id = EXCLUDED.player_id
                """, (raw_player_name, normalize_name(raw_player_name), player_id, admin_telegram_id))
                
                # Get player name for response
                cur.execute("SELECT full_name FROM players WHERE id = %s", (player_id,))
                player_row = cur.fetchone()
                player_full_name = player_row[0] if player_row else "Неизвестно"
                
                # Create entry if not exists
                cur.execute("""
                    SELECT id FROM entries
                    WHERE tournament_id = %s AND player_id = %s
                """, (tournament_id, player_id))
                existing = cur.fetchone()
                
                if not existing:
                    cur.execute("""
                        INSERT INTO entries (tournament_id, player_id, payment_status, active, first_seen_in_source, last_seen_in_source)
                        VALUES (%s, %s, 'pending', true, NOW(), NOW())
                        RETURNING id
                    """, (tournament_id, player_id))
                    entry_id = cur.fetchone()[0]
                else:
                    entry_id = existing[0]
                    cur.execute("""
                        UPDATE entries
                        SET active = true, last_seen_in_source = NOW()
                        WHERE id = %s
                    """, (entry_id,))
                
                # Update pending status
                cur.execute("""
                    UPDATE pending_entries
                    SET status = 'resolved',
                        resolved_at = NOW(),
                        resolved_player_id = %s
                    WHERE id = %s
                """, (player_id, pending_id))
                
                conn.commit()
                cur.close()
                conn.close()
                
                await bot.send_message(chat_id=chat_id, text=f"✅ Ок, привязал {raw_player_name} → {player_full_name}")
                return {"ok": True}
            except Exception as e:
                print(f"BIND RESOLVE PENDING ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Смотри логи.")
                return {"ok": True}
        
        # bind_resolve_pending_new:<pending_id> - resolve pending by creating new player
        if data.startswith("bind_resolve_pending_new:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                pending_id = int(data.split(":")[1])
                
                from_user = callback_query.get("from", {})
                admin_telegram_id = str(from_user.get("id", "")) if from_user.get("id") else None
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Get pending entry
                cur.execute("""
                    SELECT status, raw_player_name, normalized_name, tournament_id
                    FROM pending_entries
                    WHERE id = %s
                """, (pending_id,))
                pending_row = cur.fetchone()
                
                if not pending_row:
                    await bot.send_message(chat_id=chat_id, text="Запрос не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                pending_status, raw_player_name, normalized_name, tournament_id = pending_row
                
                if pending_status != 'pending':
                    await bot.answer_callback_query(callback_query["id"], text="Уже обработан")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Import normalize_name
                import sys
                import importlib.util
                spec = importlib.util.spec_from_file_location("import_lunda", "scripts/import_lunda.py")
                if spec and spec.loader:
                    import_lunda = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(import_lunda)
                    normalize_name = import_lunda.normalize_name
                else:
                    def normalize_name(s):
                        if not s:
                            return ""
                        import re
                        s = s.strip().lower().replace('ё', 'е')
                        s = re.sub(r'\s+', ' ', s)
                        return s
                
                # Create new player
                cur.execute("""
                    INSERT INTO players (full_name, normalized_name)
                    VALUES (%s, %s)
                    RETURNING id
                """, (raw_player_name, normalized_name))
                new_player_id = cur.fetchone()[0]
                
                # Create alias
                cur.execute("""
                    INSERT INTO player_aliases (alias_name, normalized_alias, player_id, created_by_telegram_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (normalized_alias) 
                    DO UPDATE SET player_id = EXCLUDED.player_id
                """, (raw_player_name, normalize_name(raw_player_name), new_player_id, admin_telegram_id))
                
                # Create entry
                cur.execute("""
                    INSERT INTO entries (tournament_id, player_id, payment_status, active, first_seen_in_source, last_seen_in_source)
                    VALUES (%s, %s, 'pending', true, NOW(), NOW())
                    RETURNING id
                """, (tournament_id, new_player_id))
                entry_id = cur.fetchone()[0]
                
                # Update pending status
                cur.execute("""
                    UPDATE pending_entries
                    SET status = 'resolved',
                        resolved_at = NOW(),
                        resolved_player_id = %s
                    WHERE id = %s
                """, (new_player_id, pending_id))
                
                conn.commit()
                cur.close()
                conn.close()
                
                await bot.send_message(chat_id=chat_id, text=f"✅ Создан новый игрок: {raw_player_name}. Участие добавлено.")
                return {"ok": True}
            except Exception as e:
                print(f"BIND RESOLVE PENDING NEW ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Смотри логи.")
                return {"ok": True}
        
        # bind_resolve_pending_skip:<pending_id> - snooze pending entry
        if data.startswith("bind_resolve_pending_skip:"):
            try:
                await bot.answer_callback_query(callback_query["id"])
                pending_id = int(data.split(":")[1])
                
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    await bot.send_message(chat_id=chat_id, text="Ошибка: база данных не настроена.")
                    return {"ok": True}
                
                conn = psycopg2.connect(database_url, sslmode="require")
                cur = conn.cursor()
                
                # Check status
                cur.execute("SELECT status FROM pending_entries WHERE id = %s", (pending_id,))
                row = cur.fetchone()
                
                if not row:
                    await bot.send_message(chat_id=chat_id, text="Запрос не найден.")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                if row[0] != 'pending':
                    await bot.answer_callback_query(callback_query["id"], text="Уже обработан")
                    cur.close()
                    conn.close()
                    return {"ok": True}
                
                # Update status to snoozed
                cur.execute("""
                    UPDATE pending_entries
                    SET status = 'snoozed',
                        notified_at = NOW()
                    WHERE id = %s
                """, (pending_id,))
                
                conn.commit()
                cur.close()
                conn.close()
                
                await bot.send_message(chat_id=chat_id, text="⏸ Отложено. Можно будет разрешить позже.")
                return {"ok": True}
            except Exception as e:
                print(f"BIND RESOLVE PENDING SKIP ERROR: {str(e)}")
                import traceback
                traceback.print_exc()
                await bot.answer_callback_query(callback_query["id"], text="Ошибка. Смотри логи.")
                return {"ok": True}

    return {"ok": True}

    from fastapi import Query
from datetime import datetime

@app.get("/admin/last-sync")
def get_last_sync():
    """Get last sync run information from sync_runs table."""
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL not set"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        cur.execute("""
            SELECT 
                id,
                source,
                started_at,
                finished_at,
                tournaments_upsert,
                players_upsert,
                entries_new,
                entries_existing,
                entries_deleted,
                entries_inactivated,
                error,
                json_path,
                json_mtime
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT 1
        """)
        
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        if not row:
            return {"ok": True, "message": "No sync runs found"}
        
        return {
            "ok": True,
            "id": row[0],
            "source": row[1],
            "started_at": row[2].isoformat() if row[2] else None,
            "finished_at": row[3].isoformat() if row[3] else None,
            "tournaments_upsert": row[4],
            "players_upsert": row[5],
            "entries_new": row[6],
            "entries_existing": row[7],
            "entries_deleted": row[8],
            "entries_inactivated": row[9],
            "error": row[10],
            "json_path": row[11],
            "json_mtime": row[12].isoformat() if row[12] else None
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/admin/process-new-entries")
async def process_new_entries(limit: int = Query(50, ge=1, le=500)):
    """
    Находит entries, которым нужно создать вечную ссылку на оплату.
    НЕ создает YooKassa payments массово - только сохраняет вечные ссылки.
    Если у игрока есть telegram_id — отправляет сообщение.
    limit — защита от массовых ошибочных созданий.
    """
    # 1. Диагностика в начале endpoint
    print(f"PROCESS_NEW_ENTRIES start, limit={limit}")
    bot_token_present = bool(os.getenv("TELEGRAM_BOT_TOKEN"))
    print(f"BOT_TOKEN present? {bot_token_present}")
    print(f"bot is None? {bot is None}")
    admin_telegram_id = os.getenv("ADMIN_TELEGRAM_ID")
    print(f"ADMIN_TELEGRAM_ID={admin_telegram_id if admin_telegram_id else 'not set'}")
    public_base_url = os.getenv("PUBLIC_BASE_URL")
    print(f"PUBLIC_BASE_URL={public_base_url if public_base_url else 'not set'}")
    
    if not public_base_url:
        return {"ok": False, "error": "PUBLIC_BASE_URL not set. Please set it in environment variables."}
    
    conn = get_db()
    cur = conn.cursor()

    # Выбираем entries, которым нужно создать вечную ссылку
    cur.execute("""
        select
          e.id as entry_id,
          e.player_id,
          e.payment_status,
          e.telegram_notified,
          e.payment_url,
          e.active,
          t.title,
          t.starts_at,
          t.price_rub,
          t.tournament_type,
          t.location,
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

    # 2. После SQL выборки
    print(f"SELECT count={len(rows)}")

    processed = 0
    notified = 0
    details = []

    for (entry_id, player_id, payment_status, telegram_notified, payment_url, active, title, starts_at, price_rub, tournament_type, location, full_name, telegram_id) in rows:
        # 3. В цикле для каждой записи - одна строка лога
        print(f"ENTRY: entry_id={entry_id}, player_id={player_id}, telegram_id={telegram_id}, payment_status={payment_status}, telegram_notified={telegram_notified}, active={active}, payment_url={payment_url}")
        
        # Создаем вечную ссылку вместо YooKassa payment
        # Для team турниров по умолчанию 50%, для personal - 100%
        if tournament_type == 'team':
            permanent_link = f"{public_base_url}/p/e/{entry_id}?pay=half"
        else:
            permanent_link = f"{public_base_url}/p/e/{entry_id}"

        # Записываем вечную ссылку в entries (payment_id и payment_url остаются NULL до реальной оплаты)
        cur.execute("""
            update entries
            set payment_url = %s
            where id = %s
        """, (permanent_link, entry_id))
        conn.commit()
        processed += 1

        # Инициализируем детали для этой entry
        entry_detail = {
            "entry_id": entry_id,
            "player_id": player_id,
            "telegram_id": str(telegram_id) if telegram_id else None,
            "status": None,
            "reason": None,
            "payment_url": permanent_link
        }

        # Определяем статус уведомления
        # Проверка случаев пропуска
        if not active:
            entry_detail["status"] = "skipped"
            entry_detail["reason"] = "inactive"
            print(f"ENTRY {entry_id}: action=skipped, reason=inactive")
        elif not telegram_id:
            entry_detail["status"] = "skipped"
            entry_detail["reason"] = "no_telegram_id"
            print(f"ENTRY {entry_id}: action=skipped, reason=no_telegram_id")
        elif telegram_notified:
            entry_detail["status"] = "skipped"
            entry_detail["reason"] = "already_notified"
            print(f"ENTRY {entry_id}: action=skipped, reason=already_notified")
        elif bot is None or not bot_token_present:
            entry_detail["status"] = "error"
            entry_detail["reason"] = "bot_not_configured"
            print(f"ENTRY {entry_id}: action=error, reason=bot_not_configured")
        else:
            # Пытаемся отправить уведомление
            try:
                chat_id = int(telegram_id)
                print(f"ENTRY {entry_id}: action=send, telegram_id={telegram_id}")

                # Format starts_at (without MSK suffix)
                if starts_at:
                    if isinstance(starts_at, datetime):
                        if starts_at.tzinfo is None:
                            starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                        else:
                            starts_at_utc = starts_at.astimezone(timezone.utc)
                        starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                        starts_at_str = starts_at_msk.strftime("%d.%m.%Y %H:%M")
                    else:
                        starts_at_str = str(starts_at)
                else:
                    starts_at_str = "Не указано"
                
                # Get location
                location_str = location or "Не указано"
                
                # Используем вечную ссылку
                # Для team турниров по умолчанию 50%, для personal - 100%
                if tournament_type == 'team':
                    permanent_link = f"{public_base_url}/p/e/{entry_id}?pay=half"
                else:
                    permanent_link = f"{public_base_url}/p/e/{entry_id}"
                
                # Формируем сообщение в зависимости от типа турнира
                if tournament_type == 'team':
                    # Team tournament - не указываем сумму, показываем кнопку "Оплатить" с callback
                    msg = (
                        "🎾 Ты записан на турнир!\n\n"
                        f"🏷️ {title}\n"
                        f"📍 {location_str}\n"
                        f"🕒 {starts_at_str}\n"
                        f"💳 Цена: {price_rub} ₽ за пару\n"
                    )
                    
                    # Создаем inline keyboard с кнопкой "Оплатить" (callback для выбора 50%/100%)
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Оплатить", callback_data=f"pay:{entry_id}")
                        ]
                    ])
                    
                    await bot.send_message(chat_id=chat_id, text=msg, reply_markup=keyboard)
                else:
                    # Personal tournament - показываем сумму и кнопку "Оплатить" с callback
                    msg = (
                        "🎾 Ты записан на турнир!\n\n"
                        f"🏷️ {title}\n"
                        f"📍 {location_str}\n"
                        f"🕒 {starts_at_str}\n"
                        f"💳 {price_rub} ₽\n\n"
                    )
                    
                    # Создаем inline keyboard с кнопкой "Оплатить" (callback для personal)
                    keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton("Оплатить", callback_data=f"pay:{entry_id}")
                        ]
                    ])
                    
                    await bot.send_message(chat_id=chat_id, text=msg, reply_markup=keyboard)

                # Обновляем telegram_notified после успешной отправки
                cur.execute("""
                    update entries
                    set telegram_notified = true,
                        telegram_notified_at = now()
                    where id = %s
                """, (entry_id,))
                conn.commit()

                # 5. После успешной отправки
                entry_detail["status"] = "sent"
                entry_detail["reason"] = None
                print(f"ENTRY {entry_id}: action=sent")
                notified += 1
            except Exception as e:
                # 6. Сохраняем ошибку в детали
                error_msg = str(e)
                entry_detail["status"] = "error"
                entry_detail["reason"] = error_msg
                print(f"ENTRY {entry_id}: action=error, reason={error_msg}")
                print("TG ERROR:", traceback.format_exc())
        
        # Добавляем детали в массив
        details.append(entry_detail)

    cur.close()
    conn.close()

    return {
        "ok": True,
        "processed": processed,
        "notified": notified,
        "details": details
    }

@app.post("/admin/process-pending-players")
async def process_pending_players(limit: int = Query(50, ge=1, le=200)):
    """
    Находит pending_entries со status='pending' и notified_at IS NULL,
    отправляет админу Telegram уведомления с кнопками для разрешения.
    """
    print(f"PROCESS_PENDING_PLAYERS start, limit={limit}")
    
    if bot is None:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN not set"}
    
    admin_telegram_id = os.getenv("ADMIN_TELEGRAM_ID")
    if not admin_telegram_id:
        return {"ok": False, "error": "ADMIN_TELEGRAM_ID not set"}
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return {"ok": False, "error": "DATABASE_URL not set"}
    
    try:
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        
        # Get pending entries that haven't been notified
        cur.execute("""
            SELECT 
                pe.id,
                pe.tournament_id,
                pe.raw_player_name,
                pe.normalized_name,
                pe.candidates,
                t.title,
                t.starts_at,
                t.location
            FROM pending_entries pe
            JOIN tournaments t ON t.id = pe.tournament_id
            WHERE pe.status = 'pending'
              AND pe.notified_at IS NULL
            ORDER BY pe.created_at ASC
            LIMIT %s
        """, (limit,))
        
        rows = cur.fetchall()
        notified_count = 0
        
        for row in rows:
            pending_id, tournament_id, raw_name, normalized_name, candidates_json, tournament_title, starts_at, location = row
            
            try:
                # Parse candidates
                candidates = json.loads(candidates_json) if candidates_json else []
                
                # Format starts_at
                if starts_at:
                    if isinstance(starts_at, datetime):
                        if starts_at.tzinfo is None:
                            starts_at_utc = starts_at.replace(tzinfo=timezone.utc)
                        else:
                            starts_at_utc = starts_at.astimezone(timezone.utc)
                        starts_at_msk = starts_at_utc.astimezone(BOT_TZ)
                        starts_at_str = starts_at_msk.strftime("%d.%m.%Y %H:%M")
                    else:
                        starts_at_str = str(starts_at)
                else:
                    starts_at_str = "Не указано"
                
                location_str = location or "Не указано"
                
                # Build message
                message = f"""⚠️ Спорное имя: {raw_name}
Турнир: {tournament_title} ({starts_at_str})
Место: {location_str}

Выбери правильного игрока:"""
                
                # Build buttons (max 8 candidates)
                buttons = []
                for cand in candidates[:8]:
                    player_name = cand.get('name', 'Неизвестно')
                    player_id = cand.get('player_id')
                    if player_id:
                        buttons.append([InlineKeyboardButton(
                            player_name,
                            callback_data=f"bind_resolve_pending:{pending_id}:{player_id}"
                        )])
                
                # Add action buttons
                buttons.append([InlineKeyboardButton(
                    "➕ Это новый игрок",
                    callback_data=f"bind_resolve_pending_new:{pending_id}"
                )])
                buttons.append([InlineKeyboardButton(
                    "⏸ Отложить",
                    callback_data=f"bind_resolve_pending_skip:{pending_id}"
                )])
                
                keyboard = InlineKeyboardMarkup(buttons)
                
                # Send message to admin
                result = await bot.send_message(
                    chat_id=admin_telegram_id,
                    text=message,
                    reply_markup=keyboard
                )
                
                # Update notified_at
                cur.execute("""
                    UPDATE pending_entries
                    SET notified_at = NOW()
                    WHERE id = %s
                """, (pending_id,))
                conn.commit()
                
                notified_count += 1
                print(f"PENDING NOTIFIED: pending_id={pending_id}, raw_name={raw_name}, candidates={len(candidates)}")
                
            except Exception as e:
                print(f"ERROR notifying pending {pending_id}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        cur.close()
        conn.close()
        
        print(f"PROCESS_PENDING_PLAYERS: found={len(rows)}, notified={notified_count}")
        
        return {
            "ok": True,
            "found": len(rows),
            "notified": notified_count
        }
        
    except Exception as e:
        print(f"PROCESS_PENDING_PLAYERS ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"ok": False, "error": str(e)}