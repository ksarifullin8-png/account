import asyncio
import logging
import sqlite3
import random
import string
import re
import os
import requests
import sys
import traceback
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Callable, Awaitable

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, PreCheckoutQuery, FSInputFile
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError
)

# ==================== НАСТРОЙКИ ====================
TOKEN = "8561605758:AAFOFA3pT3TTxzMQXWS8GxZXWGBKdlp9KpU"
CRYPTOBOT_TOKEN = "546557:AAA5MxwCASiCnPAQOnZ6cNkbhgnirFIrxhU"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
ADMIN_IDS = [7546928092]

API_ID = 35800959
API_HASH = "708e7d0bc3572355bcaf68562cc068f1"

STARS_RATE = 1.4
USDT_RATE = 70

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создаем бота с HTML-форматированием
bot = Bot(
    token=TOKEN, 
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot_username = None

# Создаем папку для сессий
if not os.path.exists('sessions'):
    os.makedirs('sessions')

# Временные хранилища для Telethon
temp_clients: Dict[str, TelegramClient] = {}
active_sessions: Dict[str, str] = {}

# ==================== БЕЗОПАСНОЕ РЕДАКТИРОВАНИЕ СООБЩЕНИЙ ====================
async def safe_edit_message(message, new_text, reply_markup=None):
    """Безопасно редактирует сообщение, избегая ошибки 'message is not modified'"""
    try:
        # Если сообщение - это callback, берем message.message
        if hasattr(message, 'message'):
            msg = message.message
        else:
            msg = message
        
        # Проверяем, изменилось ли что-то
        current_text = msg.text
        current_markup = msg.reply_markup
        
        if current_text == new_text and current_markup == reply_markup:
            return msg
        
        # Редактируем
        return await msg.edit_text(new_text, reply_markup=reply_markup)
    except Exception as e:
        if "message is not modified" not in str(e):
            logger.error(f"Ошибка редактирования: {e}")
        return message

# ==================== СОСТОЯНИЯ FSM ====================
class ProductStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_phone = State()
    waiting_for_account_password = State()
    waiting_for_code = State()
    waiting_for_password = State()

class PaymentStates(StatesGroup):
    waiting_for_stars_amount = State()
    waiting_for_sbp_amount = State()
    waiting_for_crypto_amount = State()

class AdminPaymentStates(StatesGroup):
    waiting_for_payment_details = State()

class AdminAddBalanceStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_amount = State()

class AdminSettingsStates(StatesGroup):
    waiting_for_stars = State()
    waiting_for_usdt = State()
    waiting_for_discount = State()
    waiting_for_reward = State()
    waiting_for_reviews_channel = State()

class MailingStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirm = State()

# ==================== БАЗА ДАННЫХ ====================
def init_db():
    """Инициализация базы данных (сохраняет существующие данные)"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    # Таблица пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        balance REAL DEFAULT 0,
        registered_date TEXT,
        referrer_id INTEGER DEFAULT NULL,
        referral_code TEXT UNIQUE,
        first_discount_used INTEGER DEFAULT 0,
        total_referrals INTEGER DEFAULT 0,
        total_referral_earnings REAL DEFAULT 0
    )''')
    
    # Таблица товаров (с паролем)
    c.execute('''CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        phone TEXT,
        session_string TEXT,
        region TEXT,
        account_year INTEGER,
        added_date TEXT,
        password TEXT
    )''')
    
    # Таблица покупок (с паролем)
    c.execute('''CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        product_id INTEGER,
        price REAL,
        purchase_date TEXT,
        phone TEXT,
        session_string TEXT,
        region TEXT,
        account_year INTEGER,
        password TEXT
    )''')
    
    # Таблица кодов
    c.execute('''CREATE TABLE IF NOT EXISTS account_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        code TEXT,
        received_date TEXT,
        message_text TEXT
    )''')
    
    # Таблица ожидающих платежей
    c.execute('''CREATE TABLE IF NOT EXISTS pending_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        method TEXT,
        status TEXT DEFAULT 'pending',
        created_date TEXT,
        invoice_id TEXT
    )''')
    
    # Таблица настроек
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    
    # Таблица забаненных пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS banned_users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        ban_reason TEXT,
        banned_date TEXT,
        banned_by INTEGER
    )''')
    
    # Таблица для логирования действий
    c.execute('''CREATE TABLE IF NOT EXISTS user_actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT,
        timestamp TEXT
    )''')
    
    # Настройки по умолчанию (не перезаписывают существующие)
    default_settings = [
        ('stars_rate', str(STARS_RATE)),
        ('usdt_rate', str(USDT_RATE)),
        ('referral_discount', '10'),
        ('referral_reward', '5'),
        ('reviews_channel_link', 'https://t.me/+UuMm3vm8C69mNTdi')
    ]
    
    for key, value in default_settings:
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
    
    conn.commit()
    conn.close()
    logger.info("✅ База данных инициализирована (существующие данные сохранены)")

# Инициализация БД
init_db()

# ==================== ФУНКЦИИ ДЛЯ РАБОТЫ С БАНАМИ ====================
def ban_user(user_id: int, reason: str = "Спам", admin_id: int = None):
    """Блокировка пользователя"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute("SELECT username FROM users WHERE user_id = ?", (user_id,))
    user = c.fetchone()
    username = user[0] if user else None
    
    c.execute("INSERT OR REPLACE INTO banned_users (user_id, username, ban_reason, banned_date, banned_by) VALUES (?, ?, ?, ?, ?)",
              (user_id, username, reason, now, admin_id))
    conn.commit()
    conn.close()
    logger.info(f"🚫 Пользователь {user_id} забанен. Причина: {reason}")

def unban_user(user_id: int):
    """Разблокировка пользователя"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("DELETE FROM banned_users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
    logger.info(f"✅ Пользователь {user_id} разбанен")

def is_banned(user_id: int) -> bool:
    """Проверка, забанен ли пользователь"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM banned_users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result is not None

def get_banned_users() -> List[Tuple]:
    """Получить список забаненных"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT user_id, username, ban_reason, banned_date FROM banned_users ORDER BY banned_date DESC")
    users = c.fetchall()
    conn.close()
    return users

def log_user_action(user_id: int, action: str):
    """Логирование действий пользователя"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO user_actions (user_id, action, timestamp) VALUES (?, ?, ?)",
              (user_id, action, now))
    conn.commit()
    conn.close()

async def auto_ban_spammer(user_id: int, username: str = None):
    """Автоматический бан спамера"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    time_limit = (datetime.now() - timedelta(seconds=30)).strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute("SELECT COUNT(*) FROM user_actions WHERE user_id = ? AND timestamp > ?",
              (user_id, time_limit))
    actions_count = c.fetchone()[0]
    conn.close()
    
    if actions_count > 50:
        ban_user(user_id, "Автоматический бан за спам (50+ действий за 30 секунд)")
        logger.warning(f"🤖 Автоматически забанен спамер {user_id} ({username}) - {actions_count} действий")
        
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"🚨 <b>АВТОМАТИЧЕСКИЙ БАН СПАМЕРА!</b>\n\n"
                    f"👤 ID: <code>{user_id}</code>\n"
                    f"👤 Username: @{username or 'Нет'}\n"
                    f"📊 Действий за 30 сек: <b>{actions_count}</b>"
                )
            except:
                pass
        return True
    return False

# ==================== MIDDLEWARE ДЛЯ ПРОВЕРКИ БАНОВ ====================
class BanCheckMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[types.TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user_id = None
        if hasattr(event, 'from_user') and event.from_user:
            user_id = event.from_user.id
        elif hasattr(event, 'message') and event.message and event.message.from_user:
            user_id = event.message.from_user.id
        elif hasattr(event, 'callback_query') and event.callback_query and event.callback_query.from_user:
            user_id = event.callback_query.from_user.id
        
        if user_id and is_banned(user_id):
            logger.info(f"🚫 Забаненный пользователь {user_id} попытался что-то сделать")
            
            if hasattr(event, 'message') and event.message:
                await event.message.answer("⛔ ВЫ ЗАБЛОКИРОВАНЫ ЗА СПАМ!")
            elif hasattr(event, 'callback_query') and event.callback_query:
                await event.callback_query.answer("⛔ ВЫ ЗАБЛОКИРОВАНЫ", show_alert=True)
            
            return
        
        return await handler(event, data)

# Подключаем middleware
dp.message.middleware(BanCheckMiddleware())
dp.callback_query.middleware(BanCheckMiddleware())

# ==================== ФУНКЦИИ РЕФЕРАЛЬНОЙ СИСТЕМЫ ====================
def generate_referral_code(user_id: int) -> str:
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{user_id}{random_part}"

def get_user(user_id: int, username: str = None, referrer_id: int = None) -> Optional[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = c.fetchone()
    
    if not user and username:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        referral_code = generate_referral_code(user_id)
        first_discount = 0 if referrer_id else 1
        
        c.execute("""INSERT INTO users 
                     (user_id, username, registered_date, referrer_id, referral_code, first_discount_used)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (user_id, username, now, referrer_id, referral_code, first_discount))
        
        if referrer_id:
            c.execute("UPDATE users SET total_referrals = total_referrals + 1 WHERE user_id = ?", (referrer_id,))
        
        conn.commit()
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = c.fetchone()
    
    conn.close()
    return user

def get_user_by_referral_code(code: str) -> Optional[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE referral_code = ?", (code,))
    user = c.fetchone()
    conn.close()
    return user

def can_use_discount(user_id: int) -> bool:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT first_discount_used, referrer_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return bool(result and result[0] == 0 and result[1] is not None)

def apply_first_discount(user_id: int):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET first_discount_used = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def get_referral_stats(user_id: int) -> Dict:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT username, registered_date FROM users WHERE referrer_id = ?", (user_id,))
    referrals = c.fetchall()
    c.execute("SELECT total_referrals, total_referral_earnings FROM users WHERE user_id = ?", (user_id,))
    stats = c.fetchone()
    conn.close()
    return {
        'referrals': referrals,
        'total_count': stats[0] if stats else 0,
        'total_earnings': stats[1] if stats else 0
    }

def get_all_users() -> List[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT user_id, username FROM users ORDER BY user_id")
    users = c.fetchall()
    conn.close()
    return users

# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================
def get_setting(key: str) -> Any:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT value FROM settings WHERE key = ?", (key,))
    result = c.fetchone()
    conn.close()
    
    if result is None:
        return None
    
    if key in ['stars_rate', 'usdt_rate', 'referral_discount', 'referral_reward']:
        return float(result[0])
    return result[0]

def update_setting(key: str, value: Any):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE settings SET value = ? WHERE key = ?", (str(value), key))
    conn.commit()
    conn.close()

def get_balance(user_id: int) -> float:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else 0

def update_balance(user_id: int, amount: float):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()

def add_referral_earning(user_id: int, amount: float, from_user_id: int):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ?, total_referral_earnings = total_referral_earnings + ? WHERE user_id = ?",
              (amount, amount, user_id))
    conn.commit()
    conn.close()

# ==================== ФУНКЦИИ ТОВАРОВ ====================
def get_products() -> List[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products ORDER BY id DESC")
    products = c.fetchall()
    conn.close()
    return products

def get_product(product_id: int) -> Optional[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    product = c.fetchone()
    conn.close()
    return product

def add_product(name: str, price: float, phone: str, session_string: str, region: str, year: int, password: str = None) -> int:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    year = int(year) if year else datetime.now().year
    
    c.execute("""INSERT INTO products (name, price, phone, session_string, region, account_year, added_date, password)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (name, price, phone, session_string, region, year, now, password))
    product_id = c.lastrowid
    conn.commit()
    conn.close()
    return product_id

def delete_product(product_id: int):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("DELETE FROM products WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()

# ==================== ФУНКЦИИ ПОКУПОК ====================
def add_purchase(user_id: int, product_id: int, price: float, phone: str, session_string: str, region: str, year: int, password: str = None) -> int:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO purchases 
                 (user_id, product_id, price, purchase_date, phone, session_string, region, account_year, password)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (user_id, product_id, price, now, phone, session_string, region, year, password))
    purchase_id = c.lastrowid
    conn.commit()
    conn.close()
    return purchase_id

def get_user_purchases(user_id: int) -> List[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE user_id = ? ORDER BY purchase_date DESC", (user_id,))
    purchases = c.fetchall()
    conn.close()
    return purchases

def get_purchase(purchase_id: int) -> Optional[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE id = ?", (purchase_id,))
    purchase = c.fetchone()
    conn.close()
    return purchase

# ==================== ФУНКЦИИ КОДОВ ====================
def save_code(phone: str, code: str, message_text: str, received_date: str = None):
    if received_date is None:
        received_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("INSERT INTO account_codes (phone, code, received_date, message_text) VALUES (?, ?, ?, ?)",
              (phone, code, received_date, message_text[:200]))
    conn.commit()
    conn.close()

async def get_live_codes_from_account(session_string: str, limit: int = 20) -> List[Dict]:
    codes = []
    client = None
    try:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            return codes
        
        async for message in client.iter_messages(None, limit=200):
            if not message.text:
                continue
            
            found_codes = re.findall(r'\b(\d{4,8})\b', message.text)
            for code in found_codes:
                text_lower = message.text.lower()
                if any(word in text_lower for word in ['2fa', 'пароль', 'password']):
                    code_type = "🔒 2FA"
                else:
                    code_type = "🔐 Telegram"
                
                msg_date = message.date.strftime("%d.%m %H:%M")
                codes.append({
                    'code': code,
                    'type': code_type,
                    'date': msg_date,
                    'text': message.text[:50]
                })
                
                if len(codes) >= limit:
                    break
            if len(codes) >= limit:
                break
        
        await client.disconnect()
    except Exception as e:
        logger.error(f"Error getting live codes: {e}")
    finally:
        if client and client.is_connected():
            await client.disconnect()
    return codes

# ==================== ФУНКЦИИ ПЛАТЕЖЕЙ ====================
def add_pending_payment(user_id: int, amount: float, method: str, invoice_id: str = None) -> int:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO pending_payments (user_id, amount, method, status, created_date, invoice_id) VALUES (?, ?, ?, ?, ?, ?)",
              (user_id, amount, method, 'pending', now, invoice_id))
    payment_id = c.lastrowid
    conn.commit()
    conn.close()
    return payment_id

def get_pending_payment(payment_id: int) -> Optional[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM pending_payments WHERE id = ?", (payment_id,))
    payment = c.fetchone()
    conn.close()
    return payment

def update_payment_status(payment_id: int, status: str):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE pending_payments SET status = ? WHERE id = ?", (status, payment_id))
    conn.commit()
    conn.close()

def get_pending_payments_by_status(status: str = 'pending') -> List[Tuple]:
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM pending_payments WHERE status = ?", (status,))
    payments = c.fetchall()
    conn.close()
    return payments

# ==================== TELEGRAM AUTH ====================
async def detect_region(phone: str) -> str:
    if phone.startswith('+7') or phone.startswith('7'):
        return '🇷🇺 Россия'
    elif phone.startswith('+380') or phone.startswith('380'):
        return '🇺🇦 Украина'
    elif phone.startswith('+1'):
        return '🇺🇸 США/Канада'
    elif phone.startswith('+44'):
        return '🇬🇧 Великобритания'
    elif phone.startswith('+49'):
        return '🇩🇪 Германия'
    elif phone.startswith('+33'):
        return '🇫🇷 Франция'
    elif phone.startswith('+39'):
        return '🇮🇹 Италия'
    elif phone.startswith('+34'):
        return '🇪🇸 Испания'
    elif phone.startswith('+86'):
        return '🇨🇳 Китай'
    elif phone.startswith('+81'):
        return '🇯🇵 Япония'
    elif phone.startswith('+82'):
        return '🇰🇷 Южная Корея'
    elif phone.startswith('+91'):
        return '🇮🇳 Индия'
    elif phone.startswith('+55'):
        return '🇧🇷 Бразилия'
    elif phone.startswith('+52'):
        return '🇲🇽 Мексика'
    elif phone.startswith('+61'):
        return '🇦🇺 Австралия'
    elif phone.startswith('+57'):
        return '🇸🇪 Швеция'
    elif phone.startswith('+49'):
        return '🇵🇱 Польша'
    else:
        return '🌍 Другая страна'

async def login_to_telegram(phone: str) -> Dict[str, Any]:
    try:
        logger.info(f"🔄 Начинаем вход для номера: {phone}")
        
        phone = re.sub(r'[^\d+]', '', phone)
        if not phone.startswith('+'):
            phone = '+' + phone
        
        logger.info(f"📱 Обработанный номер: {phone}")
        
        # Проверяем активную сессию
        if phone in active_sessions:
            logger.info(f"🔑 Найдена активная сессия для {phone}")
            session_string = active_sessions[phone]
            client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
                logger.info(f"✅ Аккаунт уже авторизован: {me.phone}")
                region = await detect_region(phone)
                year = getattr(me, 'date', None)
                year = year.year if year and hasattr(year, 'year') else datetime.now().year
                temp_clients[phone] = client
                return {
                    'success': True,
                    'session': session_string,
                    'region': region,
                    'year': year,
                    'already_logged': True,
                    'phone': phone,
                    'client': client
                }
        
        # Создаем новую сессию
        logger.info("🆕 Создаем новую сессию")
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            logger.info(f"✅ Уже авторизован: {me.phone}")
            session_string = client.session.save()
            region = await detect_region(phone)
            year = getattr(me, 'date', None)
            year = year.year if year and hasattr(year, 'year') else datetime.now().year
            active_sessions[phone] = session_string
            temp_clients[phone] = client
            return {
                'success': True,
                'session': session_string,
                'region': region,
                'year': year,
                'already_logged': True,
                'phone': phone,
                'client': client
            }
        else:
            logger.info(f"📱 Отправляем код на {phone}")
            await client.send_code_request(phone)
            temp_clients[phone] = client
            logger.info("✅ Код отправлен, ожидаем ввод")
            return {'success': True, 'need_code': True, 'phone': phone, 'client': client}
    
    except Exception as e:
        logger.error(f"❌ Ошибка входа: {e}")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

async def verify_code(phone: str, code: str) -> Dict[str, Any]:
    try:
        logger.info(f"\n🔍 VERIFY_CODE: Проверка кода для {phone}")
        logger.info(f"🔍 VERIFY_CODE: Код: {code}")
        
        client = temp_clients.get(phone)
        if not client:
            logger.error(f"❌ VERIFY_CODE: Клиент не найден!")
            logger.info(f"🔍 VERIFY_CODE: Доступные ключи: {list(temp_clients.keys())}")
            return {'success': False, 'error': '❌ Сессия истекла'}
        
        logger.info(f"✅ VERIFY_CODE: Клиент найден")
        
        if not client.is_connected():
            logger.info(f"🔄 VERIFY_CODE: Клиент не подключен, подключаем...")
            await client.connect()
        
        logger.info(f"🔍 VERIFY_CODE: Отправляем код...")
        await client.sign_in(code=code)
        logger.info(f"✅ VERIFY_CODE: Код принят!")
        
        me = await client.get_me()
        logger.info(f"✅ VERIFY_CODE: Аккаунт: {me.phone}")
        
        session_string = client.session.save()
        region = await detect_region(phone)
        year = getattr(me, 'date', None)
        year = year.year if year and hasattr(year, 'year') else datetime.now().year
        
        active_sessions[phone] = session_string
        logger.info(f"✅ VERIFY_CODE: Сессия сохранена")
        
        return {
            'success': True,
            'session': session_string,
            'region': region,
            'year': year,
            'phone': phone,
            'client': client
        }
    
    except SessionPasswordNeededError:
        logger.info(f"🔐 VERIFY_CODE: Требуется 2FA пароль")
        return {'success': True, 'need_password': True, 'phone': phone}
    
    except PhoneCodeInvalidError:
        logger.error(f"❌ VERIFY_CODE: Неверный код")
        return {'success': False, 'error': '❌ Неверный код'}
    
    except Exception as e:
        logger.error(f"❌ VERIFY_CODE: Ошибка: {e}")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

async def verify_password(phone: str, password: str) -> Dict[str, Any]:
    try:
        client = temp_clients.get(phone)
        if not client:
            return {'success': False, 'error': '❌ Сессия истекла'}
        
        await client.sign_in(password=password)
        me = await client.get_me()
        session_string = client.session.save()
        region = await detect_region(phone)
        year = getattr(me, 'date', None)
        year = year.year if year and hasattr(year, 'year') else datetime.now().year
        
        active_sessions[phone] = session_string
        
        return {
            'success': True,
            'session': session_string,
            'region': region,
            'year': year,
            'phone': phone,
            'client': client
        }
    
    except Exception as e:
        logger.error(f"❌ Verify password error: {e}")
        return {'success': False, 'error': str(e)}

# ==================== КРИПТО ФУНКЦИИ ====================
async def fetch_usdt_rate() -> float:
    try:
        url = f"{CRYPTOBOT_API_URL}/getExchangeRates"
        headers = {'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN}
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            for rate in data['result']:
                if rate['source'] == 'USDT' and rate['target'] == 'RUB':
                    return float(rate['rate'])
        return USDT_RATE
    except Exception as e:
        logger.error(f"USDT rate error: {e}")
        return USDT_RATE

async def create_crypto_invoice(amount_rub: float) -> Optional[Dict]:
    try:
        usdt_rate = await fetch_usdt_rate()
        amount_usdt = round(amount_rub / usdt_rate, 2)
        
        url = f"{CRYPTOBOT_API_URL}/createInvoice"
        headers = {
            'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN,
            'Content-Type': 'application/json'
        }
        payload = {
            "asset": "USDT",
            "amount": str(amount_usdt),
            "description": f"Пополнение на {amount_rub} RUB",
            "paid_btn_name": "openBot",
            "paid_btn_url": f"https://t.me/{bot_username}",
            "payload": f"crypto_{amount_rub}"
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('ok'):
                return data['result']
        return None
    
    except Exception as e:
        logger.error(f"Crypto invoice error: {e}")
        return None

# ==================== КЛАВИАТУРЫ ====================
def main_keyboard(user_id: int) -> ReplyKeyboardMarkup:
    buttons = [
        [KeyboardButton(text="🛍 КАТАЛОГ")],
        [KeyboardButton(text="💰 БАЛАНС"), KeyboardButton(text="👤 ПРОФИЛЬ")],
        [KeyboardButton(text="👥 РЕФЕРАЛЫ"), KeyboardButton(text="📜 ПОКУПКИ")],
        [KeyboardButton(text="📝 ОТЗЫВЫ"), KeyboardButton(text="📞 ПОДДЕРЖКА")]
    ]
    if user_id in ADMIN_IDS:
        buttons.append([KeyboardButton(text="⚙️ АДМИН")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def admin_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="➕ ДОБАВИТЬ ТОВАР", callback_data="admin_add_product")],
        [InlineKeyboardButton(text="🗑 УДАЛИТЬ ТОВАР", callback_data="admin_delete_product")],
        [InlineKeyboardButton(text="📦 СПИСОК ТОВАРОВ", callback_data="admin_list_products")],
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="admin_stats")],
        [InlineKeyboardButton(text="💰 НАЧИСЛИТЬ БАЛАНС", callback_data="admin_add_balance")],
        [InlineKeyboardButton(text="📢 РАССЫЛКА", callback_data="admin_mailing")],
        [InlineKeyboardButton(text="🚫 УПРАВЛЕНИЕ БАНАМИ", callback_data="admin_bans")],
        [InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="admin_settings")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_settings_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⭐ КУРС STARS", callback_data="set_stars")],
        [InlineKeyboardButton(text="💵 КУРС USDT", callback_data="set_usdt")],
        [InlineKeyboardButton(text="🎁 СКИДКА РЕФЕРАЛАМ", callback_data="set_discount")],
        [InlineKeyboardButton(text="💸 НАГРАДА ЗА РЕФЕРАЛА", callback_data="set_reward")],
        [InlineKeyboardButton(text="📢 КАНАЛ ОТЗЫВОВ", callback_data="set_reviews_channel")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="⭐ TELEGRAM STARS", callback_data="pay_stars")],
        [InlineKeyboardButton(text="💳 СБП", callback_data="pay_sbp")],
        [InlineKeyboardButton(text="₿ CRYPTOBOT", callback_data="pay_crypto")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_balance")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def catalog_keyboard(products: List[Tuple]) -> InlineKeyboardMarkup:
    buttons = []
    for product in products:
        if len(product) >= 8:
            pid, name, price, phone, session, region, year, added = product[:8]
            age = datetime.now().year - year
            button_text = f"{name} | {region} | {age} ЛЕТ | {price} ₽"
            buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"view_{pid}")])
    buttons.append([InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data="refresh_catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def product_keyboard(product_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="💳 КУПИТЬ", callback_data=f"buy_{product_id}")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_catalog")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def purchases_keyboard(purchases: List[Tuple]) -> InlineKeyboardMarkup:
    buttons = []
    for purchase in purchases:
        if len(purchase) >= 9:
            pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
            short_phone = phone[:7] + "..." if len(phone) > 7 else phone
            buttons.append([InlineKeyboardButton(
                text=f"📱 {short_phone} | {price} ₽ | {date[:10]}",
                callback_data=f"purchase_{pid}"
            )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def purchase_actions_keyboard(purchase_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🔑 ДАННЫЕ ВХОДА", callback_data=f"show_login_{purchase_id}")],
        [InlineKeyboardButton(text="📨 ПОКАЗАТЬ КОДЫ", callback_data=f"show_codes_{purchase_id}")],
        [InlineKeyboardButton(text="📁 ФАЙЛ СЕССИИ", callback_data=f"session_file_{purchase_id}")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_purchases")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def insufficient_balance_keyboard() -> InlineKeyboardMarkup:
    buttons = [[InlineKeyboardButton(text="💰 ПОПОЛНИТЬ", callback_data="show_payment_methods")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_payment_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="✍️ РЕКВИЗИТЫ", callback_data=f"send_details_{payment_id}")],
        [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ", callback_data=f"admin_confirm_{payment_id}"),
         InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"admin_reject_{payment_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def referral_keyboard() -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text="🔗 МОЯ ССЫЛКА", callback_data="show_ref_link")],
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="ref_stats")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    global bot_username
    args = message.text.split()
    referrer_id = None
    
    # Логируем действие
    log_user_action(message.from_user.id, "start")
    
    if len(args) > 1 and args[1].startswith('ref_'):
        referral_code = args[1][4:]
        referrer = get_user_by_referral_code(referral_code)
        if referrer and referrer[0] != message.from_user.id:
            referrer_id = referrer[0]
    
    user = get_user(message.from_user.id, message.from_user.username, referrer_id)
    
    welcome_text = (
        "<b>👋 ДОБРО ПОЖАЛОВАТЬ В MORGAN SHOP!</b>\n\n"
        "🔥 <b>ЛУЧШИЕ TELEGRAM АККАУНТЫ</b>\n"
        "✅ ГАРАНТИЯ КАЧЕСТВА\n"
        "📨 КОДЫ БЕРУТСЯ НАПРЯМУЮ ИЗ АККАУНТА\n\n"
        "ИСПОЛЬЗУЙ КНОПКИ НИЖЕ 👇"
    )
    
    if referrer_id:
        welcome_text += "\n\n🎉 ТЫ ПРИШЕЛ ПО РЕФЕРАЛЬНОЙ ССЫЛКЕ! ТЕБЕ ДОСТУПНА СКИДКА 10% НА ПЕРВОЕ ПОПОЛНЕНИЕ."
    
    await message.answer(welcome_text, reply_markup=main_keyboard(message.from_user.id))

@dp.message(Command("ban"))
async def cmd_ban(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    
    if message.reply_to_message:
        user_id = message.reply_to_message.from_user.id
        username = message.reply_to_message.from_user.username
        reason = " ".join(args[1:]) if len(args) > 1 else "Нарушение правил"
        ban_user(user_id, reason, message.from_user.id)
        await message.answer(f"✅ Пользователь {user_id} (@{username}) забанен!\nПричина: {reason}")
    
    elif len(args) >= 2:
        try:
            user_id = int(args[1])
            reason = " ".join(args[2:]) if len(args) > 2 else "Нарушение правил"
            ban_user(user_id, reason, message.from_user.id)
            await message.answer(f"✅ Пользователь {user_id} забанен!\nПричина: {reason}")
        except ValueError:
            await message.answer("❌ Неверный ID")

@dp.message(Command("unban"))
async def cmd_unban(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    args = message.text.split()
    if len(args) >= 2:
        try:
            user_id = int(args[1])
            unban_user(user_id)
            await message.answer(f"✅ Пользователь {user_id} разбанен!")
        except ValueError:
            await message.answer("❌ Неверный ID")

@dp.message(Command("banned"))
async def cmd_banned(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    banned = get_banned_users()
    if not banned:
        await message.answer("📭 Нет забаненных пользователей")
        return
    
    text = "🚫 <b>ЗАБАНЕННЫЕ ПОЛЬЗОВАТЕЛИ:</b>\n\n"
    for user_id, username, reason, date in banned:
        text += f"👤 ID: <code>{user_id}</code>\n"
        text += f"👤 Username: @{username or 'Нет'}\n"
        text += f"📝 Причина: {reason}\n"
        text += f"📅 Дата: {date[:16]}\n"
        text += "─" * 20 + "\n"
    
    await message.answer(text)

@dp.message(Command("debug"))
async def debug_command(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    
    text = "🔍 <b>ОТЛАДОЧНАЯ ИНФОРМАЦИЯ</b>\n\n"
    
    text += f"📱 temp_clients: {len(temp_clients)}\n"
    for phone in list(temp_clients.keys())[:3]:
        text += f"  • {phone}\n"
    
    text += f"\n🔑 active_sessions: {len(active_sessions)}\n"
    for phone in list(active_sessions.keys())[:3]:
        text += f"  • {phone}\n"
    
    await message.answer(text)

# ==================== ОСНОВНЫЕ РАЗДЕЛЫ ====================
@dp.message(F.text == "🛍 КАТАЛОГ")
async def catalog(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "catalog")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    products = get_products()
    if not products:
        await message.answer("📭 КАТАЛОГ ПУСТ. ТОВАРЫ ПОЯВЯТСЯ ПОЗЖЕ.")
        return
    await message.answer("📦 <b>ВЫБЕРИ ТОВАР ДЛЯ ПРОСМОТРА:</b>", reply_markup=catalog_keyboard(products))

@dp.message(F.text == "💰 БАЛАНС")
async def balance(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "balance")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    user_balance = get_balance(user_id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 <b>ТВОЙ БАЛАНС:</b> <code>{user_balance} ₽</code>\n"
        f"⭐ ЭКВИВАЛЕНТ: <code>{int(user_balance / stars_rate)} STARS</code>\n\n"
        f"ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    )
    await message.answer(text, reply_markup=payment_keyboard())

@dp.message(F.text == "👤 ПРОФИЛЬ")
async def profile(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "profile")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    user = get_user(user_id)
    if user is None:
        user = get_user(user_id, message.from_user.username)
    
    purchases = get_user_purchases(user_id)
    discount_status = "✅ ДОСТУПНА" if can_use_discount(user_id) else "❌ НЕ ДОСТУПНА"
    
    text = (
        f"👤 <b>ТВОЙ ПРОФИЛЬ</b>\n\n"
        f"🆔 ID: <code>{message.from_user.id}</code>\n"
        f"👤 USERNAME: @{message.from_user.username or 'НЕТ'}\n"
        f"💰 <b>БАЛАНС:</b> <code>{user[2] if user else 0} ₽</code>\n"
        f"📦 ВСЕГО ПОКУПОК: {len(purchases)}\n"
        f"🎁 СКИДКА НА ПЕРВОЕ ПОПОЛНЕНИЕ: {discount_status}\n"
        f"📅 ДАТА РЕГИСТРАЦИИ: {user[3][:10] if user else 'НЕТ'}"
    )
    await message.answer(text)

@dp.message(F.text == "👥 РЕФЕРАЛЫ")
async def referral_system(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "referral")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    user = get_user(user_id)
    
    if not user[5]:
        new_code = generate_referral_code(user_id)
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        c.execute("UPDATE users SET referral_code = ? WHERE user_id = ?", (new_code, user_id))
        conn.commit()
        conn.close()
        user = get_user(user_id)
    
    referral_link = f"https://t.me/{bot_username}?start=ref_{user[5]}"
    
    text = (
        f"👥 <b>РЕФЕРАЛЬНАЯ СИСТЕМА</b>\n\n"
        f"💰 НАГРАДА: {get_setting('referral_reward')}% ОТ ПОПОЛНЕНИЙ РЕФЕРАЛОВ\n"
        f"🎁 СКИДКА ДЛЯ РЕФЕРАЛОВ: {get_setting('referral_discount')}% НА ПЕРВОЕ ПОПОЛНЕНИЕ\n\n"
        f"🔗 ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА:\n<code>{referral_link}</code>\n\n"
        f"📤 ОТПРАВЛЯЙ ЕЁ ДРУЗЬЯМ И ПОЛУЧАЙ НАГРАДУ!"
    )
    await message.answer(text, reply_markup=referral_keyboard())

@dp.callback_query(F.data == "show_ref_link")
async def show_ref_link(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    log_user_action(user_id, "show_ref_link")
    
    user = get_user(user_id)
    if not user:
        await safe_edit_message(callback.message, "❌ ОШИБКА: ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН.")
        await callback.answer()
        return
    
    if not user[5]:
        new_code = generate_referral_code(user_id)
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        c.execute("UPDATE users SET referral_code = ? WHERE user_id = ?", (new_code, user_id))
        conn.commit()
        conn.close()
        user = get_user(user_id)
    
    referral_link = f"https://t.me/{bot_username}?start=ref_{user[5]}"
    text = (
        f"🔗 <b>ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА:</b>\n\n"
        f"<code>{referral_link}</code>\n\n"
        f"📤 ОТПРАВЛЯЙ ЕЁ ДРУЗЬЯМ И ПОЛУЧАЙ {get_setting('referral_reward')}% ОТ ИХ ПОПОЛНЕНИЙ!"
    )
    await safe_edit_message(callback.message, text, referral_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "ref_stats")
async def ref_stats(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    log_user_action(user_id, "ref_stats")
    
    stats = get_referral_stats(user_id)
    text = f"📊 <b>СТАТИСТИКА РЕФЕРАЛОВ</b>\n\n"
    text += f"👥 ПРИГЛАШЕНО: <b>{stats['total_count']}</b>\n"
    text += f"💰 ЗАРАБОТАНО: <b>{stats['total_earnings']} ₽</b>\n\n"
    
    if stats['referrals']:
        text += "СПИСОК РЕФЕРАЛОВ:\n"
        for ref in stats['referrals']:
            username = ref[0] if ref[0] else "БЕЗ USERNAME"
            date = ref[1][:10] if ref[1] else "НЕИЗВЕСТНО"
            text += f"👤 @{username} | 📅 {date}\n"
    else:
        text += "📭 У ТЕБЯ ПОКА НЕТ РЕФЕРАЛОВ."
    
    await safe_edit_message(callback.message, text, referral_keyboard())
    await callback.answer()

@dp.message(F.text == "📜 ПОКУПКИ")
async def my_purchases(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "purchases")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    purchases = get_user_purchases(user_id)
    if not purchases:
        await message.answer("📭 У ТЕБЯ ПОКА НЕТ ПОКУПОК.")
        return
    await message.answer("📜 <b>ТВОИ КУПЛЕННЫЕ АККАУНТЫ:</b>", reply_markup=purchases_keyboard(purchases))

@dp.message(F.text == "📝 ОТЗЫВЫ")
async def reviews_link(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "reviews")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    channel_link = get_setting('reviews_channel_link')
    if channel_link and channel_link != "не настроен":
        await message.answer(
            f"📢 <b>НАШ КАНАЛ С ОТЗЫВАМИ:</b>\n\n"
            f"{channel_link}\n\n"
            f"Там ты можешь почитать отзывы других покупателей!"
        )
    else:
        await message.answer(
            "📢 <b>КАНАЛ С ОТЗЫВАМИ ЕЩЁ НЕ НАСТРОЕН</b>\n\n"
            "Администратор скоро добавит ссылку."
        )

@dp.message(F.text == "📞 ПОДДЕРЖКА")
async def support(message: types.Message):
    user_id = message.from_user.id
    log_user_action(user_id, "support")
    
    if await auto_ban_spammer(user_id, message.from_user.username):
        return
    
    text = (
        "📞 <b>СЛУЖБА ПОДДЕРЖКИ</b>\n\n"
        "ПО ВСЕМ ВОПРОСАМ ПИШИ СЮДА: @deaMorgan"
    )
    await message.answer(text)

# ==================== ДЕТАЛИ ТОВАРА ====================
@dp.callback_query(F.data == "refresh_catalog")
async def refresh_catalog(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "refresh_catalog")
    
    products = get_products()
    if not products:
        await safe_edit_message(callback.message, "📭 КАТАЛОГ ПУСТ.")
        await callback.answer()
        return
    await safe_edit_message(callback.message, "📦 <b>ВЫБЕРИ ТОВАР:</b>", catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('view_'))
async def view_product(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "view_product")
    
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)
    
    if not product:
        await safe_edit_message(callback.message, "❌ ТОВАР НЕ НАЙДЕН.")
        await callback.answer()
        return
    
    product_id, name, price, phone, session, region, year, added = product[:8]
    age = datetime.now().year - year
    stars_price = int(price / get_setting('stars_rate'))
    
    text = (
        f"📦 <b>{name}</b>\n\n"
        f"🌍 <b>РЕГИОН:</b> {region}\n"
        f"📅 <b>ГОД СОЗДАНИЯ:</b> {year} ({age} ЛЕТ)\n"
        f"💰 <b>ЦЕНА:</b> <code>{price} ₽</code> / {stars_price} ⭐\n"
        f"🕐 <b>ДОБАВЛЕН:</b> {added[:10]}\n\n"
        f"📱 ТЕЛЕФОН БУДЕТ ДОСТУПЕН ПОСЛЕ ПОКУПКИ."
    )
    
    await safe_edit_message(callback.message, text, product_keyboard(product_id))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('buy_'))
async def buy_product(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    log_user_action(user_id, "buy_product")
    
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)
    
    if not product:
        await safe_edit_message(callback.message, "❌ ТОВАР НЕ НАЙДЕН.")
        await callback.answer()
        return
    
    if len(product) >= 9:
        product_id, name, price, phone, session, region, year, added, password = product[:9]
    else:
        product_id, name, price, phone, session, region, year, added = product[:8]
        password = None
    
    user_balance = get_balance(user_id)
    
    if user_balance >= price:
        update_balance(user_id, -price)
        
        purchase_id = add_purchase(
            user_id,
            product_id,
            price,
            phone,
            session,
            region,
            year,
            password
        )
        
        delete_product(product_id)
        age = datetime.now().year - year
        
        text = (
            f"✅ <b>ПОКУПКА УСПЕШНА!</b>\n\n"
            f"📦 ТОВАР: <b>{name}</b>\n"
            f"💰 ЦЕНА: <code>{price} ₽</code>\n"
            f"🌍 РЕГИОН: {region}\n"
            f"📅 ГОД: {year} ({age} ЛЕТ)\n"
            f"📱 ТЕЛЕФОН: <code>{phone}</code>\n"
        )
        
        if password and password not in ['None', '']:
            text += f"🔑 ПАРОЛЬ АККАУНТА: <code>{password}</code>\n"
        
        text += f"\n📁 ФАЙЛ СЕССИИ ДОСТУПЕН В РАЗДЕЛЕ ПОКУПКИ"
        
        await safe_edit_message(callback.message, text)
    else:
        need = price - user_balance
        await safe_edit_message(
            callback.message,
            f"❌ <b>НЕДОСТАТОЧНО СРЕДСТВ</b>\n\nНУЖНО ЕЩЕ: <code>{need} ₽</code>",
            insufficient_balance_keyboard()
        )
    await callback.answer()

# ==================== ДЕТАЛИ ПОКУПКИ ====================
@dp.callback_query(lambda c: c.data.startswith('purchase_'))
async def purchase_details(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "purchase_details")
    
    purchase_id = int(callback.data.split('_')[1])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await safe_edit_message(callback.message, "❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return
    
    pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
    
    text = (
        f"📱 <b>АККАУНТ #{pid}</b>\n\n"
        f"📱 ТЕЛЕФОН: <code>{phone}</code>\n"
        f"💰 ЦЕНА: <code>{price} ₽</code>\n"
        f"🌍 РЕГИОН: {region}\n"
        f"📅 ГОД АККАУНТА: {year}\n"
        f"📦 КУПЛЕН: {date[:16]}\n\n"
        f"ВЫБЕРИ ДЕЙСТВИЕ:"
    )
    await safe_edit_message(callback.message, text, purchase_actions_keyboard(pid))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('show_login_'))
async def show_login(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "show_login")
    
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await safe_edit_message(callback.message, "❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return
    
    if len(purchase) >= 10:
        pid, user_id, product_id, price, date, phone, session, region, year, password = purchase[:10]
    else:
        pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
        password = None
    
    text = (
        f"🔑 <b>ДАННЫЕ ДЛЯ ВХОДА (АККАУНТ #{pid})</b>\n\n"
        f"📱 ТЕЛЕФОН: <code>{phone}</code>\n"
        f"🔐 СЕССИЯ:\n<code>{session}</code>\n"
    )
    
    if password and password not in ['None', 'пропустить', '']:
        text += f"🔑 ПАРОЛЬ АККАУНТА: <code>{password}</code>\n\n"
    else:
        text += f"🔑 ПАРОЛЬ АККАУНТА: НЕ УСТАНОВЛЕН\n\n"
    
    text += "⚠️ СОХРАНИ ЭТИ ДАННЫЕ В БЕЗОПАСНОМ МЕСТЕ!"
    
    await safe_edit_message(callback.message, text, InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data=f"purchase_{purchase_id}")]
    ]))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('show_codes_'))
async def show_codes(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "show_codes")
    
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await safe_edit_message(callback.message, "❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return
    
    pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
    
    msg = await safe_edit_message(callback.message, "🔄 ПОДКЛЮЧАЮСЬ К TELEGRAM АККАУНТУ...")
    
    try:
        codes = await get_live_codes_from_account(session, limit=30)
        
        if not codes:
            text = f"📨 <b>АККАУНТ #{pid}</b>\n\n❌ НЕТ КОДОВ В ЭТОМ АККАУНТЕ"
        else:
            text = f"📨 <b>КОДЫ ИЗ TELEGRAM (АККАУНТ #{pid})</b>:\n\n"
            for i, code_data in enumerate(codes, 1):
                star = "⭐ " if i == 1 else ""
                text += f"{i}. {star}{code_data['type']} <code>{code_data['code']}</code>  |  🕐 {code_data['date']}\n"
        
        await safe_edit_message(msg, text, InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data=f"show_codes_{purchase_id}")],
            [InlineKeyboardButton(text="🔙 НАЗАД", callback_data=f"purchase_{purchase_id}")]
        ]))
    except Exception as e:
        await safe_edit_message(msg, f"❌ ОШИБКА: {str(e)[:100]}")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('session_file_'))
async def session_file(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "session_file")
    
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await safe_edit_message(callback.message, "❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return
    
    pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
    
    filename = f"session_{phone}.session"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(session)
    
    with open(filename, 'rb') as f:
        await callback.message.answer_document(
            FSInputFile(filename),
            caption=f"📁 ФАЙЛ СЕССИИ ДЛЯ {phone}"
        )
    
    os.remove(filename)
    await callback.answer()

# ==================== ПЛАТЕЖИ ====================
@dp.callback_query(F.data == "show_payment_methods")
async def show_payment_methods(callback: types.CallbackQuery):
    log_user_action(callback.from_user.id, "show_payment_methods")
    
    await safe_edit_message(callback.message, "💰 <b>ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:</b>", payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "pay_stars")
async def pay_stars(callback: types.CallbackQuery, state: FSMContext):
    log_user_action(callback.from_user.id, "pay_stars")
    
    await safe_edit_message(
        callback.message,
        f"⭐ <b>ПОПОЛНЕНИЕ ЧЕРЕЗ STARS</b>\n\n"
        f"КУРС: 1 STAR = {get_setting('stars_rate')} ₽\n"
        f"ВВЕДИ СУММУ В РУБЛЯХ:"
    )
    await state.set_state(PaymentStates.waiting_for_stars_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_stars_amount)
async def stars_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        final = amount
        
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            final = amount * (1 - discount / 100)
            apply_first_discount(message.from_user.id)
        
        stars_rate = get_setting('stars_rate')
        stars = int(final / stars_rate)
        
        prices = [LabeledPrice(label="Пополнение баланса", amount=stars)]
        payload = f"stars_{message.from_user.id}_{int(datetime.now().timestamp())}"
        
        invoice = await bot.create_invoice_link(
            title="Пополнение баланса Stars",
            description=f"{final} ₽ ({stars} ⭐)",
            payload=payload,
            currency="XTR",
            prices=prices
        )
        
        add_pending_payment(message.from_user.id, final, "stars", payload)
        
        text = (
            f"⭐ <b>СЧЕТ СОЗДАН</b>\n\n"
            f"💰 СУММА: <code>{final} ₽</code>\n"
            f"⭐ STARS: <code>{stars}</code>"
        )
        
        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 ОПЛАТИТЬ", url=invoice)]
            ])
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout_query: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message):
    payload = message.successful_payment.invoice_payload
    
    if payload.startswith("stars_"):
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        c.execute("SELECT id, user_id, amount FROM pending_payments WHERE invoice_id = ? AND status='pending'", (payload,))
        payment = c.fetchone()
        conn.close()
        
        if payment:
            pid, uid, amt = payment
            update_balance(uid, amt)
            update_payment_status(pid, 'confirmed')
            
            user = get_user(uid)
            if user and user[4]:
                reward = amt * (get_setting('referral_reward') / 100)
                update_balance(user[4], reward)
            
            await message.answer(f"✅ <b>БАЛАНС ПОПОЛНЕН НА {amt} ₽</b>")
        else:
            await message.answer("❌ ПЛАТЕЖ НЕ НАЙДЕН")

@dp.callback_query(F.data == "pay_sbp")
async def pay_sbp(callback: types.CallbackQuery, state: FSMContext):
    log_user_action(callback.from_user.id, "pay_sbp")
    
    await safe_edit_message(
        callback.message,
        "💳 <b>ПОПОЛНЕНИЕ ЧЕРЕЗ СБП</b>\n\n"
        "ВВЕДИ СУММУ (МИНИМУМ 100 ₽):"
    )
    await state.set_state(PaymentStates.waiting_for_sbp_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_sbp_amount)
async def sbp_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        if amount < 100:
            await message.answer("❌ МИНИМАЛЬНАЯ СУММА 100 ₽. ВВЕДИ ДРУГУЮ:")
            return
        
        final = amount
        
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            final = amount * (1 - discount / 100)
            apply_first_discount(message.from_user.id)
        
        payment_id = add_pending_payment(message.from_user.id, final, "sbp")
        
        for admin_id in ADMIN_IDS:
            await bot.send_message(
                admin_id,
                f"💰 <b>ЗАПРОС НА ПОПОЛНЕНИЕ</b>\n\n"
                f"👤 ПОЛЬЗОВАТЕЛЬ: @{message.from_user.username or 'НЕТ'} (ID: {message.from_user.id})\n"
                f"💵 СУММА: {amount} ₽\n"
                f"💳 К ОПЛАТЕ: {final} ₽\n"
                f"🆔 ID ПЛАТЕЖА: {payment_id}",
                reply_markup=admin_payment_keyboard(payment_id)
            )
        
        await message.answer("✅ ЗАПРОС СОЗДАН. ОЖИДАЙ, АДМИНИСТРАТОР ОТПРАВИТ РЕКВИЗИТЫ.")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "pay_crypto")
async def pay_crypto(callback: types.CallbackQuery, state: FSMContext):
    log_user_action(callback.from_user.id, "pay_crypto")
    
    await safe_edit_message(
        callback.message,
        "₿ <b>ПОПОЛНЕНИЕ ЧЕРЕЗ CRYPTOBOT</b>\n\n"
        "ВВЕДИ СУММУ В РУБЛЯХ:"
    )
    await state.set_state(PaymentStates.waiting_for_crypto_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_crypto_amount)
async def crypto_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        final = amount
        
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            final = amount * (1 - discount / 100)
            apply_first_discount(message.from_user.id)
        
        invoice = await create_crypto_invoice(final)
        if not invoice:
            await message.answer("❌ ОШИБКА ПРИ СОЗДАНИИ СЧЕТА. ПОПРОБУЙ ПОЗЖЕ.")
            await state.clear()
            return
        
        payment_id = add_pending_payment(message.from_user.id, final, "crypto", invoice['invoice_id'])
        
        text = (
            f"₿ <b>СЧЕТ СОЗДАН</b>\n\n"
            f"💰 СУММА: <code>{final} ₽</code>\n"
            f"💲 USDT: <code>{invoice['amount']}</code>"
        )
        
        await message.answer(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 ОПЛАТИТЬ", url=invoice['pay_url'])]
            ])
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

# ==================== АДМИНСКИЕ ОБРАБОТЧИКИ ПЛАТЕЖЕЙ ====================
@dp.callback_query(lambda c: c.data.startswith('send_details_'))
async def send_payment_details(callback: types.CallbackQuery, state: FSMContext):
    payment_id = int(callback.data.split('_')[2])
    await state.update_data(payment_id=payment_id)
    await safe_edit_message(callback.message, "✍️ ВВЕДИ РЕКВИЗИТЫ ДЛЯ ОПЛАТЫ:")
    await state.set_state(AdminPaymentStates.waiting_for_payment_details)
    await callback.answer()

@dp.message(AdminPaymentStates.waiting_for_payment_details)
async def payment_details_handler(message: types.Message, state: FSMContext):
    data = await state.get_data()
    payment_id = data.get('payment_id')
    payment = get_pending_payment(payment_id)
    
    if payment:
        try:
            await bot.send_message(
                payment[1],
                f"💳 <b>РЕКВИЗИТЫ ДЛЯ ОПЛАТЫ</b>\n\n"
                f"💰 СУММА: <code>{payment[2]} ₽</code>\n"
                f"📱 СПОСОБ: {payment[3].upper()}\n\n"
                f"РЕКВИЗИТЫ:\n<code>{message.text}</code>\n\n"
                f"ПОСЛЕ ОПЛАТЫ НАЖМИ КНОПКУ НИЖЕ:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Я ПЕРЕВЕЛ", callback_data=f"user_paid_{payment_id}")]
                ])
            )
            await message.answer("✅ РЕКВИЗИТЫ ОТПРАВЛЕНЫ ПОЛЬЗОВАТЕЛЮ.")
        except Exception as e:
            await message.answer(f"❌ ОШИБКА ОТПРАВКИ: {e}")
    
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith('user_paid_'))
async def user_paid(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    
    if payment:
        for admin_id in ADMIN_IDS:
            await bot.send_message(
                admin_id,
                f"💰 <b>ПОЛЬЗОВАТЕЛЬ СООБЩИЛ ОБ ОПЛАТЕ</b>\n\n"
                f"🆔 ПЛАТЕЖ ID: {payment_id}\n"
                f"👤 ПОЛЬЗОВАТЕЛЬ ID: {payment[1]}\n"
                f"💵 СУММА: {payment[2]} ₽\n"
                f"📱 МЕТОД: {payment[3]}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ", callback_data=f"admin_confirm_{payment_id}"),
                     InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"admin_reject_{payment_id}")]
                ])
            )
        await safe_edit_message(callback.message, "✅ СООБЩЕНИЕ ОТПРАВЛЕНО АДМИНИСТРАТОРУ.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_confirm_'))
async def admin_confirm_payment(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    
    if payment:
        update_balance(payment[1], payment[2])
        update_payment_status(payment_id, 'confirmed')
        
        user = get_user(payment[1])
        if user and user[4]:
            reward = payment[2] * (get_setting('referral_reward') / 100)
            update_balance(user[4], reward)
        
        try:
            await bot.send_message(
                payment[1],
                f"✅ <b>ПЛАТЕЖ ПОДТВЕРЖДЕН!</b>\n\n"
                f"💰 СУММА: <code>{payment[2]} ₽</code>\n"
                f"💳 БАЛАНС ПОПОЛНЕН."
            )
        except:
            pass
        
        await safe_edit_message(callback.message, f"✅ ПЛАТЕЖ #{payment_id} ПОДТВЕРЖДЕН.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_reject_'))
async def admin_reject_payment(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    
    if payment:
        update_payment_status(payment_id, 'rejected')
        
        try:
            await bot.send_message(
                payment[1],
                f"❌ <b>ПЛАТЕЖ ОТКЛОНЕН.</b>\n\n"
                f"💰 СУММА: <code>{payment[2]} ₽</code>\n"
                f"📞 СВЯЖИСЬ С ПОДДЕРЖКОЙ."
            )
        except:
            pass
        
        await safe_edit_message(callback.message, f"❌ ПЛАТЕЖ #{payment_id} ОТКЛОНЕН.")
    
    await callback.answer()

# ==================== АДМИН ПАНЕЛЬ ====================
@dp.message(F.text == "⚙️ АДМИН")
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У ТЕБЯ НЕТ ДОСТУПА.")
        return
    await message.answer("⚙️ <b>АДМИН ПАНЕЛЬ</b>", reply_markup=admin_keyboard())

# ----- ДОБАВЛЕНИЕ ТОВАРА -----
@dp.callback_query(F.data == "admin_add_product")
async def admin_add_product(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(callback.message, "➕ ВВЕДИ НАЗВАНИЕ ТОВАРА:")
    await state.set_state(ProductStates.waiting_for_name)
    await callback.answer()

@dp.message(ProductStates.waiting_for_name)
async def product_name_handler(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("💰 ВВЕДИ ЦЕНУ В РУБЛЯХ:")
    await state.set_state(ProductStates.waiting_for_price)

@dp.message(ProductStates.waiting_for_price)
async def product_price_handler(message: types.Message, state: FSMContext):
    try:
        price = float(message.text)
        await state.update_data(price=price)
        await message.answer("📱 ВВЕДИ НОМЕР ТЕЛЕФОНА АККАУНТА:")
        await state.set_state(ProductStates.waiting_for_phone)
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО.")

@dp.message(ProductStates.waiting_for_phone)
async def product_phone_handler(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    await state.update_data(phone=phone)
    
    await message.answer(
        "🔐 <b>ВВЕДИ ПАРОЛЬ ОТ АККАУНТА (ОБЛАЧНЫЙ ПАРОЛЬ / 2FA)</b>\n\n"
        "Если пароля нет - отправь: <code>пропустить</code>"
    )
    await state.set_state(ProductStates.waiting_for_account_password)

@dp.message(ProductStates.waiting_for_account_password)
async def product_account_password_handler(message: types.Message, state: FSMContext):
    password = message.text.strip()
    logger.info(f"🔐 Получен пароль аккаунта: {'[СКРЫТ]' if password != 'пропустить' else 'пропустить'}")
    
    if password.lower() in ['пропустить', 'нет', '-', '']:
        await state.update_data(account_password=None)
        logger.info("💾 Пароль не сохранен (пропущен)")
    else:
        await state.update_data(account_password=password)
        logger.info("💾 Пароль сохранен")
    
    data = await state.get_data()
    phone = data.get('phone')
    
    status_msg = await message.answer("🔄 ВЫПОЛНЯЮ ВХОД В TELEGRAM...")
    
    try:
        logger.info(f"📱 Начинаем вход для номера: {phone}")
        result = await login_to_telegram(phone)
        logger.info(f"📊 Результат входа: {result}")
        
        if not result['success']:
            error_text = result.get('error', 'НЕИЗВЕСТНАЯ ОШИБКА')
            logger.error(f"❌ Ошибка входа: {error_text}")
            await status_msg.edit_text(f"❌ ОШИБКА ВХОДА: {error_text}")
            await state.clear()
            return
        
        if result.get('already_logged'):
            logger.info("✅ Аккаунт уже авторизован, добавляем товар")
            data = await state.get_data()
            pid = add_product(
                data['name'],
                data['price'],
                result['phone'],
                result['session'],
                result['region'],
                result['year'],
                data.get('account_password')
            )
            logger.info(f"✅ Товар добавлен с ID: {pid}")
            await status_msg.edit_text(
                f"✅ <b>АККАУНТ УСПЕШНО ДОБАВЛЕН!</b>\n\n"
                f"📦 НАЗВАНИЕ: <b>{data['name']}</b>\n"
                f"💰 ЦЕНА: <code>{data['price']} ₽</code>\n"
                f"🌍 РЕГИОН: {result['region']}\n"
                f"📅 ГОД: {result['year']}\n"
                f"🔑 ПАРОЛЬ: <code>{data.get('account_password', 'НЕТ')}</code>\n"
                f"🆔 ID: <code>{pid}</code>"
            )
            await state.clear()
            
        elif result.get('need_code'):
            logger.info(f"📱 Требуется код подтверждения для {result['phone']}")
            await state.update_data(phone=result['phone'])
            await status_msg.edit_text(
                f"📱 <b>КОД ПОДТВЕРЖДЕНИЯ ОТПРАВЛЕН НА НОМЕР {result['phone']}</b>\n\n"
                f"ВВЕДИ КОД ИЗ TELEGRAM:"
            )
            await state.set_state(ProductStates.waiting_for_code)
        else:
            logger.error(f"❌ Неизвестный сценарий: {result}")
            await status_msg.edit_text(f"❌ НЕИЗВЕСТНЫЙ СЦЕНАРИЙ")
            await state.clear()
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
        await status_msg.edit_text(f"❌ ОШИБКА: {str(e)[:100]}")
        await state.clear()

@dp.message(ProductStates.waiting_for_code)
async def product_code_handler(message: types.Message, state: FSMContext):
    code = message.text.strip()
    logger.info(f"\n🔍 DEBUG: product_code_handler вызван")
    
    data = await state.get_data()
    phone = data.get('phone')
    logger.info(f"🔍 DEBUG: Телефон из state: {phone}")
    
    if phone in temp_clients:
        logger.info(f"✅ Клиент найден в temp_clients")
    else:
        logger.error(f"❌ Клиент НЕ найден в temp_clients!")
        logger.info(f"🔍 Доступные ключи: {list(temp_clients.keys())}")
    
    status_msg = await message.answer("🔄 ПРОВЕРЯЮ КОД...")
    result = await verify_code(phone, code)
    
    if not result['success']:
        await status_msg.edit_text(f"❌ ОШИБКА: {result.get('error', 'НЕИЗВЕСТНАЯ')}")
        await state.clear()
        return
    
    if result.get('need_password'):
        logger.info("🔐 Требуется 2FA пароль")
        await state.update_data(phone=phone)
        await status_msg.edit_text(
            "🔐 <b>ТРЕБУЕТСЯ 2FA ПАРОЛЬ (ОБЛАЧНЫЙ ПАРОЛЬ)</b>\n\n"
            "ВВЕДИ ПАРОЛЬ:"
        )
        await state.set_state(ProductStates.waiting_for_password)
    else:
        logger.info("✅ Успешный вход без 2FA")
        data = await state.get_data()
        pid = add_product(
            data['name'],
            data['price'],
            result['phone'],
            result['session'],
            result['region'],
            result['year'],
            data.get('account_password')
        )
        logger.info(f"✅ Товар добавлен с ID: {pid}")
        await status_msg.edit_text(
            f"✅ <b>АККАУНТ УСПЕШНО ДОБАВЛЕН!</b>\n\n"
            f"📦 НАЗВАНИЕ: <b>{data['name']}</b>\n"
            f"💰 ЦЕНА: <code>{data['price']} ₽</code>\n"
            f"🌍 РЕГИОН: {result['region']}\n"
            f"📅 ГОД: {result['year']}\n"
            f"🔑 ПАРОЛЬ: <code>{data.get('account_password', 'НЕТ')}</code>\n"
            f"🆔 ID: <code>{pid}</code>"
        )
        await state.clear()

@dp.message(ProductStates.waiting_for_password)
async def product_password_handler(message: types.Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    phone = data['phone']
    
    status_msg = await message.answer("🔄 ПРОВЕРЯЮ 2FA ПАРОЛЬ...")
    result = await verify_password(phone, password)
    
    if not result['success']:
        await status_msg.edit_text(f"❌ ОШИБКА: {result.get('error', 'НЕВЕРНЫЙ ПАРОЛЬ')}")
        return
    
    data = await state.get_data()
    pid = add_product(
        data['name'],
        data['price'],
        result['phone'],
        result['session'],
        result['region'],
        result['year'],
        data.get('account_password')
    )
    await status_msg.edit_text(
        f"✅ <b>АККАУНТ УСПЕШНО ДОБАВЛЕН!</b>\n\n"
        f"📦 НАЗВАНИЕ: <b>{data['name']}</b>\n"
        f"💰 ЦЕНА: <code>{data['price']} ₽</code>\n"
        f"🌍 РЕГИОН: {result['region']}\n"
        f"📅 ГОД: {result['year']}\n"
        f"🔑 ПАРОЛЬ: <code>{data.get('account_password', 'НЕТ')}</code>\n"
        f"🆔 ID: <code>{pid}</code>"
    )
    await state.clear()

# ----- УДАЛЕНИЕ ТОВАРА -----
@dp.callback_query(F.data == "admin_delete_product")
async def admin_delete_product(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await safe_edit_message(callback.message, "📭 НЕТ ТОВАРОВ.")
        await callback.answer()
        return
    
    buttons = []
    for prod in products:
        pid, name, price, *_ = prod
        buttons.append([InlineKeyboardButton(text=f"{name} | {price} ₽", callback_data=f"del_{pid}")])
    buttons.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")])
    
    await safe_edit_message(
        callback.message,
        "🗑 <b>ВЫБЕРИ ТОВАР ДЛЯ УДАЛЕНИЯ:</b>",
        InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('del_'))
async def confirm_delete(callback: types.CallbackQuery):
    pid = int(callback.data.split('_')[1])
    delete_product(pid)
    await safe_edit_message(callback.message, "✅ ТОВАР УДАЛЕН!")
    await callback.answer()

# ----- СПИСОК ТОВАРОВ -----
@dp.callback_query(F.data == "admin_list_products")
async def admin_list_products(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await safe_edit_message(callback.message, "📭 НЕТ ТОВАРОВ.")
        await callback.answer()
        return
    
    text = "📦 <b>СПИСОК ТОВАРОВ:</b>\n\n"
    for prod in products:
        pid, name, price, phone, session, region, year, added = prod[:8]
        text += f"🆔 <code>{pid}</code> | {name} | <code>{price} ₽</code> | {region} | {year}\n"
    
    await safe_edit_message(callback.message, text)
    await callback.answer()

# ----- СТАТИСТИКА -----
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM users")
    users = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM products")
    products = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM purchases")
    purchases = c.fetchone()[0]
    
    c.execute("SELECT SUM(price) FROM purchases")
    revenue = c.fetchone()[0] or 0
    
    conn.close()
    
    text = (
        f"📊 <b>СТАТИСТИКА</b>\n\n"
        f"👥 ПОЛЬЗОВАТЕЛЕЙ: <b>{users}</b>\n"
        f"📦 ТОВАРОВ: <b>{products}</b>\n"
        f"🛒 ПРОДАЖ: <b>{purchases}</b>\n"
        f"💰 ВЫРУЧКА: <b>{revenue} ₽</b>"
    )
    await safe_edit_message(callback.message, text)
    await callback.answer()

# ----- НАЧИСЛЕНИЕ БАЛАНСА -----
@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(callback.message, "💰 ВВЕДИ ID ПОЛЬЗОВАТЕЛЯ:")
    await state.set_state(AdminAddBalanceStates.waiting_for_user_id)
    await callback.answer()

@dp.message(AdminAddBalanceStates.waiting_for_user_id)
async def admin_add_balance_user_id(message: types.Message, state: FSMContext):
    try:
        uid = int(message.text.strip())
        user = get_user(uid)
        if not user:
            await message.answer("❌ ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН")
            return
        await state.update_data(target_uid=uid)
        await message.answer("💰 ВВЕДИ СУММУ:")
        await state.set_state(AdminAddBalanceStates.waiting_for_amount)
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛОВОЙ ID")

@dp.message(AdminAddBalanceStates.waiting_for_amount)
async def admin_add_balance_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.answer("❌ СУММА ДОЛЖНА БЫТЬ > 0")
            return
        
        data = await state.get_data()
        uid = data['target_uid']
        update_balance(uid, amount)
        
        await message.answer(f"✅ БАЛАНС {uid} ПОПОЛНЕН НА {amount} ₽")
        
        try:
            await bot.send_message(uid, f"💰 <b>АДМИН ПОПОЛНИЛ ТВОЙ БАЛАНС НА {amount} ₽</b>")
        except:
            pass
        
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

# ----- УПРАВЛЕНИЕ БАНАМИ -----
@dp.callback_query(F.data == "admin_bans")
async def admin_bans_menu(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    banned = get_banned_users()
    
    text = f"🚫 <b>УПРАВЛЕНИЕ БАНАМИ</b>\n\n"
    text += f"📊 Всего забанено: <b>{len(banned)}</b>\n\n"
    
    buttons = []
    for user_id, username, reason, date in banned[:5]:
        short_name = username or f"ID {user_id}"
        buttons.append([InlineKeyboardButton(
            text=f"🔨 {short_name[:20]}",
            callback_data=f"unban_{user_id}"
        )])
    
    buttons.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")])
    
    await safe_edit_message(callback.message, text, InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('unban_'))
async def admin_unban(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("❌ Нет доступа", show_alert=True)
        return
    
    user_id = int(callback.data.split('_')[1])
    unban_user(user_id)
    await safe_edit_message(callback.message, f"✅ Пользователь {user_id} разбанен!")
    await callback.answer()
        
# ----- РАССЫЛКА -----
@dp.callback_query(F.data == "admin_mailing")
async def admin_mailing_start(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(
        callback.message,
        "📢 <b>ВВЕДИ ТЕКСТ ДЛЯ РАССЫЛКИ</b>\n\n"
        "Доступны переменные:\n"
        "• <code>{{name}}</code> — username\n"
        "• <code>{{id}}</code> — ID пользователя\n\n"
        "Можно использовать HTML-теги: <b>жирный</b>, <i>курсив</i>, <code>код</code>"
    )
    await state.set_state(MailingStates.waiting_for_message)
    await callback.answer()

@dp.message(MailingStates.waiting_for_message)
async def admin_mailing_message(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    users = get_all_users()
    
    preview = message.text.replace("{{name}}", message.from_user.first_name or "User")
    preview = preview.replace("{{id}}", str(message.from_user.id))
    
    await message.answer(
        f"📢 <b>ПРЕДПРОСМОТР:</b>\n\n{preview}\n\n"
        f"👥 ВСЕГО ПОЛЬЗОВАТЕЛЕЙ: <b>{len(users)}</b>\n\n"
        f"✅ ОТПРАВИТЬ?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ДА", callback_data="mailing_send")],
            [InlineKeyboardButton(text="❌ НЕТ", callback_data="admin_back")]
        ])
    )
    await state.set_state(MailingStates.waiting_for_confirm)

@dp.callback_query(F.data == "mailing_send")
async def admin_mailing_send(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data['text']
    
    await safe_edit_message(callback.message, "🔄 НАЧИНАЮ РАССЫЛКУ...")
    
    users = get_all_users()
    success = 0
    failed = 0
    
    for uid, uname in users:
        try:
            user_text = text.replace("{{name}}", uname or "User").replace("{{id}}", str(uid))
            await bot.send_message(uid, user_text)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка отправки {uid}: {e}")
    
    await safe_edit_message(
        callback.message,
        f"✅ <b>РАССЫЛКА ЗАВЕРШЕНА!</b>\n\n"
        f"✅ УСПЕШНО: <b>{success}</b>\n"
        f"❌ ОШИБОК: <b>{failed}</b>"
    )
    await state.clear()
    await callback.answer()

# ----- НАСТРОЙКИ -----
@dp.callback_query(F.data == "admin_settings")
async def admin_settings(callback: types.CallbackQuery):
    stars = get_setting('stars_rate')
    usdt = get_setting('usdt_rate')
    discount = get_setting('referral_discount')
    reward = get_setting('referral_reward')
    
    text = (
        f"⚙️ <b>ТЕКУЩИЕ НАСТРОЙКИ:</b>\n\n"
        f"⭐ STARS: 1 = <code>{stars} ₽</code>\n"
        f"💵 USDT: 1 = <code>{usdt} ₽</code>\n"
        f"🎁 СКИДКА: <b>{discount}%</b>\n"
        f"💸 НАГРАДА: <b>{reward}%</b>"
    )
    await safe_edit_message(callback.message, text, admin_settings_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_stars")
async def set_stars(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(
        callback.message,
        f"⭐ ТЕКУЩИЙ КУРС: <code>{get_setting('stars_rate')} ₽</code>\nВВЕДИ НОВЫЙ:"
    )
    await state.set_state(AdminSettingsStates.waiting_for_stars)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_stars)
async def stars_set_handler(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text)
        if rate <= 0:
            await message.answer("❌ ПОЛОЖИТЕЛЬНОЕ ЧИСЛО")
            return
        update_setting('stars_rate', rate)
        await message.answer(f"✅ КУРС STARS: 1 = <code>{rate} ₽</code>")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_usdt")
async def set_usdt(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(
        callback.message,
        f"💵 ТЕКУЩИЙ КУРС: <code>{get_setting('usdt_rate')} ₽</code>\nВВЕДИ НОВЫЙ:"
    )
    await state.set_state(AdminSettingsStates.waiting_for_usdt)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_usdt)
async def usdt_set_handler(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text)
        if rate <= 0:
            await message.answer("❌ ПОЛОЖИТЕЛЬНОЕ ЧИСЛО")
            return
        update_setting('usdt_rate', rate)
        await message.answer(f"✅ КУРС USDT: 1 = <code>{rate} ₽</code>")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_discount")
async def set_discount(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(
        callback.message,
        f"🎁 ТЕКУЩАЯ СКИДКА: <b>{get_setting('referral_discount')}%</b>\nВВЕДИ НОВУЮ (0-100):"
    )
    await state.set_state(AdminSettingsStates.waiting_for_discount)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_discount)
async def discount_set_handler(message: types.Message, state: FSMContext):
    try:
        val = float(message.text)
        if val < 0 or val > 100:
            await message.answer("❌ ОТ 0 ДО 100")
            return
        update_setting('referral_discount', val)
        await message.answer(f"✅ СКИДКА: <b>{val}%</b>")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_reward")
async def set_reward(callback: types.CallbackQuery, state: FSMContext):
    await safe_edit_message(
        callback.message,
        f"💸 ТЕКУЩАЯ НАГРАДА: <b>{get_setting('referral_reward')}%</b>\nВВЕДИ НОВУЮ (0-100):"
    )
    await state.set_state(AdminSettingsStates.waiting_for_reward)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_reward)
async def reward_set_handler(message: types.Message, state: FSMContext):
    try:
        val = float(message.text)
        if val < 0 or val > 100:
            await message.answer("❌ ОТ 0 ДО 100")
            return
        update_setting('referral_reward', val)
        await message.answer(f"✅ НАГРАДА: <b>{val}%</b>")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_reviews_channel")
async def set_reviews_channel(callback: types.CallbackQuery, state: FSMContext):
    current = get_setting('reviews_channel_link') or "не настроен"
    await safe_edit_message(
        callback.message,
        f"📢 <b>НАСТРОЙКА КАНАЛА ДЛЯ ОТЗЫВОВ</b>\n\n"
        f"Текущий канал: {current}\n\n"
        f"Введите <b>ссылку на канал</b>:\n"
        f"• Для публичного канала: @username или https://t.me/username\n"
        f"• Для приватного канала: ссылка-приглашение https://t.me/+abc123"
    )
    await state.set_state(AdminSettingsStates.waiting_for_reviews_channel)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_reviews_channel)
async def process_reviews_channel(message: types.Message, state: FSMContext):
    channel_input = message.text.strip()
    
    if channel_input.startswith('@'):
        channel_link = f"https://t.me/{channel_input[1:]}"
    elif 't.me/' in channel_input:
        channel_link = channel_input
    else:
        channel_link = f"https://t.me/{channel_input}"
    
    update_setting('reviews_channel_link', channel_link)
    
    await message.answer(
        f"✅ <b>Канал для отзывов сохранен!</b>\n\n"
        f"Ссылка: {channel_link}"
    )
    await state.clear()

# ==================== НАВИГАЦИЯ ====================
@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    await safe_edit_message(callback.message, "⚙️ <b>АДМИН ПАНЕЛЬ</b>", admin_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await safe_edit_message(callback.message, "📭 КАТАЛОГ ПУСТ")
        await callback.answer()
        return
    await safe_edit_message(callback.message, "📦 <b>ВЫБЕРИ ТОВАР:</b>", catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(F.data == "back_to_balance")
async def back_to_balance(callback: types.CallbackQuery):
    bal = get_balance(callback.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 <b>ТВОЙ БАЛАНС:</b> <code>{bal} ₽</code>\n"
        f"⭐ ЭКВИВАЛЕНТ: <code>{int(bal/stars_rate)} STARS</code>\n\n"
        f"ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    )
    await safe_edit_message(callback.message, text, payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_purchases")
async def back_to_purchases(callback: types.CallbackQuery):
    purchases = get_user_purchases(callback.from_user.id)
    if not purchases:
        await safe_edit_message(callback.message, "📭 У ТЕБЯ НЕТ ПОКУПОК")
        await callback.answer()
        return
    await safe_edit_message(callback.message, "📜 <b>ТВОИ ПОКУПКИ:</b>", purchases_keyboard(purchases))
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await cmd_start(callback.message)
    await callback.answer()

# ==================== ЗАПУСК ====================
async def main():
    """Главная функция запуска бота"""
    global bot_username
    
    try:
        bot_info = await bot.get_me()
        bot_username = bot_info.username
        
        logger.info(f"🚀 БОТ @{bot_username} ЗАПУЩЕН!")
        logger.info("✅ Все системы работают")
        logger.info(f"👥 Администраторы: {ADMIN_IDS}")
        
        await dp.start_polling(bot, skip_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        traceback.print_exc()
