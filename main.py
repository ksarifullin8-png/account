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
async def show_ref_link(callback: types.Callback
