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
import zipfile
import json
import io
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple, Callable, Awaitable
import tempfile

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, PreCheckoutQuery, FSInputFile,
)
# Добавь это ОТДЕЛЬНО:
from aiogram.types import BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneMigrateError,
    NetworkMigrateError
)

# УДАЛИ ЭТУ СТРОКУ:
# from telethon.network import ConnectionTcpMTProxy

# ==================== НАСТРОЙКИ ====================
TOKEN = "8561605758:AAH7WUSKqYHm7zbOUkEapNP8_QSFQw9D0nA"
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
    
class AdminSessionCheckStates(StatesGroup):
    waiting_for_confirm = State()
    
class AdminDeleteStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_confirm = State()
    
class AdminSettingsStates(StatesGroup):
    waiting_for_stars = State()
    waiting_for_usdt = State()
    waiting_for_discount = State()
    waiting_for_reward = State()
    waiting_for_reviews_channel = State()

class MailingStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirm = State()
    
class CodeRetrievalStates(StatesGroup):
    waiting_for_zip = State()
    waiting_for_action = State()

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
    
    # Если пользователя нет и передан username - создаем
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

def load_proxies(filename="proxies.txt"):
    """Загружает список прокси из файла"""
    proxies = []
    try:
        with open(filename, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    # Поддерживаем форматы: ip:port, ip:port:user:pass, protocol://ip:port
                    proxies.append(line)
        logger.info(f"✅ Загружено {len(proxies)} прокси из {filename}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки прокси: {e}")
    return proxies

# Загружаем прокси при старте
proxy_list = load_proxies()

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

def add_product(name: str, price: float, phone: str, session_string: str, region: str, year: int, 
                password: str = None, spam_block: int = 0, register_date: str = None, account_age: int = 0) -> int:
    """
    Добавляет товар в базу данных.
    """
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    year = int(year) if year else datetime.now().year
    
    # Проверяем и добавляем новые колонки если их нет
    c.execute("PRAGMA table_info(products)")
    columns = [col[1] for col in c.fetchall()]
    
    if 'spam_block' not in columns:
        try:
            c.execute("ALTER TABLE products ADD COLUMN spam_block INTEGER DEFAULT 0")
            logger.info("✅ Добавлена колонка spam_block")
        except:
            pass
    
    if 'register_date' not in columns:
        try:
            c.execute("ALTER TABLE products ADD COLUMN register_date TEXT")
            logger.info("✅ Добавлена колонка register_date")
        except:
            pass
    
    if 'account_age' not in columns:
        try:
            c.execute("ALTER TABLE products ADD COLUMN account_age INTEGER DEFAULT 0")
            logger.info("✅ Добавлена колонка account_age")
        except:
            pass
    
    c.execute("""INSERT INTO products 
                 (name, price, phone, session_string, region, account_year, added_date, password, spam_block, register_date, account_age)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
              (name, price, phone, session_string, region, year, now, password, spam_block, register_date, account_age))
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
        
        async for message in client.iter_messages(777000, limit=50):
            if not message.text:
                continue
            
            found_codes = re.findall(r'\b\d{5}\b', message.text)
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
async def get_account_info(client):
    """
    Получает информацию об аккаунте с определением даты регистрации.
    Использует 3 разных метода для максимальной точности.
    """
    try:
        info = {
            'register_date': None,
            'register_timestamp': 0,
            'spam_block': 0,
            'phone': None,
            'username': None,
            'first_name': None,
            'last_name': None,
            'account_age_days': 0,
            'register_year': None
        }
        
        me = await client.get_me()
        info['phone'] = me.phone
        info['username'] = me.username
        info['first_name'] = me.first_name
        info['last_name'] = me.last_name
        
        # 🔥🔥🔥 МЕТОД 1: Через authorizations (САМЫЙ ТОЧНЫЙ) 🔥🔥🔥
        try:
            from telethon.tl.functions.account import GetAuthorizationsRequest
            auths = await client(GetAuthorizationsRequest())
            
            if auths and hasattr(auths, 'authorizations') and auths.authorizations:
                # Самая старая авторизация = дата создания
                sorted_auths = sorted(auths.authorizations, key=lambda x: x.date_created)
                oldest = sorted_auths[0]
                reg_timestamp = oldest.date_created
                reg_date = datetime.fromtimestamp(reg_timestamp)
                
                info['register_date'] = reg_date.strftime("%Y-%m-%d")
                info['register_timestamp'] = reg_timestamp
                info['register_year'] = reg_date.year
                
                days_old = (datetime.now() - reg_date).days
                info['account_age_days'] = days_old
                
                logger.info(f"✅ МЕТОД 1: Дата регистрации: {info['register_date']} (через authorizations)")
                return info
        except Exception as e:
            logger.warning(f"⚠️ МЕТОД 1 не сработал: {e}")
        
        # 🔥🔥🔥 МЕТОД 2: Через дату создания профиля 🔥🔥🔥
        try:
            from telethon.tl.functions.users import GetUsersRequest
            from telethon.tl.types import User
            
            users = await client(GetUsersRequest([me]))
            if users and users[0]:
                user = users[0]
                # Проверяем разные поля с датами
                if hasattr(user, 'date') and user.date:
                    created_date = user.date
                    if isinstance(created_date, datetime):
                        info['register_date'] = created_date.strftime("%Y-%m-%d")
                        info['register_timestamp'] = int(created_date.timestamp())
                        info['register_year'] = created_date.year
                        
                        days_old = (datetime.now() - created_date).days
                        info['account_age_days'] = days_old
                        
                        logger.info(f"✅ МЕТОД 2: Дата регистрации: {info['register_date']} (через profile)")
                        return info
        except Exception as e:
            logger.warning(f"⚠️ МЕТОД 2 не сработал: {e}")
        
        # 🔥🔥🔥 МЕТОД 3: Через ID аккаунта 🔥🔥🔥
        try:
            if me.id:
                # Формула для приблизительной даты по ID Telegram
                # ID Telegram содержит информацию о дате регистрации
                # Формула: (id - 1000000000000) / 2^22 ≈ timestamp
                approx_timestamp = (me.id - 1000000000000) / 4194304
                if approx_timestamp > 1262304000:  # > 2010 года
                    approx_date = datetime.fromtimestamp(approx_timestamp)
                    info['register_date'] = approx_date.strftime("%Y-%m-%d") + " (приблизительно)"
                    info['register_timestamp'] = int(approx_timestamp)
                    info['register_year'] = approx_date.year
                    
                    days_old = (datetime.now() - approx_date).days
                    info['account_age_days'] = days_old
                    
                    logger.info(f"✅ МЕТОД 3: Дата регистрации: {info['register_date']} (по ID)")
                    return info
        except Exception as e:
            logger.warning(f"⚠️ МЕТОД 3 не сработал: {e}")
        
        # 🔥🔥🔥 МЕТОД 4: Через год создания (fallback) 🔥🔥🔥
        if hasattr(me, 'date') and me.date:
            try:
                year = me.date.year
                info['register_year'] = year
                info['register_date'] = f"{year}-01-01 (только год)"
                logger.info(f"✅ МЕТОД 4: Год регистрации: {year}")
            except:
                pass
        
        # 🔥🔥🔥 ПРОВЕРКА СПАМБЛОКА 🔥🔥🔥
        try:
            # Пробуем отправить тестовое сообщение самому себе
            from telethon.tl.functions.messages import SendMessageRequest
            
            await client(SendMessageRequest(
                peer=await client.get_input_entity(me.id),
                message="test",
                random_id=random.randint(0, 2**63)
            ))
            info['spam_block'] = 0
            logger.info("🚫 Спамблок: НЕТ")
        except Exception as e:
            error_str = str(e)
            if 'FLOOD_WAIT' in error_str or 'RESTRICTED' in error_str:
                info['spam_block'] = 1
                logger.info("🚫 Спамблок: ЕСТЬ")
            else:
                info['spam_block'] = 0
        
        return info
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения информации: {e}")
        traceback.print_exc()
        return {
            'register_date': None,
            'register_timestamp': 0,
            'spam_block': 0,
            'phone': None,
            'username': None,
            'first_name': None,
            'last_name': None,
            'account_age_days': 0,
            'register_year': None
        }
        
async def detect_region(phone: str) -> str:
    # Европа
    if phone.startswith('+7') or phone.startswith('7'):
        return '🇷🇺 Россия'
    elif phone.startswith('+380') or phone.startswith('380'):
        return '🇺🇦 Украина'
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
    elif phone.startswith('+351'):
        return '🇵🇹 Португалия'
    elif phone.startswith('+31'):
        return '🇳🇱 Нидерланды'
    elif phone.startswith('+32'):
        return '🇧🇪 Бельгия'
    elif phone.startswith('+41'):
        return '🇨🇭 Швейцария'
    elif phone.startswith('+43'):
        return '🇦🇹 Австрия'
    elif phone.startswith('+45'):
        return '🇩🇰 Дания'
    elif phone.startswith('+46'):
        return '🇸🇪 Швеция'
    elif phone.startswith('+47'):
        return '🇳🇴 Норвегия'
    elif phone.startswith('+48'):
        return '🇵🇱 Польша'
    elif phone.startswith('+420'):
        return '🇨🇿 Чехия'
    elif phone.startswith('+421'):
        return '🇸🇰 Словакия'
    elif phone.startswith('+36'):
        return '🇭🇺 Венгрия'
    elif phone.startswith('+385'):
        return '🇭🇷 Хорватия'
    elif phone.startswith('+386'):
        return '🇸🇮 Словения'
    elif phone.startswith('+40'):
        return '🇷🇴 Румыния'
    elif phone.startswith('+359'):
        return '🇧🇬 Болгария'
    elif phone.startswith('+30'):
        return '🇬🇷 Греция'
    elif phone.startswith('+90'):
        return '🇹🇷 Турция'
    elif phone.startswith('+357'):
        return '🇨🇾 Кипр'
    elif phone.startswith('+353'):
        return '🇮🇪 Ирландия'
    elif phone.startswith('+354'):
        return '🇮🇸 Исландия'
    elif phone.startswith('+352'):
        return '🇱🇺 Люксембург'
    elif phone.startswith('+356'):
        return '🇲🇹 Мальта'
    elif phone.startswith('+377'):
        return '🇲🇨 Монако'
    elif phone.startswith('+378'):
        return '🇸🇲 Сан-Марино'
    elif phone.startswith('+379'):
        return '🇻🇦 Ватикан'
    elif phone.startswith('+381'):
        return '🇷🇸 Сербия'
    elif phone.startswith('+382'):
        return '🇲🇪 Черногория'
    elif phone.startswith('+383'):
        return '🇽🇰 Косово'
    elif phone.startswith('+387'):
        return '🇧🇦 Босния и Герцеговина'
    elif phone.startswith('+389'):
        return '🇲🇰 Северная Македония'
    elif phone.startswith('+355'):
        return '🇦🇱 Албания'
    elif phone.startswith('+373'):
        return '🇲🇩 Молдова'
    elif phone.startswith('+40'):
        return '🇷🇴 Румыния'
    elif phone.startswith('+370'):
        return '🇱🇹 Литва'
    elif phone.startswith('+371'):
        return '🇱🇻 Латвия'
    elif phone.startswith('+372'):
        return '🇪🇪 Эстония'
    elif phone.startswith('+375'):
        return '🇧🇾 Беларусь'
    elif phone.startswith('+994'):
        return '🇦🇿 Азербайджан'
    elif phone.startswith('+374'):
        return '🇦🇲 Армения'
    elif phone.startswith('+995'):
        return '🇬🇪 Грузия'
    
    # Азия
    elif phone.startswith('+86'):
        return '🇨🇳 Китай'
    elif phone.startswith('+81'):
        return '🇯🇵 Япония'
    elif phone.startswith('+82'):
        return '🇰🇷 Южная Корея'
    elif phone.startswith('+91'):
        return '🇮🇳 Индия'
    elif phone.startswith('+92'):
        return '🇵🇰 Пакистан'
    elif phone.startswith('+93'):
        return '🇦🇫 Афганистан'
    elif phone.startswith('+94'):
        return '🇱🇰 Шри-Ланка'
    elif phone.startswith('+95'):
        return '🇲🇲 Мьянма'
    elif phone.startswith('+960'):
        return '🇲🇻 Мальдивы'
    elif phone.startswith('+961'):
        return '🇱🇧 Ливан'
    elif phone.startswith('+962'):
        return '🇯🇴 Иордания'
    elif phone.startswith('+963'):
        return '🇸🇾 Сирия'
    elif phone.startswith('+964'):
        return '🇮🇶 Ирак'
    elif phone.startswith('+965'):
        return '🇰🇼 Кувейт'
    elif phone.startswith('+966'):
        return '🇸🇦 Саудовская Аравия'
    elif phone.startswith('+967'):
        return '🇾🇪 Йемен'
    elif phone.startswith('+968'):
        return '🇴🇲 Оман'
    elif phone.startswith('+971'):
        return '🇦🇪 ОАЭ'
    elif phone.startswith('+972'):
        return '🇮🇱 Израиль'
    elif phone.startswith('+973'):
        return '🇧🇭 Бахрейн'
    elif phone.startswith('+974'):
        return '🇶🇦 Катар'
    elif phone.startswith('+975'):
        return '🇧🇹 Бутан'
    elif phone.startswith('+976'):
        return '🇲🇳 Монголия'
    elif phone.startswith('+977'):
        return '🇳🇵 Непал'
    elif phone.startswith('+98'):
        return '🇮🇷 Иран'
    elif phone.startswith('+992'):
        return '🇹🇯 Таджикистан'
    elif phone.startswith('+993'):
        return '🇹🇲 Туркменистан'
    elif phone.startswith('+994'):
        return '🇦🇿 Азербайджан'
    elif phone.startswith('+995'):
        return '🇬🇪 Грузия'
    elif phone.startswith('+996'):
        return '🇰🇬 Киргизия'
    elif phone.startswith('+997'):
        return '🇰🇿 Казахстан'
    elif phone.startswith('+998'):
        return '🇺🇿 Узбекистан'
    
    # Америка
    elif phone.startswith('+1'):
        return '🇺🇸 США/Канада'
    elif phone.startswith('+52'):
        return '🇲🇽 Мексика'
    elif phone.startswith('+53'):
        return '🇨🇺 Куба'
    elif phone.startswith('+54'):
        return '🇦🇷 Аргентина'
    elif phone.startswith('+55'):
        return '🇧🇷 Бразилия'
    elif phone.startswith('+56'):
        return '🇨🇱 Чили'
    elif phone.startswith('+57'):
        return '🇨🇴 Колумбия'
    elif phone.startswith('+58'):
        return '🇻🇪 Венесуэла'
    elif phone.startswith('+591'):
        return '🇧🇴 Боливия'
    elif phone.startswith('+592'):
        return '🇬🇾 Гайана'
    elif phone.startswith('+593'):
        return '🇪🇨 Эквадор'
    elif phone.startswith('+594'):
        return '🇬🇫 Французская Гвиана'
    elif phone.startswith('+595'):
        return '🇵🇾 Парагвай'
    elif phone.startswith('+596'):
        return '🇲🇶 Мартиника'
    elif phone.startswith('+597'):
        return '🇸🇷 Суринам'
    elif phone.startswith('+598'):
        return '🇺🇾 Уругвай'
    elif phone.startswith('+599'):
        return '🇧🇶 Бонэйр/Кюрасао'
    
    # Африка
    elif phone.startswith('+20'):
        return '🇪🇬 Египет'
    elif phone.startswith('+27'):
        return '🇿🇦 ЮАР'
    elif phone.startswith('+211'):
        return '🇸🇸 Южный Судан'
    elif phone.startswith('+212'):
        return '🇲🇦 Марокко'
    elif phone.startswith('+213'):
        return '🇩🇿 Алжир'
    elif phone.startswith('+216'):
        return '🇹🇳 Тунис'
    elif phone.startswith('+218'):
        return '🇱🇾 Ливия'
    elif phone.startswith('+220'):
        return '🇬🇲 Гамбия'
    elif phone.startswith('+221'):
        return '🇸🇳 Сенегал'
    elif phone.startswith('+222'):
        return '🇲🇷 Мавритания'
    elif phone.startswith('+223'):
        return '🇲🇱 Мали'
    elif phone.startswith('+224'):
        return '🇬🇳 Гвинея'
    elif phone.startswith('+225'):
        return '🇨🇮 Кот-дИвуар'
    elif phone.startswith('+226'):
        return '🇧🇫 Буркина-Фасо'
    elif phone.startswith('+227'):
        return '🇳🇪 Нигер'
    elif phone.startswith('+228'):
        return '🇹🇬 Того'
    elif phone.startswith('+229'):
        return '🇧🇯 Бенин'
    elif phone.startswith('+230'):
        return '🇲🇺 Маврикий'
    elif phone.startswith('+231'):
        return '🇱🇷 Либерия'
    elif phone.startswith('+232'):
        return '🇸🇱 Сьерра-Леоне'
    elif phone.startswith('+233'):
        return '🇬🇭 Гана'
    elif phone.startswith('+234'):
        return '🇳🇬 Нигерия'
    elif phone.startswith('+235'):
        return '🇹🇩 Чад'
    elif phone.startswith('+236'):
        return '🇨🇫 ЦАР'
    elif phone.startswith('+237'):
        return '🇨🇲 Камерун'
    elif phone.startswith('+238'):
        return '🇨🇻 Кабо-Верде'
    elif phone.startswith('+239'):
        return '🇸🇹 Сан-Томе'
    elif phone.startswith('+240'):
        return '🇬🇶 Экваториальная Гвинея'
    elif phone.startswith('+241'):
        return '🇬🇦 Габон'
    elif phone.startswith('+242'):
        return '🇨🇬 Республика Конго'
    elif phone.startswith('+243'):
        return '🇨🇩 ДР Конго'
    elif phone.startswith('+244'):
        return '🇦🇴 Ангола'
    elif phone.startswith('+245'):
        return '🇬🇼 Гвинея-Бисау'
    elif phone.startswith('+246'):
        return '🇮🇴 Диего-Гарсия'
    elif phone.startswith('+247'):
        return '🇦🇨 Остров Вознесения'
    elif phone.startswith('+248'):
        return '🇸🇨 Сейшелы'
    elif phone.startswith('+249'):
        return '🇸🇩 Судан'
    elif phone.startswith('+250'):
        return '🇷🇼 Руанда'
    elif phone.startswith('+251'):
        return '🇪🇹 Эфиопия'
    elif phone.startswith('+252'):
        return '🇸🇴 Сомали'
    elif phone.startswith('+253'):
        return '🇩🇯 Джибути'
    elif phone.startswith('+254'):
        return '🇰🇪 Кения'
    elif phone.startswith('+255'):
        return '🇹🇿 Танзания'
    elif phone.startswith('+256'):
        return '🇺🇬 Уганда'
    elif phone.startswith('+257'):
        return '🇧🇮 Бурунди'
    elif phone.startswith('+258'):
        return '🇲🇿 Мозамбик'
    elif phone.startswith('+260'):
        return '🇿🇲 Замбия'
    elif phone.startswith('+261'):
        return '🇲🇬 Мадагаскар'
    elif phone.startswith('+262'):
        return '🇷🇪 Реюньон'
    elif phone.startswith('+263'):
        return '🇿🇼 Зимбабве'
    elif phone.startswith('+264'):
        return '🇳🇦 Намибия'
    elif phone.startswith('+265'):
        return '🇲🇼 Малави'
    elif phone.startswith('+266'):
        return '🇱🇸 Лесото'
    elif phone.startswith('+267'):
        return '🇧🇼 Ботсвана'
    elif phone.startswith('+268'):
        return '🇸🇿 Эсватини'
    elif phone.startswith('+269'):
        return '🇰🇲 Коморы'
    elif phone.startswith('+290'):
        return '🇸🇭 Остров Святой Елены'
    elif phone.startswith('+291'):
        return '🇪🇷 Эритрея'
    
    # Океания
    elif phone.startswith('+61'):
        return '🇦🇺 Австралия'
    elif phone.startswith('+64'):
        return '🇳🇿 Новая Зеландия'
    elif phone.startswith('+62'):
        return '🇮🇩 Индонезия'
    elif phone.startswith('+63'):
        return '🇵🇭 Филиппины'
    elif phone.startswith('+65'):
        return '🇸🇬 Сингапур'
    elif phone.startswith('+66'):
        return '🇹🇭 Таиланд'
    elif phone.startswith('+60'):
        return '🇲🇾 Малайзия'
    elif phone.startswith('+673'):
        return '🇧🇳 Бруней'
    elif phone.startswith('+674'):
        return '🇳🇷 Науру'
    elif phone.startswith('+675'):
        return '🇵🇬 Папуа - Новая Гвинея'
    elif phone.startswith('+676'):
        return '🇹🇴 Тонга'
    elif phone.startswith('+677'):
        return '🇸🇧 Соломоновы Острова'
    elif phone.startswith('+678'):
        return '🇻🇺 Вануату'
    elif phone.startswith('+679'):
        return '🇫🇯 Фиджи'
    elif phone.startswith('+680'):
        return '🇵🇼 Палау'
    elif phone.startswith('+681'):
        return '🇼🇫 Уоллис и Футуна'
    elif phone.startswith('+682'):
        return '🇨🇰 Острова Кука'
    elif phone.startswith('+683'):
        return '🇳🇺 Ниуэ'
    elif phone.startswith('+685'):
        return '🇼🇸 Самоа'
    elif phone.startswith('+686'):
        return '🇰🇮 Кирибати'
    elif phone.startswith('+687'):
        return '🇳🇨 Новая Каледония'
    elif phone.startswith('+688'):
        return '🇹🇻 Тувалу'
    elif phone.startswith('+689'):
        return '🇵🇫 Французская Полинезия'
    
    # Острова и территории
    elif phone.startswith('+350'):
        return '🇬🇮 Гибралтар'
    elif phone.startswith('+352'):
        return '🇱🇺 Люксембург'
    elif phone.startswith('+353'):
        return '🇮🇪 Ирландия'
    elif phone.startswith('+354'):
        return '🇮🇸 Исландия'
    elif phone.startswith('+355'):
        return '🇦🇱 Албания'
    elif phone.startswith('+356'):
        return '🇲🇹 Мальта'
    elif phone.startswith('+357'):
        return '🇨🇾 Кипр'
    elif phone.startswith('+358'):
        return '🇫🇮 Финляндия'
    elif phone.startswith('+359'):
        return '🇧🇬 Болгария'
    elif phone.startswith('+298'):
        return '🇫🇴 Фарерские острова'
    elif phone.startswith('+299'):
        return '🇬🇱 Гренландия'
    elif phone.startswith('+500'):
        return '🇫🇰 Фолкленды'
    elif phone.startswith('+501'):
        return '🇧🇿 Белиз'
    elif phone.startswith('+502'):
        return '🇬🇹 Гватемала'
    elif phone.startswith('+503'):
        return '🇸🇻 Сальвадор'
    elif phone.startswith('+504'):
        return '🇭🇳 Гондурас'
    elif phone.startswith('+505'):
        return '🇳🇮 Никарагуа'
    elif phone.startswith('+506'):
        return '🇨🇷 Коста-Рика'
    elif phone.startswith('+507'):
        return '🇵🇦 Панама'
    elif phone.startswith('+508'):
        return '🇵🇲 Сен-Пьер'
    elif phone.startswith('+509'):
        return '🇭🇹 Гаити'
    elif phone.startswith('+590'):
        return '🇬🇵 Гваделупа'
    elif phone.startswith('+591'):
        return '🇧🇴 Боливия'
    elif phone.startswith('+592'):
        return '🇬🇾 Гайана'
    elif phone.startswith('+593'):
        return '🇪🇨 Эквадор'
    elif phone.startswith('+594'):
        return '🇬🇫 Гвиана'
    elif phone.startswith('+595'):
        return '🇵🇾 Парагвай'
    elif phone.startswith('+596'):
        return '🇲🇶 Мартиника'
    elif phone.startswith('+597'):
        return '🇸🇷 Суринам'
    elif phone.startswith('+598'):
        return '🇺🇾 Уругвай'
    elif phone.startswith('+599'):
        return '🇨🇼 Кюрасао'
    elif phone.startswith('+670'):
        return '🇹🇱 Восточный Тимор'
    elif phone.startswith('+671'):
        return '🇬🇺 Гуам'
    elif phone.startswith('+672'):
        return '🇦🇶 Антарктида'
    elif phone.startswith('+673'):
        return '🇧🇳 Бруней'
    elif phone.startswith('+674'):
        return '🇳🇷 Науру'
    elif phone.startswith('+675'):
        return '🇵🇬 Папуа'
    elif phone.startswith('+676'):
        return '🇹🇴 Тонга'
    elif phone.startswith('+677'):
        return '🇸🇧 Соломоны'
    elif phone.startswith('+678'):
        return '🇻🇺 Вануату'
    elif phone.startswith('+679'):
        return '🇫🇯 Фиджи'
    elif phone.startswith('+680'):
        return '🇵🇼 Палау'
    elif phone.startswith('+681'):
        return '🇼🇫 Уоллис'
    elif phone.startswith('+682'):
        return '🇨🇚 Острова Кука'
    elif phone.startswith('+683'):
        return '🇳🇺 Ниуэ'
    elif phone.startswith('+684'):
        return '🇦🇸 Самоа'
    elif phone.startswith('+685'):
        return '🇼🇸 Самоа'
    elif phone.startswith('+686'):
        return '🇰🇮 Кирибати'
    elif phone.startswith('+687'):
        return '🇳🇨 Каледония'
    elif phone.startswith('+688'):
        return '🇹🇻 Тувалу'
    elif phone.startswith('+689'):
        return '🇵🇫 Полинезия'
    elif phone.startswith('+690'):
        return '🇹🇰 Токелау'
    elif phone.startswith('+691'):
        return '🇫🇲 Микронезия'
    elif phone.startswith('+692'):
        return '🇲🇭 Маршаллы'
    elif phone.startswith('+800'):
        return '🌐 Международный'
    else:
        return '🌍 Другая страна'

async def login_to_telegram(phone: str) -> Dict[str, Any]:
    """
    Вход в Telegram аккаунт с использованием прокси.
    """
    try:
        logger.info(f"🔄 Начинаем вход для номера: {phone}")
        
        # Очищаем номер
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
                
                temp_clients[phone] = {
                    'client': client,
                    'phone_code_hash': None
                }
                
                return {
                    'success': True,
                    'session': session_string,
                    'region': region,
                    'year': year,
                    'already_logged': True,
                    'phone': phone,
                    'client': client
                }
        
        # 🔥🔥🔥 СОЗДАЁМ КЛИЕНТА С ПРОКСИ 🔥🔥🔥
        client = await create_client_with_proxy()
        await client.connect()
        
        # Проверяем авторизацию
        if await client.is_user_authorized():
            me = await client.get_me()
            logger.info(f"✅ Уже авторизован: {me.phone}")
            session_string = client.session.save()
            region = await detect_region(phone)
            year = getattr(me, 'date', None)
            year = year.year if year and hasattr(year, 'year') else datetime.now().year
            active_sessions[phone] = session_string
            temp_clients[phone] = {
                'client': client,
                'phone_code_hash': None
            }
            return {
                'success': True,
                'session': session_string,
                'region': region,
                'year': year,
                'already_logged': True,
                'phone': phone,
                'client': client
            }
        
        # Отправляем код
        logger.info(f"📱 Отправляем код на {phone}")
        
        try:
            result = await client.send_code_request(phone)
        except PhoneMigrateError as e:
            logger.info(f"🔄 Миграция в DC {e.new_dc}")
            client.session.set_dc(e.new_dc)
            await client.connect()
            result = await client.send_code_request(phone)
        
        # Сохраняем данные
        temp_clients[phone] = {
            'client': client,
            'phone_code_hash': result.phone_code_hash
        }
        
        logger.info(f"✅ Код отправлен через прокси, phone_code_hash: {result.phone_code_hash}")
        
        return {
            'success': True, 
            'need_code': True, 
            'phone': phone
        }
    
    except Exception as e:
        logger.error(f"❌ Ошибка входа: {e}")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
        
async def verify_code(phone: str, code: str) -> Dict[str, Any]:
    """
    Подтверждение кода для виртуальных номеров.
    """
    try:
        logger.info(f"\n🔍 VERIFY_CODE: Проверка кода для {phone}")
        
        client_data = temp_clients.get(phone)
        if not client_data:
            logger.error(f"❌ VERIFY_CODE: Клиент не найден!")
            return {'success': False, 'error': '❌ Сессия истекла'}
        
        client = client_data.get('client')
        phone_code_hash = client_data.get('phone_code_hash')
        
        if not client:
            logger.error(f"❌ VERIFY_CODE: Клиент не найден в данных!")
            return {'success': False, 'error': '❌ Ошибка данных клиента'}
        
        if not client.is_connected():
            await client.connect()
        
        try:
            await client.sign_in(
                phone=phone,
                code=code,
                phone_code_hash=phone_code_hash
            )
        except SessionPasswordNeededError:
            logger.info("🔐 Требуется 2FA пароль")
            temp_clients[phone] = {
                'client': client,
                'phone_code_hash': phone_code_hash
            }
            return {'success': True, 'need_password': True, 'phone': phone}
        
        # ✅ После успешного входа получаем информацию об аккаунте
        logger.info(f"✅ VERIFY_CODE: Код принят! Получаем данные аккаунта...")
        
        # 🔥🔥🔥 ПОЛУЧАЕМ ИНФОРМАЦИЮ ОБ АККАУНТЕ 🔥🔥🔥
        account_info = await get_account_info(client)
        
        # Получаем информацию об аккаунте
        me = await client.get_me()
        session_string = client.session.save()
        region = await detect_region(phone)
        year = getattr(me, 'date', None)
        year = year.year if year and hasattr(year, 'year') else datetime.now().year
        
        # Если account_info дала год, используем его
        if account_info.get('register_year'):
            year = account_info['register_year']
        
        active_sessions[phone] = session_string
        
        temp_clients[phone] = {
            'client': client,
            'phone_code_hash': None
        }
        
        return {
            'success': True,
            'session': session_string,
            'region': region,
            'year': year,
            'phone': phone,
            'client': client,
            'account_info': account_info
        }
    
    except PhoneCodeInvalidError:
        logger.error(f"❌ VERIFY_CODE: Неверный код")
        return {'success': False, 'error': '❌ Неверный код'}
    
    except Exception as e:
        logger.error(f"❌ VERIFY_CODE: Ошибка: {e}")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}
      
async def verify_password(phone: str, password: str) -> Dict[str, Any]:
    """
    Подтверждение 2FA пароля с получением информации об аккаунте.
    """
    try:
        logger.info(f"\n🔐 VERIFY_PASSWORD: Проверка 2FA для {phone}")
        
        # Получаем данные клиента из temp_clients
        client_data = temp_clients.get(phone)
        if not client_data:
            logger.error(f"❌ VERIFY_PASSWORD: Клиент не найден!")
            return {'success': False, 'error': '❌ Сессия истекла'}
        
        # Извлекаем клиент из словаря
        client = client_data.get('client')
        
        if not client:
            logger.error(f"❌ VERIFY_PASSWORD: Клиент не найден в данных!")
            return {'success': False, 'error': '❌ Ошибка данных клиента'}
        
        logger.info(f"✅ VERIFY_PASSWORD: Клиент найден")
        
        # Подключаемся если нужно
        if not client.is_connected():
            logger.info(f"🔄 VERIFY_PASSWORD: Подключаемся...")
            await client.connect()
        
        # Отправляем 2FA пароль
        await client.sign_in(password=password)
        
        logger.info(f"✅ VERIFY_PASSWORD: 2FA пароль принят!")
        
        # 🔥🔥🔥 ПОЛУЧАЕМ ИНФОРМАЦИЮ ОБ АККАУНТЕ 🔥🔥🔥
        account_info = await get_account_info(client)
        
        # Получаем информацию об аккаунте
        me = await client.get_me()
        session_string = client.session.save()
        region = await detect_region(phone)
        year = getattr(me, 'date', None)
        year = year.year if year and hasattr(year, 'year') else datetime.now().year
        
        # Если account_info дала год, используем его
        if account_info.get('register_year'):
            year = account_info['register_year']
        
        # Сохраняем сессию
        active_sessions[phone] = session_string
        
        # Обновляем temp_clients
        temp_clients[phone] = {
            'client': client,
            'phone_code_hash': None
        }
        
        return {
            'success': True,
            'session': session_string,
            'region': region,
            'year': year,
            'phone': phone,
            'client': client,
            'account_info': account_info
        }
    
    except Exception as e:
        logger.error(f"❌ VERIFY_PASSWORD: Ошибка: {e}")
        traceback.print_exc()
        return {'success': False, 'error': str(e)}

async def check_session_valid(session_string: str) -> Dict[str, Any]:
    """
    Проверяет работоспособность сессии.
    Возвращает словарь с результатом проверки.
    """
    client = None
    try:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            return {
                'valid': False,
                'error': '❌ Сессия не авторизована'
            }
        
        me = await client.get_me()
        
        # Проверяем возможность отправки сообщения (не спам-блок)
        try:
            from telethon.tl.functions.messages import SendMessageRequest
            await client(SendMessageRequest(
                peer=await client.get_input_entity(me.id),
                message="test",
                random_id=random.randint(0, 2**63)
            ))
            spam_block = False
        except Exception as e:
            spam_block = True if 'FLOOD_WAIT' in str(e) or 'RESTRICTED' in str(e) else False
        
        await client.disconnect()
        
        return {
            'valid': True,
            'phone': me.phone,
            'username': me.username,
            'first_name': me.first_name,
            'spam_block': spam_block
        }
        
    except Exception as e:
        return {
            'valid': False,
            'error': str(e)
        }
    finally:
        if client and client.is_connected():
            await client.disconnect()
          
async def check_all_sessions() -> Dict[str, Any]:
    """
    Проверяет все сессии в таблице products.
    Возвращает статистику.
    """
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT id, name, phone, session_string FROM products WHERE session_string IS NOT NULL AND session_string != ''")
    products = c.fetchall()
    conn.close()
    
    if not products:
        return {'total': 0, 'valid': 0, 'invalid': 0, 'details': []}
    
    valid_count = 0
    invalid_count = 0
    details = []
    
    for pid, name, phone, session_string in products:
        result = await check_session_valid(session_string)
        
        if result['valid']:
            valid_count += 1
            status = '✅'
            info = f"{phone} | {name[:20]}"
        else:
            invalid_count += 1
            status = '❌'
            info = f"{phone} | {name[:20]} | Ошибка: {result.get('error', 'Неизвестно')[:50]}"
        
        details.append({
            'id': pid,
            'name': name,
            'phone': phone,
            'valid': result['valid'],
            'status': status,
            'info': info,
            'error': result.get('error') if not result['valid'] else None,
            'spam_block': result.get('spam_block', False) if result['valid'] else None
        })
        
        # Небольшая задержка, чтобы не перегружать API
        await asyncio.sleep(1)
    
    return {
        'total': len(products),
        'valid': valid_count,
        'invalid': invalid_count,
        'details': details
    } 
    
async def auto_check_sessions():
    """Автоматическая проверка сессий каждые 24 часа"""
    while True:
        try:
            logger.info("🔄 Запуск автоматической проверки сессий...")
            
            result = await check_all_sessions()
            
            # Отправляем отчёт админам
            text = (
                f"📊 <b>АВТОМАТИЧЕСКАЯ ПРОВЕРКА СЕССИЙ</b>\n\n"
                f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                f"👥 Всего аккаунтов: <b>{result['total']}</b>\n"
                f"✅ Рабочих: <b>{result['valid']}</b>\n"
                f"❌ Невалидных: <b>{result['invalid']}</b>\n\n"
            )
            
            if result['invalid'] > 0:
                text += "🚨 <b>ПРОБЛЕМНЫЕ АККАУНТЫ:</b>\n\n"
                for detail in result['details']:
                    if not detail['valid']:
                        text += f"🆔 {detail['id']} | 📱 {detail['phone']}\n"
                        text += f"❌ {detail['error'][:100]}\n\n"
                        
                        if len(text) > 3500:
                            break
            
            text += f"\n🔄 Следующая проверка через 24 часа"
            
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, text)
                except:
                    pass
            
            logger.info(f"✅ Автопроверка завершена: {result['valid']}/{result['total']} рабочих")
            
        except Exception as e:
            logger.error(f"❌ Ошибка автопроверки: {e}")
        
        # Ждём 24 часа
        await asyncio.sleep(24 * 60 * 60)
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

async def create_session_zip(product_ids: list) -> bytes:
    try:
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()

        products_data = []
        for pid in product_ids:
            c.execute("SELECT phone, session_string FROM products WHERE id = ?", (pid,))
            product = c.fetchone()
            if product and product[1]:
                phone = product[0]
                phone_clean = re.sub(r'[^\d]', '', phone)

                products_data.append({
                    "phone": phone_clean,
                    "session_string": product[1]
                })

        conn.close()

        if not products_data:
            return None

        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for product in products_data:
                phone = product["phone"]
                session_string = product["session_string"]

                try:
                    # 🔥 создаём временный файл сессии
                    temp_dir = tempfile.gettempdir()
                    session_path = os.path.join(temp_dir, f"{phone}")

                    # создаём клиент
                    client = TelegramClient(
                        session_path,
                        API_ID,
                        API_HASH
                    )

                    await client.connect()

                    # загружаем StringSession
                    client.session = StringSession(session_string)

                    # сохраняем в sqlite
                    client.session.save()

                    await client.disconnect()

                    # читаем файл
                    session_file_path = session_path + ".session"

                    if os.path.exists(session_file_path):
                        with open(session_file_path, "rb") as f:
                            zip_file.writestr(f"{phone}.session", f.read())

                        # удаляем временный файл
                        os.remove(session_file_path)

                except Exception as e:
                    print(f"Ошибка создания .session для {phone}: {e}")

        zip_buffer.seek(0)
        return zip_buffer.getvalue()

    except Exception as e:
        print(f"❌ Ошибка ZIP: {e}")
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
        [InlineKeyboardButton(text="📱 УДАЛИТЬ ПО НОМЕРУ", callback_data="admin_delete_by_phone")],
        [InlineKeyboardButton(text="📦 СПИСОК ТОВАРОВ", callback_data="admin_list_products")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ СЕССИИ", callback_data="admin_download_sessions")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ БАЗУ ДАННЫХ", callback_data="admin_download_db")],  # ← НОВАЯ КНОПКА
        [InlineKeyboardButton(text="🔍 ПРОВЕРИТЬ СЕССИИ", callback_data="admin_check_sessions")],
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

async def get_codes_from_session_file_v2(client: TelegramClient, limit: int = 10) -> List[Dict]:
    """
    Получает коды из уже подключенного клиента.
    """
    codes = []
    try:
        logger.info("🔍 Ищу коды в аккаунте...")
        
        # Ищем сообщения с кодами (от 777000 - официальный бот Telegram)
        async for message in client.iter_messages(777000, limit=100):
            if not message.text:
                continue

            text_lower = message.text.lower()
            found_codes = re.findall(r'\b(\d{4,8})\b', message.text)

            for code in found_codes:
                if len(code) > 6:
                    continue
                    
                # Определяем тип кода
                if any(word in text_lower for word in ['2fa', 'пароль', 'password', 'two-step']):
                    code_type = "🔒 2FA"
                elif any(word in text_lower for word in ['code', 'код', 'login', 'вход', 'confirmation']):
                    code_type = "🔐 Telegram"
                else:
                    continue

                msg_date = message.date.strftime("%d.%m.%Y %H:%M")
                codes.append({
                    'code': code,
                    'type': code_type,
                    'date': msg_date,
                    'text': message.text[:100]
                })
                
                if len(codes) >= limit:
                    break
                    
            if len(codes) >= limit:
                break
        
        # Если не нашли в 777000, ищем во всех диалогах
        if not codes:
            logger.info("Не найдено в 777000, ищу во всех диалогах...")
            async for message in client.iter_messages(None, limit=200):
                if not message.text:
                    continue
                    
                text_lower = message.text.lower()
                if 'login code' in text_lower or 'код подтверждения' in text_lower:
                    found_codes = re.findall(r'\b(\d{4,8})\b', message.text)
                    for code in found_codes:
                        if 4 <= len(code) <= 6:
                            codes.append({
                                'code': code,
                                'type': "🔐 Telegram",
                                'date': message.date.strftime("%d.%m.%Y %H:%M"),
                                'text': message.text[:100]
                            })
                            if len(codes) >= limit:
                                break
                if len(codes) >= limit:
                    break
        
        logger.info(f"✅ Найдено кодов: {len(codes)}")
        return codes[:limit]
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения кодов: {e}")
        return []
        
async def get_codes_from_session_file(session_path: str, limit: int = 5) -> List[Dict]:
    """
    Подключается к аккаунту по бинарному .session файлу и получает последние коды.
    """
    codes = []
    client = None
    try:
        client = TelegramClient(session_path, API_ID, API_HASH)
        await client.connect()
        if not await client.is_user_authorized():
            logger.error("❌ Сессия не авторизована")
            return []

        # Ищем сообщения с кодами
        async for message in client.iter_messages(None, limit=200):
            if not message.text:
                continue

            text_lower = message.text.lower()
            found_codes = re.findall(r'\b(\d{4,8})\b', message.text)

            for code in found_codes:
                if len(code) > 6:
                    continue
                if any(word in text_lower for word in ['2fa', 'пароль', 'password']):
                    code_type = "🔒 2FA"
                elif any(word in text_lower for word in ['code', 'код', 'login', 'вход']):
                    code_type = "🔐 Telegram"
                else:
                    continue

                msg_date = message.date.strftime("%d.%m.%Y %H:%M")
                codes.append({
                    'code': code,
                    'type': code_type,
                    'date': msg_date,
                    'text': message.text[:100]
                })
                if len(codes) >= limit:
                    break
            if len(codes) >= limit:
                break

        await client.disconnect()
        return codes[:limit]

    except Exception as e:
        logger.error(f"❌ Ошибка получения кодов: {e}")
        return []
    finally:
        if client and client.is_connected():
            await client.disconnect()

async def get_codes_from_session(session_string: str, limit: int = 5) -> List[Dict]:
    """
    Подключается к аккаунту по сессии и получает последние коды.
    Возвращает список словарей с кодом, типом, датой.
    """
    codes = []
    client = None
    try:
        client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            logger.error("❌ Сессия не авторизована")
            return []
        
        logger.info("✅ Подключен к аккаунту, ищу коды...")
        
        # Ищем сообщения с кодами
        async for message in client.iter_messages(None, limit=200):
            if not message.text:
                continue
            
            text_lower = message.text.lower()
            found_codes = re.findall(r'\b(\d{4,8})\b', message.text)
            
            for code in found_codes:
                # Пропускаем слишком длинные числа (номера телефонов)
                if len(code) > 6:
                    continue
                    
                # Определяем тип кода
                if any(word in text_lower for word in ['2fa', 'пароль', 'password']):
                    code_type = "🔒 2FA"
                elif any(word in text_lower for word in ['code', 'код', 'login', 'вход']):
                    code_type = "🔐 Telegram"
                else:
                    continue
                
                msg_date = message.date.strftime("%d.%m.%Y %H:%M")
                codes.append({
                    'code': code,
                    'type': code_type,
                    'date': msg_date,
                    'text': message.text[:100]
                })
                
                if len(codes) >= limit:
                    break
            
            if len(codes) >= limit:
                break
        
        await client.disconnect()
        logger.info(f"✅ Найдено кодов: {len(codes)}")
        return codes[:limit]
        
    except Exception as e:
        logger.error(f"❌ Ошибка получения кодов: {e}")
        return []
    finally:
        if client and client.is_connected():
            await client.disconnect()
            
async def create_client_with_proxy(proxy_string=None):
    """Создаёт клиента с прокси или без"""
    try:
        # Если есть список прокси, выбираем случайный
        if proxy_list:
            proxy_string = random.choice(proxy_list)
            logger.info(f"🔌 Использую прокси: {proxy_string}")
            
            # Парсим строку прокси
            if ':' in proxy_string:
                parts = proxy_string.split(':')
                if len(parts) == 2:
                    # ip:port
                    return TelegramClient(
                        StringSession(), 
                        API_ID, 
                        API_HASH,
                        proxy=('socks5', parts[0], int(parts[1]))
                    )
                elif len(parts) == 4:
                    # ip:port:user:pass
                    return TelegramClient(
                        StringSession(), 
                        API_ID, 
                        API_HASH,
                        proxy=('socks5', parts[0], int(parts[1]), True, parts[2], parts[3])
                    )
        
        # Если нет прокси, создаём без
        return TelegramClient(StringSession(), API_ID, API_HASH)
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания клиента: {e}")
        return TelegramClient(StringSession(), API_ID, API_HASH)
        
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
    
    # ⚠️ ВАЖНО: даже если username нет, передаем хотя бы user_id для создания
    user = get_user(user_id, message.from_user.username)
    
    # Если пользователя всё равно нет - создаем принудительно
    if not user:
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        referral_code = generate_referral_code(user_id)
        
        c.execute("""INSERT INTO users 
                     (user_id, username, registered_date, referral_code, first_discount_used)
                     VALUES (?, ?, ?, ?, ?)""",
                  (user_id, message.from_user.username or f"user_{user_id}", now, referral_code, 1))
        conn.commit()
        conn.close()
        user = get_user(user_id)
    
    # Проверяем реферальный код
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
    
    # Распаковываем с учетом новых полей
    if len(product) >= 11:
        product_id, name, price, phone, session, region, year, added, password, spam_block, register_date = product[:11]
    else:
        product_id, name, price, phone, session, region, year, added, password = product[:9]
        spam_block = 0
        register_date = None
    
    age = datetime.now().year - year
    stars_price = int(price / get_setting('stars_rate'))
    
    # Форматируем дату регистрации
    reg_date_text = register_date if register_date else "неизвестно"
    
    # Спамблок
    spam_text = "✅ НЕТ" if spam_block == 0 else "❌ ЕСТЬ"
    
    text = (
        f"📦 <b>{name}</b>\n\n"
        f"🌍 <b>РЕГИОН:</b> {region}\n"
        f"📅 <b>ГОД РЕГИСТРАЦИИ НА ПРОДАЖУ:</b> {year} ({age} ЛЕТ)\n"
        f"📆 <b>ДАТА РЕГАККА:</b> {reg_date_text}\n"
        f"🚫 <b>СПАМБЛОК:</b> {spam_text}\n"
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

@dp.callback_query(F.data == "admin_check_sessions")
async def admin_check_sessions_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало ручной проверки сессий"""
    await safe_edit_message(
        callback.message,
        "🔍 <b>ПРОВЕРКА СЕССИЙ</b>\n\n"
        "Выбери действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 ПРОВЕРИТЬ ВСЕ СЕССИИ", callback_data="check_all_sessions")],
            [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "check_all_sessions")
async def admin_check_all_sessions(callback: types.CallbackQuery):
    """Ручная проверка всех сессий"""
    await callback.message.edit_text("🔄 НАЧИНАЮ ПРОВЕРКУ ВСЕХ СЕССИЙ...\n\nЭто может занять несколько минут...")
    await callback.answer()
    
    try:
        result = await check_all_sessions()
        
        text = (
            f"📊 <b>РЕЗУЛЬТАТ ПРОВЕРКИ СЕССИЙ</b>\n\n"
            f"📅 {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
            f"👥 Всего аккаунтов: <b>{result['total']}</b>\n"
            f"✅ Рабочих: <b>{result['valid']}</b>\n"
            f"❌ Невалидных: <b>{result['invalid']}</b>\n\n"
        )
        
        if result['details']:
            text += "📋 <b>ДЕТАЛИ:</b>\n\n"
            for detail in result['details'][:20]:  # Показываем первые 20
                if detail['valid']:
                    text += f"{detail['status']} 🆔 {detail['id']} | 📱 {detail['phone']}\n"
                    if detail.get('spam_block'):
                        text += f"   ⚠️ ВНИМАНИЕ: Аккаунт в спам-блоке!\n"
                else:
                    text += f"{detail['status']} 🆔 {detail['id']} | 📱 {detail['phone']}\n"
                    text += f"   ❌ {detail['error'][:80]}\n"
                text += "\n"
            
            if len(result['details']) > 20:
                text += f"... и ещё {len(result['details']) - 20} аккаунтов\n"
        
        # Добавляем кнопку для удаления невалидных
        if result['invalid'] > 0:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🗑 УДАЛИТЬ НЕВАЛИДНЫЕ", callback_data="delete_invalid_sessions")],
                [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
            ])
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
            ])
        
        await callback.message.edit_text(text, reply_markup=keyboard)
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка проверки: {e}")
        logger.error(f"Ошибка проверки сессий: {e}")

@dp.callback_query(F.data == "delete_invalid_sessions")
async def delete_invalid_sessions(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение удаления невалидных сессий"""
    await safe_edit_message(
        callback.message,
        "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
        "Ты собираешься удалить все аккаунты с нерабочими сессиями.\n"
        "Это действие необратимо!\n\n"
        "❓ Подтверди удаление:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ДА, УДАЛИТЬ", callback_data="confirm_delete_invalid")],
            [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="admin_back")]
        ])
    )
    await callback.answer()
    await state.set_state(AdminSessionCheckStates.waiting_for_confirm)

@dp.callback_query(F.data == "confirm_delete_invalid")
async def confirm_delete_invalid(callback: types.CallbackQuery, state: FSMContext):
    """Удаление невалидных сессий"""
    await callback.message.edit_text("🔄 ПРОВЕРЯЮ И УДАЛЯЮ НЕВАЛИДНЫЕ СЕССИИ...")
    await callback.answer()
    
    try:
        # Проверяем все сессии
        result = await check_all_sessions()
        
        if result['invalid'] == 0:
            await callback.message.edit_text("✅ Нет невалидных сессий для удаления.")
            await state.clear()
            return
        
        # Собираем ID невалидных
        invalid_ids = [detail['id'] for detail in result['details'] if not detail['valid']]
        
        # Удаляем
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        
        deleted = 0
        for pid in invalid_ids:
            c.execute("DELETE FROM products WHERE id = ?", (pid,))
            deleted += c.rowcount
        
        conn.commit()
        conn.close()
        
        await callback.message.edit_text(
            f"✅ <b>УДАЛЕНИЕ ЗАВЕРШЕНО!</b>\n\n"
            f"🗑 Удалено аккаунтов: <b>{deleted}</b>\n"
            f"📊 Всего аккаунтов в каталоге: <b>{len(get_products())}</b>"
        )
        await state.clear()
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}")
        await state.clear()
        
@dp.callback_query(lambda c: c.data.startswith('session_file_'))
async def session_file(callback: types.CallbackQuery):
    """Скачивание файла сессии после покупки"""
    log_user_action(callback.from_user.id, "session_file")
    
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await safe_edit_message(callback.message, "❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return
    
    await callback.answer("🔄 Создаю файл сессии...", show_alert=False)
    
    # Распаковываем покупку
    if len(purchase) >= 10:
        pid, user_id, product_id, price, date, phone, session_string, region, year, password = purchase[:10]
    else:
        pid, user_id, product_id, price, date, phone, session_string, region, year = purchase[:9]
    
    import tempfile
    import shutil
    
    temp_dir = None
    try:
        # Создаем временную директорию
        temp_dir = tempfile.mkdtemp()
        session_path = os.path.join(temp_dir, f"session_{phone.replace('+', '')}")
        
        # Создаем клиент с StringSession
        client = TelegramClient(session_path, API_ID, API_HASH)
        client.session = StringSession(session_string)
        await client.connect()
        
        if await client.is_user_authorized():
            # Сохраняем сессию в файл
            client.session.save()
            await client.disconnect()
            
            # Путь к сохраненному .session файлу
            session_file_path = session_path + ".session"
            
            if os.path.exists(session_file_path):
                # Читаем файл
                with open(session_file_path, 'rb') as f:
                    file_data = f.read()
                
                # Отправляем файл
                clean_phone = phone.replace('+', '').replace(' ', '')
                filename = f"telegram_{clean_phone}.session"
                
                await callback.message.answer_document(
                    BufferedInputFile(file_data, filename=filename),
                    caption=f"📁 <b>ФАЙЛ СЕССИИ</b>\n\n"
                            f"📱 Телефон: <code>{phone}</code>\n"
                            f"💰 Цена: {price} ₽\n"
                            f"📅 Куплен: {date[:16]}\n\n"
                            f"⚠️ Сохраните файл в безопасном месте!"
                )
            else:
                await callback.message.answer("❌ Не удалось создать файл сессии")
        else:
            await callback.message.answer("❌ Сессия не авторизована")
            
    except Exception as e:
        logger.error(f"Ошибка создания файла сессии: {e}")
        await callback.message.answer(f"❌ Ошибка: {str(e)[:200]}")
        
    finally:
        # Очищаем временные файлы
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except:
                pass

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

@dp.callback_query(F.data == "download_all_sessions")
async def download_all_sessions(callback: types.CallbackQuery):
    """Скачивает сессии всех аккаунтов"""
    await callback.message.edit_text("🔄 Создаю архив со всеми сессиями...")
    
    # Получаем все товары
    products = get_products()
    product_ids = [p[0] for p in products if len(p) >= 5 and p[4]]
    
    if not product_ids:
        await callback.message.edit_text("❌ Нет аккаунтов с сессиями.")
        await callback.answer()
        return
    
    # Создаем архив
    zip_data = await create_session_zip(product_ids)
    
    if not zip_data:
        await callback.message.edit_text("❌ Не удалось создать архив.")
        await callback.answer()
        return
    
    # Отправляем файл
    import io
    file = io.BytesIO(zip_data)
    file.name = f"all_accounts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    
    await callback.message.answer_document(
        BufferedInputFile(file.getvalue(), filename=file.name),
        caption=f"📥 Архив со всеми сессиями ({len(product_ids)} аккаунтов)"
    )
    await callback.answer()
    
@dp.callback_query(F.data == "admin_download_sessions")
async def admin_download_sessions(callback: types.CallbackQuery):
    """Показывает список товаров для скачивания сессий"""
    products = get_products()
    
    if not products:
        await safe_edit_message(callback.message, "📭 Нет товаров в каталоге.")
        await callback.answer()
        return
    
    # Фильтруем только те, у которых есть сессия
    products_with_session = []
    for p in products:
        if len(p) >= 5 and p[4]:  # session_string
            products_with_session.append(p)
    
    if not products_with_session:
        await safe_edit_message(callback.message, "📭 Нет товаров с сохраненными сессиями.")
        await callback.answer()
        return
    
    # Создаем клавиатуру с товарами
    buttons = []
    for prod in products_with_session[:10]:  # показываем первые 10
        pid, name, price, phone = prod[:4]
        short_phone = phone[-4:] if phone else "no phone"
        buttons.append([InlineKeyboardButton(
            text=f"📦 {name[:20]} | {short_phone} | {price}₽",
            callback_data=f"download_session_{pid}"
        )])
    
    # Кнопки для массового скачивания
    buttons.append([
        InlineKeyboardButton(text="📥 Скачать ВСЕ", callback_data="download_all_sessions")
    ])
    buttons.append([
        InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")
    ])
    
    await safe_edit_message(
        callback.message,
        "📥 <b>ВЫБЕРИ АККАУНТ ДЛЯ СКАЧИВАНИЯ</b>\n\n"
        f"Всего аккаунтов с сессиями: {len(products_with_session)}",
        InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()
    
@dp.callback_query(F.data == "admin_download_db")
async def admin_download_db(callback: types.CallbackQuery):
    """Скачивание файла базы данных shop.db"""
    await callback.answer("📦 Подготавливаю базу данных...", show_alert=False)
    
    try:
        # Проверяем существование файла
        db_path = 'shop.db'
        if not os.path.exists(db_path):
            await callback.message.edit_text("❌ Файл базы данных не найден!")
            return
        
        # Получаем размер файла
        file_size = os.path.getsize(db_path)
        file_size_mb = file_size / (1024 * 1024)
        
        # Отправляем файл
        with open(db_path, 'rb') as f:
            file_data = f.read()
        
        # Создаем имя файла с датой
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"shop_backup_{timestamp}.db"
        
        await callback.message.answer_document(
            BufferedInputFile(file_data, filename=filename),
            caption=f"📥 <b>БАЗА ДАННЫХ</b>\n\n"
                    f"📅 Дата: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                    f"📦 Размер: {file_size_mb:.2f} MB\n"
                    f"⚠️ Храните в безопасном месте!"
        )
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}")
        logger.error(f"Ошибка скачивания БД: {e}")
        
@dp.callback_query(lambda c: c.data.startswith('download_session_'))
async def download_single_session(callback: types.CallbackQuery):
    await callback.answer("🔄 Создаю архив...", show_alert=False)
    
    product_id = int(callback.data.split('_')[2])
    msg = await callback.message.answer("🔄 Создаю архив...")
    
    try:
        zip_data = await create_session_zip([product_id])
        if not zip_data:
            await msg.edit_text("❌ Ошибка")
            return
        
        file = io.BytesIO(zip_data)
        file.name = f"account_{product_id}.zip"
        
        await callback.message.answer_document(
            BufferedInputFile(file.getvalue(), filename=file.name),
            caption=f"✅ Архив с SQLite .session для аккаунта #{product_id}"
        )
        await msg.delete()
    except Exception as e:
        await msg.edit_text(f"❌ {e}")
  
@dp.callback_query(F.data == "admin_delete_by_phone")
async def admin_delete_by_phone_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало удаления по номеру телефона"""
    await safe_edit_message(
        callback.message,
        "📱 <b>УДАЛЕНИЕ ТОВАРОВ ПО НОМЕРУ</b>\n\n"
        "Введи номер телефона (например: +79001234567 или 79001234567):"
    )
    await state.set_state(AdminDeleteStates.waiting_for_phone)
    await callback.answer()

@dp.message(AdminDeleteStates.waiting_for_phone)
async def admin_delete_by_phone_process(message: types.Message, state: FSMContext):
    """Обработка введенного номера"""
    phone_input = message.text.strip()
    
    # Очищаем номер для поиска
    phone_clean = re.sub(r'[^\d+]', '', phone_input)
    if not phone_clean.startswith('+'):
        phone_clean = '+' + phone_clean
    
    # Ищем товары в базе
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT id, name, price FROM products WHERE phone LIKE ?", (f'%{phone_clean}%',))
    products = c.fetchall()
    conn.close()
    
    if not products:
        await message.answer(
            f"❌ Товары с номером {phone_clean} не найдены.\n\n"
            f"Попробуй другой номер или отправь /cancel"
        )
        return
    
    # Сохраняем ID товаров
    product_ids = [p[0] for p in products]
    await state.update_data(delete_products=product_ids, delete_phone=phone_clean)
    
    # Показываем список для подтверждения
    text = f"📱 <b>Номер:</b> {phone_clean}\n"
    text += f"📦 <b>Найдено товаров:</b> {len(products)}\n\n"
    
    for i, (pid, name, price) in enumerate(products, 1):
        text += f"{i}. 🆔 {pid} | {name[:30]} | {price} ₽\n"
    
    text += "\n❓ Удалить все эти товары?"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ ДА, УДАЛИТЬ ВСЕ", callback_data="confirm_delete_by_phone")],
        [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="admin_back")]
    ])
    
    await message.answer(text, reply_markup=keyboard)
    await state.set_state(AdminDeleteStates.waiting_for_confirm)

@dp.callback_query(F.data == "confirm_delete_by_phone")
async def admin_delete_by_phone_confirm(callback: types.CallbackQuery, state: FSMContext):
    """Подтверждение удаления"""
    data = await state.get_data()
    product_ids = data.get('delete_products', [])
    phone = data.get('delete_phone', 'неизвестный номер')
    
    if not product_ids:
        await callback.message.edit_text("❌ Ошибка: нет товаров для удаления.")
        await state.clear()
        await callback.answer()
        return
    
    # Удаляем товары
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    deleted = 0
    for pid in product_ids:
        c.execute("DELETE FROM products WHERE id = ?", (pid,))
        deleted += c.rowcount
    
    conn.commit()
    conn.close()
    
    await callback.message.edit_text(
        f"✅ <b>ГОТОВО!</b>\n\n"
        f"📱 Номер: {phone}\n"
        f"🗑 Удалено товаров: {deleted}"
    )
    await state.clear()
    await callback.answer()
    
@dp.message(Command("cancel"))
async def cancel_operation(message: types.Message, state: FSMContext):
    """Отмена текущей операции"""
    current_state = await state.get_state()
    if current_state is None:
        await message.answer("❌ Нет активной операции.")
        return
    
    await state.clear()
    await message.answer("✅ Операция отменена.")
    
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


async def process_zip_file(zip_bytes: bytes) -> list:
    """
    Обрабатывает ZIP архив с сессиями.
    Поддерживает:
    - .session (SQLite файл)
    - .session.txt (StringSession)
    """
    results = []
    temp_files = []  # Для очистки

    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            file_list = z.namelist()

            for file_name in file_list:
                # Пропускаем мусор
                if not (file_name.endswith(".session") or file_name.endswith(".session.txt")):
                    continue

                with z.open(file_name) as f:
                    content = f.read()

                # Обычный .session (sqlite)
                if file_name.endswith(".session"):
                    try:
                        # Сохраняем во временный файл
                        import tempfile
                        temp_dir = tempfile.gettempdir()
                        temp_path = os.path.join(temp_dir, f"temp_session_{hash(file_name)}.session")
                        
                        with open(temp_path, "wb") as temp_file:
                            temp_file.write(content)
                        
                        temp_files.append(temp_path)
                        
                        # Пытаемся извлечь номер из имени файла или из сессии
                        phone = os.path.basename(file_name).replace(".session", "")
                        if not phone or phone == "session":
                            # Пробуем прочитать из файла
                            try:
                                client = TelegramClient(temp_path, API_ID, API_HASH)
                                await client.connect()
                                if await client.is_user_authorized():
                                    me = await client.get_me()
                                    phone = me.phone or phone
                                await client.disconnect()
                            except:
                                pass

                        results.append({
                            "type": "file",
                            "session": temp_path,
                            "phone": phone
                        })

                    except Exception as e:
                        logger.error(f"Ошибка обработки .session: {e}")

                # StringSession (.session.txt)
                elif file_name.endswith(".session.txt"):
                    try:
                        session_string = content.decode().strip()
                        phone = os.path.basename(file_name).replace(".session.txt", "")
                        
                        # Проверяем, что строка валидная
                        if session_string and len(session_string) > 10:
                            results.append({
                                "type": "string",
                                "session": session_string,
                                "phone": phone
                            })
                    except Exception as e:
                        logger.error(f"Ошибка обработки .session.txt: {e}")

        if not results:
            raise Exception(f"❌ В ZIP архиве не найден файл .session.\n\nНайдены файлы: {', '.join(file_list)}")

        return results

    except zipfile.BadZipFile:
        raise Exception("❌ Это не ZIP файл или он поврежден")
    except Exception as e:
        raise Exception(f"❌ Ошибка обработки ZIP: {str(e)}")
        
@dp.callback_query(F.data == "refresh_codes", CodeRetrievalStates.waiting_for_action)
async def refresh_codes(callback: types.CallbackQuery, state: FSMContext):
    """Обновляет список кодов, используя сохранённый файл сессии"""
    data = await state.get_data()
    session_path = data.get('session_path')
    if not session_path or not os.path.exists(session_path):
        await callback.message.edit_text("❌ Сессия не найдена. Загрузи ZIP заново.")
        await state.clear()
        await callback.answer()
        return

    await callback.message.edit_text("🔄 ОБНОВЛЯЮ КОДЫ...")

    try:
        codes = await get_codes_from_session_file(session_path, limit=5)

        if not codes:
            await callback.message.edit_text(
                "❌ <b>НЕТ НОВЫХ КОДОВ</b>\n\n"
                "Попробуй позже или проверь аккаунт."
            )
            await callback.answer()
            return

        await state.update_data(codes=codes, last_codes=codes)

        text = "📨 <b>ОБНОВЛЕННЫЕ КОДЫ</b>\n\n"
        for i, code_data in enumerate(codes, 1):
            star = "⭐ " if i == 1 else ""
            text += f"{i}. {star}{code_data['type']} <code>{code_data['code']}</code>\n"
            text += f"   🕐 {code_data['date']}\n"
            if code_data.get('text'):
                text += f"   💬 {code_data['text'][:50]}...\n"
            text += "\n"

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data="refresh_codes")],
            [InlineKeyboardButton(text="🔙 В МЕНЮ", callback_data="back_to_main")]
        ])

        await callback.message.edit_text(text, reply_markup=keyboard)
        await callback.answer()

    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {str(e)[:200]}")
        await state.clear()
        await callback.answer()
    
@dp.callback_query(F.data == "back_to_main")
async def back_to_main_from_codes(callback: types.CallbackQuery, state: FSMContext):
    """Возврат в главное меню и удаление временных файлов"""
    data = await state.get_data()
    session_path = data.get('session_path')
    if session_path:
        try:
            temp_dir = os.path.dirname(session_path)
            import shutil
            shutil.rmtree(temp_dir, ignore_errors=True)
            logger.info(f"🗑 Удалена временная папка: {temp_dir}")
        except Exception as e:
            logger.error(f"Ошибка удаления: {e}")
    await state.clear()
    await cmd_start(callback.message)
    await callback.answer()
    
@dp.message(CodeRetrievalStates.waiting_for_zip, F.document)
async def handle_zip(message: types.Message, state: FSMContext):
    try:
        document = message.document

        # Проверяем формат
        if not document.file_name.lower().endswith(".zip"):
            await message.answer("❌ Отправь ZIP архив с сессиями")
            return

        await message.answer("📦 Загружаю архив...")

        # Скачиваем файл
        file = await bot.get_file(document.file_id)
        file_bytes = await bot.download_file(file.file_path)
        zip_bytes = file_bytes.read()

        await message.answer("🔍 Обрабатываю...")

        # Обрабатываем ZIP
        sessions = await process_zip_file(zip_bytes)

        if not sessions:
            await message.answer("❌ Сессии не найдены")
            return

        result_text = f"✅ Найдено сессий: {len(sessions)}\n\n"
        all_codes_text = ""

        for s in sessions:
            phone = s.get("phone", "unknown")
            
            try:
                # Создаем временную директорию для сессии
                import tempfile
                temp_dir = tempfile.mkdtemp()
                
                # Подключение
                if s["type"] == "file":
                    # Копируем временный файл в новую директорию
                    import shutil
                    temp_session_path = os.path.join(temp_dir, os.path.basename(s["session"]))
                    shutil.copy2(s["session"], temp_session_path)
                    client = TelegramClient(temp_session_path, API_ID, API_HASH)
                else:
                    client = TelegramClient(StringSession(s["session"]), API_ID, API_HASH)

                await client.connect()
                
                # Проверяем авторизацию
                if not await client.is_user_authorized():
                    result_text += f"📱 {phone} — ❌ не авторизована\n"
                    await client.disconnect()
                    continue
                
                # Получаем информацию об аккаунте для проверки
                me = await client.get_me()
                result_text += f"📱 {phone} — ✅ авторизован (@{me.username or me.first_name})\n"
                
                # Получаем коды
                codes = await get_codes_from_session_file_v2(client, limit=10)
                
                await client.disconnect()
                
                # Удаляем временную папку
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)

                if not codes:
                    result_text += f"   ⚠️ кодов не найдено\n"
                    continue

                result_text += f"   ✅ найдено {len(codes)} кодов\n\n"

                for c in codes:
                    all_codes_text += (
                        f"📱 {phone}\n"
                        f"{c['type']} <code>{c['code']}</code>\n"
                        f"🕒 {c['date']}\n"
                        f"📝 {c.get('text', '')[:100]}\n\n"
                    )

            except Exception as e:
                result_text += f"📱 {phone} — ❌ ошибка: {str(e)[:100]}\n"
                logger.error(f"Ошибка с {phone}: {e}")
                continue

        # Отправляем результат
        await message.answer(result_text[:4000])

        if all_codes_text:
            # Разбиваем на части, если текст слишком длинный
            if len(all_codes_text) > 4000:
                for i in range(0, len(all_codes_text), 4000):
                    await message.answer(f"🔑 КОДЫ:\n\n{all_codes_text[i:i+4000]}")
            else:
                await message.answer(f"🔑 КОДЫ:\n\n{all_codes_text}")
        else:
            await message.answer("❌ Коды не найдены. Возможно, аккаунт новый и ещё не получал коды.")

        await state.clear()

    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        logger.error(f"Ошибка в handle_zip: {e}")
        traceback.print_exc()
        await state.clear()
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
    
    if not phone:
        await message.answer("❌ Ошибка: номер не найден")
        await state.clear()
        return
    
    if phone not in temp_clients:
        logger.error(f"❌ Клиент НЕ найден в temp_clients!")
        await message.answer("❌ Сессия истекла. Начни заново.")
        await state.clear()
        return
    
    status_msg = await message.answer("🔄 ПРОВЕРЯЮ КОД...")
    result = await verify_code(phone, code)
    
    if not result['success']:
        error_text = result.get('error', 'НЕИЗВЕСТНАЯ ОШИБКА')
        await status_msg.edit_text(f"❌ {error_text}")
        
        # Если код неверный, предлагаем попробовать снова
        if "Неверный код" in error_text:
            await message.answer(
                "❌ Неверный код. Попробуй еще раз:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🔄 Отправить код заново", callback_data="resend_code")]
                ])
            )
        else:
            await state.clear()
        return
    
    if result.get('need_password'):
        logger.info("🔐 Требуется 2FA пароль")
        await state.update_data(phone=phone)
        await status_msg.edit_text(
            "🔐 <b>ТРЕБУЕТСЯ 2FA ПАРОЛЬ</b>\n\n"
            "ВВЕДИ ПАРОЛЬ:"
        )
        await state.set_state(ProductStates.waiting_for_password)
    else:
        logger.info("✅ Успешный вход")
        
        data = await state.get_data()
        account_info = result.get('account_info', {})
        register_date = account_info.get('register_date')
        spam_block = account_info.get('spam_block', 0)
        account_age = account_info.get('account_age_days', 0)
        
        pid = add_product(
            data['name'],
            data['price'],
            result['phone'],
            result['session'],
            result['region'],
            result['year'],
            data.get('account_password'),
            spam_block,
            register_date,
            account_age
        )
        
        reg_date_text = register_date if register_date else "неизвестно"
        spam_text = "✅ НЕТ" if spam_block == 0 else "❌ ЕСТЬ"
        
        await status_msg.edit_text(
            f"✅ <b>АККАУНТ УСПЕШНО ДОБАВЛЕН!</b>\n\n"
            f"📦 НАЗВАНИЕ: <b>{data['name']}</b>\n"
            f"💰 ЦЕНА: <code>{data['price']} ₽</code>\n"
            f"🌍 РЕГИОН: {result['region']}\n"
            f"📅 ГОД ВЫСТАВЛЕНИЕ НА ПРОДАЖУ: {result['year']}\n"
            f"📆 ДАТА РЕГА АККА: <b>{reg_date_text}</b>\n"
            f"🚫 СПАМБЛОК: <b>{spam_text}</b>\n"
            f"🔑 ПАРОЛЬ: <code>{data.get('account_password', 'НЕТ')}</code>\n"
            f"🆔 ID: <code>{pid}</code>"
        )
        await state.clear()

@dp.callback_query(F.data == "resend_code")
async def resend_code_handler(callback: types.CallbackQuery, state: FSMContext):
    """Повторная отправка кода"""
    data = await state.get_data()
    phone = data.get('phone')
    
    if not phone:
        await callback.message.edit_text("❌ Ошибка: номер не найден")
        await callback.answer()
        return
    
    client_data = temp_clients.get(phone)
    if not client_data:
        await callback.message.edit_text("❌ Сессия истекла, начни заново")
        await callback.answer()
        await state.clear()
        return
    
    client = client_data.get('client')
    
    try:
        await callback.message.edit_text("🔄 Отправляю код заново...")
        
        # Отправляем код заново
        result = await client.send_code_request(phone)
        
        # Обновляем хеш
        temp_clients[phone] = {
            'client': client,
            'phone_code_hash': result.phone_code_hash
        }
        
        await callback.message.edit_text(
            f"📱 <b>НОВЫЙ КОД ОТПРАВЛЕН!</b>\n\n"
            f"Проверь виртуальный номер в течение 1-2 минут.\n"
            f"Код обычно приходит SMS-сообщением.\n\n"
            f"✍️ ВВЕДИ КОД:"
        )
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка: {e}")
    
    await callback.answer()
    
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
    
    # Получаем данные из state
    data = await state.get_data()
    
    # 🔥🔥🔥 ПОЛУЧАЕМ ИНФОРМАЦИЮ ОБ АККАУНТЕ 🔥🔥🔥
    account_info = result.get('account_info', {})
    register_date = account_info.get('register_date')
    spam_block = account_info.get('spam_block', 0)
    
    # Добавляем товар в базу
    pid = add_product(
        data['name'],
        data['price'],
        result['phone'],
        result['session'],
        result['region'],
        result['year'],
        data.get('account_password'),
        spam_block,        # ← АВТОМАТИЧЕСКИ
        register_date      # ← АВТОМАТИЧЕСКИ
    )
    
    logger.info(f"✅ Товар добавлен с ID: {pid}")
    logger.info(f"📅 Дата регистрации: {register_date}")
    logger.info(f"🚫 Спамблок: {'ЕСТЬ' if spam_block else 'НЕТ'}")
    
    # Формируем сообщение
    reg_date_text = register_date if register_date else "неизвестно"
    spam_text = "✅ НЕТ" if spam_block == 0 else "❌ ЕСТЬ"
    
    await status_msg.edit_text(
        f"✅ <b>АККАУНТ УСПЕШНО ДОБАВЛЕН!</b>\n\n"
        f"📦 НАЗВАНИЕ: <b>{data['name']}</b>\n"
        f"💰 ЦЕНА: <code>{data['price']} ₽</code>\n"
        f"🌍 РЕГИОН: {result['region']}\n"
        f"📅 ГОД: {result['year']}\n"
        f"📆 ДАТА РЕГА: <b>{reg_date_text}</b>\n"
        f"🚫 СПАМБЛОК: <b>{spam_text}</b>\n"
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
        
        # 🔥 ЗАПУСКАЕМ ФОНОВУЮ ПРОВЕРКУ СЕССИЙ
        asyncio.create_task(auto_check_sessions())
        logger.info("🔄 Запущена фоновая проверка сессий (каждые 24 часа)")
        
        await dp.start_polling(bot, skip_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        traceback.print_exc()
    finally:
        await bot.session.close()
        
        # ==================== ЗАПУСК ====================
if __name__ == '__main__':
    asyncio.run(main())