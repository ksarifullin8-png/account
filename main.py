import asyncio
import logging
import sqlite3
import random
import string
import re
import os
import requests
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, PreCheckoutQuery, FSInputFile
)
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PhoneNumberUnoccupiedError,
    PhoneNumberInvalidError
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ==================== НАСТРОЙКИ ====================
TOKEN = "8561605758:AAH7WUSKqYHm7zbOUkEapNP8_QSFQw9D0nA"
CRYPTOBOT_TOKEN = "546557:AAA5MxwCASiCnPAQOnZ6cNkbhgnirFIrxhU"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
ADMIN_IDS = [7546928092]

API_ID = 35800959
API_HASH = "708e7d0bc3572355bcaf68562cc068f1"

STARS_RATE = 1.4
USDT_RATE = 70

bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot_username = None

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if not os.path.exists('sessions'):
    os.makedirs('sessions')

temp_clients: Dict[str, TelegramClient] = {}
active_sessions: Dict[str, str] = {}

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
    waiting_for_reviews_channel = State()  # ДОБАВЬ ЭТУ СТРОКУ

class MailingStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirm = State()

# ==================== БАЗА ДАННЫХ ====================
def init_db():
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
    
    # Таблица товаров - создаем СРАЗУ с колонкой password
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
    
    # Таблица покупок - создаем СРАЗУ с колонкой password
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
    
    # Остальные таблицы
    c.execute('''CREATE TABLE IF NOT EXISTS account_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        code TEXT,
        received_date TEXT,
        message_text TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS pending_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        method TEXT,
        status TEXT DEFAULT 'pending',
        created_date TEXT,
        invoice_id TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )''')
    
    # Настройки по умолчанию
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
    logger.info("База данных инициализирована")

def upgrade_db():
    """Обновление структуры базы данных - на случай если таблицы уже есть без password"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    # Проверяем products
    c.execute("PRAGMA table_info(products)")
    columns = [col[1] for col in c.fetchall()]
    if 'password' not in columns:
        try:
            c.execute("ALTER TABLE products ADD COLUMN password TEXT")
            logger.info("✅ Добавлена колонка password в products")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
    
    # Проверяем purchases
    c.execute("PRAGMA table_info(purchases)")
    columns = [col[1] for col in c.fetchall()]
    if 'password' not in columns:
        try:
            c.execute("ALTER TABLE purchases ADD COLUMN password TEXT")
            logger.info("✅ Добавлена колонка password в purchases")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
    
    conn.commit()
    conn.close()

# Инициализация и обновление БД
init_db()
upgrade_db()  # ← ЭТО РЕШИТ ТВОЮ ПРОБЛЕМУ

# ==================== ФУНКЦИИ РЕФЕРАЛЬНОЙ СИСТЕМЫ ====================
def generate_referral_code(user_id: int) -> str:
    """Генерация реферального кода"""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{user_id}{random_part}"

def get_user(user_id: int, username: str = None, referrer_id: int = None) -> Optional[Tuple]:
    """Получение или создание пользователя"""
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
    """Поиск пользователя по реферальному коду"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE referral_code = ?", (code,))
    user = c.fetchone()
    conn.close()
    return user

def can_use_discount(user_id: int) -> bool:
    """Проверка доступности скидки"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT first_discount_used, referrer_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return bool(result and result[0] == 0 and result[1] is not None)

def apply_first_discount(user_id: int):
    """Применение первой скидки"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET first_discount_used = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def get_referral_stats(user_id: int) -> Dict:
    """Получение статистики рефералов"""
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
    """Получение всех пользователей"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT user_id, username FROM users ORDER BY user_id")
    users = c.fetchall()
    conn.close()
    return users

# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================
def get_setting(key: str) -> Any:
    """Получение настройки"""
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
    """Обновление настройки"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE settings SET value = ? WHERE key = ?", (str(value), key))
    conn.commit()
    conn.close()

def get_balance(user_id: int) -> float:
    """Получение баланса"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else 0

def update_balance(user_id: int, amount: float):
    """Обновление баланса"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()

def add_referral_earning(user_id: int, amount: float, from_user_id: int):
    """Начисление реферального бонуса"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ?, total_referral_earnings = total_referral_earnings + ? WHERE user_id = ?",
              (amount, amount, user_id))
    conn.commit()
    conn.close()

# ==================== ФУНКЦИИ ТОВАРОВ ====================
def get_products() -> List[Tuple]:
    """Получение всех товаров"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products ORDER BY id DESC")
    products = c.fetchall()
    conn.close()
    return products

def get_product(product_id: int) -> Optional[Tuple]:
    """Получение товара по ID"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    product = c.fetchone()
    conn.close()
    return product

def add_product(name: str, price: float, phone: str, session_string: str, region: str, year: int, password: str = None) -> int:
    """Добавление товара"""
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
    """Удаление товара"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("DELETE FROM products WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()

# ==================== ФУНКЦИИ ПОКУПОК ====================
def add_purchase(user_id: int, product_id: int, price: float, phone: str, session_string: str, region: str, year: int, password: str = None) -> int:
    """Добавление покупки"""
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
    """Получение покупок пользователя"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE user_id = ? ORDER BY purchase_date DESC", (user_id,))
    purchases = c.fetchall()
    conn.close()
    return purchases

def get_purchase(purchase_id: int) -> Optional[Tuple]:
    """Получение покупки по ID"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE id = ?", (purchase_id,))
    purchase = c.fetchone()
    conn.close()
    return purchase

# ==================== ФУНКЦИИ КОДОВ ====================
def save_code(phone: str, code: str, message_text: str, received_date: str = None):
    """Сохранение кода в базу"""
    if received_date is None:
        received_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("INSERT INTO account_codes (phone, code, received_date, message_text) VALUES (?, ?, ?, ?)",
              (phone, code, received_date, message_text[:200]))
    conn.commit()
    conn.close()

def get_codes(phone: str, limit: int = 20) -> List[Tuple]:
    """Получение кодов по номеру телефона"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT code, received_date, message_text FROM account_codes WHERE phone = ? ORDER BY received_date DESC LIMIT ?", (phone, limit))
    codes = c.fetchall()
    conn.close()
    return codes

async def get_live_codes_from_account(session_string: str, limit: int = 20) -> List[Dict]:
    """Получение кодов напрямую из аккаунта"""
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
    """Добавление ожидающего платежа"""
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
    """Получение ожидающего платежа"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM pending_payments WHERE id = ?", (payment_id,))
    payment = c.fetchone()
    conn.close()
    return payment

def update_payment_status(payment_id: int, status: str):
    """Обновление статуса платежа"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE pending_payments SET status = ? WHERE id = ?", (status, payment_id))
    conn.commit()
    conn.close()

def get_pending_payments_by_status(status: str = 'pending') -> List[Tuple]:
    """Получение платежей по статусу"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM pending_payments WHERE status = ?", (status,))
    payments = c.fetchall()
    conn.close()
    return payments

# ==================== TELEGRAM AUTH ====================
async def detect_region(phone: str) -> str:
    """Определение региона по номеру телефона"""
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
    else:
        return '🌍 Другая страна'

async def login_to_telegram(phone: str) -> Dict[str, Any]:
    """Вход в Telegram аккаунт"""
    try:
        phone = re.sub(r'[^\d+]', '', phone)
        if not phone.startswith('+'):
            phone = '+' + phone
        
        if phone in active_sessions:
            session_string = active_sessions[phone]
            client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
            await client.connect()
            if await client.is_user_authorized():
                me = await client.get_me()
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
        
        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
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
            await client.send_code_request(phone)
            temp_clients[phone] = client
            return {'success': True, 'need_code': True, 'phone': phone, 'client': client}
    
    except Exception as e:
        logger.error(f"Login error: {e}")
        return {'success': False, 'error': str(e)}

async def verify_code(phone: str, code: str) -> Dict[str, Any]:
    """Подтверждение кода"""
    try:
        client = temp_clients.get(phone)
        if not client:
            return {'success': False, 'error': '❌ Сессия истекла'}
        
        await client.sign_in(code=code)
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
    
    except SessionPasswordNeededError:
        return {'success': True, 'need_password': True, 'phone': phone}
    except PhoneCodeInvalidError:
        return {'success': False, 'error': '❌ Неверный код'}
    except Exception as e:
        logger.error(f"Verify code error: {e}")
        return {'success': False, 'error': str(e)}

async def verify_password(phone: str, password: str) -> Dict[str, Any]:
    """Подтверждение 2FA пароля"""
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
        logger.error(f"Verify password error: {e}")
        return {'success': False, 'error': str(e)}

# ==================== КРИПТО ФУНКЦИИ ====================
async def fetch_usdt_rate() -> float:
    """Получение курса USDT"""
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
    """Создание счета в CryptoBot"""
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
    """Главная клавиатура"""
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
    """Админ клавиатура"""
    buttons = [
        [InlineKeyboardButton(text="➕ ДОБАВИТЬ ТОВАР", callback_data="admin_add_product")],
        [InlineKeyboardButton(text="🗑 УДАЛИТЬ ТОВАР", callback_data="admin_delete_product")],
        [InlineKeyboardButton(text="📦 СПИСОК ТОВАРОВ", callback_data="admin_list_products")],
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="admin_stats")],
        [InlineKeyboardButton(text="💰 НАЧИСЛИТЬ БАЛАНС", callback_data="admin_add_balance")],
        [InlineKeyboardButton(text="📢 РАССЫЛКА", callback_data="admin_mailing")],
        [InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="admin_settings")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_settings_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура настроек"""
    buttons = [
        [InlineKeyboardButton(text="⭐ КУРС STARS", callback_data="set_stars")],
        [InlineKeyboardButton(text="💵 КУРС USDT", callback_data="set_usdt")],
        [InlineKeyboardButton(text="🎁 СКИДКА РЕФЕРАЛАМ", callback_data="set_discount")],
        [InlineKeyboardButton(text="💸 НАГРАДА ЗА РЕФЕРАЛА", callback_data="set_reward")],
        [InlineKeyboardButton(text="📢 КАНАЛ ОТЗЫВОВ", callback_data="set_reviews_channel")],  # НОВАЯ КНОПКА
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура способов оплаты"""
    buttons = [
        [InlineKeyboardButton(text="⭐ TELEGRAM STARS", callback_data="pay_stars")],
        [InlineKeyboardButton(text="💳 СБП", callback_data="pay_sbp")],
        [InlineKeyboardButton(text="₿ CRYPTOBOT", callback_data="pay_crypto")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_balance")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def catalog_keyboard(products: List[Tuple]) -> InlineKeyboardMarkup:
    """Клавиатура каталога"""
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
    """Клавиатура товара"""
    buttons = [
        [InlineKeyboardButton(text="💳 КУПИТЬ", callback_data=f"buy_{product_id}")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_catalog")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def purchases_keyboard(purchases: List[Tuple]) -> InlineKeyboardMarkup:
    """Клавиатура покупок"""
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
    """Клавиатура действий с покупкой"""
    buttons = [
        [InlineKeyboardButton(text="🔑 ДАННЫЕ ВХОДА", callback_data=f"show_login_{purchase_id}")],
        [InlineKeyboardButton(text="📨 ПОКАЗАТЬ КОДЫ", callback_data=f"show_codes_{purchase_id}")],
        [InlineKeyboardButton(text="📁 ФАЙЛ СЕССИИ", callback_data=f"session_file_{purchase_id}")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_purchases")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def insufficient_balance_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура при недостатке средств"""
    buttons = [[InlineKeyboardButton(text="💰 ПОПОЛНИТЬ", callback_data="show_payment_methods")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_payment_keyboard(payment_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для админа (платежи)"""
    buttons = [
        [InlineKeyboardButton(text="✍️ РЕКВИЗИТЫ", callback_data=f"send_details_{payment_id}")],
        [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ", callback_data=f"admin_confirm_{payment_id}"),
         InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"admin_reject_{payment_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def referral_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура рефералов"""
    buttons = [
        [InlineKeyboardButton(text="🔗 МОЯ ССЫЛКА", callback_data="show_ref_link")],
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="ref_stats")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# ==================== ОБРАБОТЧИКИ КОМАНД ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    global bot_username
    args = message.text.split()
    referrer_id = None
    
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

# ==================== ОСНОВНЫЕ РАЗДЕЛЫ ====================
@dp.message(F.text == "🛍 КАТАЛОГ")
async def catalog(message: types.Message):
    """Каталог товаров"""
    products = get_products()
    if not products:
        await message.answer("📭 КАТАЛОГ ПУСТ. ТОВАРЫ ПОЯВЯТСЯ ПОЗЖЕ.")
        return
    await message.answer("📦 <b>ВЫБЕРИ ТОВАР ДЛЯ ПРОСМОТРА:</b>", reply_markup=catalog_keyboard(products))

@dp.message(F.text == "💰 БАЛАНС")
async def balance(message: types.Message):
    """Баланс пользователя"""
    user_balance = get_balance(message.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 <b>ТВОЙ БАЛАНС:</b> <code>{user_balance} ₽</code>\n"
        f"⭐ ЭКВИВАЛЕНТ: <code>{int(user_balance / stars_rate)} STARS</code>\n\n"
        f"ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    )
    await message.answer(text, reply_markup=payment_keyboard())

@dp.message(F.text == "👤 ПРОФИЛЬ")
async def profile(message: types.Message):
    """Профиль пользователя"""
    user = get_user(message.from_user.id)
    if user is None:
        user = get_user(message.from_user.id, message.from_user.username)
    
    purchases = get_user_purchases(message.from_user.id)
    discount_status = "✅ ДОСТУПНА" if can_use_discount(message.from_user.id) else "❌ НЕ ДОСТУПНА"
    
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
    """Реферальная система"""
    user = get_user(message.from_user.id)
    
    if not user[5]:
        new_code = generate_referral_code(message.from_user.id)
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        c.execute("UPDATE users SET referral_code = ? WHERE user_id = ?", (new_code, message.from_user.id))
        conn.commit()
        conn.close()
        user = get_user(message.from_user.id)
    
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
    """Показать реферальную ссылку"""
    user = get_user(callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ ОШИБКА: ПОЛЬЗОВАТЕЛЬ НЕ НАЙДЕН.")
        await callback.answer()
        return
    
    if not user[5]:
        new_code = generate_referral_code(callback.from_user.id)
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        c.execute("UPDATE users SET referral_code = ? WHERE user_id = ?", (new_code, callback.from_user.id))
        conn.commit()
        conn.close()
        user = get_user(callback.from_user.id)
    
    referral_link = f"https://t.me/{bot_username}?start=ref_{user[5]}"
    text = (
        f"🔗 <b>ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА:</b>\n\n"
        f"<code>{referral_link}</code>\n\n"
        f"📤 ОТПРАВЛЯЙ ЕЁ ДРУЗЬЯМ И ПОЛУЧАЙ {get_setting('referral_reward')}% ОТ ИХ ПОПОЛНЕНИЙ!"
    )
    await callback.message.edit_text(text, reply_markup=referral_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "ref_stats")
async def ref_stats(callback: types.CallbackQuery):
    """Статистика рефералов"""
    stats = get_referral_stats(callback.from_user.id)
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
    
    await callback.message.edit_text(text, reply_markup=referral_keyboard())
    await callback.answer()

@dp.message(F.text == "📜 ПОКУПКИ")
async def my_purchases(message: types.Message):
    """Список покупок"""
    purchases = get_user_purchases(message.from_user.id)
    if not purchases:
        await message.answer("📭 У ТЕБЯ ПОКА НЕТ ПОКУПОК.")
        return
    await message.answer("📜 <b>ТВОИ КУПЛЕННЫЕ АККАУНТЫ:</b>", reply_markup=purchases_keyboard(purchases))

@dp.message(F.text == "📝 ОТЗЫВЫ")
async def reviews_link(message: types.Message):
    """Показывает ссылку на канал с отзывами"""
    channel_link = get_setting('reviews_channel_link')
    
    if channel_link and channel_link != "не настроен":
        await message.answer(
            f"📢 <b>НАШ КАНАЛ С ОТЗЫВАМИ</b>\n\n"
            f"👉 {channel_link}\n\n"
            f"Там ты можешь почитать отзывы других покупателей!"
        )
    else:
        await message.answer(
            "📢 <b>КАНАЛ С ОТЗЫВАМИ ЕЩЁ НЕ ДОБАВЛЕН</b>\n\n"
            "Администратор скоро добавит ссылку."
        )

@dp.message(F.text == "📞 ПОДДЕРЖКА")
async def support(message: types.Message):
    """Поддержка"""
    text = (
        "📞 <b>СЛУЖБА ПОДДЕРЖКИ</b>\n\n"
        "ПО ВСЕМ ВОПРОСАМ ПИШИ СЮДА: @deaMorgan"
    )
    await message.answer(text)
    
    # ==================== ДЕТАЛИ ТОВАРА ====================
@dp.callback_query(F.data == "refresh_catalog")
async def refresh_catalog(callback: types.CallbackQuery):
    """Обновление каталога"""
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 КАТАЛОГ ПУСТ.")
        await callback.answer()
        return
    await callback.message.edit_text("📦 <b>ВЫБЕРИ ТОВАР:</b>", reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('view_'))
async def view_product(callback: types.CallbackQuery):
    """Просмотр товара"""
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)
    
    if not product:
        await callback.message.edit_text("❌ ТОВАР НЕ НАЙДЕН.")
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
    await callback.message.edit_text(text, reply_markup=product_keyboard(product_id))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('buy_'))
async def buy_product(callback: types.CallbackQuery):
    """Покупка товара"""
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)
    
    if not product:
        await callback.message.edit_text("❌ ТОВАР НЕ НАЙДЕН.")
        await callback.answer()
        return
    
    if len(product) >= 9:
        product_id, name, price, phone, session, region, year, added, password = product[:9]
    else:
        product_id, name, price, phone, session, region, year, added = product[:8]
        password = None
    
    user_balance = get_balance(callback.from_user.id)
    
    if user_balance >= price:
        update_balance(callback.from_user.id, -price)
        
        purchase_id = add_purchase(
            callback.from_user.id,
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
            f"📱 ТЕЛЕФОН: <code>{phone}</code>\n\n"
            f"по вопросам просьба обращаться к <b>администратору @deaMorgan</b>"
        )
        
        if password and password not in ['None', '']:
            text += f"🔑 ПАРОЛЬ АККАУНТА: <code>{password}</code>\n"
        
        text += f"\n📁 ФАЙЛ СЕССИИ ДОСТУПЕН В РАЗДЕЛЕ ПОКУПКИ"
        
        await callback.message.edit_text(text)
    else:
        need = price - user_balance
        await callback.message.edit_text(
            f"❌ <b>НЕДОСТАТОЧНО СРЕДСТВ</b>\n\nНУЖНО ЕЩЕ: <code>{need} ₽</code>",
            reply_markup=insufficient_balance_keyboard()
        )
    await callback.answer()
    
    # ==================== ДЕТАЛИ ПОКУПКИ ====================
@dp.callback_query(lambda c: c.data.startswith('purchase_'))
async def purchase_details(callback: types.CallbackQuery):
    """Детали покупки"""
    purchase_id = int(callback.data.split('_')[1])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await callback.message.edit_text("❌ ПОКУПКА НЕ НАЙДЕНА.")
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
        f"по вопросам просьба обращаться к <b>администратору @deaMorgan</b>\n"
        f"ВЫБЕРИ ДЕЙСТВИЕ:"
    )
    await callback.message.edit_text(text, reply_markup=purchase_actions_keyboard(pid))
    await callback.answer()
    
@dp.callback_query(lambda c: c.data.startswith('show_login_'))
async def show_login(callback: types.CallbackQuery):
    """Показать данные для входа"""
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await callback.message.edit_text("❌ ПОКУПКА НЕ НАЙДЕНА.")
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
    
    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data=f"purchase_{purchase_id}")]
    ]))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('show_codes_'))
async def show_codes(callback: types.CallbackQuery):
    """Показать коды из аккаунта"""
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await callback.message.edit_text("❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return
    
    pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
    
    msg = await callback.message.edit_text("🔄 ПОДКЛЮЧАЮСЬ К TELEGRAM АККАУНТУ...")
    
    try:
        codes = await get_live_codes_from_account(session, limit=30)
        
        if not codes:
            text = f"📨 <b>АККАУНТ #{pid}</b>\n\n❌ НЕТ КОДОВ В ЭТОМ АККАУНТЕ"
        else:
            text = f"📨 <b>КОДЫ ИЗ TELEGRAM (АККАУНТ #{pid})</b>:\n\n"
            for i, code_data in enumerate(codes, 1):
                star = "⭐ " if i == 1 else ""
                text += f"{i}. {star}{code_data['type']} <code>{code_data['code']}</code>  |  🕐 {code_data['date']}\n"
        
        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data=f"show_codes_{purchase_id}")],
            [InlineKeyboardButton(text="🔙 НАЗАД", callback_data=f"purchase_{purchase_id}")]
        ]))
    except Exception as e:
        await msg.edit_text(f"❌ ОШИБКА: {str(e)[:100]}")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('session_file_'))
async def session_file(callback: types.CallbackQuery):
    """Отправка файла сессии"""
    purchase_id = int(callback.data.split('_')[2])
    purchase = get_purchase(purchase_id)
    
    if not purchase or purchase[1] != callback.from_user.id:
        await callback.message.edit_text("❌ ПОКУПКА НЕ НАЙДЕНА.")
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
    """Показать способы оплаты"""
    await callback.message.edit_text("💰 <b>ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:</b>", reply_markup=payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "pay_stars")
async def pay_stars(callback: types.CallbackQuery, state: FSMContext):
    """Оплата Stars"""
    await callback.message.edit_text(
        f"⭐ <b>ПОПОЛНЕНИЕ ЧЕРЕЗ STARS</b>\n\n"
        f"КУРС: 1 STAR = {get_setting('stars_rate')} ₽\n"
        f"ВВЕДИ СУММУ В РУБЛЯХ:"
    )
    await state.set_state(PaymentStates.waiting_for_stars_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_stars_amount)
async def stars_amount_handler(message: types.Message, state: FSMContext):
    """Обработка суммы для Stars"""
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
    """Предчек PAY"""
    await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: types.Message):
    """Успешная оплата"""
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
    """Оплата СБП"""
    await callback.message.edit_text(
        "💳 <b>ПОПОЛНЕНИЕ ЧЕРЕЗ СБП</b>\n\n"
        "ВВЕДИ СУММУ (МИНИМУМ 100 ₽):"
    )
    await state.set_state(PaymentStates.waiting_for_sbp_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_sbp_amount)
async def sbp_amount_handler(message: types.Message, state: FSMContext):
    """Обработка суммы для СБП"""
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

@dp.callback_query(F.data == "set_reviews_channel")
async def set_reviews_channel(callback: types.CallbackQuery, state: FSMContext):
    """Настройка канала для отзывов"""
    current = get_setting('reviews_channel_link') or "не настроен"
    await callback.message.edit_text(
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
    """Сохранение ссылки на канал отзывов"""
    channel_input = message.text.strip()
    
    # Очищаем ссылку от мусора
    if channel_input.startswith('@'):
        channel_link = f"https://t.me/{channel_input[1:]}"
    elif 't.me/' in channel_input:
        # Оставляем как есть
        channel_link = channel_input
    else:
        # Если просто имя
        channel_link = f"https://t.me/{channel_input}"
    
    # Просто сохраняем ссылку, ничего не отправляем
    update_setting('reviews_channel_link', channel_link)
    
    await message.answer(
        f"✅ <b>Канал для отзывов сохранен!</b>\n\n"
        f"Ссылка: {channel_link}\n\n"
        f"Теперь пользователи будут видеть эту ссылку в разделе «📝 ОТЗЫВЫ»"
    )
    await state.clear()
    
@dp.callback_query(F.data == "pay_crypto")
async def pay_crypto(callback: types.CallbackQuery, state: FSMContext):
    """Оплата CryptoBot"""
    await callback.message.edit_text(
        "₿ <b>ПОПОЛНЕНИЕ ЧЕРЕЗ CRYPTOBOT</b>\n\n"
        "ВВЕДИ СУММУ В РУБЛЯХ:"
    )
    await state.set_state(PaymentStates.waiting_for_crypto_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_crypto_amount)
async def crypto_amount_handler(message: types.Message, state: FSMContext):
    """Обработка суммы для CryptoBot"""
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
            f"💲 USDT: <code>{invoice['amount']}</code>\n\n"
            f"⏳ ПОСЛЕ ОПЛАТЫ БАЛАНС БУДЕТ НАЧИСЛЕН АВТОМАТИЧЕСКИ В ТЕЧЕНИЕ НЕСКОЛЬКИХ СЕКУНД"
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

async def check_crypto_payment_status(invoice_id: str) -> bool:
    """Проверяет, оплачен ли счет в CryptoBot"""
    try:
        url = f"{CRYPTOBOT_API_URL}/getInvoices"
        headers = {'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN}
        params = {'invoice_ids': invoice_id}
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data.get('ok') and data.get('result') and len(data['result']['items']) > 0:
                invoice = data['result']['items'][0]
                # Статус 'paid' означает, что счет оплачен
                return invoice.get('status') == 'paid'
        return False
    except Exception as e:
        logger.error(f"Error checking crypto payment: {e}")
        return False
        
async def check_crypto_payments():
    """Фоновая задача для проверки неоплаченных крипто-платежей"""
    while True:
        try:
            # Получаем все ожидающие крипто-платежи
            conn = sqlite3.connect('shop.db')
            c = conn.cursor()
            c.execute("SELECT id, user_id, amount, invoice_id FROM pending_payments WHERE method='crypto' AND status='pending'")
            payments = c.fetchall()
            conn.close()
            
            for payment_id, user_id, amount, invoice_id in payments:
                if await check_crypto_payment_status(invoice_id):
                    # Платеж оплачен - начисляем баланс
                    update_balance(user_id, amount)
                    update_payment_status(payment_id, 'confirmed')
                    
                    # Реферальный бонус
                    user = get_user(user_id)
                    if user and user[4]:
                        reward = amount * (get_setting('referral_reward') / 100)
                        update_balance(user[4], reward)
                    
                    # Уведомляем пользователя
                    try:
                        await bot.send_message(
                            user_id,
                            f"✅ <b>ОПЛАТА ПОДТВЕРЖДЕНА!</b>\n\n"
                            f"💰 СУММА: <code>{amount} ₽</code>\n"
                            f"💳 БАЛАНС ПОПОЛНЕН."
                        )
                    except:
                        pass
                    
                    logger.info(f"Crypto payment {payment_id} confirmed automatically")
            
            # Проверяем каждые 10 секунд
            await asyncio.sleep(10)
            
        except Exception as e:
            logger.error(f"Error in crypto payment checker: {e}")
            await asyncio.sleep(30)
       
async def on_startup():
    """Действия при запуске бота"""
    asyncio.create_task(check_crypto_payments())
    logger.info("🚀 Crypto payment checker started")
    
# ==================== АДМИНСКИЕ ОБРАБОТЧИКИ ПЛАТЕЖЕЙ ====================
@dp.callback_query(lambda c: c.data.startswith('send_details_'))
async def send_payment_details(callback: types.CallbackQuery, state: FSMContext):
    """Отправка реквизитов"""
    payment_id = int(callback.data.split('_')[2])
    await state.update_data(payment_id=payment_id)
    await callback.message.edit_text("✍️ ВВЕДИ РЕКВИЗИТЫ ДЛЯ ОПЛАТЫ:")
    await state.set_state(AdminPaymentStates.waiting_for_payment_details)
    await callback.answer()

@dp.message(AdminPaymentStates.waiting_for_payment_details)
async def payment_details_handler(message: types.Message, state: FSMContext):
    """Обработка реквизитов"""
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
    """Пользователь сообщил об оплате"""
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
        await callback.message.edit_text("✅ СООБЩЕНИЕ ОТПРАВЛЕНО АДМИНИСТРАТОРУ.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_confirm_'))
async def admin_confirm_payment(callback: types.CallbackQuery):
    """Подтверждение платежа админом"""
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
        
        await callback.message.edit_text(f"✅ ПЛАТЕЖ #{payment_id} ПОДТВЕРЖДЕН.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_reject_'))
async def admin_reject_payment(callback: types.CallbackQuery):
    """Отклонение платежа админом"""
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
        
        await callback.message.edit_text(f"❌ ПЛАТЕЖ #{payment_id} ОТКЛОНЕН.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('check_crypto_'))
async def check_crypto_payment(callback: types.CallbackQuery):
    """Проверка крипто-платежа"""
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    
    if not payment or payment[4] != 'pending':
        await callback.message.edit_text("❌ ПЛАТЕЖ НЕ НАЙДЕН ИЛИ УЖЕ ОБРАБОТАН.")
        await callback.answer()
        return
    
    await callback.message.edit_text("⏳ ОЖИДАНИЕ ПОДТВЕРЖДЕНИЯ ОТ АДМИНИСТРАТОРА...")
    await callback.answer()

# ==================== АДМИН ПАНЕЛЬ ====================
@dp.message(F.text == "⚙️ АДМИН")
async def admin_panel(message: types.Message):
    """Админ панель"""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У ТЕБЯ НЕТ ДОСТУПА.")
        return
    await message.answer("⚙️ <b>АДМИН ПАНЕЛЬ</b>", reply_markup=admin_keyboard())

# ----- ДОБАВЛЕНИЕ ТОВАРА -----
@dp.callback_query(F.data == "admin_add_product")
async def admin_add_product(callback: types.CallbackQuery, state: FSMContext):
    """Начало добавления товара"""
    await callback.message.edit_text("➕ ВВЕДИ НАЗВАНИЕ ТОВАРА:")
    await state.set_state(ProductStates.waiting_for_name)
    await callback.answer()

@dp.message(ProductStates.waiting_for_name)
async def product_name_handler(message: types.Message, state: FSMContext):
    """Обработка названия товара"""
    await state.update_data(name=message.text)
    await message.answer("💰 ВВЕДИ ЦЕНУ В РУБЛЯХ:")
    await state.set_state(ProductStates.waiting_for_price)

@dp.message(ProductStates.waiting_for_price)
async def product_price_handler(message: types.Message, state: FSMContext):
    """Обработка цены товара"""
    try:
        price = float(message.text)
        await state.update_data(price=price)
        await message.answer("📱 ВВЕДИ НОМЕР ТЕЛЕФОНА АККАУНТА:")
        await state.set_state(ProductStates.waiting_for_phone)
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО.")

@dp.message(ProductStates.waiting_for_phone)
async def product_phone_handler(message: types.Message, state: FSMContext):
    """Обработка номера телефона"""
    phone = message.text.strip()
    await state.update_data(phone=phone)
    
    await message.answer(
        "🔐 <b>ВВЕДИ ПАРОЛЬ ОТ АККАУНТА (ОБЛАЧНЫЙ ПАРОЛЬ / 2FA)</b>\n\n"
        "Если пароля нет - отправь: <code>пропустить</code>"
    )
    await state.set_state(ProductStates.waiting_for_account_password)

@dp.message(ProductStates.waiting_for_account_password)
async def product_account_password_handler(message: types.Message, state: FSMContext):
    """Обработка пароля аккаунта"""
    pwd = message.text.strip()
    if pwd.lower() in ['пропустить', 'нет', '-', '']:
        await state.update_data(account_password=None)
    else:
        await state.update_data(account_password=pwd)
    
    data = await state.get_data()
    phone = data['phone']
    
    status_msg = await message.answer("🔄 ВЫПОЛНЯЮ ВХОД В TELEGRAM...")
    result = await login_to_telegram(phone)
    
    if not result['success']:
        await status_msg.edit_text(f"❌ ОШИБКА ВХОДА: {result.get('error', 'НЕИЗВЕСТНАЯ')}")
        await state.clear()
        return
    
    if result.get('already_logged'):
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
    elif result.get('need_code'):
        await state.update_data(phone=result['phone'])
        await status_msg.edit_text(
            f"📱 КОД ОТПРАВЛЕН НА {result['phone']}\n\nВВЕДИ КОД:"
        )
        await state.set_state(ProductStates.waiting_for_code)
    else:
        await status_msg.edit_text(f"❌ НЕИЗВЕСТНЫЙ СЦЕНАРИЙ: {result}")
        await state.clear()

@dp.message(ProductStates.waiting_for_code)
async def product_code_handler(message: types.Message, state: FSMContext):
    """Обработка кода подтверждения"""
    code = message.text.strip()
    data = await state.get_data()
    phone = data['phone']
    
    status_msg = await message.answer("🔄 ПРОВЕРЯЮ КОД...")
    result = await verify_code(phone, code)
    
    if not result['success']:
        await status_msg.edit_text(f"❌ ОШИБКА: {result.get('error', 'НЕИЗВЕСТНАЯ')}")
        await state.clear()
        return
    
    if result.get('need_password'):
        await state.update_data(phone=phone)
        await status_msg.edit_text(
            "🔐 <b>ТРЕБУЕТСЯ 2FA ПАРОЛЬ (ОБЛАЧНЫЙ ПАРОЛЬ)</b>\n\n"
            "ВВЕДИ ПАРОЛЬ:"
        )
        await state.set_state(ProductStates.waiting_for_password)
    else:
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

@dp.message(ProductStates.waiting_for_password)
async def product_password_handler(message: types.Message, state: FSMContext):
    """Обработка 2FA пароля"""
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
    """Удаление товара"""
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 НЕТ ТОВАРОВ.")
        await callback.answer()
        return
    
    buttons = []
    for prod in products:
        pid, name, price, *_ = prod
        buttons.append([InlineKeyboardButton(text=f"{name} | {price} ₽", callback_data=f"del_{pid}")])
    buttons.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")])
    
    await callback.message.edit_text(
        "🗑 <b>ВЫБЕРИ ТОВАР ДЛЯ УДАЛЕНИЯ:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('del_'))
async def confirm_delete(callback: types.CallbackQuery):
    """Подтверждение удаления"""
    pid = int(callback.data.split('_')[1])
    delete_product(pid)
    await callback.message.edit_text("✅ ТОВАР УДАЛЕН!")
    await callback.answer()

# ----- СПИСОК ТОВАРОВ -----
@dp.callback_query(F.data == "admin_list_products")
async def admin_list_products(callback: types.CallbackQuery):
    """Список товаров"""
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 НЕТ ТОВАРОВ.")
        await callback.answer()
        return
    
    text = "📦 <b>СПИСОК ТОВАРОВ:</b>\n\n"
    for prod in products:
        pid, name, price, phone, session, region, year, added = prod[:8]
        text += f"🆔 <code>{pid}</code> | {name} | <code>{price} ₽</code> | {region} | {year}\n"
    
    await callback.message.edit_text(text)
    await callback.answer()

# ----- СТАТИСТИКА -----
@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    """Статистика"""
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
    await callback.message.edit_text(text)
    await callback.answer()

# ----- НАЧИСЛЕНИЕ БАЛАНСА -----
@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало начисления баланса"""
    await callback.message.edit_text("💰 ВВЕДИ ID ПОЛЬЗОВАТЕЛЯ:")
    await state.set_state(AdminAddBalanceStates.waiting_for_user_id)
    await callback.answer()

@dp.message(AdminAddBalanceStates.waiting_for_user_id)
async def admin_add_balance_user_id(message: types.Message, state: FSMContext):
    """Обработка ID пользователя"""
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
    """Обработка суммы"""
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

# ----- РАССЫЛКА -----
@dp.callback_query(F.data == "admin_mailing")
async def admin_mailing_start(callback: types.CallbackQuery, state: FSMContext):
    """Начало рассылки"""
    await callback.message.edit_text(
        "📢 <b>ВВЕДИ ТЕКСТ ДЛЯ РАССЫЛКИ</b>\n\n"
        "Доступны переменные:\n"
        "• <code>{name}</code> — username\n"
        "• <code>{id}</code> — ID пользователя\n\n"
        "Можно использовать HTML-теги: <b>жирный</b>, <i>курсив</i>, <code>код</code>"
    )
    await state.set_state(MailingStates.waiting_for_message)
    await callback.answer()

@dp.message(MailingStates.waiting_for_message)
async def admin_mailing_message(message: types.Message, state: FSMContext):
    """Обработка текста рассылки"""
    await state.update_data(text=message.text)
    users = get_all_users()
    
    preview = message.text.replace("{name}", message.from_user.first_name or "User")
    preview = preview.replace("{id}", str(message.from_user.id))
    
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
    """Отправка рассылки"""
    data = await state.get_data()
    text = data['text']
    
    await callback.message.edit_text("🔄 НАЧИНАЮ РАССЫЛКУ...")
    
    users = get_all_users()
    success = 0
    failed = 0
    
    for uid, uname in users:
        try:
            user_text = text.replace("{name}", uname or "User").replace("{id}", str(uid))
            await bot.send_message(uid, user_text, parse_mode="HTML")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка отправки {uid}: {e}")
    
    await callback.message.edit_text(
        f"✅ <b>РАССЫЛКА ЗАВЕРШЕНА!</b>\n\n"
        f"✅ УСПЕШНО: <b>{success}</b>\n"
        f"❌ ОШИБОК: <b>{failed}</b>"
    )
    await state.clear()
    await callback.answer()

# ----- НАСТРОЙКИ -----
@dp.callback_query(F.data == "admin_settings")
async def admin_settings(callback: types.CallbackQuery):
    """Настройки"""
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
    await callback.message.edit_text(text, reply_markup=admin_settings_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_stars")
async def set_stars(callback: types.CallbackQuery, state: FSMContext):
    """Изменение курса Stars"""
    await callback.message.edit_text(f"⭐ ТЕКУЩИЙ КУРС: <code>{get_setting('stars_rate')} ₽</code>\nВВЕДИ НОВЫЙ:")
    await state.set_state(AdminSettingsStates.waiting_for_stars)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_stars)
async def stars_set_handler(message: types.Message, state: FSMContext):
    """Обработка нового курса Stars"""
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
    """Изменение курса USDT"""
    await callback.message.edit_text(f"💵 ТЕКУЩИЙ КУРС: <code>{get_setting('usdt_rate')} ₽</code>\nВВЕДИ НОВЫЙ:")
    await state.set_state(AdminSettingsStates.waiting_for_usdt)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_usdt)
async def usdt_set_handler(message: types.Message, state: FSMContext):
    """Обработка нового курса USDT"""
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
    """Изменение скидки"""
    await callback.message.edit_text(f"🎁 ТЕКУЩАЯ СКИДКА: <b>{get_setting('referral_discount')}%</b>\nВВЕДИ НОВУЮ (0-100):")
    await state.set_state(AdminSettingsStates.waiting_for_discount)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_discount)
async def discount_set_handler(message: types.Message, state: FSMContext):
    """Обработка новой скидки"""
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
    """Изменение награды"""
    await callback.message.edit_text(f"💸 ТЕКУЩАЯ НАГРАДА: <b>{get_setting('referral_reward')}%</b>\nВВЕДИ НОВУЮ (0-100):")
    await state.set_state(AdminSettingsStates.waiting_for_reward)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_reward)
async def reward_set_handler(message: types.Message, state: FSMContext):
    """Обработка новой награды"""
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

# ==================== НАВИГАЦИЯ ====================
@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    """Назад в админку"""
    await callback.message.edit_text("⚙️ <b>АДМИН ПАНЕЛЬ</b>", reply_markup=admin_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: types.CallbackQuery):
    """Назад в каталог"""
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 КАТАЛОГ ПУСТ")
        await callback.answer()
        return
    await callback.message.edit_text("📦 <b>ВЫБЕРИ ТОВАР:</b>", reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(F.data == "back_to_balance")
async def back_to_balance(callback: types.CallbackQuery):
    """Назад к балансу"""
    bal = get_balance(callback.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 <b>ТВОЙ БАЛАНС:</b> <code>{bal} ₽</code>\n"
        f"⭐ ЭКВИВАЛЕНТ: <code>{int(bal/stars_rate)} STARS</code>\n\n"
        f"ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    )
    await callback.message.edit_text(text, reply_markup=payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_purchases")
async def back_to_purchases(callback: types.CallbackQuery):
    """Назад к покупкам"""
    purchases = get_user_purchases(callback.from_user.id)
    if not purchases:
        await callback.message.edit_text("📭 У ТЕБЯ НЕТ ПОКУПОК")
        await callback.answer()
        return
    await callback.message.edit_text("📜 <b>ТВОИ ПОКУПКИ:</b>", reply_markup=purchases_keyboard(purchases))
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    """Назад в главное меню"""
    await cmd_start(callback.message)
    await callback.answer()

# ==================== ЗАПУСК БОТА ====================
async def on_startup():
    """Действия при запуске бота"""
    # Запускаем фоновую задачу для проверки крипто-платежей
    asyncio.create_task(check_crypto_payments())
    logger.info("🚀 Фоновая проверка крипто-платежей запущена")

async def main():
    """Главная функция запуска бота"""
    global bot_username
    
    # Получаем информацию о боте
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    
    # Выполняем действия при запуске
    await on_startup()
    
    # Запускаем бота
    logger.info(f"🚀 БОТ @{bot_username} ЗАПУЩЕН И ГОТОВ К РАБОТЕ!")
    logger.info("✅ Все системы функционируют нормально")
    logger.info("📨 Коды берутся напрямую из Telegram аккаунтов")
    logger.info("💰 Реферальная система активна")
    logger.info("⚙️ Админ-панель доступна")
    
    # Запускаем поллинг
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")