import asyncio
import logging
import sqlite3
import random
import string
import re
import os
import requests
from datetime import datetime

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, PreCheckoutQuery
)
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    FloodWaitError
)

# ==================== НАСТРОЙКИ ====================
TOKEN = "8561605758:AAFOFA3pT3TTxzMQXWS8GxZXWGBKdlp9KpU"
CRYPTOBOT_TOKEN = "546557:AAA5MxwCASiCnPAQOnZ6cNkbhgnirFIrxhU"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
ADMIN_IDS = [7546928092]          # ID администраторов

API_ID = 35800959
API_HASH = "708e7d0bc3572355bcaf68562cc068f1"

STARS_RATE = 1.4      # курс по умолчанию
USDT_RATE = 70

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot_username = None

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

if not os.path.exists('sessions'):
    os.makedirs('sessions')

# Временные хранилища Telethon
temp_clients = {}      # phone -> client
active_sessions = {}   # phone -> session_string

# ==================== СОСТОЯНИЯ FSM ====================
class ProductStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_phone = State()
    waiting_for_account_password = State()   # облачный пароль (2FA / обычный)
    waiting_for_code = State()
    waiting_for_password = State()            # 2FA (если уже запрошен)

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

class MailingStates(StatesGroup):
    waiting_for_message = State()
    waiting_for_confirm = State()

# ==================== БАЗА ДАННЫХ (ОСНОВНЫЕ ТАБЛИЦЫ) ====================
def init_db():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    # Пользователи
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
    # Товары
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
    # Покупки
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
    # Коды (история)
    c.execute('''CREATE TABLE IF NOT EXISTS account_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        phone TEXT,
        code TEXT,
        received_date TEXT,
        message_text TEXT
    )''')
    # Ожидающие платежи
    c.execute('''CREATE TABLE IF NOT EXISTS pending_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        method TEXT,
        status TEXT DEFAULT 'pending',
        created_date TEXT,
        invoice_id TEXT
    )''')
    # Настройки
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

init_db()

# ==================== ФУНКЦИИ ДЛЯ РЕФЕРАЛЬНОЙ СИСТЕМЫ ====================
def generate_referral_code(user_id):
    """Генерирует уникальный реферальный код для пользователя"""
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{user_id}{random_part}"

def get_user(user_id, username=None, referrer_id=None):
    """Возвращает пользователя или создаёт нового, если его нет"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = c.fetchone()

    if not user and username:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        referral_code = generate_referral_code(user_id)
        # Если пользователь пришёл по реферальной ссылке, даём ему право на скидку
        first_discount = 0 if referrer_id else 1  # 0 – скидка доступна, 1 – уже использована или не положена
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

def get_user_by_referral_code(code):
    """Поиск пользователя по реферальному коду"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE referral_code = ?", (code,))
    user = c.fetchone()
    conn.close()
    return user

def can_use_discount(user_id):
    """Проверяет, может ли пользователь воспользоваться скидкой (пришёл по рефералке и ещё не использовал)"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT first_discount_used, referrer_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result and result[0] == 0 and result[1] is not None

def apply_first_discount(user_id):
    """Помечает, что пользователь использовал скидку"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET first_discount_used = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def add_referral_earning(user_id, amount, from_user_id):
    """Начисляет бонус рефереру (user_id) за пополнение реферала (from_user_id)"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE users SET balance = balance + ?, total_referral_earnings = total_referral_earnings + ? WHERE user_id = ?",
              (amount, amount, user_id))
    # (Опционально) записываем выплату в отдельную таблицу, но у нас её пока нет
    conn.commit()
    conn.close()

def get_referral_stats(user_id):
    """Возвращает количество рефералов и общий заработок"""
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

def get_all_users():
    """Возвращает список всех пользователей (ID, username) для рассылки"""
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT user_id, username FROM users ORDER BY user_id")
    users = c.fetchall()
    conn.close()
    return users

# ==================== ДРУГИЕ ФУНКЦИИ РАБОТЫ С БД ====================
def get_setting(key):
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

def update_setting(key, value):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE settings SET value = ? WHERE key = ?", (str(value), key))
    conn.commit()
    conn.close()

def get_balance(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else 0

def update_balance(user_id, amount):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()

# ==================== TELEGRAM AUTH ====================
async def detect_region(phone: str) -> str:
    """Определяет регион по коду телефона"""
    try:
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
    except:
        return '❓ Неизвестно'

async def login_to_telegram(phone: str):
    """
    Начинает процесс входа: либо сразу возвращает сессию, либо запрашивает код.
    Возвращает словарь с ключами success, session, region, year, already_logged, need_code, phone, client
    """
    try:
        # Очистка номера
        phone = re.sub(r'[^\d+]', '', phone)
        if not phone.startswith('+'):
            phone = '+' + phone

        # Если уже есть активная сессия в памяти – используем её
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

        # Создаём новую сессию
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

async def verify_code(phone: str, code: str):
    """Подтверждает код, возвращает сессию или запрос 2FA"""
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
        # Требуется 2FA (облачный пароль)
        return {'success': True, 'need_password': True, 'phone': phone}
    except PhoneCodeInvalidError:
        return {'success': False, 'error': '❌ Неверный код'}
    except Exception as e:
        logger.error(f"Verify code error: {e}")
        return {'success': False, 'error': str(e)}

async def verify_password(phone: str, password: str):
    """Подтверждает 2FA пароль (облачный пароль)"""
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

# ==================== ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ КОДОВ ИЗ АККАУНТА ====================
def save_code(phone: str, code: str, message_text: str, received_date: str = None):
    """Сохраняет код в таблицу account_codes (опционально)"""
    if received_date is None:
        received_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("INSERT INTO account_codes (phone, code, received_date, message_text) VALUES (?, ?, ?, ?)",
              (phone, code, received_date, message_text[:200]))
    conn.commit()
    conn.close()

async def get_live_codes_from_account(session_string: str, limit: int = 20):
    """
    Подключается к аккаунту и вытаскивает последние сообщения с кодами.
    Возвращает список словарей с ключами code, type, date, text.
    """
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
            # Ищем все последовательности цифр длиной 4-8
            found_codes = re.findall(r'\b(\d{4,8})\b', message.text)
            for code in found_codes:
                # Определяем тип
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

# ==================== ФУНКЦИИ ДЛЯ ТОВАРОВ ====================
def get_products():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products ORDER BY id DESC")
    products = c.fetchall()
    conn.close()
    return products

def get_product(product_id: int):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    product = c.fetchone()
    conn.close()
    return product

def add_product(name: str, price: float, phone: str, session_string: str, region: str, year: int, password: str = None):
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

# ==================== КЛАВИАТУРЫ ====================
def main_keyboard(user_id: int):
    buttons = [
        [KeyboardButton(text="🛍 КАТАЛОГ")],
        [KeyboardButton(text="💰 БАЛАНС"), KeyboardButton(text="👤 ПРОФИЛЬ")],
        [KeyboardButton(text="👥 РЕФЕРАЛЫ"), KeyboardButton(text="📜 ПОКУПКИ")],
        [KeyboardButton(text="📝 ОТЗЫВЫ"), KeyboardButton(text="📞 ПОДДЕРЖКА")]
    ]
    if user_id in ADMIN_IDS:
        buttons.append([KeyboardButton(text="⚙️ АДМИН")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def admin_keyboard():
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

def admin_settings_keyboard():
    buttons = [
        [InlineKeyboardButton(text="⭐ КУРС STARS", callback_data="set_stars")],
        [InlineKeyboardButton(text="💵 КУРС USDT", callback_data="set_usdt")],
        [InlineKeyboardButton(text="🎁 СКИДКА РЕФЕРАЛАМ", callback_data="set_discount")],
        [InlineKeyboardButton(text="💸 НАГРАДА ЗА РЕФЕРАЛА", callback_data="set_reward")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_keyboard():
    buttons = [
        [InlineKeyboardButton(text="⭐ TELEGRAM STARS", callback_data="pay_stars")],
        [InlineKeyboardButton(text="💳 СБП", callback_data="pay_sbp")],
        [InlineKeyboardButton(text="₿ CRYPTOBOT", callback_data="pay_crypto")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_balance")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def catalog_keyboard(products):
    buttons = []
    for product in products:
        if len(product) >= 8:
            pid, name, price, phone, session, region, year, added = product[:8]
            age = datetime.now().year - year
            button_text = f"{name} | {region} | {age} ЛЕТ | {price} ₽"
            buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"view_{pid}")])
    buttons.append([InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data="refresh_catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def product_keyboard(product_id: int):
    buttons = [
        [InlineKeyboardButton(text="💳 КУПИТЬ", callback_data=f"buy_{product_id}")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_catalog")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def purchases_keyboard(purchases):
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

def purchase_actions_keyboard(purchase_id: int):
    buttons = [
        [InlineKeyboardButton(text="🔑 ДАННЫЕ ВХОДА", callback_data=f"show_login_{purchase_id}")],
        [InlineKeyboardButton(text="📨 ПОКАЗАТЬ КОДЫ", callback_data=f"show_codes_{purchase_id}")],
        [InlineKeyboardButton(text="📁 ФАЙЛ СЕССИИ", callback_data=f"session_file_{purchase_id}")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_purchases")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def insufficient_balance_keyboard():
    buttons = [[InlineKeyboardButton(text="💰 ПОПОЛНИТЬ", callback_data="show_payment_methods")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_payment_keyboard(payment_id: int):
    buttons = [
        [InlineKeyboardButton(text="✍️ РЕКВИЗИТЫ", callback_data=f"send_details_{payment_id}")],
        [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ", callback_data=f"admin_confirm_{payment_id}"),
         InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"admin_reject_{payment_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def referral_keyboard():
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

    if len(args) > 1 and args[1].startswith('ref_'):
        referral_code = args[1][4:]
        referrer = get_user_by_referral_code(referral_code)
        if referrer and referrer[0] != message.from_user.id:
            referrer_id = referrer[0]

    user = get_user(message.from_user.id, message.from_user.username, referrer_id)

    welcome_text = (
        "👋 ДОБРО ПОЖАЛОВАТЬ В MORGAN SHOP!\n\n"
        "🔥 ЛУЧШИЕ TELEGRAM АККАУНТЫ\n"
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
    products = get_products()
    if not products:
        await message.answer("📭 КАТАЛОГ ПУСТ. ТОВАРЫ ПОЯВЯТСЯ ПОЗЖЕ.")
        return
    await message.answer("📦 ВЫБЕРИ ТОВАР ДЛЯ ПРОСМОТРА:", reply_markup=catalog_keyboard(products))

@dp.message(F.text == "💰 БАЛАНС")
async def balance(message: types.Message):
    user_balance = get_balance(message.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 ТВОЙ БАЛАНС: {user_balance} ₽\n"
        f"⭐ ЭКВИВАЛЕНТ: {int(user_balance / stars_rate)} STARS\n\n"
        f"ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    )
    await message.answer(text, reply_markup=payment_keyboard())

@dp.message(F.text == "👤 ПРОФИЛЬ")
async def profile(message: types.Message):
    user = get_user(message.from_user.id)
    if user is None:
        user = get_user(message.from_user.id, message.from_user.username)

    purchases = get_user_purchases(message.from_user.id)
    discount_status = "✅ ДОСТУПНА" if can_use_discount(message.from_user.id) else "❌ НЕ ДОСТУПНА"

    text = (
        f"👤 ТВОЙ ПРОФИЛЬ\n\n"
        f"🆔 ID: {message.from_user.id}\n"
        f"👤 USERNAME: @{message.from_user.username or 'НЕТ'}\n"
        f"💰 БАЛАНС: {user[2] if user else 0} ₽\n"
        f"📦 ВСЕГО ПОКУПОК: {len(purchases)}\n"
        f"🎁 СКИДКА НА ПЕРВОЕ ПОПОЛНЕНИЕ: {discount_status}\n"
        f"📅 ДАТА РЕГИСТРАЦИИ: {user[3][:10] if user else 'НЕТ'}"
    )
    await message.answer(text)

@dp.message(F.text == "👥 РЕФЕРАЛЫ")
async def referral_system(message: types.Message):
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
        f"👥 РЕФЕРАЛЬНАЯ СИСТЕМА\n\n"
        f"💰 НАГРАДА: {get_setting('referral_reward')}% ОТ ПОПОЛНЕНИЙ РЕФЕРАЛОВ\n"
        f"🎁 СКИДКА ДЛЯ РЕФЕРАЛОВ: {get_setting('referral_discount')}% НА ПЕРВОЕ ПОПОЛНЕНИЕ\n\n"
        f"🔗 ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА:\n{referral_link}\n\n"
        f"📤 ОТПРАВЛЯЙ ЕЁ ДРУЗЬЯМ И ПОЛУЧАЙ НАГРАДУ!"
    )
    await message.answer(text, reply_markup=referral_keyboard())

@dp.callback_query(F.data == "show_ref_link")
async def show_ref_link(callback: types.CallbackQuery):
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
        f"🔗 ТВОЯ РЕФЕРАЛЬНАЯ ССЫЛКА:\n\n"
        f"{referral_link}\n\n"
        f"📤 ОТПРАВЛЯЙ ЕЁ ДРУЗЬЯМ И ПОЛУЧАЙ {get_setting('referral_reward')}% ОТ ИХ ПОПОЛНЕНИЙ!"
    )
    await callback.message.edit_text(text, reply_markup=referral_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "ref_stats")
async def ref_stats(callback: types.CallbackQuery):
    stats = get_referral_stats(callback.from_user.id)
    text = f"📊 СТАТИСТИКА РЕФЕРАЛОВ\n\n"
    text += f"👥 ПРИГЛАШЕНО: {stats['total_count']}\n"
    text += f"💰 ЗАРАБОТАНО: {stats['total_earnings']} ₽\n\n"
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
    purchases = get_user_purchases(message.from_user.id)
    if not purchases:
        await message.answer("📭 У ТЕБЯ ПОКА НЕТ ПОКУПОК.")
        return
    await message.answer("📜 ТВОИ КУПЛЕННЫЕ АККАУНТЫ:", reply_markup=purchases_keyboard(purchases))

@dp.message(F.text == "📝 ОТЗЫВЫ")
async def reviews_link(message: types.Message):
    channel_link = get_setting('reviews_channel_link')
    if channel_link:
        await message.answer(f"📢 НАШ КАНАЛ С ОТЗЫВАМИ:\n{channel_link}")
    else:
        await message.answer("📢 КАНАЛ С ОТЗЫВАМИ ЕЩЁ НЕ НАСТРОЕН.")

@dp.message(F.text == "📞 ПОДДЕРЖКА")
async def support(message: types.Message):
    text = (
        "📞 СЛУЖБА ПОДДЕРЖКИ\n\n"
        "ПО ВСЕМ ВОПРОСАМ ПИШИ СЮДА: @deaMorgan"
    )
    await message.answer(text)

# ==================== ДЕТАЛИ ТОВАРА ====================
@dp.callback_query(F.data == "refresh_catalog")
async def refresh_catalog(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 КАТАЛОГ ПУСТ.")
        await callback.answer()
        return
    await callback.message.edit_text("📦 ВЫБЕРИ ТОВАР:", reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('view_'))
async def view_product(callback: types.CallbackQuery):
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
        f"📦 {name}\n\n"
        f"🌍 РЕГИОН: {region}\n"
        f"📅 ГОД СОЗДАНИЯ: {year} ({age} ЛЕТ)\n"
        f"💰 ЦЕНА: {price} ₽ / {stars_price} ⭐\n"
        f"🕐 ДОБАВЛЕН: {added[:10]}\n\n"
        f"📱 ТЕЛЕФОН БУДЕТ ДОСТУПЕН ПОСЛЕ ПОКУПКИ."
    )
    await callback.message.edit_text(text, reply_markup=product_keyboard(product_id))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('buy_'))
async def buy_product(callback: types.CallbackQuery):
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)

    if not product:
        await callback.message.edit_text("❌ ТОВАР НЕ НАЙДЕН.")
        await callback.answer()
        return

    # product: id, name, price, phone, session, region, year, added, password
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
            f"✅ ПОКУПКА УСПЕШНА!\n\n"
            f"📦 ТОВАР: {name}\n"
            f"💰 ЦЕНА: {price} ₽\n"
            f"🌍 РЕГИОН: {region}\n"
            f"📅 ГОД: {year} ({age} ЛЕТ)\n"
            f"📱 ТЕЛЕФОН: {phone}\n"
        )
        if password and password not in ['None', '']:
            text += f"🔑 ПАРОЛЬ АККАУНТА: {password}\n"
        text += f"\n📁 ФАЙЛ СЕССИИ ДОСТУПЕН В РАЗДЕЛЕ ПОКУПКИ"

        await callback.message.edit_text(text)
    else:
        need = price - user_balance
        await callback.message.edit_text(
            f"❌ НЕДОСТАТОЧНО СРЕДСТВ\n\nНУЖНО ЕЩЕ: {need} ₽",
            reply_markup=insufficient_balance_keyboard()
        )
    await callback.answer()

# ==================== ДЕТАЛИ ПОКУПКИ ====================
@dp.callback_query(lambda c: c.data.startswith('purchase_'))
async def purchase_details(callback: types.CallbackQuery):
    purchase_id = int(callback.data.split('_')[1])
    purchase = get_purchase(purchase_id)

    if not purchase or purchase[1] != callback.from_user.id:
        await callback.message.edit_text("❌ ПОКУПКА НЕ НАЙДЕНА.")
        await callback.answer()
        return

    pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]

    text = (
        f"📱 АККАУНТ #{pid}\n\n"
        f"📱 ТЕЛЕФОН: {phone}\n"
        f"💰 ЦЕНА: {price} ₽\n"
        f"🌍 РЕГИОН: {region}\n"
        f"📅 ГОД АККАУНТА: {year}\n"
        f"📦 КУПЛЕН: {date[:16]}\n\n"
        f"ВЫБЕРИ ДЕЙСТВИЕ:"
    )
    await callback.message.edit_text(text, reply_markup=purchase_actions_keyboard(pid))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('show_login_'))
async def show_login(callback: types.CallbackQuery):
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
        f"🔑 ДАННЫЕ ДЛЯ ВХОДА (АККАУНТ #{pid})\n\n"
        f"📱 ТЕЛЕФОН: {phone}\n"
        f"🔐 СЕССИЯ: {session}\n"
    )
    if password and password not in ['None', 'пропустить', '']:
        text += f"🔑 ПАРОЛЬ АККАУНТА: {password}\n\n"
    else:
        text += f"🔑 ПАРОЛЬ АККАУНТА: НЕ УСТАНОВЛЕН\n\n"
    text += "⚠️ СОХРАНИ ЭТИ ДАННЫЕ В БЕЗОПАСНОМ МЕСТЕ!"

    await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data=f"purchase_{purchase_id}")]
    ]))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('show_codes_'))
async def show_codes(callback: types.CallbackQuery):
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
            text = f"📨 АККАУНТ #{pid}\n\n❌ НЕТ КОДОВ В ЭТОМ АККАУНТЕ"
        else:
            text = f"📨 КОДЫ ИЗ TELEGRAM (АККАУНТ #{pid}):\n\n"
            for i, code_data in enumerate(codes, 1):
                star = "⭐ " if i == 1 else ""
                text += f"{i}. {star}{code_data['type']} {code_data['code']}  |  🕐 {code_data['date']}\n"

        await msg.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data=f"show_codes_{purchase_id}")],
            [InlineKeyboardButton(text="🔙 НАЗАД", callback_data=f"purchase_{purchase_id}")]
        ]))
    except Exception as e:
        await msg.edit_text(f"❌ ОШИБКА: {str(e)[:100]}")

    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('session_file_'))
async def session_file(callback: types.CallbackQuery):
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
            types.FSInputFile(filename),
            caption=f"📁 ФАЙЛ СЕССИИ ДЛЯ {phone}"
        )

    os.remove(filename)
    await callback.answer()

# ==================== АДМИН ПАНЕЛЬ ====================
@dp.message(F.text == "⚙️ АДМИН")
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У ТЕБЯ НЕТ ДОСТУПА.")
        return
    await message.answer("⚙️ АДМИН ПАНЕЛЬ", reply_markup=admin_keyboard())

# ----- ДОБАВЛЕНИЕ ТОВАРА -----
@dp.callback_query(F.data == "admin_add_product")
async def admin_add_product(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("➕ ВВЕДИ НАЗВАНИЕ ТОВАРА:")
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
        "🔐 ВВЕДИ ПАРОЛЬ ОТ АККАУНТА (ОБЛАЧНЫЙ ПАРОЛЬ / 2FA)\n"
        "Если пароля нет - отправь: пропустить"
    )
    await state.set_state(ProductStates.waiting_for_account_password)

@dp.message(ProductStates.waiting_for_account_password)
async def product_account_password_handler(message: types.Message, state: FSMContext):
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
        # Уже авторизован – добавляем товар сразу
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
            f"✅ АККАУНТ УСПЕШНО ДОБАВЛЕН!\n\n"
            f"📦 {data['name']}\n💰 {data['price']} ₽\n🌍 {result['region']}\n📅 {result['year']}\n"
            f"🔑 ПАРОЛЬ: {data.get('account_password', 'НЕТ')}\n🆔 ID: {pid}"
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
            "🔐 ТРЕБУЕТСЯ 2FA ПАРОЛЬ (ОБЛАЧНЫЙ ПАРОЛЬ)\n\n"
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
            f"✅ АККАУНТ УСПЕШНО ДОБАВЛЕН!\n\n"
            f"📦 {data['name']}\n💰 {data['price']} ₽\n🌍 {result['region']}\n📅 {result['year']}\n"
            f"🔑 ПАРОЛЬ: {data.get('account_password', 'НЕТ')}\n🆔 ID: {pid}"
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
        f"✅ АККАУНТ УСПЕШНО ДОБАВЛЕН!\n\n"
        f"📦 {data['name']}\n💰 {data['price']} ₽\n🌍 {result['region']}\n📅 {result['year']}\n"
        f"🔑 ПАРОЛЬ: {data.get('account_password', 'НЕТ')}\n🆔 ID: {pid}"
    )
    await state.clear()

# ----- УДАЛЕНИЕ, СПИСОК, СТАТИСТИКА, РАССЫЛКА -----
@dp.callback_query(F.data == "admin_delete_product")
async def admin_delete_product(callback: types.CallbackQuery):
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
    await callback.message.edit_text("🗑 ВЫБЕРИ ТОВАР ДЛЯ УДАЛЕНИЯ:",
                                     reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('del_'))
async def confirm_delete(callback: types.CallbackQuery):
    pid = int(callback.data.split('_')[1])
    delete_product(pid)
    await callback.message.edit_text("✅ ТОВАР УДАЛЕН!")
    await callback.answer()

@dp.callback_query(F.data == "admin_list_products")
async def admin_list_products(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 НЕТ ТОВАРОВ.")
        await callback.answer()
        return
    text = "📦 СПИСОК ТОВАРОВ:\n\n"
    for prod in products:
        pid, name, price, phone, session, region, year, added = prod[:8]
        text += f"🆔 {pid} | {name} | {price} ₽ | {region} | {year}\n"
    await callback.message.edit_text(text)
    await callback.answer()

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
        f"📊 СТАТИСТИКА\n\n"
        f"👥 ПОЛЬЗОВАТЕЛЕЙ: {users}\n"
        f"📦 ТОВАРОВ: {products}\n"
        f"🛒 ПРОДАЖ: {purchases}\n"
        f"💰 ВЫРУЧКА: {revenue} ₽"
    )
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(F.data == "admin_mailing")
async def admin_mailing_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📢 ВВЕДИ ТЕКСТ ДЛЯ РАССЫЛКИ\n\n"
        "Доступны переменные:\n"
        "• {name} — username\n"
        "• {id} — ID пользователя"
    )
    await state.set_state(MailingStates.waiting_for_message)
    await callback.answer()

@dp.message(MailingStates.waiting_for_message)
async def admin_mailing_message(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    users = get_all_users()
    preview = message.text.replace("{name}", message.from_user.first_name or "User")
    preview = preview.replace("{id}", str(message.from_user.id))
    await message.answer(
        f"📢 ПРЕДПРОСМОТР:\n\n{preview}\n\n"
        f"👥 ВСЕГО ПОЛЬЗОВАТЕЛЕЙ: {len(users)}\n\n"
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
    await callback.message.edit_text("🔄 НАЧИНАЮ РАССЫЛКУ...")
    users = get_all_users()
    success = 0
    failed = 0
    for uid, uname in users:
        try:
            user_text = text.replace("{name}", uname or "User").replace("{id}", str(uid))
            await bot.send_message(uid, user_text)
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"Ошибка отправки {uid}: {e}")
    await callback.message.edit_text(
        f"✅ РАССЫЛКА ЗАВЕРШЕНА!\n"
        f"✅ УСПЕШНО: {success}\n❌ ОШИБОК: {failed}"
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
        f"⚙️ ТЕКУЩИЕ НАСТРОЙКИ:\n\n"
        f"⭐ STARS: 1 = {stars} ₽\n"
        f"💵 USDT: 1 = {usdt} ₽\n"
        f"🎁 СКИДКА: {discount}%\n"
        f"💸 НАГРАДА: {reward}%"
    )
    await callback.message.edit_text(text, reply_markup=admin_settings_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_stars")
async def set_stars(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(f"⭐ ТЕКУЩИЙ КУРС: {get_setting('stars_rate')} ₽\nВВЕДИ НОВЫЙ:")
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
        await message.answer(f"✅ КУРС STARS: 1 = {rate} ₽")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_usdt")
async def set_usdt(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(f"💵 ТЕКУЩИЙ КУРС: {get_setting('usdt_rate')} ₽\nВВЕДИ НОВЫЙ:")
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
        await message.answer(f"✅ КУРС USDT: 1 = {rate} ₽")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_discount")
async def set_discount(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(f"🎁 ТЕКУЩАЯ СКИДКА: {get_setting('referral_discount')}%\nВВЕДИ НОВУЮ (0-100):")
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
        await message.answer(f"✅ СКИДКА: {val}%")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "set_reward")
async def set_reward(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(f"💸 ТЕКУЩАЯ НАГРАДА: {get_setting('referral_reward')}%\nВВЕДИ НОВУЮ (0-100):")
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
        await message.answer(f"✅ НАГРАДА: {val}%")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text("💰 ВВЕДИ ID ПОЛЬЗОВАТЕЛЯ:")
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
            await message.answer("❌ СУММА ДОЛЖНА БЫТЬ >0")
            return
        data = await state.get_data()
        uid = data['target_uid']
        update_balance(uid, amount)
        await message.answer(f"✅ БАЛАНС {uid} ПОПОЛНЕН НА {amount} ₽")
        try:
            await bot.send_message(uid, f"💰 АДМИН ПОПОЛНИЛ ТВОЙ БАЛАНС НА {amount} ₽")
        except:
            pass
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

# ----- ПЛАТЕЖИ -----
@dp.callback_query(F.data == "show_payment_methods")
async def show_payment_methods(callback: types.CallbackQuery):
    await callback.message.edit_text("💰 ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:", reply_markup=payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "pay_stars")
async def pay_stars(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(f"⭐ КУРС: 1 STAR = {get_setting('stars_rate')} ₽\nВВЕДИ СУММУ В РУБЛЯХ:")
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
        prices = [LabeledPrice(label="Пополнение", amount=stars)]
        payload = f"stars_{message.from_user.id}_{int(datetime.now().timestamp())}"
        invoice = await bot.create_invoice_link(
            title="Пополнение баланса",
            description=f"{final} ₽ ({stars} ⭐)",
            payload=payload,
            currency="XTR",
            prices=prices
        )
        add_pending_payment(message.from_user.id, final, "stars", payload)
        await message.answer(
            f"⭐ СЧЕТ СОЗДАН\n💰 {final} ₽\n⭐ {stars} STARS",
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
            await message.answer(f"✅ БАЛАНС ПОПОЛНЕН НА {amt} ₽")
        else:
            await message.answer("❌ ПЛАТЕЖ НЕ НАЙДЕН")

# (СБП и Крипто обрабатываются аналогично, но для краткости здесь не расписаны – они есть в исходном коде пользователя)

# ----- НАВИГАЦИЯ -----
@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    await callback.message.edit_text("⚙️ АДМИН ПАНЕЛЬ", reply_markup=admin_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 КАТАЛОГ ПУСТ")
        await callback.answer()
        return
    await callback.message.edit_text("📦 ВЫБЕРИ ТОВАР:", reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(F.data == "back_to_balance")
async def back_to_balance(callback: types.CallbackQuery):
    bal = get_balance(callback.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = f"💰 БАЛАНС: {bal} ₽\n⭐ ЭКВИВАЛЕНТ: {int(bal/stars_rate)} ⭐\n\nВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    await callback.message.edit_text(text, reply_markup=payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_purchases")
async def back_to_purchases(callback: types.CallbackQuery):
    purchases = get_user_purchases(callback.from_user.id)
    if not purchases:
        await callback.message.edit_text("📭 У ТЕБЯ НЕТ ПОКУПОК")
        await callback.answer()
        return
    await callback.message.edit_text("📜 ТВОИ ПОКУПКИ:", reply_markup=purchases_keyboard(purchases))
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await cmd_start(callback.message)
    await callback.answer()

# ==================== ЗАПУСК ====================
async def main():
    global bot_username
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    logger.info(f"🚀 БОТ @{bot_username} ЗАПУЩЕН!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
