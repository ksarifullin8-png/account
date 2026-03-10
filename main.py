import asyncio
from asyncio import events
import logging
import sqlite3
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, LabeledPrice, PreCheckoutQuery
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, FloodWaitError
import random
import string
import re
import os
import requests
import time

# ==================== НАСТРОЙКИ ====================
TOKEN = "8561605758:AAFOFA3pT3TTxzMQXWS8GxZXWGBKdlp9KpU"
CRYPTOBOT_TOKEN = "546557:AAA5MxwCASiCnPAQOnZ6cNkbhgnirFIrxhU"
CRYPTOBOT_API_URL = "https://pay.crypt.bot/api"
ADMIN_IDS = [7546928092]

API_ID = 35800959
API_HASH = "708e7d0bc3572355bcaf68562cc068f1"

STARS_RATE = 1.4
USDT_RATE = 70

bot = Bot(token=TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
bot_username = None

logging.basicConfig(level=logging.INFO)

if not os.path.exists('sessions'):
    os.makedirs('sessions')

# Временные хранилища
temp_clients = {}  # phone -> client
active_sessions = {}  # phone -> session_string

# ==================== СОСТОЯНИЯ FSM ====================
class ProductStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_account_password = State()  # новый стейт для пароля аккаунта

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
    
    # Таблица товаров
    c.execute('''CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        phone TEXT,
        session_string TEXT,
        region TEXT,
        account_year INTEGER,
        added_date TEXT
    )''')
    
    # Таблица покупок
    c.execute('''CREATE TABLE IF NOT EXISTS purchases (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        product_id INTEGER,
        price REAL,
        purchase_date TEXT,
        phone TEXT,
        session_string TEXT,
        region TEXT,
        account_year INTEGER
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
    print("✅ База данных инициализирована")

async def get_all_codes_from_account(phone, limit=30):
    """Получает ВСЕ коды из аккаунта с РЕАЛЬНЫМИ датами"""
    codes = []
    try:
        if phone not in temp_clients:
            if phone not in active_sessions:
                return codes
            client = TelegramClient(StringSession(active_sessions[phone]), API_ID, API_HASH)
            await client.connect()
            temp_clients[phone] = client
        else:
            client = temp_clients[phone]
        
        if not await client.is_user_authorized():
            return codes
        
        print(f"🔍 Ищем коды для {phone}...")
        
        # Ищем сообщения с цифрами
        async for message in client.iter_messages(None, limit=200):
            if message.text:
                # Ищем ЛЮБЫЕ цифры от 4 до 8 знаков
                code_matches = re.findall(r'\b(\d{4,8})\b', message.text)
                for code in code_matches:
                    # Определяем тип кода
                    code_type = "Telegram"
                    if "2fa" in message.text.lower() or "пароль" in message.text.lower() or "password" in message.text.lower():
                        code_type = "2FA"
                    
                    # РЕАЛЬНАЯ дата сообщения
                    msg_date = message.date.strftime("%Y-%m-%d %H:%M:%S")
                    
                    codes.append({
                        'code': code,
                        'type': code_type,
                        'date': msg_date,  # ← РЕАЛЬНАЯ ДАТА!
                        'sender': message.sender_id if message.sender_id else 0,
                        'full_text': message.text[:100]
                    })
                    
                    print(f"✅ Нашел код {code} от {msg_date}")
                    
                    if len(codes) >= limit:
                        break
            
            if len(codes) >= limit:
                break
        
        # Сохраняем в базу с РЕАЛЬНОЙ датой
        for code_data in codes:
            save_code(
                phone, 
                code_data['code'], 
                f"[{code_data['type']}] {code_data['full_text']}",
                code_data['date']  # ← Передаем реальную дату!
            )
        
        # Сортируем по дате (самые новые первые)
        codes.sort(key=lambda x: x['date'], reverse=True)
        
        print(f"✅ Всего найдено кодов: {len(codes)}")
        return codes
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return []
    
# ==================== ФУНКЦИИ БАЗЫ ДАННЫХ ====================
def get_all_users():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT user_id, username FROM users ORDER BY user_id")
    users = c.fetchall()
    conn.close()
    return users

def get_user(user_id, username=None, referrer_id=None):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = c.fetchone()
    
    if not user and username:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        referral_code = f"{user_id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
        first_discount = 0 if referrer_id else 1
        
        c.execute("INSERT INTO users (user_id, username, registered_date, referrer_id, referral_code, first_discount_used) VALUES (?, ?, ?, ?, ?, ?)",
                  (user_id, username, now, referrer_id, referral_code, first_discount))
        if referrer_id:
            c.execute("UPDATE users SET total_referrals = total_referrals + 1 WHERE user_id = ?", (referrer_id,))
        conn.commit()
        c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = c.fetchone()
    
    conn.close()
    return user

def get_user_by_referral_code(code):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE referral_code = ?", (code,))
    user = c.fetchone()
    conn.close()
    return user

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

def can_use_discount(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT first_discount_used, referrer_id FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result and result[0] == 0 and result[1] is not None

def apply_first_discount(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET first_discount_used = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def get_products():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products ORDER BY id DESC")
    products = c.fetchall()
    conn.close()
    return products

def upgrade_db():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    # Добавляем колонку password в products если её нет
    try:
        c.execute("ALTER TABLE products ADD COLUMN password TEXT")
        print("✅ Добавлена колонка password в products")
    except:
        pass
    
    # Добавляем колонку password в purchases если её нет
    try:
        c.execute("ALTER TABLE purchases ADD COLUMN password TEXT")
        print("✅ Добавлена колонка password в purchases")
    except:
        pass
    
    conn.commit()
    conn.close()

# Вызови эту функцию после init_db()
upgrade_db()

def add_product(name, price, phone, session_string, region, year, password=None):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    year = int(year) if year else datetime.now().year
    
    c.execute("INSERT INTO products (name, price, phone, session_string, region, account_year, added_date, password) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              (name, price, phone, session_string, region, year, now, password))
    product_id = c.lastrowid
    conn.commit()
    conn.close()
    return product_id

def get_product(product_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    product = c.fetchone()
    conn.close()
    return product

def delete_product(product_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("DELETE FROM products WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()

def update_balance(user_id, amount):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (amount, user_id))
    conn.commit()
    conn.close()


def get_balance(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    
    if result is None:
        return 0
    return result[0]

def add_purchase(user_id, product_id, price, phone, session_string, region, year, password=None):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO purchases (user_id, product_id, price, purchase_date, phone, session_string, region, account_year, password) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
              (user_id, product_id, price, now, phone, session_string, region, year, password))
    purchase_id = c.lastrowid
    conn.commit()
    conn.close()
    return purchase_id

def get_user_purchases(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE user_id = ? ORDER BY purchase_date DESC", (user_id,))
    purchases = c.fetchall()
    conn.close()
    return purchases

def get_purchase(purchase_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE id = ?", (purchase_id,))
    purchase = c.fetchone()
    conn.close()
    return purchase

def save_code(phone, code, message_text, received_date=None):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    # Если дата не передана, используем текущую
    if received_date is None:
        received_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute("INSERT INTO account_codes (phone, code, received_date, message_text) VALUES (?, ?, ?, ?)",
              (phone, code, received_date, message_text[:200]))
    conn.commit()
    conn.close()

def get_codes(phone, limit=20):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT code, received_date, message_text FROM account_codes WHERE phone = ? ORDER BY received_date DESC LIMIT ?", (phone, limit))
    codes = c.fetchall()
    conn.close()
    return codes

def add_pending_payment(user_id, amount, method, invoice_id=None):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("INSERT INTO pending_payments (user_id, amount, method, status, created_date, invoice_id) VALUES (?, ?, ?, ?, ?, ?)",
              (user_id, amount, method, 'pending', now, invoice_id))
    payment_id = c.lastrowid
    conn.commit()
    conn.close()
    return payment_id

def get_pending_payment(payment_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM pending_payments WHERE id = ?", (payment_id,))
    payment = c.fetchone()
    conn.close()
    return payment

def update_payment_status(payment_id, status):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("UPDATE pending_payments SET status = ? WHERE id = ?", (status, payment_id))
    conn.commit()
    conn.close()

# ==================== TELEGRAM AUTH ====================
async def detect_region(phone):
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

async def login_to_telegram(phone):
    try:
        phone = re.sub(r'[^\d+]', '', phone)
        if not phone.startswith('+'):
            phone = '+' + phone
        
        # Проверяем, есть ли уже активная сессия
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
        
        # Создаем новую сессию
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
        logging.error(f"Login error: {e}")
        return {'success': False, 'error': str(e)}

async def verify_code(phone, code):
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
    except Exception as e:
        return {'success': False, 'error': str(e)}

async def verify_password(phone, password):
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
        return {'success': False, 'error': str(e)}

# ==================== ПОЛУЧЕНИЕ КОДОВ ====================
async def monitor_account_codes(phone):
    """Мониторит аккаунт и сохраняет коды в реальном времени"""
    try:
        if phone not in temp_clients:
            if phone not in active_sessions:
                return
            client = TelegramClient(StringSession(active_sessions[phone]), API_ID, API_HASH)
            await client.connect()
            temp_clients[phone] = client
        else:
            client = temp_clients[phone]
        
        if not await client.is_user_authorized():
            return
        
        @client.on(events.NewMessage)
        async def handler(event):
            if event.message.text:
                text = event.message.text
                code_match = re.search(r'\b(\d{4,6})\b', text)
                if code_match:
                    code = code_match.group(1)
                    # Проверяем, что это похоже на код подтверждения
                    text_lower = text.lower()
                    if any(word in text_lower for word in ['code', 'код', 'login', 'вход', 'telegram']):
                        save_code(phone, code, text)
                        print(f"✅ Сохранен код {code} для {phone}")
        
        await client.run_until_disconnected()
    except Exception as e:
        print(f"❌ Ошибка мониторинга {phone}: {e}")

async def get_recent_codes(phone, limit=20):
    """Получает последние коды из базы или直接从 аккаунта"""
    # Сначала проверяем базу
    codes = get_codes(phone, limit)
    
    # Если в базе мало кодов, пробуем получить из аккаунта
    if len(codes) < 5 and phone in active_sessions:
        try:
            if phone not in temp_clients:
                client = TelegramClient(StringSession(active_sessions[phone]), API_ID, API_HASH)
                await client.connect()
                temp_clients[phone] = client
            else:
                client = temp_clients[phone]
            
            if await client.is_user_authorized():
                async for message in client.iter_messages(None, limit=50):
                    if message.text:
                        code_match = re.search(r'\b(\d{4,6})\b', message.text)
                        if code_match:
                            code = code_match.group(1)
                            text_lower = message.text.lower()
                            if any(word in text_lower for word in ['code', 'код', 'login', 'вход']):
                                # Сохраняем в базу
                                save_code(phone, code, message.text)
                
                # Обновляем список из базы
                codes = get_codes(phone, limit)
        except Exception as e:
            print(f"❌ Ошибка получения кодов: {e}")
    
    return codes

def get_codes(phone, limit=30):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    # Сортируем по реальной дате
    c.execute("SELECT code, received_date, message_text FROM account_codes WHERE phone = ? ORDER BY received_date DESC LIMIT ?", (phone, limit))
    codes = c.fetchall()
    conn.close()
    return codes
# ==================== КРИПТО ФУНКЦИИ ====================
async def fetch_usdt_rate():
    try:
        url = f"{CRYPTOBOT_API_URL}/getExchangeRates"
        headers = {'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for rate in data['result']:
                if rate['source'] == 'USDT' and rate['target'] == 'RUB':
                    return float(rate['rate'])
        return USDT_RATE
    except:
        return USDT_RATE

async def create_crypto_invoice(amount_rub):
    usdt_rate = await fetch_usdt_rate()
    amount_usdt = round(amount_rub / usdt_rate, 2)
    url = f"{CRYPTOBOT_API_URL}/createInvoice"
    headers = {'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN, 'Content-Type': 'application/json'}
    payload = {
        "asset": "USDT",
        "amount": str(amount_usdt),
        "description": f"Пополнение на {amount_rub} RUB",
        "paid_btn_name": "openBot",
        "paid_btn_url": f"https://t.me/{bot_username}",
        "payload": f"crypto_{amount_rub}"
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            data = response.json()
            if data['ok']:
                return data['result']
        return None
    except:
        return None

# ==================== КЛАВИАТУРЫ ====================
def main_keyboard(user_id):
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
        [InlineKeyboardButton(text="📢 РАССЫЛКА", callback_data="admin_mailing")],  # НОВАЯ КНОПКА
        [InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="admin_settings")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

@dp.callback_query(F.data == "admin_mailing")
async def admin_mailing_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📢 ВВЕДИ ТЕКСТ ДЛЯ РАССЫЛКИ:\n\n"
        "Можно использовать:\n"
        "• {name} - имя пользователя\n"
        "• {id} - ID пользователя\n"
        "• \\n - перенос строки"
    )
    await state.set_state(MailingStates.waiting_for_message)
    await callback.answer()

@dp.message(MailingStates.waiting_for_message)
async def admin_mailing_message(message: types.Message, state: FSMContext):
    await state.update_data(text=message.text)
    
    # Показываем предпросмотр
    users = get_all_users()
    preview_text = message.text.replace("{name}", message.from_user.first_name or "User")
    preview_text = preview_text.replace("{id}", str(message.from_user.id))
    
    await message.answer(
        f"📢 ПРЕДПРОСМОТР РАССЫЛКИ:\n\n{preview_text}\n\n"
        f"👥 ВСЕГО ПОЛЬЗОВАТЕЛЕЙ: {len(users)}\n\n"
        f"✅ ОТПРАВИТЬ?",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ ДА, ОТПРАВИТЬ", callback_data="mailing_send")],
            [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="admin_back")]
        ])
    )
    await state.set_state(MailingStates.waiting_for_confirm)

@dp.callback_query(F.data == "mailing_send")
async def admin_mailing_send(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    text = data.get('text')
    
    await callback.message.edit_text("🔄 НАЧИНАЮ РАССЫЛКУ...")
    
    users = get_all_users()
    success = 0
    failed = 0
    
    for user_id, username in users:
        try:
            # Заменяем переменные
            user_text = text.replace("{name}", username or "User")
            user_text = user_text.replace("{id}", str(user_id))
            
            await bot.send_message(user_id, user_text)
            success += 1
            await asyncio.sleep(0.05)  # Защита от флуда
        except Exception as e:
            failed += 1
            print(f"Ошибка отправки {user_id}: {e}")
    
    await callback.message.edit_text(
        f"✅ РАССЫЛКА ЗАВЕРШЕНА!\n\n"
        f"📊 РЕЗУЛЬТАТ:\n"
        f"✅ ОТПРАВЛЕНО: {success}\n"
        f"❌ ОШИБОК: {failed}\n"
        f"👥 ВСЕГО: {len(users)}"
    )
    await state.clear()

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
            product_id, name, price, phone, session, region, year, added = product[:8]
            age = datetime.now().year - year
            button_text = f"{name} | {region} | {age} ЛЕТ | {price} ₽"
            buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"view_{product_id}")])
    buttons.append([InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data="refresh_catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def product_keyboard(product_id):
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

def purchase_actions_keyboard(purchase_id):
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

def admin_payment_keyboard(payment_id):
    buttons = [
        [InlineKeyboardButton(text="✍️ РЕКВИЗИТЫ", callback_data=f"send_details_{payment_id}")],
        [InlineKeyboardButton(text="✅ ПОДТВЕРДИТЬ", callback_data=f"admin_confirm_{payment_id}"),
         InlineKeyboardButton(text="❌ ОТКЛОНИТЬ", callback_data=f"admin_reject_{payment_id}")]
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
        "📨 КОДЫ СОХРАНЯЮТСЯ АВТОМАТИЧЕСКИ\n\n"
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
        new_code = f"{message.from_user.id}{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}"
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
    await message.answer(text)

@dp.message(F.text == "📜 ПОКУПКИ")
async def my_purchases(message: types.Message):
    purchases = get_user_purchases(message.from_user.id)
    if not purchases:
        await message.answer("📭 У ТЕБЯ ПОКА НЕТ ПОКУПОК.")
        return
    
    text = "📜 ТВОИ КУПЛЕННЫЕ АККАУНТЫ:\n\n"
    await message.answer(text, reply_markup=purchases_keyboard(purchases))

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
    
    # В product теперь 9 полей (добавился password)
    if len(product) >= 9:
        product_id, name, price, phone, session, region, year, added, password = product[:9]
    else:
        product_id, name, price, phone, session, region, year, added = product[:8]
        password = None
    
    user_balance = get_balance(callback.from_user.id)
    
    if user_balance >= price:
        # Списываем баланс
        update_balance(callback.from_user.id, -price)
        
        # Добавляем покупку с паролем
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
        
        # Удаляем товар из каталога
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
            text += f"🔑 ПАРОЛЬ: {password}\n"
        
        text += f"\n📁 ФАЙЛ СЕССИИ ДОСТУПЕН В РАЗДЕЛЕ ПОКУПКИ"
        
        await callback.message.edit_text(text)
    else:
        need = price - user_balance
        text = f"❌ НЕДОСТАТОЧНО СРЕДСТВ\n\nНУЖНО ЕЩЕ: {need} ₽"
        await callback.message.edit_text(text, reply_markup=insufficient_balance_keyboard())
    
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
    
    # В purchase теперь 10 полей (добавился password)
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
        text += f"🔑 ПАРОЛЬ: {password}\n\n"
    else:
        text += f"🔑 ПАРОЛЬ: НЕ УСТАНОВЛЕН\n\n"
    
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
    
    # Показываем загрузку
    msg = await callback.message.edit_text("🔄 ПОДКЛЮЧАЮСЬ К TELEGRAM АККАУНТУ...")
    
    try:
        # Подключаемся к аккаунту
        client = TelegramClient(StringSession(session), API_ID, API_HASH)
        await client.connect()
        
        if not await client.is_user_authorized():
            await msg.edit_text("❌ НЕ УДАЛОСЬ ПОДКЛЮЧИТЬСЯ К АККАУНТУ")
            await callback.answer()
            return
        
        await msg.edit_text("🔍 ИЩУ КОДЫ В СООБЩЕНИЯХ...")
        
        # Ищем сообщения с кодами
        codes = []
        async for message in client.iter_messages(None, limit=200):
            if message.text:
                # Ищем цифры от 4 до 8 знаков
                code_matches = re.findall(r'\b(\d{4,8})\b', message.text)
                for code in code_matches:
                    # Проверяем, что это похоже на код (не номер телефона)
                    if len(code) >= 4 and len(code) <= 8:
                        # Определяем тип кода
                        text_lower = message.text.lower()
                        if any(word in text_lower for word in ['2fa', 'пароль', 'password']):
                            code_type = "🔒 2FA"
                        else:
                            code_type = "🔐 Telegram"
                        
                        # Дата сообщения
                        msg_date = message.date.strftime("%d.%m %H:%M")
                        
                        codes.append({
                            'code': code,
                            'type': code_type,
                            'date': msg_date,
                            'text': message.text[:50]
                        })
                        
                        if len(codes) >= 30:
                            break
            if len(codes) >= 30:
                break
        
        await client.disconnect()
        
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
    
    # Создаем файл сессии
    session_filename = f"session_{phone}.session"
    with open(session_filename, 'w', encoding='utf-8') as f:
        f.write(session)
    
    # Отправляем файл
    with open(session_filename, 'rb') as f:
        await callback.message.answer_document(
            types.FSInputFile(session_filename),
            caption=f"📁 ФАЙЛ СЕССИИ ДЛЯ {phone}"
        )
    
    # Удаляем временный файл
    os.remove(session_filename)
    
    await callback.answer()

# ==================== ПЛАТЕЖИ ====================
@dp.callback_query(F.data == "show_payment_methods")
async def show_payment_methods(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💰 ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:",
        reply_markup=payment_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "pay_stars")
async def pay_stars(callback: types.CallbackQuery, state: FSMContext):
    stars_rate = get_setting('stars_rate')
    await callback.message.edit_text(
        f"⭐ ПОПОЛНЕНИЕ ЧЕРЕЗ STARS\n\n"
        f"КУРС: 1 STAR = {stars_rate} ₽\n"
        f"ВВЕДИ СУММУ В РУБЛЯХ:"
    )
    await state.set_state(PaymentStates.waiting_for_stars_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_stars_amount)
async def stars_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        final_amount = amount
        
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            final_amount = amount * (1 - discount / 100)
            apply_first_discount(message.from_user.id)
        
        stars_rate = get_setting('stars_rate')
        stars_amount = int(final_amount / stars_rate)
        
        prices = [LabeledPrice(label="ПОПОЛНЕНИЕ БАЛАНСА", amount=stars_amount)]
        payload = f"stars_{message.from_user.id}_{int(datetime.now().timestamp())}"
        
        invoice_link = await bot.create_invoice_link(
            title="ПОПОЛНЕНИЕ БАЛАНСА STARS",
            description=f"{final_amount} ₽ ({stars_amount} ⭐)",
            payload=payload,
            currency="XTR",
            prices=prices
        )
        
        add_pending_payment(message.from_user.id, final_amount, "stars", payload)
        
        text = f"⭐ СЧЕТ СОЗДАН\n\n💰 СУММА: {final_amount} ₽\n⭐ STARS: {stars_amount}"
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 ОПЛАТИТЬ STARS", url=invoice_link)]
        ]))
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
            payment_id, user_id, amount = payment
            update_balance(user_id, amount)
            update_payment_status(payment_id, 'confirmed')
            
            user = get_user(user_id)
            if user and user[4]:
                reward_percent = get_setting('referral_reward')
                reward_amount = amount * (reward_percent / 100)
                update_balance(user[4], reward_amount)
            
            await message.answer(f"✅ ОПЛАТА ПОДТВЕРЖДЕНА! БАЛАНС ПОПОЛНЕН НА {amount} ₽.")
        else:
            await message.answer("❌ ПЛАТЕЖ НЕ НАЙДЕН. НАПИШИ В ПОДДЕРЖКУ.")

@dp.callback_query(F.data == "pay_sbp")
async def pay_sbp(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "💳 ПОПОЛНЕНИЕ ЧЕРЕЗ СБП\n\n"
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
        
        final_amount = amount
        
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            final_amount = amount * (1 - discount / 100)
            apply_first_discount(message.from_user.id)
        
        payment_id = add_pending_payment(message.from_user.id, final_amount, "sbp")
        
        for admin_id in ADMIN_IDS:
            await bot.send_message(
                admin_id,
                f"💰 ЗАПРОС НА ПОПОЛНЕНИЕ\n\n"
                f"👤 ПОЛЬЗОВАТЕЛЬ: @{message.from_user.username or 'НЕТ'} (ID: {message.from_user.id})\n"
                f"💵 СУММА: {amount} ₽\n"
                f"💳 К ОПЛАТЕ: {final_amount} ₽\n"
                f"🆔 ID ПЛАТЕЖА: {payment_id}",
                reply_markup=admin_payment_keyboard(payment_id)
            )
        
        await message.answer("✅ ЗАПРОС СОЗДАН. ОЖИДАЙ, АДМИНИСТРАТОР ОТПРАВИТ РЕКВИЗИТЫ.")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

@dp.callback_query(F.data == "pay_crypto")
async def pay_crypto(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "₿ ПОПОЛНЕНИЕ ЧЕРЕЗ CRYPTOBOT\n\n"
        "ВВЕДИ СУММУ В РУБЛЯХ:"
    )
    await state.set_state(PaymentStates.waiting_for_crypto_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_crypto_amount)
async def crypto_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        final_amount = amount
        
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            final_amount = amount * (1 - discount / 100)
            apply_first_discount(message.from_user.id)
        
        invoice = await create_crypto_invoice(final_amount)
        if not invoice:
            await message.answer("❌ ОШИБКА ПРИ СОЗДАНИИ СЧЕТА. ПОПРОБУЙ ПОЗЖЕ.")
            await state.clear()
            return
        
        payment_id = add_pending_payment(message.from_user.id, final_amount, "crypto", invoice['invoice_id'])
        
        text = f"₿ СЧЕТ СОЗДАН\n\n💰 СУММА: {final_amount} ₽\n💲 USDT: {invoice['amount']}"
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 ОПЛАТИТЬ В CRYPTOBOT", url=invoice['pay_url'])],
            [InlineKeyboardButton(text="✅ Я ОПЛАТИЛ", callback_data=f"check_crypto_{payment_id}")]
        ]))
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО")

# ==================== АДМИНСКИЕ ОБРАБОТЧИКИ ПЛАТЕЖЕЙ ====================
@dp.callback_query(lambda c: c.data.startswith('send_details_'))
async def send_payment_details(callback: types.CallbackQuery, state: FSMContext):
    payment_id = int(callback.data.split('_')[2])
    await state.update_data(payment_id=payment_id)
    await callback.message.edit_text("✍️ ВВЕДИ РЕКВИЗИТЫ ДЛЯ ОПЛАТЫ:")
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
                f"💳 РЕКВИЗИТЫ ДЛЯ ОПЛАТЫ\n\n"
                f"💰 СУММА: {payment[2]} ₽\n"
                f"📱 СПОСОБ: {payment[3].upper()}\n\n"
                f"РЕКВИЗИТЫ:\n{message.text}\n\n"
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
                f"💰 ПОЛЬЗОВАТЕЛЬ СООБЩИЛ ОБ ОПЛАТЕ\n\n"
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
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    
    if payment:
        update_balance(payment[1], payment[2])
        update_payment_status(payment_id, 'confirmed')
        
        user = get_user(payment[1])
        if user and user[4]:
            reward_percent = get_setting('referral_reward')
            reward_amount = payment[2] * (reward_percent / 100)
            update_balance(user[4], reward_amount)
        
        try:
            await bot.send_message(
                payment[1],
                f"✅ ПЛАТЕЖ ПОДТВЕРЖДЕН!\n\n"
                f"💰 СУММА: {payment[2]} ₽\n"
                f"💳 БАЛАНС ПОПОЛНЕН."
            )
        except:
            pass
        
        await callback.message.edit_text(f"✅ ПЛАТЕЖ #{payment_id} ПОДТВЕРЖДЕН.")
    
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
                f"❌ ПЛАТЕЖ ОТКЛОНЕН.\n\n"
                f"💰 СУММА: {payment[2]} ₽\n"
                f"📞 СВЯЖИСЬ С ПОДДЕРЖКОЙ."
            )
        except:
            pass
        
        await callback.message.edit_text(f"❌ ПЛАТЕЖ #{payment_id} ОТКЛОНЕН.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('check_crypto_'))
async def check_crypto_payment(callback: types.CallbackQuery):
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
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У ТЕБЯ НЕТ ДОСТУПА.")
        return
    
    await message.answer(
        "⚙️ АДМИН ПАНЕЛЬ\n\n"
        "ВЫБЕРИ ДЕЙСТВИЕ:",
        reply_markup=admin_keyboard()
    )

@dp.callback_query(F.data == "admin_add_product")
async def admin_add_product(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "➕ ДОБАВЛЕНИЕ ТОВАРА\n\n"
        "ВВЕДИ НАЗВАНИЕ ТОВАРА:"
    )
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
    
    # Спрашиваем про пароль
    await message.answer("🔐 ВВЕДИ ПАРОЛЬ ОТ АККАУНТА (ЕСЛИ ЕСТЬ, ИЛИ ОТПРАВЬ ПРОПУСТИТЬ):")
    await state.set_state(ProductStates.waiting_for_account_password)
    
    status_msg = await message.answer("🔄 ВЫПОЛНЯЮ ВХОД В АККАУНТ...")
    result = await login_to_telegram(phone)
    
    if result['success']:
        if result.get('already_logged'):
            data = await state.get_data()
            product_id = add_product(
                data['name'],
                data['price'],
                result['phone'],
                result['session'],
                result['region'],
                result['year']
            )
            await status_msg.edit_text(
                f"✅ АККАУНТ УСПЕШНО ДОБАВЛЕН!\n\n"
                f"📦 НАЗВАНИЕ: {data['name']}\n"
                f"💰 ЦЕНА: {data['price']} ₽\n"
                f"🌍 РЕГИОН: {result['region']}\n"
                f"📅 ГОД СОЗДАНИЯ: {result['year']}\n"
                f"🆔 ID ТОВАРА: {product_id}"
            )
            await state.clear()
        elif result.get('need_code'):
            await state.update_data(phone=result['phone'])
            await status_msg.edit_text(
                f"📱 КОД ПОДТВЕРЖДЕНИЯ ОТПРАВЛЕН НА НОМЕР {result['phone']}\n\n"
                f"ВВЕДИ КОД ИЗ TELEGRAM:"
            )
            await state.set_state(ProductStates.waiting_for_code)
        else:
            await status_msg.edit_text(f"❌ ОШИБКА: {result.get('error', 'НЕИЗВЕСТНАЯ ОШИБКА')}")
    else:
        await status_msg.edit_text(f"❌ ОШИБКА ВХОДА: {result.get('error', 'НЕИЗВЕСТНАЯ ОШИБКА')}")

@dp.message(ProductStates.waiting_for_code)
async def product_code_handler(message: types.Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data.get('phone')
    
    status_msg = await message.answer("🔄 ПРОВЕРЯЮ КОД...")
    result = await verify_code(phone, code)
    
    if result['success']:
        if result.get('need_password'):
            await state.update_data(phone=result['phone'])
            await status_msg.edit_text(
                "🔐 ТРЕБУЕТСЯ ПАРОЛЬ ДВУХФАКТОРНОЙ АУТЕНТИФИКАЦИИ\n\n"
                "ВВЕДИ ПАРОЛЬ:"
            )
            await state.set_state(ProductStates.waiting_for_password)
        else:
            data = await state.get_data()
            product_id = add_product(
                data['name'],
                data['price'],
                result['phone'],
                result['session'],
                result['region'],
                result['year']
            )
            await status_msg.edit_text(
                f"✅ АККАУНТ УСПЕШНО ДОБАВЛЕН!\n\n"
                f"📦 НАЗВАНИЕ: {data['name']}\n"
                f"💰 ЦЕНА: {data['price']} ₽\n"
                f"🌍 РЕГИОН: {result['region']}\n"
                f"📅 ГОД СОЗДАНИЯ: {result['year']}\n"
                f"🆔 ID ТОВАРА: {product_id}"
            )
            await state.clear()
    else:
        await status_msg.edit_text(f"❌ ОШИБКА: {result.get('error', 'НЕИЗВЕСТНАЯ ОШИБКА')}")

@dp.message(ProductStates.waiting_for_account_password)
async def product_account_password_handler(message: types.Message, state: FSMContext):
    password = message.text.strip()
    if password.lower() in ['пропустить', 'нет', '-', '']:
        await state.update_data(account_password=None)
    else:
        await state.update_data(account_password=password)
    
    data = await state.get_data()
    phone = data.get('phone')
    
    status_msg = await message.answer("🔄 ВЫПОЛНЯЮ ВХОД В АККАУНТ...")
    result = await login_to_telegram(phone)
    
    if result['success']:
        product_id = add_product(
            data['name'],
            data['price'],
            result['phone'],
            result['session'],
            result['region'],
            result['year']
        )
        await status_msg.edit_text(
            f"✅ АККАУНТ УСПЕШНО ДОБАВЛЕН!\n\n"
            f"📦 НАЗВАНИЕ: {data['name']}\n"
            f"💰 ЦЕНА: {data['price']} ₽\n"
            f"🌍 РЕГИОН: {result['region']}\n"
            f"📅 ГОД СОЗДАНИЯ: {result['year']}\n"
            f"🆔 ID ТОВАРА: {product_id}"
        )
        await state.clear()
    else:
        await status_msg.edit_text(f"❌ ОШИБКА: {result.get('error', 'НЕВЕРНЫЙ ПАРОЛЬ')}")

@dp.callback_query(F.data == "admin_delete_product")
async def admin_delete_product(callback: types.CallbackQuery):
    products = get_products()
    
    if not products:
        await callback.message.edit_text("📭 НЕТ ТОВАРОВ.")
        await callback.answer()
        return
    
    buttons = []
    for product in products:
        if len(product) >= 8:
            product_id, name, price, phone, session, region, year, added = product[:8]
            buttons.append([InlineKeyboardButton(
                text=f"{name} | {region} | {price} ₽",
                callback_data=f"del_{product_id}"
            )])
    
    buttons.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="admin_back")])
    
    await callback.message.edit_text(
        "🗑 ВЫБЕРИ ТОВАР ДЛЯ УДАЛЕНИЯ:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('del_'))
async def confirm_delete(callback: types.CallbackQuery):
    product_id = int(callback.data.split('_')[1])
    delete_product(product_id)
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
    for product in products:
        if len(product) >= 8:
            product_id, name, price, phone, session, region, year, added = product[:8]
            text += (
                f"🆔 ID: {product_id}\n"
                f"📦 НАЗВАНИЕ: {name}\n"
                f"💰 ЦЕНА: {price} ₽\n"
                f"📱 ТЕЛЕФОН: {phone}\n"
                f"🌍 РЕГИОН: {region}\n"
                f"📅 ГОД: {year}\n"
                f"{'─' * 30}\n"
            )
    
    if len(text) > 4000:
        for i in range(0, len(text), 4000):
            await callback.message.answer(text[i:i+4000])
    else:
        await callback.message.edit_text(text)
    
    await callback.answer()

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(callback: types.CallbackQuery):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    c.execute("SELECT COUNT(*) FROM users")
    users = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM users WHERE referrer_id IS NOT NULL")
    referred = c.fetchone()[0]
    
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
        f"👥 ПО РЕФЕРАЛАМ: {referred}\n"
        f"📦 ТОВАРОВ: {products}\n"
        f"🛒 ПРОДАЖ: {purchases}\n"
        f"💰 ВЫРУЧКА: {revenue} ₽"
    )
    
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "💰 ВВЕДИ ID ПОЛЬЗОВАТЕЛЯ, КОТОРОМУ ХОЧЕШЬ НАЧИСЛИТЬ БАЛАНС:"
    )
    await state.set_state(AdminAddBalanceStates.waiting_for_user_id)
    await callback.answer()

@dp.message(AdminAddBalanceStates.waiting_for_user_id)
async def admin_add_balance_user_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
        user = get_user(user_id)
        
        if not user:
            await message.answer("❌ ПОЛЬЗОВАТЕЛЬ С ТАКИМ ID НЕ НАЙДЕН.")
            return
        
        await state.update_data(target_user_id=user_id)
        await message.answer("💰 ВВЕДИ СУММУ ДЛЯ НАЧИСЛЕНИЯ:")
        await state.set_state(AdminAddBalanceStates.waiting_for_amount)
    except ValueError:
        await message.answer("❌ ВВЕДИ КОРРЕКТНЫЙ ЧИСЛОВОЙ ID.")

@dp.message(AdminAddBalanceStates.waiting_for_amount)
async def admin_add_balance_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.answer("❌ СУММА ДОЛЖНА БЫТЬ ПОЛОЖИТЕЛЬНОЙ.")
            return
        
        data = await state.get_data()
        user_id = data['target_user_id']
        
        update_balance(user_id, amount)
        
        await message.answer(f"✅ БАЛАНС ПОЛЬЗОВАТЕЛЯ {user_id} ПОПОЛНЕН НА {amount} ₽.")
        
        try:
            await bot.send_message(
                user_id,
                f"💰 АДМИНИСТРАТОР ПОПОЛНИЛ ТВОЙ БАЛАНС НА {amount} ₽."
            )
        except:
            pass
        
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ КОРРЕКТНУЮ СУММУ.")

@dp.callback_query(F.data == "admin_settings")
async def admin_settings(callback: types.CallbackQuery):
    stars_rate = get_setting('stars_rate')
    usdt_rate = get_setting('usdt_rate')
    discount = get_setting('referral_discount')
    reward = get_setting('referral_reward')
    
    text = (
        f"⚙️ ТЕКУЩИЕ НАСТРОЙКИ:\n\n"
        f"⭐ КУРС STARS: 1 STAR = {stars_rate} ₽\n"
        f"💵 КУРС USDT: 1 USDT = {usdt_rate} ₽\n"
        f"🎁 СКИДКА РЕФЕРАЛАМ: {discount}%\n"
        f"💸 НАГРАДА ЗА РЕФЕРАЛА: {reward}%\n\n"
        f"ВЫБЕРИ ЧТО ИЗМЕНИТЬ:"
    )
    
    await callback.message.edit_text(text, reply_markup=admin_settings_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_stars")
async def set_stars_rate(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"⭐ ВВЕДИ НОВЫЙ КУРС STARS:\nТЕКУЩИЙ: {get_setting('stars_rate')} ₽"
    )
    await state.set_state(AdminSettingsStates.waiting_for_stars)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_stars)
async def stars_rate_handler(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text)
        if rate <= 0:
            await message.answer("❌ ВВЕДИ ПОЛОЖИТЕЛЬНОЕ ЧИСЛО:")
            return
        update_setting('stars_rate', rate)
        await message.answer(f"✅ КУРС ОБНОВЛЕН: 1 STAR = {rate} ₽")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО.")

@dp.callback_query(F.data == "set_usdt")
async def set_usdt_rate(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"💵 ВВЕДИ НОВЫЙ КУРС USDT:\nТЕКУЩИЙ: {get_setting('usdt_rate')} ₽"
    )
    await state.set_state(AdminSettingsStates.waiting_for_usdt)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_usdt)
async def usdt_rate_handler(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text)
        if rate <= 0:
            await message.answer("❌ ВВЕДИ ПОЛОЖИТЕЛЬНОЕ ЧИСЛО:")
            return
        update_setting('usdt_rate', rate)
        await message.answer(f"✅ КУРС ОБНОВЛЕН: 1 USDT = {rate} ₽")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО.")

@dp.callback_query(F.data == "set_discount")
async def set_discount(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"🎁 ВВЕДИ НОВЫЙ ПРОЦЕНТ СКИДКИ:\nТЕКУЩИЙ: {get_setting('referral_discount')}%"
    )
    await state.set_state(AdminSettingsStates.waiting_for_discount)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_discount)
async def discount_percent_handler(message: types.Message, state: FSMContext):
    try:
        percent = float(message.text)
        if percent < 0 or percent > 100:
            await message.answer("❌ ПРОЦЕНТ ОТ 0 ДО 100:")
            return
        update_setting('referral_discount', percent)
        await message.answer(f"✅ СКИДКА ОБНОВЛЕНА: {percent}%")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО.")

@dp.callback_query(F.data == "set_reward")
async def set_reward(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"💸 ВВЕДИ НОВЫЙ ПРОЦЕНТ НАГРАДЫ:\nТЕКУЩИЙ: {get_setting('referral_reward')}%"
    )
    await state.set_state(AdminSettingsStates.waiting_for_reward)
    await callback.answer()

@dp.message(AdminSettingsStates.waiting_for_reward)
async def reward_percent_handler(message: types.Message, state: FSMContext):
    try:
        percent = float(message.text)
        if percent < 0 or percent > 100:
            await message.answer("❌ ПРОЦЕНТ ОТ 0 ДО 100:")
            return
        update_setting('referral_reward', percent)
        await message.answer(f"✅ НАГРАДА ОБНОВЛЕНА: {percent}%")
        await state.clear()
    except ValueError:
        await message.answer("❌ ВВЕДИ ЧИСЛО.")

# ==================== НАВИГАЦИЯ ====================
@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "⚙️ АДМИН ПАНЕЛЬ\n\nВЫБЕРИ ДЕЙСТВИЕ:",
        reply_markup=admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 КАТАЛОГ ПУСТ.")
        await callback.answer()
        return
    await callback.message.edit_text("📦 ВЫБЕРИ ТОВАР:", reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(F.data == "back_to_balance")
async def back_to_balance(callback: types.CallbackQuery):
    user_balance = get_balance(callback.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 ТВОЙ БАЛАНС: {user_balance} ₽\n"
        f"⭐ ЭКВИВАЛЕНТ: {int(user_balance / stars_rate)} ⭐\n\n"
        f"ВЫБЕРИ СПОСОБ ПОПОЛНЕНИЯ:"
    )
    await callback.message.edit_text(text, reply_markup=payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_purchases")
async def back_to_purchases(callback: types.CallbackQuery):
    purchases = get_user_purchases(callback.from_user.id)
    if not purchases:
        await callback.message.edit_text("📭 У ТЕБЯ НЕТ ПОКУПОК.")
        await callback.answer()
        return
    await callback.message.edit_text("📜 ТВОИ ПОКУПКИ:", reply_markup=purchases_keyboard(purchases))
    await callback.answer()

# ==================== ЗАПУСК ====================
async def main():
    global bot_username
    
    # Инициализация БД
    init_db()
    
    # Получаем username бота
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    
    print(f"🚀 БОТ @{bot_username} ЗАПУЩЕН!")
    print("✅ ВСЕ СИСТЕМЫ РАБОТАЮТ")
    print("📨 КОДЫ БУДУТ СОХРАНЯТЬСЯ АВТОМАТИЧЕСКИ")
    
    # Запускаем поллинг
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())