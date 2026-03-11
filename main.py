import asyncio
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
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
import random
import string
import re
import os
import requests

# -------------------- НАСТРОЙКИ --------------------
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

# -------------------- БАЗА ДАННЫХ --------------------
def init_db():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    # Таблица пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY,
                  username TEXT,
                  balance REAL DEFAULT 0,
                  registered_date TEXT,
                  referrer_id INTEGER DEFAULT NULL,
                  referral_code TEXT UNIQUE,
                  first_discount_used INTEGER DEFAULT 0,
                  total_referrals INTEGER DEFAULT 0,
                  total_referral_earnings REAL DEFAULT 0)''')
    
    # Таблица товаров
    c.execute('''CREATE TABLE IF NOT EXISTS products
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  name TEXT,
                  price REAL,
                  phone TEXT,
                  session_string TEXT,
                  region TEXT,
                  account_year INTEGER,
                  added_date TEXT)''')
    
    # Таблица покупок
    c.execute('''CREATE TABLE IF NOT EXISTS purchases
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  product_id INTEGER,
                  price REAL,
                  purchase_date TEXT,
                  phone TEXT,
                  session_string TEXT,
                  region TEXT,
                  account_year INTEGER)''')
    
    # Таблица ожидающих платежей (для СБП и Crypto)
    c.execute('''CREATE TABLE IF NOT EXISTS pending_payments
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  amount REAL,
                  method TEXT,
                  status TEXT DEFAULT 'pending',
                  created_date TEXT,
                  invoice_id TEXT)''')
    
    # Таблица реферальных выплат
    c.execute('''CREATE TABLE IF NOT EXISTS referral_payments
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  user_id INTEGER,
                  amount REAL,
                  from_user_id INTEGER,
                  payment_date TEXT,
                  status TEXT DEFAULT 'pending')''')
    
    # Таблица настроек
    c.execute('''CREATE TABLE IF NOT EXISTS settings
                 (key TEXT PRIMARY KEY,
                  value TEXT)''')
    
    # Добавляем настройки по умолчанию
    default_settings = [
        ('stars_rate', str(STARS_RATE)),
        ('usdt_rate', str(USDT_RATE)),
        ('referral_discount', '10'),
        ('referral_reward', '5'),
        ('reviews_channel_link', '')
    ]
    
    for key, value in default_settings:
        c.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
    
    conn.commit()
    conn.close()

def upgrade_db():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    
    # Проверяем и добавляем колонки в products
    c.execute("PRAGMA table_info(products)")
    columns = [col[1] for col in c.fetchall()]
    required_products = ['phone', 'session_string', 'region', 'account_year', 'added_date']
    for col in required_products:
        if col not in columns:
            try:
                c.execute(f"ALTER TABLE products ADD COLUMN {col} TEXT")
                print(f"Добавлена колонка {col} в products")
            except:
                pass
    
    # Проверяем и добавляем колонки в purchases
    c.execute("PRAGMA table_info(purchases)")
    columns = [col[1] for col in c.fetchall()]
    required_purchases = ['phone', 'session_string', 'region', 'account_year']
    for col in required_purchases:
        if col not in columns:
            try:
                c.execute(f"ALTER TABLE purchases ADD COLUMN {col} TEXT")
                print(f"Добавлена колонка {col} в purchases")
            except:
                pass
    
    # Проверяем и добавляем колонку invoice_id в pending_payments
    c.execute("PRAGMA table_info(pending_payments)")
    columns = [col[1] for col in c.fetchall()]
    if 'invoice_id' not in columns:
        try:
            c.execute("ALTER TABLE pending_payments ADD COLUMN invoice_id TEXT")
            print("Добавлена колонка invoice_id в pending_payments")
        except:
            pass
    
    conn.commit()
    conn.close()

init_db()
upgrade_db()

# -------------------- СОСТОЯНИЯ FSM --------------------
class ProductStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_price = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()

class PaymentStates(StatesGroup):
    waiting_for_sbp_amount = State()
    waiting_for_stars_amount = State()
    waiting_for_crypto_amount = State()

class SupportStates(StatesGroup):
    waiting_for_message = State()

class ReferralStates(StatesGroup):
    waiting_for_discount_percent = State()
    waiting_for_reward_percent = State()
    waiting_for_stars_rate = State()
    waiting_for_usdt_rate = State()
    waiting_for_reviews_channel = State()

class AdminPaymentStates(StatesGroup):
    waiting_for_payment_details = State()

class AdminAddBalanceStates(StatesGroup):
    waiting_for_user_id = State()
    waiting_for_amount = State()

temp_clients = {}

# -------------------- TELEGRAM AUTH --------------------
async def login_to_telegram(phone):
    try:
        phone = re.sub(r'[^\d+]', '', phone)
        if not phone.startswith('+'):
            phone = '+' + phone
        
        session_file = f'sessions/{phone}.session'
        client = TelegramClient(session_file, API_ID, API_HASH)
        await client.connect()
        
        if await client.is_user_authorized():
            me = await client.get_me()
            session_string = client.session.save()
            region = await detect_region(phone)
            year = getattr(me, 'date', None)
            if year and hasattr(year, 'year'):
                year = year.year
            else:
                year = datetime.now().year
            await client.disconnect()
            return {
                'success': True,
                'session': session_string,
                'region': region,
                'year': year,
                'already_logged': True,
                'phone': phone
            }
        else:
            await client.send_code_request(phone)
            temp_clients[phone] = client
            return {'success': True, 'need_code': True, 'phone': phone}
    except Exception as e:
        logging.error(f"Login error: {e}")
        return {'success': False, 'error': str(e)}

async def verify_code(phone, code):
    try:
        client = temp_clients.get(phone)
        if not client:
            return {'success': False, 'error': 'Сессия истекла. Начните заново.'}
        await client.sign_in(code=code)
        me = await client.get_me()
        session_string = client.session.save()
        region = await detect_region(phone)
        year = getattr(me, 'date', None)
        if year and hasattr(year, 'year'):
            year = year.year
        else:
            year = datetime.now().year
        del temp_clients[phone]
        await client.disconnect()
        return {
            'success': True,
            'session': session_string,
            'region': region,
            'year': year,
            'phone': phone
        }
    except SessionPasswordNeededError:
        return {'success': True, 'need_password': True, 'phone': phone}
    except PhoneCodeInvalidError:
        return {'success': False, 'error': 'Неверный код подтверждения'}
    except Exception as e:
        logging.error(f"Code verification error: {e}")
        return {'success': False, 'error': str(e)}

async def verify_password(phone, password):
    try:
        client = temp_clients.get(phone)
        if not client:
            return {'success': False, 'error': 'Сессия истекла. Начните заново.'}
        await client.sign_in(password=password)
        me = await client.get_me()
        session_string = client.session.save()
        region = await detect_region(phone)
        year = getattr(me, 'date', None)
        if year and hasattr(year, 'year'):
            year = year.year
        else:
            year = datetime.now().year
        del temp_clients[phone]
        await client.disconnect()
        return {
            'success': True,
            'session': session_string,
            'region': region,
            'year': year,
            'phone': phone
        }
    except Exception as e:
        logging.error(f"Password verification error: {e}")
        return {'success': False, 'error': str(e)}

async def detect_region(phone):
    try:
        if phone.startswith('+7') or phone.startswith('7'):
            return 'Россия'
        elif phone.startswith('+380') or phone.startswith('380'):
            return 'Украина'
        elif phone.startswith('+1'):
            return 'США/Канада'
        elif phone.startswith('+44'):
            return 'Великобритания'
        elif phone.startswith('+49'):
            return 'Германия'
        elif phone.startswith('+33'):
            return 'Франция'
        elif phone.startswith('+39'):
            return 'Италия'
        elif phone.startswith('+34'):
            return 'Испания'
        elif phone.startswith('+86'):
            return 'Китай'
        elif phone.startswith('+81'):
            return 'Япония'
        elif phone.startswith('+82'):
            return 'Южная Корея'
        elif phone.startswith('+91'):
            return 'Индия'
        elif phone.startswith('+55'):
            return 'Бразилия'
        elif phone.startswith('+52'):
            return 'Мексика'
        elif phone.startswith('+61'):
            return 'Австралия'
        else:
            return 'Другая страна'
    except:
        return 'Неизвестно'

# -------------------- ПОЛУЧЕНИЕ КУРСА USDT --------------------
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

# -------------------- CRYPTOBOT INVOICE --------------------
async def create_crypto_invoice(amount_rub):
    """Создаёт счёт в CryptoBot и возвращает данные для оплаты"""
    usdt_rate = await fetch_usdt_rate()
    amount_usdt = round(amount_rub / usdt_rate, 2)
    url = f"{CRYPTOBOT_API_URL}/createInvoice"
    headers = {'Crypto-Pay-API-Token': CRYPTOBOT_TOKEN, 'Content-Type': 'application/json'}
    payload = {
        "asset": "USDT",
        "amount": str(amount_usdt),
        "description": f"Пополнение баланса на {amount_rub} RUB",
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

# -------------------- ФУНКЦИИ БД --------------------
def get_user(user_id, username=None, referrer_id=None):
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

def generate_referral_code(user_id):
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{user_id}{random_part}"

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
    # Если это числовая настройка, возвращаем float
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

def add_referral_earning(user_id, amount, from_user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("UPDATE users SET balance = balance + ?, total_referral_earnings = total_referral_earnings + ? WHERE user_id = ?", 
              (amount, amount, user_id))
    c.execute("""INSERT INTO referral_payments (user_id, amount, from_user_id, payment_date, status) 
                 VALUES (?, ?, ?, ?, 'completed')""",
              (user_id, amount, from_user_id, now))
    conn.commit()
    conn.close()

def get_referral_stats(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT username, registered_date, total_referral_earnings FROM users WHERE referrer_id = ?", (user_id,))
    referrals = c.fetchall()
    c.execute("SELECT total_referrals, total_referral_earnings FROM users WHERE user_id = ?", (user_id,))
    stats = c.fetchone()
    conn.close()
    return {
        'referrals': referrals,
        'total_count': stats[0] if stats else 0,
        'total_earnings': stats[1] if stats else 0
    }

def get_products():
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products ORDER BY id DESC")
    products = c.fetchall()
    conn.close()
    return products

def add_product(name, price, phone, session_string, region, year):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    year = int(year) if year else datetime.now().year
    c.execute("""INSERT INTO products (name, price, phone, session_string, region, account_year, added_date) 
                 VALUES (?, ?, ?, ?, ?, ?, ?)""",
              (name, price, phone, session_string, region, year, now))
    conn.commit()
    product_id = c.lastrowid
    conn.close()
    return product_id

def delete_product(product_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("DELETE FROM products WHERE id = ?", (product_id,))
    conn.commit()
    conn.close()

def get_product(product_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    product = c.fetchone()
    conn.close()
    return product

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
    balance = c.fetchone()
    conn.close()
    return balance[0] if balance else 0

def add_purchase(user_id, product_id, price, phone, session_string, region, year):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO purchases 
                 (user_id, product_id, price, purchase_date, phone, session_string, region, account_year) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
              (user_id, product_id, price, now, phone, session_string, region, year))
    conn.commit()
    conn.close()

def get_user_purchases(user_id):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE user_id = ? ORDER BY purchase_date DESC", (user_id,))
    purchases = c.fetchall()
    conn.close()
    return purchases

def add_pending_payment(user_id, amount, method, invoice_id=None):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""INSERT INTO pending_payments (user_id, amount, method, status, created_date, invoice_id) 
                 VALUES (?, ?, ?, ?, ?, ?)""",
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

def get_pending_payments_by_status(status='pending'):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM pending_payments WHERE status = ?", (status,))
    payments = c.fetchall()
    conn.close()
    return payments

# -------------------- КЛАВИАТУРЫ --------------------
def main_keyboard(user_id):
    buttons = [
        [KeyboardButton(text="🛍 Каталог")],
        [KeyboardButton(text="💰 Баланс"), KeyboardButton(text="📱 Профиль")],
        [KeyboardButton(text="👥 Рефералы"), KeyboardButton(text="📜 Мои покупки")],
        [KeyboardButton(text="📝 Отзывы"), KeyboardButton(text="📞 Поддержка")]
    ]
    if user_id in ADMIN_IDS:
        buttons.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def admin_keyboard():
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить товар", callback_data="admin_add_product")],
        [InlineKeyboardButton(text="🗑 Удалить товар", callback_data="admin_delete_product")],
        [InlineKeyboardButton(text="📦 Список товаров", callback_data="admin_list_products")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats")],
        [InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users")],
        [InlineKeyboardButton(text="💳 Ожидающие платежи", callback_data="admin_pending_payments")],
        [InlineKeyboardButton(text="💰 Начислить баланс", callback_data="admin_add_balance")],
        [InlineKeyboardButton(text="⚙️ Настройки", callback_data="admin_settings")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_settings_keyboard():
    buttons = [
        [InlineKeyboardButton(text="💰 Курс Stars", callback_data="set_stars_rate")],
        [InlineKeyboardButton(text="💵 Курс USDT", callback_data="set_usdt_rate")],
        [InlineKeyboardButton(text="🎁 Скидка рефералам", callback_data="set_discount")],
        [InlineKeyboardButton(text="💸 Награда за реферала", callback_data="set_reward")],
        [InlineKeyboardButton(text="📢 Канал отзывов", callback_data="set_reviews_channel")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def payment_keyboard():
    buttons = [
        [InlineKeyboardButton(text="⭐ Telegram Stars", callback_data="pay_stars")],
        [InlineKeyboardButton(text="💳 СБП", callback_data="pay_sbp")],
        [InlineKeyboardButton(text="₿ CryptoBot", callback_data="pay_crypto")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_balance")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def catalog_keyboard(products):
    buttons = []
    for product in products:
        if len(product) >= 8:
            product_id, name, price, phone, session, region, year, added = product[:8]
        else:
            continue
        current_year = datetime.now().year
        age = current_year - year
        button_text = f"{name} | {region} | {age} лет | {price} RUB"
        buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"view_{product_id}")])
    buttons.append([InlineKeyboardButton(text="🔄 Обновить", callback_data="refresh_catalog")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def product_detail_keyboard(product_id):
    buttons = [
        [InlineKeyboardButton(text="💳 Купить", callback_data=f"buy_{product_id}")],
        [InlineKeyboardButton(text="🔙 Назад к каталогу", callback_data="back_to_catalog")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def support_keyboard():
    buttons = [
        [InlineKeyboardButton(text="✍️ Написать сообщение", callback_data="support_write")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def referral_keyboard():
    buttons = [
        [InlineKeyboardButton(text="🔗 Моя реферальная ссылка", callback_data="show_ref_link")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="ref_stats")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def insufficient_balance_keyboard():
    buttons = [[InlineKeyboardButton(text="💰 Пополнить баланс", callback_data="show_payment_methods")]]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_payment_keyboard(payment_id):
    buttons = [
        [InlineKeyboardButton(text="✍️ Написать реквизиты", callback_data=f"send_payment_details_{payment_id}")],
        [InlineKeyboardButton(text="✅ Подтвердить оплату", callback_data=f"admin_confirm_payment_{payment_id}")],
        [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment_{payment_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def purchase_details_keyboard(purchase_id):
    buttons = [
        [InlineKeyboardButton(text="🔑 Показать данные для входа", callback_data=f"show_purchase_{purchase_id}")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# -------------------- ОБРАБОТЧИКИ --------------------
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
        "👋 Добро пожаловать в магазин Telegram аккаунтов!\n\n"
        "Здесь вы можете купить качественные аккаунты Telegram.\n"
        "Используйте кнопки ниже для навигации."
    )
    if referrer_id:
        welcome_text += "\n\n🎉 Вы пришли по реферальной ссылке! Вам доступна скидка 10% на первое пополнение."
    await message.answer(welcome_text, reply_markup=main_keyboard(message.from_user.id))

@dp.message(F.text == "🛍 Каталог")
async def catalog(message: types.Message):
    products = get_products()
    if not products:
        await message.answer("📭 Каталог пуст. Товары появятся позже.")
        return
    text = "📦 Каталог товаров:\n\nВыберите товар для просмотра деталей:"
    await message.answer(text, reply_markup=catalog_keyboard(products))

@dp.callback_query(F.data == "refresh_catalog")
async def refresh_catalog(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 Каталог пуст.")
        return
    text = "📦 Каталог товаров:\n\nВыберите товар для просмотра деталей:"
    await callback.message.edit_text(text, reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('view_'))
async def view_product(callback: types.CallbackQuery):
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)
    if not product:
        await callback.message.edit_text("❌ Товар не найден.")
        await callback.answer()
        return
    product_id, name, price, phone, session, region, year, added = product[:8]
    current_year = datetime.now().year
    age = current_year - year
    text = (
        f"📦 {name}\n\n"
        f"🌍 Регион: {region}\n"
        f"📅 Год создания: {year} ({age} лет)\n"
        f"💰 Цена: {price} RUB / {int(price / get_setting('stars_rate'))} ⭐\n"
        f"🕐 Добавлен: {added}\n\n"
        f"Номер телефона будет доступен после покупки.\n"
        f"Нажмите кнопку ниже для покупки:"
    )
    await callback.message.edit_text(text, reply_markup=product_detail_keyboard(product_id))
    await callback.answer()

@dp.message(F.text == "👥 Рефералы")
async def referral_system(message: types.Message):
    stats = get_referral_stats(message.from_user.id)
    user = get_user(message.from_user.id)
    text = (
        f"👥 Реферальная система\n\n"
        f"💰 Награда: {get_setting('referral_reward')}% от пополнений рефералов\n"
        f"🎁 Скидка для рефералов: {get_setting('referral_discount')}% на первое пополнение\n\n"
        f"📊 Ваша статистика:\n"
        f"👤 Приглашено: {stats['total_count']} чел.\n"
        f"💰 Заработано: {stats['total_earnings']} RUB\n"
    )
    await message.answer(text, reply_markup=referral_keyboard())

@dp.callback_query(F.data == "show_ref_link")
async def show_ref_link(callback: types.CallbackQuery):
    user = get_user(callback.from_user.id)
    if not user:
        await callback.message.edit_text("❌ Ошибка: пользователь не найден.")
        await callback.answer()
        return

    if not user[4]:
        new_code = generate_referral_code(callback.from_user.id)
        conn = sqlite3.connect('shop.db')
        c = conn.cursor()
        c.execute("UPDATE users SET referral_code = ? WHERE user_id = ?", (new_code, callback.from_user.id))
        conn.commit()
        conn.close()
        user = get_user(callback.from_user.id)

    referral_link = f"https://t.me/{bot_username}?start=ref_{user[4]}"
    text = (
        f"🔗 Ваша реферальная ссылка:\n\n"
        f"{referral_link}\n\n"
        f"📤 Отправьте эту ссылку друзьям.\n"
        f"Когда они зарегистрируются и пополнят баланс, вы получите {get_setting('referral_reward')}% от суммы!"
    )
    await callback.message.edit_text(text, reply_markup=referral_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "ref_stats")
async def ref_stats(callback: types.CallbackQuery):
    stats = get_referral_stats(callback.from_user.id)
    text = f"📊 Детальная статистика рефералов\n\n"
    text += f"👥 Всего приглашено: {stats['total_count']}\n"
    text += f"💰 Всего заработано: {stats['total_earnings']} RUB\n\n"
    if stats['referrals']:
        text += "Список рефералов:\n"
        for ref in stats['referrals']:
            username = ref[0] if ref[0] else "Без username"
            date = ref[1].split()[0] if ref[1] else "Неизвестно"
            earnings = ref[2] if ref[2] else 0
            text += f"👤 @{username} | 📅 {date} | 💰 {earnings} RUB\n"
    else:
        text += "📭 У вас пока нет рефералов. Приглашайте друзей!"
    await callback.message.edit_text(text, reply_markup=referral_keyboard())
    await callback.answer()

@dp.message(F.text == "💰 Баланс")
async def balance(message: types.Message):
    user_balance = get_balance(message.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 Ваш баланс: {user_balance} RUB\n"
        f"⭐ Эквивалент в Stars: {int(user_balance / stars_rate)} ⭐\n\n"
        f"Выберите способ пополнения:"
    )
    await message.answer(text, reply_markup=payment_keyboard())

@dp.message(F.text == "📱 Профиль")
async def profile(message: types.Message):
    user = get_user(message.from_user.id)
    purchases = get_user_purchases(message.from_user.id)
    stats = get_referral_stats(message.from_user.id)
    discount_status = "✅ Доступна" if can_use_discount(message.from_user.id) else "❌ Не доступна"
    text = (
        f"📱 Ваш профиль\n\n"
        f"🆔 ID: {message.from_user.id}\n"
        f"👤 Username: @{message.from_user.username if message.from_user.username else 'отсутствует'}\n"
        f"💰 Баланс: {user[2]} RUB\n"
        f"📦 Всего покупок: {len(purchases)}\n"
        f"👥 Приглашено друзей: {stats['total_count']}\n"
        f"💸 Заработано на рефералах: {stats['total_earnings']} RUB\n"
        f"🎁 Скидка на первое пополнение: {discount_status}\n"
        f"📅 Зарегистрирован: {user[3]}"
    )
    await message.answer(text)

@dp.message(F.text == "📜 Мои покупки")
async def my_purchases(message: types.Message):
    purchases = get_user_purchases(message.from_user.id)
    if not purchases:
        await message.answer("📭 У вас пока нет покупок.")
        return
    for purchase in purchases:
        if len(purchase) >= 9:
            pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
        else:
            continue
        # Показываем только общую информацию, данные скрыты
        text = (
            f"📜 Покупка #{pid}\n\n"
            f"💰 Цена: {price} RUB\n"
            f"🌍 Регион: {region}\n"
            f"📅 Год аккаунта: {year}\n"
            f"📅 Дата покупки: {date}\n\n"
            f"Данные для входа скрыты. Нажмите кнопку ниже, чтобы показать."
        )
        await message.answer(text, reply_markup=purchase_details_keyboard(pid))

@dp.callback_query(lambda c: c.data.startswith('show_purchase_'))
async def show_purchase_details(callback: types.CallbackQuery):
    purchase_id = int(callback.data.split('_')[2])
    # Ищем покупку в БД
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT * FROM purchases WHERE id = ? AND user_id = ?", (purchase_id, callback.from_user.id))
    purchase = c.fetchone()
    conn.close()
    if not purchase:
        await callback.message.edit_text("❌ Покупка не найдена.")
        await callback.answer()
        return
    if len(purchase) >= 9:
        pid, user_id, product_id, price, date, phone, session, region, year = purchase[:9]
    else:
        await callback.message.edit_text("❌ Ошибка данных.")
        await callback.answer()
        return
    text = (
        f"🔑 Данные для входа (покупка #{pid}):\n\n"
        f"📱 Телефон: {phone}\n"
        f"🔐 Сессия: {session}\n\n"
        f"Сохраните эти данные в безопасном месте."
    )
    await callback.message.edit_text(text)
    await callback.answer()

@dp.message(F.text == "📝 Отзывы")
async def reviews_link(message: types.Message):
    channel_link = get_setting('reviews_channel_link')
    if channel_link:
        await message.answer(f"📢 Наш канал с отзывами:\n{channel_link}")
    else:
        await message.answer("📢 Канал с отзывами ещё не настроен.")

@dp.message(F.text == "📞 Поддержка")
async def support(message: types.Message):
    text = (
        "📞 Служба поддержки\n\n"
        "Если у вас возникли вопросы, напишите сообщение администратору."
    )
    await message.answer(text, reply_markup=support_keyboard())

@dp.callback_query(F.data == "support_write")
async def support_write(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "✍️ Напишите ваше сообщение для администратора."
    )
    await state.set_state(SupportStates.waiting_for_message)
    await callback.answer()

@dp.message(SupportStates.waiting_for_message)
async def support_message_handler(message: types.Message, state: FSMContext):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"📩 Новое сообщение в поддержку\n\n"
                f"👤 От: @{message.from_user.username or 'Нет username'} (ID: {message.from_user.id})\n"
                f"💬 Сообщение: {message.text}",
            )
        except:
            pass
    await message.answer("✅ Ваше сообщение отправлено администратору.")
    await state.clear()

# -------------------- ПЛАТЕЖИ --------------------
@dp.callback_query(F.data == "show_payment_methods")
async def show_payment_methods(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "💰 Выберите способ пополнения:",
        reply_markup=payment_keyboard()
    )
    await callback.answer()

# ---- Stars (Telegram Payments) ----
@dp.callback_query(F.data == "pay_stars")
async def pay_stars(callback: types.CallbackQuery, state: FSMContext):
    stars_rate = get_setting('stars_rate')
    await callback.message.edit_text(
        f"⭐ Пополнение через Stars\n\n"
        f"Курс: 1 Star = {stars_rate} RUB\n"
        f"Введите сумму в рублях:"
    )
    await state.set_state(PaymentStates.waiting_for_stars_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_stars_amount)
async def stars_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        final_amount = amount
        discount_amount = 0
        used_discount = False
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            discount_amount = amount * (discount / 100)
            final_amount = amount - discount_amount
            apply_first_discount(message.from_user.id)
            used_discount = True
        stars_rate = get_setting('stars_rate')
        stars_amount = int(final_amount / stars_rate)
        
        prices = [LabeledPrice(label="Пополнение баланса", amount=stars_amount)]
        payload = f"stars_{message.from_user.id}_{int(datetime.now().timestamp())}"
        invoice_link = await bot.create_invoice_link(
            title="Пополнение баланса Stars",
            description=f"Пополнение на {final_amount} RUB ({stars_amount} Stars)",
            payload=payload,
            currency="XTR",
            prices=prices
        )
        
        payment_id = add_pending_payment(message.from_user.id, final_amount, "stars", payload)
        
        discount_message = f"\n🎁 Скидка {discount}% применена! Сумма: {final_amount} RUB" if used_discount else ""
        
        await message.answer(
            f"⭐ Счет создан{discount_message}\n\n"
            f"💵 Сумма: {final_amount} RUB\n"
            f"⭐ Stars: {stars_amount} ⭐\n\n"
            f"Нажмите кнопку ниже для оплаты:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить Stars", url=invoice_link)]
            ])
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

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
                add_referral_earning(user[4], reward_amount, user_id)
            await message.answer(f"✅ Оплата подтверждена! Баланс пополнен на {amount} RUB.")
        else:
            await message.answer("❌ Платёж не найден, но деньги списаны. Обратитесь в поддержку.")

# ---- СБП ----
@dp.callback_query(F.data == "pay_sbp")
async def pay_sbp(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "💳 Пополнение через СБП\n\n"
        "Введите сумму пополнения в рублях (минимум 100 RUB):"
    )
    await state.set_state(PaymentStates.waiting_for_sbp_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_sbp_amount)
async def sbp_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        if amount < 100:
            await message.answer("❌ Минимальная сумма - 100 RUB. Введите другую сумму:")
            return
        final_amount = amount
        discount_amount = 0
        used_discount = False
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            discount_amount = amount * (discount / 100)
            final_amount = amount - discount_amount
            apply_first_discount(message.from_user.id)
            used_discount = True
        payment_id = add_pending_payment(message.from_user.id, final_amount, "sbp")
        for admin_id in ADMIN_IDS:
            discount_text = f"\n🎁 Применена скидка: {discount_amount} RUB" if used_discount else ""
            await bot.send_message(
                admin_id,
                f"💰 Запрос на пополнение\n\n"
                f"👤 Пользователь: @{message.from_user.username or 'Нет username'} (ID: {message.from_user.id})\n"
                f"💵 Сумма: {amount} RUB\n"
                f"💳 К оплате: {final_amount} RUB{discount_text}\n"
                f"📱 Метод: СБП\n"
                f"🆔 ID платежа: {payment_id}",
                reply_markup=admin_payment_keyboard(payment_id)
            )
        discount_message = f"\n🎁 Скидка {discount}% применена! К оплате: {final_amount} RUB" if used_discount else ""
        await message.answer(
            f"✅ Запрос создан.{discount_message}\n\n"
            f"Ожидайте, администратор отправит реквизиты."
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

# ---- CryptoBot ----
@dp.callback_query(F.data == "pay_crypto")
async def pay_crypto(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"₿ Пополнение через CryptoBot\n\n"
        f"Введите сумму в RUB:"
    )
    await state.set_state(PaymentStates.waiting_for_crypto_amount)
    await callback.answer()

@dp.message(PaymentStates.waiting_for_crypto_amount)
async def crypto_amount_handler(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text)
        final_amount = amount
        discount_amount = 0
        used_discount = False
        if can_use_discount(message.from_user.id):
            discount = get_setting('referral_discount')
            discount_amount = amount * (discount / 100)
            final_amount = amount - discount_amount
            apply_first_discount(message.from_user.id)
            used_discount = True

        invoice = await create_crypto_invoice(final_amount)
        if not invoice:
            await message.answer("❌ Ошибка при создании счёта. Попробуйте позже.")
            await state.clear()
            return

        payment_id = add_pending_payment(message.from_user.id, final_amount, "crypto", invoice['invoice_id'])
        discount_message = f"\n🎁 Скидка {discount}% применена! Сумма: {final_amount} RUB" if used_discount else ""

        pay_button = InlineKeyboardButton(text="💳 Оплатить в CryptoBot", url=invoice['pay_url'])
        check_button = InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"check_crypto_{payment_id}")
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[pay_button], [check_button]])

        await message.answer(
            f"₿ Счёт создан{discount_message}\n\n"
            f"💵 Сумма: {final_amount} RUB\n"
            f"💲 USDT: {invoice['amount']}\n\n"
            f"Нажмите кнопку для оплаты:",
            reply_markup=keyboard
        )
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

# ---- Админские обработчики платежей ----
@dp.callback_query(lambda c: c.data.startswith('send_payment_details_'))
async def send_payment_details(callback: types.CallbackQuery, state: FSMContext):
    payment_id = int(callback.data.split('_')[3])
    await state.update_data(payment_id=payment_id)
    await callback.message.edit_text("✍️ Введите реквизиты для оплаты:")
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
                f"💳 Реквизиты для оплаты\n\n"
                f"💰 Сумма: {payment[2]} RUB\n"
                f"📱 Способ: {payment[3].upper()}\n\n"
                f"Реквизиты:\n{message.text}\n\n"
                f"После оплаты нажмите кнопку ниже:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Я перевел", callback_data=f"user_paid_{payment_id}")]
                ])
            )
            await message.answer("✅ Реквизиты отправлены пользователю.")
        except:
            await message.answer("❌ Ошибка отправки.")
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith('user_paid_'))
async def user_paid(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    if payment:
        for admin_id in ADMIN_IDS:
            await bot.send_message(
                admin_id,
                f"💰 Пользователь сообщил об оплате\n\n"
                f"🆔 Платеж ID: {payment_id}\n"
                f"👤 Пользователь ID: {payment[1]}\n"
                f"💵 Сумма: {payment[2]} RUB\n"
                f"📱 Метод: {payment[3]}",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_payment_{payment_id}"),
                     InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment_{payment_id}")]
                ])
            )
        await callback.message.edit_text("✅ Сообщение отправлено администратору.")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_confirm_payment_'))
async def admin_confirm_payment(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[3])
    payment = get_pending_payment(payment_id)
    if payment:
        update_balance(payment[1], payment[2])
        update_payment_status(payment_id, 'confirmed')
        user = get_user(payment[1])
        if user and user[4]:
            reward_percent = get_setting('referral_reward')
            reward_amount = payment[2] * (reward_percent / 100)
            add_referral_earning(user[4], reward_amount, payment[1])
        try:
            await bot.send_message(
                payment[1],
                f"✅ Платеж подтвержден!\n\n"
                f"💰 Сумма: {payment[2]} RUB\n"
                f"💳 Баланс пополнен."
            )
        except:
            pass
        await callback.message.edit_text(f"✅ Платеж #{payment_id} подтвержден.")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('admin_reject_payment_'))
async def admin_reject_payment(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[3])
    payment = get_pending_payment(payment_id)
    if payment:
        update_payment_status(payment_id, 'rejected')
        try:
            await bot.send_message(
                payment[1],
                f"❌ Платеж отклонен.\n\n"
                f"💰 Сумма: {payment[2]} RUB\n"
                f"📞 Свяжитесь с поддержкой."
            )
        except:
            pass
        await callback.message.edit_text(f"❌ Платеж #{payment_id} отклонен.")
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('check_crypto_'))
async def check_crypto_payment(callback: types.CallbackQuery):
    payment_id = int(callback.data.split('_')[2])
    payment = get_pending_payment(payment_id)
    if not payment or payment[4] != 'pending':
        await callback.message.edit_text("❌ Платеж не найден или уже обработан.")
        await callback.answer()
        return
    await callback.message.edit_text("⏳ Ожидание подтверждения от администратора...")
    await callback.answer()

# -------------------- ПОКУПКА ТОВАРА --------------------
@dp.callback_query(lambda c: c.data.startswith('buy_'))
async def buy_product_handler(callback: types.CallbackQuery):
    product_id = int(callback.data.split('_')[1])
    product = get_product(product_id)
    if not product or len(product) < 8:
        await callback.message.edit_text("❌ Товар не найден.")
        await callback.answer()
        return
    product_id, name, price, phone, session, region, year, added = product[:8]
    user_balance = get_balance(callback.from_user.id)
    if user_balance >= price:
        update_balance(callback.from_user.id, -price)
        # Добавляем покупку
        add_purchase(
            callback.from_user.id, 
            product_id, 
            price, 
            phone,
            session,
            region,
            year
        )
        delete_product(product_id)
        current_year = datetime.now().year
        age = current_year - year
        text = (
            f"✅ Покупка успешна!\n\n"
            f"📦 Товар: {name}\n"
            f"💰 Цена: {price} RUB\n"
            f"🌍 Регион: {region}\n"
            f"📅 Год: {year} ({age} лет)\n\n"
            f"Данные для входа сохранены в истории покупок.\n"
            f"Перейдите в раздел «Мои покупки» и нажмите кнопку, чтобы увидеть их."
        )
        await callback.message.edit_text(text)
    else:
        await callback.message.edit_text(
            f"❌ Недостаточно средств\n\n"
            f"💰 Цена: {price} RUB\n"
            f"💳 Ваш баланс: {user_balance} RUB",
            reply_markup=insufficient_balance_keyboard()
        )
    await callback.answer()

# -------------------- АДМИН ПАНЕЛЬ --------------------
@dp.message(F.text == "⚙️ Админ панель")
async def admin_panel(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("❌ У вас нет доступа.")
        return
    await message.answer(
        "⚙️ Админ панель\n\n"
        "Выберите действие:",
        reply_markup=admin_keyboard()
    )

@dp.callback_query(F.data == "admin_settings")
async def admin_settings(callback: types.CallbackQuery):
    stars_rate = get_setting('stars_rate')
    usdt_rate = get_setting('usdt_rate')
    discount = get_setting('referral_discount')
    reward = get_setting('referral_reward')
    channel_link = get_setting('reviews_channel_link') or "не указана"
    text = (
        f"⚙️ Текущие настройки:\n\n"
        f"⭐ Stars курс: 1 Star = {stars_rate} RUB\n"
        f"💵 USDT курс: 1 USDT = {usdt_rate} RUB\n"
        f"🎁 Скидка рефералам: {discount}%\n"
        f"💸 Награда за реферала: {reward}%\n"
        f"📢 Канал отзывов: {channel_link}\n\n"
        f"Выберите для изменения:"
    )
    await callback.message.edit_text(text, reply_markup=admin_settings_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "set_stars_rate")
async def set_stars_rate(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"⭐ Введите новый курс Stars:\nТекущий: {get_setting('stars_rate')} RUB"
    )
    await state.set_state(ReferralStates.waiting_for_stars_rate)
    await callback.answer()

@dp.message(ReferralStates.waiting_for_stars_rate)
async def stars_rate_handler(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text)
        if rate <= 0:
            await message.answer("❌ Введите положительное число:")
            return
        update_setting('stars_rate', rate)
        await message.answer(f"✅ Курс обновлен: 1 Star = {rate} RUB")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

@dp.callback_query(F.data == "set_usdt_rate")
async def set_usdt_rate(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"💵 Введите новый курс USDT:\nТекущий: {get_setting('usdt_rate')} RUB"
    )
    await state.set_state(ReferralStates.waiting_for_usdt_rate)
    await callback.answer()

@dp.message(ReferralStates.waiting_for_usdt_rate)
async def usdt_rate_handler(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text)
        if rate <= 0:
            await message.answer("❌ Введите положительное число:")
            return
        update_setting('usdt_rate', rate)
        await message.answer(f"✅ Курс обновлен: 1 USDT = {rate} RUB")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

@dp.callback_query(F.data == "set_discount")
async def set_discount(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"🎁 Введите процент скидки:\nТекущий: {get_setting('referral_discount')}%"
    )
    await state.set_state(ReferralStates.waiting_for_discount_percent)
    await callback.answer()

@dp.message(ReferralStates.waiting_for_discount_percent)
async def discount_percent_handler(message: types.Message, state: FSMContext):
    try:
        percent = float(message.text)
        if percent < 0 or percent > 100:
            await message.answer("❌ Процент от 0 до 100:")
            return
        update_setting('referral_discount', percent)
        await message.answer(f"✅ Скидка обновлена: {percent}%")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

@dp.callback_query(F.data == "set_reward")
async def set_reward(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        f"💸 Введите процент награды:\nТекущий: {get_setting('referral_reward')}%"
    )
    await state.set_state(ReferralStates.waiting_for_reward_percent)
    await callback.answer()

@dp.message(ReferralStates.waiting_for_reward_percent)
async def reward_percent_handler(message: types.Message, state: FSMContext):
    try:
        percent = float(message.text)
        if percent < 0 or percent > 100:
            await message.answer("❌ Процент от 0 до 100:")
            return
        update_setting('referral_reward', percent)
        await message.answer(f"✅ Награда обновлена: {percent}%")
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите число.")

@dp.callback_query(F.data == "set_reviews_channel")
async def set_reviews_channel(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "📢 Введите username канала для отзывов (например, myreviews или @myreviews):"
    )
    await state.set_state(ReferralStates.waiting_for_reviews_channel)
    await callback.answer()

@dp.message(ReferralStates.waiting_for_reviews_channel)
async def process_reviews_channel(message: types.Message, state: FSMContext):
    raw_input = message.text.strip()
    username = raw_input.replace('@', '').replace('t.me/', '').replace('https://', '').replace('http://', '').split('/')[-1]
    if not username:
        await message.answer("❌ Введите корректный username канала.")
        return

    channel_link = f"https://t.me/{username}"
    try:
        await bot.send_message(f"@{username}", "✅ Бот успешно подключён к каналу отзывов!")
        update_setting('reviews_channel_link', channel_link)
        await message.answer(f"✅ Канал отзывов установлен: {channel_link}\nТестовое сообщение отправлено.")
    except Exception as e:
        logging.error(f"Failed to send test message to channel: {e}")
        await message.answer("❌ Не удалось отправить сообщение в канал. Убедитесь, что бот добавлен в канал как администратор.")
    await state.clear()

@dp.callback_query(F.data == "admin_add_balance")
async def admin_add_balance_start(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "💰 Введите ID пользователя, которому хотите начислить баланс:"
    )
    await state.set_state(AdminAddBalanceStates.waiting_for_user_id)
    await callback.answer()

@dp.message(AdminAddBalanceStates.waiting_for_user_id)
async def admin_add_balance_user_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text.strip())
        user = get_user(user_id)
        if not user:
            await message.answer("❌ Пользователь с таким ID не найден.")
            return
        await state.update_data(target_user_id=user_id)
        await message.answer("💰 Введите сумму для начисления:")
        await state.set_state(AdminAddBalanceStates.waiting_for_amount)
    except ValueError:
        await message.answer("❌ Введите корректный числовой ID.")

@dp.message(AdminAddBalanceStates.waiting_for_amount)
async def admin_add_balance_amount(message: types.Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной.")
            return
        data = await state.get_data()
        user_id = data['target_user_id']
        update_balance(user_id, amount)
        await message.answer(f"✅ Баланс пользователя {user_id} пополнен на {amount} RUB.")
        try:
            await bot.send_message(
                user_id,
                f"💰 Администратор пополнил ваш баланс на {amount} RUB."
            )
        except:
            pass
        await state.clear()
    except ValueError:
        await message.answer("❌ Введите корректную сумму.")

@dp.callback_query(F.data == "admin_add_product")
async def admin_add_product(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.edit_text(
        "➕ Добавление товара\n\n"
        "Введите название товара (например: Telegram Premium 1 год):"
    )
    await state.set_state(ProductStates.waiting_for_name)
    await callback.answer()

@dp.message(ProductStates.waiting_for_name)
async def product_name_handler(message: types.Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("💰 Введите цену в рублях:")
    await state.set_state(ProductStates.waiting_for_price)

@dp.message(ProductStates.waiting_for_price)
async def product_price_handler(message: types.Message, state: FSMContext):
    try:
        price = float(message.text)
        await state.update_data(price=price)
        await message.answer("📱 Введите номер телефона аккаунта (в формате +79001234567):")
        await state.set_state(ProductStates.waiting_for_phone)
    except ValueError:
        await message.answer("❌ Введите число.")

@dp.message(ProductStates.waiting_for_phone)
async def product_phone_handler(message: types.Message, state: FSMContext):
    phone = message.text.strip()
    await state.update_data(phone=phone)
    status_msg = await message.answer("🔄 Выполняю вход в аккаунт...")
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
                f"✅ Аккаунт успешно добавлен!\n\n"
                f"📦 Название: {data['name']}\n"
                f"💰 Цена: {data['price']} RUB\n"
                f"🌍 Регион: {result['region']}\n"
                f"📅 Год создания: {result['year']}\n"
                f"🆔 ID товара: {product_id}"
            )
            await state.clear()
        elif result.get('need_code'):
            await state.update_data(phone=result['phone'])
            await status_msg.edit_text(
                f"📱 Код подтверждения отправлен на номер {result['phone']}\n\n"
                f"Введите код из Telegram:"
            )
            await state.set_state(ProductStates.waiting_for_code)
        else:
            await status_msg.edit_text(f"❌ Ошибка: {result.get('error', 'Неизвестная ошибка')}")
    else:
        await status_msg.edit_text(f"❌ Ошибка входа: {result.get('error', 'Неизвестная ошибка')}")

@dp.message(ProductStates.waiting_for_code)
async def product_code_handler(message: types.Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data.get('phone')
    status_msg = await message.answer("🔄 Проверяю код...")
    result = await verify_code(phone, code)
    if result['success']:
        if result.get('need_password'):
            await state.update_data(phone=result['phone'])
            await status_msg.edit_text(
                "🔐 Требуется пароль двухфакторной аутентификации\n\n"
                "Введите пароль:"
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
                f"✅ Аккаунт успешно добавлен!\n\n"
                f"📦 Название: {data['name']}\n"
                f"💰 Цена: {data['price']} RUB\n"
                f"🌍 Регион: {result['region']}\n"
                f"📅 Год создания: {result['year']}\n"
                f"🆔 ID товара: {product_id}"
            )
            await state.clear()
    else:
        await status_msg.edit_text(f"❌ Ошибка: {result.get('error', 'Неизвестная ошибка')}")

@dp.message(ProductStates.waiting_for_password)
async def product_password_handler(message: types.Message, state: FSMContext):
    password = message.text.strip()
    data = await state.get_data()
    phone = data.get('phone')
    status_msg = await message.answer("🔄 Проверяю пароль...")
    result = await verify_password(phone, password)
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
            f"✅ Аккаунт успешно добавлен!\n\n"
            f"📦 Название: {data['name']}\n"
            f"💰 Цена: {data['price']} RUB\n"
            f"🌍 Регион: {result['region']}\n"
            f"📅 Год создания: {result['year']}\n"
            f"🆔 ID товара: {product_id}"
        )
        await state.clear()
    else:
        await status_msg.edit_text(f"❌ Ошибка: {result.get('error', 'Неверный пароль')}")

@dp.callback_query(F.data == "admin_delete_product")
async def admin_delete_product(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 Нет товаров.")
        await callback.answer()
        return
    buttons = []
    for product in products:
        if len(product) >= 8:
            product_id, name, price, phone, session, region, year, added = product[:8]
        else:
            continue
        buttons.append([InlineKeyboardButton(
            text=f"{name} | {region} | {price} RUB",
            callback_data=f"del_{product_id}"
        )])
    buttons.append([InlineKeyboardButton(text="🔙 Назад", callback_data="admin_back")])
    await callback.message.edit_text(
        "🗑 Выберите товар для удаления:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('del_'))
async def confirm_delete(callback: types.CallbackQuery):
    product_id = int(callback.data.split('_')[1])
    delete_product(product_id)
    await callback.message.edit_text("✅ Товар удален!")
    await callback.answer()

@dp.callback_query(F.data == "admin_list_products")
async def admin_list_products(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 Нет товаров.")
        await callback.answer()
        return
    text = "📦 Список товаров:\n\n"
    for product in products:
        if len(product) >= 8:
            product_id, name, price, phone, session, region, year, added = product[:8]
        else:
            continue
        text += (
            f"🆔 ID: {product_id}\n"
            f"📦 Название: {name}\n"
            f"💰 Цена: {price} RUB\n"
            f"📱 Телефон: {phone}\n"
            f"🌍 Регион: {region}\n"
            f"📅 Год: {year}\n"
            f"{'-' * 30}\n"
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
    c.execute("SELECT SUM(total_referral_earnings) FROM users")
    referral_paid = c.fetchone()[0] or 0
    c.execute("SELECT COUNT(*) FROM pending_payments WHERE status='pending'")
    pending = c.fetchone()[0]
    conn.close()
    text = (
        f"📊 Статистика\n\n"
        f"👥 Пользователей: {users}\n"
        f"👥 По рефералам: {referred}\n"
        f"📦 Товаров: {products}\n"
        f"🛒 Продаж: {purchases}\n"
        f"💰 Выручка: {revenue} RUB\n"
        f"💸 Рефералам: {referral_paid} RUB\n"
        f"⏳ Ожидают оплаты: {pending}"
    )
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(F.data == "admin_users")
async def admin_users(callback: types.CallbackQuery):
    conn = sqlite3.connect('shop.db')
    c = conn.cursor()
    c.execute("SELECT user_id, username, balance, registered_date, total_referrals, total_referral_earnings FROM users ORDER BY registered_date DESC LIMIT 10")
    users = c.fetchall()
    conn.close()
    text = "👥 Последние 10 пользователей:\n\n"
    for user in users:
        purchases = len(get_user_purchases(user[0]))
        text += (
            f"🆔 ID: {user[0]}\n"
            f"👤 Username: @{user[1] or 'Нет'}\n"
            f"💰 Баланс: {user[2]} RUB\n"
            f"📦 Покупок: {purchases}\n"
            f"👥 Рефералов: {user[4]}\n"
            f"💸 Заработано: {user[5]} RUB\n"
            f"📅 Дата: {user[3]}\n"
            f"{'-' * 15}\n"
        )
    await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(F.data == "admin_pending_payments")
async def admin_pending_payments(callback: types.CallbackQuery):
    payments = get_pending_payments_by_status('pending')
    if not payments:
        await callback.message.edit_text("📭 Нет ожидающих платежей.")
        await callback.answer()
        return
    for payment in payments:
        text = (
            f"💳 Платеж #{payment[0]}\n\n"
            f"👤 Пользователь ID: {payment[1]}\n"
            f"💰 Сумма: {payment[2]} RUB\n"
            f"📱 Метод: {payment[3]}\n"
            f"📅 Дата: {payment[5]}"
        )
        buttons = [
            [InlineKeyboardButton(text="✍️ Реквизиты", callback_data=f"send_payment_details_{payment[0]}"),
             InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"admin_confirm_payment_{payment[0]}")],
            [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"admin_reject_payment_{payment[0]}")]
        ]
        await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await callback.answer()

@dp.callback_query(F.data == "admin_back")
async def admin_back(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "⚙️ Админ панель\n\n"
        "Выберите действие:",
        reply_markup=admin_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_catalog")
async def back_to_catalog(callback: types.CallbackQuery):
    products = get_products()
    if not products:
        await callback.message.edit_text("📭 Каталог пуст.")
        await callback.answer()
        return
    text = "📦 Каталог товаров:\n\nВыберите товар для просмотра деталей:"
    await callback.message.edit_text(text, reply_markup=catalog_keyboard(products))
    await callback.answer()

@dp.callback_query(F.data == "back_to_balance")
async def back_to_balance(callback: types.CallbackQuery):
    user_balance = get_balance(callback.from_user.id)
    stars_rate = get_setting('stars_rate')
    text = (
        f"💰 Ваш баланс: {user_balance} RUB\n"
        f"⭐ Эквивалент в Stars: {int(user_balance / stars_rate)} ⭐\n\n"
        f"Выберите способ пополнения:"
    )
    await callback.message.edit_text(text, reply_markup=payment_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: types.CallbackQuery):
    await cmd_start(callback.message)
    await callback.answer()

# -------------------- ЗАПУСК --------------------
async def main():
    global bot_username
    bot_info = await bot.get_me()
    bot_username = bot_info.username
    print(f"Бот @{bot_username} запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
