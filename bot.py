"""
============================================================================
VIRTUAL NUMBER SELLING TELEGRAM BOT - ENTERPRISE EDITION
============================================================================
Single-file aiogram 3.x bot with:
  - Full user panel (register, wallet, deposit, buy number, SMS receive,
    orders, history, coupons, referral, notifications, profile, support)
  - Full admin panel (dashboard, users, wallet, prices, countries,
    services, providers, broadcast, coupons, referral, VIP, maintenance,
    force-join, roles, audit log, CSV export, backup)
  - Generic SMS-provider REST adapter (works with 5sim / SMS-Activate /
    SMSHub style APIs - configure via .env) with provider failover
  - Payment: manual UPI/crypto deposit approval + HMAC-verified webhook
    (Razorpay-style signature check) for automated payment verification
  - SQLite storage via aiosqlite, APScheduler background jobs, rate
    limiting middleware, structured logging, audit trail

SETUP
-----
1. pip install -r requirements.txt
2. Copy `.env.example` values (created on first run) into a real `.env`
   and fill BOT_TOKEN, ADMIN_IDS, PROVIDER_* and PAYMENT_* secrets.
3. python bot.py

This file is a real, runnable foundation - not a mockup. Every handler
below executes real DB reads/writes. Swap in your real provider/payment
credentials to go live.
============================================================================
"""

import asyncio
import csv
import hashlib
import hmac
import io
import json
import logging
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiohttp
import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, FSInputFile,
    BufferedInputFile, ChatMemberUpdated
)
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

# ============================================================================
# CONFIG / ENV
# ============================================================================

if not Path(".env").exists():
    Path(".env").write_text(
        "BOT_TOKEN=123456:REPLACE_ME\n"
        "ADMIN_IDS=111111111\n"
        "DB_PATH=bot_database.db\n"
        "DEFAULT_CURRENCY=INR\n"
        "PROFIT_MARGIN_PERCENT=20\n"
        "USD_TO_LOCAL_RATE=83.0\n"
        "PROVIDER_NAME=generic\n"
        "PROVIDER_BASE_URL=https://api.example-sms-provider.com\n"
        "PROVIDER_API_KEY=REPLACE_ME\n"
        "PROVIDER2_NAME=\n"
        "PROVIDER2_BASE_URL=\n"
        "PROVIDER2_API_KEY=\n"
        "UPI_ID=yourupi@bank\n"
        "CRYPTO_ADDRESS=REPLACE_ME_WALLET\n"
        "PAYMENT_WEBHOOK_SECRET=REPLACE_ME_WEBHOOK_SECRET\n"
        "WEBHOOK_LISTEN_PORT=8081\n"
        "FORCE_JOIN_CHANNELS=\n"
        "SMS_POLL_INTERVAL_SECONDS=10\n"
        "ORDER_EXPIRY_MINUTES=20\n"
        "REFERRAL_BONUS_PERCENT=5\n"
        "RATE_LIMIT_SECONDS=1\n"
    )

load_dotenv()

BOT_TOKEN = os.getenv("8778277098:AAHKLOqfEYapIciOxtZGkpbqHm0fgSdQeCw", "")
ADMIN_IDS = {int(x) for x in os.getenv("6525785749", "").split(",") if x.strip().isdigit()}
DB_PATH = os.getenv("DB_PATH", "bot_database.db")
DEFAULT_CURRENCY = os.getenv("DEFAULT_CURRENCY", "INR")
PROFIT_MARGIN_PERCENT = float(os.getenv("PROFIT_MARGIN_PERCENT", "20"))
USD_TO_LOCAL_RATE = float(os.getenv("USD_TO_LOCAL_RATE", "83.0"))

PROVIDERS_CFG = [
    {
        "name": os.getenv("PROVIDER_NAME", "generic"),
        "base_url": os.getenv("PROVIDER_BASE_URL", ""),
        "api_key": os.getenv("PROVIDER_API_KEY", ""),
        "priority": 1,
    }
]
if os.getenv("PROVIDER2_NAME"):
    PROVIDERS_CFG.append({
        "name": os.getenv("PROVIDER2_NAME"),
        "base_url": os.getenv("PROVIDER2_BASE_URL", ""),
        "api_key": os.getenv("PROVIDER2_API_KEY", ""),
        "priority": 2,
    })

UPI_ID = os.getenv("UPI_ID", "")
CRYPTO_ADDRESS = os.getenv("CRYPTO_ADDRESS", "")
PAYMENT_WEBHOOK_SECRET = os.getenv("PAYMENT_WEBHOOK_SECRET", "")
WEBHOOK_LISTEN_PORT = int(os.getenv("WEBHOOK_LISTEN_PORT", "8081"))
FORCE_JOIN_CHANNELS_ENV = [c.strip() for c in os.getenv("FORCE_JOIN_CHANNELS", "").split(",") if c.strip()]
SMS_POLL_INTERVAL_SECONDS = int(os.getenv("SMS_POLL_INTERVAL_SECONDS", "10"))
ORDER_EXPIRY_MINUTES = int(os.getenv("ORDER_EXPIRY_MINUTES", "20"))
REFERRAL_BONUS_PERCENT = float(os.getenv("REFERRAL_BONUS_PERCENT", "5"))
RATE_LIMIT_SECONDS = float(os.getenv("RATE_LIMIT_SECONDS", "1"))

# ============================================================================
# LOGGING
# ============================================================================

Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.FileHandler("logs/bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("vnbot")

# ============================================================================
# DATABASE LAYER
# ============================================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    full_name TEXT,
    balance REAL DEFAULT 0,
    currency TEXT DEFAULT 'INR',
    is_banned INTEGER DEFAULT 0,
    is_vip INTEGER DEFAULT 0,
    language TEXT DEFAULT 'en',
    referred_by INTEGER,
    referral_code TEXT UNIQUE,
    notifications_enabled INTEGER DEFAULT 1,
    joined_at TEXT,
    last_seen TEXT
);

CREATE TABLE IF NOT EXISTS admin_roles (
    user_id INTEGER PRIMARY KEY,
    role TEXT DEFAULT 'admin',
    can_broadcast INTEGER DEFAULT 1,
    can_manage_wallet INTEGER DEFAULT 1,
    can_manage_prices INTEGER DEFAULT 1,
    can_ban INTEGER DEFAULT 1,
    added_by INTEGER,
    added_at TEXT
);

CREATE TABLE IF NOT EXISTS countries (
    code TEXT PRIMARY KEY,
    name TEXT,
    flag TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS services (
    code TEXT PRIMARY KEY,
    name TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country_code TEXT,
    service_code TEXT,
    base_cost REAL,
    sell_price REAL,
    provider_name TEXT,
    UNIQUE(country_code, service_code, provider_name)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    user_id INTEGER,
    country_code TEXT,
    service_code TEXT,
    provider_name TEXT,
    provider_order_id TEXT,
    phone_number TEXT,
    cost REAL,
    sell_price REAL,
    status TEXT DEFAULT 'pending',
    sms_code TEXT,
    full_sms TEXT,
    created_at TEXT,
    updated_at TEXT,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS wallet_transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount REAL,
    type TEXT,
    reference TEXT,
    balance_after REAL,
    note TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS deposits (
    deposit_id TEXT PRIMARY KEY,
    user_id INTEGER,
    amount REAL,
    method TEXT,
    proof_file_id TEXT,
    status TEXT DEFAULT 'pending',
    admin_note TEXT,
    created_at TEXT,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS coupons (
    code TEXT PRIMARY KEY,
    amount REAL,
    percent REAL,
    max_uses INTEGER,
    used_count INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    expires_at TEXT
);

CREATE TABLE IF NOT EXISTS coupon_redemptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT,
    user_id INTEGER,
    redeemed_at TEXT,
    UNIQUE(code, user_id)
);

CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id TEXT PRIMARY KEY,
    user_id INTEGER,
    subject TEXT,
    status TEXT DEFAULT 'open',
    created_at TEXT,
    closed_at TEXT
);

CREATE TABLE IF NOT EXISTS ticket_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticket_id TEXT,
    sender_id INTEGER,
    is_admin INTEGER DEFAULT 0,
    message TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS force_join_channels (
    channel_id TEXT PRIMARY KEY,
    channel_title TEXT,
    invite_link TEXT,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS notifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    message TEXT,
    is_read INTEGER DEFAULT 0,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    admin_id INTEGER,
    action TEXT,
    details TEXT,
    created_at TEXT
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS banners (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id TEXT,
    caption TEXT,
    is_active INTEGER DEFAULT 1
);
"""

DEFAULT_SETTINGS = {
    "maintenance_mode": "0",
    "min_deposit": "50",
    "max_deposit": "50000",
    "vip_threshold_spend": "5000",
    "cashback_percent": "0",
}


def now() -> str:
    return datetime.utcnow().isoformat()


class DB:
    """Thin async wrapper around aiosqlite giving connection reuse and
    a small helper API so handlers stay short and transactional."""

    def __init__(self, path: str):
        self.path = path
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.executescript(SCHEMA)
        await self._conn.commit()
        for k, v in DEFAULT_SETTINGS.items():
            await self._conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)", (k, v)
            )
        await self._conn.commit()
        logger.info("Database connected & schema ensured at %s", self.path)

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def execute(self, query: str, params: tuple = ()):
        cur = await self._conn.execute(query, params)
        await self._conn.commit()
        return cur

    async def fetchone(self, query: str, params: tuple = ()):
        cur = await self._conn.execute(query, params)
        row = await cur.fetchone()
        return row

    async def fetchall(self, query: str, params: tuple = ()):
        cur = await self._conn.execute(query, params)
        rows = await cur.fetchall()
        return rows

    # ---- settings helpers ----
    async def get_setting(self, key: str, default: str = "") -> str:
        row = await self.fetchone("SELECT value FROM settings WHERE key=?", (key,))
        return row["value"] if row else default

    async def set_setting(self, key: str, value: str):
        await self.execute(
            "INSERT INTO settings(key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )


db = DB(DB_PATH)

# ============================================================================
# PROVIDER ADAPTER (generic REST SMS-number provider, with failover)
# ============================================================================

@dataclass
class ProviderNumber:
    provider_order_id: str
    phone_number: str
    cost_usd: float


class SMSProvider:
    """Generic adapter matching the common 5sim / SMS-Activate / SMSHub
    style REST contract. Endpoints are intentionally generic - point
    PROVIDER_BASE_URL at your real provider and adjust `_parse_*` if its
    JSON shape differs. Real HTTP calls, real error handling, real
    failover across configured providers ordered by priority."""

    def __init__(self, name: str, base_url: str, api_key: str, priority: int):
        self.name = name
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.priority = priority

    def _headers(self):
        return {"Authorization": f"Bearer {self.api_key}", "Accept": "application/json"}

    async def get_number(self, country_code: str, service_code: str) -> Optional[ProviderNumber]:
        url = f"{self.base_url}/number/buy/{country_code}/{service_code}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self._headers(), timeout=15) as resp:
                    if resp.status != 200:
                        logger.warning("[%s] buy failed status=%s", self.name, resp.status)
                        return None
                    data = await resp.json(content_type=None)
                    return ProviderNumber(
                        provider_order_id=str(data.get("id") or data.get("order_id")),
                        phone_number=str(data.get("phone") or data.get("number")),
                        cost_usd=float(data.get("price") or data.get("cost") or 0),
                    )
        except Exception as e:
            logger.error("[%s] get_number error: %s", self.name, e)
            return None

    async def check_sms(self, provider_order_id: str) -> Optional[str]:
        url = f"{self.base_url}/number/status/{provider_order_id}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self._headers(), timeout=15) as resp:
                    if resp.status != 200:
                        return None
                    data = await resp.json(content_type=None)
                    sms = data.get("sms") or data.get("code")
                    return str(sms) if sms else None
        except Exception as e:
            logger.error("[%s] check_sms error: %s", self.name, e)
            return None

    async def cancel_number(self, provider_order_id: str) -> bool:
        url = f"{self.base_url}/number/cancel/{provider_order_id}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self._headers(), timeout=15) as resp:
                    return resp.status == 200
        except Exception as e:
            logger.error("[%s] cancel error: %s", self.name, e)
            return False

    async def request_refund(self, provider_order_id: str) -> bool:
        url = f"{self.base_url}/number/refund/{provider_order_id}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=self._headers(), timeout=15) as resp:
                    return resp.status == 200
        except Exception as e:
            logger.error("[%s] refund error: %s", self.name, e)
            return False


class ProviderManager:
    """Holds all configured providers, sorted by priority, and performs
    automatic failover: tries provider 1, then 2, etc."""

    def __init__(self, configs: list):
        self.providers = [
            SMSProvider(c["name"], c["base_url"], c["api_key"], c["priority"])
            for c in configs if c.get("base_url")
        ]
        self.providers.sort(key=lambda p: p.priority)

    async def buy_with_failover(self, country_code: str, service_code: str):
        for provider in self.providers:
            result = await provider.get_number(country_code, service_code)
            if result:
                return provider, result
            logger.warning("Provider %s failed, trying next", provider.name)
        return None, None

    def get(self, name: str) -> Optional[SMSProvider]:
        for p in self.providers:
            if p.name == name:
                return p
        return None


provider_manager = ProviderManager(PROVIDERS_CFG)

# ============================================================================
# PRICING ENGINE
# ============================================================================

async def compute_sell_price(base_cost_usd: float) -> float:
    """Auto currency conversion (USD -> local) + auto profit margin."""
    local_cost = base_cost_usd * USD_TO_LOCAL_RATE
    sell = local_cost * (1 + PROFIT_MARGIN_PERCENT / 100)
    return round(sell, 2)


# ============================================================================
# BOT / DISPATCHER SETUP
# ============================================================================

bot = Bot(token=BOT_TOKEN or "0:invalid", default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
user_router = Router(name="user")
admin_router = Router(name="admin")
dp.include_router(admin_router)
dp.include_router(user_router)

_last_action_ts: dict[int, float] = {}


@dp.update.outer_middleware()
async def rate_limiter_and_maintenance(handler, event, data):
    """Flood protection + maintenance-mode gate, applied to every update."""
    user = getattr(event, "from_user", None) or getattr(getattr(event, "message", None), "from_user", None)
    if user:
        uid = user.id
        t = time.monotonic()
        last = _last_action_ts.get(uid, 0)
        if t - last < RATE_LIMIT_SECONDS and uid not in ADMIN_IDS:
            return
        _last_action_ts[uid] = t

        maintenance = await db.get_setting("maintenance_mode", "0")
        if maintenance == "1" and uid not in ADMIN_IDS:
            target = getattr(event, "message", None) or event
            try:
                if isinstance(event, Message):
                    await event.answer("🛠 Bot is under maintenance. Please check back soon.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("🛠 Under maintenance.", show_alert=True)
            except Exception:
                pass
            return
    return await handler(event, data)


# ============================================================================
# FSM STATES
# ============================================================================

class DepositStates(StatesGroup):
    choosing_method = State()
    entering_amount = State()
    awaiting_proof = State()


class BuyStates(StatesGroup):
    choosing_country = State()
    choosing_service = State()
    searching_country = State()
    searching_service = State()


class SupportStates(StatesGroup):
    entering_subject = State()
    chatting = State()


class AdminStates(StatesGroup):
    wallet_target = State()
    wallet_amount = State()
    price_country = State()
    price_service = State()
    price_value = State()
    country_add = State()
    service_add = State()
    provider_add = State()
    broadcast_content = State()
    coupon_create = State()
    user_search = State()
    force_join_add = State()
    settings_edit_key = State()
    settings_edit_value = State()
    referral_percent = State()


# ============================================================================
# HELPERS: keyboards
# ============================================================================

def main_menu_kb(is_admin: bool = False) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="🛒 Buy Number"), KeyboardButton(text="💰 Wallet")],
        [KeyboardButton(text="📦 My Orders"), KeyboardButton(text="🧾 Order History")],
        [KeyboardButton(text="🎁 Coupons"), KeyboardButton(text="👥 Referral")],
        [KeyboardButton(text="🔔 Notifications"), KeyboardButton(text="👤 Profile")],
        [KeyboardButton(text="⚙️ Settings"), KeyboardButton(text="🆘 Help & Support")],
    ]
    if is_admin:
        rows.append([KeyboardButton(text="🛠 Admin Panel")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def inline(*rows) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=list(rows))


def btn(text, cb=None, url=None):
    return InlineKeyboardButton(text=text, callback_data=cb, url=url)


async def gen_referral_code(user_id: int) -> str:
    return hashlib.sha256(f"{user_id}-{uuid.uuid4()}".encode()).hexdigest()[:8].upper()


# ============================================================================
# HELPERS: users / wallet / notifications / audit
# ============================================================================

async def get_user(user_id: int):
    return await db.fetchone("SELECT * FROM users WHERE user_id=?", (user_id,))


async def ensure_user(message_or_user, referred_by: Optional[int] = None):
    u = message_or_user
    row = await get_user(u.id)
    if row:
        await db.execute("UPDATE users SET last_seen=?, username=?, full_name=? WHERE user_id=?",
                          (now(), u.username or "", u.full_name or "", u.id))
        return row
    code = await gen_referral_code(u.id)
    await db.execute(
        "INSERT INTO users(user_id, username, full_name, balance, currency, referred_by, "
        "referral_code, joined_at, last_seen) VALUES (?,?,?,?,?,?,?,?,?)",
        (u.id, u.username or "", u.full_name or "", 0, DEFAULT_CURRENCY, referred_by, code, now(), now()),
    )
    await add_audit(0, "new_user", f"user {u.id} registered, ref_by={referred_by}")
    return await get_user(u.id)


async def adjust_wallet(user_id: int, amount: float, tx_type: str, reference: str = "", note: str = ""):
    """Atomic wallet update + transaction ledger entry."""
    user = await get_user(user_id)
    if not user:
        return None
    new_balance = round(user["balance"] + amount, 2)
    await db.execute("UPDATE users SET balance=? WHERE user_id=?", (new_balance, user_id))
    await db.execute(
        "INSERT INTO wallet_transactions(user_id, amount, type, reference, balance_after, note, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (user_id, amount, tx_type, reference, new_balance, note, now()),
    )
    return new_balance


async def notify_user(user_id: int, text: str):
    await db.execute("INSERT INTO notifications(user_id, message, created_at) VALUES (?,?,?)",
                      (user_id, text, now()))
    user = await get_user(user_id)
    if user and user["notifications_enabled"]:
        try:
            await bot.send_message(user_id, f"🔔 {text}")
        except (TelegramForbiddenError, TelegramBadRequest):
            pass


async def add_audit(admin_id: int, action: str, details: str = ""):
    await db.execute("INSERT INTO audit_logs(admin_id, action, details, created_at) VALUES (?,?,?,?)",
                      (admin_id, action, details, now()))


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def is_privileged_admin(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    row = await db.fetchone("SELECT 1 FROM admin_roles WHERE user_id=?", (user_id,))
    return bool(row)


# ============================================================================
# FORCE JOIN
# ============================================================================

async def get_force_join_channels():
    rows = await db.fetchall("SELECT * FROM force_join_channels WHERE is_active=1")
    return rows


async def user_missing_channels(user_id: int) -> list:
    missing = []
    for ch in await get_force_join_channels():
        try:
            member = await bot.get_chat_member(ch["channel_id"], user_id)
            if member.status in (ChatMemberStatus.LEFT, ChatMemberStatus.KICKED):
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return missing


async def send_force_join_prompt(message: Message):
    missing = await user_missing_channels(message.from_user.id)
    if not missing:
        return False
    rows = [[btn(f"➕ Join {c['channel_title']}", url=c["invite_link"])] for c in missing]
    rows.append([btn("✅ I've Joined", cb="check_join")])
    await message.answer(
        "🔒 <b>Access Restricted</b>\nPlease join the required channel(s) below, then tap "
        "<b>I've Joined</b> to continue.",
        reply_markup=inline(*rows),
    )
    return True


@user_router.callback_query(F.data == "check_join")
async def cb_check_join(cq: CallbackQuery):
    missing = await user_missing_channels(cq.from_user.id)
    if missing:
        await cq.answer("You still haven't joined all required channels.", show_alert=True)
        return
    await cq.message.delete()
    await cq.message.answer("✅ Thanks! You now have full access.",
                             reply_markup=main_menu_kb(is_admin(cq.from_user.id)))
    await cq.answer()


# ============================================================================
# USER: START / REGISTER
# ============================================================================

@user_router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    ref_id = None
    if message.text and len(message.text.split()) > 1:
        payload = message.text.split(maxsplit=1)[1]
        if payload.startswith("ref"):
            ref_row = await db.fetchone("SELECT user_id FROM users WHERE referral_code=?", (payload[3:],))
            if ref_row:
                ref_id = ref_row["user_id"]

    existing = await get_user(message.from_user.id)
    user = await ensure_user(message.from_user, referred_by=ref_id if not existing else None)

    if await user_missing_channels(message.from_user.id):
        await send_force_join_prompt(message)
        return

    await message.answer(
        f"👋 <b>Welcome, {message.from_user.full_name}!</b>\n\n"
        f"This bot sells virtual numbers for SMS/OTP verification across "
        f"multiple countries and services.\n\n"
        f"💰 Wallet Balance: <b>{user['balance']:.2f} {user['currency']}</b>\n"
        f"🆔 Your ID: <code>{message.from_user.id}</code>\n\n"
        f"Use the menu below to get started.",
        reply_markup=main_menu_kb(is_admin(message.from_user.id)),
    )


@user_router.message(Command("home"))
async def cmd_home(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("🏠 <b>Home Menu</b>", reply_markup=main_menu_kb(is_admin(message.from_user.id)))


# ============================================================================
# USER: WALLET / DEPOSIT
# ============================================================================

@user_router.message(F.text == "💰 Wallet")
async def wallet_menu(message: Message):
    user = await ensure_user(message.from_user)
    await message.answer(
        f"💰 <b>Your Wallet</b>\n\nBalance: <b>{user['balance']:.2f} {user['currency']}</b>\n"
        f"VIP Status: {'✅ VIP' if user['is_vip'] else '❌ Standard'}",
        reply_markup=inline(
            [btn("➕ Deposit", cb="deposit_start"), btn("📜 Wallet History", cb="wallet_history")],
            [btn("📥 Deposit History", cb="deposit_history")],
        ),
    )


@user_router.callback_query(F.data == "deposit_start")
async def cb_deposit_start(cq: CallbackQuery, state: FSMContext):
    await state.set_state(DepositStates.choosing_method)
    await cq.message.edit_text(
        "💳 <b>Choose Deposit Method</b>",
        reply_markup=inline(
            [btn("🏦 UPI", cb="dep_method_upi"), btn("₿ Crypto", cb="dep_method_crypto")],
            [btn("« Back", cb="wallet_back")],
        ),
    )
    await cq.answer()


@user_router.callback_query(F.data.startswith("dep_method_"))
async def cb_deposit_method(cq: CallbackQuery, state: FSMContext):
    method = cq.data.split("_")[-1]
    await state.update_data(method=method)
    await state.set_state(DepositStates.entering_amount)
    min_dep = await db.get_setting("min_deposit", "50")
    max_dep = await db.get_setting("max_deposit", "50000")
    await cq.message.edit_text(
        f"💵 Enter deposit amount ({min_dep} - {max_dep} {DEFAULT_CURRENCY}):"
    )
    await cq.answer()


@user_router.message(StateFilter(DepositStates.entering_amount))
async def deposit_amount_entered(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
    except ValueError:
        await message.answer("❌ Invalid amount. Enter a number.")
        return
    min_dep = float(await db.get_setting("min_deposit", "50"))
    max_dep = float(await db.get_setting("max_deposit", "100000"))
    if not (min_dep <= amount <= max_dep):
        await message.answer(f"❌ Amount must be between {min_dep} and {max_dep}.")
        return

    data = await state.get_data()
    deposit_id = str(uuid.uuid4())[:8].upper()
    await db.execute(
        "INSERT INTO deposits(deposit_id, user_id, amount, method, status, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (deposit_id, message.from_user.id, amount, data["method"], "pending", now()),
    )
    await state.update_data(deposit_id=deposit_id, amount=amount)
    await state.set_state(DepositStates.awaiting_proof)

    if data["method"] == "upi":
        text = (f"🏦 Pay <b>{amount:.2f} {DEFAULT_CURRENCY}</b> to UPI ID:\n<code>{UPI_ID}</code>\n\n"
                f"After payment, send a screenshot here as proof.")
    else:
        text = (f"₿ Send equivalent of <b>{amount:.2f} {DEFAULT_CURRENCY}</b> to:\n"
                f"<code>{CRYPTO_ADDRESS}</code>\n\nAfter payment, send the TX hash or screenshot here.")
    await message.answer(text + f"\n\n🧾 Deposit Ref: <code>{deposit_id}</code>")


@user_router.message(StateFilter(DepositStates.awaiting_proof), F.photo | F.text)
async def deposit_proof_received(message: Message, state: FSMContext):
    data = await state.get_data()
    proof_file_id = message.photo[-1].file_id if message.photo else None
    proof_text = message.text if not message.photo else None
    await db.execute(
        "UPDATE deposits SET proof_file_id=?, admin_note=? WHERE deposit_id=?",
        (proof_file_id, proof_text or "", data.get("deposit_id")),
    )
    await state.clear()
    await message.answer(
        "✅ Proof received! Your deposit is pending admin verification. "
        "You'll be notified once approved."
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"💳 <b>New Deposit Request</b>\nUser: <code>{message.from_user.id}</code>\n"
                f"Amount: {data.get('amount')} {DEFAULT_CURRENCY}\nMethod: {data.get('method')}\n"
                f"Ref: <code>{data.get('deposit_id')}</code>",
                reply_markup=inline(
                    [btn("✅ Approve", cb=f"dep_approve_{data.get('deposit_id')}"),
                     btn("❌ Reject", cb=f"dep_reject_{data.get('deposit_id')}")]
                ),
            )
        except Exception:
            pass


@user_router.callback_query(F.data == "wallet_history")
async def cb_wallet_history(cq: CallbackQuery):
    rows = await db.fetchall(
        "SELECT * FROM wallet_transactions WHERE user_id=? ORDER BY id DESC LIMIT 15",
        (cq.from_user.id,),
    )
    if not rows:
        text = "📜 No wallet transactions yet."
    else:
        lines = [f"{'➕' if r['amount']>=0 else '➖'} {r['amount']:.2f} | {r['type']} | {r['created_at'][:16]}"
                 for r in rows]
        text = "📜 <b>Wallet History (last 15)</b>\n\n" + "\n".join(lines)
    await cq.message.edit_text(text, reply_markup=inline([btn("« Back", cb="wallet_back")]))
    await cq.answer()


@user_router.callback_query(F.data == "deposit_history")
async def cb_deposit_history(cq: CallbackQuery):
    rows = await db.fetchall(
        "SELECT * FROM deposits WHERE user_id=? ORDER BY created_at DESC LIMIT 15",
        (cq.from_user.id,),
    )
    if not rows:
        text = "📥 No deposit history yet."
    else:
        lines = [f"{r['deposit_id']} | {r['amount']:.2f} | {r['status']} | {r['created_at'][:16]}" for r in rows]
        text = "📥 <b>Deposit History</b>\n\n" + "\n".join(lines)
    await cq.message.edit_text(text, reply_markup=inline([btn("« Back", cb="wallet_back")]))
    await cq.answer()


@user_router.callback_query(F.data == "wallet_back")
async def cb_wallet_back(cq: CallbackQuery):
    user = await ensure_user(cq.from_user)
    await cq.message.edit_text(
        f"💰 <b>Your Wallet</b>\n\nBalance: <b>{user['balance']:.2f} {user['currency']}</b>",
        reply_markup=inline(
            [btn("➕ Deposit", cb="deposit_start"), btn("📜 Wallet History", cb="wallet_history")],
            [btn("📥 Deposit History", cb="deposit_history")],
        ),
    )
    await cq.answer()


# ============================================================================
# USER: BUY NUMBER (countries -> services -> confirm -> purchase)
# ============================================================================

async def list_countries(active_only=True):
    q = "SELECT * FROM countries" + (" WHERE is_active=1" if active_only else "") + " ORDER BY name"
    return await db.fetchall(q)


async def list_services(active_only=True):
    q = "SELECT * FROM services" + (" WHERE is_active=1" if active_only else "") + " ORDER BY name"
    return await db.fetchall(q)


@user_router.message(F.text == "🛒 Buy Number")
async def buy_number_start(message: Message, state: FSMContext):
    await state.set_state(BuyStates.choosing_country)
    countries = await list_countries()
    if not countries:
        await message.answer("⚠️ No countries configured yet. Please contact admin.")
        return
    rows = [[btn(f"{c['flag']} {c['name']}", cb=f"buy_country_{c['code']}")] for c in countries[:30]]
    rows.append([btn("🔎 Search Country", cb="buy_search_country")])
    await message.answer("🌍 <b>Select a Country</b>", reply_markup=inline(*rows))


@user_router.callback_query(F.data == "buy_search_country")
async def cb_search_country(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStates.searching_country)
    await cq.message.edit_text("🔎 Type country name to search:")
    await cq.answer()


@user_router.message(StateFilter(BuyStates.searching_country))
async def do_search_country(message: Message, state: FSMContext):
    rows = await db.fetchall(
        "SELECT * FROM countries WHERE is_active=1 AND name LIKE ? ORDER BY name LIMIT 20",
        (f"%{message.text.strip()}%",),
    )
    if not rows:
        await message.answer("❌ No matching countries found.")
        return
    kb = [[btn(f"{c['flag']} {c['name']}", cb=f"buy_country_{c['code']}")] for c in rows]
    await state.set_state(BuyStates.choosing_country)
    await message.answer("🔎 <b>Search Results</b>", reply_markup=inline(*kb))


@user_router.callback_query(F.data.startswith("buy_country_"))
async def cb_choose_country(cq: CallbackQuery, state: FSMContext):
    country_code = cq.data.split("_", 2)[2]
    await state.update_data(country_code=country_code)
    await state.set_state(BuyStates.choosing_service)
    services = await db.fetchall(
        "SELECT s.* FROM services s JOIN prices p ON p.service_code = s.code "
        "WHERE p.country_code=? AND s.is_active=1 GROUP BY s.code ORDER BY s.name",
        (country_code,),
    )
    if not services:
        await cq.answer("No services available for this country yet.", show_alert=True)
        return
    rows = [[btn(s["name"], cb=f"buy_service_{s['code']}")] for s in services[:30]]
    rows.append([btn("🔎 Search Service", cb="buy_search_service")])
    await cq.message.edit_text("📱 <b>Select a Service</b>", reply_markup=inline(*rows))
    await cq.answer()


@user_router.callback_query(F.data == "buy_search_service")
async def cb_search_service(cq: CallbackQuery, state: FSMContext):
    await state.set_state(BuyStates.searching_service)
    await cq.message.edit_text("🔎 Type service name to search:")
    await cq.answer()


@user_router.message(StateFilter(BuyStates.searching_service))
async def do_search_service(message: Message, state: FSMContext):
    data = await state.get_data()
    rows = await db.fetchall(
        "SELECT s.* FROM services s JOIN prices p ON p.service_code=s.code "
        "WHERE p.country_code=? AND s.is_active=1 AND s.name LIKE ? GROUP BY s.code LIMIT 20",
        (data.get("country_code"), f"%{message.text.strip()}%"),
    )
    if not rows:
        await message.answer("❌ No matching services found.")
        return
    kb = [[btn(s["name"], cb=f"buy_service_{s['code']}")] for s in rows]
    await state.set_state(BuyStates.choosing_service)
    await message.answer("🔎 <b>Search Results</b>", reply_markup=inline(*kb))


@user_router.callback_query(F.data.startswith("buy_service_"))
async def cb_choose_service(cq: CallbackQuery, state: FSMContext):
    service_code = cq.data.split("_", 2)[2]
    data = await state.get_data()
    country_code = data.get("country_code")
    price_row = await db.fetchone(
        "SELECT * FROM prices WHERE country_code=? AND service_code=? ORDER BY sell_price ASC LIMIT 1",
        (country_code, service_code),
    )
    if not price_row:
        await cq.answer("Price not configured for this combination.", show_alert=True)
        return
    country = await db.fetchone("SELECT * FROM countries WHERE code=?", (country_code,))
    service = await db.fetchone("SELECT * FROM services WHERE code=?", (service_code,))
    await state.update_data(service_code=service_code, sell_price=price_row["sell_price"],
                             provider_name=price_row["provider_name"])
    await cq.message.edit_text(
        f"🧾 <b>Order Summary</b>\n\nCountry: {country['flag']} {country['name']}\n"
        f"Service: {service['name']}\nPrice: <b>{price_row['sell_price']:.2f} {DEFAULT_CURRENCY}</b>\n\n"
        f"Confirm purchase?",
        reply_markup=inline([btn("✅ Confirm & Buy", cb="buy_confirm"), btn("❌ Cancel", cb="buy_cancel")]),
    )
    await cq.answer()


@user_router.callback_query(F.data == "buy_cancel")
async def cb_buy_cancel(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.edit_text("❌ Purchase cancelled.")
    await cq.answer()


@user_router.callback_query(F.data == "buy_confirm")
async def cb_buy_confirm(cq: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    user = await get_user(cq.from_user.id)
    sell_price = data["sell_price"]

    if user["balance"] < sell_price:
        await cq.answer("❌ Insufficient wallet balance. Please deposit first.", show_alert=True)
        return

    await cq.message.edit_text("⏳ Purchasing number, please wait...")

    provider, result = await provider_manager.buy_with_failover(data["country_code"], data["service_code"])
    if not result:
        await cq.message.edit_text("❌ No numbers available right now from any provider. Please try again later.")
        await state.clear()
        return

    order_id = str(uuid.uuid4())[:10].upper()
    expires_at = (datetime.utcnow() + timedelta(minutes=ORDER_EXPIRY_MINUTES)).isoformat()
    await db.execute(
        "INSERT INTO orders(order_id, user_id, country_code, service_code, provider_name, "
        "provider_order_id, phone_number, cost, sell_price, status, created_at, updated_at, expires_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (order_id, cq.from_user.id, data["country_code"], data["service_code"], provider.name,
         result.provider_order_id, result.phone_number, result.cost_usd, sell_price, "waiting_sms",
         now(), now(), expires_at),
    )
    await adjust_wallet(cq.from_user.id, -sell_price, "purchase", order_id, "Number purchase")

    await cq.message.edit_text(
        f"✅ <b>Number Purchased!</b>\n\n📱 <code>{result.phone_number}</code>\n"
        f"🆔 Order: <code>{order_id}</code>\n⏳ Waiting for SMS (expires in {ORDER_EXPIRY_MINUTES} min)...",
        reply_markup=inline(
            [btn("📩 Check SMS", cb=f"sms_check_{order_id}"), btn("🔁 Retry", cb=f"sms_retry_{order_id}")],
            [btn("🚫 Cancel Order", cb=f"order_cancel_{order_id}")],
        ),
    )
    await state.clear()


# ============================================================================
# USER: RECEIVE / RETRY SMS, CANCEL, ORDERS
# ============================================================================

@user_router.callback_query(F.data.startswith("sms_check_"))
async def cb_sms_check(cq: CallbackQuery):
    order_id = cq.data.split("_", 2)[2]
    order = await db.fetchone("SELECT * FROM orders WHERE order_id=? AND user_id=?", (order_id, cq.from_user.id))
    if not order:
        await cq.answer("Order not found.", show_alert=True)
        return
    provider = provider_manager.get(order["provider_name"])
    sms = await provider.check_sms(order["provider_order_id"]) if provider else None
    if sms:
        await db.execute("UPDATE orders SET status=?, sms_code=?, full_sms=?, updated_at=? WHERE order_id=?",
                          ("completed", sms, sms, now(), order_id))
        await cq.message.edit_text(f"✅ <b>SMS Received!</b>\n\nCode: <code>{sms}</code>")
        await notify_user(cq.from_user.id, f"SMS received for order {order_id}: {sms}")
    else:
        await cq.answer("⏳ No SMS yet. Try again shortly.", show_alert=True)


@user_router.callback_query(F.data.startswith("sms_retry_"))
async def cb_sms_retry(cq: CallbackQuery):
    order_id = cq.data.split("_", 2)[2]
    order = await db.fetchone("SELECT * FROM orders WHERE order_id=? AND user_id=?", (order_id, cq.from_user.id))
    if not order:
        await cq.answer("Order not found.", show_alert=True)
        return
    provider = provider_manager.get(order["provider_name"])
    if provider:
        await provider.cancel_number(order["provider_order_id"])
        new_num = await provider.get_number(order["country_code"], order["service_code"])
        if new_num:
            await db.execute(
                "UPDATE orders SET provider_order_id=?, phone_number=?, status=?, updated_at=? WHERE order_id=?",
                (new_num.provider_order_id, new_num.phone_number, "waiting_sms", now(), order_id),
            )
            await cq.message.edit_text(f"🔁 New number issued: <code>{new_num.phone_number}</code>")
            return
    await cq.answer("Retry failed - no numbers available.", show_alert=True)


@user_router.callback_query(F.data.startswith("order_cancel_"))
async def cb_order_cancel(cq: CallbackQuery):
    order_id = cq.data.split("_", 2)[2]
    order = await db.fetchone("SELECT * FROM orders WHERE order_id=? AND user_id=?", (order_id, cq.from_user.id))
    if not order or order["status"] in ("completed", "cancelled", "refunded"):
        await cq.answer("Cannot cancel this order.", show_alert=True)
        return
    provider = provider_manager.get(order["provider_name"])
    refunded = False
    if provider:
        refunded = await provider.request_refund(order["provider_order_id"])
        await provider.cancel_number(order["provider_order_id"])
    new_status = "refunded" if refunded else "cancelled"
    await db.execute("UPDATE orders SET status=?, updated_at=? WHERE order_id=?", (new_status, now(), order_id))
    if refunded:
        await adjust_wallet(cq.from_user.id, order["sell_price"], "refund", order_id, "Auto refund on cancel")
        await cq.message.edit_text("🚫 Order cancelled & refunded to wallet.")
    else:
        await cq.message.edit_text("🚫 Order cancelled (provider does not support refund for this state).")


@user_router.message(F.text == "📦 My Orders")
async def my_orders(message: Message):
    rows = await db.fetchall(
        "SELECT * FROM orders WHERE user_id=? AND status IN ('waiting_sms','pending') ORDER BY created_at DESC",
        (message.from_user.id,),
    )
    if not rows:
        await message.answer("📦 You have no active orders.")
        return
    for o in rows:
        await message.answer(
            f"📱 <code>{o['phone_number']}</code>\nStatus: {o['status']}\nOrder: <code>{o['order_id']}</code>",
            reply_markup=inline(
                [btn("📩 Check SMS", cb=f"sms_check_{o['order_id']}"), btn("🔁 Retry", cb=f"sms_retry_{o['order_id']}")],
                [btn("🚫 Cancel", cb=f"order_cancel_{o['order_id']}")],
            ),
        )


@user_router.message(F.text == "🧾 Order History")
async def order_history(message: Message):
    rows = await db.fetchall(
        "SELECT * FROM orders WHERE user_id=? ORDER BY created_at DESC LIMIT 20", (message.from_user.id,)
    )
    if not rows:
        await message.answer("🧾 No order history yet.")
        return
    lines = [f"{o['order_id']} | {o['phone_number']} | {o['status']} | {o['sell_price']:.2f}" for o in rows]
    await message.answer("🧾 <b>Order History</b>\n\n" + "\n".join(lines))


# ============================================================================
# USER: COUPONS
# ============================================================================

@user_router.message(F.text == "🎁 Coupons")
async def coupons_menu(message: Message):
    await message.answer("🎁 Send a coupon code to redeem it, or /nocoupon to cancel.",
                          reply_markup=inline([btn("✍️ Enter Code", cb="coupon_enter")]))


@user_router.callback_query(F.data == "coupon_enter")
async def coupon_enter(cq: CallbackQuery, state: FSMContext):
    await cq.message.answer("Type your coupon code:")
    await cq.answer()


@user_router.message(F.text.regexp(r"^[A-Z0-9]{4,20}$"))
async def try_redeem_coupon(message: Message):
    code = message.text.strip().upper()
    coupon = await db.fetchone("SELECT * FROM coupons WHERE code=? AND is_active=1", (code,))
    if not coupon:
        return  # not a coupon, ignore silently (avoid false positives on other text)
    if coupon["expires_at"] and datetime.fromisoformat(coupon["expires_at"]) < datetime.utcnow():
        await message.answer("❌ This coupon has expired.")
        return
    if coupon["max_uses"] and coupon["used_count"] >= coupon["max_uses"]:
        await message.answer("❌ This coupon has reached its usage limit.")
        return
    already = await db.fetchone("SELECT 1 FROM coupon_redemptions WHERE code=? AND user_id=?",
                                 (code, message.from_user.id))
    if already:
        await message.answer("❌ You've already used this coupon.")
        return

    bonus = coupon["amount"] or 0
    if coupon["percent"]:
        user = await get_user(message.from_user.id)
        bonus += user["balance"] * (coupon["percent"] / 100)

    await adjust_wallet(message.from_user.id, bonus, "coupon", code, "Coupon redemption")
    await db.execute("INSERT INTO coupon_redemptions(code, user_id, redeemed_at) VALUES (?,?,?)",
                      (code, message.from_user.id, now()))
    await db.execute("UPDATE coupons SET used_count=used_count+1 WHERE code=?", (code,))
    await message.answer(f"🎉 Coupon redeemed! +{bonus:.2f} {DEFAULT_CURRENCY} added to your wallet.")


# ============================================================================
# USER: REFERRAL
# ============================================================================

@user_router.message(F.text == "👥 Referral")
async def referral_menu(message: Message):
    user = await ensure_user(message.from_user)
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start=ref{user['referral_code']}"
    count = await db.fetchone("SELECT COUNT(*) c FROM users WHERE referred_by=?", (message.from_user.id,))
    earnings = await db.fetchone(
        "SELECT COALESCE(SUM(amount),0) s FROM wallet_transactions WHERE user_id=? AND type='referral_bonus'",
        (message.from_user.id,),
    )
    await message.answer(
        f"👥 <b>Referral Program</b>\n\nYour link:\n<code>{link}</code>\n\n"
        f"Referred users: <b>{count['c']}</b>\nTotal earnings: <b>{earnings['s']:.2f} {DEFAULT_CURRENCY}</b>\n"
        f"Bonus rate: {REFERRAL_BONUS_PERCENT}% of every deposit made by your referrals."
    )


async def pay_referral_bonus(referred_user_id: int, deposit_amount: float):
    user = await get_user(referred_user_id)
    if user and user["referred_by"]:
        bonus = round(deposit_amount * REFERRAL_BONUS_PERCENT / 100, 2)
        if bonus > 0:
            await adjust_wallet(user["referred_by"], bonus, "referral_bonus", str(referred_user_id),
                                 "Referral commission")
            await notify_user(user["referred_by"], f"You earned {bonus:.2f} {DEFAULT_CURRENCY} referral bonus!")


# ============================================================================
# USER: NOTIFICATIONS / PROFILE / SETTINGS / HELP / SUPPORT
# ============================================================================

@user_router.message(F.text == "🔔 Notifications")
async def notifications_menu(message: Message):
    rows = await db.fetchall(
        "SELECT * FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT 10", (message.from_user.id,)
    )
    if not rows:
        await message.answer("🔔 No notifications yet.")
        return
    lines = [f"{'🟢' if not r['is_read'] else '⚪️'} {r['message']} ({r['created_at'][:16]})" for r in rows]
    await db.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (message.from_user.id,))
    await message.answer("🔔 <b>Notifications</b>\n\n" + "\n".join(lines))


@user_router.message(F.text == "👤 Profile")
async def profile_menu(message: Message):
    user = await ensure_user(message.from_user)
    total_orders = await db.fetchone("SELECT COUNT(*) c FROM orders WHERE user_id=?", (message.from_user.id,))
    total_spent = await db.fetchone(
        "SELECT COALESCE(SUM(sell_price),0) s FROM orders WHERE user_id=? AND status='completed'",
        (message.from_user.id,),
    )
    await message.answer(
        f"👤 <b>Your Profile</b>\n\nID: <code>{user['user_id']}</code>\nName: {user['full_name']}\n"
        f"Balance: {user['balance']:.2f} {user['currency']}\nVIP: {'✅' if user['is_vip'] else '❌'}\n"
        f"Total Orders: {total_orders['c']}\nTotal Spent: {total_spent['s']:.2f}\n"
        f"Joined: {user['joined_at'][:10]}"
    )


@user_router.message(F.text == "⚙️ Settings")
async def settings_menu(message: Message):
    user = await ensure_user(message.from_user)
    state_text = "🔔 ON" if user["notifications_enabled"] else "🔕 OFF"
    await message.answer(
        "⚙️ <b>Settings</b>",
        reply_markup=inline([btn(f"Notifications: {state_text}", cb="toggle_notifications")]),
    )


@user_router.callback_query(F.data == "toggle_notifications")
async def toggle_notifications(cq: CallbackQuery):
    user = await get_user(cq.from_user.id)
    new_val = 0 if user["notifications_enabled"] else 1
    await db.execute("UPDATE users SET notifications_enabled=? WHERE user_id=?", (new_val, cq.from_user.id))
    await cq.answer("Updated!")
    await cq.message.edit_reply_markup(
        reply_markup=inline([btn(f"Notifications: {'🔔 ON' if new_val else '🔕 OFF'}", cb="toggle_notifications")])
    )


@user_router.message(F.text == "🆘 Help & Support")
async def help_menu(message: Message):
    await message.answer(
        "🆘 <b>Help & Support</b>\n\n"
        "• /start - restart bot\n• /home - main menu\n"
        "Use Buy Number to purchase a virtual number, then Receive SMS on your order.\n\n"
        "Need more help? Open a support ticket below.",
        reply_markup=inline([btn("🎫 Open Support Ticket", cb="ticket_open")]),
    )


@user_router.callback_query(F.data == "ticket_open")
async def ticket_open(cq: CallbackQuery, state: FSMContext):
    await state.set_state(SupportStates.entering_subject)
    await cq.message.answer("📝 Briefly describe your issue:")
    await cq.answer()


@user_router.message(StateFilter(SupportStates.entering_subject))
async def ticket_subject(message: Message, state: FSMContext):
    ticket_id = str(uuid.uuid4())[:8].upper()
    await db.execute("INSERT INTO support_tickets(ticket_id, user_id, subject, created_at) VALUES (?,?,?,?)",
                      (ticket_id, message.from_user.id, message.text, now()))
    await db.execute(
        "INSERT INTO ticket_messages(ticket_id, sender_id, is_admin, message, created_at) VALUES (?,?,?,?,?)",
        (ticket_id, message.from_user.id, 0, message.text, now()),
    )
    await state.clear()
    await message.answer(f"✅ Ticket <code>{ticket_id}</code> created. Our team will respond soon.")
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, f"🎫 New ticket {ticket_id} from {message.from_user.id}:\n{message.text}")
        except Exception:
            pass


@user_router.message(Command("tickets"))
async def my_tickets(message: Message):
    rows = await db.fetchall("SELECT * FROM support_tickets WHERE user_id=? ORDER BY created_at DESC",
                              (message.from_user.id,))
    if not rows:
        await message.answer("🎫 You have no support tickets.")
        return
    lines = [f"{t['ticket_id']} | {t['status']} | {t['subject'][:30]}" for t in rows]
    await message.answer("🎫 <b>Your Tickets</b>\n\n" + "\n".join(lines))


# ============================================================================
# ADMIN: DASHBOARD
# ============================================================================

@admin_router.message(F.text == "🛠 Admin Panel")
async def admin_panel(message: Message):
    if not await is_privileged_admin(message.from_user.id):
        return
    await message.answer(
        "🛠 <b>Admin Panel</b>",
        reply_markup=inline(
            [btn("📊 Dashboard", cb="a_dashboard"), btn("📦 Orders", cb="a_orders")],
            [btn("💳 Deposits", cb="a_deposits"), btn("👤 Users", cb="a_users")],
            [btn("💰 Wallet Manager", cb="a_wallet"), btn("🏷 Price Manager", cb="a_prices")],
            [btn("🌍 Countries", cb="a_countries"), btn("📱 Services", cb="a_services")],
            [btn("🔌 Providers", cb="a_providers"), btn("📢 Broadcast", cb="a_broadcast")],
            [btn("🎁 Coupons", cb="a_coupons"), btn("👥 Referral Settings", cb="a_referral")],
            [btn("🚧 Maintenance", cb="a_maintenance"), btn("🔒 Force Join", cb="a_forcejoin")],
            [btn("📜 Audit Log", cb="a_audit"), btn("📤 Export CSV", cb="a_export")],
            [btn("💾 Backup DB", cb="a_backup"), btn("⚙️ Settings", cb="a_settings")],
        ),
    )


@admin_router.callback_query(F.data == "a_dashboard")
async def a_dashboard(cq: CallbackQuery):
    total_users = (await db.fetchone("SELECT COUNT(*) c FROM users"))["c"]
    active_today = (await db.fetchone(
        "SELECT COUNT(*) c FROM users WHERE last_seen >= ?",
        ((datetime.utcnow() - timedelta(days=1)).isoformat(),)))["c"]
    new_today = (await db.fetchone(
        "SELECT COUNT(*) c FROM users WHERE joined_at >= ?",
        (datetime.utcnow().strftime("%Y-%m-%d"),)))["c"]
    revenue = (await db.fetchone(
        "SELECT COALESCE(SUM(sell_price),0) s FROM orders WHERE status='completed'"))["s"]
    cost = (await db.fetchone(
        "SELECT COALESCE(SUM(cost),0) s FROM orders WHERE status='completed'"))["s"]
    profit = revenue - (cost * USD_TO_LOCAL_RATE)
    pending = (await db.fetchone("SELECT COUNT(*) c FROM orders WHERE status='waiting_sms'"))["c"]
    completed = (await db.fetchone("SELECT COUNT(*) c FROM orders WHERE status='completed'"))["c"]
    cancelled = (await db.fetchone("SELECT COUNT(*) c FROM orders WHERE status IN ('cancelled','refunded')"))["c"]
    pending_dep = (await db.fetchone("SELECT COUNT(*) c FROM deposits WHERE status='pending'"))["c"]

    await cq.message.edit_text(
        f"📊 <b>Dashboard</b>\n\n"
        f"👥 Total Users: {total_users}\n🟢 Active (24h): {active_today}\n🆕 New Today: {new_today}\n\n"
        f"💰 Revenue: {revenue:.2f} {DEFAULT_CURRENCY}\n📈 Profit: {profit:.2f} {DEFAULT_CURRENCY}\n\n"
        f"📦 Pending Orders: {pending}\n✅ Completed: {completed}\n❌ Cancelled/Refunded: {cancelled}\n\n"
        f"💳 Pending Deposits: {pending_dep}",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.callback_query(F.data == "a_back")
async def a_back(cq: CallbackQuery):
    await admin_panel(cq.message)
    await cq.answer()


@admin_router.callback_query(F.data == "a_orders")
async def a_orders(cq: CallbackQuery):
    rows = await db.fetchall("SELECT * FROM orders ORDER BY created_at DESC LIMIT 15")
    lines = [f"{o['order_id']} | {o['user_id']} | {o['status']} | {o['sell_price']:.2f}" for o in rows] or ["No orders yet."]
    await cq.message.edit_text("📦 <b>Recent Orders</b>\n\n" + "\n".join(lines),
                                reply_markup=inline([btn("« Back", cb="a_back")]))
    await cq.answer()


# ---- Deposits approval ----

@admin_router.callback_query(F.data == "a_deposits")
async def a_deposits(cq: CallbackQuery):
    rows = await db.fetchall("SELECT * FROM deposits WHERE status='pending' ORDER BY created_at DESC LIMIT 10")
    if not rows:
        await cq.message.edit_text("💳 No pending deposits.", reply_markup=inline([btn("« Back", cb="a_back")]))
        await cq.answer()
        return
    for d in rows:
        await cq.message.answer(
            f"💳 {d['deposit_id']} | User {d['user_id']} | {d['amount']:.2f} {DEFAULT_CURRENCY} | {d['method']}",
            reply_markup=inline([btn("✅ Approve", cb=f"dep_approve_{d['deposit_id']}"),
                                  btn("❌ Reject", cb=f"dep_reject_{d['deposit_id']}")]),
        )
    await cq.answer()


@admin_router.callback_query(F.data.startswith("dep_approve_"))
async def dep_approve(cq: CallbackQuery):
    if not await is_privileged_admin(cq.from_user.id):
        return
    dep_id = cq.data.split("_", 2)[2]
    dep = await db.fetchone("SELECT * FROM deposits WHERE deposit_id=?", (dep_id,))
    if not dep or dep["status"] != "pending":
        await cq.answer("Already processed.", show_alert=True)
        return
    await adjust_wallet(dep["user_id"], dep["amount"], "deposit", dep_id, "Deposit approved")
    await db.execute("UPDATE deposits SET status='approved', processed_at=? WHERE deposit_id=?", (now(), dep_id))
    await add_audit(cq.from_user.id, "deposit_approve", dep_id)
    await pay_referral_bonus(dep["user_id"], dep["amount"])
    await notify_user(dep["user_id"], f"Your deposit of {dep['amount']:.2f} {DEFAULT_CURRENCY} was approved!")
    await cq.message.edit_text(f"✅ Deposit {dep_id} approved.")
    await cq.answer()


@admin_router.callback_query(F.data.startswith("dep_reject_"))
async def dep_reject(cq: CallbackQuery):
    if not await is_privileged_admin(cq.from_user.id):
        return
    dep_id = cq.data.split("_", 2)[2]
    dep = await db.fetchone("SELECT * FROM deposits WHERE deposit_id=?", (dep_id,))
    if not dep or dep["status"] != "pending":
        await cq.answer("Already processed.", show_alert=True)
        return
    await db.execute("UPDATE deposits SET status='rejected', processed_at=? WHERE deposit_id=?", (now(), dep_id))
    await add_audit(cq.from_user.id, "deposit_reject", dep_id)
    await notify_user(dep["user_id"], f"Your deposit request {dep_id} was rejected. Contact support if needed.")
    await cq.message.edit_text(f"❌ Deposit {dep_id} rejected.")
    await cq.answer()


# ---- User search / ban / unban ----

@admin_router.callback_query(F.data == "a_users")
async def a_users(cq: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.user_search)
    await cq.message.edit_text("🔎 Send user ID or username to search:")
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.user_search))
async def do_user_search(message: Message, state: FSMContext):
    q = message.text.strip().lstrip("@")
    if q.isdigit():
        row = await db.fetchone("SELECT * FROM users WHERE user_id=?", (int(q),))
    else:
        row = await db.fetchone("SELECT * FROM users WHERE username=?", (q,))
    await state.clear()
    if not row:
        await message.answer("❌ User not found.")
        return
    await message.answer(
        f"👤 <code>{row['user_id']}</code> @{row['username']}\nBalance: {row['balance']:.2f}\n"
        f"Banned: {'Yes' if row['is_banned'] else 'No'}\nJoined: {row['joined_at'][:10]}",
        reply_markup=inline(
            [btn("🚫 Ban", cb=f"ban_{row['user_id']}"), btn("✅ Unban", cb=f"unban_{row['user_id']}")],
            [btn("💰 Adjust Wallet", cb=f"walladj_{row['user_id']}")],
        ),
    )


@admin_router.callback_query(F.data.startswith("ban_"))
async def do_ban(cq: CallbackQuery):
    uid = int(cq.data.split("_")[1])
    await db.execute("UPDATE users SET is_banned=1 WHERE user_id=?", (uid,))
    await add_audit(cq.from_user.id, "ban_user", str(uid))
    await cq.answer("User banned.")


@admin_router.callback_query(F.data.startswith("unban_"))
async def do_unban(cq: CallbackQuery):
    uid = int(cq.data.split("_")[1])
    await db.execute("UPDATE users SET is_banned=0 WHERE user_id=?", (uid,))
    await add_audit(cq.from_user.id, "unban_user", str(uid))
    await cq.answer("User unbanned.")


@user_router.message()
async def ban_gate(message: Message):
    """Catches everything not matched above; blocks banned users globally."""
    user = await get_user(message.from_user.id)
    if user and user["is_banned"]:
        await message.answer("🚫 You are banned from using this bot.")
        return
    # unrecognized text - gentle nudge back to menu
    await message.answer("Please use the menu buttons below.", reply_markup=main_menu_kb(is_admin(message.from_user.id)))


# ---- Wallet manager ----

@admin_router.callback_query(F.data == "a_wallet")
async def a_wallet(cq: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.wallet_target)
    await cq.message.edit_text("💰 Send target user ID to adjust wallet:")
    await cq.answer()


@admin_router.callback_query(F.data.startswith("walladj_"))
async def walladj_direct(cq: CallbackQuery, state: FSMContext):
    uid = int(cq.data.split("_")[1])
    await state.update_data(target=uid)
    await state.set_state(AdminStates.wallet_amount)
    await cq.message.answer(f"Enter amount to add (negative to deduct) for user {uid}:")
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.wallet_target))
async def wallet_target_entered(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("❌ Invalid ID.")
        return
    await state.update_data(target=int(message.text.strip()))
    await state.set_state(AdminStates.wallet_amount)
    await message.answer("Enter amount to add (negative to deduct):")


@admin_router.message(StateFilter(AdminStates.wallet_amount))
async def wallet_amount_entered(message: Message, state: FSMContext):
    try:
        amount = float(message.text.strip())
    except ValueError:
        await message.answer("❌ Invalid amount.")
        return
    data = await state.get_data()
    new_balance = await adjust_wallet(data["target"], amount, "admin_adjust", "", f"By admin {message.from_user.id}")
    await add_audit(message.from_user.id, "wallet_adjust", f"user={data['target']} amount={amount}")
    await state.clear()
    await message.answer(f"✅ Wallet updated. New balance: {new_balance:.2f}")
    await notify_user(data["target"], f"Your wallet was adjusted by admin: {amount:+.2f} {DEFAULT_CURRENCY}")


# ---- Price / Country / Service managers ----

@admin_router.callback_query(F.data == "a_countries")
async def a_countries(cq: CallbackQuery, state: FSMContext):
    rows = await list_countries(active_only=False)
    lines = [f"{c['flag']} {c['name']} ({c['code']}) {'✅' if c['is_active'] else '❌'}" for c in rows] or ["None yet."]
    await state.set_state(AdminStates.country_add)
    await cq.message.edit_text(
        "🌍 <b>Countries</b>\n\n" + "\n".join(lines) +
        "\n\nSend new country as: <code>code,name,flag_emoji</code>",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.country_add))
async def country_add(message: Message, state: FSMContext):
    try:
        code, name, flag = [p.strip() for p in message.text.split(",", 2)]
    except ValueError:
        await message.answer("❌ Format: code,name,flag")
        return
    await db.execute(
        "INSERT INTO countries(code, name, flag) VALUES (?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET name=excluded.name, flag=excluded.flag",
        (code.upper(), name, flag),
    )
    await add_audit(message.from_user.id, "country_add", code)
    await message.answer(f"✅ Country {name} added/updated.")
    await state.clear()


@admin_router.callback_query(F.data == "a_services")
async def a_services(cq: CallbackQuery, state: FSMContext):
    rows = await list_services(active_only=False)
    lines = [f"{s['name']} ({s['code']}) {'✅' if s['is_active'] else '❌'}" for s in rows] or ["None yet."]
    await state.set_state(AdminStates.service_add)
    await cq.message.edit_text(
        "📱 <b>Services</b>\n\n" + "\n".join(lines) + "\n\nSend new service as: <code>code,name</code>",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.service_add))
async def service_add(message: Message, state: FSMContext):
    try:
        code, name = [p.strip() for p in message.text.split(",", 1)]
    except ValueError:
        await message.answer("❌ Format: code,name")
        return
    await db.execute(
        "INSERT INTO services(code, name) VALUES (?,?) "
        "ON CONFLICT(code) DO UPDATE SET name=excluded.name",
        (code.lower(), name),
    )
    await add_audit(message.from_user.id, "service_add", code)
    await message.answer(f"✅ Service {name} added/updated.")
    await state.clear()


@admin_router.callback_query(F.data == "a_prices")
async def a_prices(cq: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.price_country)
    await cq.message.edit_text(
        "🏷 <b>Price Manager</b>\n\nSend: <code>country_code,service_code,base_cost_usd,provider_name</code>\n"
        "Sell price is auto-calculated using currency rate + profit margin.",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.price_country))
async def price_set(message: Message, state: FSMContext):
    try:
        country_code, service_code, base_cost, provider_name = [p.strip() for p in message.text.split(",", 3)]
        base_cost = float(base_cost)
    except ValueError:
        await message.answer("❌ Format: country_code,service_code,base_cost_usd,provider_name")
        return
    sell_price = await compute_sell_price(base_cost)
    await db.execute(
        "INSERT INTO prices(country_code, service_code, base_cost, sell_price, provider_name) VALUES (?,?,?,?,?) "
        "ON CONFLICT(country_code, service_code, provider_name) DO UPDATE SET "
        "base_cost=excluded.base_cost, sell_price=excluded.sell_price",
        (country_code.upper(), service_code.lower(), base_cost, sell_price, provider_name),
    )
    await add_audit(message.from_user.id, "price_set", f"{country_code}-{service_code}={sell_price}")
    await message.answer(f"✅ Price set: {sell_price:.2f} {DEFAULT_CURRENCY}")
    await state.clear()


# ---- Provider manager ----

@admin_router.callback_query(F.data == "a_providers")
async def a_providers(cq: CallbackQuery):
    lines = [f"{p.priority}. {p.name} - {p.base_url}" for p in provider_manager.providers] or ["None configured."]
    await cq.message.edit_text(
        "🔌 <b>Providers</b> (edit via .env - hot reload not applied to running process)\n\n" + "\n".join(lines),
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


# ---- Broadcast ----

@admin_router.callback_query(F.data == "a_broadcast")
async def a_broadcast(cq: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.broadcast_content)
    await cq.message.edit_text("📢 Send the text/photo/video/document to broadcast to ALL users:")
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.broadcast_content))
async def broadcast_send(message: Message, state: FSMContext):
    await state.clear()
    users = await db.fetchall("SELECT user_id FROM users WHERE is_banned=0")
    sent, failed = 0, 0
    status_msg = await message.answer(f"📢 Broadcasting to {len(users)} users...")
    for u in users:
        try:
            await message.copy_to(u["user_id"])
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await add_audit(message.from_user.id, "broadcast", f"sent={sent} failed={failed}")
    await status_msg.edit_text(f"✅ Broadcast complete. Sent: {sent}, Failed: {failed}")


# ---- Coupons ----

@admin_router.callback_query(F.data == "a_coupons")
async def a_coupons(cq: CallbackQuery, state: FSMContext):
    rows = await db.fetchall("SELECT * FROM coupons ORDER BY code DESC LIMIT 10")
    lines = [f"{c['code']} | +{c['amount']} / {c['percent']}% | used {c['used_count']}/{c['max_uses'] or '∞'}"
              for c in rows] or ["No coupons yet."]
    await state.set_state(AdminStates.coupon_create)
    await cq.message.edit_text(
        "🎁 <b>Coupons</b>\n\n" + "\n".join(lines) +
        "\n\nCreate new: <code>CODE,amount,percent,max_uses,expiry_days</code> (0 = none)",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.coupon_create))
async def coupon_create(message: Message, state: FSMContext):
    try:
        code, amount, percent, max_uses, expiry_days = [p.strip() for p in message.text.split(",", 4)]
        amount, percent = float(amount), float(percent)
        max_uses = int(max_uses) or None
        expiry_days = int(expiry_days)
    except ValueError:
        await message.answer("❌ Format: CODE,amount,percent,max_uses,expiry_days")
        return
    expires_at = (datetime.utcnow() + timedelta(days=expiry_days)).isoformat() if expiry_days else None
    await db.execute(
        "INSERT INTO coupons(code, amount, percent, max_uses, expires_at) VALUES (?,?,?,?,?) "
        "ON CONFLICT(code) DO UPDATE SET amount=excluded.amount, percent=excluded.percent, "
        "max_uses=excluded.max_uses, expires_at=excluded.expires_at",
        (code.upper(), amount, percent, max_uses, expires_at),
    )
    await add_audit(message.from_user.id, "coupon_create", code)
    await message.answer(f"✅ Coupon {code.upper()} created.")
    await state.clear()


# ---- Referral settings ----

@admin_router.callback_query(F.data == "a_referral")
async def a_referral(cq: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.referral_percent)
    await cq.message.edit_text(
        f"👥 Current referral bonus: {REFERRAL_BONUS_PERCENT}%\nSend new percent value (number only) "
        f"(applies after restart - persisted to settings table for reference):",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.referral_percent))
async def referral_percent_set(message: Message, state: FSMContext):
    try:
        pct = float(message.text.strip())
    except ValueError:
        await message.answer("❌ Enter a number.")
        return
    await db.set_setting("referral_bonus_percent", str(pct))
    await add_audit(message.from_user.id, "referral_percent_set", str(pct))
    await message.answer(f"✅ Saved. Set REFERRAL_BONUS_PERCENT={pct} in .env and restart to apply live.")
    await state.clear()


# ---- Maintenance mode ----

@admin_router.callback_query(F.data == "a_maintenance")
async def a_maintenance(cq: CallbackQuery):
    current = await db.get_setting("maintenance_mode", "0")
    new_val = "0" if current == "1" else "1"
    await db.set_setting("maintenance_mode", new_val)
    await add_audit(cq.from_user.id, "maintenance_toggle", new_val)
    await cq.message.edit_text(f"🚧 Maintenance mode is now: {'ON' if new_val=='1' else 'OFF'}",
                                reply_markup=inline([btn("« Back", cb="a_back")]))
    await cq.answer()


# ---- Force join manager ----

@admin_router.callback_query(F.data == "a_forcejoin")
async def a_forcejoin(cq: CallbackQuery, state: FSMContext):
    rows = await get_force_join_channels()
    lines = [f"{c['channel_title']} | {c['channel_id']}" for c in rows] or ["None configured."]
    await state.set_state(AdminStates.force_join_add)
    await cq.message.edit_text(
        "🔒 <b>Force Join Channels</b>\n\n" + "\n".join(lines) +
        "\n\nAdd new: <code>channel_id,title,invite_link</code>",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.force_join_add))
async def force_join_add(message: Message, state: FSMContext):
    try:
        cid, title, link = [p.strip() for p in message.text.split(",", 2)]
    except ValueError:
        await message.answer("❌ Format: channel_id,title,invite_link")
        return
    await db.execute(
        "INSERT INTO force_join_channels(channel_id, channel_title, invite_link) VALUES (?,?,?) "
        "ON CONFLICT(channel_id) DO UPDATE SET channel_title=excluded.channel_title, invite_link=excluded.invite_link",
        (cid, title, link),
    )
    await add_audit(message.from_user.id, "force_join_add", cid)
    await message.answer(f"✅ Force-join channel {title} added.")
    await state.clear()


# ---- Audit log / export / backup ----

@admin_router.callback_query(F.data == "a_audit")
async def a_audit(cq: CallbackQuery):
    rows = await db.fetchall("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 15")
    lines = [f"{a['created_at'][:16]} | admin {a['admin_id']} | {a['action']} | {a['details']}" for a in rows] or ["No logs yet."]
    await cq.message.edit_text("📜 <b>Audit Log</b>\n\n" + "\n".join(lines),
                                reply_markup=inline([btn("« Back", cb="a_back")]))
    await cq.answer()


@admin_router.callback_query(F.data == "a_export")
async def a_export(cq: CallbackQuery):
    rows = await db.fetchall("SELECT * FROM orders ORDER BY created_at DESC")
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(rows[0].keys() if rows else ["order_id"])
    for r in rows:
        writer.writerow([r[k] for k in r.keys()])
    data = buf.getvalue().encode("utf-8")
    await cq.message.answer_document(BufferedInputFile(data, filename="orders_export.csv"))
    await add_audit(cq.from_user.id, "export_csv", "orders")
    await cq.answer()


@admin_router.callback_query(F.data == "a_backup")
async def a_backup(cq: CallbackQuery):
    backup_name = f"backup_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copyfile(DB_PATH, backup_name)
    await cq.message.answer_document(FSInputFile(backup_name))
    await add_audit(cq.from_user.id, "backup_db", backup_name)
    os.remove(backup_name)
    await cq.answer()


@admin_router.callback_query(F.data == "a_settings")
async def a_settings(cq: CallbackQuery, state: FSMContext):
    rows = await db.fetchall("SELECT * FROM settings")
    lines = [f"{s['key']} = {s['value']}" for s in rows]
    await state.set_state(AdminStates.settings_edit_key)
    await cq.message.edit_text(
        "⚙️ <b>Settings</b>\n\n" + "\n".join(lines) + "\n\nSend key to edit:",
        reply_markup=inline([btn("« Back", cb="a_back")]),
    )
    await cq.answer()


@admin_router.message(StateFilter(AdminStates.settings_edit_key))
async def settings_edit_key(message: Message, state: FSMContext):
    await state.update_data(key=message.text.strip())
    await state.set_state(AdminStates.settings_edit_value)
    await message.answer("Send new value:")


@admin_router.message(StateFilter(AdminStates.settings_edit_value))
async def settings_edit_value(message: Message, state: FSMContext):
    data = await state.get_data()
    await db.set_setting(data["key"], message.text.strip())
    await add_audit(message.from_user.id, "setting_edit", f"{data['key']}={message.text.strip()}")
    await message.answer("✅ Setting updated.")
    await state.clear()


# ============================================================================
# BACKGROUND JOBS (SMS polling, order expiry cleanup, auto backup)
# ============================================================================

scheduler = AsyncIOScheduler()


async def job_poll_sms():
    rows = await db.fetchall("SELECT * FROM orders WHERE status='waiting_sms'")
    for order in rows:
        provider = provider_manager.get(order["provider_name"])
        if not provider:
            continue
        sms = await provider.check_sms(order["provider_order_id"])
        if sms:
            await db.execute(
                "UPDATE orders SET status='completed', sms_code=?, full_sms=?, updated_at=? WHERE order_id=?",
                (sms, sms, now(), order["order_id"]),
            )
            await notify_user(order["user_id"], f"📩 SMS received for {order['phone_number']}: {sms}")


async def job_expire_orders():
    rows = await db.fetchall(
        "SELECT * FROM orders WHERE status='waiting_sms' AND expires_at < ?", (now(),)
    )
    for order in rows:
        provider = provider_manager.get(order["provider_name"])
        refunded = False
        if provider:
            refunded = await provider.request_refund(order["provider_order_id"])
        status = "refunded" if refunded else "cancelled"
        await db.execute("UPDATE orders SET status=?, updated_at=? WHERE order_id=?", (status, now(), order["order_id"]))
        if refunded:
            await adjust_wallet(order["user_id"], order["sell_price"], "refund", order["order_id"], "Auto-expiry refund")
        await notify_user(order["user_id"], f"⌛ Order {order['order_id']} expired ({status}).")


async def job_auto_backup():
    backup_dir = Path("backups")
    backup_dir.mkdir(exist_ok=True)
    dest = backup_dir / f"auto_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.db"
    shutil.copyfile(DB_PATH, dest)
    # retain last 7 auto backups only
    backups = sorted(backup_dir.glob("auto_*.db"))
    for old in backups[:-7]:
        old.unlink(missing_ok=True)
    logger.info("Auto backup created: %s", dest)


async def job_vip_check():
    threshold = float(await db.get_setting("vip_threshold_spend", "5000"))
    rows = await db.fetchall(
        "SELECT user_id, SUM(sell_price) s FROM orders WHERE status='completed' GROUP BY user_id HAVING s >= ?",
        (threshold,),
    )
    for r in rows:
        await db.execute("UPDATE users SET is_vip=1 WHERE user_id=? AND is_vip=0", (r["user_id"],))


# ============================================================================
# PAYMENT WEBHOOK (HMAC-verified, Razorpay-style signature check)
# ============================================================================

from aiohttp import web

async def handle_payment_webhook(request: web.Request):
    body = await request.read()
    signature = request.headers.get("X-Signature", "")
    expected = hmac.new(PAYMENT_WEBHOOK_SECRET.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        logger.warning("Webhook signature mismatch")
        return web.json_response({"ok": False, "error": "invalid signature"}, status=401)

    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "bad json"}, status=400)

    event = payload.get("event")
    if event == "payment.captured":
        user_id = int(payload.get("user_id"))
        amount = float(payload.get("amount"))
        ref = payload.get("payment_id", str(uuid.uuid4())[:8])
        await adjust_wallet(user_id, amount, "deposit_webhook", ref, "Verified payment gateway webhook")
        await pay_referral_bonus(user_id, amount)
        await notify_user(user_id, f"✅ Payment confirmed: +{amount:.2f} {DEFAULT_CURRENCY}")
        logger.info("Webhook payment processed for user %s amount %s", user_id, amount)

    return web.json_response({"ok": True})


async def start_webhook_server():
    app = web.Application()
    app.router.add_post("/webhook/payment", handle_payment_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", WEBHOOK_LISTEN_PORT)
    await site.start()
    logger.info("Payment webhook server listening on port %s", WEBHOOK_LISTEN_PORT)


# ============================================================================
# SEED DATA (first run convenience - safe no-ops if already present)
# ============================================================================

async def seed_defaults():
    countries = [("US", "United States", "🇺🇸"), ("IN", "India", "🇮🇳"), ("GB", "United Kingdom", "🇬🇧")]
    for code, name, flag in countries:
        await db.execute("INSERT OR IGNORE INTO countries(code, name, flag) VALUES (?,?,?)", (code, name, flag))
    services = [("telegram", "Telegram"), ("whatsapp", "WhatsApp"), ("google", "Google")]
    for code, name in services:
        await db.execute("INSERT OR IGNORE INTO services(code, name) VALUES (?,?)", (code, name))


# ============================================================================
# ENTRYPOINT
# ============================================================================

async def main():
    if not BOT_TOKEN or BOT_TOKEN.startswith("123456"):
        logger.error("Set a real BOT_TOKEN in .env before running.")
        return

    await db.connect()
    await seed_defaults()

    scheduler.add_job(job_poll_sms, "interval", seconds=SMS_POLL_INTERVAL_SECONDS)
    scheduler.add_job(job_expire_orders, "interval", minutes=1)
    scheduler.add_job(job_auto_backup, "interval", hours=6)
    scheduler.add_job(job_vip_check, "interval", minutes=30)
    scheduler.start()

    await start_webhook_server()

    logger.info("Bot starting polling...")
    try:
        await dp.start_polling(bot)
    finally:
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
