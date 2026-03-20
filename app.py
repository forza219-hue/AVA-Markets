#!/usr/bin/env python3
import os
import json
import time
import html
import math
import sqlite3
import secrets
import bcrypt
import logging
import random
import threading
import requests

from datetime import datetime, timedelta
from functools import wraps
from dotenv import load_dotenv
from flask import Flask, request, redirect, make_response, render_template_string, g, jsonify, abort
from werkzeug.middleware.proxy_fix import ProxyFix

try:
    import stripe
except Exception:
    stripe = None

try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
except Exception:
    Limiter = None
    get_remote_address = None

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s - AVA V2 - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Config:
    HOST = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
    DATABASE = os.environ.get("DATABASE_URL", "ava_markets_v2.db").strip()
    SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32)).strip()
    DOMAIN = os.environ.get("DOMAIN", "").strip().rstrip("/")
    COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "false").lower() == "true"

    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    STRIPE_PRICE_PRO_MONTHLY = os.environ.get("STRIPE_PRICE_PRO_MONTHLY", "").strip()
    STRIPE_PRICE_PRO_YEARLY = os.environ.get("STRIPE_PRICE_PRO_YEARLY", "").strip()
    STRIPE_PRICE_ELITE_MONTHLY = os.environ.get("STRIPE_PRICE_ELITE_MONTHLY", "").strip()
    STRIPE_PRICE_ELITE_YEARLY = os.environ.get("STRIPE_PRICE_ELITE_YEARLY", "").strip()

    FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
    ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "you@example.com").strip().lower()

    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

    RATE_LIMIT_STORAGE_URI = os.environ.get("RATE_LIMIT_STORAGE_URI", "memory://")

    CRYPTO_CACHE_TTL = 300
    STOCK_CACHE_TTL = 600
    PAGE_SIZE_CRYPTO = 100
    PAGE_SIZE_STOCKS = 100
    SIGNAL_MIN_CONFIDENCE = 66
    SIGNAL_MIN_RR = 1.2


if stripe and Config.STRIPE_SECRET_KEY:
    stripe.api_key = Config.STRIPE_SECRET_KEY

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config["SECRET_KEY"] = Config.SECRET_KEY

if Limiter:
    limiter = Limiter(
        key_func=get_remote_address,
        app=app,
        storage_uri=Config.RATE_LIMIT_STORAGE_URI,
        default_limits=["300 per hour"]
    )
else:
    class _NoopLimiter:
        def limit(self, *args, **kwargs):
            return lambda fn: fn
    limiter = _NoopLimiter()


CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
:root{
  --bg:#06080d;
  --bg2:#0a0f17;
  --bg3:#101722;
  --card:rgba(255,255,255,.045);
  --card2:rgba(255,255,255,.03);
  --border:rgba(255,255,255,.08);
  --text:#f9fafb;
  --muted:#95a1b4;
  --yellow:#facc15;
  --yellow2:#fde047;
  --yellow3:#eab308;
  --green:#22c55e;
  --red:#ef4444;
  --blue:#38bdf8;
  --shadow:0 28px 70px rgba(0,0,0,.45);
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{
  margin:0;
  font-family:'Inter',sans-serif;
  color:var(--text);
  background:
    radial-gradient(circle at 10% 0%, rgba(250,204,21,.08), transparent 25%),
    radial-gradient(circle at 90% 0%, rgba(56,189,248,.08), transparent 20%),
    radial-gradient(circle at 50% 100%, rgba(250,204,21,.05), transparent 28%),
    linear-gradient(160deg,var(--bg),var(--bg2) 45%, var(--bg3));
}
a{text-decoration:none;color:inherit}
.container{max-width:1280px;margin:0 auto;padding:0 24px}
.nav{
  display:flex;justify-content:space-between;align-items:center;
  padding:18px 0;position:sticky;top:0;z-index:100;
  background:rgba(6,8,13,.72);backdrop-filter:blur(18px);
  border-bottom:1px solid rgba(255,255,255,.05);
}
.logo{
  font-size:1.34rem;font-weight:900;letter-spacing:.3px;
  background:linear-gradient(90deg,#fff,var(--yellow2),var(--yellow3));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
}
.nav-links{display:flex;gap:16px;align-items:center;flex-wrap:wrap}
.nav-links a{color:var(--muted);font-weight:700;transition:.2s}
.nav-links a:hover{color:var(--text)}
.hero{
  display:grid;grid-template-columns:1.12fr .88fr;gap:28px;
  align-items:center;padding:74px 0 38px;
}
.hero-card,.card,.table-shell,.glass{
  background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03));
  border:1px solid var(--border);
  border-radius:24px;
  box-shadow:var(--shadow);
}
.hero-card{padding:36px}
.card{padding:22px}
.glass{padding:18px}
.section{padding:26px 0 72px}
.badge{
  display:inline-flex;align-items:center;gap:8px;
  padding:8px 14px;border-radius:999px;
  background:rgba(250,204,21,.12);
  border:1px solid rgba(250,204,21,.24);
  color:#fde68a;font-size:.85rem;font-weight:800;
  margin-bottom:16px;
}
h1{font-size:clamp(2.5rem,5vw,4.8rem);line-height:1.02;margin:0 0 18px;font-weight:900}
h2{margin:0 0 14px}
h3{margin:0 0 8px}
p{color:var(--muted);line-height:1.7;font-size:1rem}
.hero-sub{font-size:1.08rem;max-width:720px}
.btns{display:flex;gap:14px;flex-wrap:wrap;margin-top:22px}
.btn{
  display:inline-flex;align-items:center;justify-content:center;
  padding:14px 18px;border-radius:14px;font-weight:800;
  border:1px solid transparent;cursor:pointer;transition:.2s;
}
.btn:hover{transform:translateY(-2px)}
.btn-primary{
  background:linear-gradient(90deg,var(--yellow2),var(--yellow3));
  color:#111827;
  box-shadow:0 12px 30px rgba(250,204,21,.18);
}
.btn-secondary{
  background:rgba(255,255,255,.04);
  border-color:rgba(255,255,255,.1);
  color:var(--text);
}
.btn-dark{
  background:#0d131d;border:1px solid rgba(255,255,255,.08);color:#fff;
}
.grid-2{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:20px}
.grid-3{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}
.grid-4{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}
.kpi{
  padding:18px;border-radius:18px;
  background:rgba(255,255,255,.03);
  border:1px solid rgba(255,255,255,.05)
}
.kpi .num{font-size:1.7rem;font-weight:900;color:#fff}
.kpi .label{color:var(--muted);font-size:.92rem}
.table-shell{overflow:hidden}
.market-table{width:100%;border-collapse:collapse}
.market-table th,.market-table td{
  padding:16px 14px;border-bottom:1px solid rgba(255,255,255,.06);
  text-align:left;vertical-align:top;
}
.market-table th{
  color:#d6dce5;background:rgba(255,255,255,.02);font-size:.91rem;font-weight:800;
}
.market-table tr:hover{background:rgba(255,255,255,.02)}
.asset-name strong{display:block}
.asset-name span{display:block;color:var(--muted);font-size:.84rem;margin-top:4px}
.asset-row{display:flex;align-items:center;gap:10px}
.asset-logo{
  width:28px;height:28px;border-radius:50%;object-fit:cover;background:#fff;
}
.asset-icon{
  width:28px;height:28px;display:inline-flex;align-items:center;justify-content:center;
  font-size:1.1rem;
}
.up{color:var(--green)}
.down{color:var(--red)}
.signal{
  display:inline-flex;padding:8px 12px;border-radius:999px;font-weight:800;font-size:.81rem
}
.signal-buy{background:rgba(34,197,94,.14);color:#86efac}
.signal-hold{background:rgba(250,204,21,.12);color:#fde68a}
.signal-sell{background:rgba(239,68,68,.14);color:#fca5a5}
.pill{
  display:inline-flex;padding:7px 12px;border-radius:999px;
  background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);
  color:#fff;font-size:.8rem;font-weight:700
}
.form-shell{display:flex;justify-content:center;align-items:center;min-height:70vh}
.form-card{
  width:100%;max-width:460px;padding:36px;border-radius:24px;
  background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03));
  border:1px solid var(--border);box-shadow:var(--shadow)
}
.form-card input,.form-card select{
  width:100%;padding:14px 16px;margin:10px 0;border-radius:14px;
  border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);
  color:var(--text);outline:none;transition:.2s
}
.form-card input:focus,.form-card select:focus{border-color:rgba(250,204,21,.45)}
.form-card button{
  width:100%;padding:14px 18px;margin-top:10px;border:none;border-radius:14px;
  background:linear-gradient(90deg,var(--yellow2),var(--yellow3));
  color:#111827;font-weight:900;cursor:pointer
}
.error{
  color:#fecaca;background:rgba(239,68,68,.12);padding:10px;border-radius:10px;
  font-size:.92rem;margin-bottom:10px
}
.success{
  color:#bbf7d0;background:rgba(34,197,94,.12);padding:10px;border-radius:10px;
  font-size:.92rem;margin-bottom:10px
}
.metric-box{
  background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.05);
  padding:16px;border-radius:16px
}
.candle-container{
  height:200px;display:flex;align-items:flex-end;gap:4px;padding:20px 0;
  border-bottom:1px solid rgba(255,255,255,.05)
}
.candle{flex:1;position:relative;height:100%}
.wick{position:absolute;left:50%;transform:translateX(-50%);width:2px;border-radius:2px}
.body{position:absolute;left:50%;transform:translateX(-50%);width:80%;border-radius:3px;max-width:12px}
.c-up .wick{background:#34d399}.c-up .body{background:linear-gradient(180deg,#34d399,#16a34a)}
.c-down .wick{background:#f87171}.c-down .body{background:linear-gradient(180deg,#f87171,#dc2626)}
.pagination{display:flex;gap:10px;flex-wrap:wrap;margin-top:22px}
.page-link{
  padding:10px 14px;border-radius:12px;background:rgba(255,255,255,.05);
  border:1px solid rgba(255,255,255,.08);color:var(--text);font-weight:800
}
.price-card{
  position:relative;padding:26px;border-radius:24px;
  background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03));
  border:1px solid var(--border);box-shadow:var(--shadow)
}
.price-card.featured{
  border-color:rgba(250,204,21,.34);
  box-shadow:0 24px 70px rgba(250,204,21,.08), var(--shadow);
}
.price{
  font-size:2.4rem;font-weight:900;margin:8px 0 6px
}
.small{font-size:.92rem;color:var(--muted)}
.muted{color:var(--muted)}
.blur-lock{
  position:relative;overflow:hidden
}
.blur-lock .blurred{
  filter:blur(5px);
  opacity:.7;
  user-select:none;
  pointer-events:none;
}
.blur-overlay{
  position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  background:linear-gradient(180deg, rgba(6,8,13,.05), rgba(6,8,13,.72));
}
.lock-card{
  max-width:360px;text-align:center;padding:20px;border-radius:18px;
  background:rgba(10,15,23,.9);border:1px solid rgba(250,204,21,.2)
}
.hero-mini-chart{
  min-height:320px;display:flex;flex-direction:column;justify-content:space-between
}
.alert-box{
  border:1px solid rgba(250,204,21,.18);background:rgba(250,204,21,.06);
  color:#fde68a;padding:14px;border-radius:14px
}
.footer{padding:28px 0 40px;color:var(--muted);font-size:.9rem}
.footer-top{
  display:flex;justify-content:space-between;gap:20px;flex-wrap:wrap;
  padding:26px 0;border-top:1px solid rgba(255,255,255,.06);margin-top:40px
}
.disclaimer{
  color:#8fa0b6;font-size:.84rem;line-height:1.7;max-width:980px
}
.hr{height:1px;background:rgba(255,255,255,.06);margin:24px 0}
@media (max-width: 960px){
  .hero{grid-template-columns:1fr}
  .nav{flex-direction:column;gap:14px}
}
"""


CRYPTO_TOP_90 = [
    ("BTC", "Bitcoin"), ("ETH", "Ethereum"), ("BNB", "BNB"), ("SOL", "Solana"), ("XRP", "XRP"), ("DOGE", "Dogecoin"), ("ADA", "Cardano"), ("AVAX", "Avalanche"), ("LINK", "Chainlink"), ("DOT", "Polkadot"),
    ("MATIC", "Polygon"), ("LTC", "Litecoin"), ("BCH", "Bitcoin Cash"), ("ATOM", "Cosmos"), ("UNI", "Uniswap"), ("NEAR", "NEAR Protocol"), ("APT", "Aptos"), ("ARB", "Arbitrum"), ("OP", "Optimism"), ("SUI", "Sui"),
    ("PEPE", "Pepe"), ("SHIB", "Shiba Inu"), ("TRX", "TRON"), ("ETC", "Ethereum Classic"), ("XLM", "Stellar"), ("HBAR", "Hedera"), ("ICP", "Internet Computer"), ("FIL", "Filecoin"), ("INJ", "Injective"), ("RNDR", "Render"),
    ("TAO", "Bittensor"), ("IMX", "Immutable"), ("SEI", "Sei"), ("TIA", "Celestia"), ("JUP", "Jupiter"), ("PYTH", "Pyth Network"), ("BONK", "Bonk"), ("WIF", "dogwifhat"), ("FET", "Fetch.ai"), ("RUNE", "THORChain")
]

STOCK_UNIVERSE = [
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"), ("AMZN", "Amazon"), ("GOOGL", "Alphabet"), ("META", "Meta"), ("TSLA", "Tesla"), ("BRK-B", "Berkshire"),
    ("JPM", "JPMorgan"), ("V", "Visa"), ("MA", "Mastercard"), ("UNH", "UnitedHealth"), ("XOM", "Exxon"), ("LLY", "Eli Lilly"), ("AVGO", "Broadcom"), ("ORCL", "Oracle"),
    ("GC=F", "Gold Futures"), ("SI=F", "Silver Futures"), ("CL=F", "Oil Futures")
]

STOCK_DOMAINS = {
    "AAPL": "apple.com", "MSFT": "microsoft.com", "NVDA": "nvidia.com", "AMZN": "amazon.com", "GOOGL": "google.com", "META": "meta.com", "TSLA": "tesla.com", "BRK-B": "berkshirehathaway.com",
    "JPM": "jpmorganchase.com", "V": "visa.com", "MA": "mastercard.com", "UNH": "uhc.com", "XOM": "exxonmobil.com", "LLY": "lilly.com", "AVGO": "broadcom.com", "ORCL": "oracle.com"
}

PLAN_META = {
    "free": {
        "label": "Free",
        "monthly": 0,
        "yearly": 0
    },
    "pro_monthly": {
        "label": "Pro Monthly",
        "tier": "pro",
        "billing": "monthly",
        "price": 19
    },
    "pro_yearly": {
        "label": "Pro Yearly",
        "tier": "pro",
        "billing": "yearly",
        "price": 190
    },
    "elite_monthly": {
        "label": "Elite Monthly",
        "tier": "elite",
        "billing": "monthly",
        "price": 49
    },
    "elite_yearly": {
        "label": "Elite Yearly",
        "tier": "elite",
        "billing": "yearly",
        "price": 490
    }
}


def h(v):
    return html.escape("" if v is None else str(v), quote=True)


def get_stock_logo(sym):
    d = STOCK_DOMAINS.get(str(sym).upper())
    return f"https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL&url=http://{d}&size=128" if d else ""


def get_crypto_logo(sym):
    return f"https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/128/color/{str(sym).lower()}.png"


def get_asset_icon(sym):
    return {"GC=F": "🥇", "SI=F": "🥈", "CL=F": "🛢️"}.get(str(sym).upper(), "📈")


def fmt_price(v, sym=None):
    try:
        vf = float(v)
        return f"${vf:,.2f}" if vf >= 1 else f"${vf:.4f}" if vf >= 0.01 else f"${vf:.8f}"
    except Exception:
        return "$0.00"


def fmt_change(v):
    try:
        return f"{float(v):+.2f}%"
    except Exception:
        return "+0.00%"


def pct_change(price, prev):
    return 0.0 if prev in [0, None] else ((price - prev) / prev) * 100.0


def compute_light_signal(c):
    try:
        c = float(c)
    except Exception:
        c = 0.0
    return "BUY" if c >= 2.0 else "SELL" if c <= -2.0 else "HOLD"


def normalize_symbol_id(sym):
    return str(sym).replace("=", "_").replace("-", "_").replace("/", "_")


class Database:
    def __init__(self, path):
        self.path = path
        self.init()

    def conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL;")
        return c

    def init(self):
        c = self.conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            tier TEXT NOT NULL DEFAULT 'free',
            billing_cycle TEXT,
            stripe_customer_id TEXT,
            stripe_sub_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS market_cache (
            cache_key TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS active_signals (
            signal_id TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            name TEXT NOT NULL,
            signal TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            regime TEXT,
            entry_price REAL NOT NULL,
            stop_loss REAL NOT NULL,
            take_profit_1 REAL NOT NULL,
            take_profit_2 REAL NOT NULL,
            risk_reward REAL NOT NULL,
            reason TEXT,
            price REAL NOT NULL,
            change_pct REAL NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS signal_history (
            history_id TEXT PRIMARY KEY,
            signal_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            name TEXT NOT NULL,
            signal TEXT NOT NULL,
            confidence INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            stop_loss REAL NOT NULL,
            take_profit_1 REAL NOT NULL,
            take_profit_2 REAL NOT NULL,
            risk_reward REAL NOT NULL,
            outcome TEXT NOT NULL DEFAULT 'OPEN',
            outcome_note TEXT,
            updated_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS alert_subscribers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            active INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS broadcast_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel TEXT NOT NULL,
            message_hash TEXT NOT NULL,
            created_at INTEGER NOT NULL
        );
        """)
        c.commit()
        c.close()

    def create_user(self, email, password):
        c = self.conn()
        pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        try:
            c.execute("INSERT INTO users (email, password_hash) VALUES (?, ?)", (email.lower().strip(), pw_hash))
            c.commit()
            row = c.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
            return dict(row) if row else None
        except Exception:
            return None
        finally:
            c.close()

    def verify_user(self, email, password):
        c = self.conn()
        u = c.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
        c.close()
        if u and bcrypt.checkpw(password.encode(), u["password_hash"].encode()):
            return dict(u)
        return None

    def create_session(self, user_id):
        token = secrets.token_hex(32)
        exp = datetime.utcnow() + timedelta(days=30)
        c = self.conn()
        c.execute("INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)", (token, user_id, exp))
        c.commit()
        c.close()
        return token

    def get_user_by_session(self, token):
        if not token:
            return None
        c = self.conn()
        u = c.execute("""
            SELECT u.* FROM users u
            JOIN sessions s ON s.user_id = u.id
            WHERE s.token = ? AND s.expires_at > ?
        """, (token, datetime.utcnow())).fetchone()
        c.close()
        return dict(u) if u else None

    def get_user_by_id(self, user_id):
        c = self.conn()
        row = c.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        c.close()
        return dict(row) if row else None

    def upgrade_user(self, user_id, tier, customer_id=None, sub_id=None, billing_cycle=None):
        c = self.conn()
        c.execute("""
            UPDATE users
            SET tier = ?, stripe_customer_id = COALESCE(?, stripe_customer_id),
                stripe_sub_id = COALESCE(?, stripe_sub_id), billing_cycle = COALESCE(?, billing_cycle)
            WHERE id = ?
        """, (tier, customer_id, sub_id, billing_cycle, user_id))
        c.commit()
        c.close()

    def cache_get(self, key, ttl):
        c = self.conn()
        r = c.execute("SELECT payload_json, updated_at FROM market_cache WHERE cache_key = ?", (key,)).fetchone()
        c.close()
        if r and (int(time.time()) - r["updated_at"]) <= ttl:
            return json.loads(r["payload_json"])
        return None

    def cache_get_stale(self, key):
        c = self.conn()
        r = c.execute("SELECT payload_json FROM market_cache WHERE cache_key = ?", (key,)).fetchone()
        c.close()
        return json.loads(r["payload_json"]) if r else None

    def cache_set(self, key, payload):
        c = self.conn()
        c.execute("REPLACE INTO market_cache (cache_key, payload_json, updated_at) VALUES (?, ?, ?)",
                  (key, json.dumps(payload), int(time.time())))
        c.commit()
        c.close()

    def replace_active_signals(self, signals):
        c = self.conn()
        c.execute("DELETE FROM active_signals")
        now = int(time.time())
        for s in signals:
            c.execute("""
                INSERT INTO active_signals (
                    signal_id, symbol, asset_type, name, signal, confidence, regime,
                    entry_price, stop_loss, take_profit_1, take_profit_2,
                    risk_reward, reason, price, change_pct, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                s["signal_id"], s["symbol"], s["asset_type"], s["name"], s["signal"],
                s["confidence"], s["regime"], s["entry_price"], s["stop_loss"],
                s["take_profit_1"], s["take_profit_2"], s["risk_reward"], s["reason"],
                s["price"], s["change_pct"], now
            ))
        c.commit()
        c.close()

    def get_active_signals(self, asset_type=None, limit=50):
        c = self.conn()
        if asset_type:
            rows = c.execute("""
                SELECT * FROM active_signals
                WHERE asset_type = ?
                ORDER BY confidence DESC, risk_reward DESC, updated_at DESC
                LIMIT ?
            """, (asset_type, limit)).fetchall()
        else:
            rows = c.execute("""
                SELECT * FROM active_signals
                ORDER BY confidence DESC, risk_reward DESC, updated_at DESC
                LIMIT ?
            """, (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def sync_signal_history(self, signals):
        c = self.conn()
        now = int(time.time())
        active_ids = set()

        for s in signals:
            active_ids.add(s["signal_id"])
            row = c.execute("SELECT * FROM signal_history WHERE signal_id = ? AND outcome = 'OPEN'", (s["signal_id"],)).fetchone()
            if not row:
                c.execute("""
                    INSERT INTO signal_history (
                        history_id, signal_id, symbol, asset_type, name, signal, confidence,
                        entry_price, stop_loss, take_profit_1, take_profit_2, risk_reward,
                        outcome, outcome_note, updated_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', '', ?, ?)
                """, (
                    secrets.token_hex(16), s["signal_id"], s["symbol"], s["asset_type"], s["name"], s["signal"],
                    s["confidence"], s["entry_price"], s["stop_loss"], s["take_profit_1"],
                    s["take_profit_2"], s["risk_reward"], now, now
                ))
            else:
                c.execute("UPDATE signal_history SET updated_at = ? WHERE history_id = ?", (now, row["history_id"]))

        stale_open = c.execute("SELECT * FROM signal_history WHERE outcome = 'OPEN'").fetchall()
        for row in stale_open:
            if row["signal_id"] not in active_ids:
                c.execute("""
                    UPDATE signal_history
                    SET outcome = 'EXPIRED', outcome_note = 'Signal rotated out of active set.', updated_at = ?
                    WHERE history_id = ?
                """, (now, row["history_id"]))

        c.commit()
        c.close()

    def get_signal_history(self, limit=100):
        c = self.conn()
        rows = c.execute("""
            SELECT * FROM signal_history
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def get_signal_stats(self):
        c = self.conn()
        total = c.execute("SELECT COUNT(*) AS n FROM signal_history").fetchone()["n"]
        open_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome = 'OPEN'").fetchone()["n"]
        expired_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome = 'EXPIRED'").fetchone()["n"]
        tp1_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome = 'TP1_HIT'").fetchone()["n"]
        tp2_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome = 'TP2_HIT'").fetchone()["n"]
        stopped_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome = 'STOPPED'").fetchone()["n"]
        closed_wins = tp1_n + tp2_n
        closed_total = closed_wins + stopped_n
        win_rate = round((closed_wins / closed_total) * 100, 2) if closed_total > 0 else 0.0
        c.close()
        return {
            "total": total,
            "open": open_n,
            "expired": expired_n,
            "tp1_hit": tp1_n,
            "tp2_hit": tp2_n,
            "stopped": stopped_n,
            "win_rate": win_rate
        }

    def subscribe_email(self, email):
        c = self.conn()
        try:
            c.execute("INSERT OR IGNORE INTO alert_subscribers (email, active) VALUES (?, 1)", (email.lower().strip(),))
            c.commit()
            return True
        except Exception:
            return False
        finally:
            c.close()

    def get_subscriber_count(self):
        c = self.conn()
        n = c.execute("SELECT COUNT(*) AS n FROM alert_subscribers WHERE active = 1").fetchone()["n"]
        c.close()
        return n

    def get_user_count(self):
        c = self.conn()
        n = c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"]
        c.close()
        return n

    def get_paid_user_count(self):
        c = self.conn()
        n = c.execute("SELECT COUNT(*) AS n FROM users WHERE tier IN ('pro','elite')").fetchone()["n"]
        c.close()
        return n


db = Database(Config.DATABASE)
MEM_CACHE = {}


def get_web_user():
    return db.get_user_by_session(request.cookies.get("session_token"))


@app.before_request
def load_req():
    g.user = get_web_user()


def is_admin():
    return bool(g.user and str(g.user.get("email", "")).lower() == Config.ADMIN_EMAIL)


def require_auth(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        if not g.user:
            return redirect("/login")
        return fn(*args, **kwargs)
    return wrap


def require_tier(min_tier):
    tier_order = {"free": 0, "pro": 1, "elite": 2}

    def deco(fn):
        @wraps(fn)
        def wrap(*args, **kwargs):
            if not g.user:
                return redirect("/login")
            current = tier_order.get(g.user.get("tier", "free"), 0)
            needed = tier_order.get(min_tier, 0)
            if current < needed:
                return redirect("/pricing")
            return fn(*args, **kwargs)
        return wrap
    return deco


def require_admin(fn):
    @wraps(fn)
    def wrap(*args, **kwargs):
        if not g.user:
            return redirect("/login")
        if not is_admin():
            abort(403)
        return fn(*args, **kwargs)
    return wrap


def calc_ema(prices, period):
    if not prices:
        return []
    k = 2 / (period + 1)
    ema = [prices[0]]
    for p in prices[1:]:
        ema.append(p * k + ema[-1] * (1 - k))
    return ema


def calc_sma(prices, period):
    out = []
    for i in range(len(prices)):
        if i + 1 < period:
            out.append(prices[i])
        else:
            out.append(sum(prices[i + 1 - period:i + 1]) / period)
    return out


def calc_rsi(prices, period=14):
    if len(prices) <= period:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(prices)):
        d = prices[i] - prices[i - 1]
        gains.append(d if d > 0 else 0)
        losses.append(abs(d) if d < 0 else 0)
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def calc_macd(prices):
    if len(prices) < 35:
        return 0.0, 0.0, 0.0
    ema12 = calc_ema(prices, 12)
    ema26 = calc_ema(prices, 26)
    macd_line = [a - b for a, b in zip(ema12, ema26)]
    signal_line = calc_ema(macd_line, 9)
    hist = macd_line[-1] - signal_line[-1]
    return macd_line[-1], signal_line[-1], hist


def calc_atr_proxy(candles, period=14):
    if len(candles) < period + 1:
        closes = [c["close"] for c in candles]
        if not closes:
            return 0.0
        return max((max(closes) - min(closes)) / max(1, min(len(closes), 5)), closes[-1] * 0.01)

    trs = []
    for i in range(1, len(candles)):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    recent = trs[-period:]
    return sum(recent) / len(recent) if recent else 0.0


def ava_brain_analyze(candles):
    if len(candles) < 60:
        return {
            "signal": "HOLD",
            "conf": 50,
            "regime": "Insufficient Data",
            "reason": "Not enough history to form an advanced AVA opinion.",
            "score": 0
        }

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    current = closes[-1]
    prev_close = closes[-2]

    ema9 = calc_ema(closes, 9)
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    sma50 = calc_sma(closes, 50)

    ema9_now, ema9_prev = ema9[-1], ema9[-2]
    ema20_now, ema20_prev = ema20[-1], ema20[-2]
    ema50_now, ema50_prev = ema50[-1], ema50[-2]
    sma50_now = sma50[-1]

    rsi = calc_rsi(closes, 14)
    macd_line, macd_signal, macd_hist = calc_macd(closes)
    atr = calc_atr_proxy(candles, 14)

    recent_high_10 = max(highs[-10:-1])
    recent_low_10 = min(lows[-10:-1])
    recent_high_20 = max(highs[-20:-1])
    recent_low_20 = min(lows[-20:-1])

    score = 0
    reasons = []

    if current > ema9_now > ema20_now > ema50_now:
        score += 4
        regime = "Strong Bull Trend"
        reasons.append("Price is stacked above EMA-9, EMA-20 and EMA-50 in full bullish alignment.")
    elif current < ema9_now < ema20_now < ema50_now:
        score -= 4
        regime = "Strong Bear Trend"
        reasons.append("Price is stacked below EMA-9, EMA-20 and EMA-50 in full bearish alignment.")
    elif current > ema20_now and current > ema50_now:
        score += 2
        regime = "Bullish Bias"
        reasons.append("Price is holding above key moving averages.")
    elif current < ema20_now and current < ema50_now:
        score -= 2
        regime = "Bearish Bias"
        reasons.append("Price is trading below key moving averages.")
    else:
        regime = "Range / Mixed"
        reasons.append("Trend structure is mixed and less decisive.")

    if ema9_now > ema9_prev:
        score += 1
        reasons.append("Fast trend slope is rising.")
    else:
        score -= 1
        reasons.append("Fast trend slope is fading.")

    if ema20_now > ema20_prev:
        score += 1
    else:
        score -= 1

    if ema50_now > ema50_prev:
        score += 1
    else:
        score -= 1

    if current > sma50_now:
        score += 1
        reasons.append("Price is above the 50-period mean.")
    else:
        score -= 1
        reasons.append("Price is below the 50-period mean.")

    if rsi < 30:
        score += 2
        reasons.append(f"RSI at {rsi:.1f} shows oversold rebound potential.")
    elif rsi < 45:
        score -= 1
        reasons.append(f"RSI at {rsi:.1f} leans weak.")
    elif rsi > 70:
        score -= 1
        reasons.append(f"RSI at {rsi:.1f} is overextended.")
    elif rsi > 55:
        score += 2
        reasons.append(f"RSI at {rsi:.1f} confirms healthy bullish momentum.")
    else:
        reasons.append(f"RSI is neutral at {rsi:.1f}.")

    if macd_line > macd_signal and macd_hist > 0:
        score += 2
        reasons.append("MACD is positive and expanding.")
    elif macd_line < macd_signal and macd_hist < 0:
        score -= 2
        reasons.append("MACD is negative and weakening.")
    else:
        reasons.append("MACD is mixed.")

    if current > recent_high_10 and current > prev_close:
        score += 2
        reasons.append("Fresh breakout above 10-period resistance.")
    elif current < recent_low_10 and current < prev_close:
        score -= 2
        reasons.append("Fresh breakdown below 10-period support.")

    if current > recent_high_20:
        score += 2
        reasons.append("Price is pressing above 20-period structure highs.")
    elif current < recent_low_20:
        score -= 2
        reasons.append("Price is trading below 20-period structure support.")

    if atr > 0:
        volatility_ratio = atr / max(current, 0.000001)
        if volatility_ratio > 0.03:
            reasons.append("Volatility is elevated, increasing both opportunity and risk.")
        else:
            reasons.append("Volatility is controlled and trend-friendly.")

    sig = "BUY" if score >= 6 else "SELL" if score <= -6 else "HOLD"
    conf = min(98, max(50, 54 + abs(score) * 5))

    return {
        "signal": sig,
        "conf": conf,
        "regime": regime,
        "reason": " ".join(reasons),
        "score": score
    }


def get_cached_payload(key, ttl):
    mem = MEM_CACHE.get(key)
    if mem and (int(time.time()) - mem["updated_at"] <= ttl):
        return mem["data"]
    db_payload = db.cache_get(key, ttl)
    if db_payload:
        MEM_CACHE[key] = {"data": db_payload, "updated_at": int(time.time())}
    return db_payload


def set_cached_payload(key, payload):
    now = int(time.time())
    MEM_CACHE[key] = {"data": payload, "updated_at": now}
    db.cache_set(key, payload)


def _perform_crypto_fetch():
    results = []

    try:
        r = requests.get("https://api.gateio.ws/api/v4/spot/tickers", timeout=10)
        if r.status_code == 200:
            market_map = {item.get("currency_pair", "").replace("_USDT", ""): item for item in r.json()}
            for symbol, name in CRYPTO_TOP_90:
                item = market_map.get(symbol)
                if not item:
                    continue
                try:
                    price = float(item.get("last", 0))
                    change = float(item.get("change_percentage", 0))
                    if price > 0:
                        results.append({
                            "symbol": symbol,
                            "name": name,
                            "price": price,
                            "change": change,
                            "dir": "up" if change >= 0 else "down",
                            "signal": compute_light_signal(change),
                            "logo": get_crypto_logo(symbol),
                            "icon": "₿"
                        })
                except Exception:
                    continue
    except Exception as e:
        logger.warning(f"Gate.io fetch failed: {e}")

    if not results:
        try:
            logger.info("Using Bitget fallback for crypto...")
            r = requests.get("https://api.bitget.com/api/v2/spot/market/tickers", timeout=10)
            if r.status_code == 200:
                market_map = {item.get("symbol", "").replace("USDT", ""): item for item in r.json().get("data", [])}
                for symbol, name in CRYPTO_TOP_90:
                    item = market_map.get(symbol)
                    if not item:
                        continue
                    try:
                        price = float(item.get("lastPr", 0))
                        change = float(item.get("change24h", 0)) * 100.0
                        if price > 0:
                            results.append({
                                "symbol": symbol,
                                "name": name,
                                "price": price,
                                "change": change,
                                "dir": "up" if change >= 0 else "down",
                                "signal": compute_light_signal(change),
                                "logo": get_crypto_logo(symbol),
                                "icon": "₿"
                            })
                    except Exception:
                        continue
        except Exception as e:
            logger.warning(f"Bitget fallback failed: {e}")

    if not results:
        logger.error("All crypto APIs failed. Using simulated failsafe data.")
        for symbol, name in CRYPTO_TOP_90:
            price = random.uniform(0.1, 50000.0)
            change = random.uniform(-10.0, 10.0)
            results.append({
                "symbol": symbol,
                "name": name,
                "price": price,
                "change": change,
                "dir": "up" if change >= 0 else "down",
                "signal": compute_light_signal(change),
                "logo": get_crypto_logo(symbol),
                "icon": "₿"
            })

    if results:
        set_cached_payload("crypto_list", results)


def _perform_stock_fetch():
    results = []

    if Config.FINNHUB_API_KEY:
        try:
            for symbol, name in STOCK_UNIVERSE:
                api_symbol = symbol.replace("-", ".")
                r = requests.get("https://finnhub.io/api/v1/quote",
                                 params={"symbol": api_symbol, "token": Config.FINNHUB_API_KEY}, timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    price = float(data.get("c") or 0)
                    prev = float(data.get("pc") or price)
                    if price > 0:
                        change = pct_change(price, prev)
                        results.append({
                            "symbol": symbol,
                            "name": name,
                            "price": price,
                            "change": change,
                            "dir": "up" if change >= 0 else "down",
                            "signal": compute_light_signal(change),
                            "logo": get_stock_logo(symbol),
                            "icon": get_asset_icon(symbol)
                        })
                time.sleep(0.1)
        except Exception as e:
            logger.warning(f"Finnhub fetch failed: {e}")

    if not results:
        headers = {"User-Agent": "Mozilla/5.0"}
        for symbol, name in STOCK_UNIVERSE:
            try:
                r = requests.get(f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d",
                                 headers=headers, timeout=5)
                if r.status_code == 200:
                    res = r.json().get("chart", {}).get("result", [])
                    if res:
                        meta = res[0].get("meta", {})
                        price = float(meta.get("regularMarketPrice", 0))
                        prev = float(meta.get("previousClose", price))
                        if price > 0:
                            change = pct_change(price, prev)
                            results.append({
                                "symbol": symbol,
                                "name": name,
                                "price": price,
                                "change": change,
                                "dir": "up" if change >= 0 else "down",
                                "signal": compute_light_signal(change),
                                "logo": get_stock_logo(symbol),
                                "icon": get_asset_icon(symbol)
                            })
                time.sleep(0.2)
            except Exception:
                continue

    if not results:
        logger.error("All stock APIs failed. Using simulated failsafe data.")
        for symbol, name in STOCK_UNIVERSE:
            price = random.uniform(10.0, 1000.0)
            change = random.uniform(-5.0, 5.0)
            results.append({
                "symbol": symbol,
                "name": name,
                "price": price,
                "change": change,
                "dir": "up" if change >= 0 else "down",
                "signal": compute_light_signal(change),
                "logo": get_stock_logo(symbol),
                "icon": get_asset_icon(symbol)
            })

    if results:
        set_cached_payload("stock_list", results)


def fetch_crypto_quotes_safe():
    return get_cached_payload("crypto_list", Config.CRYPTO_CACHE_TTL) or db.cache_get_stale("crypto_list") or []


def fetch_stock_quotes_safe():
    return get_cached_payload("stock_list", Config.STOCK_CACHE_TTL) or db.cache_get_stale("stock_list") or []


def fetch_crypto_candles(symbol, limit=120):
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/kline",
            params={"category": "spot", "symbol": f"{symbol}USDT", "interval": "60", "limit": limit},
            timeout=5
        )
        if r.status_code == 200 and r.json().get("retCode") == 0:
            raw = r.json().get("result", {}).get("list", [])
            raw.reverse()
            return [{
                "ts": int(c[0]) // 1000,
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4])
            } for c in raw]
    except Exception:
        pass
    return []


def fetch_stock_candles(symbol):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=6mo",
                         headers=headers, timeout=5)
        if r.status_code == 200:
            res = r.json().get("chart", {}).get("result", [])
            if res:
                timestamps = res[0].get("timestamp", [])
                quote = res[0].get("indicators", {}).get("quote", [{}])[0]
                candles = []
                for i in range(len(timestamps)):
                    try:
                        if quote.get("close")[i] is not None:
                            candles.append({
                                "ts": timestamps[i],
                                "open": float(quote["open"][i]),
                                "high": float(quote["high"][i]),
                                "low": float(quote["low"][i]),
                                "close": float(quote["close"][i])
                            })
                    except Exception:
                        continue
                return candles[-140:]
    except Exception:
        pass
    return []


def build_trade_setup(asset, candles, asset_type):
    if not candles or len(candles) < 60:
        return None

    brain = ava_brain_analyze(candles)
    current = float(candles[-1]["close"])
    atr = max(calc_atr_proxy(candles, 14), current * 0.008)

    highs = [c["high"] for c in candles[-20:]]
    lows = [c["low"] for c in candles[-20:]]
    recent_high = max(highs)
    recent_low = min(lows)

    signal = brain["signal"]
    if signal == "HOLD":
        return None

    if signal == "BUY":
        entry = current
        stop = max(current - atr * 1.25, recent_low * 0.995)
        if stop >= entry:
            stop = current - atr
        risk = max(entry - stop, 0.000001)
        tp1 = entry + risk * 1.6
        tp2 = entry + risk * 2.8
        rr = (tp1 - entry) / risk
    else:
        entry = current
        stop = min(current + atr * 1.25, recent_high * 1.005)
        if stop <= entry:
            stop = current + atr
        risk = max(stop - entry, 0.000001)
        tp1 = entry - risk * 1.6
        tp2 = entry - risk * 2.8
        rr = (entry - tp1) / risk

    if stop <= 0 or tp1 <= 0 or tp2 <= 0:
        return None

    return {
        "signal_id": f"{asset_type}:{asset['symbol']}",
        "symbol": asset["symbol"],
        "asset_type": asset_type,
        "name": asset["name"],
        "signal": signal,
        "confidence": int(brain["conf"]),
        "regime": brain["regime"],
        "entry_price": round(entry, 8),
        "stop_loss": round(stop, 8),
        "take_profit_1": round(tp1, 8),
        "take_profit_2": round(tp2, 8),
        "risk_reward": round(rr, 2),
        "reason": brain["reason"],
        "price": round(float(asset["price"]), 8),
        "change_pct": round(float(asset["change"]), 4),
        "updated_at": int(time.time())
    }


def evaluate_signal_history_outcomes():
    history = db.get_signal_history(limit=200)
    active_by_symbol = {}
    for a in fetch_crypto_quotes_safe():
        active_by_symbol[f"crypto:{a['symbol']}"] = float(a["price"])
    for a in fetch_stock_quotes_safe():
        active_by_symbol[f"stock:{a['symbol']}"] = float(a["price"])

    c = db.conn()
    now = int(time.time())
    for row in history:
        if row["outcome"] != "OPEN":
            continue

        key = row["signal_id"]
        live_price = active_by_symbol.get(key)
        if live_price is None:
            continue

        signal = row["signal"]
        entry = float(row["entry_price"])
        stop = float(row["stop_loss"])
        tp1 = float(row["take_profit_1"])
        tp2 = float(row["take_profit_2"])

        outcome = None
        note = ""

        if signal == "BUY":
            if live_price <= stop:
                outcome = "STOPPED"
                note = "Price moved below stop level."
            elif live_price >= tp2:
                outcome = "TP2_HIT"
                note = "Price reached second target."
            elif live_price >= tp1:
                outcome = "TP1_HIT"
                note = "Price reached first target."
        elif signal == "SELL":
            if live_price >= stop:
                outcome = "STOPPED"
                note = "Price moved above stop level."
            elif live_price <= tp2:
                outcome = "TP2_HIT"
                note = "Price reached second target."
            elif live_price <= tp1:
                outcome = "TP1_HIT"
                note = "Price reached first target."

        if outcome:
            c.execute("""
                UPDATE signal_history
                SET outcome = ?, outcome_note = ?, updated_at = ?
                WHERE history_id = ?
            """, (outcome, note, now, row["history_id"]))

    c.commit()
    c.close()


def generate_active_signals():
    signals = []

    crypto_assets = fetch_crypto_quotes_safe()[:24]
    for asset in crypto_assets:
        try:
            candles = fetch_crypto_candles(asset["symbol"], limit=120)
            setup = build_trade_setup(asset, candles, "crypto")
            if setup and setup["confidence"] >= Config.SIGNAL_MIN_CONFIDENCE and setup["risk_reward"] >= Config.SIGNAL_MIN_RR:
                signals.append(setup)
        except Exception as e:
            logger.warning(f"Signal generation failed for crypto {asset.get('symbol')}: {e}")

    stock_assets = fetch_stock_quotes_safe()[:18]
    for asset in stock_assets:
        try:
            candles = fetch_stock_candles(asset["symbol"])
            setup = build_trade_setup(asset, candles, "stock")
            if setup and setup["confidence"] >= Config.SIGNAL_MIN_CONFIDENCE and setup["risk_reward"] >= Config.SIGNAL_MIN_RR:
                signals.append(setup)
        except Exception as e:
            logger.warning(f"Signal generation failed for stock {asset.get('symbol')}: {e}")

    signals.sort(key=lambda x: (x["confidence"], x["risk_reward"]), reverse=True)
    signals = signals[:25]
    db.replace_active_signals(signals)
    db.sync_signal_history(signals)
    evaluate_signal_history_outcomes()
    logger.info(f"Generated {len(signals)} active AVA signals.")
    return signals


def draw_candles_html(candles):
    if not candles:
        return "<div class='candle-container' style='justify-content:center; align-items:center; color:#cbd5e1;'>No chart data available.</div>"

    sample = candles[-30:]
    highs = [c["high"] for c in sample]
    lows = [c["low"] for c in sample]
    max_h, min_l = max(highs), min(lows)
    span = max(max_h - min_l, 0.000001)

    html_parts = ["<div class='candle-container'>"]
    for c in sample:
        h_pct = (max_h - c["high"]) / span * 100
        l_pct = (max_h - c["low"]) / span * 100
        o_pct = (max_h - c["open"]) / span * 100
        close_pct = (max_h - c["close"]) / span * 100

        top = min(o_pct, close_pct)
        height = max(abs(o_pct - close_pct), 1.5)
        color_class = "c-up" if c["close"] >= c["open"] else "c-down"

        html_parts.append(f"""
        <div class="candle {color_class}">
          <div class="wick" style="top:{h_pct}%; bottom:{100-l_pct}%;"></div>
          <div class="body" style="top:{top}%; height:{height}%;"></div>
        </div>
        """)
    html_parts.append("</div>")
    return "".join(html_parts)


def paginate(items, page, per_page):
    if not items:
        return [], 0, 1, 1
    total = len(items)
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    return items[start:start + per_page], total, pages, page


def render_pagination(base_url, current, pages):
    if pages <= 1:
        return ""
    parts = ["<div class='pagination'>"]
    for p in range(1, pages + 1):
        parts.append(f"<a class='page-link' href='{h(base_url)}?page={p}'>{p}</a>")
    parts.append("</div>")
    return "".join(parts)


def subscription_label(user):
    if not user:
        return "Guest"
    tier = user.get("tier", "free")
    billing = user.get("billing_cycle") or ""
    if tier == "free":
        return "Free"
    return f"{tier.title()} {billing.title()}".strip()


def tier_badge_html(user):
    if not user:
        return "<span class='pill'>Guest</span>"
    tier = user.get("tier", "free")
    if tier == "elite":
        return "<span class='pill' style='background:rgba(250,204,21,.14);border-color:rgba(250,204,21,.25);color:#fde68a;'>Elite</span>"
    if tier == "pro":
        return "<span class='pill' style='background:rgba(56,189,248,.12);border-color:rgba(56,189,248,.22);color:#bae6fd;'>Pro</span>"
    return "<span class='pill'>Free</span>"


def top_signal_cards_html(signals, blurred=False):
    cards = ""
    for s in signals[:3]:
        signal_class = s["signal"].lower()
        link = f"/crypto/{h(s['symbol'])}" if s["asset_type"] == "crypto" else f"/stocks/{h(s['symbol'])}"
        inner = f"""
        <div class="metric-box">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
            <div>
              <div style="font-weight:900;font-size:1.05rem;"><a href="{link}">{h(s['symbol'])}</a></div>
              <div class="small">{h(s['name'])} • {h(s['asset_type'].upper())}</div>
            </div>
            <span class="signal signal-{signal_class}">{h(s['signal'])}</span>
          </div>
          <div class="hr"></div>
          <div class="grid-2" style="gap:10px;">
            <div><div class="small">Entry</div><strong>{fmt_price(s['entry_price'])}</strong></div>
            <div><div class="small">Stop</div><strong>{fmt_price(s['stop_loss'])}</strong></div>
            <div><div class="small">TP1</div><strong>{fmt_price(s['take_profit_1'])}</strong></div>
            <div><div class="small">Confidence</div><strong>{int(s['confidence'])}%</strong></div>
          </div>
        </div>
        """
        cards += inner

    if not blurred:
        return f"<div class='grid-3'>{cards or '<p class=\"muted\">No active signals yet.</p>'}</div>"

    return f"""
    <div class="blur-lock">
      <div class="blurred">
        <div class="grid-3">{cards or '<p class="muted">No active signals yet.</p>'}</div>
      </div>
      <div class="blur-overlay">
        <div class="lock-card">
          <div class="badge" style="margin-bottom:10px;">Premium Locked</div>
          <h3 style="margin-bottom:8px;">Unlock Live AVA Trade Setups</h3>
          <p style="margin-bottom:16px;">See exact entries, stops, targets, confidence, and full market regime analysis.</p>
          <a href="/pricing" class="btn btn-primary">Unlock Pro</a>
        </div>
      </div>
    </div>
    """


def render_footer():
    return """
    <div class="footer-top">
      <div>
        <div class="logo">AVA Markets</div>
        <div class="small" style="margin-top:8px;">Aurora-grade market intelligence for crypto, stocks, and macro traders.</div>
      </div>
      <div class="disclaimer">
        AVA Markets provides market intelligence, algorithmic trade ideas, and educational analysis only.
        Nothing on this website constitutes financial advice, investment advice, or a solicitation to buy or sell any security,
        derivative, or digital asset. Trading and investing involve substantial risk, including possible loss of capital.
        Always do your own research and consult a licensed financial professional where appropriate.
      </div>
    </div>
    <div class="footer">AVA Markets © 2026 — Built for systematic traders.</div>
    """


def nav_layout(title, content):
    if g.user:
        user_nav = f"""
        <a href="/dashboard">Dashboard</a>
        <a href="/logout">Logout</a>
        """
    else:
        user_nav = """
        <a href="/login">Login</a>
        <a href="/register">Register</a>
        """

    admin_link = '<a href="/admin">Admin</a>' if is_admin() else ""

    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>{{ title }}</title>
      <style>{{ css }}</style>
    </head>
    <body>
      <div class="container">
        <nav class="nav">
          <div class="logo"><a href="/">AVA Markets</a></div>
          <div class="nav-links">
            <a href="/">Home</a>
            <a href="/crypto">Crypto</a>
            <a href="/stocks">Stocks</a>
            <a href="/signals">Signals</a>
            <a href="/history">History</a>
            <a href="/pricing">Pricing</a>
            {{ admin_link|safe }}
            {{ user_nav|safe }}
          </div>
        </nav>
        {{ content|safe }}
        {{ footer|safe }}
      </div>
    </body>
    </html>
    """, title=title, css=CSS, content=content, user_nav=user_nav, admin_link=admin_link, footer=render_footer())


def live_update_script(page_type):
    return f"""<script>
    setInterval(async () => {{
        try {{
            const res = await fetch('/api/live/{page_type}-list');
            const data = await res.json();
            const stamp = document.getElementById('live-updated');
            if(stamp) stamp.textContent = 'Last updated: ' + data.updated_at + ' UTC';

            data.items.forEach(item => {{
                let safe_id = item.symbol.replace(/[^A-Za-z0-9]/g, '_');
                let p = document.getElementById('price-' + safe_id);
                if(p) p.textContent = item.price_display;
                let c = document.getElementById('change-' + safe_id);
                if(c) {{
                    c.textContent = item.change_display;
                    c.className = item.dir;
                }}
                let s = document.getElementById('signal-' + safe_id);
                if(s) {{
                    s.textContent = item.signal;
                    s.className = 'signal signal-' + item.signal.toLowerCase();
                }}
            }});
        }} catch(e) {{}}
    }}, 30000);
    </script>"""


def send_telegram_message(text):
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": Config.TELEGRAM_CHAT_ID, "text": text},
            timeout=8
        )
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
        return False


def send_discord_message(text):
    if not Config.DISCORD_WEBHOOK_URL:
        return False
    try:
        r = requests.post(Config.DISCORD_WEBHOOK_URL, json={"content": text}, timeout=8)
        return r.status_code in (200, 204)
    except Exception as e:
        logger.warning(f"Discord send failed: {e}")
        return False


def build_top_signals_broadcast(signals):
    if not signals:
        return None
    top = signals[:3]
    lines = ["🚀 AVA Top Signals"]
    for s in top:
        lines.append(
            f"{s['symbol']} | {s['signal']} | Entry {fmt_price(s['entry_price'])} | TP1 {fmt_price(s['take_profit_1'])} | Conf {s['confidence']}%"
        )
    return "\n".join(lines)


def maybe_broadcast_top_signals(signals):
    text = build_top_signals_broadcast(signals)
    if not text:
        return
    send_telegram_message(text)
    send_discord_message(text)


@app.route("/api/live/crypto-list")
def api_live_crypto():
    assets = fetch_crypto_quotes_safe()
    items = [{
        "symbol": a.get("symbol", ""),
        "price_display": fmt_price(a.get("price", 0)),
        "change_display": fmt_change(a.get("change", 0)),
        "dir": a.get("dir", "down"),
        "signal": a.get("signal", "HOLD")
    } for a in assets]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})


@app.route("/api/live/stocks-list")
def api_live_stocks():
    assets = fetch_stock_quotes_safe()
    items = [{
        "symbol": a.get("symbol", ""),
        "price_display": fmt_price(a.get("price", 0), a.get("symbol")),
        "change_display": fmt_change(a.get("change", 0)),
        "dir": a.get("dir", "down"),
        "signal": a.get("signal", "HOLD")
    } for a in assets]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})


@app.route("/api/active-signals")
@require_tier("pro")
def api_active_signals():
    asset_type = (request.args.get("type") or "").strip().lower()
    if asset_type not in ("crypto", "stock", ""):
        asset_type = ""
    signals = db.get_active_signals(asset_type=asset_type or None, limit=50)
    return jsonify({
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(signals),
        "items": signals
    })


@app.route("/", methods=["GET"])
def home():
    signals = db.get_active_signals(limit=6)
    stats = db.get_signal_stats()
    user_count = db.get_user_count()
    subs = db.get_subscriber_count()

    if g.user and g.user.get("tier") in ("pro", "elite"):
        signals_section = top_signal_cards_html(signals[:3], blurred=False)
    else:
        signals_section = top_signal_cards_html(signals[:3], blurred=True)

    content = f"""
    <section class="hero">
      <div class="hero-card">
        <div class="badge">AVA Super Brain • Aurora Edition</div>
        <h1>Trade cleaner. Scan faster. Move with conviction.</h1>
        <p class="hero-sub">
          AVA Markets surfaces ranked crypto and stock trade setups with entries, stops, targets,
          confidence scoring, regime detection, and premium signal intelligence.
        </p>
        <div class="btns">
          <a class="btn btn-primary" href="/pricing">Unlock Pro</a>
          <a class="btn btn-secondary" href="/signals">View Signals</a>
          <a class="btn btn-dark" href="/crypto">Explore Markets</a>
        </div>
        <div class="grid-4" style="margin-top:24px;">
          <div class="kpi"><div class="num">{stats['total']}</div><div class="label">Signals Recorded</div></div>
          <div class="kpi"><div class="num">{stats['win_rate']}%</div><div class="label">Tracked Win Rate</div></div>
          <div class="kpi"><div class="num">{user_count}</div><div class="label">Registered Users</div></div>
          <div class="kpi"><div class="num">{subs}</div><div class="label">Alert Subscribers</div></div>
        </div>
      </div>

      <div class="card hero-mini-chart">
        <div>
          <div class="badge">Top Setups Now</div>
          <h2>Premium AVA Trade Feed</h2>
          <p>Highest conviction setups ranked by confidence and risk/reward.</p>
        </div>
        {signals_section}
      </div>
    </section>

    <section class="section">
      <div class="grid-3">
        <div class="card">
          <div class="badge">Live Scan</div>
          <h3>Crypto + Stocks</h3>
          <p>Track liquid assets across digital and traditional markets from one dashboard.</p>
        </div>
        <div class="card">
          <div class="badge">Ranked Setups</div>
          <h3>Entry, Stop, Targets</h3>
          <p>AVA transforms raw chart structure into systematic trade setups with actionable levels.</p>
        </div>
        <div class="card">
          <div class="badge">Signal Quality</div>
          <h3>Confidence + Regime</h3>
          <p>Trend stack, momentum, breakout structure, MACD, RSI, and volatility all feed the scoring engine.</p>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="card">
        <div class="badge">Pricing</div>
        <h2>Built for serious traders, priced for fast adoption.</h2>
        <div class="grid-3" style="margin-top:20px;">
          <div class="price-card">
            <div class="pill">Free</div>
            <div class="price">$0</div>
            <p>Get market dashboards, light signals, and premium previews.</p>
            <ul style="line-height:2;color:var(--text);">
              <li>Live crypto + stock lists</li>
              <li>Basic signal labels</li>
              <li>Homepage premium previews</li>
              <li>Email alert signup</li>
            </ul>
            <a class="btn btn-secondary" style="width:100%;" href="/register">Start Free</a>
          </div>

          <div class="price-card featured">
            <div class="pill" style="background:rgba(56,189,248,.12);border-color:rgba(56,189,248,.2);color:#bae6fd;">Best to Start</div>
            <div class="price">$19<span class="small">/mo</span></div>
            <p>Unlock the full AVA trading workflow with active signal trades and analysis.</p>
            <ul style="line-height:2;color:var(--text);">
              <li>Full Active Signal Trades</li>
              <li>AVA Brain detail pages</li>
              <li>Signal history + win-rate</li>
              <li>Trade setup levels</li>
            </ul>
            <a class="btn btn-primary" style="width:100%;" href="/pricing">Get Pro</a>
          </div>

          <div class="price-card">
            <div class="pill" style="background:rgba(250,204,21,.14);border-color:rgba(250,204,21,.24);color:#fde68a;">Elite</div>
            <div class="price">$49<span class="small">/mo</span></div>
            <p>For power users who want priority access, premium alerts, and expansion features.</p>
            <ul style="line-height:2;color:var(--text);">
              <li>Everything in Pro</li>
              <li>Telegram / Discord alert support</li>
              <li>Highest conviction workflow</li>
              <li>Future elite features first</li>
            </ul>
            <a class="btn btn-secondary" style="width:100%;" href="/pricing">Go Elite</a>
          </div>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="grid-2">
        <div class="card">
          <div class="badge">Win-Rate Transparency</div>
          <h2>AVA tracks outcomes over time.</h2>
          <p>
            We don’t just show shiny setups. AVA stores signal history, tracks outcomes,
            and computes performance so the product can improve and users can trust what they see.
          </p>
          <a href="/history" class="btn btn-secondary">View History</a>
        </div>

        <div class="card">
          <div class="badge">Free Alerts</div>
          <h2>Join the AVA alert list.</h2>
          <p>Get notified when major AVA setups, product upgrades, and premium launches go live.</p>
          <form method="POST" action="/subscribe-alerts">
            <input type="email" name="email" placeholder="Enter your email" required style="width:100%;padding:14px 16px;margin:10px 0;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:var(--text);outline:none;">
            <button class="btn btn-primary" type="submit">Subscribe for Alerts</button>
          </form>
        </div>
      </div>
    </section>
    """
    return nav_layout("AVA Markets — Aurora Intelligence", content)


@app.route("/subscribe-alerts", methods=["POST"])
def subscribe_alerts():
    email = request.form.get("email", "").strip().lower()
    if not email or "@" not in email:
        return nav_layout("Alert Signup", """
        <section class="section"><div class="card"><div class="error">Please enter a valid email address.</div><a href="/" class="btn btn-secondary">Go Back</a></div></section>
        """)
    db.subscribe_email(email)
    return nav_layout("Alert Signup", f"""
    <section class="section">
      <div class="card">
        <div class="success">You’re subscribed, mate. AVA will keep you posted.</div>
        <p>We’ll use this list for product updates, top setups, and future premium alert campaigns.</p>
        <a href="/" class="btn btn-primary">Back to Home</a>
      </div>
    </section>
    """)


@app.route("/crypto")
def crypto():
    try:
        page = int(request.args.get("page", 1))
    except Exception:
        page = 1

    search = (request.args.get("q") or "").strip().lower()
    assets = fetch_crypto_quotes_safe()
    if search:
        assets = [a for a in assets if search in str(a.get("symbol", "")).lower() or search in str(a.get("name", "")).lower()]

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_CRYPTO)

    rows = ""
    for a in page_items:
        safe_id = h(normalize_symbol_id(a.get("symbol", "")))
        fallback = f"<span class='asset-icon' style='display:none;'>{h(a.get('icon', '₿'))}</span>"
        media = f'<img class="asset-logo" src="{h(a.get("logo", ""))}" onerror="this.style.display=\'none\'; this.nextElementSibling.style.display=\'inline-flex\';">{fallback}'
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">{media}<a href="/crypto/{h(a.get('symbol',''))}">{h(a.get('symbol',''))}</a></strong>
            <span>{h(a.get('name',''))}</span>
          </td>
          <td id="price-{safe_id}">{fmt_price(a.get("price",0))}</td>
          <td id="change-{safe_id}" class="{a.get('dir','down')}">{fmt_change(a.get("change",0))}</td>
          <td><span id="signal-{safe_id}" class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Live Market Feed</div>
      <h1>Crypto Markets</h1>
      <div id="live-updated" class="small" style="margin-bottom:20px;">Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>24h</th><th>AVA Signal</th></tr>
          {rows or "<tr><td colspan='4'>Cache warming up. Refresh in 10 seconds.</td></tr>"}
        </table>
      </div>
      {render_pagination('/crypto', current, pages)}
      <div class="small" style="margin-top:12px;">{total} crypto assets loaded</div>
    </section>
    {live_update_script("crypto")}
    """
    return nav_layout("Crypto — AVA", content)


@app.route("/stocks")
def stocks():
    try:
        page = int(request.args.get("page", 1))
    except Exception:
        page = 1

    search = (request.args.get("q") or "").strip().lower()
    assets = fetch_stock_quotes_safe()
    if search:
        assets = [a for a in assets if search in str(a.get("symbol", "")).lower() or search in str(a.get("name", "")).lower()]

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_STOCKS)

    rows = ""
    for a in page_items:
        safe_id = h(normalize_symbol_id(a.get("symbol", "")))
        fallback = f"<span class='asset-icon' style='display:none;'>{h(a.get('icon', '📈'))}</span>"
        media = f'<img class="asset-logo" src="{h(a.get("logo", ""))}" onerror="this.style.display=\'none\'; this.nextElementSibling.style.display=\'inline-flex\';">{fallback}' if a.get("logo") else f"<span class='asset-icon'>{h(a.get('icon','📈'))}</span>"
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">{media}<a href="/stocks/{h(a.get('symbol',''))}">{h(a.get('symbol',''))}</a></strong>
            <span>{h(a.get('name',''))}</span>
          </td>
          <td id="price-{safe_id}">{fmt_price(a.get("price",0), a.get("symbol"))}</td>
          <td id="change-{safe_id}" class="{a.get('dir','down')}">{fmt_change(a.get("change",0))}</td>
          <td><span id="signal-{safe_id}" class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Live Market Feed</div>
      <h1>Stocks & Commodities</h1>
      <div id="live-updated" class="small" style="margin-bottom:20px;">Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>AVA Signal</th></tr>
          {rows or "<tr><td colspan='4'>Cache warming up. Refresh in 10 seconds.</td></tr>"}
        </table>
      </div>
      {render_pagination('/stocks', current, pages)}
      <div class="small" style="margin-top:12px;">{total} stock/commodity assets loaded</div>
    </section>
    {live_update_script("stocks")}
    """
    return nav_layout("Stocks — AVA", content)


@app.route("/signals")
def signals():
    asset_type = (request.args.get("type") or "").strip().lower()
    if asset_type not in ("crypto", "stock", ""):
        asset_type = ""

    signals = db.get_active_signals(asset_type=asset_type or None, limit=100)

    if not g.user or g.user.get("tier", "free") == "free":
        content = f"""
        <section class="section">
          <div class="badge">Premium Signal Center</div>
          <h1>Active Signal Trades</h1>
          <p>Unlock exact entries, stops, targets, confidence scoring and premium AVA trade intelligence.</p>
          {top_signal_cards_html(signals[:3], blurred=True)}
          <div class="card" style="margin-top:24px;">
            <h3>What unlocks in Pro?</h3>
            <ul style="line-height:2; color:var(--text);">
              <li>Full ranked signal table</li>
              <li>Signal history + win-rate tracking</li>
              <li>Detail pages with AVA Brain explanations</li>
              <li>Trade levels and regime classification</li>
            </ul>
            <a href="/pricing" class="btn btn-primary">Unlock Pro</a>
          </div>
        </section>
        """
        return nav_layout("Signals — AVA", content)

    rows = ""
    for s in signals:
        link = f"/crypto/{h(s['symbol'])}" if s["asset_type"] == "crypto" else f"/stocks/{h(s['symbol'])}"
        rows += f"""
        <tr>
          <td><strong><a href="{link}">{h(s['symbol'])}</a></strong><div class="small">{h(s['name'])}</div></td>
          <td>{h(s['asset_type'].upper())}</td>
          <td><span class="signal signal-{h(s['signal'].lower())}">{h(s['signal'])}</span></td>
          <td>{int(s['confidence'])}%</td>
          <td>{fmt_price(s['entry_price'])}</td>
          <td>{fmt_price(s['stop_loss'])}</td>
          <td>{fmt_price(s['take_profit_1'])}</td>
          <td>{fmt_price(s['take_profit_2'])}</td>
          <td>{h(s['risk_reward'])}:1</td>
        </tr>
        """

    elite_box = ""
    if g.user.get("tier") == "elite":
        elite_box = """
        <div class="alert-box" style="margin-bottom:18px;">
          Elite tier active: Telegram / Discord alert channels can be used for top signal broadcasting.
        </div>
        """

    content = f"""
    <section class="section">
      <div class="badge">AVA Super Brain</div>
      <h1>Active Signal Trades</h1>
      <p>Live algorithmic setups ranked by AVA confidence and risk/reward profile.</p>
      {elite_box}
      <div class="btns" style="margin-bottom:20px;">
        <a class="btn btn-secondary" href="/signals">All</a>
        <a class="btn btn-secondary" href="/signals?type=crypto">Crypto</a>
        <a class="btn btn-secondary" href="/signals?type=stock">Stocks</a>
      </div>
      <div class="table-shell">
        <table class="market-table">
          <tr>
            <th>Asset</th>
            <th>Type</th>
            <th>Signal</th>
            <th>Confidence</th>
            <th>Entry</th>
            <th>Stop</th>
            <th>TP1</th>
            <th>TP2</th>
            <th>R:R</th>
          </tr>
          {rows or "<tr><td colspan='9'>No active signals available yet.</td></tr>"}
        </table>
      </div>
    </section>
    """
    return nav_layout("Active Signal Trades — AVA", content)


@app.route("/history")
def history():
    stats = db.get_signal_stats()

    if not g.user or g.user.get("tier", "free") == "free":
        content = f"""
        <section class="section">
          <div class="badge">Premium History</div>
          <h1>Signal History & Win Rate</h1>
          <div class="grid-4" style="margin-bottom:20px;">
            <div class="kpi"><div class="num">{stats['total']}</div><div class="label">Signals Logged</div></div>
            <div class="kpi"><div class="num">{stats['open']}</div><div class="label">Open</div></div>
            <div class="kpi"><div class="num">{stats['tp1_hit'] + stats['tp2_hit']}</div><div class="label">Tracked Wins</div></div>
            <div class="kpi"><div class="num">{stats['win_rate']}%</div><div class="label">Win Rate</div></div>
          </div>
          <div class="card">
            <p>Unlock the full signal history table, performance tracking, and detailed trade outcomes with Pro.</p>
            <a href="/pricing" class="btn btn-primary">Unlock Pro</a>
          </div>
        </section>
        """
        return nav_layout("Signal History — AVA", content)

    rows = ""
    for r in db.get_signal_history(limit=120):
        rows += f"""
        <tr>
          <td><strong>{h(r['symbol'])}</strong><div class="small">{h(r['name'])}</div></td>
          <td>{h(r['asset_type'].upper())}</td>
          <td><span class="signal signal-{h(r['signal'].lower())}">{h(r['signal'])}</span></td>
          <td>{int(r['confidence'])}%</td>
          <td>{fmt_price(r['entry_price'])}</td>
          <td>{fmt_price(r['take_profit_1'])}</td>
          <td>{fmt_price(r['take_profit_2'])}</td>
          <td>{h(r['outcome'])}</td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Performance Transparency</div>
      <h1>Signal History & Win Rate</h1>
      <div class="grid-4" style="margin-bottom:20px;">
        <div class="kpi"><div class="num">{stats['total']}</div><div class="label">Signals Logged</div></div>
        <div class="kpi"><div class="num">{stats['open']}</div><div class="label">Open</div></div>
        <div class="kpi"><div class="num">{stats['tp1_hit'] + stats['tp2_hit']}</div><div class="label">Tracked Wins</div></div>
        <div class="kpi"><div class="num">{stats['win_rate']}%</div><div class="label">Win Rate</div></div>
      </div>
      <div class="table-shell">
        <table class="market-table">
          <tr>
            <th>Asset</th>
            <th>Type</th>
            <th>Signal</th>
            <th>Confidence</th>
            <th>Entry</th>
            <th>TP1</th>
            <th>TP2</th>
            <th>Outcome</th>
          </tr>
          {rows or "<tr><td colspan='8'>No signal history yet.</td></tr>"}
        </table>
      </div>
    </section>
    """
    return nav_layout("History — AVA", content)


@app.route("/crypto/<symbol>")
@require_tier("pro")
def crypto_detail(symbol):
    symbol = symbol.upper()
    assets = fetch_crypto_quotes_safe()
    asset = next((a for a in assets if a.get("symbol") == symbol), None)
    if not asset:
        abort(404)

    candles = fetch_crypto_candles(symbol)
    brain = ava_brain_analyze(candles)

    elite_hint = ""
    if g.user.get("tier") == "elite":
        elite_hint = "<div class='alert-box' style='margin-bottom:20px;'>Elite mode active: top-signal alert infrastructure is enabled for premium expansion flows.</div>"

    content = f"""
    <section class="section">
      <div class="badge">Premium Intelligence</div>
      <h1>{h(asset['name'])} ({symbol})</h1>
      <div style="font-size:2.4rem;font-weight:900;margin-bottom:20px;">
        {fmt_price(asset['price'])}
        <span class="{asset['dir']}" style="font-size:1.15rem;">{fmt_change(asset['change'])}</span>
      </div>
      {elite_hint}
      <div class="grid-2" style="margin-bottom:24px;">
        <div class="card">
          <h3>AVA Brain Verdict</h3>
          <p><strong>{brain['signal']}</strong> signal with <strong>{brain['conf']}%</strong> confidence.</p>
          <div style="margin-top:12px;"><span class="signal signal-{brain['signal'].lower()}">{brain['signal']}</span></div>
        </div>
        <div class="card">
          <h3>Market Regime</h3>
          <p><strong>{brain['regime']}</strong></p>
          <p class="small">{brain['reason']}</p>
        </div>
      </div>
      <h2>Price Action (1H)</h2>
      <div class="card">{draw_candles_html(candles)}</div>
    </section>
    """
    return nav_layout(f"{symbol} — AVA", content)


@app.route("/stocks/<symbol>")
@require_tier("pro")
def stock_detail(symbol):
    symbol = symbol.upper()
    assets = fetch_stock_quotes_safe()
    asset = next((a for a in assets if a.get("symbol") == symbol), None)
    if not asset:
        abort(404)

    candles = fetch_stock_candles(symbol)
    brain = ava_brain_analyze(candles)

    elite_hint = ""
    if g.user.get("tier") == "elite":
        elite_hint = "<div class='alert-box' style='margin-bottom:20px;'>Elite mode active: premium alert and top-conviction workflow is enabled.</div>"

    content = f"""
    <section class="section">
      <div class="badge">Premium Intelligence</div>
      <h1>{h(asset['name'])} ({symbol})</h1>
      <div style="font-size:2.4rem;font-weight:900;margin-bottom:20px;">
        {fmt_price(asset['price'], symbol)}
        <span class="{asset['dir']}" style="font-size:1.15rem;">{fmt_change(asset['change'])}</span>
      </div>
      {elite_hint}
      <div class="grid-2" style="margin-bottom:24px;">
        <div class="card">
          <h3>AVA Brain Verdict</h3>
          <p><strong>{brain['signal']}</strong> signal with <strong>{brain['conf']}%</strong> confidence.</p>
          <div style="margin-top:12px;"><span class="signal signal-{brain['signal'].lower()}">{brain['signal']}</span></div>
        </div>
        <div class="card">
          <h3>Market Regime</h3>
          <p><strong>{brain['regime']}</strong></p>
          <p class="small">{brain['reason']}</p>
        </div>
      </div>
      <h2>Price Action (Daily)</h2>
      <div class="card">{draw_candles_html(candles)}</div>
    </section>
    """
    return nav_layout(f"{symbol} — AVA", content)


@app.route("/register", methods=["GET", "POST"])
def register():
    if g.user:
        return redirect("/dashboard")
    err = ""
    if request.method == "POST":
        e = request.form.get("email", "").strip()
        p = request.form.get("password", "").strip()
        if len(p) < 6:
            err = "Password must be at least 6 characters."
        else:
            u = db.create_user(e, p)
            if not u:
                err = "Email already exists."
            else:
                resp = make_response(redirect("/dashboard"))
                resp.set_cookie("session_token", db.create_session(u["id"]), httponly=True, secure=Config.COOKIE_SECURE, samesite="Lax")
                return resp

    content = f"""
    <div class="form-shell">
      <div class="form-card">
        <div class="badge">Create Account</div>
        <h2>Start with AVA</h2>
        {f"<div class='error'>{err}</div>" if err else ""}
        <form method="POST">
          <input type="email" name="email" placeholder="Email" required>
          <input type="password" name="password" placeholder="Password" required>
          <button type="submit">Create Account</button>
        </form>
      </div>
    </div>
    """
    return nav_layout("Register — AVA", content)


@app.route("/login", methods=["GET", "POST"])
def login():
    if g.user:
        return redirect("/dashboard")
    err = ""
    if request.method == "POST":
        u = db.verify_user(request.form.get("email", ""), request.form.get("password", ""))
        if not u:
            err = "Invalid credentials."
        else:
            resp = make_response(redirect("/dashboard"))
            resp.set_cookie("session_token", db.create_session(u["id"]), httponly=True, secure=Config.COOKIE_SECURE, samesite="Lax")
            return resp

    content = f"""
    <div class="form-shell">
      <div class="form-card">
        <div class="badge">Welcome Back</div>
        <h2>Login</h2>
        {f"<div class='error'>{err}</div>" if err else ""}
        <form method="POST">
          <input type="email" name="email" placeholder="Email" required>
          <input type="password" name="password" placeholder="Password" required>
          <button type="submit">Login</button>
        </form>
      </div>
    </div>
    """
    return nav_layout("Login — AVA", content)


@app.route("/logout")
def logout():
    resp = make_response(redirect("/"))
    resp.delete_cookie("session_token")
    return resp


@app.route("/dashboard")
@require_auth
def dashboard():
    u = g.user
    signals = db.get_active_signals(limit=6)

    signal_cards = ""
    for s in signals:
        link = f"/crypto/{h(s['symbol'])}" if s["asset_type"] == "crypto" else f"/stocks/{h(s['symbol'])}"
        signal_cards += f"""
        <div class="metric-box">
          <div style="display:flex;justify-content:space-between;align-items:center;">
            <strong><a href="{link}">{h(s['symbol'])}</a></strong>
            <span class="signal signal-{h(s['signal'].lower())}">{h(s['signal'])}</span>
          </div>
          <div class="small" style="margin-top:8px;">{h(s['name'])}</div>
          <div style="margin-top:10px;">Entry: <strong>{fmt_price(s['entry_price'])}</strong></div>
          <div>TP1: <strong>{fmt_price(s['take_profit_1'])}</strong></div>
          <div>Conf: <strong>{int(s['confidence'])}%</strong></div>
        </div>
        """

    upgrade_box = ""
    if u["tier"] == "free":
        upgrade_box = """
        <div class="alert-box" style="margin-bottom:16px;">
          You’re on Free. Upgrade to unlock full active signals, history, and AVA Brain detail pages.
        </div>
        """
    elif u["tier"] == "pro":
        upgrade_box = """
        <div class="alert-box" style="margin-bottom:16px;">
          You’re on Pro. Upgrade to Elite for premium alert flows and expansion features.
        </div>
        """

    content = f"""
    <section class="section">
      <div class="badge">Account Hub</div>
      <h1>Dashboard</h1>
      {upgrade_box}
      <div class="grid-2" style="margin-bottom:20px;">
        <div class="card">
          <h3>Account Details</h3>
          <p><strong>Email:</strong> {h(u['email'])}</p>
          <p><strong>Plan:</strong> {tier_badge_html(u)}</p>
          <p><strong>Subscription:</strong> {h(subscription_label(u))}</p>
          <p>Access Level: {'✅ Premium' if u['tier'] in ('pro','elite') else '❌ Free Only'}</p>
        </div>
        <div class="card">
          <h3>Next Step</h3>
          <p>Use AVA to monitor ranked setups, check performance history, and upgrade as your workflow scales.</p>
          <a href="/pricing" class="btn btn-primary" style="width:100%;margin-top:10px;">Manage / Upgrade Plan</a>
        </div>
      </div>

      <div class="card">
        <h3>Top AVA Trades</h3>
        <div class="grid-3">
          {signal_cards or "<p>No active AVA trades generated yet.</p>"}
        </div>
        <a href="/signals" class="btn btn-secondary" style="margin-top:14px;">Open Signal Center</a>
      </div>
    </section>
    """
    return nav_layout("Dashboard — AVA", content)


@app.route("/pricing")
def pricing():
    content = """
    <section class="section">
      <div style="text-align:center; margin-bottom:34px;">
        <div class="badge">Monetization Engine</div>
        <h1>Choose your AVA plan</h1>
        <p>Start free. Upgrade when you want ranked active signals, tracked history, and premium workflow tools.</p>
      </div>

      <div class="grid-3" style="align-items:stretch;">
        <div class="price-card">
          <div class="pill">Free</div>
          <div class="price">$0</div>
          <p>For curious users and light scanners.</p>
          <ul style="line-height:2; color:var(--text);">
            <li>Crypto + stock dashboards</li>
            <li>Basic list signals</li>
            <li>Premium previews</li>
            <li>Email alert signup</li>
          </ul>
          <a href="/register" class="btn btn-secondary" style="width:100%;">Start Free</a>
        </div>

        <div class="price-card featured">
          <div class="pill" style="background:rgba(56,189,248,.12);border-color:rgba(56,189,248,.22);color:#bae6fd;">Most Popular</div>
          <div class="price">$19<span class="small">/mo</span></div>
          <div class="small" style="margin-bottom:14px;">or $190 yearly</div>
          <p>Best for most traders who want real AVA workflow value.</p>
          <ul style="line-height:2; color:var(--text);">
            <li>Active Signal Trades</li>
            <li>AVA Brain detail pages</li>
            <li>Signal history + win-rate</li>
            <li>Entry / stop / target levels</li>
          </ul>
          <div class="btns" style="margin-top:10px;">
            <form action="/checkout/pro_monthly" method="POST" style="flex:1;"><button type="submit" class="btn btn-primary" style="width:100%;">Get Pro Monthly</button></form>
            <form action="/checkout/pro_yearly" method="POST" style="flex:1;"><button type="submit" class="btn btn-secondary" style="width:100%;">Get Pro Yearly</button></form>
          </div>
        </div>

        <div class="price-card">
          <div class="pill" style="background:rgba(250,204,21,.14);border-color:rgba(250,204,21,.22);color:#fde68a;">Power Users</div>
          <div class="price">$49<span class="small">/mo</span></div>
          <div class="small" style="margin-bottom:14px;">or $490 yearly</div>
          <p>For advanced users wanting alert-driven premium expansion features.</p>
          <ul style="line-height:2; color:var(--text);">
            <li>Everything in Pro</li>
            <li>Telegram / Discord alert support</li>
            <li>Priority premium features</li>
            <li>Highest conviction workflow</li>
          </ul>
          <div class="btns" style="margin-top:10px;">
            <form action="/checkout/elite_monthly" method="POST" style="flex:1;"><button type="submit" class="btn btn-primary" style="width:100%;">Get Elite Monthly</button></form>
            <form action="/checkout/elite_yearly" method="POST" style="flex:1;"><button type="submit" class="btn btn-secondary" style="width:100%;">Get Elite Yearly</button></form>
          </div>
        </div>
      </div>
    </section>
    """
    return nav_layout("Pricing — AVA", content)


def stripe_price_id_for_plan(plan_key):
    plan_map = {
        "pro_monthly": Config.STRIPE_PRICE_PRO_MONTHLY,
        "pro_yearly": Config.STRIPE_PRICE_PRO_YEARLY,
        "elite_monthly": Config.STRIPE_PRICE_ELITE_MONTHLY,
        "elite_yearly": Config.STRIPE_PRICE_ELITE_YEARLY
    }
    return plan_map.get(plan_key, "")


@app.route("/checkout/<plan_key>", methods=["POST"])
@require_auth
def checkout(plan_key):
    if not stripe or not Config.STRIPE_SECRET_KEY:
        return "Stripe not configured", 500

    if plan_key not in ("pro_monthly", "pro_yearly", "elite_monthly", "elite_yearly"):
        return "Invalid plan", 400

    price_id = stripe_price_id_for_plan(plan_key)
    if not price_id:
        return f"Stripe price ID missing for {plan_key}", 500

    plan = PLAN_META[plan_key]
    try:
        session = stripe.checkout.Session.create(
            mode="subscription",
            customer_email=g.user["email"],
            client_reference_id=str(g.user["id"]),
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=f"{Config.DOMAIN}/dashboard" if Config.DOMAIN else "/dashboard",
            cancel_url=f"{Config.DOMAIN}/pricing" if Config.DOMAIN else "/pricing",
            metadata={"tier": plan["tier"], "billing": plan["billing"], "plan_key": plan_key}
        )
        return redirect(session.url)
    except Exception as e:
        return str(e), 500


@app.route("/webhook/stripe", methods=["POST"])
def stripe_webhook():
    if not stripe:
        return "OK", 200

    payload = request.data
    sig = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig, Config.STRIPE_WEBHOOK_SECRET)
    except Exception:
        return "Bad Signature", 400

    if event.get("type") == "checkout.session.completed":
        obj = event.get("data", {}).get("object", {})
        uid = obj.get("client_reference_id")
        md = obj.get("metadata", {}) or {}
        tier = md.get("tier", "pro")
        billing = md.get("billing", "monthly")
        if uid:
            db.upgrade_user(int(uid), tier, obj.get("customer"), obj.get("subscription"), billing)

    return "OK", 200


@app.route("/admin")
@require_admin
def admin():
    stats = db.get_signal_stats()
    users = db.get_user_count()
    paid = db.get_paid_user_count()
    subs = db.get_subscriber_count()
    active = len(db.get_active_signals(limit=200))
    history = db.get_signal_history(limit=25)

    rows = ""
    for r in history:
        rows += f"""
        <tr>
          <td>{h(r['symbol'])}</td>
          <td>{h(r['asset_type'])}</td>
          <td>{h(r['signal'])}</td>
          <td>{int(r['confidence'])}%</td>
          <td>{h(r['outcome'])}</td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Admin Control</div>
      <h1>Admin Stats</h1>

      <div class="grid-4" style="margin-bottom:24px;">
        <div class="kpi"><div class="num">{users}</div><div class="label">Users</div></div>
        <div class="kpi"><div class="num">{paid}</div><div class="label">Paid Users</div></div>
        <div class="kpi"><div class="num">{subs}</div><div class="label">Alert Subscribers</div></div>
        <div class="kpi"><div class="num">{active}</div><div class="label">Active Signals</div></div>
      </div>

      <div class="grid-4" style="margin-bottom:24px;">
        <div class="kpi"><div class="num">{stats['total']}</div><div class="label">Signal History</div></div>
        <div class="kpi"><div class="num">{stats['open']}</div><div class="label">Open Signals</div></div>
        <div class="kpi"><div class="num">{stats['tp1_hit'] + stats['tp2_hit']}</div><div class="label">Tracked Wins</div></div>
        <div class="kpi"><div class="num">{stats['win_rate']}%</div><div class="label">Win Rate</div></div>
      </div>

      <div class="card" style="margin-bottom:20px;">
        <h3>Broadcast Controls</h3>
        <div class="btns">
          <form action="/admin/broadcast" method="POST"><button class="btn btn-primary" type="submit">Broadcast Top Signals</button></form>
          <form action="/admin/promote-me" method="POST"><button class="btn btn-secondary" type="submit">Promote My Admin To Elite</button></form>
        </div>
      </div>

      <div class="table-shell">
        <table class="market-table">
          <tr><th>Symbol</th><th>Type</th><th>Signal</th><th>Confidence</th><th>Outcome</th></tr>
          {rows or "<tr><td colspan='5'>No history yet.</td></tr>"}
        </table>
      </div>
    </section>
    """
    return nav_layout("Admin — AVA", content)


@app.route("/admin/broadcast", methods=["POST"])
@require_admin
def admin_broadcast():
    signals = db.get_active_signals(limit=3)
    maybe_broadcast_top_signals(signals)
    return redirect("/admin")


@app.route("/admin/promote-me", methods=["POST"])
@require_admin
def admin_promote_me():
    db.upgrade_user(g.user["id"], "elite", billing_cycle="manual")
    return redirect("/admin")


@app.route("/debug/promote/<tier>")
@require_auth
def debug_promote(tier):
    if tier not in ("free", "pro", "elite"):
        return "invalid tier", 400
    db.upgrade_user(g.user["id"], tier, billing_cycle="manual")
    return redirect("/dashboard")


_bg_started = False


def start_background_refresh():
    global _bg_started
    if _bg_started:
        return

    def background_loop():
        logger.info("Background loop starting...")
        try:
            _perform_crypto_fetch()
            _perform_stock_fetch()
            signals = generate_active_signals()
            maybe_broadcast_top_signals(signals[:3])
            logger.info("Initial data + signals ready.")
        except Exception as e:
            logger.warning(f"Initial background cycle failed: {e}")

        while True:
            time.sleep(60)
            try:
                _perform_crypto_fetch()
            except Exception as e:
                logger.warning(f"Scheduled crypto fetch failed: {e}")

            try:
                _perform_stock_fetch()
            except Exception as e:
                logger.warning(f"Scheduled stock fetch failed: {e}")

            try:
                signals = generate_active_signals()
                maybe_broadcast_top_signals(signals[:3])
            except Exception as e:
                logger.warning(f"Scheduled signal generation failed: {e}")

    threading.Thread(target=background_loop, daemon=True).start()
    _bg_started = True


if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not Config.DEBUG:
    start_background_refresh()


if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
