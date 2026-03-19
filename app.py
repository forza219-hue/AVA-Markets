#!/usr/bin/env python3
import os
import json
import time
import math
import html
import sqlite3
import secrets
import bcrypt
import logging
import random
import threading
import requests
import pandas as pd
import yfinance as yf

from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import urlparse, quote_plus

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - AVA MARKETS - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class Config:
    HOST = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

    DATABASE = os.environ.get("DATABASE_URL", "ava_markets_core.db").strip()

    SECRET_KEY = os.environ.get("SECRET_KEY", "").strip()
    DOMAIN = os.environ.get("DOMAIN", "").strip().rstrip("/")

    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()

    ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "").strip()
    ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "").strip()

    CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "hello@avamarkets.com").strip()

    REQUESTS_TIMEOUT = int(os.environ.get("REQUESTS_TIMEOUT", "12"))

    CRYPTO_CACHE_TTL = int(os.environ.get("CRYPTO_CACHE_TTL", "300"))
    STOCK_CACHE_TTL = int(os.environ.get("STOCK_CACHE_TTL", "600"))
    DETAIL_CACHE_TTL = int(os.environ.get("DETAIL_CACHE_TTL", "300"))

    PAGE_SIZE_CRYPTO = 25
    PAGE_SIZE_STOCKS = 20

    COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "false").lower() == "true"

    ENABLE_BACKGROUND_REFRESH = os.environ.get("ENABLE_BACKGROUND_REFRESH", "true").lower() == "true"
    BACKGROUND_REFRESH_SECONDS = int(os.environ.get("BACKGROUND_REFRESH_SECONDS", "180"))
    BACKGROUND_REFRESH_LEADER = os.environ.get("BACKGROUND_REFRESH_LEADER", "true").lower() == "true"

    DETAIL_WARM_CRYPTO = [
        s.strip().upper() for s in os.environ.get(
            "DETAIL_WARM_CRYPTO", "BTC,ETH,SOL,XRP,DOGE,ADA,AVAX,LINK,PEPE,BONK"
        ).split(",") if s.strip()
    ]
    DETAIL_WARM_STOCKS = [
        s.strip().upper() for s in os.environ.get(
            "DETAIL_WARM_STOCKS", "AAPL,MSFT,NVDA,AMZN,GOOGL,TSLA,GC=F,CL=F"
        ).split(",") if s.strip()
    ]

    RATE_LIMIT_STORAGE_URI = os.environ.get("RATE_LIMIT_STORAGE_URI", "memory://")


def validate_runtime_config():
    missing = []
    for key in ["SECRET_KEY", "DOMAIN", "ADMIN_USERNAME", "ADMIN_PASSWORD"]:
        if not getattr(Config, key, ""):
            missing.append(key)
    if Config.STRIPE_SECRET_KEY and not Config.STRIPE_WEBHOOK_SECRET:
        missing.append("STRIPE_WEBHOOK_SECRET")
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    if not Config.DOMAIN.startswith(("https://", "http://")):
        raise RuntimeError("DOMAIN must include protocol, e.g. https://yourdomain.com")
    logger.info("Config validated. SQLite WAL mode enabled.")


validate_runtime_config()

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
        default_limits=["240 per hour", "20 per minute"]
    )
else:
    logger.warning("Flask-Limiter not installed; rate limiting disabled.")
    class _NoopLimiter:
        def limit(self, *args, **kwargs):
            def deco(fn): return fn
            return deco
    limiter = _NoopLimiter()


CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
:root{
  --bg:#0b0f19;--bg2:#121826;--card:rgba(255,255,255,.05);--border:rgba(255,255,255,.09);
  --text:#f8fafc;--muted:#94a3b8;--blue:#2563eb;--blue2:#60a5fa;--green:#22c55e;
  --red:#ef4444;--yellow:#f59e0b;--shadow:0 24px 60px rgba(0,0,0,.35);
}
*{box-sizing:border-box} html{scroll-behavior:smooth}
body{
  margin:0;font-family:'Inter',sans-serif;color:var(--text);
  background:
    radial-gradient(circle at top left, rgba(37,99,235,.16), transparent 28%),
    radial-gradient(circle at top right, rgba(96,165,250,.12), transparent 24%),
    linear-gradient(145deg,var(--bg),var(--bg2));
}
a{text-decoration:none;color:inherit}
.container{max-width:1240px;margin:0 auto;padding:0 24px}
.nav{
  display:flex;justify-content:space-between;align-items:center;padding:20px 0;
  position:sticky;top:0;z-index:20;background:rgba(11,15,25,.78);
  backdrop-filter:blur(14px);border-bottom:1px solid rgba(255,255,255,.04);
}
.logo{
  font-size:1.3rem;font-weight:800;
  background:linear-gradient(90deg,#fff,var(--blue2),var(--blue));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
}
.nav-links{display:flex;gap:16px;flex-wrap:wrap}
.nav-links a{color:var(--muted);font-weight:600}
.nav-links a:hover{color:var(--text)}
.hero{display:grid;grid-template-columns:1.05fr .95fr;gap:28px;align-items:center;padding:72px 0 48px}
.hero-card,.card,.table-shell,.price-card,.dashboard-card{
  background:rgba(255,255,255,.05);border:1px solid var(--border);border-radius:24px;box-shadow:var(--shadow);
}
.hero-card{padding:36px}.card,.price-card,.dashboard-card{padding:22px}
.badge{
  display:inline-block;padding:8px 14px;border-radius:999px;background:rgba(37,99,235,.14);
  border:1px solid rgba(96,165,250,.24);color:#bfdbfe;font-size:.88rem;font-weight:700;margin-bottom:18px;
}
h1{font-size:clamp(2.4rem,5vw,4.4rem);line-height:1.02;margin:0 0 18px}
h2{margin:0 0 14px}.section{padding:30px 0 72px}.section-title{font-size:2rem;margin:0 0 14px}
p{color:var(--muted);line-height:1.7;font-size:1.02rem}.section-sub{max-width:820px;margin:0 0 24px;color:var(--muted)}
.btns{display:flex;gap:14px;flex-wrap:wrap;margin-top:20px}
.btn{
  display:inline-flex;align-items:center;justify-content:center;padding:14px 18px;border-radius:14px;
  font-weight:700;border:1px solid transparent;cursor:pointer;
}
.btn-primary{background:linear-gradient(90deg,var(--blue2),var(--blue));color:#fff}
.btn-secondary{background:rgba(255,255,255,.05);border-color:rgba(255,255,255,.10);color:var(--text)}
.market-grid,.dashboard-grid{display:grid;gap:18px}
.market-grid{grid-template-columns:repeat(auto-fit,minmax(220px,1fr))}
.dashboard-grid{grid-template-columns:repeat(auto-fit,minmax(260px,1fr))}
.table-shell{overflow:hidden}
.market-table{width:100%;border-collapse:collapse}
.market-table th,.market-table td{padding:16px 14px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;vertical-align:top}
.market-table th{color:#cbd5e1;background:rgba(255,255,255,.02);font-size:.92rem}
.asset-name strong{display:block}.asset-name span{display:block;color:var(--muted);font-size:.85rem;margin-top:4px}
.up{color:var(--green)} .down{color:var(--red)}
.signal{display:inline-flex;padding:8px 12px;border-radius:999px;font-weight:700;font-size:.82rem}
.signal-buy{background:rgba(34,197,94,.14);color:#86efac}
.signal-hold{background:rgba(245,158,11,.14);color:#fde68a}
.signal-sell{background:rgba(239,68,68,.14);color:#fca5a5}
.signal-locked{background:rgba(255,255,255,.08);color:#cbd5e1}
.candle-box{
  background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01));
  border:1px solid rgba(255,255,255,.06);
  border-radius:18px;padding:16px;
}
.candles{height:170px;display:flex;align-items:flex-end;gap:6px}
.candle{flex:1;position:relative;height:140px}
.wick{position:absolute;left:50%;transform:translateX(-50%);width:2px;background:#cbd5e1;border-radius:999px}
.body{position:absolute;left:50%;transform:translateX(-50%);width:8px;border-radius:4px}
.body.green{background:linear-gradient(180deg,#34d399,#16a34a)}
.body.red{background:linear-gradient(180deg,#f87171,#dc2626)}
.form-shell{display:flex;justify-content:center;align-items:center;min-height:70vh}
.form-card{
  width:100%;max-width:460px;background:rgba(255,255,255,.05);border:1px solid var(--border);
  border-radius:24px;box-shadow:var(--shadow);padding:30px;
}
.form-card input{
  width:100%;padding:14px 16px;margin:10px 0;border-radius:14px;border:1px solid rgba(255,255,255,.10);
  background:rgba(255,255,255,.04);color:var(--text);outline:none;
}
.form-card button{
  width:100%;padding:14px 18px;margin-top:10px;border:none;border-radius:14px;
  background:linear-gradient(90deg,var(--blue2),var(--blue));color:#fff;font-weight:700;cursor:pointer;
}
.error{color:#fca5a5;margin-top:10px}
.key{background:#0f172a;padding:12px;border-radius:12px;word-break:break-all;font-family:monospace;font-size:13px}
.tier{display:inline-flex;padding:8px 12px;border-radius:999px;background:rgba(37,99,235,.14);color:#bfdbfe;font-weight:700}
.footer{padding:30px 0 60px;color:var(--muted);text-align:center}
.detail-grid{display:grid;grid-template-columns:1.1fr .9fr;gap:20px}
.mini-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:14px}
.metric-card{
  background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);border-radius:18px;padding:16px;
}
.metric-card h3{margin:0 0 8px}
.pagination{display:flex;gap:10px;flex-wrap:wrap;margin-top:22px;align-items:center}
.page-link{
  padding:10px 14px;border-radius:12px;background:rgba(255,255,255,.05);
  border:1px solid rgba(255,255,255,.08);color:var(--text);font-weight:700
}
.search-box{
  width:100%;max-width:420px;padding:14px 16px;border-radius:14px;
  border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:var(--text);outline:none;
}
.asset-row{display:flex;align-items:center;gap:10px;}
.asset-logo{
  width:24px;height:24px;border-radius:50%;object-fit:cover;
  background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);flex-shrink:0;
}
.asset-icon{
  width:24px;height:24px;display:inline-flex;align-items:center;justify-content:center;
  font-size:1rem;flex-shrink:0;
}
.asset-feature{display:flex;align-items:center;gap:12px;}
.asset-feature-logo{
  width:32px;height:32px;border-radius:50%;object-fit:cover;
  background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);
}
.asset-feature-icon{
  width:32px;height:32px;display:inline-flex;align-items:center;justify-content:center;
  font-size:1.25rem;
}
.asset-subtitle{color:var(--muted);font-size:.92rem;margin-top:4px;}
.featured-shell{display:flex;flex-direction:column;gap:10px;}
.live-stamp{color:var(--muted);font-size:.88rem;margin-top:10px}
ul{color:var(--muted)}
@media (max-width: 900px){
  .hero,.detail-grid,.mini-grid{grid-template-columns:1fr}
  .nav{flex-direction:column;gap:14px}
  .nav-links{justify-content:center}
}
"""


CRYPTO_TOP_90 = [
    ("BTC", "Bitcoin"), ("ETH", "Ethereum"), ("BNB", "BNB"), ("SOL", "Solana"), ("XRP", "XRP"),
    ("DOGE", "Dogecoin"), ("ADA", "Cardano"), ("AVAX", "Avalanche"), ("LINK", "Chainlink"), ("DOT", "Polkadot"),
    ("MATIC", "Polygon"), ("LTC", "Litecoin"), ("BCH", "Bitcoin Cash"), ("ATOM", "Cosmos"), ("UNI", "Uniswap"),
    ("NEAR", "NEAR Protocol"), ("APT", "Aptos"), ("ARB", "Arbitrum"), ("OP", "Optimism"), ("SUI", "Sui"),
    ("PEPE", "Pepe"), ("SHIB", "Shiba Inu"), ("TRX", "TRON"), ("ETC", "Ethereum Classic"), ("XLM", "Stellar"),
    ("HBAR", "Hedera"), ("ICP", "Internet Computer"), ("FIL", "Filecoin"), ("INJ", "Injective"), ("RNDR", "Render"),
    ("TAO", "Bittensor"), ("IMX", "Immutable"), ("SEI", "Sei"), ("TIA", "Celestia"), ("JUP", "Jupiter"),
    ("PYTH", "Pyth Network"), ("BONK", "Bonk"), ("WIF", "dogwifhat"), ("FET", "Fetch.ai"), ("RUNE", "THORChain"),
    ("AAVE", "Aave"), ("MKR", "Maker"), ("ALGO", "Algorand"), ("VET", "VeChain"), ("EGLD", "MultiversX"),
    ("THETA", "Theta Network"), ("SAND", "The Sandbox"), ("MANA", "Decentraland"), ("AXS", "Axie Infinity"),
    ("GRT", "The Graph"), ("FLOW", "Flow"), ("KAS", "Kaspa"), ("KAVA", "Kava"), ("DYDX", "dYdX"),
    ("WLD", "Worldcoin"), ("ARKM", "Arkham"), ("STRK", "Starknet"), ("ENA", "Ethena"), ("ONDO", "Ondo"),
    ("JASMY", "JasmyCoin"), ("LDO", "Lido DAO"), ("CRV", "Curve DAO Token"), ("SNX", "Synthetix"), ("COMP", "Compound"),
    ("1INCH", "1inch"), ("BAT", "Basic Attention Token"), ("ZEC", "Zcash"), ("DASH", "Dash"), ("CHZ", "Chiliz"),
    ("ROSE", "Oasis"), ("QTUM", "Qtum"), ("IOTA", "IOTA"), ("ZIL", "Zilliqa"), ("KSM", "Kusama"),
    ("GMT", "STEPN"), ("BLUR", "Blur"), ("ACE", "Fusionist"), ("NEO", "NEO"), ("CFX", "Conflux"),
    ("FTM", "Fantom"), ("GALA", "Gala"), ("LRC", "Loopring"), ("ENS", "Ethereum Name Service"), ("SXP", "Solar"),
    ("HOT", "Holo"), ("ANKR", "Ankr"), ("ICX", "ICON"), ("SC", "Siacoin"), ("CKB", "Nervos Network"),
    ("MASK", "Mask Network"), ("YFI", "yearn.finance"), ("WOO", "WOO"), ("SKL", "SKALE"),
]
CRYPTO_NAME_MAP = {symbol: name for symbol, name in CRYPTO_TOP_90}

STOCK_UNIVERSE = [
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"), ("AMZN", "Amazon"),
    ("GOOGL", "Alphabet"), ("META", "Meta"), ("TSLA", "Tesla"), ("BRK-B", "Berkshire Hathaway"),
    ("JPM", "JPMorgan"), ("V", "Visa"), ("MA", "Mastercard"), ("UNH", "UnitedHealth"),
    ("XOM", "Exxon Mobil"), ("LLY", "Eli Lilly"), ("AVGO", "Broadcom"), ("ORCL", "Oracle"),
    ("COST", "Costco"), ("PG", "Procter & Gamble"), ("HD", "Home Depot"), ("NFLX", "Netflix"),
    ("ABBV", "AbbVie"), ("KO", "Coca-Cola"), ("PEP", "PepsiCo"), ("MRK", "Merck"),
    ("BAC", "Bank of America"), ("WMT", "Walmart"), ("CVX", "Chevron"), ("AMD", "AMD"),
    ("ADBE", "Adobe"), ("CRM", "Salesforce"), ("ASML", "ASML"), ("TSM", "Taiwan Semiconductor"),
    ("NVO", "Novo Nordisk"), ("SAP", "SAP"), ("SONY", "Sony"), ("TM", "Toyota"),
    ("BABA", "Alibaba"), ("PDD", "PDD"), ("SHEL", "Shell"), ("BP", "BP"),
    ("SHOP", "Shopify"), ("MELI", "MercadoLibre"), ("IBM", "IBM"), ("INTC", "Intel"),
    ("QCOM", "Qualcomm"),
    ("GC=F", "Gold Futures"), ("SI=F", "Silver Futures"), ("PL=F", "Platinum Futures"),
    ("CL=F", "Oil Futures"), ("SIG", "Diamonds Proxy"),
]
STOCK_NAME_MAP = {s: n for s, n in STOCK_UNIVERSE}

STOCK_DOMAINS = {
    "AAPL": "apple.com", "MSFT": "microsoft.com", "NVDA": "nvidia.com", "AMZN": "amazon.com",
    "GOOGL": "google.com", "META": "meta.com", "TSLA": "tesla.com", "BRK-B": "berkshirehathaway.com",
    "JPM": "jpmorganchase.com", "V": "visa.com", "MA": "mastercard.com", "UNH": "uhc.com",
    "XOM": "exxonmobil.com", "LLY": "lilly.com", "AVGO": "broadcom.com", "ORCL": "oracle.com",
    "COST": "costco.com", "PG": "pg.com", "HD": "homedepot.com", "NFLX": "netflix.com",
    "ABBV": "abbvie.com", "KO": "coca-colacompany.com", "PEP": "pepsico.com", "MRK": "merck.com",
    "BAC": "bankofamerica.com", "WMT": "walmart.com", "CVX": "chevron.com", "AMD": "amd.com",
    "ADBE": "adobe.com", "CRM": "salesforce.com", "ASML": "asml.com", "TSM": "tsmc.com",
    "NVO": "novonordisk.com", "SAP": "sap.com", "SONY": "sony.com", "TM": "toyota.com",
    "BABA": "alibaba.com", "PDD": "pddholdings.com", "SHEL": "shell.com", "BP": "bp.com",
    "SHOP": "shopify.com", "MELI": "mercadolibre.com", "IBM": "ibm.com", "INTC": "intel.com",
    "QCOM": "qualcomm.com",
}


def h(value):
    return html.escape("" if value is None else str(value), quote=True)


def safe_query_value(value):
    return quote_plus("" if value is None else str(value))


def get_stock_logo(symbol):
    domain = STOCK_DOMAINS.get(symbol.upper())
    if not domain:
        return None
    return f"https://logo.clearbit.com/{domain}"


def get_crypto_logo(symbol):
    return f"https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/128/color/{symbol.lower()}.png"


def get_asset_icon(symbol):
    mapping = {"GC=F": "🥇", "SI=F": "🥈", "PL=F": "🔘", "CL=F": "🛢️", "SIG": "💎"}
    return mapping.get(symbol.upper(), "📈")


class Database:
    def __init__(self, path):
        self.path = path
        self.init()

    def conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL;")
        c.execute("PRAGMA synchronous=NORMAL;")
        c.execute("PRAGMA foreign_keys=ON;")
        c.execute("PRAGMA busy_timeout=5000;")
        return c

    def init(self):
        c = self.conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            api_key TEXT UNIQUE NOT NULL,
            tier TEXT NOT NULL DEFAULT 'free',
            stripe_customer_id TEXT,
            stripe_subscription_id TEXT,
            subscription_status TEXT NOT NULL DEFAULT 'inactive',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS admin_sessions (
            token TEXT PRIMARY KEY,
            expires_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            provider TEXT,
            payment_id TEXT,
            amount REAL,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, asset_type, symbol),
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS signal_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            signal TEXT NOT NULL,
            confidence REAL NOT NULL,
            confidence_label TEXT,
            regime TEXT,
            trend_state TEXT,
            price_at_signal REAL,
            explanation_json TEXT,
            factors_json TEXT,
            signal_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS signal_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_id INTEGER NOT NULL,
            horizon TEXT NOT NULL,
            price_after REAL,
            return_pct REAL,
            outcome TEXT,
            evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(snapshot_id, horizon),
            FOREIGN KEY(snapshot_id) REFERENCES signal_snapshots(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS signal_changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            signal_type TEXT NOT NULL,
            old_signal TEXT,
            new_signal TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS market_cache (
            cache_key TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id);
        CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires_at ON admin_sessions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_watchlists_user_id ON watchlists(user_id);
        CREATE INDEX IF NOT EXISTS idx_signal_snapshots_lookup ON signal_snapshots(asset_type, symbol, timeframe, signal_type, id DESC);
        CREATE INDEX IF NOT EXISTS idx_signal_snapshots_created_at ON signal_snapshots(created_at);
        CREATE INDEX IF NOT EXISTS idx_signal_outcomes_snapshot_horizon ON signal_outcomes(snapshot_id, horizon);
        """)
        c.commit()
        c.close()

    def create_user(self, email, password):
        c = self.conn()
        api_key = f"ava_{secrets.token_hex(24)}"
        password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        try:
            c.execute("""
                INSERT INTO users (email, password_hash, api_key, tier, subscription_status)
                VALUES (?, ?, ?, 'free', 'inactive')
            """, (email.lower().strip(), password_hash, api_key))
            c.commit()
            row = c.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
            return dict(row) if row else None
        except sqlite3.IntegrityError:
            return None
        finally:
            c.close()

    def verify_user(self, email, password):
        c = self.conn()
        row = c.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
        c.close()
        if not row:
            return None
        if bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
            return dict(row)
        return None

    def create_session(self, user_id, days=30):
        token = secrets.token_hex(32)
        expires_at = datetime.utcnow() + timedelta(days=days)
        c = self.conn()
        c.execute("INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)", (token, user_id, expires_at))
        c.commit()
        c.close()
        return token

    def get_user_by_session(self, token):
        if not token:
            return None
        c = self.conn()
        row = c.execute("""
            SELECT u.* FROM users u
            JOIN sessions s ON s.user_id = u.id
            WHERE s.token = ? AND (s.expires_at IS NULL OR s.expires_at > ?)
        """, (token, datetime.utcnow())).fetchone()
        c.close()
        return dict(row) if row else None

    def delete_session(self, token):
        c = self.conn()
        c.execute("DELETE FROM sessions WHERE token = ?", (token,))
        c.commit()
        c.close()

    def create_admin_session(self, hours=12):
        token = secrets.token_hex(32)
        expires_at = datetime.utcnow() + timedelta(hours=hours)
        c = self.conn()
        c.execute("INSERT INTO admin_sessions (token, expires_at) VALUES (?, ?)", (token, expires_at))
        c.commit()
        c.close()
        return token

    def get_admin_session(self, token):
        if not token:
            return None
        c = self.conn()
        row = c.execute("""
            SELECT token, expires_at, created_at
            FROM admin_sessions
            WHERE token = ? AND (expires_at IS NULL OR expires_at > ?)
        """, (token, datetime.utcnow())).fetchone()
        c.close()
        return dict(row) if row else None

    def delete_admin_session(self, token):
        c = self.conn()
        c.execute("DELETE FROM admin_sessions WHERE token = ?", (token,))
        c.commit()
        c.close()

    def update_user(self, user_id, **kwargs):
        if not kwargs:
            return
        allowed = {
            "tier", "stripe_customer_id", "stripe_subscription_id",
            "subscription_status", "api_key", "password_hash"
        }
        fields = []
        values = []
        for k, v in kwargs.items():
            if k not in allowed:
                continue
            fields.append(f"{k} = ?")
            values.append(v)
        if not fields:
            return
        values.append(user_id)
        c = self.conn()
        c.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = ?", values)
        c.commit()
        c.close()

    def get_user_by_id(self, user_id):
        c = self.conn()
        row = c.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        c.close()
        return dict(row) if row else None

    def log_payment(self, user_id, provider, payment_id, amount, status):
        c = self.conn()
        c.execute("""
            INSERT INTO payments (user_id, provider, payment_id, amount, status)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, provider, payment_id, amount, status))
        c.commit()
        c.close()

    def get_all_users(self):
        c = self.conn()
        rows = c.execute("SELECT id, email, tier, subscription_status, created_at FROM users ORDER BY id DESC").fetchall()
        c.close()
        return [dict(r) for r in rows]

    def get_all_payments(self):
        c = self.conn()
        rows = c.execute("""
            SELECT p.*, u.email FROM payments p
            LEFT JOIN users u ON u.id = p.user_id ORDER BY p.id DESC LIMIT 100
        """).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def add_watchlist(self, user_id, asset_type, symbol):
        c = self.conn()
        c.execute("INSERT OR IGNORE INTO watchlists (user_id, asset_type, symbol) VALUES (?, ?, ?)", (user_id, asset_type, symbol.upper()))
        c.commit()
        c.close()

    def remove_watchlist(self, user_id, asset_type, symbol):
        c = self.conn()
        c.execute("DELETE FROM watchlists WHERE user_id = ? AND asset_type = ? AND symbol = ?", (user_id, asset_type, symbol.upper()))
        c.commit()
        c.close()

    def get_watchlist(self, user_id):
        c = self.conn()
        rows = c.execute("SELECT * FROM watchlists WHERE user_id = ? ORDER BY created_at DESC", (user_id,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def latest_snapshot(self, asset_type, symbol, timeframe, signal_type):
        c = self.conn()
        row = c.execute("""
            SELECT * FROM signal_snapshots
            WHERE asset_type = ? AND symbol = ? AND timeframe = ? AND signal_type = ?
            ORDER BY id DESC LIMIT 1
        """, (asset_type, symbol.upper(), timeframe, signal_type)).fetchone()
        c.close()
        return dict(row) if row else None

    def insert_signal_snapshot(self, asset_type, symbol, timeframe, signal_type, signal, confidence,
                               confidence_label, regime, trend_state, price_at_signal,
                               explanation, factors, signal_hash):
        c = self.conn()
        c.execute("""
            INSERT INTO signal_snapshots (
                asset_type, symbol, timeframe, signal_type, signal, confidence,
                confidence_label, regime, trend_state, price_at_signal,
                explanation_json, factors_json, signal_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            asset_type, symbol.upper(), timeframe, signal_type, signal, confidence,
            confidence_label, regime, trend_state, price_at_signal,
            json.dumps(explanation), json.dumps(factors), signal_hash
        ))
        snapshot_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.commit()
        c.close()
        return snapshot_id

    def log_signal_change(self, asset_type, symbol, timeframe, signal_type, old_signal, new_signal):
        c = self.conn()
        c.execute("""
            INSERT INTO signal_changes (
                asset_type, symbol, timeframe, signal_type, old_signal, new_signal
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (asset_type, symbol.upper(), timeframe, signal_type, old_signal, new_signal))
        c.commit()
        c.close()

    def get_recent_signal_changes(self, limit=50):
        c = self.conn()
        rows = c.execute("SELECT * FROM signal_changes ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def insert_signal_outcome(self, snapshot_id, horizon, price_after, return_pct, outcome):
        c = self.conn()
        c.execute("""
            INSERT OR IGNORE INTO signal_outcomes (
                snapshot_id, horizon, price_after, return_pct, outcome
            ) VALUES (?, ?, ?, ?, ?)
        """, (snapshot_id, horizon, price_after, return_pct, outcome))
        c.commit()
        c.close()

    def outcome_exists(self, snapshot_id, horizon):
        c = self.conn()
        row = c.execute("SELECT 1 FROM signal_outcomes WHERE snapshot_id = ? AND horizon = ? LIMIT 1", (snapshot_id, horizon)).fetchone()
        c.close()
        return bool(row)

    def get_recent_snapshots_for_outcomes(self, days=10, limit=500):
        c = self.conn()
        rows = c.execute("""
            SELECT * FROM signal_snapshots WHERE created_at >= datetime('now', ?)
            ORDER BY id DESC LIMIT ?
        """, (f"-{int(days)} days", limit)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def get_performance_summary(self, days=30):
        c = self.conn()
        rows = c.execute("""
            SELECT s.asset_type, s.symbol, s.signal, s.timeframe, s.signal_type,
                   s.confidence, s.created_at, o.horizon, o.return_pct, o.outcome
            FROM signal_snapshots s
            JOIN signal_outcomes o ON o.snapshot_id = s.id
            WHERE s.created_at >= datetime('now', ?) ORDER BY s.id DESC
        """, (f"-{int(days)} days",)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def upsert_signal_snapshot_if_changed(self, asset_type, symbol, timeframe, signal_type, signal, confidence,
                                          confidence_label, regime, trend_state, price_at_signal,
                                          explanation, factors):
        raw = json.dumps({
            "signal": signal,
            "confidence": round(confidence, 4),
            "regime": regime,
            "trend_state": trend_state,
            "factors": factors
        }, sort_keys=True)

        prev = self.latest_snapshot(asset_type, symbol, timeframe, signal_type)
        if prev:
            prev_factors = []
            try: prev_factors = json.loads(prev.get("factors_json") or "[]")
            except Exception: pass

            prev_raw = json.dumps({
                "signal": prev.get("signal"),
                "confidence": round(float(prev.get("confidence") or 0), 4),
                "regime": prev.get("regime"),
                "trend_state": prev.get("trend_state"),
                "factors": prev_factors
            }, sort_keys=True)

            if prev_raw == raw:
                return prev.get("id"), False

            if prev.get("signal") != signal:
                self.log_signal_change(asset_type, symbol, timeframe, signal_type, prev.get("signal"), signal)

        sid = self.insert_signal_snapshot(
            asset_type, symbol, timeframe, signal_type, signal, confidence,
            confidence_label, regime, trend_state, price_at_signal,
            explanation, factors, secrets.token_hex(16)
        )
        return sid, True

    def cache_get(self, cache_key, ttl_seconds):
        c = self.conn()
        row = c.execute("SELECT payload_json, updated_at FROM market_cache WHERE cache_key = ?", (cache_key,)).fetchone()
        c.close()
        if not row: return None
        if int(time.time()) - int(row["updated_at"]) > ttl_seconds: return None
        try: return json.loads(row["payload_json"])
        except: return None

    def cache_get_stale(self, cache_key):
        c = self.conn()
        row = c.execute("SELECT payload_json FROM market_cache WHERE cache_key = ?", (cache_key,)).fetchone()
        c.close()
        if not row: return None
        try: return json.loads(row["payload_json"])
        except: return None

    def cache_set(self, cache_key, payload):
        c = self.conn()
        c.execute("""
            INSERT INTO market_cache (cache_key, payload_json, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at
        """, (cache_key, json.dumps(payload), int(time.time())))
        c.commit()
        c.close()

    def purge_expired_rows(self):
        c = self.conn()
        c.execute("DELETE FROM sessions WHERE expires_at IS NOT NULL AND expires_at <= ?", (datetime.utcnow(),))
        c.execute("DELETE FROM admin_sessions WHERE expires_at IS NOT NULL AND expires_at <= ?", (datetime.utcnow(),))
        c.execute("DELETE FROM market_cache WHERE updated_at < ?", (int(time.time()) - (86400 * 14),))
        c.commit()
        c.close()


db = Database(Config.DATABASE)


class StripeManager:
    @staticmethod
    def create_checkout(user_id, email, tier, success_url, cancel_url):
        if not stripe or not Config.STRIPE_SECRET_KEY: return None
        prices = {"basic": 900, "pro": 2900, "elite": 7900}
        if tier not in prices: return None
        try:
            return stripe.checkout.Session.create(
                payment_method_types=["card"], customer_email=email, client_reference_id=str(user_id),
                line_items=[{"price_data": {"currency": "usd", "product_data": {"name": f"AVA Markets - {tier.title()}"}, "unit_amount": prices[tier], "recurring": {"interval": "month"}}, "quantity": 1}],
                mode="subscription", success_url=success_url, cancel_url=cancel_url, metadata={"user_id": str(user_id), "tier": tier}
            )
        except Exception as e:
            logger.error(f"Stripe checkout error: {e}")
            return None

sm = StripeManager()


def pct_change(a, b):
    if b in [0, None]: return 0.0
    return ((a - b) / b) * 100.0


def fmt_price(v, symbol=None):
    if symbol == "ASML": return f"€{v:,.2f}"
    if v >= 1000: return f"${v:,.2f}"
    if v >= 1: return f"${v:,.2f}"
    if v >= 0.01: return f"${v:.4f}"
    if v >= 0.0001: return f"${v:.6f}"
    if v >= 0.000001: return f"${v:.8f}"
    return f"${v:.10f}"


def fmt_change(v):
    return f"{v:+.2f}%"


def get_int_arg(name, default=1, min_value=1, max_value=100000):
    try: val = int(request.args.get(name, default))
    except: val = default
    return max(min_value, min(max_value, val))


def fallback_candles_html(seed=1):
    random.seed(seed)
    bars = []
    top = 24
    for i in range(20):
        wick = random.randint(55, 120)
        height = random.randint(12, 52)
        color = "green" if i % 2 == 0 or random.random() > 0.42 else "red"
        top = max(8, min(65, top + random.randint(-8, 8)))
        bars.append({"wick": wick, "top": top, "height": height, "color": color})

    html_parts = ['<div class="candles">']
    for b in bars:
        html_parts.append(f"""
        <div class="candle">
          <div class="wick" style="top:0;height:{b['wick']}px;"></div>
          <div class="body {b['color']}" style="top:{b['top']}px;height:{b['height']}px;"></div>
        </div>
        """)
    html_parts.append("</div>")
    return "".join(html_parts)

def render_candles_from_ohlc(candles, height=140):
    if not candles: return fallback_candles_html()
    sample = candles[-20:]
    highs = [c["high"] for c in sample]
    lows = [c["low"] for c in sample]
    max_high = max(highs)
    min_low = min(lows)
    span = max(max_high - min_low, 1e-9)

    html_parts = ['<div class="candles">']
    for c in sample:
        high_y = (max_high - c["high"]) / span * height
        low_y = (max_high - c["low"]) / span * height
        open_y = (max_high - c["open"]) / span * height
        close_y = (max_high - c["close"]) / span * height

        body_top = min(open_y, close_y)
        body_height = max(abs(close_y - open_y), 6)
        wick_top = high_y
        wick_height = max(low_y - high_y, body_height + 6)
        color = "green" if c["close"] >= c["open"] else "red"

        html_parts.append(f"""
        <div class="candle">
          <div class="wick" style="top:{wick_top:.1f}px;height:{wick_height:.1f}px;"></div>
          <div class="body {color}" style="top:{body_top:.1f}px;height:{body_height:.1f}px;"></div>
        </div>
        """)
    html_parts.append("</div>")
    return "".join(html_parts)


def normalize_dt_to_ts(value):
    try: return int(value.timestamp())
    except: return None


def compute_rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0: return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_ema(values, period):
    if not values: return []
    k = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]: ema.append(v * k + ema[-1] * (1 - k))
    return ema


def compute_atr(candles, period=14):
    if len(candles) < 2: return 0.0
    trs = []
    for i in range(1, len(candles)):
        h_ = candles[i]["high"]
        l_ = candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h_ - l_, abs(h_ - pc), abs(l_ - pc)))
    if not trs: return 0.0
    period = min(period, len(trs))
    return sum(trs[-period:]) / period


def compute_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow: return {"macd": 0.0, "signal": 0.0, "hist": 0.0}
    ema_fast = compute_ema(closes, fast)
    ema_slow = compute_ema(closes, slow)
    macd_line = [f - s for f, s in zip(ema_fast[-len(ema_slow):], ema_slow)]
    signal_line = compute_ema(macd_line, signal)
    hist = macd_line[-1] - signal_line[-1]
    return {"macd": macd_line[-1], "signal": signal_line[-1], "hist": hist}


def compute_bollinger(closes, period=20, std_mult=2):
    if len(closes) < period:
        last = closes[-1] if closes else 0.0
        return {"mid": last, "upper": last, "lower": last, "bandwidth": 0.0}
    window = closes[-period:]
    mean = sum(window) / period
    variance = sum((x - mean) ** 2 for x in window) / period
    std = variance ** 0.5
    upper = mean + std_mult * std
    lower = mean - std_mult * std
    bandwidth = ((upper - lower) / mean) if mean else 0.0
    return {"mid": mean, "upper": upper, "lower": lower, "bandwidth": bandwidth}


def confidence_label(conf):
    pct = conf * 100
    if pct <= 60: return "Weak"
    if pct <= 75: return "Moderate"
    return "Strong"


def extract_market_features(candles):
    if not candles or len(candles) < 20: return None
    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    last, prev = closes[-1], closes[-2]
    sma20 = sum(closes[-20:]) / 20
    sma50 = sum(closes[-50:]) / min(50, len(closes))
    ema20 = compute_ema(closes, 20)[-1]
    ema50 = compute_ema(closes, 50)[-1]

    move_5 = pct_change(last, closes[-5])
    move_10 = pct_change(last, closes[-10])
    rsi = compute_rsi(closes, period=min(14, len(closes) - 1))
    atr = compute_atr(candles, period=min(14, len(candles) - 1))
    atr_pct = (atr / last) * 100 if last else 0.0

    macd = compute_macd(closes)
    bb = compute_bollinger(closes)

    recent_high_20 = max(highs[-20:])
    recent_low_20 = min(lows[-20:])
    breakout_up = last >= recent_high_20 * 0.998
    breakout_down = last <= recent_low_20 * 1.002

    range_now = highs[-1] - lows[-1]
    range_avg = sum((highs[i] - lows[i]) for i in range(-5, 0)) / 5 if len(highs) >= 5 else range_now
    volatility_ratio = (range_now / range_avg) if range_avg else 1.0

    n = len(closes)
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(closes) / n
    denom = sum((xi - x_mean) ** 2 for xi in x) or 1e-9
    slope = sum((x[i] - x_mean) * (closes[i] - y_mean) for i in range(n)) / denom
    slope_pct = (slope / last) * 100 if last else 0.0

    return {
        "last": last, "prev": prev, "sma20": sma20, "sma50": sma50, "ema20": ema20, "ema50": ema50,
        "move_5": move_5, "move_10": move_10, "rsi": rsi, "atr": atr, "atr_pct": atr_pct,
        "macd": macd["macd"], "macd_signal": macd["signal"], "macd_hist": macd["hist"],
        "bb_mid": bb["mid"], "bb_upper": bb["upper"], "bb_lower": bb["lower"], "bb_bandwidth": bb["bandwidth"],
        "volatility_ratio": volatility_ratio, "slope": slope, "slope_pct": slope_pct,
        "breakout_up": breakout_up, "breakout_down": breakout_down,
    }


def detect_market_regime(feats):
    if not feats: return "unknown"
    trend_up = feats["ema20"] > feats["ema50"] and feats["slope_pct"] > 0.02
    trend_down = feats["ema20"] < feats["ema50"] and feats["slope_pct"] < -0.02
    volatile = feats["atr_pct"] > 3.5 or feats["volatility_ratio"] > 1.6
    compressed = feats["bb_bandwidth"] < 0.04 and feats["atr_pct"] < 1.2
    breakout = feats["breakout_up"] or feats["breakout_down"]

    if breakout and volatile: return "high_vol_breakout"
    if trend_up and volatile: return "trending_up_volatile"
    if trend_down and volatile: return "trending_down_volatile"
    if trend_up: return "trending_up"
    if trend_down: return "trending_down"
    if compressed: return "low_vol_compression"
    return "range_bound"


def ava_hypothesis_engine(candles, timeframe="1h", signal_type="short_term"):
    feats = extract_market_features(candles)
    if not feats:
        return {"signal": "HOLD", "confidence": 0.50, "confidence_label": "Weak", "trend_state": "Neutral", "trend_strength": "Low", "forecast_trend": "Stable", "projected_change": "+0.00%", "regime": "unknown", "dominant_factors": ["insufficient_data"], "reasoning": "Not enough candle history for AVA Brain.", "explanations": ["Insufficient data."], "score": 0}

    regime = detect_market_regime(feats)
    score = 0
    explanations, factors = [], []

    if feats["last"] > feats["ema20"]:
        score += 2; explanations.append("Price is above EMA20, supporting short-term strength."); factors.append("price_above_ema20")
    else:
        score -= 2; explanations.append("Price is below EMA20, weakening short-term structure."); factors.append("price_below_ema20")

    if feats["ema20"] > feats["ema50"]:
        score += 2; explanations.append("EMA20 is above EMA50, confirming bullish trend alignment."); factors.append("ema20_above_ema50")
    else:
        score -= 2; explanations.append("EMA20 is below EMA50, confirming bearish trend alignment."); factors.append("ema20_below_ema50")

    if 55 <= feats["rsi"] <= 70:
        score += 1; explanations.append(f"RSI at {feats['rsi']:.1f} is bullish without being heavily overbought."); factors.append("rsi_bullish_balanced")
    elif feats["rsi"] >= 75:
        if regime.startswith("trending_up"): explanations.append(f"RSI at {feats['rsi']:.1f} is elevated, but strong uptrends can sustain overbought readings."); factors.append("rsi_overbought_trend")
        else: score -= 1; explanations.append(f"RSI at {feats['rsi']:.1f} is overbought in a non-trending regime."); factors.append("rsi_overbought_range")
    elif feats["rsi"] <= 30:
        if regime.startswith("trending_down"): explanations.append(f"RSI at {feats['rsi']:.1f} is oversold, but downtrends can remain weak."); factors.append("rsi_oversold_downtrend")
        else: score += 1; explanations.append(f"RSI at {feats['rsi']:.1f} suggests a potential rebound condition."); factors.append("rsi_oversold_rebound")
    elif feats["rsi"] < 45:
        score -= 1; explanations.append(f"RSI at {feats['rsi']:.1f} leans bearish."); factors.append("rsi_bearish")

    if feats["macd_hist"] > 0: score += 2; explanations.append("MACD histogram is positive, showing improving momentum."); factors.append("macd_hist_positive")
    else: score -= 2; explanations.append("MACD histogram is negative, showing fading momentum."); factors.append("macd_hist_negative")

    if feats["move_5"] > 1.5: score += 1; explanations.append("5-period momentum is positive."); factors.append("positive_5p_momentum")
    elif feats["move_5"] < -1.5: score -= 1; explanations.append("5-period momentum is negative."); factors.append("negative_5p_momentum")

    if feats["breakout_up"]: score += 2; explanations.append("Price is testing or breaking the recent 20-period high."); factors.append("breakout_up")
    elif feats["breakout_down"]: score -= 2; explanations.append("Price is testing or breaking the recent 20-period low."); factors.append("breakout_down")

    if regime == "trending_up": score += 1; explanations.append("Market regime is trending up."); factors.append("regime_trending_up")
    elif regime == "trending_down": score -= 1; explanations.append("Market regime is trending down."); factors.append("regime_trending_down")

    if score >= 5: signal = "BUY"
    elif score <= -5: signal = "SELL"
    else: signal = "HOLD"

    confidence = min(0.90, max(0.50, 0.52 + abs(score) * 0.045))
    c_label = confidence_label(confidence)
    trend_strength = "High" if abs(score) >= 7 else "Medium" if abs(score) >= 4 else "Low"
    if score > 1: trend_state = "Bullish"; forecast_trend = "Upward"
    elif score < -1: trend_state = "Bearish"; forecast_trend = "Downward"
    else: trend_state = "Neutral"; forecast_trend = "Stable"

    projected_change_val = feats["move_10"] * 0.6 + feats["slope_pct"] * 12

    return {
        "signal": signal, "confidence": round(confidence, 2), "confidence_label": c_label,
        "trend_state": trend_state, "trend_strength": trend_strength, "forecast_trend": forecast_trend,
        "projected_change": f"{projected_change_val:+.2f}%", "regime": regime,
        "dominant_factors": factors[:6], "reasoning": " ".join(explanations[:3]),
        "explanations": explanations[:6], "score": score
    }


def aggregate_multi_timeframe_brain(asset_type, symbol, tf_map):
    signal_map = {"15m": ("short_term", 1.0), "1h": ("intraday", 1.2), "4h": ("swing", 1.5)} if asset_type == "crypto" else {"1d": ("short_term", 1.0), "1wk": ("trend", 1.4), "1mo": ("macro", 1.6)}
    per_tf = {}
    weighted_score = 0.0
    total_weight = 0.0

    for tf, candles in tf_map.items():
        stype, weight = signal_map[tf]
        brain = ava_hypothesis_engine(candles or [], timeframe=tf, signal_type=stype)
        per_tf[tf] = {"timeframe": tf, "signal_type": stype, **brain}
        weighted_score += brain["score"] * weight
        total_weight += weight

    avg_score = (weighted_score / total_weight) if total_weight else 0.0
    overall = "BUY" if avg_score >= 4 else "SELL" if avg_score <= -4 else "HOLD"
    confidence = min(0.90, max(0.50, 0.54 + abs(avg_score) * 0.04))
    
    all_regimes = [v["regime"] for v in per_tf.values()]
    all_factors = []
    for v in per_tf.values(): all_factors.extend(v.get("dominant_factors", []))

    explanations = [f"{tf} / {v['signal_type']}: {v['signal']} ({v['trend_state']}, {v['regime']})" for tf, v in per_tf.items()]

    trend_state = "Bullish" if avg_score > 2 else "Bearish" if avg_score < -2 else "Neutral"
    forecast_trend = "Upward" if avg_score > 2 else "Downward" if avg_score < -2 else "Stable"

    return {
        "signal": overall, "confidence": round(confidence, 2), "confidence_label": confidence_label(confidence),
        "trend_state": trend_state, "trend_strength": "High" if abs(avg_score) >= 6 else "Medium" if abs(avg_score) >= 3 else "Low",
        "forecast_trend": forecast_trend, "projected_change": f"{avg_score:+.2f} score",
        "regime": ", ".join(sorted(set(all_regimes))), "dominant_factors": list(dict.fromkeys(all_factors))[:8],
        "reasoning": " | ".join(explanations[:3]), "explanations": explanations, "score": avg_score, "timeframes": per_tf
    }


def compute_light_signal(change):
    if change >= 2.0: return "BUY"
    if change <= -2.0: return "SELL"
    return "HOLD"


MEM_CACHE = {}

def get_cached_payload(cache_key, ttl_seconds):
    mem = MEM_CACHE.get(cache_key)
    now = int(time.time())
    if mem and mem["data"] is not None and (now - mem["updated_at"] <= ttl_seconds):
        return mem["data"]
    db_payload = db.cache_get(cache_key, ttl_seconds)
    if db_payload is not None:
        MEM_CACHE[cache_key] = {"data": db_payload, "updated_at": now}
        return db_payload
    return None

def set_cached_payload(cache_key, payload):
    now = int(time.time())
    MEM_CACHE[cache_key] = {"data": payload, "updated_at": now}
    db.cache_set(cache_key, payload)

def get_stale_payload(cache_key):
    mem = MEM_CACHE.get(cache_key)
    if mem and mem["data"] is not None:
        return mem["data"]
    return db.cache_get_stale(cache_key)


def fetch_crypto_candles(symbol, interval="15m", limit=80):
    yf_interval_map = {"15m": ("15m", "5d"), "1h": ("60m", "20d"), "4h": ("1d", "3mo")}
    yf_sym = f"{symbol.upper()}-USD"
    y_int, y_per = yf_interval_map.get(interval, ("15m", "5d"))
    
    try:
        df = yf.download(yf_sym, interval=y_int, period=y_per, progress=False)
        if df is None or df.empty: return None
        
        candles = []
        for idx, row in df.tail(limit).iterrows():
            ts = normalize_dt_to_ts(idx.to_pydatetime()) or int(time.time())
            # Safely handle multi-index columns if newer yfinance version applies them
            open_p = float(row["Open"].iloc[0]) if isinstance(row["Open"], pd.Series) else float(row["Open"])
            high_p = float(row["High"].iloc[0]) if isinstance(row["High"], pd.Series) else float(row["High"])
            low_p = float(row["Low"].iloc[0]) if isinstance(row["Low"], pd.Series) else float(row["Low"])
            close_p = float(row["Close"].iloc[0]) if isinstance(row["Close"], pd.Series) else float(row["Close"])
            
            candles.append({"ts": ts, "open": open_p, "high": high_p, "low": low_p, "close": close_p})
        return candles or None
    except Exception as e:
        logger.warning(f"yfinance crypto candles failed for {symbol}: {e}")
        return None


def fetch_stock_candles(symbol, period="6mo", interval="1d"):
    try:
        ticker = yf.Ticker(symbol.upper())
        hist = ticker.history(period=period, interval=interval, auto_adjust=False)
        if hist is None or hist.empty: return None

        candles = []
        for idx, row in hist.tail(120).iterrows():
            ts = normalize_dt_to_ts(idx.to_pydatetime()) or int(time.time())
            candles.append({
                "ts": ts,
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"])
            })
        return candles or None
    except Exception:
        return None


def fetch_crypto_multi_timeframe(symbol):
    return {
        "15m": fetch_crypto_candles(symbol, interval="15m", limit=100),
        "1h": fetch_crypto_candles(symbol, interval="1h", limit=100),
        "4h": fetch_crypto_candles(symbol, interval="4h", limit=100),
    }


def fetch_stock_multi_timeframe(symbol):
    return {
        "1d": fetch_stock_candles(symbol, period="6mo", interval="1d"),
        "1wk": fetch_stock_candles(symbol, period="2y", interval="1wk"),
        "1mo": fetch_stock_candles(symbol, period="5y", interval="1mo"),
    }


def fetch_crypto_quotes_safe(force_refresh=False):
    cache_key = "crypto_list"
    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.CRYPTO_CACHE_TTL)
        if cached is not None: return cached

    symbols_map = {f"{s}-USD": s for s, _ in CRYPTO_TOP_90}
    symbols_str = " ".join(symbols_map.keys())
    results = []

    try:
        data = yf.download(symbols_str, period="7d", interval="1d", group_by='ticker', threads=True, progress=False)
        for yf_sym, original_sym in symbols_map.items():
            try:
                df = data[yf_sym] if len(symbols_map) > 1 else data
                # Fill missing weekend gaps
                series = df['Close'].ffill().dropna()
                
                if len(series) < 1: continue
                last_close = float(series.iloc[-1])
                prev_close = float(series.iloc[-2]) if len(series) > 1 else last_close
                change = pct_change(last_close, prev_close)
                
                results.append({
                    "symbol": original_sym,
                    "name": CRYPTO_NAME_MAP.get(original_sym, original_sym),
                    "price": last_close,
                    "change": change,
                    "dir": "up" if change >= 0 else "down",
                    "signal": compute_light_signal(change),
                    "logo": get_crypto_logo(original_sym),
                    "icon": "₿"
                })
            except Exception:
                continue

        if results:
            set_cached_payload(cache_key, results)
            return results
    except Exception as e:
        logger.warning(f"Bulk yfinance crypto fetch failed: {e}")

    return get_stale_payload(cache_key) or []


def fetch_stock_quotes_safe(force_refresh=False):
    cache_key = "stock_list"
    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.STOCK_CACHE_TTL)
        if cached is not None: return cached

    symbols = [s for s, _ in STOCK_UNIVERSE]
    symbols_str = " ".join(symbols)
    results = []
    
    try:
        data = yf.download(symbols_str, period="7d", interval="1d", group_by='ticker', threads=True, progress=False)
        for symbol, name in STOCK_UNIVERSE:
            try:
                df = data[symbol] if len(symbols) > 1 else data
                # Fill missing weekend gaps for Gold/Oil/Stocks
                series = df['Close'].ffill().dropna()
                
                if len(series) < 1: continue
                last_close = float(series.iloc[-1])
                prev_close = float(series.iloc[-2]) if len(series) > 1 else last_close
                change = pct_change(last_close, prev_close)
                
                results.append({
                    "symbol": symbol,
                    "name": name,
                    "price": last_close,
                    "change": change,
                    "dir": "up" if change >= 0 else "down",
                    "signal": compute_light_signal(change),
                    "logo": get_stock_logo(symbol),
                    "icon": get_asset_icon(symbol),
                })
            except Exception:
                continue

        if results:
            set_cached_payload(cache_key, results)
            return results
    except Exception as e:
        logger.warning(f"Bulk yfinance stock fetch failed: {e}")

    return get_stale_payload(cache_key) or []


def paginate(items, page, per_page):
    total = len(items)
    pages = max(1, math.ceil(total / per_page)) if total > 0 else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    return items[start:start + per_page], total, pages, page

def detail_cache_key(asset_type, symbol):
    return f"detail::{asset_type}::{symbol.upper()}"


def build_crypto_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in fetch_crypto_quotes_safe()}
    if symbol not in light_map: return None

    asset = dict(light_map[symbol])
    tf_map = fetch_crypto_multi_timeframe(symbol)
    primary_candles = tf_map.get("15m") or tf_map.get("1h") or tf_map.get("4h") or []
    agg = aggregate_multi_timeframe_brain("crypto", symbol, tf_map)

    feats = extract_market_features(primary_candles)
    asset.update({
        "price_display": fmt_price(asset["price"], asset["symbol"]),
        "change_display": fmt_change(asset["change"]),
        "detail_candles": render_candles_from_ohlc(primary_candles) if primary_candles else fallback_candles_html(99),
        "signal_meta": {
            "signal": agg["signal"], "confidence": agg["confidence"], "confidence_label": agg["confidence_label"],
            "confidence_note": "Confidence reflects internal indicator agreement, not guaranteed outcome probability.",
            "rsi": round(feats["rsi"], 2) if feats else 50.0, "momentum": round(feats["move_5"], 2) if feats else 0.0,
            "summary": agg["reasoning"], "why": agg["explanations"],
        },
        "forecast": {
            "trend": agg["forecast_trend"], "projected_change": agg["projected_change"],
            "confidence_band": f"{int(agg['confidence'] * 100)}%",
            "summary": f"Multi-timeframe crypto forecast: {agg['forecast_trend'].lower()} with {agg['trend_strength'].lower()} conviction."
        },
        "trend_data": {
            "state": agg["trend_state"], "strength": agg["trend_strength"], "read": agg["regime"],
            "summary": f"Dominant factors: {', '.join(agg['dominant_factors'][:4])}."
        },
        "signal": agg["signal"], "multi_timeframes": agg["timeframes"], "overall_brain": agg
    })

    for tf, item in agg["timeframes"].items():
        db.upsert_signal_snapshot_if_changed("crypto", symbol, tf, item["signal_type"], item["signal"], item["confidence"], item["confidence_label"], item["regime"], item["trend_state"], asset["price"], item["explanations"], item["dominant_factors"])
    return asset


def build_stock_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in fetch_stock_quotes_safe()}
    if symbol not in light_map: return None

    asset = dict(light_map[symbol])
    tf_map = fetch_stock_multi_timeframe(symbol)
    primary_candles = tf_map.get("1d") or tf_map.get("1wk") or tf_map.get("1mo") or []
    agg = aggregate_multi_timeframe_brain("stock", symbol, tf_map)

    feats = extract_market_features(primary_candles)
    asset.update({
        "price_display": fmt_price(asset["price"], asset["symbol"]),
        "change_display": fmt_change(asset["change"]),
        "detail_candles": render_candles_from_ohlc(primary_candles) if primary_candles else fallback_candles_html(101),
        "signal_meta": {
            "signal": agg["signal"], "confidence": agg["confidence"], "confidence_label": agg["confidence_label"],
            "confidence_note": "Confidence reflects internal indicator agreement, not guaranteed outcome probability.",
            "rsi": round(feats["rsi"], 2) if feats else 50.0, "momentum": round(feats["move_5"], 2) if feats else 0.0,
            "summary": agg["reasoning"], "why": agg["explanations"],
        },
        "forecast": {
            "trend": agg["forecast_trend"], "projected_change": agg["projected_change"],
            "confidence_band": f"{int(agg['confidence'] * 100)}%",
            "summary": f"Multi-timeframe forecast: {agg['forecast_trend'].lower()} with {agg['trend_strength'].lower()} conviction."
        },
        "trend_data": {
            "state": agg["trend_state"], "strength": agg["trend_strength"], "read": agg["regime"],
            "summary": f"Dominant factors: {', '.join(agg['dominant_factors'][:4])}."
        },
        "signal": agg["signal"], "multi_timeframes": agg["timeframes"], "overall_brain": agg
    })

    for tf, item in agg["timeframes"].items():
        db.upsert_signal_snapshot_if_changed("stock", symbol, tf, item["signal_type"], item["signal"], item["confidence"], item["confidence_label"], item["regime"], item["trend_state"], asset["price"], item["explanations"], item["dominant_factors"])
    return asset


def get_crypto_detail(symbol, force_refresh=False):
    cache_key = detail_cache_key("crypto", symbol)
    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.DETAIL_CACHE_TTL)
        if cached is not None: return cached
    asset = build_crypto_detail(symbol)
    if asset:
        set_cached_payload(cache_key, asset)
        return asset
    return get_stale_payload(cache_key)


def get_stock_detail(symbol, force_refresh=False):
    cache_key = detail_cache_key("stock", symbol)
    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.DETAIL_CACHE_TTL)
        if cached is not None: return cached
    asset = build_stock_detail(symbol)
    if asset:
        set_cached_payload(cache_key, asset)
        return asset
    return get_stale_payload(cache_key)


def refresh_single_detail_cache(asset_type, symbol):
    if asset_type == "crypto": return get_crypto_detail(symbol, force_refresh=True)
    return get_stock_detail(symbol, force_refresh=True)


def warm_detail_caches():
    for symbol in Config.DETAIL_WARM_CRYPTO:
        try: refresh_single_detail_cache("crypto", symbol)
        except: pass
    for symbol in Config.DETAIL_WARM_STOCKS:
        try: refresh_single_detail_cache("stock", symbol)
        except: pass


def evaluate_signal_outcome(signal, entry_price, current_price):
    if not entry_price or not current_price: return None, None
    ret = pct_change(current_price, entry_price)
    if signal == "BUY": outcome = "correct" if ret > 0 else "incorrect"
    elif signal == "SELL": outcome = "correct" if ret < 0 else "incorrect"
    else: outcome = "correct" if abs(ret) < 1.0 else "incorrect"
    return ret, outcome


def parse_sqlite_dt(v):
    try: return datetime.fromisoformat(str(v).replace(" ", "T"))
    except: return None


def select_price_near_target(points, target_dt):
    if not points: return None
    target_ts = int(target_dt.timestamp())
    ordered = sorted([p for p in points if p.get("ts") and p.get("close") is not None], key=lambda x: x["ts"])
    if not ordered: return None
    after = [p for p in ordered if p["ts"] >= target_ts]
    if after: return float(after[0]["close"])
    return float(ordered[-1]["close"])


def get_price_for_horizon(asset_type, symbol, created_dt, horizon):
    target_dt = created_dt + {"1h": timedelta(hours=1), "24h": timedelta(hours=24), "7d": timedelta(days=7)}[horizon]
    points = fetch_crypto_candles(symbol, interval="15m", limit=72) if asset_type == "crypto" else fetch_stock_candles(symbol, period="1mo", interval="1d")
    return select_price_near_target(points, target_dt)


def refresh_signal_outcomes():
    rows = db.get_recent_snapshots_for_outcomes(days=10, limit=400)
    now = datetime.utcnow()
    for row in rows:
        created = parse_sqlite_dt(row["created_at"])
        if not created: continue
        age_hours = (now - created).total_seconds() / 3600.0
        for horizon, threshold in [("1h", 1), ("24h", 24), ("7d", 24 * 7)]:
            if age_hours < threshold or db.outcome_exists(row["id"], horizon): continue
            price_at_horizon = get_price_for_horizon(row["asset_type"], row["symbol"], created, horizon)
            if not price_at_horizon: continue
            ret, outcome = evaluate_signal_outcome(row["signal"], row["price_at_signal"], price_at_horizon)
            if ret is not None: db.insert_signal_outcome(row["id"], horizon, price_at_horizon, ret, outcome)


def summarize_performance(rows):
    if not rows: return {"total": 0, "buy_accuracy": 0, "sell_accuracy": 0, "hold_accuracy": 0, "avg_return": 0, "best_assets": []}
    total = len(rows)
    buy, sell, hold = [r for r in rows if r["signal"] == "BUY"], [r for r in rows if r["signal"] == "SELL"], [r for r in rows if r["signal"] == "HOLD"]
    def acc(items): return round(100 * sum(1 for x in items if x["outcome"] == "correct") / len(items), 1) if items else 0
    avg_return = round(sum(float(r["return_pct"] or 0) for r in rows) / total, 2) if total else 0
    per_symbol = {}
    for r in rows: per_symbol.setdefault(r["symbol"], []).append(float(r["return_pct"] or 0))
    ranked = sorted(((sym, round(sum(vals) / len(vals), 2)) for sym, vals in per_symbol.items()), key=lambda x: x[1], reverse=True)
    return {"total": total, "buy_accuracy": acc(buy), "sell_accuracy": acc(sell), "hold_accuracy": acc(hold), "avg_return": avg_return, "best_assets": ranked[:5]}


def legal_disclaimer_html():
    return """
    <div class="card" style="margin-top:24px;">
      <h3>Disclaimer</h3>
      <p>AVA Markets provides technical market intelligence and indicator-based analysis for educational purposes only. It is not financial advice.</p>
    </div>
    """


def homepage_json_ld():
    return [
        {"@context": "https://schema.org", "@type": "Organization", "name": "AVA Markets", "url": Config.DOMAIN},
        {"@context": "https://schema.org", "@type": "WebSite", "name": "AVA Markets", "url": Config.DOMAIN, "potentialAction": {"@type": "SearchAction", "target": f"{Config.DOMAIN}/crypto?q={{search_term_string}}", "query-input": "required name=search_term_string"}}
    ]


def live_update_script(page_type=None, symbol=None, asset_type=None):
    if page_type == "crypto_list":
        return "<script>async function refreshCryptoList(){ try{ const res = await fetch('/api/live/crypto-list'); const data = await res.json(); document.getElementById('live-updated-crypto').textContent = 'Last updated: ' + data.updated_at + ' UTC'; data.items.forEach(item => { const p = document.getElementById('price-'+item.symbol); if(p) p.textContent = item.price_display; const c = document.getElementById('change-'+item.symbol); if(c) {c.textContent = item.change_display; c.className = item.dir;} const s = document.getElementById('signal-'+item.symbol); if(s) {s.textContent = item.signal; s.className = 'signal signal-' + item.signal.toLowerCase();} }); }catch(e){} } setInterval(refreshCryptoList, 30000);</script>"
    if page_type == "stock_list":
        return "<script>async function refreshStockList(){ try{ const res = await fetch('/api/live/stocks-list'); const data = await res.json(); document.getElementById('live-updated-stocks').textContent = 'Last updated: ' + data.updated_at + ' UTC'; data.items.forEach(item => { const safe = item.symbol.replace(/[^A-Za-z0-9]/g, '_'); const p = document.getElementById('price-'+safe); if(p) p.textContent = item.price_display; const c = document.getElementById('change-'+safe); if(c) {c.textContent = item.change_display; c.className = item.dir;} const s = document.getElementById('signal-'+safe); if(s) {s.textContent = item.signal; s.className = 'signal signal-' + item.signal.toLowerCase();} }); }catch(e){} } setInterval(refreshStockList, 45000);</script>"
    if page_type == "detail":
        interval = 30000 if asset_type == "crypto" else 45000
        return f"<script>async function refreshDetail(){{ try{{ const res = await fetch('/api/live/{asset_type}/{symbol.upper()}'); const data = await res.json(); document.getElementById('detail-price').textContent = data.price_display; const c = document.getElementById('detail-change'); c.textContent = data.change_display; c.className = data.dir; const s = document.getElementById('detail-signal'); s.textContent = data.signal; s.className = 'signal signal-' + data.signal.toLowerCase(); document.getElementById('detail-updated').textContent = 'Last updated: ' + data.updated_at + ' UTC'; }}catch(e){{}} }} setInterval(refreshDetail, {interval});</script>"
    return ""


def nav_layout(title, content, extra_head="", meta_description=None, canonical_url=None, og_type="website", robots_content="index, follow", json_ld_override=None):
    if not meta_description: meta_description = "AVA Markets provides crypto, stock, and commodity market intelligence with live updates and signals."
    if not canonical_url: canonical_url = f"{Config.DOMAIN}{request.path}"
    json_ld_payload = json_ld_override if json_ld_override is not None else {"@context": "https://schema.org", "@type": "WebPage", "name": title, "url": canonical_url, "description": meta_description}

    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{ title }}</title><meta name="description" content="{{ meta_description }}"><meta name="robots" content="{{ robots_content }}"><link rel="canonical" href="{{ canonical_url }}">
  <meta property="og:title" content="{{ title }}"><meta property="og:description" content="{{ meta_description }}"><meta property="og:type" content="{{ og_type }}"><meta property="og:url" content="{{ canonical_url }}">
  <script type="application/ld+json">{{ json_ld_payload|tojson|safe }}</script>
  <style>{{ css }}</style>{{ extra_head|safe }}
</head>
<body>
  <div class="container">
    <nav class="nav">
      <div class="logo"><a href="/">AVA Markets</a></div>
      <div class="nav-links">
        <a href="/">Home</a><a href="/crypto">Crypto</a><a href="/stocks">Stocks</a><a href="/pricing">Pricing</a>
        {% if user %}<a href="/dashboard">Dashboard</a><a href="/logout">Logout</a>{% else %}<a href="/login">Login</a><a href="/register">Register</a>{% endif %}
      </div>
    </nav>
    {{ content|safe }}
    <div class="footer">AVA Markets © 2026 — crypto, stocks, signals, live updates, trends, and forecasts.</div>
  </div>
</body>
</html>
    """, title=title, content=content, css=CSS, user=g.get("user"), extra_head=extra_head, meta_description=meta_description, canonical_url=canonical_url, og_type=og_type, robots_content=robots_content, json_ld_payload=json_ld_payload)


def get_web_user(): return db.get_user_by_session(request.cookies.get("session_token"))
def get_web_admin(): return db.get_admin_session(request.cookies.get("admin_token"))

@app.before_request
def load_request_state():
    g.user = get_web_user()
    g.admin = get_web_admin()

def require_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.user: return redirect("/login")
        return fn(*args, **kwargs)
    return wrapper

def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.admin: return redirect("/admin/login")
        return fn(*args, **kwargs)
    return wrapper

def is_paid_active(): return bool(g.user and g.user.get("subscription_status") in ["active", "trialing"])
def current_tier(): return g.user["tier"] if g.user else "free"
def can_access_signals(): return is_paid_active() and current_tier() in ["basic", "pro", "elite", "enterprise"]
def can_access_forecast(): return is_paid_active() and current_tier() in ["pro", "elite", "enterprise"]
def can_access_trends(): return is_paid_active() and current_tier() in ["pro", "elite", "enterprise"]

@app.route("/api/live/crypto-list")
@limiter.limit("120 per minute")
def api_live_crypto_list():
    items = [{"symbol": a["symbol"], "price_display": fmt_price(a["price"], a["symbol"]), "change_display": fmt_change(a["change"]), "dir": a["dir"], "signal": a["signal"]} for a in fetch_crypto_quotes_safe()]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})

@app.route("/api/live/stocks-list")
@limiter.limit("120 per minute")
def api_live_stocks_list():
    items = [{"symbol": a["symbol"], "price_display": fmt_price(a["price"], a["symbol"]), "change_display": fmt_change(a["change"]), "dir": a["dir"], "signal": a["signal"]} for a in fetch_stock_quotes_safe()]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})

@app.route("/api/live/crypto/<symbol>")
@limiter.limit("60 per minute")
def api_live_crypto_detail(symbol):
    asset = get_crypto_detail(symbol)
    if not asset: return jsonify({"error": "not_found"}), 404
    return jsonify({"symbol": asset["symbol"], "price_display": asset["price_display"], "change_display": asset["change_display"], "dir": asset["dir"], "signal": asset["signal"], "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})

@app.route("/api/live/stock/<symbol>")
@limiter.limit("60 per minute")
def api_live_stock_detail(symbol):
    asset = get_stock_detail(symbol)
    if not asset: return jsonify({"error": "not_found"}), 404
    return jsonify({"symbol": asset["symbol"], "price_display": asset["price_display"], "change_display": asset["change_display"], "dir": asset["dir"], "signal": asset["signal"], "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")})


@app.route("/")
def home():
    crypto_list = fetch_crypto_quotes_safe()
    stock_list = fetch_stock_quotes_safe()

    fc = dict(crypto_list[0]) if crypto_list else {"symbol": "N/A", "name": "N/A", "price": 0.0, "change": 0.0, "dir": "down", "signal": "HOLD", "logo": None}
    fs = dict(stock_list[0]) if stock_list else {"symbol": "N/A", "name": "N/A", "price": 0.0, "change": 0.0, "dir": "down", "signal": "HOLD", "icon": "📈"}

    fc["price_display"], fc["change_display"] = fmt_price(fc["price"], fc["symbol"]), fmt_change(fc["change"])
    fs["price_display"], fs["change_display"] = fmt_price(fs["price"], fs["symbol"]), fmt_change(fs["change"])

    content = render_template_string("""
    <section class="hero">
      <div class="hero-card">
        <div class="badge">AVA Markets Core</div>
        <h1>Live-feeling market intelligence for crypto, stocks, and commodities.</h1>
        <div class="btns"><a class="btn btn-primary" href="/crypto">Explore Crypto</a><a class="btn btn-secondary" href="/stocks">Explore Stocks</a></div>
      </div>
      <div class="card featured-shell">
        <div class="badge">Featured {{ fc.symbol }}</div>
        <div style="font-size:2.4rem;font-weight:800;">{{ fc.price_display }} <span class="{{ fc.dir }}" style="font-size:1rem;">{{ fc.change_display }}</span></div>
        <div class="candle-box">{{ crypto_candles|safe }}</div>
      </div>
    </section>
    """, fc=fc, crypto_candles=fallback_candles_html(1))

    return nav_layout("AVA Markets - Crypto & Stock Signals", content, json_ld_override=homepage_json_ld())


@app.route("/crypto")
def crypto():
    page, search = get_int_arg("page", 1), (request.args.get("q") or "").strip().lower()
    assets = [dict(a) for a in fetch_crypto_quotes_safe()]
    
    for a in assets: 
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])
        
    if search: 
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]
    
    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_CRYPTO)
    
    rows = ""
    for a in page_items:
        fallback_html = f"<span class='asset-icon'>{h(a.get('icon','₿'))}</span>"
        media = f'<img class="asset-logo" src="{h(a.get("logo",""))}" onerror="this.outerHTML=`{fallback_html}`">'
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">{media}<a href="/crypto/{h(a['symbol'])}">{h(a['symbol'])}</a></strong>
            <span>{h(a['name'])}</span>
          </td>
          <td id="price-{h(a['symbol'])}">{h(a['price_display'])}</td>
          <td id="change-{h(a['symbol'])}" class="{a['dir']}">{h(a['change_display'])}</td>
          <td><span class="signal signal-{a['signal'].lower()}">{a['signal']}</span></td>
        </tr>
        """

    content = f"""
    <section class="section">
      <h1>Crypto</h1>
      <div id="live-updated-crypto" class="live-stamp">Last updated: {h(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))} UTC</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>24h</th><th>Signal</th></tr>
          {rows or "<tr><td colspan='4'>No data available.</td></tr>"}
        </table>
      </div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout("Crypto Markets - AVA", content, extra_head=live_update_script("crypto_list"))


@app.route("/stocks")
def stocks():
    page, search = get_int_arg("page", 1), (request.args.get("q") or "").strip().lower()
    assets = [dict(a) for a in fetch_stock_quotes_safe()]
    
    for a in assets: 
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])
        
    if search: 
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]
    
    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_STOCKS)
    
    rows = ""
    for a in page_items:
        safe_id = h(a["symbol"].replace("=", "_"))
        fallback_html = f"<span class='asset-icon'>{h(a.get('icon','📈'))}</span>"
        media = f'<img class="asset-logo" src="{h(a.get("logo",""))}" onerror="this.outerHTML=`{fallback_html}`">'
        
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">{media}<a href="/stocks/{h(a['symbol'])}">{h(a['symbol'])}</a></strong>
            <span>{h(a['name'])}</span>
          </td>
          <td id="price-{safe_id}">{h(a['price_display'])}</td>
          <td id="change-{safe_id}" class="{a['dir']}">{h(a['change_display'])}</td>
          <td><span class="signal signal-{a['signal'].lower()}">{a['signal']}</span></td>
        </tr>
        """

    content = f"""
    <section class="section">
      <h1>Stocks + Commodities</h1>
      <div id="live-updated-stocks" class="live-stamp">Last updated: {h(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))} UTC</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>Signal</th></tr>
          {rows or "<tr><td colspan='4'>No data available.</td></tr>"}
        </table>
      </div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout("Stock Markets - AVA", content, extra_head=live_update_script("stock_list"))


@app.route("/crypto/<symbol>")
def crypto_detail(symbol):
    asset = get_crypto_detail(symbol)
    if not asset: abort(404)
    content = f"""
    <section class="section">
      <h1>{h(asset['symbol'])}</h1>
      <div id="detail-price" style="font-size:2rem; font-weight:800;">{h(asset['price_display'])}</div>
      <div id="detail-change" class="{asset['dir']}">{h(asset['change_display'])}</div>
      <div class="card" style="margin-top:20px;">{asset['detail_candles']}</div>
    </section>
    """
    return nav_layout(f"{asset['symbol']} - AVA Markets", content, extra_head=live_update_script("detail", symbol, "crypto"))


@app.route("/stocks/<symbol>")
def stock_detail(symbol):
    asset = get_stock_detail(symbol)
    if not asset: abort(404)
    content = f"""
    <section class="section">
      <h1>{h(asset['symbol'])}</h1>
      <div id="detail-price" style="font-size:2rem; font-weight:800;">{h(asset['price_display'])}</div>
      <div id="detail-change" class="{asset['dir']}">{h(asset['change_display'])}</div>
      <div class="card" style="margin-top:20px;">{asset['detail_candles']}</div>
    </section>
    """
    return nav_layout(f"{asset['symbol']} - AVA Markets", content, extra_head=live_update_script("detail", symbol, "stock"))


@app.route("/register", methods=["GET", "POST"])
def register():
    if g.user: return redirect("/dashboard")
    if request.method == "GET":
        return nav_layout("Register", "<div class='form-shell'><div class='form-card'><h1>Register</h1><form method='POST'><input type='email' name='email' placeholder='Email' required><input type='password' name='password' placeholder='Password' required><button type='submit'>Register</button></form></div></div>")
    email, password = request.form.get("email", "").strip(), request.form.get("password", "").strip()
    user = db.create_user(email, password)
    if not user: return redirect("/register")
    token = db.create_session(user["id"])
    resp = make_response(redirect("/dashboard"))
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", secure=Config.COOKIE_SECURE)
    return resp


@app.route("/login", methods=["GET", "POST"])
def login():
    if g.user: return redirect("/dashboard")
    if request.method == "GET":
        return nav_layout("Login", "<div class='form-shell'><div class='form-card'><h1>Login</h1><form method='POST'><input type='email' name='email' placeholder='Email' required><input type='password' name='password' placeholder='Password' required><button type='submit'>Login</button></form></div></div>")
    email, password = request.form.get("email", "").strip(), request.form.get("password", "").strip()
    user = db.verify_user(email, password)
    if not user: return redirect("/login")
    token = db.create_session(user["id"])
    resp = make_response(redirect("/dashboard"))
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", secure=Config.COOKIE_SECURE)
    return resp


@app.route("/logout")
def logout():
    token = request.cookies.get("session_token")
    if token: db.delete_session(token)
    resp = make_response(redirect("/"))
    resp.delete_cookie("session_token")
    return resp


@app.route("/dashboard")
@require_login
def dashboard():
    return nav_layout("Dashboard", f"<section class='section'><h1>Welcome {h(g.user['email'])}</h1><p>Your tier: {h(g.user['tier'])}</p></section>")


@app.route("/pricing")
def pricing():
    return nav_layout("Pricing", "<section class='section'><h1>Pricing</h1><div class='market-grid'><div class='price-card'><h3>Basic</h3><p>$9/mo</p></div><div class='price-card'><h3>Pro</h3><p>$29/mo</p></div></div></section>")


@app.errorhandler(404)
def not_found(e): return nav_layout("404", "<section class='section'><h1>404 Not Found</h1></section>"), 404

@app.errorhandler(500)
def server_error(e): return nav_layout("500", "<section class='section'><h1>500 Error</h1></section>"), 500


_bg_started = False

def background_refresh_loop():
    while True:
        try: fetch_crypto_quotes_safe(force_refresh=True)
        except: pass
        try: fetch_stock_quotes_safe(force_refresh=True)
        except: pass
        try: warm_detail_caches()
        except: pass
        try: refresh_signal_outcomes()
        except: pass
        try: db.purge_expired_rows()
        except: pass
        time.sleep(max(60, Config.BACKGROUND_REFRESH_SECONDS))


def start_background_refresh():
    global _bg_started
    if _bg_started: return
    if not Config.ENABLE_BACKGROUND_REFRESH or not Config.BACKGROUND_REFRESH_LEADER: return

    def init_and_loop():
        # INITIAL FAST WARMUP (Solves the 502 error)
        try: fetch_crypto_quotes_safe(force_refresh=True)
        except: pass
        try: fetch_stock_quotes_safe(force_refresh=True)
        except: pass
        
        # CONTINUOUS LOOP
        background_refresh_loop()

    t = threading.Thread(target=init_and_loop, daemon=True)
    t.start()
    _bg_started = True
    logger.info("Background thread started. App booted successfully.")


if (not Config.DEBUG) or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    start_background_refresh()

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
