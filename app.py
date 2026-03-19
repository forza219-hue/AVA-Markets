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

    # SQLite path only in this file. If you migrate to Postgres, swap DB layer first.
    DATABASE = os.environ.get("DATABASE_URL", "ava_markets_core.db").strip()

    SECRET_KEY = os.environ.get("SECRET_KEY", "").strip()
    DOMAIN = os.environ.get("DOMAIN", "").strip().rstrip("/")

    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()

    ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "").strip()
    ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "").strip()

    CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "hello@avamarkets.com").strip()

    REQUESTS_TIMEOUT = int(os.environ.get("REQUESTS_TIMEOUT", "12"))
    FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
    TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "").strip()

    CRYPTO_CACHE_TTL = int(os.environ.get("CRYPTO_CACHE_TTL", "300"))
    STOCK_CACHE_TTL = int(os.environ.get("STOCK_CACHE_TTL", "600"))
    DETAIL_CACHE_TTL = int(os.environ.get("DETAIL_CACHE_TTL", "300"))

    PAGE_SIZE_CRYPTO = 25
    PAGE_SIZE_STOCKS = 20

    COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "true" if not DEBUG else "false").lower() == "true"

    ENABLE_BACKGROUND_REFRESH = os.environ.get("ENABLE_BACKGROUND_REFRESH", "true").lower() == "true"
    BACKGROUND_REFRESH_SECONDS = int(os.environ.get("BACKGROUND_REFRESH_SECONDS", "180"))
    BACKGROUND_REFRESH_LEADER = os.environ.get("BACKGROUND_REFRESH_LEADER", "true").lower() == "true"

    DETAIL_WARM_CRYPTO = [
        s.strip().upper() for s in os.environ.get(
            "DETAIL_WARM_CRYPTO", "BTC,ETH,SOL,XRP,DOGE,ADA,AVAX,LINK"
        ).split(",") if s.strip()
    ]
    DETAIL_WARM_STOCKS = [
        s.strip().upper() for s in os.environ.get(
            "DETAIL_WARM_STOCKS", "AAPL,MSFT,NVDA,AMZN,GOOGL,TSLA,GC=F,CL=F"
        ).split(",") if s.strip()
    ]

    RATE_LIMIT_STORAGE_URI = os.environ.get("RATE_LIMIT_STORAGE_URI", "memory://")

    TIERS = {
        "free": {"price": 0},
        "basic": {"price": 9},
        "pro": {"price": 29},
        "elite": {"price": 79},
        "enterprise": {"price": None},
    }


def validate_runtime_config():
    missing = []

    for key in ["SECRET_KEY", "DOMAIN", "ADMIN_USERNAME", "ADMIN_PASSWORD"]:
        if not getattr(Config, key, ""):
            missing.append(key)

    if Config.STRIPE_SECRET_KEY and not Config.STRIPE_WEBHOOK_SECRET:
        missing.append("STRIPE_WEBHOOK_SECRET")

    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

    if Config.DATABASE.startswith(("postgres://", "postgresql://")):
        raise RuntimeError(
            "This file currently uses sqlite3 + WAL hardening. "
            "If you want PostgreSQL, migrate the DB layer first."
        )

    if not Config.DOMAIN.startswith(("https://", "http://")):
        raise RuntimeError("DOMAIN must include protocol, e.g. https://yourdomain.com")

    if not Config.DEBUG and Config.DOMAIN.startswith("http://"):
        logger.warning("DOMAIN is using http:// in non-debug mode. Prefer https:// in production.")

    logger.info("Config validated. SQLite WAL mode will be enabled.")
    logger.info("Postgres migration plan: move users/sessions/payments/watchlists/signal_* and market_cache first.")


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
    logger.warning("Flask-Limiter not installed; rate limiting is disabled.")

    class _NoopLimiter:
        def limit(self, *args, **kwargs):
            def deco(fn):
                return fn
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

COINGECKO_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "BNB": "binancecoin", "SOL": "solana", "XRP": "ripple",
    "DOGE": "dogecoin", "ADA": "cardano", "AVAX": "avalanche-2", "LINK": "chainlink", "DOT": "polkadot",
    "MATIC": "matic-network", "LTC": "litecoin", "BCH": "bitcoin-cash", "ATOM": "cosmos", "UNI": "uniswap",
    "NEAR": "near", "APT": "aptos", "ARB": "arbitrum", "OP": "optimism", "SUI": "sui",
    "SHIB": "shiba-inu", "TRX": "tron", "ETC": "ethereum-classic", "XLM": "stellar",
    "HBAR": "hedera-hashgraph", "ICP": "internet-computer", "FIL": "filecoin",
    "INJ": "injective-protocol", "RNDR": "render-token", "AAVE": "aave",
}

KRAKEN_PAIRS = {
    "BTC": "XBTUSD", "ETH": "ETHUSD", "SOL": "SOLUSD", "XRP": "XRPUSD",
    "DOGE": "DOGEUSD", "ADA": "ADAUSD", "AVAX": "AVAXUSD", "DOT": "DOTUSD",
    "LINK": "LINKUSD", "LTC": "LTCUSD", "BCH": "BCHUSD", "ATOM": "ATOMUSD",
    "UNI": "UNIUSD", "TRX": "TRXUSD", "ETC": "ETCUSD", "XLM": "XLMUSD",
}

KRAKEN_OHLC_MAP = {
    "BTC": "XXBTZUSD", "ETH": "XETHZUSD", "SOL": "SOLUSD", "XRP": "XXRPZUSD",
    "DOGE": "XDGUSD", "ADA": "ADAUSD", "AVAX": "AVAXUSD", "DOT": "DOTUSD",
    "LINK": "LINKUSD", "LTC": "XLTCZUSD", "BCH": "BCHUSD", "ATOM": "ATOMUSD",
    "UNI": "UNIUSD", "TRX": "TRXUSD", "ETC": "XETCZUSD", "XLM": "XXLMZUSD",
}


def h(value):
    return html.escape("" if value is None else str(value), quote=True)


def safe_query_value(value):
    return quote_plus("" if value is None else str(value))


def get_stock_logo(symbol):
    domain = STOCK_DOMAINS.get(symbol.upper())
    return f"https://logo.clearbit.com/{domain}" if domain else None


def get_crypto_logo(symbol, provider_logo=None):
    if provider_logo:
        return provider_logo
    return f"https://cryptoicons.org/api/icon/{symbol.lower()}/200"


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
        CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires_at ON admin_sessions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_watchlists_user_id ON watchlists(user_id);
        CREATE INDEX IF NOT EXISTS idx_signal_snapshots_lookup ON signal_snapshots(asset_type, symbol, timeframe, signal_type, id DESC);
        CREATE INDEX IF NOT EXISTS idx_signal_snapshots_created_at ON signal_snapshots(created_at);
        CREATE INDEX IF NOT EXISTS idx_signal_outcomes_snapshot_horizon ON signal_outcomes(snapshot_id, horizon);
        CREATE INDEX IF NOT EXISTS idx_signal_changes_lookup ON signal_changes(asset_type, symbol, timeframe, signal_type, id DESC);
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

    def get_user_by_stripe_customer(self, customer_id):
        c = self.conn()
        row = c.execute("SELECT * FROM users WHERE stripe_customer_id = ?", (customer_id,)).fetchone()
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
        rows = c.execute("""
            SELECT id, email, tier, subscription_status, created_at
            FROM users
            ORDER BY id DESC
        """).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def get_all_payments(self):
        c = self.conn()
        rows = c.execute("""
            SELECT p.*, u.email
            FROM payments p
            LEFT JOIN users u ON u.id = p.user_id
            ORDER BY p.id DESC
            LIMIT 100
        """).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def add_watchlist(self, user_id, asset_type, symbol):
        c = self.conn()
        c.execute("""
            INSERT OR IGNORE INTO watchlists (user_id, asset_type, symbol)
            VALUES (?, ?, ?)
        """, (user_id, asset_type, symbol.upper()))
        c.commit()
        c.close()

    def remove_watchlist(self, user_id, asset_type, symbol):
        c = self.conn()
        c.execute("""
            DELETE FROM watchlists
            WHERE user_id = ? AND asset_type = ? AND symbol = ?
        """, (user_id, asset_type, symbol.upper()))
        c.commit()
        c.close()

    def get_watchlist(self, user_id):
        c = self.conn()
        rows = c.execute("""
            SELECT * FROM watchlists
            WHERE user_id = ?
            ORDER BY created_at DESC
        """, (user_id,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def latest_snapshot(self, asset_type, symbol, timeframe, signal_type):
        c = self.conn()
        row = c.execute("""
            SELECT * FROM signal_snapshots
            WHERE asset_type = ? AND symbol = ? AND timeframe = ? AND signal_type = ?
            ORDER BY id DESC
            LIMIT 1
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
        rows = c.execute("""
            SELECT * FROM signal_changes
            ORDER BY id DESC
            LIMIT ?
        """, (limit,)).fetchall()
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
        row = c.execute("""
            SELECT 1
            FROM signal_outcomes
            WHERE snapshot_id = ? AND horizon = ?
            LIMIT 1
        """, (snapshot_id, horizon)).fetchone()
        c.close()
        return bool(row)

    def get_recent_snapshots_for_outcomes(self, days=10, limit=500):
        c = self.conn()
        rows = c.execute("""
            SELECT *
            FROM signal_snapshots
            WHERE created_at >= datetime('now', ?)
            ORDER BY id DESC
            LIMIT ?
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
            WHERE s.created_at >= datetime('now', ?)
            ORDER BY s.id DESC
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
            try:
                prev_factors = json.loads(prev.get("factors_json") or "[]")
            except Exception:
                pass

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
        row = c.execute("""
            SELECT payload_json, updated_at
            FROM market_cache
            WHERE cache_key = ?
        """, (cache_key,)).fetchone()
        c.close()

        if not row:
            return None

        age = int(time.time()) - int(row["updated_at"])
        if age > ttl_seconds:
            return None

        try:
            return json.loads(row["payload_json"])
        except Exception:
            return None

    def cache_get_stale(self, cache_key):
        c = self.conn()
        row = c.execute("""
            SELECT payload_json
            FROM market_cache
            WHERE cache_key = ?
        """, (cache_key,)).fetchone()
        c.close()

        if not row:
            return None

        try:
            return json.loads(row["payload_json"])
        except Exception:
            return None

    def cache_set(self, cache_key, payload):
        c = self.conn()
        c.execute("""
            INSERT INTO market_cache (cache_key, payload_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(cache_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
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
        if not stripe or not Config.STRIPE_SECRET_KEY:
            return None

        prices = {"basic": 900, "pro": 2900, "elite": 7900}
        if tier not in prices:
            return None

        try:
            return stripe.checkout.Session.create(
                payment_method_types=["card"],
                customer_email=email,
                client_reference_id=str(user_id),
                line_items=[{
                    "price_data": {
                        "currency": "usd",
                        "product_data": {"name": f"AVA Markets - {tier.title()}"},
                        "unit_amount": prices[tier],
                        "recurring": {"interval": "month"}
                    },
                    "quantity": 1
                }],
                mode="subscription",
                success_url=success_url,
                cancel_url=cancel_url,
                metadata={"user_id": str(user_id), "tier": tier}
            )
        except Exception as e:
            logger.error(f"Stripe checkout error: {e}")
            return None

    @staticmethod
    def create_portal(customer_id, return_url):
        if not stripe or not Config.STRIPE_SECRET_KEY:
            return None
        try:
            return stripe.billing_portal.Session.create(customer=customer_id, return_url=return_url)
        except Exception as e:
            logger.error(f"Stripe portal error: {e}")
            return None


sm = StripeManager()


def pct_change(a, b):
    if b in [0, None]:
        return 0.0
    return ((a - b) / b) * 100.0


def fmt_price(v, symbol=None):
    if symbol == "ASML":
        return f"€{v:,.2f}"
    if v >= 1:
        return f"${v:,.2f}"
    return f"${v:.4f}"


def fmt_change(v):
    return f"{v:+.2f}%"


def get_int_arg(name, default=1, min_value=1, max_value=100000):
    raw = request.args.get(name, default)
    try:
        val = int(raw)
    except Exception:
        val = default
    return max(min_value, min(max_value, val))


def safe_redirect_target(default="/dashboard"):
    ref = request.referrer or ""
    if not ref:
        return default
    try:
        parsed_ref = urlparse(ref)
        parsed_base = urlparse(Config.DOMAIN)
        if parsed_ref.netloc and parsed_ref.netloc != parsed_base.netloc:
            return default
        path = parsed_ref.path or default
        if not path.startswith("/"):
            return default
        if parsed_ref.query:
            path = f"{path}?{parsed_ref.query}"
        return path
    except Exception:
        return default


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
    if not candles:
        return fallback_candles_html()

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
    try:
        return int(value.timestamp())
    except Exception:
        return None


def fetch_crypto_candles_kraken(symbol, interval="15m", limit=80):
    interval_map = {"15m": 15, "1h": 60, "4h": 240}
    pair = KRAKEN_OHLC_MAP.get(symbol.upper())
    if not pair:
        return None

    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": pair, "interval": interval_map.get(interval, 15)},
            timeout=Config.REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        payload = r.json()

        if payload.get("error"):
            raise Exception(str(payload["error"]))

        result = payload.get("result", {})
        rows = result.get(pair) or []
        candles = []
        for row in rows[-limit:]:
            candles.append({
                "ts": int(float(row[0])),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4])
            })
        return candles or None
    except Exception as e:
        logger.warning(f"fetch_crypto_candles_kraken failed for {symbol}: {e}")
        return None


def fetch_crypto_candles_kucoin(symbol, interval="15m", limit=80):
    tf_map = {"15m": "15min", "1h": "1hour", "4h": "4hour"}
    market = f"{symbol.upper()}-USDT"
    try:
        r = requests.get(
            "https://api.kucoin.com/api/v1/market/candles",
            params={"type": tf_map.get(interval, "15min"), "symbol": market},
            timeout=Config.REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        payload = r.json()
        data = payload.get("data") or []
        candles = []
        for row in data[:limit]:
            candles.append({
                "ts": int(row[0]),
                "open": float(row[1]),
                "close": float(row[2]),
                "high": float(row[3]),
                "low": float(row[4]),
            })
        candles = sorted(candles, key=lambda x: x["ts"])
        normalized = [{
            "ts": c["ts"],
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"]
        } for c in candles]
        return normalized or None
    except Exception as e:
        logger.warning(f"fetch_crypto_candles_kucoin failed for {symbol}: {e}")
        return None


def fetch_crypto_candles(symbol, interval="15m", limit=80):
    return (
        fetch_crypto_candles_kraken(symbol, interval=interval, limit=limit)
        or fetch_crypto_candles_kucoin(symbol, interval=interval, limit=limit)
    )


def fetch_stock_candles(symbol, period="6mo", interval="1d"):
    try:
        ticker = yf.Ticker(symbol.upper())
        hist = ticker.history(period=period, interval=interval, auto_adjust=False)
        if hist is None or hist.empty:
            return None

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


def compute_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains = []
    losses = []
    for i in range(-period, 0):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(abs(min(diff, 0)))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_ema(values, period):
    if not values:
        return []
    k = 2 / (period + 1)
    ema = [values[0]]
    for v in values[1:]:
        ema.append(v * k + ema[-1] * (1 - k))
    return ema


def compute_atr(candles, period=14):
    if len(candles) < 2:
        return 0.0
    trs = []
    for i in range(1, len(candles)):
        h_ = candles[i]["high"]
        l_ = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h_ - l_, abs(h_ - pc), abs(l_ - pc))
        trs.append(tr)
    if not trs:
        return 0.0
    period = min(period, len(trs))
    return sum(trs[-period:]) / period


def compute_macd(closes, fast=12, slow=26, signal=9):
    if len(closes) < slow:
        return {"macd": 0.0, "signal": 0.0, "hist": 0.0}
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
    if pct <= 60:
        return "Weak"
    if pct <= 75:
        return "Moderate"
    return "Strong"


def extract_market_features(candles):
    if not candles or len(candles) < 20:
        return None

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    last = closes[-1]
    prev = closes[-2]

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
        "last": last,
        "prev": prev,
        "sma20": sma20,
        "sma50": sma50,
        "ema20": ema20,
        "ema50": ema50,
        "move_5": move_5,
        "move_10": move_10,
        "rsi": rsi,
        "atr": atr,
        "atr_pct": atr_pct,
        "macd": macd["macd"],
        "macd_signal": macd["signal"],
        "macd_hist": macd["hist"],
        "bb_mid": bb["mid"],
        "bb_upper": bb["upper"],
        "bb_lower": bb["lower"],
        "bb_bandwidth": bb["bandwidth"],
        "volatility_ratio": volatility_ratio,
        "slope": slope,
        "slope_pct": slope_pct,
        "breakout_up": breakout_up,
        "breakout_down": breakout_down,
    }


def detect_market_regime(feats):
    if not feats:
        return "unknown"
    trend_up = feats["ema20"] > feats["ema50"] and feats["slope_pct"] > 0.02
    trend_down = feats["ema20"] < feats["ema50"] and feats["slope_pct"] < -0.02
    volatile = feats["atr_pct"] > 3.5 or feats["volatility_ratio"] > 1.6
    compressed = feats["bb_bandwidth"] < 0.04 and feats["atr_pct"] < 1.2
    breakout = feats["breakout_up"] or feats["breakout_down"]

    if breakout and volatile:
        return "high_vol_breakout"
    if trend_up and volatile:
        return "trending_up_volatile"
    if trend_down and volatile:
        return "trending_down_volatile"
    if trend_up:
        return "trending_up"
    if trend_down:
        return "trending_down"
    if compressed:
        return "low_vol_compression"
    return "range_bound"


def ava_hypothesis_engine(candles, timeframe="1h", signal_type="short_term"):
    feats = extract_market_features(candles)
    if not feats:
        return {
            "signal": "HOLD",
            "confidence": 0.50,
            "confidence_label": "Weak",
            "trend_state": "Neutral",
            "trend_strength": "Low",
            "forecast_trend": "Stable",
            "projected_change": "+0.00%",
            "regime": "unknown",
            "dominant_factors": ["insufficient_data"],
            "reasoning": "Not enough candle history for AVA Brain v2.",
            "explanations": ["Insufficient data for full signal model."],
            "score": 0
        }

    regime = detect_market_regime(feats)
    score = 0
    explanations = []
    factors = []

    if feats["last"] > feats["ema20"]:
        score += 2
        explanations.append("Price is above EMA20, supporting short-term strength.")
        factors.append("price_above_ema20")
    else:
        score -= 2
        explanations.append("Price is below EMA20, weakening short-term structure.")
        factors.append("price_below_ema20")

    if feats["ema20"] > feats["ema50"]:
        score += 2
        explanations.append("EMA20 is above EMA50, confirming bullish trend alignment.")
        factors.append("ema20_above_ema50")
    else:
        score -= 2
        explanations.append("EMA20 is below EMA50, confirming bearish trend alignment.")
        factors.append("ema20_below_ema50")

    if 55 <= feats["rsi"] <= 70:
        score += 1
        explanations.append(f"RSI at {feats['rsi']:.1f} is bullish without being heavily overbought.")
        factors.append("rsi_bullish_balanced")
    elif feats["rsi"] >= 75:
        if regime.startswith("trending_up"):
            explanations.append(f"RSI at {feats['rsi']:.1f} is elevated, but strong uptrends can sustain overbought readings.")
            factors.append("rsi_overbought_trend")
        else:
            score -= 1
            explanations.append(f"RSI at {feats['rsi']:.1f} is overbought in a non-trending regime.")
            factors.append("rsi_overbought_range")
    elif feats["rsi"] <= 30:
        if regime.startswith("trending_down"):
            explanations.append(f"RSI at {feats['rsi']:.1f} is oversold, but downtrends can remain weak.")
            factors.append("rsi_oversold_downtrend")
        else:
            score += 1
            explanations.append(f"RSI at {feats['rsi']:.1f} suggests a potential rebound condition.")
            factors.append("rsi_oversold_rebound")
    elif feats["rsi"] < 45:
        score -= 1
        explanations.append(f"RSI at {feats['rsi']:.1f} leans bearish.")
        factors.append("rsi_bearish")

    if feats["macd_hist"] > 0:
        score += 2
        explanations.append("MACD histogram is positive, showing improving momentum.")
        factors.append("macd_hist_positive")
    else:
        score -= 2
        explanations.append("MACD histogram is negative, showing fading momentum.")
        factors.append("macd_hist_negative")

    if feats["move_5"] > 1.5:
        score += 1
        explanations.append("5-period momentum is positive.")
        factors.append("positive_5p_momentum")
    elif feats["move_5"] < -1.5:
        score -= 1
        explanations.append("5-period momentum is negative.")
        factors.append("negative_5p_momentum")

    if feats["breakout_up"]:
        score += 2
        explanations.append("Price is testing or breaking the recent 20-period high.")
        factors.append("breakout_up")
    elif feats["breakout_down"]:
        score -= 2
        explanations.append("Price is testing or breaking the recent 20-period low.")
        factors.append("breakout_down")

    if regime == "trending_up":
        score += 1
        explanations.append("Market regime is trending up.")
        factors.append("regime_trending_up")
    elif regime == "trending_down":
        score -= 1
        explanations.append("Market regime is trending down.")
        factors.append("regime_trending_down")
    elif regime == "range_bound":
        explanations.append("Market regime is range-bound, so breakout conviction is lower.")
        factors.append("regime_range_bound")
    elif regime == "high_vol_breakout":
        explanations.append("Market regime is high-volatility breakout; signal quality may be stronger but risk is elevated.")
        factors.append("regime_high_vol_breakout")
    elif regime == "low_vol_compression":
        explanations.append("Market regime is low-volatility compression; breakout risk is building.")
        factors.append("regime_low_vol_compression")

    if feats["atr_pct"] > 5:
        score -= 1
        explanations.append("ATR is elevated, increasing execution risk.")
        factors.append("atr_elevated")

    if score >= 5:
        signal = "BUY"
    elif score <= -5:
        signal = "SELL"
    else:
        signal = "HOLD"

    confidence = min(0.90, max(0.50, 0.52 + abs(score) * 0.045))
    c_label = confidence_label(confidence)

    if abs(score) >= 7:
        trend_strength = "High"
    elif abs(score) >= 4:
        trend_strength = "Medium"
    else:
        trend_strength = "Low"

    if score > 1:
        trend_state = "Bullish"
        forecast_trend = "Upward"
    elif score < -1:
        trend_state = "Bearish"
        forecast_trend = "Downward"
    else:
        trend_state = "Neutral"
        forecast_trend = "Stable"

    projected_change_val = feats["move_10"] * 0.6 + feats["slope_pct"] * 12
    projected_change = f"{projected_change_val:+.2f}%"

    return {
        "signal": signal,
        "confidence": round(confidence, 2),
        "confidence_label": c_label,
        "trend_state": trend_state,
        "trend_strength": trend_strength,
        "forecast_trend": forecast_trend,
        "projected_change": projected_change,
        "regime": regime,
        "dominant_factors": factors[:6],
        "reasoning": " ".join(explanations[:3]),
        "explanations": explanations[:6],
        "score": score
    }


def aggregate_multi_timeframe_brain(asset_type, symbol, tf_map):
    if asset_type == "crypto":
        signal_map = {
            "15m": ("short_term", 1.0),
            "1h": ("intraday", 1.2),
            "4h": ("swing", 1.5),
        }
    else:
        signal_map = {
            "1d": ("short_term", 1.0),
            "1wk": ("trend", 1.4),
            "1mo": ("macro", 1.6),
        }

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
    if avg_score >= 4:
        overall = "BUY"
    elif avg_score <= -4:
        overall = "SELL"
    else:
        overall = "HOLD"

    confidence = min(0.90, max(0.50, 0.54 + abs(avg_score) * 0.04))
    c_label = confidence_label(confidence)

    all_regimes = [v["regime"] for v in per_tf.values()]
    all_factors = []
    for v in per_tf.values():
        all_factors.extend(v.get("dominant_factors", []))

    explanations = []
    for tf, v in per_tf.items():
        explanations.append(f"{tf} / {v['signal_type']}: {v['signal']} ({v['trend_state']}, {v['regime']})")

    if avg_score > 2:
        trend_state = "Bullish"
        forecast_trend = "Upward"
    elif avg_score < -2:
        trend_state = "Bearish"
        forecast_trend = "Downward"
    else:
        trend_state = "Neutral"
        forecast_trend = "Stable"

    return {
        "signal": overall,
        "confidence": round(confidence, 2),
        "confidence_label": c_label,
        "trend_state": trend_state,
        "trend_strength": "High" if abs(avg_score) >= 6 else "Medium" if abs(avg_score) >= 3 else "Low",
        "forecast_trend": forecast_trend,
        "projected_change": f"{avg_score:+.2f} score",
        "regime": ", ".join(sorted(set(all_regimes))),
        "dominant_factors": list(dict.fromkeys(all_factors))[:8],
        "reasoning": " | ".join(explanations[:3]),
        "explanations": explanations,
        "score": avg_score,
        "timeframes": per_tf
    }


def compute_light_signal(change):
    if change >= 2.0:
        return "BUY"
    if change <= -2.0:
        return "SELL"
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


def merge_ordered_assets(provider_lists, ordered_universe):
    merged = {}
    for plist in provider_lists:
        for item in plist or []:
            sym = (item.get("symbol") or "").upper()
            if not sym:
                continue
            if sym not in merged:
                merged[sym] = dict(item)
            else:
                for k, v in item.items():
                    if merged[sym].get(k) in [None, "", []] and v not in [None, "", []]:
                        merged[sym][k] = v

    return [merged[symbol] for symbol, _ in ordered_universe if symbol in merged]


def fetch_crypto_from_coingecko():
    priority_symbols = [
        "BTC", "ETH", "BNB", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT",
        "MATIC", "LTC", "BCH", "ATOM", "UNI", "NEAR", "APT", "ARB", "OP", "SUI",
        "SHIB", "TRX", "ETC", "XLM", "HBAR", "ICP", "FIL", "INJ", "RNDR", "AAVE"
    ]

    symbol_to_id = {s: COINGECKO_IDS[s] for s in priority_symbols if s in COINGECKO_IDS}
    if not symbol_to_id:
        return []

    ids_needed = list(symbol_to_id.values())
    reverse_ids = {v: k for k, v in symbol_to_id.items()}

    r = requests.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "ids": ",".join(ids_needed),
            "order": "market_cap_desc",
            "per_page": len(ids_needed),
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h"
        },
        timeout=Config.REQUESTS_TIMEOUT,
        headers={"accept": "application/json"}
    )
    r.raise_for_status()
    data = r.json()

    results = []
    for item in data:
        coin_id = item.get("id", "")
        symbol = reverse_ids.get(coin_id)
        if not symbol:
            continue
        try:
            price = float(item.get("current_price") or 0)
            change = float(item.get("price_change_percentage_24h") or 0)
        except Exception:
            continue
        if price <= 0:
            continue
        results.append({
            "symbol": symbol,
            "name": CRYPTO_NAME_MAP.get(symbol, item.get("name") or symbol),
            "price": price,
            "change": change,
            "dir": "up" if change >= 0 else "down",
            "signal": compute_light_signal(change),
            "logo": get_crypto_logo(symbol, item.get("image"))
        })
    return results


def fetch_crypto_from_kraken():
    results = []
    for symbol, pair_code in KRAKEN_PAIRS.items():
        try:
            r = requests.get(
                "https://api.kraken.com/0/public/Ticker",
                params={"pair": pair_code},
                timeout=Config.REQUESTS_TIMEOUT
            )
            r.raise_for_status()
            payload = r.json()

            if payload.get("error"):
                continue

            raw = payload.get("result", {})
            if not raw:
                continue

            value = list(raw.values())[0]
            price = float(value["c"][0])
            open_price = float(value["o"])
            change = pct_change(price, open_price)

            if price <= 0:
                continue

            results.append({
                "symbol": symbol,
                "name": CRYPTO_NAME_MAP.get(symbol, symbol),
                "price": price,
                "change": change,
                "dir": "up" if change >= 0 else "down",
                "signal": compute_light_signal(change),
                "logo": get_crypto_logo(symbol),
            })
            time.sleep(0.06)
        except Exception as e:
            logger.warning(f"Kraken fetch failed for {symbol}: {e}")
    return results


def fetch_crypto_from_kucoin():
    try:
        r = requests.get(
            "https://api.kucoin.com/api/v1/market/allTickers",
            timeout=Config.REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        payload = r.json()
        items = payload.get("data", {}).get("ticker", []) or []

        market_map = {}
        for item in items:
            sym_name = (item.get("symbol") or "").upper()
            if sym_name.endswith("-USDT"):
                base = sym_name.split("-")[0]
                market_map[base] = item

        results = []
        for symbol, _name in CRYPTO_TOP_90:
            item = market_map.get(symbol)
            if not item:
                continue
            try:
                price = float(item.get("last") or 0)
                change_rate = float(item.get("changeRate") or 0) * 100.0
            except Exception:
                continue
            if price <= 0:
                continue
            results.append({
                "symbol": symbol,
                "name": CRYPTO_NAME_MAP.get(symbol, symbol),
                "price": price,
                "change": change_rate,
                "dir": "up" if change_rate >= 0 else "down",
                "signal": compute_light_signal(change_rate),
                "logo": get_crypto_logo(symbol),
            })
        return results
    except Exception as e:
        logger.warning(f"KuCoin fetch failed: {e}")
        return []


def fetch_crypto_quotes_safe(force_refresh=False):
    cache_key = "crypto_list"

    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.CRYPTO_CACHE_TTL)
        if cached is not None:
            return cached

    provider_lists = []

    try:
        cg = fetch_crypto_from_coingecko()
        if cg:
            provider_lists.append(cg)
    except Exception as e:
        logger.warning(f"CoinGecko failed: {e}")

    try:
        kr = fetch_crypto_from_kraken()
        if kr:
            provider_lists.append(kr)
    except Exception as e:
        logger.warning(f"Kraken failed: {e}")

    try:
        kc = fetch_crypto_from_kucoin()
        if kc:
            provider_lists.append(kc)
    except Exception as e:
        logger.warning(f"KuCoin failed: {e}")

    merged = merge_ordered_assets(provider_lists, CRYPTO_TOP_90)
    if merged:
        set_cached_payload(cache_key, merged)
        return merged

    stale = get_stale_payload(cache_key)
    if stale is not None:
        logger.warning("Returning stale cached crypto data")
        return stale

    return []


def normalize_stock_symbol_for_twelvedata(symbol):
    return {"BRK-B": "BRK.B"}.get(symbol.upper(), symbol.upper())


def normalize_stock_symbol_for_finnhub(symbol):
    return {"BRK-B": "BRK.B"}.get(symbol.upper(), symbol.upper())


def fetch_stock_quotes_from_twelvedata():
    if not Config.TWELVEDATA_API_KEY:
        raise Exception("TWELVEDATA_API_KEY not set")

    results = []
    equity_symbols = [s for s, _ in STOCK_UNIVERSE if "=" not in s and s != "SIG"]

    for original_symbol in equity_symbols:
        api_symbol = normalize_stock_symbol_for_twelvedata(original_symbol)
        r = requests.get(
            "https://api.twelvedata.com/quote",
            params={"symbol": api_symbol, "apikey": Config.TWELVEDATA_API_KEY},
            timeout=Config.REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        item = r.json()

        if isinstance(item, dict) and item.get("code"):
            continue

        try:
            price = float(item.get("close") or 0)
            percent_change_raw = item.get("percent_change")
            change = float(str(percent_change_raw).replace("%", "")) if percent_change_raw not in [None, ""] else 0.0
        except Exception:
            continue

        if price <= 0:
            continue

        results.append({
            "symbol": original_symbol,
            "name": STOCK_NAME_MAP.get(original_symbol, original_symbol),
            "price": price,
            "change": change,
            "dir": "up" if change >= 0 else "down",
            "signal": compute_light_signal(change),
            "logo": get_stock_logo(original_symbol),
            "icon": get_asset_icon(original_symbol),
        })
        time.sleep(0.10)

    return results


def fetch_stock_quotes_from_finnhub():
    if not Config.FINNHUB_API_KEY:
        raise Exception("FINNHUB_API_KEY not set")

    payload = []
    equity_symbols = [s for s, _ in STOCK_UNIVERSE if "=" not in s and s != "SIG"]

    for symbol in equity_symbols:
        api_symbol = normalize_stock_symbol_for_finnhub(symbol)
        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": api_symbol, "token": Config.FINNHUB_API_KEY},
            timeout=Config.REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        data = r.json()

        current_price = data.get("c")
        prev_close = data.get("pc")
        if current_price in [None, 0]:
            continue

        try:
            current_price = float(current_price)
            prev_close = float(prev_close) if prev_close not in [None, 0] else current_price
            change = pct_change(current_price, prev_close)
        except Exception:
            continue

        payload.append({
            "symbol": symbol,
            "name": STOCK_NAME_MAP.get(symbol, symbol),
            "price": current_price,
            "change": change,
            "dir": "up" if change >= 0 else "down",
            "signal": compute_light_signal(change),
            "logo": get_stock_logo(symbol),
            "icon": get_asset_icon(symbol),
        })
        time.sleep(0.10)

    return payload


def fetch_stock_quotes_from_yfinance():
    payload = []
    targets = [s for s, _ in STOCK_UNIVERSE]
    try:
        data = yf.download(
            tickers=" ".join(targets),
            period="5d",
            interval="1d",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True
        )
        if data is None or data.empty:
            return []

        for symbol, name in STOCK_UNIVERSE:
            try:
                if len(targets) == 1:
                    hist = data.tail(2)
                else:
                    hist = data[symbol].dropna().tail(2)
                if hist is None or hist.empty:
                    continue
                last_close = float(hist["Close"].iloc[-1])
                prev_close = float(hist["Close"].iloc[-2]) if len(hist) > 1 else last_close
                change = pct_change(last_close, prev_close)
                payload.append({
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
    except Exception as e:
        logger.warning(f"yfinance stock quotes failed: {e}")
    return payload


def fetch_stock_quotes_safe(force_refresh=False):
    cache_key = "stock_list"

    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.STOCK_CACHE_TTL)
        if cached is not None:
            return cached

    payload = []

    try:
        payload = fetch_stock_quotes_from_twelvedata()
    except Exception as e:
        logger.warning(f"Twelve Data stock fetch failed: {e}")

    if not payload:
        try:
            payload = fetch_stock_quotes_from_finnhub()
        except Exception as e:
            logger.warning(f"Finnhub stock fetch failed: {e}")

    if not payload:
        try:
            payload = fetch_stock_quotes_from_yfinance()
        except Exception as e:
            logger.warning(f"yfinance stock fetch failed: {e}")

    if payload:
        set_cached_payload(cache_key, payload)
        return payload

    stale = get_stale_payload(cache_key)
    if stale is not None:
        logger.warning("Returning stale cached stock data")
        return stale

    return []


def paginate(items, page, per_page):
    total = len(items)
    pages = max(1, math.ceil(total / per_page)) if total > 0 else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], total, pages, page

def detail_cache_key(asset_type, symbol):
    return f"detail::{asset_type}::{symbol.upper()}"


def build_crypto_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in fetch_crypto_quotes_safe()}
    if symbol not in light_map:
        return None

    asset = dict(light_map[symbol])
    tf_map = fetch_crypto_multi_timeframe(symbol)
    primary_candles = tf_map.get("15m") or tf_map.get("1h") or tf_map.get("4h") or []
    agg = aggregate_multi_timeframe_brain("crypto", symbol, tf_map)

    feats = extract_market_features(primary_candles)
    signal_meta = {
        "signal": agg["signal"],
        "confidence": agg["confidence"],
        "confidence_label": agg["confidence_label"],
        "confidence_note": "Confidence reflects internal indicator agreement, not guaranteed outcome probability.",
        "rsi": round(feats["rsi"], 2) if feats else 50.0,
        "momentum": round(feats["move_5"], 2) if feats else 0.0,
        "summary": agg["reasoning"],
        "why": agg["explanations"],
    }

    forecast = {
        "trend": agg["forecast_trend"],
        "projected_change": agg["projected_change"],
        "confidence_band": f"{int(agg['confidence'] * 100)}%",
        "summary": f"Multi-timeframe crypto forecast: {agg['forecast_trend'].lower()} with {agg['trend_strength'].lower()} conviction."
    }

    trend = {
        "state": agg["trend_state"],
        "strength": agg["trend_strength"],
        "read": agg["regime"],
        "summary": f"Dominant factors: {', '.join(agg['dominant_factors'][:4])}."
    }

    for tf, item in agg["timeframes"].items():
        db.upsert_signal_snapshot_if_changed(
            "crypto", symbol, tf, item["signal_type"], item["signal"],
            item["confidence"], item["confidence_label"], item["regime"],
            item["trend_state"], asset["price"], item["explanations"], item["dominant_factors"]
        )

    asset.update({
        "price_display": fmt_price(asset["price"], asset["symbol"]),
        "change_display": fmt_change(asset["change"]),
        "detail_candles": render_candles_from_ohlc(primary_candles) if primary_candles else fallback_candles_html(99),
        "signal_meta": signal_meta,
        "forecast": forecast,
        "trend_data": trend,
        "signal": agg["signal"],
        "multi_timeframes": agg["timeframes"],
        "overall_brain": agg
    })
    return asset


def build_stock_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in fetch_stock_quotes_safe()}
    if symbol not in light_map:
        return None

    asset = dict(light_map[symbol])
    tf_map = fetch_stock_multi_timeframe(symbol)
    primary_candles = tf_map.get("1d") or tf_map.get("1wk") or tf_map.get("1mo") or []
    agg = aggregate_multi_timeframe_brain("stock", symbol, tf_map)

    feats = extract_market_features(primary_candles)
    signal_meta = {
        "signal": agg["signal"],
        "confidence": agg["confidence"],
        "confidence_label": agg["confidence_label"],
        "confidence_note": "Confidence reflects internal indicator agreement, not guaranteed outcome probability.",
        "rsi": round(feats["rsi"], 2) if feats else 50.0,
        "momentum": round(feats["move_5"], 2) if feats else 0.0,
        "summary": agg["reasoning"],
        "why": agg["explanations"],
    }

    forecast = {
        "trend": agg["forecast_trend"],
        "projected_change": agg["projected_change"],
        "confidence_band": f"{int(agg['confidence'] * 100)}%",
        "summary": f"Multi-timeframe stock forecast: {agg['forecast_trend'].lower()} with {agg['trend_strength'].lower()} conviction."
    }

    trend = {
        "state": agg["trend_state"],
        "strength": agg["trend_strength"],
        "read": agg["regime"],
        "summary": f"Dominant factors: {', '.join(agg['dominant_factors'][:4])}."
    }

    for tf, item in agg["timeframes"].items():
        db.upsert_signal_snapshot_if_changed(
            "stock", symbol, tf, item["signal_type"], item["signal"],
            item["confidence"], item["confidence_label"], item["regime"],
            item["trend_state"], asset["price"], item["explanations"], item["dominant_factors"]
        )

    asset.update({
        "price_display": fmt_price(asset["price"], asset["symbol"]),
        "change_display": fmt_change(asset["change"]),
        "detail_candles": render_candles_from_ohlc(primary_candles) if primary_candles else fallback_candles_html(101),
        "signal_meta": signal_meta,
        "forecast": forecast,
        "trend_data": trend,
        "signal": agg["signal"],
        "multi_timeframes": agg["timeframes"],
        "overall_brain": agg
    })
    return asset


def get_crypto_detail(symbol, force_refresh=False):
    symbol = symbol.upper()
    cache_key = detail_cache_key("crypto", symbol)

    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.DETAIL_CACHE_TTL)
        if cached is not None:
            return cached

    asset = build_crypto_detail(symbol)
    if asset:
        set_cached_payload(cache_key, asset)
        return asset

    stale = get_stale_payload(cache_key)
    return stale


def get_stock_detail(symbol, force_refresh=False):
    symbol = symbol.upper()
    cache_key = detail_cache_key("stock", symbol)

    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.DETAIL_CACHE_TTL)
        if cached is not None:
            return cached

    asset = build_stock_detail(symbol)
    if asset:
        set_cached_payload(cache_key, asset)
        return asset

    stale = get_stale_payload(cache_key)
    return stale


def refresh_single_detail_cache(asset_type, symbol):
    if asset_type == "crypto":
        return get_crypto_detail(symbol, force_refresh=True)
    return get_stock_detail(symbol, force_refresh=True)


def warm_detail_caches():
    for symbol in Config.DETAIL_WARM_CRYPTO:
        try:
            refresh_single_detail_cache("crypto", symbol)
        except Exception as e:
            logger.warning(f"Warm crypto detail failed for {symbol}: {e}")

    for symbol in Config.DETAIL_WARM_STOCKS:
        try:
            refresh_single_detail_cache("stock", symbol)
        except Exception as e:
            logger.warning(f"Warm stock detail failed for {symbol}: {e}")


def evaluate_signal_outcome(signal, entry_price, current_price):
    if not entry_price or not current_price:
        return None, None
    ret = pct_change(current_price, entry_price)
    if signal == "BUY":
        outcome = "correct" if ret > 0 else "incorrect"
    elif signal == "SELL":
        outcome = "correct" if ret < 0 else "incorrect"
    else:
        outcome = "correct" if abs(ret) < 1.0 else "incorrect"
    return ret, outcome


def parse_sqlite_dt(v):
    try:
        return datetime.fromisoformat(str(v).replace(" ", "T"))
    except Exception:
        return None


def select_price_near_target(points, target_dt):
    if not points:
        return None

    target_ts = int(target_dt.timestamp())
    ordered = sorted([p for p in points if p.get("ts") and p.get("close") is not None], key=lambda x: x["ts"])
    if not ordered:
        return None

    after = [p for p in ordered if p["ts"] >= target_ts]
    if after:
        return float(after[0]["close"])

    return float(ordered[-1]["close"])


def fetch_crypto_history_points_for_horizon(symbol, horizon):
    if horizon == "1h":
        return fetch_crypto_candles(symbol, interval="15m", limit=24)
    if horizon == "24h":
        return fetch_crypto_candles(symbol, interval="1h", limit=48)
    if horizon == "7d":
        return fetch_crypto_candles(symbol, interval="4h", limit=72)
    return None


def fetch_stock_history_points_for_horizon(symbol, horizon):
    try:
        ticker = yf.Ticker(symbol.upper())
        if horizon == "1h":
            hist = ticker.history(period="10d", interval="60m", auto_adjust=False)
        elif horizon == "24h":
            hist = ticker.history(period="60d", interval="60m", auto_adjust=False)
        else:
            hist = ticker.history(period="1y", interval="1d", auto_adjust=False)

        if hist is None or hist.empty:
            return None

        points = []
        for idx, row in hist.iterrows():
            ts = normalize_dt_to_ts(idx.to_pydatetime())
            if ts is None:
                continue
            points.append({
                "ts": ts,
                "close": float(row["Close"])
            })
        return points or None
    except Exception:
        return None


def get_price_for_horizon(asset_type, symbol, created_dt, horizon):
    horizon_map = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
    }
    target_dt = created_dt + horizon_map[horizon]

    if asset_type == "crypto":
        points = fetch_crypto_history_points_for_horizon(symbol, horizon)
    else:
        points = fetch_stock_history_points_for_horizon(symbol, horizon)

    return select_price_near_target(points, target_dt)


def refresh_signal_outcomes():
    rows = db.get_recent_snapshots_for_outcomes(days=10, limit=400)
    now = datetime.utcnow()

    for row in rows:
        created = parse_sqlite_dt(row["created_at"])
        if not created:
            continue

        age_hours = (now - created).total_seconds() / 3600.0

        checks = [
            ("1h", 1),
            ("24h", 24),
            ("7d", 24 * 7),
        ]

        for horizon, threshold in checks:
            if age_hours < threshold:
                continue
            if db.outcome_exists(row["id"], horizon):
                continue

            price_at_horizon = get_price_for_horizon(row["asset_type"], row["symbol"], created, horizon)
            if not price_at_horizon:
                continue

            ret, outcome = evaluate_signal_outcome(row["signal"], row["price_at_signal"], price_at_horizon)
            if ret is not None:
                db.insert_signal_outcome(row["id"], horizon, price_at_horizon, ret, outcome)


def summarize_performance(rows):
    if not rows:
        return {"total": 0, "buy_accuracy": 0, "sell_accuracy": 0, "hold_accuracy": 0, "avg_return": 0, "best_assets": []}

    total = len(rows)
    buy = [r for r in rows if r["signal"] == "BUY"]
    sell = [r for r in rows if r["signal"] == "SELL"]
    hold = [r for r in rows if r["signal"] == "HOLD"]

    def acc(items):
        if not items:
            return 0
        return round(100 * sum(1 for x in items if x["outcome"] == "correct") / len(items), 1)

    avg_return = round(sum(float(r["return_pct"] or 0) for r in rows) / total, 2) if total else 0

    per_symbol = {}
    for r in rows:
        per_symbol.setdefault(r["symbol"], []).append(float(r["return_pct"] or 0))
    ranked = sorted(((sym, round(sum(vals) / len(vals), 2)) for sym, vals in per_symbol.items()), key=lambda x: x[1], reverse=True)

    return {
        "total": total,
        "buy_accuracy": acc(buy),
        "sell_accuracy": acc(sell),
        "hold_accuracy": acc(hold),
        "avg_return": avg_return,
        "best_assets": ranked[:5]
    }


def legal_disclaimer_html():
    return """
    <div class="card" style="margin-top:24px;">
      <h3>Disclaimer</h3>
      <p>
        AVA Markets provides technical market intelligence and indicator-based analysis
        for educational purposes only. It is not financial advice. Confidence reflects
        internal indicator agreement, not guaranteed outcome probability. Past performance
        does not guarantee future results.
      </p>
    </div>
    """


def build_breadcrumbs(items):
    return {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {
                "@type": "ListItem",
                "position": idx + 1,
                "name": item["name"],
                "item": item["url"]
            }
            for idx, item in enumerate(items)
        ]
    }


def homepage_json_ld():
    return [
        {
            "@context": "https://schema.org",
            "@type": "Organization",
            "name": "AVA Markets",
            "url": Config.DOMAIN,
            "description": "Crypto, stock, and commodity market intelligence with live updates, signals, forecasts, and trends."
        },
        {
            "@context": "https://schema.org",
            "@type": "WebSite",
            "name": "AVA Markets",
            "url": Config.DOMAIN,
            "potentialAction": {
                "@type": "SearchAction",
                "target": f"{Config.DOMAIN}/crypto?q={{search_term_string}}",
                "query-input": "required name=search_term_string"
            }
        }
    ]


def faq_json_ld(items):
    return {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {
                "@type": "Question",
                "name": q,
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": a
                }
            }
            for q, a in items
        ]
    }


def asset_json_ld(asset_type, asset):
    page_url = f"{Config.DOMAIN}/crypto/{asset['symbol']}" if asset_type == "crypto" else f"{Config.DOMAIN}/stocks/{asset['symbol']}"
    return {
        "@context": "https://schema.org",
        "@type": "FinancialProduct",
        "name": f"{asset['name']} ({asset['symbol']})",
        "description": f"{asset['name']} market page with live price updates, signals, trends, and forecasts on AVA Markets.",
        "url": page_url,
        "category": "Cryptocurrency" if asset_type == "crypto" else "Stock / Commodity"
    }


def live_update_script(page_type=None, symbol=None, asset_type=None):
    if page_type == "crypto_list":
        return """
<script>
async function refreshCryptoList(){
  try{
    const res = await fetch('/api/live/crypto-list');
    const data = await res.json();
    const stamp = document.getElementById('live-updated-crypto');
    if (stamp) stamp.textContent = 'Last updated: ' + data.updated_at + ' UTC';

    data.items.forEach(item => {
      const priceEl = document.getElementById('price-' + item.symbol);
      const changeEl = document.getElementById('change-' + item.symbol);
      const signalEl = document.getElementById('signal-' + item.symbol);

      if (priceEl) priceEl.textContent = item.price_display;
      if (changeEl) {
        changeEl.textContent = item.change_display;
        changeEl.className = item.dir === 'up' ? 'up' : 'down';
      }
      if (signalEl) {
        signalEl.textContent = item.signal;
        signalEl.className = 'signal ' + (item.signal === 'BUY' ? 'signal-buy' : item.signal === 'SELL' ? 'signal-sell' : 'signal-hold');
      }
    });
  } catch(e){}
}
setInterval(refreshCryptoList, 30000);
</script>
        """

    if page_type == "stock_list":
        return """
<script>
async function refreshStockList(){
  try{
    const res = await fetch('/api/live/stocks-list');
    const data = await res.json();
    const stamp = document.getElementById('live-updated-stocks');
    if (stamp) stamp.textContent = 'Last updated: ' + data.updated_at + ' UTC';

    data.items.forEach(item => {
      const safe = item.symbol.replace(/[^A-Za-z0-9]/g, '_');
      const priceEl = document.getElementById('price-' + safe);
      const changeEl = document.getElementById('change-' + safe);
      const signalEl = document.getElementById('signal-' + safe);

      if (priceEl) priceEl.textContent = item.price_display;
      if (changeEl) {
        changeEl.textContent = item.change_display;
        changeEl.className = item.dir === 'up' ? 'up' : 'down';
      }
      if (signalEl) {
        signalEl.textContent = item.signal;
        signalEl.className = 'signal ' + (item.signal === 'BUY' ? 'signal-buy' : item.signal === 'SELL' ? 'signal-sell' : 'signal-hold');
      }
    });
  } catch(e){}
}
setInterval(refreshStockList, 45000);
</script>
        """

    if page_type == "detail":
        endpoint = f"/api/live/{asset_type}/{symbol.upper()}"
        interval = 30000 if asset_type == "crypto" else 45000
        return f"""
<script>
async function refreshDetail(){{
  try{{
    const res = await fetch('{endpoint}');
    const data = await res.json();
    const priceEl = document.getElementById('detail-price');
    const changeEl = document.getElementById('detail-change');
    const signalEl = document.getElementById('detail-signal');
    const confEl = document.getElementById('detail-confidence');
    const stamp = document.getElementById('detail-updated');

    if (priceEl) priceEl.textContent = data.price_display;
    if (changeEl) {{
      changeEl.textContent = data.change_display;
      changeEl.className = data.dir === 'up' ? 'up' : 'down';
    }}
    if (signalEl) {{
      signalEl.textContent = data.signal;
      signalEl.className = 'signal ' + (data.signal === 'BUY' ? 'signal-buy' : data.signal === 'SELL' ? 'signal-sell' : 'signal-hold');
    }}
    if (confEl) confEl.textContent = 'Confidence: ' + data.confidence_text;
    if (stamp) stamp.textContent = 'Last updated: ' + data.updated_at + ' UTC';
  }} catch(e){{}}
}}
setInterval(refreshDetail, {interval});
</script>
        """
    return ""


def nav_layout(
    title,
    content,
    extra_head="",
    meta_description=None,
    canonical_url=None,
    og_type="website",
    robots_content="index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1",
    json_ld_override=None
):
    if not meta_description:
        meta_description = "AVA Markets provides crypto, stock, and commodity market intelligence with live updates, multi-timeframe signals, trends, forecasts, and transparent performance tracking."

    if not canonical_url:
        canonical_url = f"{Config.DOMAIN}{request.path}"

    default_json_ld = {
        "@context": "https://schema.org",
        "@type": "WebSite" if request.path == "/" else "WebPage",
        "name": title,
        "url": canonical_url,
        "description": meta_description
    }

    json_ld_payload = json_ld_override if json_ld_override is not None else default_json_ld

    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">

  <title>{{ title }}</title>
  <meta name="description" content="{{ meta_description }}">
  <meta name="robots" content="{{ robots_content }}">
  <link rel="canonical" href="{{ canonical_url }}">

  <meta property="og:title" content="{{ title }}">
  <meta property="og:description" content="{{ meta_description }}">
  <meta property="og:type" content="{{ og_type }}">
  <meta property="og:url" content="{{ canonical_url }}">
  <meta property="og:site_name" content="AVA Markets">

  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="{{ title }}">
  <meta name="twitter:description" content="{{ meta_description }}">

  {% if json_ld_payload %}
    {% if json_ld_payload is string %}
      <script type="application/ld+json">{{ json_ld_payload|safe }}</script>
    {% else %}
      <script type="application/ld+json">{{ json_ld_payload|tojson|safe }}</script>
    {% endif %}
  {% endif %}

  <style>{{ css }}</style>
  {{ extra_head|safe }}
</head>
<body>
  <div class="container">
    <nav class="nav">
      <div class="logo"><a href="/">AVA Markets</a></div>
      <div class="nav-links">
        <a href="/">Home</a>
        <a href="/crypto">Crypto</a>
        <a href="/stocks">Stocks</a>
        <a href="/forecast">Forecast</a>
        <a href="/trends">Trends</a>
        <a href="/performance">Performance</a>
        <a href="/signal-changes">Signal Changes</a>
        <a href="/pricing">Pricing</a>
        {% if user %}
          <a href="/dashboard">Dashboard</a>
          <a href="/logout">Logout</a>
        {% else %}
          <a href="/login">Login</a>
          <a href="/register">Register</a>
        {% endif %}
        <a href="/admin/login">Admin</a>
      </div>
    </nav>

    {{ content|safe }}

    <div class="footer">AVA Markets © 2026 — crypto, stocks, premium signals, live updates, trends, and forecasts.</div>
  </div>
</body>
</html>
    """,
    title=title,
    content=content,
    css=CSS,
    user=g.get("user"),
    extra_head=extra_head,
    meta_description=meta_description,
    canonical_url=canonical_url,
    og_type=og_type,
    robots_content=robots_content,
    json_ld_payload=json_ld_payload)


def get_web_user():
    return db.get_user_by_session(request.cookies.get("session_token"))


def get_web_admin():
    return db.get_admin_session(request.cookies.get("admin_token"))


@app.before_request
def load_request_state():
    g.user = get_web_user()
    g.admin = get_web_admin()


@app.after_request
def set_security_headers(resp):
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; "
        "img-src 'self' https: data:; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com data:; "
        "script-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; "
        "frame-ancestors 'none'; "
        "base-uri 'self'; "
        "form-action 'self' https://checkout.stripe.com https://billing.stripe.com;"
    )
    if Config.COOKIE_SECURE:
        resp.headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload")
    return resp


def require_login(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.user:
            return redirect("/login")
        return fn(*args, **kwargs)
    return wrapper


def require_admin(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.admin:
            return redirect("/admin/login")
        return fn(*args, **kwargs)
    return wrapper


def is_paid_active():
    user = g.user or get_web_user()
    return bool(user and user.get("subscription_status") in ["active", "trialing"])


def current_tier():
    user = g.user or get_web_user()
    return user["tier"] if user else "free"


def can_access_signals():
    return is_paid_active() and current_tier() in ["basic", "pro", "elite", "enterprise"]


def can_access_forecast():
    return is_paid_active() and current_tier() in ["pro", "elite", "enterprise"]


def can_access_trends():
    return is_paid_active() and current_tier() in ["pro", "elite", "enterprise"]


@app.route("/robots.txt")
def robots_txt():
    content = f"""User-agent: *
Allow: /
Disallow: /admin
Disallow: /admin/login
Disallow: /dashboard
Disallow: /login
Disallow: /register

Sitemap: {Config.DOMAIN}/sitemap.xml
"""
    resp = make_response(content)
    resp.headers["Content-Type"] = "text/plain; charset=utf-8"
    return resp


@app.route("/sitemap.xml")
def sitemap_xml():
    static_pages = [
        "/", "/crypto", "/stocks", "/forecast", "/trends",
        "/performance", "/signal-changes", "/pricing"
    ]

    crypto_assets = fetch_crypto_quotes_safe()[:30]
    stock_assets = fetch_stock_quotes_safe()[:30]

    urls = []
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    for path in static_pages:
        urls.append({
            "loc": f"{Config.DOMAIN}{path}",
            "lastmod": now,
            "changefreq": "daily" if path in ["/", "/crypto", "/stocks", "/performance"] else "weekly",
            "priority": "1.0" if path == "/" else "0.8"
        })

    for a in crypto_assets:
        urls.append({
            "loc": f"{Config.DOMAIN}/crypto/{a['symbol']}",
            "lastmod": now,
            "changefreq": "hourly",
            "priority": "0.7"
        })

    for a in stock_assets:
        urls.append({
            "loc": f"{Config.DOMAIN}/stocks/{a['symbol']}",
            "lastmod": now,
            "changefreq": "daily",
            "priority": "0.7"
        })

    xml = ['<?xml version="1.0" encoding="UTF-8"?>']
    xml.append('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">')
    for u in urls:
        xml.append(f"""
  <url>
    <loc>{h(u['loc'])}</loc>
    <lastmod>{h(u['lastmod'])}</lastmod>
    <changefreq>{h(u['changefreq'])}</changefreq>
    <priority>{h(u['priority'])}</priority>
  </url>
        """)
    xml.append("</urlset>")

    resp = make_response("".join(xml))
    resp.headers["Content-Type"] = "application/xml; charset=utf-8"
    return resp


@app.route("/api/live/crypto-list")
@limiter.limit("120 per minute")
def api_live_crypto_list():
    items = []
    for a in fetch_crypto_quotes_safe():
        items.append({
            "symbol": a["symbol"],
            "price_display": fmt_price(a["price"], a["symbol"]),
            "change_display": fmt_change(a["change"]),
            "dir": a["dir"],
            "signal": a["signal"]
        })
    return jsonify({
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "items": items
    })


@app.route("/api/live/stocks-list")
@limiter.limit("120 per minute")
def api_live_stocks_list():
    items = []
    for a in fetch_stock_quotes_safe():
        items.append({
            "symbol": a["symbol"],
            "price_display": fmt_price(a["price"], a["symbol"]),
            "change_display": fmt_change(a["change"]),
            "dir": a["dir"],
            "signal": a["signal"]
        })
    return jsonify({
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "items": items
    })


@app.route("/api/live/crypto/<symbol>")
@limiter.limit("60 per minute")
def api_live_crypto_detail(symbol):
    asset = get_crypto_detail(symbol)
    if not asset:
        return jsonify({"error": "not_found"}), 404
    return jsonify({
        "symbol": asset["symbol"],
        "price_display": asset["price_display"],
        "change_display": asset["change_display"],
        "dir": asset["dir"],
        "signal": asset["signal"],
        "confidence_text": f"{int(asset['signal_meta']['confidence'] * 100)}% — {asset['signal_meta']['confidence_label']}",
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    })


@app.route("/api/live/stock/<symbol>")
@limiter.limit("60 per minute")
def api_live_stock_detail(symbol):
    asset = get_stock_detail(symbol)
    if not asset:
        return jsonify({"error": "not_found"}), 404
    return jsonify({
        "symbol": asset["symbol"],
        "price_display": asset["price_display"],
        "change_display": asset["change_display"],
        "dir": asset["dir"],
        "signal": asset["signal"],
        "confidence_text": f"{int(asset['signal_meta']['confidence'] * 100)}% — {asset['signal_meta']['confidence_label']}",
        "updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    })


@app.route("/")
def home():
    crypto_list = fetch_crypto_quotes_safe()
    stock_list = fetch_stock_quotes_safe()

    featured_crypto = dict(crypto_list[0]) if crypto_list else {
        "symbol": "N/A",
        "name": "Crypto data unavailable",
        "price": 0.0,
        "change": 0.0,
        "dir": "down",
        "signal": "HOLD",
        "logo": None,
    }

    featured_stock = dict(stock_list[0]) if stock_list else {
        "symbol": "N/A",
        "name": "Stock / commodity data unavailable",
        "price": 0.0,
        "change": 0.0,
        "dir": "down",
        "signal": "HOLD",
        "logo": None,
        "icon": "📈",
    }

    featured_crypto["price_display"] = fmt_price(featured_crypto["price"], featured_crypto["symbol"])
    featured_crypto["change_display"] = fmt_change(featured_crypto["change"])
    featured_stock["price_display"] = fmt_price(featured_stock["price"], featured_stock["symbol"])
    featured_stock["change_display"] = fmt_change(featured_stock["change"])

    crypto_candles = None
    if featured_crypto["symbol"] != "N/A":
        crypto_candles = fetch_crypto_candles(featured_crypto["symbol"], interval="15m", limit=40)

    stock_candles = None
    if featured_stock["symbol"] != "N/A":
        stock_candles = fetch_stock_candles(featured_stock["symbol"], period="3mo", interval="1d")

    content = render_template_string("""
    <section class="hero">
      <div class="hero-card">
        <div class="badge">AVA Markets Core</div>
        <h1>Live-feeling market intelligence for crypto, stocks, and commodities.</h1>
        <p>
          Browse market data for free, unlock multi-timeframe signals on Basic,
          and access forecast + trend intelligence on Pro and above.
        </p>
        <div class="btns">
          <a class="btn btn-primary" href="/crypto">Explore Crypto</a>
          <a class="btn btn-secondary" href="/stocks">Explore Stocks</a>
          <a class="btn btn-secondary" href="/performance">See Track Record</a>
        </div>
      </div>

      <div class="card featured-shell">
        <div class="badge">Featured {{ featured_crypto.symbol }}</div>
        <div class="asset-feature">
          {% if featured_crypto.logo %}
            <img class="asset-feature-logo" src="{{ featured_crypto.logo }}" alt="{{ featured_crypto.symbol }}" onerror="this.style.display='none'">
          {% endif %}
          <div>
            <h2 style="margin:0;">
              {% if featured_crypto.symbol != "N/A" %}
                <a href="/crypto/{{ featured_crypto.symbol }}">{{ featured_crypto.symbol }}</a> — {{ featured_crypto.name }}
              {% else %}
                {{ featured_crypto.name }}
              {% endif %}
            </h2>
            <div class="asset-subtitle">Top crypto market snapshot</div>
          </div>
        </div>
        <div style="font-size:2.4rem;font-weight:800;">{{ featured_crypto.price_display }}
          <span class="{{ 'up' if featured_crypto.dir == 'up' else 'down' }}" style="font-size:1rem;">{{ featured_crypto.change_display }}</span>
        </div>
        <p>Free preview shows lightweight market sentiment. Premium pages unlock multi-timeframe signals.</p>
        <div style="margin:16px 0 18px;">
          {% if featured_crypto.symbol != "N/A" and signals %}
            <span class="signal {{ 'signal-buy' if featured_crypto.signal == 'BUY' else 'signal-hold' if featured_crypto.signal == 'HOLD' else 'signal-sell' }}">{{ featured_crypto.signal }}</span>
          {% elif featured_crypto.symbol != "N/A" %}
            <span class="signal signal-locked">Signal Locked</span>
          {% else %}
            <span class="signal signal-locked">Unavailable</span>
          {% endif %}
        </div>
        <div class="candle-box">{{ crypto_candles|safe }}</div>
      </div>
    </section>

    <section class="section">
      <h2 class="section-title">Featured stock / commodity</h2>
      <div class="card featured-shell">
        <div class="asset-feature">
          {% if featured_stock.logo %}
            <img class="asset-feature-logo" src="{{ featured_stock.logo }}" alt="{{ featured_stock.symbol }}" onerror="this.style.display='none'">
          {% else %}
            <span class="asset-feature-icon">{{ featured_stock.icon or "📈" }}</span>
          {% endif %}
          <div>
            <h2 style="margin:0;">
              {% if featured_stock.symbol != "N/A" %}
                <a href="/stocks/{{ featured_stock.symbol }}">{{ featured_stock.symbol }}</a> — {{ featured_stock.name }}
              {% else %}
                {{ featured_stock.name }}
              {% endif %}
            </h2>
            <div class="asset-subtitle">Global stock / commodity snapshot</div>
          </div>
        </div>
        <p>{{ featured_stock.price_display }} <span class="{{ 'up' if featured_stock.dir == 'up' else 'down' }}">{{ featured_stock.change_display }}</span></p>
        <div class="candle-box">{{ stock_candles|safe }}</div>
      </div>
    </section>
    """,
    featured_crypto=featured_crypto,
    featured_stock=featured_stock,
    crypto_candles=render_candles_from_ohlc(crypto_candles) if crypto_candles else fallback_candles_html(1),
    stock_candles=render_candles_from_ohlc(stock_candles) if stock_candles else fallback_candles_html(2),
    signals=can_access_signals())

    seo_block = """
    <section class="section">
      <div class="card">
        <h2>Crypto signals, stock signals, and market trend analysis in one platform</h2>
        <p>
          AVA Markets helps users discover crypto signals, stock signals, commodity movement,
          trend analysis, and forecast intelligence through a live-updating market dashboard.
          Whether you are tracking Bitcoin, Ethereum, Apple, NVIDIA, gold, or oil, the platform
          is designed to make technical market intelligence easier to understand.
        </p>
        <p>
          Use AVA Markets to explore buy, hold, and sell signals, compare short-term and trend-based
          views, inspect confidence labels, and review transparent signal performance across markets.
        </p>
      </div>
    </section>
    """

    popular_links = """
    <section class="section">
      <div class="card">
        <h2>Popular market pages</h2>
        <div class="btns">
          <a class="btn btn-secondary" href="/crypto/BTC">Bitcoin (BTC)</a>
          <a class="btn btn-secondary" href="/crypto/ETH">Ethereum (ETH)</a>
          <a class="btn btn-secondary" href="/crypto/SOL">Solana (SOL)</a>
          <a class="btn btn-secondary" href="/stocks/AAPL">Apple (AAPL)</a>
          <a class="btn btn-secondary" href="/stocks/MSFT">Microsoft (MSFT)</a>
          <a class="btn btn-secondary" href="/stocks/NVDA">NVIDIA (NVDA)</a>
          <a class="btn btn-secondary" href="/stocks/GC=F">Gold</a>
          <a class="btn btn-secondary" href="/stocks/CL=F">Oil</a>
        </div>
      </div>
    </section>
    """

    full_content = content + seo_block + popular_links

    return nav_layout(
        "Crypto Signals, Stock Signals, Trends & Forecasts - AVA Markets",
        full_content,
        meta_description="AVA Markets is a live-updating crypto and stock signals platform with market forecasts, trend analysis, buy-hold-sell views, and transparent performance tracking.",
        json_ld_override=homepage_json_ld()
    )


@app.route("/crypto")
def crypto():
    page = get_int_arg("page", default=1, min_value=1)
    search = (request.args.get("q") or "").strip().lower()

    assets = [dict(a) for a in fetch_crypto_quotes_safe()]
    for a in assets:
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])

    if search:
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_CRYPTO)
    unlocked = can_access_signals()

    rows = ""
    for a in page_items:
        sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[a["signal"]]
        signal_html = f'<span id="signal-{h(a["symbol"])}" class="signal {sig_class}">{h(a["signal"])}</span>' if unlocked else '<span class="signal signal-locked">Locked</span>'
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">
              <img class="asset-logo" src="{h(a.get('logo', ''))}" alt="{h(a['symbol'])}" onerror="this.style.display='none'">
              <a href="/crypto/{h(a['symbol'])}">{h(a['symbol'])}</a>
            </strong>
            <span>{h(a['name'])}</span>
          </td>
          <td id="price-{h(a['symbol'])}">{h(a['price_display'])}</td>
          <td id="change-{h(a['symbol'])}" class="{'up' if a['dir']=='up' else 'down'}">{h(a['change_display'])}</td>
          <td>{signal_html}</td>
          <td>{"Unlocked" if unlocked else "Basic+"}</td>
        </tr>
        """

    pagination = ""
    sq = safe_query_value(search)
    if current > 1:
        pagination += f'<a class="page-link" href="/crypto?page={current-1}&q={sq}">Previous</a>'
    pagination += f'<span class="page-link">Page {current} / {pages}</span>'
    if current < pages:
        pagination += f'<a class="page-link" href="/crypto?page={current+1}&q={sq}">Next</a>'

    content = f"""
    <section class="section">
      <h1>Crypto</h1>
      <p class="section-sub">Top crypto assets with live-updating previews and premium multi-timeframe detail pages.</p>
      <div id="live-updated-crypto" class="live-stamp">Last updated: {h(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))} UTC</div>

      <form method="GET" style="margin-bottom:20px;">
        <input class="search-box" type="text" name="q" placeholder="Search crypto symbol or name..." value="{h(search)}">
      </form>

      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>24h</th><th>Preview Signal</th><th>Access</th></tr>
          {rows or "<tr><td colspan='5'>No crypto data available right now.</td></tr>"}
        </table>
      </div>

      <div class="pagination">{pagination}</div>
      {legal_disclaimer_html()}
    </section>
    """

    return nav_layout(
        "Crypto Market Signals - AVA Markets",
        content,
        extra_head=live_update_script("crypto_list"),
        meta_description="Browse live-updating crypto markets on AVA Markets with price movement previews, premium multi-timeframe signals, and detailed asset intelligence.",
        json_ld_override=build_breadcrumbs([
            {"name": "Home", "url": Config.DOMAIN + "/"},
            {"name": "Crypto", "url": Config.DOMAIN + "/crypto"}
        ])
    )


@app.route("/stocks")
def stocks():
    page = get_int_arg("page", default=1, min_value=1)
    search = (request.args.get("q") or "").strip().lower()

    assets = [dict(a) for a in fetch_stock_quotes_safe()]
    for a in assets:
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])

    if search:
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_STOCKS)
    unlocked = can_access_signals()

    rows = ""
    for a in page_items:
        safe_id = a["symbol"].replace("=", "_").replace("-", "_")
        media = (
            f'<img class="asset-logo" src="{h(a.get("logo",""))}" alt="{h(a["symbol"])}" onerror="this.style.display=\'none\'">'
            if a.get("logo") else
            f'<span class="asset-icon">{h(a.get("icon","📈"))}</span>'
        )
        sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[a["signal"]]
        signal_html = f'<span id="signal-{h(safe_id)}" class="signal {sig_class}">{h(a["signal"])}</span>' if unlocked else '<span class="signal signal-locked">Locked</span>'
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">
              {media}
              <a href="/stocks/{h(a['symbol'])}">{h(a['symbol'])}</a>
            </strong>
            <span>{h(a['name'])}</span>
          </td>
          <td id="price-{h(safe_id)}">{h(a['price_display'])}</td>
          <td id="change-{h(safe_id)}" class="{'up' if a['dir']=='up' else 'down'}">{h(a['change_display'])}</td>
          <td>{signal_html}</td>
          <td>{"Unlocked" if unlocked else "Basic+"}</td>
        </tr>
        """

    pagination = ""
    sq = safe_query_value(search)
    if current > 1:
        pagination += f'<a class="page-link" href="/stocks?page={current-1}&q={sq}">Previous</a>'
    pagination += f'<span class="page-link">Page {current} / {pages}</span>'
    if current < pages:
        pagination += f'<a class="page-link" href="/stocks?page={current+1}&q={sq}">Next</a>'

    content = f"""
    <section class="section">
      <h1>Stocks + Commodities</h1>
      <p class="section-sub">Global market list with live-updating previews and premium multi-timeframe detail analysis.</p>
      <div id="live-updated-stocks" class="live-stamp">Last updated: {h(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))} UTC</div>

      <form method="GET" style="margin-bottom:20px;">
        <input class="search-box" type="text" name="q" placeholder="Search stock or commodity..." value="{h(search)}">
      </form>

      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>Preview Signal</th><th>Access</th></tr>
          {rows or "<tr><td colspan='5'>No stock data available right now.</td></tr>"}
        </table>
      </div>

      <div class="pagination">{pagination}</div>
      {legal_disclaimer_html()}
    </section>
    """

    return nav_layout(
        "Stocks and Commodities Signals - AVA Markets",
        content,
        extra_head=live_update_script("stock_list"),
        meta_description="Explore stocks and commodities on AVA Markets with live-updating prices, market previews, premium multi-timeframe signals, trend analysis, and forecasts.",
        json_ld_override=build_breadcrumbs([
            {"name": "Home", "url": Config.DOMAIN + "/"},
            {"name": "Stocks", "url": Config.DOMAIN + "/stocks"}
        ])
    )


@app.route("/crypto/<symbol>")
def crypto_detail(symbol):
    asset = get_crypto_detail(symbol)
    if not asset:
        abort(404)

    unlocked_signals = can_access_signals()
    unlocked_forecast = can_access_forecast()
    unlocked_trends = can_access_trends()

    in_watchlist = False
    if g.user:
        wl = db.get_watchlist(g.user["id"])
        in_watchlist = any(x["asset_type"] == "crypto" and x["symbol"] == asset["symbol"] for x in wl)

    sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[asset["signal"]]
    signal_html = f'<span id="detail-signal" class="signal {sig_class}">{h(asset["signal"])}</span>' if unlocked_signals else '<span class="signal signal-locked">Locked</span>'

    content = render_template_string("""
    <section class="section">
      <div class="asset-feature" style="margin-bottom:12px;">
        {% if asset.logo %}
          <img class="asset-feature-logo" src="{{ asset.logo }}" alt="{{ asset.symbol }}" onerror="this.style.display='none'">
        {% endif %}
        <div>
          <h1 style="margin:0;">{{ asset.symbol }} — {{ asset.name }}</h1>
          <div class="asset-subtitle">Multi-timeframe crypto intelligence with short-term, intraday, and swing signals</div>
          <div id="detail-updated" class="live-stamp">Last updated: {{ updated_at }} UTC</div>
        </div>
      </div>

      <div class="btns" style="margin-bottom:18px;">
        {% if user %}
          {% if in_watchlist %}
          <form method="POST" action="/watchlist/remove/crypto/{{ asset.symbol }}">
            <button class="btn btn-secondary" type="submit">Remove from Watchlist</button>
          </form>
          {% else %}
          <form method="POST" action="/watchlist/add/crypto/{{ asset.symbol }}">
            <button class="btn btn-primary" type="submit">Add to Watchlist</button>
          </form>
          {% endif %}
        {% endif %}
      </div>

      <div class="detail-grid">
        <div class="card">
          <div class="badge">Live Market</div>
          <h2><span id="detail-price">{{ asset.price_display }}</span> <span id="detail-change" class="{{ 'up' if asset.dir == 'up' else 'down' }}">{{ asset.change_display }}</span></h2>
          <div style="margin:12px 0 18px;">{{ signal_html|safe }}</div>
          <div class="candle-box">{{ asset.detail_candles|safe }}</div>
        </div>

        <div class="mini-grid">
          <div class="metric-card">
            <h3>Overall Signal</h3>
            <p>{% if unlocked_signals %}{{ asset.signal_meta.signal }}{% else %}Locked{% endif %}</p>
            <p id="detail-confidence">Confidence: {% if unlocked_signals %}{{ (asset.signal_meta.confidence * 100)|round(0) }}% — {{ asset.signal_meta.confidence_label }}{% else %}Basic+{% endif %}</p>
          </div>
          <div class="metric-card">
            <h3>RSI</h3>
            <p>{% if unlocked_signals %}{{ asset.signal_meta.rsi }}{% else %}Locked{% endif %}</p>
            <p>Momentum: {% if unlocked_signals %}{{ asset.signal_meta.momentum }}%{% else %}Basic+{% endif %}</p>
          </div>
          <div class="metric-card">
            <h3>Trend</h3>
            <p>{% if unlocked_trends %}{{ asset.trend_data.state }}{% else %}Locked{% endif %}</p>
            <p>{% if unlocked_trends %}{{ asset.trend_data.strength }} strength{% else %}Pro+{% endif %}</p>
          </div>
          <div class="metric-card">
            <h3>Forecast</h3>
            <p>{% if unlocked_forecast %}{{ asset.forecast.projected_change }}{% else %}Locked{% endif %}</p>
            <p>{% if unlocked_forecast %}{{ asset.forecast.trend }}{% else %}Pro+{% endif %}</p>
          </div>
        </div>
      </div>

      <div class="card" style="margin-top:24px;">
        <h2>Signal Types</h2>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Timeframe</th><th>Type</th><th>Signal</th><th>Confidence</th><th>Trend</th><th>Regime</th></tr>
            {% for tf, item in asset.multi_timeframes.items() %}
            <tr>
              <td>{{ tf }}</td>
              <td>{{ item.signal_type }}</td>
              <td>{% if unlocked_signals %}{{ item.signal }}{% else %}Locked{% endif %}</td>
              <td>{% if unlocked_signals %}{{ (item.confidence * 100)|round(0) }}% — {{ item.confidence_label }}{% else %}Basic+{% endif %}</td>
              <td>{% if unlocked_trends %}{{ item.trend_state }}{% else %}Locked{% endif %}</td>
              <td>{% if unlocked_trends %}{{ item.regime }}{% else %}Pro+{% endif %}</td>
            </tr>
            {% endfor %}
          </table>
        </div>
      </div>

      <div class="card" style="margin-top:24px;">
        <h2>Why this signal?</h2>
        <p><strong>Confidence note:</strong> {{ asset.signal_meta.confidence_note }}</p>
        <ul>
          {% if unlocked_signals %}
            {% for line in asset.signal_meta.why %}
              <li>{{ line }}</li>
            {% endfor %}
          {% else %}
            <li>Upgrade to Basic+ to inspect signal reasoning.</li>
          {% endif %}
        </ul>
      </div>

      {{ disclaimer|safe }}
    </section>
    """,
    asset=asset,
    signal_html=signal_html,
    unlocked_signals=unlocked_signals,
    unlocked_forecast=unlocked_forecast,
    unlocked_trends=unlocked_trends,
    in_watchlist=in_watchlist,
    user=g.get("user"),
    disclaimer=legal_disclaimer_html(),
    updated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

    return nav_layout(
        f"{asset['symbol']} Price, Signal & Forecast - AVA Markets",
        content,
        extra_head=live_update_script("detail", asset["symbol"], "crypto"),
        meta_description=f"Track {asset['name']} ({asset['symbol']}) on AVA Markets with live price updates, multi-timeframe signal analysis, trend regime view, and forecast intelligence.",
        canonical_url=f"{Config.DOMAIN}/crypto/{asset['symbol']}",
        og_type="article",
        json_ld_override=[asset_json_ld("crypto", asset), build_breadcrumbs([
            {"name": "Home", "url": Config.DOMAIN + "/"},
            {"name": "Crypto", "url": Config.DOMAIN + "/crypto"},
            {"name": asset["symbol"], "url": f"{Config.DOMAIN}/crypto/{asset['symbol']}"}
        ])]
    )


@app.route("/stocks/<symbol>")
def stock_detail(symbol):
    asset = get_stock_detail(symbol)
    if not asset:
        abort(404)

    unlocked_signals = can_access_signals()
    unlocked_forecast = can_access_forecast()
    unlocked_trends = can_access_trends()

    in_watchlist = False
    if g.user:
        wl = db.get_watchlist(g.user["id"])
        in_watchlist = any(x["asset_type"] == "stock" and x["symbol"] == asset["symbol"] for x in wl)

    sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[asset["signal"]]
    signal_html = f'<span id="detail-signal" class="signal {sig_class}">{h(asset["signal"])}</span>' if unlocked_signals else '<span class="signal signal-locked">Locked</span>'

    content = render_template_string("""
    <section class="section">
      <div class="asset-feature" style="margin-bottom:12px;">
        {% if asset.logo %}
          <img class="asset-feature-logo" src="{{ asset.logo }}" alt="{{ asset.symbol }}" onerror="this.style.display='none'">
        {% else %}
          <span class="asset-feature-icon">{{ asset.icon or "📈" }}</span>
        {% endif %}
        <div>
          <h1 style="margin:0;">{{ asset.symbol }} — {{ asset.name }}</h1>
          <div class="asset-subtitle">Multi-timeframe stock intelligence with short-term, trend, and macro signals</div>
          <div id="detail-updated" class="live-stamp">Last updated: {{ updated_at }} UTC</div>
        </div>
      </div>

      <div class="btns" style="margin-bottom:18px;">
        {% if user %}
          {% if in_watchlist %}
          <form method="POST" action="/watchlist/remove/stock/{{ asset.symbol }}">
            <button class="btn btn-secondary" type="submit">Remove from Watchlist</button>
          </form>
          {% else %}
          <form method="POST" action="/watchlist/add/stock/{{ asset.symbol }}">
            <button class="btn btn-primary" type="submit">Add to Watchlist</button>
          </form>
          {% endif %}
        {% endif %}
      </div>

      <div class="detail-grid">
        <div class="card">
          <div class="badge">Live Market</div>
          <h2><span id="detail-price">{{ asset.price_display }}</span> <span id="detail-change" class="{{ 'up' if asset.dir == 'up' else 'down' }}">{{ asset.change_display }}</span></h2>
          <div style="margin:12px 0 18px;">{{ signal_html|safe }}</div>
          <div class="candle-box">{{ asset.detail_candles|safe }}</div>
        </div>

        <div class="mini-grid">
          <div class="metric-card">
            <h3>Overall Signal</h3>
            <p>{% if unlocked_signals %}{{ asset.signal_meta.signal }}{% else %}Locked{% endif %}</p>
            <p id="detail-confidence">Confidence: {% if unlocked_signals %}{{ (asset.signal_meta.confidence * 100)|round(0) }}% — {{ asset.signal_meta.confidence_label }}{% else %}Basic+{% endif %}</p>
          </div>
          <div class="metric-card">
            <h3>RSI</h3>
            <p>{% if unlocked_signals %}{{ asset.signal_meta.rsi }}{% else %}Locked{% endif %}</p>
            <p>Momentum: {% if unlocked_signals %}{{ asset.signal_meta.momentum }}%{% else %}Basic+{% endif %}</p>
          </div>
          <div class="metric-card">
            <h3>Trend</h3>
            <p>{% if unlocked_trends %}{{ asset.trend_data.state }}{% else %}Locked{% endif %}</p>
            <p>{% if unlocked_trends %}{{ asset.trend_data.strength }} strength{% else %}Pro+{% endif %}</p>
          </div>
          <div class="metric-card">
            <h3>Forecast</h3>
            <p>{% if unlocked_forecast %}{{ asset.forecast.projected_change }}{% else %}Locked{% endif %}</p>
            <p>{% if unlocked_forecast %}{{ asset.forecast.trend }}{% else %}Pro+{% endif %}</p>
          </div>
        </div>
      </div>

      <div class="card" style="margin-top:24px;">
        <h2>Signal Types</h2>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Timeframe</th><th>Type</th><th>Signal</th><th>Confidence</th><th>Trend</th><th>Regime</th></tr>
            {% for tf, item in asset.multi_timeframes.items() %}
            <tr>
              <td>{{ tf }}</td>
              <td>{{ item.signal_type }}</td>
              <td>{% if unlocked_signals %}{{ item.signal }}{% else %}Locked{% endif %}</td>
              <td>{% if unlocked_signals %}{{ (item.confidence * 100)|round(0) }}% — {{ item.confidence_label }}{% else %}Basic+{% endif %}</td>
              <td>{% if unlocked_trends %}{{ item.trend_state }}{% else %}Locked{% endif %}</td>
              <td>{% if unlocked_trends %}{{ item.regime }}{% else %}Pro+{% endif %}</td>
            </tr>
            {% endfor %}
          </table>
        </div>
      </div>

      <div class="card" style="margin-top:24px;">
        <h2>Why this signal?</h2>
        <p><strong>Confidence note:</strong> {{ asset.signal_meta.confidence_note }}</p>
        <ul>
          {% if unlocked_signals %}
            {% for line in asset.signal_meta.why %}
              <li>{{ line }}</li>
            {% endfor %}
          {% else %}
            <li>Upgrade to Basic+ to inspect signal reasoning.</li>
          {% endif %}
        </ul>
      </div>

      {{ disclaimer|safe }}
    </section>
    """,
    asset=asset,
    signal_html=signal_html,
    unlocked_signals=unlocked_signals,
    unlocked_forecast=unlocked_forecast,
    unlocked_trends=unlocked_trends,
    in_watchlist=in_watchlist,
    user=g.get("user"),
    disclaimer=legal_disclaimer_html(),
    updated_at=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))

    return nav_layout(
        f"{asset['symbol']} Price, Signal & Forecast - AVA Markets",
        content,
        extra_head=live_update_script("detail", asset["symbol"], "stock"),
        meta_description=f"Track {asset['name']} ({asset['symbol']}) on AVA Markets with live price updates, multi-timeframe signal analysis, trend regime view, and forecast intelligence.",
        canonical_url=f"{Config.DOMAIN}/stocks/{asset['symbol']}",
        og_type="article",
        json_ld_override=[asset_json_ld("stock", asset), build_breadcrumbs([
            {"name": "Home", "url": Config.DOMAIN + "/"},
            {"name": "Stocks", "url": Config.DOMAIN + "/stocks"},
            {"name": asset["symbol"], "url": f"{Config.DOMAIN}/stocks/{asset['symbol']}"}
        ])]
    )

@app.route("/forecast")
def forecast():
    unlocked = can_access_forecast()
    sample = get_crypto_detail("BTC") or {}
    f = sample.get("forecast", {"trend": "Stable", "projected_change": "+0.00%", "confidence_band": "90%", "summary": "Not enough data."})

    cards = """
      <div class="price-card"><h3>Trend</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Projected View</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Confidence Band</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
    """
    if unlocked:
        cards = f"""
          <div class="price-card"><h3>Trend</h3><div style="font-size:2rem;font-weight:800;" class="up">{h(f['trend'])}</div><p>{h(f['summary'])}</p></div>
          <div class="price-card"><h3>Projected View</h3><div style="font-size:2rem;font-weight:800;" class="up">{h(f['projected_change'])}</div><p>Multi-timeframe projection signal output.</p></div>
          <div class="price-card"><h3>Confidence Band</h3><div style="font-size:2rem;font-weight:800;">{h(f['confidence_band'])}</div><p>Indicator agreement level, not guaranteed outcome probability.</p></div>
        """
    content = f"""
    <section class="section">
      <h1>Forecast</h1>
      <p class="section-sub">Forecast intelligence is built from multi-timeframe AVA Brain analysis.</p>
      <div class="market-grid">{cards}</div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout(
        "Forecast Intelligence - AVA Markets",
        content,
        meta_description="Explore AVA Markets forecast intelligence built from multi-timeframe technical analysis across crypto, stocks, and commodities."
    )


@app.route("/trends")
def trends():
    unlocked = can_access_trends()
    sample = get_crypto_detail("BTC") or {}
    t = sample.get("trend_data", {"state": "Neutral", "strength": "Low", "read": "Mixed", "summary": "Not enough data."})

    cards = """
      <div class="price-card"><h3>Trend State</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Trend Strength</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Regime Read</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
    """
    if unlocked:
        cards = f"""
          <div class="price-card"><h3>Trend State</h3><div style="font-size:2rem;font-weight:800;" class="up">{h(t['state'])}</div><p>{h(t['summary'])}</p></div>
          <div class="price-card"><h3>Trend Strength</h3><div style="font-size:2rem;font-weight:800;">{h(t['strength'])}</div><p>Directional magnitude from the multi-timeframe model.</p></div>
          <div class="price-card"><h3>Regime Read</h3><div style="font-size:2rem;font-weight:800;">{h(t['read'])}</div><p>Current market structure classification.</p></div>
        """
    content = f"""
    <section class="section">
      <h1>Trends</h1>
      <p class="section-sub">Trend intelligence adapts to market regime instead of relying on one static rule set.</p>
      <div class="market-grid">{cards}</div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout(
        "Trend Intelligence - AVA Markets",
        content,
        meta_description="Explore AVA Markets trend intelligence with regime-aware market analysis, trend state, strength, and directional context."
    )


@app.route("/performance")
def performance():
    refresh_signal_outcomes()
    rows = db.get_performance_summary(days=30)
    summary = summarize_performance(rows)

    table_rows = ""
    for r in rows[:100]:
        outcome_class = "up" if r["outcome"] == "correct" else "down"
        ret_val = float(r["return_pct"] or 0)
        conf_pct = float(r["confidence"] or 0) * 100
        table_rows += f"""
        <tr>
          <td>{h(r['asset_type'])}</td>
          <td>{h(r['symbol'])}</td>
          <td>{h(r['timeframe'])}</td>
          <td>{h(r['signal_type'])}</td>
          <td>{h(r['signal'])}</td>
          <td>{conf_pct:.0f}%</td>
          <td class="{outcome_class}">{h(r['outcome'])}</td>
          <td class="{'up' if ret_val >= 0 else 'down'}">{ret_val:+.2f}%</td>
          <td>{h(r['horizon'])}</td>
          <td>{h(r['created_at'])}</td>
        </tr>
        """

    best_assets_html = "".join(f"<li>{h(sym)}: {ret:+.2f}% avg</li>" for sym, ret in summary["best_assets"]) or "<li>No evaluated signals yet.</li>"

    content = f"""
    <section class="section">
      <h1>Signal Performance</h1>
      <p class="section-sub">Transparent 30-day signal track record across evaluated horizons.</p>

      <div class="market-grid">
        <div class="price-card"><h3>Total Evaluated</h3><div style="font-size:2rem;font-weight:800;">{summary['total']}</div></div>
        <div class="price-card"><h3>BUY Accuracy</h3><div style="font-size:2rem;font-weight:800;">{summary['buy_accuracy']}%</div></div>
        <div class="price-card"><h3>SELL Accuracy</h3><div style="font-size:2rem;font-weight:800;">{summary['sell_accuracy']}%</div></div>
        <div class="price-card"><h3>HOLD Accuracy</h3><div style="font-size:2rem;font-weight:800;">{summary['hold_accuracy']}%</div></div>
      </div>

      <div class="card" style="margin-top:24px;">
        <h3>Average Return</h3>
        <p style="font-size:1.2rem;"><strong>{summary['avg_return']:+.2f}%</strong></p>
        <h3>Best Assets</h3>
        <ul>{best_assets_html}</ul>
      </div>

      <div class="table-shell" style="margin-top:24px;">
        <table class="market-table">
          <tr>
            <th>Market</th><th>Symbol</th><th>Timeframe</th><th>Signal Type</th>
            <th>Signal</th><th>Confidence</th><th>Outcome</th><th>Return</th><th>Horizon</th><th>Created</th>
          </tr>
          {table_rows or "<tr><td colspan='10'>No performance data yet.</td></tr>"}
        </table>
      </div>

      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout(
        "Signal Performance - AVA Markets",
        content,
        meta_description="Review transparent AVA Markets signal performance, including BUY, SELL, and HOLD accuracy, evaluated returns, and recent track record data."
    )


@app.route("/signal-changes")
def signal_changes():
    rows = db.get_recent_signal_changes(limit=100)
    table_rows = ""
    for r in rows:
        table_rows += f"""
        <tr>
          <td>{h(r['asset_type'])}</td>
          <td>{h(r['symbol'])}</td>
          <td>{h(r['timeframe'])}</td>
          <td>{h(r['signal_type'])}</td>
          <td>{h(r.get('old_signal') or '-')}</td>
          <td>{h(r.get('new_signal') or '-')}</td>
          <td>{h(r['changed_at'])}</td>
        </tr>
        """
    content = f"""
    <section class="section">
      <h1>Recent Signal Changes</h1>
      <p class="section-sub">Track when signals flipped, not just what they are now.</p>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Market</th><th>Symbol</th><th>Timeframe</th><th>Type</th><th>Old</th><th>New</th><th>Changed</th></tr>
          {table_rows or "<tr><td colspan='7'>No signal changes yet.</td></tr>"}
        </table>
      </div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout(
        "Recent Signal Changes - AVA Markets",
        content,
        meta_description="Track recent signal flips across crypto, stocks, and commodities on AVA Markets, including timeframe, signal type, and change history."
    )


@app.route("/pricing")
def pricing():
    billing_ready = bool(stripe and Config.STRIPE_SECRET_KEY and Config.STRIPE_WEBHOOK_SECRET)

    content = render_template_string("""
    <section class="section">
      <h1>Pricing</h1>
      <p class="section-sub">Launch pricing designed to reduce friction while proving trust and retention.</p>

      {% if not billing_ready %}
      <div class="card" style="margin-bottom:24px;">
        <h3>Billing temporarily unavailable</h3>
        <p>Stripe checkout is not available right now. Browse the platform and try again later.</p>
      </div>
      {% endif %}

      <div class="market-grid">
        <div class="price-card">
          <h3>Free</h3>
          <div style="font-size:2.4rem;font-weight:800;">$0</div>
          <p>Browse crypto + stocks • candles • market movement previews</p>
        </div>

        <div class="price-card">
          <h3>Basic</h3>
          <div style="font-size:2.4rem;font-weight:800;">$9</div>
          <p>Unlock multi-timeframe crypto + stock signals and confidence labels</p>
          {% if user and billing_ready %}
            <form method="POST" action="/checkout/basic"><button class="btn btn-primary" type="submit">Choose Basic</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Pro</h3>
          <div style="font-size:2.4rem;font-weight:800;">$29</div>
          <p>Everything in Basic • forecasts • trend regime views • better decision context</p>
          {% if user and billing_ready %}
            <form method="POST" action="/checkout/pro"><button class="btn btn-primary" type="submit">Choose Pro</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Elite</h3>
          <div style="font-size:2.4rem;font-weight:800;">$79</div>
          <p>Everything in Pro • highest access tier • positioned for serious users</p>
          {% if user and billing_ready %}
            <form method="POST" action="/checkout/elite"><button class="btn btn-primary" type="submit">Choose Elite</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Enterprise</h3>
          <div style="font-size:2.4rem;font-weight:800;">Custom</div>
          <p>API access, custom market coverage, team seats, white-label, and direct support</p>
          <a class="btn btn-secondary" href="mailto:{{ contact_email }}">Contact Us</a>
        </div>
      </div>

      {% if not user %}
      <div class="btns">
        <a class="btn btn-primary" href="/register">Create Account</a>
        <a class="btn btn-secondary" href="/login">Login</a>
      </div>
      {% endif %}

      {{ disclaimer|safe }}
    </section>
    """, user=g.get("user"), contact_email=Config.CONTACT_EMAIL, disclaimer=legal_disclaimer_html(), billing_ready=billing_ready)

    return nav_layout(
        "Pricing - AVA Markets",
        content,
        meta_description="View AVA Markets pricing for free market browsing, Basic signal access, Pro forecast and trend intelligence, Elite access, and custom Enterprise plans.",
        json_ld_override=faq_json_ld([
            ("What does the Free plan include?", "The Free plan includes browsing crypto, stocks, commodities, candle views, and market movement previews."),
            ("What does Basic unlock?", "Basic unlocks multi-timeframe signal access for crypto and stocks."),
            ("What does Pro unlock?", "Pro unlocks everything in Basic plus forecast views and trend intelligence."),
            ("Is Enterprise pricing fixed?", "No. Enterprise is custom and designed for API access, teams, and advanced commercial use cases.")
        ])
    )


@app.route("/watchlist/add/<asset_type>/<symbol>", methods=["POST"])
@require_login
def watchlist_add(asset_type, symbol):
    if asset_type not in ["crypto", "stock"]:
        return redirect("/dashboard")
    db.add_watchlist(g.user["id"], asset_type, symbol)
    return redirect(safe_redirect_target("/dashboard"))


@app.route("/watchlist/remove/<asset_type>/<symbol>", methods=["POST"])
@require_login
def watchlist_remove(asset_type, symbol):
    if asset_type not in ["crypto", "stock"]:
        return redirect("/dashboard")
    db.remove_watchlist(g.user["id"], asset_type, symbol)
    return redirect(safe_redirect_target("/dashboard"))


@app.route("/register", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def register():
    if g.user:
        return redirect("/dashboard")

    if request.method == "GET":
        content = """
        <div class="form-shell">
          <div class="form-card">
            <h1>Create account</h1>
            <p>Start using AVA Markets in minutes.</p>
            <form method="POST">
              <input type="email" name="email" placeholder="Email" required>
              <input type="password" name="password" placeholder="Password (min 8 chars)" required>
              <button type="submit">Register</button>
            </form>
          </div>
        </div>
        """
        return nav_layout(
            "Register - AVA Markets",
            content,
            robots_content="noindex, nofollow"
        )

    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()

    if not email or "@" not in email or len(password) < 8:
        return nav_layout(
            "Register Error",
            '<div class="form-shell"><div class="form-card"><div class="error">Valid email and password (min 8 chars) required.</div><a href="/register">Try again</a></div></div>',
            robots_content="noindex, nofollow"
        )

    user = db.create_user(email, password)
    if not user:
        return nav_layout(
            "Register Error",
            '<div class="form-shell"><div class="form-card"><div class="error">Email already registered.</div><a href="/login">Login instead</a></div></div>',
            robots_content="noindex, nofollow"
        )

    token = db.create_session(user["id"])
    resp = make_response(redirect("/dashboard"))
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", secure=Config.COOKIE_SECURE, max_age=30 * 86400)
    return resp


@app.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def login():
    if g.user:
        return redirect("/dashboard")

    if request.method == "GET":
        content = """
        <div class="form-shell">
          <div class="form-card">
            <h1>Login</h1>
            <p>Access your AVA Markets account.</p>
            <form method="POST">
              <input type="email" name="email" placeholder="Email" required>
              <input type="password" name="password" placeholder="Password" required>
              <button type="submit">Login</button>
            </form>
          </div>
        </div>
        """
        return nav_layout(
            "Login - AVA Markets",
            content,
            robots_content="noindex, nofollow"
        )

    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()

    user = db.verify_user(email, password)
    if not user:
        return nav_layout(
            "Login Error",
            '<div class="form-shell"><div class="form-card"><div class="error">Invalid credentials.</div><a href="/login">Try again</a></div></div>',
            robots_content="noindex, nofollow"
        )

    token = db.create_session(user["id"])
    resp = make_response(redirect("/dashboard"))
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", secure=Config.COOKIE_SECURE, max_age=30 * 86400)
    return resp


@app.route("/logout")
def logout():
    token = request.cookies.get("session_token")
    if token:
        db.delete_session(token)
    resp = make_response(redirect("/"))
    resp.delete_cookie("session_token", secure=Config.COOKIE_SECURE, samesite="Lax")
    return resp


@app.route("/dashboard")
@require_login
def dashboard():
    user = g.user
    watchlist = db.get_watchlist(user["id"])

    watch_rows = ""
    for item in watchlist[:20]:
        href = f"/crypto/{h(item['symbol'])}" if item["asset_type"] == "crypto" else f"/stocks/{h(item['symbol'])}"
        watch_rows += f"""
        <tr>
          <td>{h(item['asset_type'])}</td>
          <td><a href="{href}">{h(item['symbol'])}</a></td>
          <td>{h(item['created_at'])}</td>
          <td>
            <form method="POST" action="/watchlist/remove/{h(item['asset_type'])}/{h(item['symbol'])}">
              <button class="btn btn-secondary" type="submit">Remove</button>
            </form>
          </td>
        </tr>
        """

    billing_button = """
        <form method="POST" action="/billing/portal">
          <button class="btn btn-primary" type="submit">Billing Portal</button>
        </form>
    """ if user.get("stripe_customer_id") else '<a class="btn btn-primary" href="/pricing">Upgrade Plan</a>'

    content = f"""
    <section class="section">
      <h1>Dashboard</h1>
      <p class="section-sub">Your AVA Markets account overview.</p>

      <div class="dashboard-grid">
        <div class="dashboard-card">
          <h3>Account</h3>
          <p><strong>Email:</strong> {h(user['email'])}</p>
          <p><strong>Tier:</strong> <span class="tier">{h(user['tier'])}</span></p>
          <p><strong>Subscription:</strong> {h(user['subscription_status'])}</p>
        </div>

        <div class="dashboard-card">
          <h3>Access</h3>
          <p>Signals: {"Unlocked" if can_access_signals() else "Locked"}</p>
          <p>Forecast: {"Unlocked" if can_access_forecast() else "Locked"}</p>
          <p>Trends: {"Unlocked" if can_access_trends() else "Locked"}</p>
        </div>

        <div class="dashboard-card">
          <h3>API Key</h3>
          <div class="key">{h(user['api_key'])}</div>
        </div>
      </div>

      <div class="card" style="margin-top:24px;">
        <h2>Watchlist</h2>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Market</th><th>Symbol</th><th>Added</th><th>Action</th></tr>
            {watch_rows or "<tr><td colspan='4'>No watchlist items yet.</td></tr>"}
          </table>
        </div>
      </div>

      <div class="btns">
        {billing_button}
        <a class="btn btn-secondary" href="/performance">View Performance</a>
        <a class="btn btn-secondary" href="/signal-changes">View Signal Changes</a>
        <a class="btn btn-secondary" href="/logout">Logout</a>
      </div>
    </section>
    """
    return nav_layout(
        "Dashboard - AVA Markets",
        content,
        robots_content="noindex, nofollow"
    )


@app.route("/checkout/<tier>", methods=["POST"])
@require_login
@limiter.limit("10 per hour")
def checkout(tier):
    tier = tier.lower()
    if tier not in ["basic", "pro", "elite"]:
        return redirect("/pricing")

    if not stripe or not Config.STRIPE_SECRET_KEY or not Config.STRIPE_WEBHOOK_SECRET:
        return nav_layout(
            "Billing Unavailable",
            "<section class='section'><div class='card'><h1>Billing unavailable</h1><p>Stripe checkout is not configured correctly. Please try again later.</p></div></section>",
            robots_content="noindex, nofollow"
        ), 503

    success_url = f"{Config.DOMAIN}/dashboard?checkout=success"
    cancel_url = f"{Config.DOMAIN}/pricing?checkout=cancel"

    session = sm.create_checkout(g.user["id"], g.user["email"], tier, success_url, cancel_url)
    if session and getattr(session, "url", None):
        return redirect(session.url)

    return nav_layout(
        "Billing Error",
        "<section class='section'><div class='card'><h1>Checkout error</h1><p>We could not create your Stripe checkout session. No upgrade was applied.</p></div></section>",
        robots_content="noindex, nofollow"
    ), 503


@app.route("/billing/portal", methods=["POST"])
@require_login
@limiter.limit("20 per hour")
def billing_portal():
    if not g.user.get("stripe_customer_id"):
        return redirect("/dashboard")
    session = sm.create_portal(g.user["stripe_customer_id"], f"{Config.DOMAIN}/dashboard")
    if session and getattr(session, "url", None):
        return redirect(session.url)
    return redirect("/dashboard")


@app.route("/webhook/stripe", methods=["POST"])
@limiter.limit("60 per hour")
def stripe_webhook():
    if not stripe or not Config.STRIPE_SECRET_KEY or not Config.STRIPE_WEBHOOK_SECRET:
        logger.error("Stripe webhook requested but Stripe/webhook secret is not configured.")
        return jsonify({"error": "webhook_not_configured"}), 503

    payload = request.data
    sig = request.headers.get("Stripe-Signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig, Config.STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        logger.error(f"Bad Stripe signature: {e}")
        return jsonify({"error": "bad_signature"}), 400

    etype = event.get("type", "")
    obj = event.get("data", {}).get("object", {})

    if etype == "checkout.session.completed":
        meta = obj.get("metadata", {})
        uid = int(meta.get("user_id", 0))
        tier = (meta.get("tier") or "basic").lower()
        if uid and tier in ["basic", "pro", "elite"]:
            db.update_user(
                uid,
                stripe_customer_id=obj.get("customer"),
                stripe_subscription_id=obj.get("subscription"),
                tier=tier,
                subscription_status="active"
            )

    elif etype == "invoice.payment_succeeded":
        customer_id = obj.get("customer")
        amount = (obj.get("amount_paid") or 0) / 100
        user = db.get_user_by_stripe_customer(customer_id)
        if user:
            db.log_payment(user["id"], "stripe", obj.get("id", ""), amount, "succeeded")

    elif etype in ["customer.subscription.deleted", "customer.subscription.updated"]:
        customer_id = obj.get("customer")
        status = obj.get("status", "canceled")
        user = db.get_user_by_stripe_customer(customer_id)
        if user:
            if status in ["canceled", "unpaid", "incomplete_expired"]:
                db.update_user(user["id"], tier="free", subscription_status=status)
            else:
                db.update_user(user["id"], subscription_status=status)

    return jsonify({"received": True})


@app.route("/admin/login", methods=["GET", "POST"])
@limiter.limit("10 per minute")
def admin_login():
    if g.admin:
        return redirect("/admin")

    error = ""
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if secrets.compare_digest(username, Config.ADMIN_USERNAME) and secrets.compare_digest(password, Config.ADMIN_PASSWORD):
            admin_token = db.create_admin_session(hours=12)
            resp = make_response(redirect("/admin"))
            resp.set_cookie("admin_token", admin_token, httponly=True, samesite="Strict", secure=Config.COOKIE_SECURE, max_age=60 * 60 * 12)
            return resp

        error = "Invalid admin credentials."

    content = render_template_string("""
    <div class="form-shell">
      <div class="form-card">
        <h1>Admin Login</h1>
        <p>Restricted access.</p>
        {% if error %}<div class="error">{{ error }}</div>{% endif %}
        <form method="POST">
          <input type="text" name="username" placeholder="Username" required>
          <input type="password" name="password" placeholder="Password" required>
          <button type="submit">Enter Admin</button>
        </form>
      </div>
    </div>
    """, error=error)
    return nav_layout(
        "Admin Login - AVA Markets",
        content,
        robots_content="noindex, nofollow"
    )


@app.route("/admin/logout")
def admin_logout():
    token = request.cookies.get("admin_token")
    if token:
        db.delete_admin_session(token)
    resp = make_response(redirect("/"))
    resp.delete_cookie("admin_token", secure=Config.COOKIE_SECURE, samesite="Strict")
    return resp


@app.route("/admin")
@require_admin
def admin():
    users = db.get_all_users()
    payments = db.get_all_payments()

    user_rows = ""
    for u in users:
        user_rows += f"""
        <tr>
          <td>{h(u['id'])}</td>
          <td>{h(u['email'])}</td>
          <td>{h(u['tier'])}</td>
          <td>{h(u['subscription_status'])}</td>
          <td>{h(u['created_at'])}</td>
        </tr>
        """

    payment_rows = ""
    for p in payments:
        payment_rows += f"""
        <tr>
          <td>{h(p.get('email') or '-')}</td>
          <td>{h(p['provider'])}</td>
          <td>{h(p['payment_id'])}</td>
          <td>{h(p['amount'])}</td>
          <td>{h(p['status'])}</td>
          <td>{h(p['created_at'])}</td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="btns" style="justify-content:space-between;">
        <h1>Admin Panel</h1>
        <a class="btn btn-secondary" href="/admin/logout">Logout</a>
      </div>

      <div class="section">
        <h2 class="section-title">Users</h2>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>ID</th><th>Email</th><th>Tier</th><th>Status</th><th>Created</th></tr>
            {user_rows or "<tr><td colspan='5'>No users found.</td></tr>"}
          </table>
        </div>
      </div>

      <div class="section">
        <h2 class="section-title">Payments</h2>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Email</th><th>Provider</th><th>Payment ID</th><th>Amount</th><th>Status</th><th>Created</th></tr>
            {payment_rows or "<tr><td colspan='6'>No payments found.</td></tr>"}
          </table>
        </div>
      </div>
    </section>
    """
    return nav_layout(
        "Admin - AVA Markets",
        content,
        robots_content="noindex, nofollow"
    )


@app.errorhandler(404)
def not_found(e):
    return nav_layout(
        "404",
        "<section class='section'><div class='card'><h1>404</h1><p>Page not found.</p></div></section>",
        robots_content="noindex, nofollow"
    ), 404


@app.errorhandler(500)
def server_error(e):
    logger.exception("Unhandled server error")
    return nav_layout(
        "500",
        "<section class='section'><div class='card'><h1>500</h1><p>Internal server error.</p></div></section>",
        robots_content="noindex, nofollow"
    ), 500


_bg_started = False


def background_refresh_loop():
    while True:
        try:
            fetch_crypto_quotes_safe(force_refresh=True)
        except Exception as e:
            logger.warning(f"Background crypto refresh failed: {e}")

        try:
            fetch_stock_quotes_safe(force_refresh=True)
        except Exception as e:
            logger.warning(f"Background stock refresh failed: {e}")

        try:
            warm_detail_caches()
        except Exception as e:
            logger.warning(f"Background detail warm failed: {e}")

        try:
            refresh_signal_outcomes()
        except Exception as e:
            logger.warning(f"Background outcome refresh failed: {e}")

        try:
            db.purge_expired_rows()
        except Exception as e:
            logger.warning(f"Background cleanup failed: {e}")

        time.sleep(max(60, Config.BACKGROUND_REFRESH_SECONDS))


def start_background_refresh():
    global _bg_started
    if _bg_started:
        return
    if not Config.ENABLE_BACKGROUND_REFRESH or not Config.BACKGROUND_REFRESH_LEADER:
        logger.info("Background refresh disabled or not leader.")
        return

    t = threading.Thread(target=background_refresh_loop, daemon=True)
    t.start()
    _bg_started = True
    logger.info("Background refresh thread started.")


try:
    fetch_crypto_quotes_safe(force_refresh=True)
except Exception as e:
    logger.warning(f"Initial crypto cache warm failed: {e}")

try:
    fetch_stock_quotes_safe(force_refresh=True)
except Exception as e:
    logger.warning(f"Initial stock cache warm failed: {e}")

try:
    warm_detail_caches()
except Exception as e:
    logger.warning(f"Initial detail warm failed: {e}")

try:
    refresh_signal_outcomes()
except Exception as e:
    logger.warning(f"Initial outcome refresh failed: {e}")

try:
    db.purge_expired_rows()
except Exception as e:
    logger.warning(f"Initial cleanup failed: {e}")

# Start once in normal runtime.
if (not Config.DEBUG) or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    start_background_refresh()


if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
