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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - AVA MARKETS - %(levelname)s - %(message)s")
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
    DETAIL_WARM_CRYPTO = [s.strip().upper() for s in os.environ.get("DETAIL_WARM_CRYPTO", "BTC,ETH,SOL,XRP,DOGE,ADA,AVAX,LINK,PEPE,BONK").split(",") if s.strip()]
    DETAIL_WARM_STOCKS = [s.strip().upper() for s in os.environ.get("DETAIL_WARM_STOCKS", "AAPL,MSFT,NVDA,AMZN,GOOGL,TSLA,GC=F,CL=F").split(",") if s.strip()]
    RATE_LIMIT_STORAGE_URI = os.environ.get("RATE_LIMIT_STORAGE_URI", "memory://")

def validate_runtime_config():
    missing = [k for k in ["SECRET_KEY", "DOMAIN", "ADMIN_USERNAME", "ADMIN_PASSWORD"] if not getattr(Config, k, "")]
    if Config.STRIPE_SECRET_KEY and not Config.STRIPE_WEBHOOK_SECRET: missing.append("STRIPE_WEBHOOK_SECRET")
    if missing: raise RuntimeError(f"Missing config: {', '.join(missing)}")
    if not Config.DOMAIN.startswith(("https://", "http://")): raise RuntimeError("DOMAIN must include protocol")

validate_runtime_config()

if stripe and Config.STRIPE_SECRET_KEY: stripe.api_key = Config.STRIPE_SECRET_KEY

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config["SECRET_KEY"] = Config.SECRET_KEY

if Limiter: limiter = Limiter(key_func=get_remote_address, app=app, storage_uri=Config.RATE_LIMIT_STORAGE_URI, default_limits=["240 per hour", "20 per minute"])
else: 
    class _NoopLimiter: 
        def limit(self, *args, **kwargs): return lambda fn: fn
    limiter = _NoopLimiter()

CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
:root{--bg:#0b0f19;--bg2:#121826;--card:rgba(255,255,255,.05);--border:rgba(255,255,255,.09);--text:#f8fafc;--muted:#94a3b8;--blue:#2563eb;--blue2:#60a5fa;--green:#22c55e;--red:#ef4444;--yellow:#f59e0b;--shadow:0 24px 60px rgba(0,0,0,.35);}
*{box-sizing:border-box} html{scroll-behavior:smooth}
body{margin:0;font-family:'Inter',sans-serif;color:var(--text);background:radial-gradient(circle at top left, rgba(37,99,235,.16), transparent 28%),radial-gradient(circle at top right, rgba(96,165,250,.12), transparent 24%),linear-gradient(145deg,var(--bg),var(--bg2));}
a{text-decoration:none;color:inherit} .container{max-width:1240px;margin:0 auto;padding:0 24px}
.nav{display:flex;justify-content:space-between;align-items:center;padding:20px 0;position:sticky;top:0;z-index:20;background:rgba(11,15,25,.78);backdrop-filter:blur(14px);border-bottom:1px solid rgba(255,255,255,.04);}
.logo{font-size:1.3rem;font-weight:800;background:linear-gradient(90deg,#fff,var(--blue2),var(--blue));-webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.nav-links{display:flex;gap:16px;flex-wrap:wrap} .nav-links a{color:var(--muted);font-weight:600} .nav-links a:hover{color:var(--text)}
.hero{display:grid;grid-template-columns:1.05fr .95fr;gap:28px;align-items:center;padding:72px 0 48px}
.hero-card,.card,.table-shell,.price-card,.dashboard-card{background:rgba(255,255,255,.05);border:1px solid var(--border);border-radius:24px;box-shadow:var(--shadow);}
.hero-card{padding:36px} .card,.price-card,.dashboard-card{padding:22px}
.badge{display:inline-block;padding:8px 14px;border-radius:999px;background:rgba(37,99,235,.14);border:1px solid rgba(96,165,250,.24);color:#bfdbfe;font-size:.88rem;font-weight:700;margin-bottom:18px;}
h1{font-size:clamp(2.4rem,5vw,4.4rem);line-height:1.02;margin:0 0 18px} h2{margin:0 0 14px} .section{padding:30px 0 72px} .section-title{font-size:2rem;margin:0 0 14px}
p{color:var(--muted);line-height:1.7;font-size:1.02rem} .section-sub{max-width:820px;margin:0 0 24px;color:var(--muted)}
.btns{display:flex;gap:14px;flex-wrap:wrap;margin-top:20px}
.btn{display:inline-flex;align-items:center;justify-content:center;padding:14px 18px;border-radius:14px;font-weight:700;border:1px solid transparent;cursor:pointer;}
.btn-primary{background:linear-gradient(90deg,var(--blue2),var(--blue));color:#fff} .btn-secondary{background:rgba(255,255,255,.05);border-color:rgba(255,255,255,.10);color:var(--text)}
.market-grid,.dashboard-grid{display:grid;gap:18px} .market-grid{grid-template-columns:repeat(auto-fit,minmax(220px,1fr))} .dashboard-grid{grid-template-columns:repeat(auto-fit,minmax(260px,1fr))}
.table-shell{overflow:hidden} .market-table{width:100%;border-collapse:collapse} .market-table th,.market-table td{padding:16px 14px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;vertical-align:top}
.market-table th{color:#cbd5e1;background:rgba(255,255,255,.02);font-size:.92rem}
.asset-name strong{display:block} .asset-name span{display:block;color:var(--muted);font-size:.85rem;margin-top:4px}
.up{color:var(--green)} .down{color:var(--red)}
.signal{display:inline-flex;padding:8px 12px;border-radius:999px;font-weight:700;font-size:.82rem}
.signal-buy{background:rgba(34,197,94,.14);color:#86efac} .signal-hold{background:rgba(245,158,11,.14);color:#fde68a} .signal-sell{background:rgba(239,68,68,.14);color:#fca5a5} .signal-locked{background:rgba(255,255,255,.08);color:#cbd5e1}
.candle-box{background:linear-gradient(180deg,rgba(255,255,255,.03),rgba(255,255,255,.01));border:1px solid rgba(255,255,255,.06);border-radius:18px;padding:16px;}
.candles{height:170px;display:flex;align-items:flex-end;gap:6px} .candle{flex:1;position:relative;height:140px}
.wick{position:absolute;left:50%;transform:translateX(-50%);width:2px;background:#cbd5e1;border-radius:999px} .body{position:absolute;left:50%;transform:translateX(-50%);width:8px;border-radius:4px}
.body.green{background:linear-gradient(180deg,#34d399,#16a34a)} .body.red{background:linear-gradient(180deg,#f87171,#dc2626)}
.form-shell{display:flex;justify-content:center;align-items:center;min-height:70vh} .form-card{width:100%;max-width:460px;background:rgba(255,255,255,.05);border:1px solid var(--border);border-radius:24px;box-shadow:var(--shadow);padding:30px;}
.form-card input{width:100%;padding:14px 16px;margin:10px 0;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:var(--text);outline:none;}
.form-card button{width:100%;padding:14px 18px;margin-top:10px;border:none;border-radius:14px;background:linear-gradient(90deg,var(--blue2),var(--blue));color:#fff;font-weight:700;cursor:pointer;}
.error{color:#fca5a5;margin-top:10px} .key{background:#0f172a;padding:12px;border-radius:12px;word-break:break-all;font-family:monospace;font-size:13px}
.tier{display:inline-flex;padding:8px 12px;border-radius:999px;background:rgba(37,99,235,.14);color:#bfdbfe;font-weight:700}
.footer{padding:30px 0 60px;color:var(--muted);text-align:center}
.asset-row{display:flex;align-items:center;gap:10px;} .asset-logo{width:24px;height:24px;border-radius:50%;object-fit:cover;background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.08);flex-shrink:0;}
.asset-icon{width:24px;height:24px;display:inline-flex;align-items:center;justify-content:center;font-size:1rem;flex-shrink:0;}
.pagination{display:flex;gap:10px;flex-wrap:wrap;margin-top:22px;align-items:center} .page-link{padding:10px 14px;border-radius:12px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);color:var(--text);font-weight:700}
.search-box{width:100%;max-width:420px;padding:14px 16px;border-radius:14px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:var(--text);outline:none;}
@media (max-width: 900px){ .hero{grid-template-columns:1fr} .nav{flex-direction:column;gap:14px} .nav-links{justify-content:center} }
"""

CRYPTO_TOP_90 = [
    ("BTC", "Bitcoin"), ("ETH", "Ethereum"), ("BNB", "BNB"), ("SOL", "Solana"), ("XRP", "XRP"), ("DOGE", "Dogecoin"), ("ADA", "Cardano"), ("AVAX", "Avalanche"), ("LINK", "Chainlink"), ("DOT", "Polkadot"),
    ("MATIC", "Polygon"), ("LTC", "Litecoin"), ("BCH", "Bitcoin Cash"), ("ATOM", "Cosmos"), ("UNI", "Uniswap"), ("NEAR", "NEAR Protocol"), ("APT", "Aptos"), ("ARB", "Arbitrum"), ("OP", "Optimism"), ("SUI", "Sui"),
    ("PEPE", "Pepe"), ("SHIB", "Shiba Inu"), ("TRX", "TRON"), ("ETC", "Ethereum Classic"), ("XLM", "Stellar"), ("HBAR", "Hedera"), ("ICP", "Internet Computer"), ("FIL", "Filecoin"), ("INJ", "Injective"), ("RNDR", "Render"),
    ("TAO", "Bittensor"), ("IMX", "Immutable"), ("SEI", "Sei"), ("TIA", "Celestia"), ("JUP", "Jupiter"), ("PYTH", "Pyth Network"), ("BONK", "Bonk"), ("WIF", "dogwifhat"), ("FET", "Fetch.ai"), ("RUNE", "THORChain"),
    ("AAVE", "Aave"), ("MKR", "Maker"), ("ALGO", "Algorand"), ("VET", "VeChain"), ("EGLD", "MultiversX"), ("THETA", "Theta Network"), ("SAND", "The Sandbox"), ("MANA", "Decentraland"), ("AXS", "Axie Infinity"),
    ("GRT", "The Graph"), ("FLOW", "Flow"), ("KAS", "Kaspa"), ("KAVA", "Kava"), ("DYDX", "dYdX"), ("WLD", "Worldcoin"), ("ARKM", "Arkham"), ("STRK", "Starknet"), ("ENA", "Ethena"), ("ONDO", "Ondo"),
    ("JASMY", "JasmyCoin"), ("LDO", "Lido DAO"), ("CRV", "Curve DAO Token"), ("SNX", "Synthetix"), ("COMP", "Compound"), ("1INCH", "1inch"), ("BAT", "Basic Attention Token"), ("ZEC", "Zcash"), ("DASH", "Dash"),
    ("CHZ", "Chiliz"), ("ROSE", "Oasis"), ("QTUM", "Qtum"), ("IOTA", "IOTA"), ("ZIL", "Zilliqa"), ("KSM", "Kusama"), ("GMT", "STEPN"), ("BLUR", "Blur"), ("ACE", "Fusionist"), ("NEO", "NEO"), ("CFX", "Conflux"),
    ("FTM", "Fantom"), ("GALA", "Gala"), ("LRC", "Loopring"), ("ENS", "Ethereum Name Service"), ("SXP", "Solar"), ("HOT", "Holo"), ("ANKR", "Ankr"), ("ICX", "ICON"), ("SC", "Siacoin"), ("CKB", "Nervos Network"),
    ("MASK", "Mask Network"), ("YFI", "yearn.finance"), ("WOO", "WOO"), ("SKL", "SKALE"),
]
CRYPTO_NAME_MAP = {s: n for s, n in CRYPTO_TOP_90}

STOCK_UNIVERSE = [
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"), ("AMZN", "Amazon"), ("GOOGL", "Alphabet"), ("META", "Meta"), ("TSLA", "Tesla"), ("BRK-B", "Berkshire Hathaway"),
    ("JPM", "JPMorgan"), ("V", "Visa"), ("MA", "Mastercard"), ("UNH", "UnitedHealth"), ("XOM", "Exxon Mobil"), ("LLY", "Eli Lilly"), ("AVGO", "Broadcom"), ("ORCL", "Oracle"),
    ("COST", "Costco"), ("PG", "Procter & Gamble"), ("HD", "Home Depot"), ("NFLX", "Netflix"), ("ABBV", "AbbVie"), ("KO", "Coca-Cola"), ("PEP", "PepsiCo"), ("MRK", "Merck"),
    ("BAC", "Bank of America"), ("WMT", "Walmart"), ("CVX", "Chevron"), ("AMD", "AMD"), ("ADBE", "Adobe"), ("CRM", "Salesforce"), ("ASML", "ASML"), ("TSM", "Taiwan Semiconductor"),
    ("NVO", "Novo Nordisk"), ("SAP", "SAP"), ("SONY", "Sony"), ("TM", "Toyota"), ("BABA", "Alibaba"), ("PDD", "PDD"), ("SHEL", "Shell"), ("BP", "BP"), ("SHOP", "Shopify"),
    ("MELI", "MercadoLibre"), ("IBM", "IBM"), ("INTC", "Intel"), ("QCOM", "Qualcomm"),
    ("GC=F", "Gold Futures"), ("SI=F", "Silver Futures"), ("PL=F", "Platinum Futures"), ("CL=F", "Oil Futures"), ("SIG", "Diamonds Proxy"),
]
STOCK_NAME_MAP = {s: n for s, n in STOCK_UNIVERSE}
STOCK_DOMAINS = {"AAPL": "apple.com", "MSFT": "microsoft.com", "NVDA": "nvidia.com", "AMZN": "amazon.com", "GOOGL": "google.com", "META": "meta.com", "TSLA": "tesla.com", "GC=F": None, "CL=F": None}

def h(v): return html.escape("" if v is None else str(v), quote=True)
def get_stock_logo(sym): return f"https://logo.clearbit.com/{STOCK_DOMAINS.get(sym.upper())}" if STOCK_DOMAINS.get(sym.upper()) else ""
def get_crypto_logo(sym): return f"https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/128/color/{sym.lower()}.png"
def get_asset_icon(sym): return {"GC=F": "🥇", "SI=F": "🥈", "PL=F": "🔘", "CL=F": "🛢️", "SIG": "💎"}.get(sym.upper(), "📈")
def pct_change(a, b): return 0.0 if b in [0, None] else ((a - b) / b) * 100.0
def fmt_price(v, s=None): return f"€{v:,.2f}" if s == "ASML" else f"${v:,.2f}" if v >= 1 else f"${v:.4f}" if v >= 0.01 else f"${v:.8f}"
def fmt_change(v): return f"{v:+.2f}%"

class Database:
    def __init__(self, path): self.path, self.init() = path, None
    def conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
        c.execute("PRAGMA journal_mode=WAL;")
        return c
    def init(self):
        c = self.conn()
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, api_key TEXT UNIQUE NOT NULL, tier TEXT NOT NULL DEFAULT 'free', stripe_customer_id TEXT, stripe_subscription_id TEXT, subscription_status TEXT NOT NULL DEFAULT 'inactive', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        CREATE TABLE IF NOT EXISTS sessions (token TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TIMESTAMP, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE);
        CREATE TABLE IF NOT EXISTS market_cache (cache_key TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at INTEGER NOT NULL);
        """)
        c.commit(); c.close()
    def cache_get(self, key, ttl):
        c = self.conn()
        row = c.execute("SELECT payload_json, updated_at FROM market_cache WHERE cache_key = ?", (key,)).fetchone()
        c.close()
        if row and (int(time.time()) - int(row["updated_at"]) <= ttl): return json.loads(row["payload_json"])
        return None
    def cache_get_stale(self, key):
        c = self.conn(); row = c.execute("SELECT payload_json FROM market_cache WHERE cache_key = ?", (key,)).fetchone(); c.close()
        return json.loads(row["payload_json"]) if row else None
    def cache_set(self, key, payload):
        c = self.conn()
        c.execute("INSERT INTO market_cache (cache_key, payload_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(cache_key) DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at", (key, json.dumps(payload), int(time.time())))
        c.commit(); c.close()
    def get_user_by_session(self, token):
        if not token: return None
        c = self.conn(); row = c.execute("SELECT u.* FROM users u JOIN sessions s ON s.user_id = u.id WHERE s.token = ?", (token,)).fetchone(); c.close()
        return dict(row) if row else None

db = Database(Config.DATABASE)
MEM_CACHE = {}

def get_cached_payload(key, ttl):
    mem = MEM_CACHE.get(key)
    if mem and (int(time.time()) - mem["updated_at"] <= ttl): return mem["data"]
    db_payload = db.cache_get(key, ttl)
    if db_payload: MEM_CACHE[key] = {"data": db_payload, "updated_at": int(time.time())}
    return db_payload

def set_cached_payload(key, payload):
    now = int(time.time())
    MEM_CACHE[key] = {"data": payload, "updated_at": now}
    db.cache_set(key, payload)

def compute_light_signal(change): return "BUY" if change >= 2.0 else "SELL" if change <= -2.0 else "HOLD"

# ==========================================
# ROBUST API FETCHERS (NO GEOBLOCK, NO 429)
# ==========================================

def fetch_crypto_quotes_safe(force_refresh=False):
    cache_key = "crypto_list"
    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.CRYPTO_CACHE_TTL)
        if cached is not None: return cached

    try:
        # KUCOIN API - 1 request for ALL markets, NO geo-blocks, NO api keys needed.
        r = requests.get("https://api.kucoin.com/api/v1/market/allTickers", timeout=15)
        r.raise_for_status()
        data = r.json()
        market_data = data.get("data", {}).get("ticker", [])

        market_map = {}
        for item in market_data:
            sym = item.get("symbol", "")
            if sym.endswith("-USDT"):
                market_map[sym.split("-")[0]] = item

        results = []
        for symbol, name in CRYPTO_TOP_90:
            item = market_map.get(symbol)
            if not item: continue
            price = float(item.get("last", 0))
            change = float(item.get("changeRate", 0)) * 100.0  # Kucoin returns 0.05 for 5%
            
            if price > 0:
                results.append({
                    "symbol": symbol, "name": name, "price": price, "change": change,
                    "dir": "up" if change >= 0 else "down", "signal": compute_light_signal(change),
                    "logo": get_crypto_logo(symbol), "icon": "₿"
                })

        if results:
            set_cached_payload(cache_key, results)
            return results
    except Exception as e:
        logger.warning(f"Kucoin crypto fetch failed: {e}")

    return db.cache_get_stale(cache_key) or []


def fetch_stock_quotes_safe(force_refresh=False):
    cache_key = "stock_list"
    if not force_refresh:
        cached = get_cached_payload(cache_key, Config.STOCK_CACHE_TTL)
        if cached is not None: return cached

    symbols = [s for s, _ in STOCK_UNIVERSE]
    symbols_str = " ".join(symbols)
    results = []
    
    try:
        # THREADS=FALSE avoids Rate Limit 429 Errors on Render
        data = yf.download(symbols_str, period="5d", interval="1d", group_by='ticker', threads=False, progress=False)
        for symbol, name in STOCK_UNIVERSE:
            try:
                df = data[symbol] if len(symbols) > 1 else data
                series = df['Close'].ffill().dropna()
                if len(series) < 1: continue
                last_close = float(series.iloc[-1])
                prev_close = float(series.iloc[-2]) if len(series) > 1 else last_close
                change = pct_change(last_close, prev_close)
                
                results.append({
                    "symbol": symbol, "name": name, "price": last_close, "change": change,
                    "dir": "up" if change >= 0 else "down", "signal": compute_light_signal(change),
                    "logo": get_stock_logo(symbol), "icon": get_asset_icon(symbol)
                })
            except Exception:
                continue

        if results:
            set_cached_payload(cache_key, results)
            return results
    except Exception as e:
        logger.warning(f"Bulk yfinance stock fetch failed: {e}")

    return db.cache_get_stale(cache_key) or []


# ==========================================
# FRONTEND TEMPLATING & ROUTES
# ==========================================

def nav_layout(title, content, extra_head=""):
    return render_template_string("""
    <!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><title>{{ title }}</title><style>{{ css }}</style>{{ extra_head|safe }}</head>
    <body>
      <div class="container">
        <nav class="nav">
          <div class="logo"><a href="/">AVA Markets</a></div>
          <div class="nav-links"><a href="/">Home</a><a href="/crypto">Crypto</a><a href="/stocks">Stocks</a></div>
        </nav>
        {{ content|safe }}
      </div>
    </body></html>
    """, title=title, content=content, css=CSS, extra_head=extra_head)

def live_update_script(page_type):
    return f"""<script>
    setInterval(async () => {{
        try {{
            const res = await fetch('/api/live/{page_type}-list'); const data = await res.json();
            document.getElementById('live-updated').textContent = 'Last updated: ' + data.updated_at + ' UTC';
            data.items.forEach(item => {{
                let safe_id = item.symbol.replace(/[^A-Za-z0-9]/g, '_');
                let p = document.getElementById('price-'+safe_id); if(p) p.textContent = item.price_display;
                let c = document.getElementById('change-'+safe_id); if(c) {{c.textContent = item.change_display; c.className = item.dir;}}
                let s = document.getElementById('signal-'+safe_id); if(s) {{s.textContent = item.signal; s.className = 'signal signal-' + item.signal.toLowerCase();}}
            }});
        }} catch(e){{}}
    }}, 30000);</script>"""

@app.route("/api/live/crypto-list")
def api_live_crypto():
    items = [{"symbol": a["symbol"], "price_display": fmt_price(a["price"], a["symbol"]), "change_display": fmt_change(a["change"]), "dir": a["dir"], "signal": a["signal"]} for a in fetch_crypto_quotes_safe()]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})

@app.route("/api/live/stocks-list")
def api_live_stocks():
    items = [{"symbol": a["symbol"], "price_display": fmt_price(a["price"], a["symbol"]), "change_display": fmt_change(a["change"]), "dir": a["dir"], "signal": a["signal"]} for a in fetch_stock_quotes_safe()]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})

@app.route("/")
def home():
    cl = fetch_crypto_quotes_safe()
    fc = cl[0] if cl else {"symbol": "N/A", "price": 0.0, "change": 0.0, "dir": "down"}
    content = f"""<section class="hero"><div class="hero-card"><h1>Market Intelligence</h1><div class="btns"><a class="btn btn-primary" href="/crypto">Explore Crypto</a><a class="btn btn-secondary" href="/stocks">Explore Stocks</a></div></div></section>"""
    return nav_layout("AVA Markets", content)

@app.route("/crypto")
def crypto():
    assets = fetch_crypto_quotes_safe()
    rows = "".join(f'<tr><td><strong class="asset-row"><img class="asset-logo" src="{h(a["logo"])}" onerror="this.outerHTML=`<span class=\\\'asset-icon\\\'>₿</span>`">{h(a["symbol"])}</strong></td><td id="price-{h(a["symbol"])}">{fmt_price(a["price"])}</td><td id="change-{h(a["symbol"])}" class="{a["dir"]}">{fmt_change(a["change"])}</td><td><span id="signal-{h(a["symbol"])}" class="signal signal-{a["signal"].lower()}">{a["signal"]}</span></td></tr>' for a in assets[:30])
    content = f'<section class="section"><h1>Crypto</h1><div id="live-updated" class="live-stamp">Loading live data...</div><div class="table-shell"><table class="market-table"><tr><th>Asset</th><th>Price</th><th>24h</th><th>Signal</th></tr>{rows}</table></div></section>'
    return nav_layout("Crypto - AVA", content, extra_head=live_update_script("crypto"))

@app.route("/stocks")
def stocks():
    assets = fetch_stock_quotes_safe()
    rows = "".join(f'<tr><td><strong class="asset-row"><img class="asset-logo" src="{h(a.get("logo",""))}" onerror="this.outerHTML=`<span class=\\\'asset-icon\\\'>{h(a["icon"])}</span>`">{h(a["symbol"])}</strong></td><td id="price-{h(a["symbol"].replace("=","_"))}">{fmt_price(a["price"])}</td><td id="change-{h(a["symbol"].replace("=","_"))}" class="{a["dir"]}">{fmt_change(a["change"])}</td><td><span id="signal-{h(a["symbol"].replace("=","_"))}" class="signal signal-{a["signal"].lower()}">{a["signal"]}</span></td></tr>' for a in assets)
    content = f'<section class="section"><h1>Stocks & Commodities</h1><div id="live-updated" class="live-stamp">Loading live data...</div><div class="table-shell"><table class="market-table"><tr><th>Asset</th><th>Price</th><th>1D</th><th>Signal</th></tr>{rows}</table></div></section>'
    return nav_layout("Stocks - AVA", content, extra_head=live_update_script("stocks"))

# ==========================================
# BACKGROUND BOOTER (PREVENTS 502 ERRORS)
# ==========================================
_bg_started = False
def start_background_refresh():
    global _bg_started
    if _bg_started: return
    def loop():
        try: fetch_crypto_quotes_safe(force_refresh=True)
        except: pass
        try: fetch_stock_quotes_safe(force_refresh=True)
        except: pass
        while True:
            time.sleep(120)
            try: fetch_crypto_quotes_safe(force_refresh=True)
            except: pass
            try: fetch_stock_quotes_safe(force_refresh=True)
            except: pass
    threading.Thread(target=loop, daemon=True).start()
    _bg_started = True

if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not Config.DEBUG:
    start_background_refresh()

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
