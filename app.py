#!/usr/bin/env python3
import os
import json
import time
import html
import sqlite3
import logging
import threading
import requests

from datetime import datetime
from functools import wraps
from urllib.parse import quote_plus

from dotenv import load_dotenv
from flask import Flask, request, redirect, make_response, render_template_string, jsonify
from werkzeug.middleware.proxy_fix import ProxyFix

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
    SECRET_KEY = os.environ.get("SECRET_KEY", "fallback_secret_key").strip()
    DOMAIN = os.environ.get("DOMAIN", "").strip().rstrip("/")
    CRYPTO_CACHE_TTL = 300
    STOCK_CACHE_TTL = 600
    PAGE_SIZE_CRYPTO = 100
    PAGE_SIZE_STOCKS = 100
    RATE_LIMIT_STORAGE_URI = os.environ.get("RATE_LIMIT_STORAGE_URI", "memory://")

app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config["SECRET_KEY"] = Config.SECRET_KEY

if Limiter: 
    limiter = Limiter(key_func=get_remote_address, app=app, storage_uri=Config.RATE_LIMIT_STORAGE_URI, default_limits=["240 per hour"])
else: 
    class _NoopLimiter: 
        def limit(self, *args, **kwargs): return lambda fn: fn
    limiter = _NoopLimiter()

CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
:root{--bg:#0b0f19;--bg2:#121826;--card:rgba(255,255,255,.05);--border:rgba(255,255,255,.09);--text:#f8fafc;--muted:#94a3b8;--blue:#2563eb;--blue2:#60a5fa;--green:#22c55e;--red:#ef4444;--shadow:0 24px 60px rgba(0,0,0,.35);}
*{box-sizing:border-box} html{scroll-behavior:smooth}
body{margin:0;font-family:'Inter',sans-serif;color:var(--text);background:radial-gradient(circle at top left, rgba(37,99,235,.16), transparent 28%),radial-gradient(circle at top right, rgba(96,165,250,.12), transparent 24%),linear-gradient(145deg,var(--bg),var(--bg2));}
a{text-decoration:none;color:inherit} .container{max-width:1240px;margin:0 auto;padding:0 24px}
.nav{display:flex;justify-content:space-between;align-items:center;padding:20px 0;position:sticky;top:0;z-index:20;background:rgba(11,15,25,.78);backdrop-filter:blur(14px);border-bottom:1px solid rgba(255,255,255,.04);}
.logo{font-size:1.3rem;font-weight:800;background:linear-gradient(90deg,#fff,var(--blue2),var(--blue));-webkit-background-clip:text;-webkit-text-fill-color:transparent;}
.nav-links{display:flex;gap:16px;flex-wrap:wrap} .nav-links a{color:var(--muted);font-weight:600} .nav-links a:hover{color:var(--text)}
.hero{display:grid;grid-template-columns:1.05fr .95fr;gap:28px;align-items:center;padding:72px 0 48px}
.hero-card,.card,.table-shell{background:rgba(255,255,255,.05);border:1px solid var(--border);border-radius:24px;box-shadow:var(--shadow);}
.hero-card{padding:36px} .card{padding:22px}
.badge{display:inline-block;padding:8px 14px;border-radius:999px;background:rgba(37,99,235,.14);border:1px solid rgba(96,165,250,.24);color:#bfdbfe;font-size:.88rem;font-weight:700;margin-bottom:18px;}
h1{font-size:clamp(2.4rem,5vw,4.4rem);line-height:1.02;margin:0 0 18px} h2{margin:0 0 14px} .section{padding:30px 0 72px}
p{color:var(--muted);line-height:1.7;font-size:1.02rem}
.btns{display:flex;gap:14px;flex-wrap:wrap;margin-top:20px}
.btn{display:inline-flex;align-items:center;justify-content:center;padding:14px 18px;border-radius:14px;font-weight:700;border:1px solid transparent;cursor:pointer;}
.btn-primary{background:linear-gradient(90deg,var(--blue2),var(--blue));color:#fff} .btn-secondary{background:rgba(255,255,255,.05);border-color:rgba(255,255,255,.10);color:var(--text)}
.table-shell{overflow:hidden} .market-table{width:100%;border-collapse:collapse} .market-table th,.market-table td{padding:16px 14px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left;}
.market-table th{color:#cbd5e1;background:rgba(255,255,255,.02);font-size:.92rem}
.asset-name strong{display:block} .asset-name span{display:block;color:var(--muted);font-size:.85rem;margin-top:4px}
.up{color:var(--green)} .down{color:var(--red)}
.signal{display:inline-flex;padding:8px 12px;border-radius:999px;font-weight:700;font-size:.82rem}
.signal-buy{background:rgba(34,197,94,.14);color:#86efac} .signal-hold{background:rgba(245,158,11,.14);color:#fde68a} .signal-sell{background:rgba(239,68,68,.14);color:#fca5a5}
.asset-row{display:flex;align-items:center;gap:10px;} .asset-logo{width:24px;height:24px;border-radius:50%;object-fit:cover;background:#fff;}
.asset-icon{width:24px;height:24px;display:inline-flex;align-items:center;justify-content:center;font-size:1rem;}
.pagination{display:flex;gap:10px;margin-top:22px;} .page-link{padding:10px 14px;border-radius:12px;background:rgba(255,255,255,.05);border:1px solid rgba(255,255,255,.08);color:var(--text);font-weight:700}
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

STOCK_DOMAINS = {
    "AAPL": "apple.com", "MSFT": "microsoft.com", "NVDA": "nvidia.com", "AMZN": "amazon.com", "GOOGL": "google.com", "META": "meta.com", "TSLA": "tesla.com", "BRK-B": "berkshirehathaway.com",
    "JPM": "jpmorganchase.com", "V": "visa.com", "MA": "mastercard.com", "UNH": "uhc.com", "XOM": "exxonmobil.com", "LLY": "lilly.com", "AVGO": "broadcom.com", "ORCL": "oracle.com",
    "COST": "costco.com", "PG": "pg.com", "HD": "homedepot.com", "NFLX": "netflix.com", "ABBV": "abbvie.com", "KO": "coca-colacompany.com", "PEP": "pepsico.com", "MRK": "merck.com",
    "BAC": "bankofamerica.com", "WMT": "walmart.com", "CVX": "chevron.com", "AMD": "amd.com", "ADBE": "adobe.com", "CRM": "salesforce.com", "ASML": "asml.com", "TSM": "tsmc.com",
    "NVO": "novonordisk.com", "SAP": "sap.com", "SONY": "sony.com", "TM": "toyota.com", "BABA": "alibaba.com", "PDD": "pddholdings.com", "SHEL": "shell.com", "BP": "bp.com",
    "SHOP": "shopify.com", "MELI": "mercadolibre.com", "IBM": "ibm.com", "INTC": "intel.com", "QCOM": "qualcomm.com",
}

def h(v): return html.escape("" if v is None else str(v), quote=True)

def get_stock_logo(sym): 
    domain = STOCK_DOMAINS.get(str(sym).upper())
    return f"https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL&url=http://{domain}&size=128" if domain else ""

def get_crypto_logo(sym): return f"https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/128/color/{str(sym).lower()}.png"
def get_asset_icon(sym): return {"GC=F": "🥇", "SI=F": "🥈", "PL=F": "🔘", "CL=F": "🛢️", "SIG": "💎"}.get(str(sym).upper(), "📈")
def fmt_price(v): 
    try:
        vf = float(v)
        return f"${vf:,.2f}" if vf >= 1 else f"${vf:.4f}" if vf >= 0.01 else f"${vf:.8f}"
    except: return "$0.00"
def fmt_change(v): 
    try: return f"{float(v):+.2f}%"
    except: return "+0.00%"

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
        CREATE TABLE IF NOT EXISTS market_cache (cache_key TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at INTEGER NOT NULL);
        """)
        c.commit(); c.close()
    def cache_get(self, key, ttl):
        c = self.conn()
        row = c.execute("SELECT payload_json, updated_at FROM market_cache WHERE cache_key = ?", (key,)).fetchone()
        c.close()
        if row and (int(time.time()) - int(row["updated_at"]) <= ttl): 
            try: return json.loads(row["payload_json"])
            except: return None
        return None
    def cache_get_stale(self, key):
        c = self.conn(); row = c.execute("SELECT payload_json FROM market_cache WHERE cache_key = ?", (key,)).fetchone(); c.close()
        if row:
            try: return json.loads(row["payload_json"])
            except: return None
        return None
    def cache_set(self, key, payload):
        c = self.conn()
        c.execute("INSERT INTO market_cache (cache_key, payload_json, updated_at) VALUES (?, ?, ?) ON CONFLICT(cache_key) DO UPDATE SET payload_json = excluded.payload_json, updated_at = excluded.updated_at", (key, json.dumps(payload), int(time.time())))
        c.commit(); c.close()

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

def compute_light_signal(change): 
    try:
        c = float(change)
        return "BUY" if c >= 2.0 else "SELL" if c <= -2.0 else "HOLD"
    except: return "HOLD"

    # ==========================================
# NON-BLOCKING API FETCHERS (BACKGROUND ONLY)
# ==========================================

def _perform_crypto_fetch():
    results = []
    try:
        # PRIMARY API: KUCOIN (Extremely reliable, no DNS issues on Render)
        r = requests.get("https://api.kucoin.com/api/v1/market/allTickers", timeout=15)
        if r.status_code == 200:
            market_data = r.json().get("data", {}).get("ticker", [])
            market_map = {}
            for item in market_data:
                sym = str(item.get("symbol", ""))
                if sym.endswith("-USDT"):
                    market_map[sym.split("-")[0]] = item

            for symbol, name in CRYPTO_TOP_90:
                item = market_map.get(symbol)
                if not item: continue
                
                try:
                    price = float(item.get("last", 0))
                    change = float(item.get("changeRate", 0)) * 100.0
                except:
                    continue
                
                if price > 0:
                    results.append({
                        "symbol": symbol, "name": name, "price": price, "change": change,
                        "dir": "up" if change >= 0 else "down", "signal": compute_light_signal(change),
                        "logo": get_crypto_logo(symbol), "icon": "₿"
                    })
    except Exception as e:
        logger.error(f"KuCoin fetch failed: {e}")

    # FALLBACK API: MEXC (Kicks in instantly if KuCoin fails)
    if not results:
        try:
            logger.info("Using MEXC fallback for Crypto...")
            r = requests.get("https://api.mexc.com/api/v3/ticker/24hr", timeout=15)
            if r.status_code == 200:
                market_data = r.json()
                market_map = {str(item.get("symbol", "")).replace("USDT", ""): item for item in market_data if str(item.get("symbol", "")).endswith("USDT")}
                
                for symbol, name in CRYPTO_TOP_90:
                    item = market_map.get(symbol)
                    if not item: continue
                    
                    try:
                        price = float(item.get("lastPrice", 0))
                        change = float(item.get("priceChangePercent", 0)) * 100.0 # MEXC returns raw decimal
                    except:
                        continue
                    
                    if price > 0:
                        results.append({
                            "symbol": symbol, "name": name, "price": price, "change": change,
                            "dir": "up" if change >= 0 else "down", "signal": compute_light_signal(change),
                            "logo": get_crypto_logo(symbol), "icon": "₿"
                        })
        except Exception as e:
            logger.error(f"MEXC fallback failed: {e}")

    if results: 
        set_cached_payload("crypto_list", results)


def _perform_stock_fetch():
    # Direct Yahoo Finance QUOTE API 
    # Fetches all 50 assets in 1 single call. Bypasses rate limits and weekend missing data.
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}
    
    try:
        symbols_str = ",".join([s for s, _ in STOCK_UNIVERSE])
        url = f"https://query2.finance.yahoo.com/v7/finance/quote?symbols={symbols_str}"
        
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200:
            data = r.json().get("quoteResponse", {}).get("result", [])
            
            # Map the results
            res_map = {}
            for item in data:
                sym = str(item.get("symbol", ""))
                try:
                    price = float(item.get("regularMarketPrice", 0))
                    change = float(item.get("regularMarketChangePercent", 0)) # Official 24h change
                except:
                    continue
                
                if price > 0:
                    res_map[sym] = {
                        "symbol": sym, "name": STOCK_NAME_MAP.get(sym, sym), "price": price, "change": change,
                        "dir": "up" if change >= 0 else "down", "signal": compute_light_signal(change),
                        "logo": get_stock_logo(sym), "icon": get_asset_icon(sym)
                    }
                    
            # Ensure the output matches our exact defined order
            results = []
            for symbol, _ in STOCK_UNIVERSE:
                if symbol in res_map:
                    results.append(res_map[symbol])

            if results: 
                set_cached_payload("stock_list", results)
                
    except Exception as e:
        logger.error(f"Background Stock fetch failed: {e}")


# Routes NEVER block. They just read the local SQLite cache instantly.
def fetch_crypto_quotes_safe():
    return get_cached_payload("crypto_list", Config.CRYPTO_CACHE_TTL) or db.cache_get_stale("crypto_list") or []

def fetch_stock_quotes_safe():
    return get_cached_payload("stock_list", Config.STOCK_CACHE_TTL) or db.cache_get_stale("stock_list") or []


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def paginate(items, page, per_page):
    if not items: return [], 0, 1, 1
    total = len(items)
    pages = max(1, math.ceil(total / per_page)) if total > 0 else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    return items[start:start + per_page], total, pages, page

def legal_disclaimer_html():
    return """
    <div class="card" style="margin-top:24px;">
      <h3>Disclaimer</h3>
      <p>AVA Markets provides technical market intelligence and indicator-based analysis for educational purposes only. It is not financial advice.</p>
    </div>
    """

# ==========================================
# FRONTEND TEMPLATING & ROUTES
# ==========================================

def nav_layout(title, content, extra_head=""):
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>{{ title }}</title>
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
          </div>
        </nav>
        {{ content|safe }}
        <div class="footer">AVA Markets © 2026 — Crypto, Stocks, and Commodities.</div>
      </div>
    </body>
    </html>
    """, title=title, content=content, css=CSS, extra_head=extra_head)

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
                let p = document.getElementById('price-'+safe_id); if(p) p.textContent = item.price_display;
                let c = document.getElementById('change-'+safe_id); if(c) {{c.textContent = item.change_display; c.className = item.dir;}}
                let s = document.getElementById('signal-'+safe_id); if(s) {{s.textContent = item.signal; s.className = 'signal signal-' + item.signal.toLowerCase();}}
            }});
        }} catch(e){{}}
    }}, 15000);
    </script>"""

@app.route("/api/live/crypto-list")
def api_live_crypto():
    assets = fetch_crypto_quotes_safe()
    items = [{"symbol": a.get("symbol",""), "price_display": fmt_price(a.get("price",0)), "change_display": fmt_change(a.get("change",0)), "dir": a.get("dir","down"), "signal": a.get("signal","HOLD")} for a in assets]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})

@app.route("/api/live/stocks-list")
def api_live_stocks():
    assets = fetch_stock_quotes_safe()
    items = [{"symbol": a.get("symbol",""), "price_display": fmt_price(a.get("price",0)), "change_display": fmt_change(a.get("change",0)), "dir": a.get("dir","down"), "signal": a.get("signal","HOLD")} for a in assets]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})

@app.route("/")
def home():
    cl = fetch_crypto_quotes_safe()
    fc = cl[0] if cl else {"symbol": "Data Loading...", "price": 0.0, "change": 0.0, "dir": "down", "signal": "HOLD"}
    
    content = f"""
    <section class="hero">
      <div class="hero-card">
        <div class="badge">AVA Markets Core</div>
        <h1>Market Intelligence that never sleeps.</h1>
        <p>Live-updating tracking for Crypto, Stocks, Gold, and Oil.</p>
        <div class="btns">
          <a class="btn btn-primary" href="/crypto">Explore Crypto</a>
          <a class="btn btn-secondary" href="/stocks">Explore Stocks</a>
        </div>
      </div>
      <div class="card featured-shell">
        <div class="badge">Market Status</div>
        <h2>Data Engine is Online</h2>
        <p>Prices update securely in the background without slowing down the website.</p>
      </div>
    </section>
    """
    return nav_layout("AVA Markets - Intelligence", content)

@app.route("/crypto")
def crypto():
    try:
        page = int(request.args.get("page", 1))
    except:
        page = 1
        
    search = (request.args.get("q") or "").strip().lower()
    assets = fetch_crypto_quotes_safe()
    
    if search:
        assets = [a for a in assets if search in str(a.get("symbol","")).lower() or search in str(a.get("name","")).lower()]
        
    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_CRYPTO)
    
    rows = ""
    for a in page_items:
        fallback = f"<span class='asset-icon' style='display:none;'>{h(a.get('icon','₿'))}</span>"
        media = f'<img class="asset-logo" src="{h(a.get("logo",""))}" onerror="this.style.display=\'none\'; this.nextElementSibling.style.display=\'inline-flex\';">{fallback}'

        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">{media} {h(a.get("symbol",""))}</strong>
            <span>{h(a.get("name",""))}</span>
          </td>
          <td id="price-{h(a.get('symbol',''))}">{fmt_price(a.get("price",0))}</td>
          <td id="change-{h(a.get('symbol',''))}" class="{a.get('dir','down')}">{fmt_change(a.get("change",0))}</td>
          <td><span id="signal-{h(a.get('symbol',''))}" class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """
        
    status_msg = "Live data fetching in background..." if not rows else f"Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"

    content = f"""
    <section class="section">
      <h1>Crypto</h1>
      <div id="live-updated" class="live-stamp">{status_msg}</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>24h</th><th>Signal</th></tr>
          {rows or "<tr><td colspan='4'>Cache is warming up. Refresh in 10 seconds.</td></tr>"}
        </table>
      </div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout("Crypto - AVA", content, extra_head=live_update_script("crypto"))

@app.route("/stocks")
def stocks():
    try:
        page = int(request.args.get("page", 1))
    except:
        page = 1
        
    search = (request.args.get("q") or "").strip().lower()
    assets = fetch_stock_quotes_safe()
    
    if search:
        assets = [a for a in assets if search in str(a.get("symbol","")).lower() or search in str(a.get("name","")).lower()]
        
    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_STOCKS)
    
    rows = ""
    for a in page_items:
        safe_id = h(str(a.get("symbol","")).replace("=", "_"))
        
        fallback = f"<span class='asset-icon' style='display:none;'>{h(a.get('icon','📈'))}</span>"
        if a.get("logo"):
            media = f'<img class="asset-logo" src="{h(a.get("logo",""))}" onerror="this.style.display=\'none\'; this.nextElementSibling.style.display=\'inline-flex\';">{fallback}'
        else:
            media = f"<span class='asset-icon'>{h(a.get('icon','📈'))}</span>"
        
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">{media} {h(a.get("symbol",""))}</strong>
            <span>{h(a.get("name",""))}</span>
          </td>
          <td id="price-{safe_id}">{fmt_price(a.get("price",0))}</td>
          <td id="change-{safe_id}" class="{a.get('dir','down')}">{fmt_change(a.get("change",0))}</td>
          <td><span id="signal-{safe_id}" class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """

    status_msg = "Live data fetching in background..." if not rows else f"Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC"

    content = f"""
    <section class="section">
      <h1>Stocks & Commodities</h1>
      <div id="live-updated" class="live-stamp">{status_msg}</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>Signal</th></tr>
          {rows or "<tr><td colspan='4'>Cache is warming up. Refresh in 10 seconds.</td></tr>"}
        </table>
      </div>
      {legal_disclaimer_html()}
    </section>
    """
    return nav_layout("Stocks - AVA", content, extra_head=live_update_script("stocks"))


# ==========================================
# BACKGROUND BOOTER (PREVENTS 502/TIMEOUTS)
# ==========================================

_bg_started = False

def start_background_refresh():
    global _bg_started
    if _bg_started: return
    
    def background_loop():
        # Step 1: Initial load right after the server boots safely
        logger.info("Background thread: Fetching initial data...")
        _perform_crypto_fetch()
        _perform_stock_fetch()
        logger.info("Background thread: Initial data cached successfully.")
        
        # Step 2: Loop forever, safely fetching
        while True:
            time.sleep(60)  # Re-fetch data safely every 60 seconds
            _perform_crypto_fetch()
            _perform_stock_fetch()
            
    # Daemon=True ensures thread closes safely when Render restarts server
    t = threading.Thread(target=background_loop, daemon=True)
    t.start()
    _bg_started = True

# Start the background fetcher immediately
if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not Config.DEBUG:
    start_background_refresh()

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
