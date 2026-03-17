#!/usr/bin/env python3
import os
import json
import time
import math
import sqlite3
import secrets
import bcrypt
import logging
import random
import threading
import requests
import yfinance as yf
from datetime import datetime, timedelta, UTC
from functools import wraps

from dotenv import load_dotenv
from flask import Flask, request, redirect, make_response, render_template_string, g, jsonify, abort

try:
    import stripe
except Exception:
    stripe = None

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - AVA MARKETS - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Config:
    HOST = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"
    DATABASE = os.environ.get("DATABASE_URL", "ava_markets_snapshot.db")
    SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
    DOMAIN = os.environ.get("DOMAIN", f"http://localhost:{PORT}")

    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin")
    ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "AvaAdmin2024!")

    TIERS = {
        "free": {"price": 0},
        "basic": {"price": 19},
        "pro": {"price": 49},
        "elite": {"price": 149},
    }

    PAGE_SIZE_CRYPTO = 25
    PAGE_SIZE_STOCKS = 20


if stripe and Config.STRIPE_SECRET_KEY:
    stripe.api_key = Config.STRIPE_SECRET_KEY

app = Flask(__name__)
app.config["SECRET_KEY"] = Config.SECRET_KEY


class Database:
    def __init__(self, path):
        self.path = path
        self.init()

    def conn(self):
        c = sqlite3.connect(self.path, check_same_thread=False)
        c.row_factory = sqlite3.Row
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            provider TEXT,
            payment_id TEXT,
            amount REAL,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            prediction_type TEXT,
            confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS snapshots (
            cache_key TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
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
        expires_at = datetime.now() + timedelta(days=days)
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
        """, (token, datetime.now())).fetchone()
        c.close()
        return dict(row) if row else None

    def delete_session(self, token):
        c = self.conn()
        c.execute("DELETE FROM sessions WHERE token = ?", (token,))
        c.commit()
        c.close()

    def update_user(self, user_id, **kwargs):
        if not kwargs:
            return
        fields = []
        values = []
        for k, v in kwargs.items():
            fields.append(f"{k} = ?")
            values.append(v)
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

    def get_recent_predictions(self, user_id, limit=10):
        c = self.conn()
        rows = c.execute("""
            SELECT prediction_type, confidence, created_at
            FROM predictions
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
        """, (user_id, limit)).fetchall()
        c.close()
        return [dict(r) for r in rows]

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

    def set_snapshot(self, cache_key, payload):
        c = self.conn()
        c.execute("""
            INSERT INTO snapshots (cache_key, payload, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cache_key) DO UPDATE SET
                payload = excluded.payload,
                updated_at = CURRENT_TIMESTAMP
        """, (cache_key, json.dumps(payload)))
        c.commit()
        c.close()

    def get_snapshot(self, cache_key):
        c = self.conn()
        row = c.execute("SELECT payload, updated_at FROM snapshots WHERE cache_key = ?", (cache_key,)).fetchone()
        c.close()
        if not row:
            return None
        return {
            "payload": json.loads(row["payload"]),
            "updated_at": row["updated_at"]
        }


db = Database(Config.DATABASE)


class StripeManager:
    @staticmethod
    def create_checkout(user_id, email, tier, success_url, cancel_url):
        if not stripe or not Config.STRIPE_SECRET_KEY:
            return None
        prices = {"basic": 1900, "pro": 4900, "elite": 14900}
        try:
            return stripe.checkout.Session.create(
                payment_method_types=["card"],
                customer_email=email,
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
            return stripe.billing_portal.Session.create(
                customer=customer_id,
                return_url=return_url
            )
        except Exception as e:
            logger.error(f"Stripe portal error: {e}")
            return None


sm = StripeManager()

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
h1{font-size:clamp(2.5rem,5vw,4.6rem);line-height:1.02;margin:0 0 18px}
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
.market-table th,.market-table td{padding:16px 14px;border-bottom:1px solid rgba(255,255,255,.06);text-align:left}
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
.pagination{
  display:flex;gap:10px;flex-wrap:wrap;margin-top:22px;align-items:center
}
.page-link{
  padding:10px 14px;border-radius:12px;background:rgba(255,255,255,.05);
  border:1px solid rgba(255,255,255,.08);color:var(--text);font-weight:700
}
.search-box{
  width:100%;max-width:420px;padding:14px 16px;border-radius:14px;
  border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.04);color:var(--text);outline:none;
}
@media (max-width: 900px){
  .hero,.detail-grid,.mini-grid{grid-template-columns:1fr}
  .nav{flex-direction:column;gap:14px}
  .nav-links{justify-content:center}
}
"""

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

    ("GC=F", "Gold Futures"),
    ("SI=F", "Silver Futures"),
    ("PL=F", "Platinum Futures"),
    ("CL=F", "Oil Futures"),
    ("SIG", "Diamonds Proxy"),
]

STOCK_NAME_MAP = {s: n for s, n in STOCK_UNIVERSE}

BINANCE_SYMBOLS = {
    "BTC": "BTCUSDT", "ETH": "ETHUSDT", "SOL": "SOLUSDT", "BNB": "BNBUSDT",
    "XRP": "XRPUSDT", "DOGE": "DOGEUSDT", "ADA": "ADAUSDT", "AVAX": "AVAXUSDT",
}


def symbol_to_binance_pair(symbol):
    s = symbol.upper()
    if s in BINANCE_SYMBOLS:
        return BINANCE_SYMBOLS[s]
    if s.endswith("USDT"):
        return s
    return f"{s}USDT"


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

    html = ['<div class="candles">']
    for b in bars:
        html.append(f"""
        <div class="candle">
          <div class="wick" style="top:0;height:{b['wick']}px;"></div>
          <div class="body {b['color']}" style="top:{b['top']}px;height:{b['height']}px;"></div>
        </div>
        """)
    html.append("</div>")
    return "".join(html)


def render_candles_from_ohlc(candles, height=140):
    if not candles:
        return fallback_candles_html()

    sample = candles[-20:]
    highs = [c["high"] for c in sample]
    lows = [c["low"] for c in sample]
    max_high = max(highs)
    min_low = min(lows)
    span = max(max_high - min_low, 1e-9)

    html = ['<div class="candles">']
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

        html.append(f"""
        <div class="candle">
          <div class="wick" style="top:{wick_top:.1f}px;height:{wick_height:.1f}px;"></div>
          <div class="body {color}" style="top:{body_top:.1f}px;height:{body_height:.1f}px;"></div>
        </div>
        """)
    html.append("</div>")
    return "".join(html)


def fetch_crypto_candles(symbol, interval="15m", limit=60):
    try:
        pair = symbol_to_binance_pair(symbol)
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": pair, "interval": interval, "limit": limit},
            timeout=12
        )
        r.raise_for_status()
        return [{
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4])
        } for row in r.json()]
    except Exception as e:
        logger.warning(f"fetch_crypto_candles failed for {symbol}: {e}")
        return None


def fetch_stock_candles(symbol, period="6mo", interval="1d"):
    try:
        ticker = yf.Ticker(symbol.upper())
        hist = ticker.history(period=period, interval=interval)
        if hist is None or hist.empty:
            hist = ticker.history(period="1y", interval="1d")

        candles = []
        for _, row in hist.tail(60).iterrows():
            candles.append({
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"])
            })
        return candles or None
    except Exception as e:
        logger.warning(f"fetch_stock_candles failed for {symbol}: {e}")
        return None


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


def extract_market_features(candles):
    if not candles or len(candles) < 10:
        return None

    closes = [c["close"] for c in candles]
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    last = closes[-1]
    prev = closes[-2]
    sma5 = sum(closes[-5:]) / 5
    sma10 = sum(closes[-10:]) / 10
    move_1 = pct_change(last, prev)
    move_5 = pct_change(last, closes[-5])
    intrabar = pct_change(last, opens[-1])
    rsi = compute_rsi(closes, period=min(14, len(closes) - 1))

    range_now = highs[-1] - lows[-1]
    range_avg = sum((highs[i] - lows[i]) for i in range(-5, 0)) / 5 if len(highs) >= 5 else range_now
    volatility_ratio = (range_now / range_avg) if range_avg else 1.0

    n = len(closes)
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(closes) / n
    denom = sum((xi - x_mean) ** 2 for xi in x) or 1e-9
    slope = sum((x[i] - x_mean) * (closes[i] - y_mean) for i in range(n)) / denom

    return {
        "last": last,
        "prev": prev,
        "sma5": sma5,
        "sma10": sma10,
        "move_1": move_1,
        "move_5": move_5,
        "intrabar": intrabar,
        "rsi": rsi,
        "volatility_ratio": volatility_ratio,
        "slope": slope,
        "closes": closes,
    }


def ava_hypothesis_engine(candles):
    feats = extract_market_features(candles)
    if not feats:
        return {
            "signal": "HOLD",
            "confidence": 0.50,
            "bullish_score": 0.0,
            "bearish_score": 0.0,
            "neutral_score": 1.0,
            "trend_state": "Neutral",
            "trend_strength": "Low",
            "forecast_trend": "Stable",
            "projected_change": "+0.00%",
            "regime": "Unknown",
            "dominant_factors": ["insufficient_data"],
            "reasoning": "Not enough candle history for AVA Brain v1."
        }

    bullish = 0.0
    bearish = 0.0
    neutral = 0.0
    factors = []
    regime = "Mixed"

    if feats["last"] > feats["sma5"]:
        bullish += 1.2
        factors.append(("price_above_sma5", 1.2))
    else:
        bearish += 1.2
        factors.append(("price_below_sma5", 1.2))

    if feats["sma5"] > feats["sma10"]:
        bullish += 1.1
        factors.append(("sma5_above_sma10", 1.1))
    else:
        bearish += 1.1
        factors.append(("sma5_below_sma10", 1.1))

    if feats["move_5"] > 2:
        bullish += 1.3
        factors.append(("strong_5_period_momentum", 1.3))
    elif feats["move_5"] < -2:
        bearish += 1.3
        factors.append(("weak_5_period_momentum", 1.3))
    else:
        neutral += 0.7
        factors.append(("mixed_5_period_momentum", 0.7))

    if feats["move_1"] > 0.5:
        bullish += 0.5
        factors.append(("positive_recent_move", 0.5))
    elif feats["move_1"] < -0.5:
        bearish += 0.5
        factors.append(("negative_recent_move", 0.5))
    else:
        neutral += 0.4
        factors.append(("flat_recent_move", 0.4))

    if feats["rsi"] >= 62:
        bullish += 0.9
        factors.append(("rsi_bullish", 0.9))
    elif feats["rsi"] <= 38:
        bearish += 0.9
        factors.append(("rsi_bearish", 0.9))
    else:
        neutral += 0.8
        factors.append(("rsi_balanced", 0.8))

    if feats["intrabar"] > 0.3:
        bullish += 0.4
        factors.append(("intrabar_buying", 0.4))
    elif feats["intrabar"] < -0.3:
        bearish += 0.4
        factors.append(("intrabar_selling", 0.4))
    else:
        neutral += 0.3
        factors.append(("intrabar_mixed", 0.3))

    if feats["slope"] > 0.05:
        bullish += 1.0
        factors.append(("positive_slope", 1.0))
        forecast_trend = "Upward"
    elif feats["slope"] < -0.05:
        bearish += 1.0
        factors.append(("negative_slope", 1.0))
        forecast_trend = "Downward"
    else:
        neutral += 1.0
        factors.append(("flat_slope", 1.0))
        forecast_trend = "Stable"

    if feats["volatility_ratio"] > 1.5:
        neutral += 0.7
        regime = "Volatile"
        factors.append(("high_volatility", 0.7))
    elif abs(feats["move_5"]) > 3 and abs(feats["slope"]) > 0.05:
        regime = "Trending"

    scores = {"BUY": bullish, "SELL": bearish, "HOLD": neutral}
    signal = max(scores, key=scores.get)
    ordered = sorted(scores.values(), reverse=True)
    gap = ordered[0] - ordered[1]
    confidence = min(0.95, 0.52 + gap * 0.12)

    if bullish > bearish + 1.0:
        trend_state = "Bullish"
    elif bearish > bullish + 1.0:
        trend_state = "Bearish"
    else:
        trend_state = "Neutral"

    strength_raw = max(bullish, bearish, neutral)
    if strength_raw > 3.5:
        trend_strength = "High"
    elif strength_raw > 2.0:
        trend_strength = "Medium"
    else:
        trend_strength = "Low"

    projected_change_val = feats["move_5"] * 0.8 + feats["slope"] * 10
    projected_change = f"{projected_change_val:+.2f}%"

    dominant_factors = [name for name, weight in sorted(factors, key=lambda x: x[1], reverse=True)[:4]]

    if signal == "BUY":
        reasoning = "AVA sees stronger bullish structure than bearish pressure across trend, RSI, and momentum."
    elif signal == "SELL":
        reasoning = "AVA sees stronger bearish structure than bullish support across trend, RSI, and momentum."
    else:
        reasoning = "AVA sees mixed signals, so neutral positioning is preferred over aggressive direction."

    return {
        "signal": signal,
        "confidence": round(confidence, 2),
        "bullish_score": round(bullish, 2),
        "bearish_score": round(bearish, 2),
        "neutral_score": round(neutral, 2),
        "trend_state": trend_state,
        "trend_strength": trend_strength,
        "forecast_trend": forecast_trend,
        "projected_change": projected_change,
        "regime": regime,
        "dominant_factors": dominant_factors,
        "reasoning": reasoning
    }


def compute_light_signal(change):
    if change >= 2.0:
        return "BUY"
    if change <= -2.0:
        return "SELL"
    return "HOLD"


def snapshot_crypto_quotes():
    try:
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 100,
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h"
            },
            timeout=12
        )
        r.raise_for_status()
        data = r.json()
        payload = []
        for item in data[:100]:
            change = float(item.get("price_change_percentage_24h") or 0)
            payload.append({
                "symbol": item.get("symbol", "").upper(),
                "name": item.get("name", item.get("symbol", "").upper()),
                "price": float(item.get("current_price") or 0),
                "change": change,
                "dir": "up" if change >= 0 else "down",
                "signal": compute_light_signal(change)
            })
        db.set_snapshot("crypto_quotes_top_100", payload)
        logger.info("Updated crypto quote snapshot")
    except Exception as e:
        logger.warning(f"snapshot_crypto_quotes failed: {e}")


def snapshot_stock_quotes():
    try:
        symbols = [s for s, _ in STOCK_UNIVERSE]
        tickers = yf.Tickers(" ".join(symbols))
        payload = []

        for symbol in symbols:
            ticker = tickers.tickers[symbol]
            price = None
            prev_close = None

            try:
                info = ticker.fast_info
                price = info.get("lastPrice")
                prev_close = info.get("previousClose")
            except Exception:
                pass

            if price is None or prev_close in [None, 0]:
                hist = ticker.history(period="2d", interval="1d")
                if len(hist) >= 2:
                    prev_close = float(hist["Close"].iloc[-2])
                    price = float(hist["Close"].iloc[-1])
                elif len(hist) == 1:
                    price = float(hist["Close"].iloc[-1])
                    prev_close = price

            if price is None:
                continue

            change = pct_change(price, prev_close)
            payload.append({
                "symbol": symbol,
                "name": STOCK_NAME_MAP.get(symbol, symbol),
                "price": float(price),
                "change": float(change),
                "dir": "up" if change >= 0 else "down",
                "signal": compute_light_signal(change)
            })

        db.set_snapshot("stock_quotes_top_45_plus_commodities", payload)
        logger.info("Updated stock quote snapshot")
    except Exception as e:
        logger.warning(f"snapshot_stock_quotes failed: {e}")


def snapshot_worker():
    while True:
        try:
            snapshot_crypto_quotes()
            snapshot_stock_quotes()
        except Exception as e:
            logger.warning(f"snapshot_worker error: {e}")
        time.sleep(120)


def start_snapshot_worker():
    t = threading.Thread(target=snapshot_worker, daemon=True)
    t.start()
    logger.info("Snapshot worker started")


def get_crypto_quotes_snapshot():
    snap = db.get_snapshot("crypto_quotes_top_100")
    if snap and snap["payload"]:
        return snap["payload"]
    snapshot_crypto_quotes()
    snap = db.get_snapshot("crypto_quotes_top_100")
    return snap["payload"] if snap else []


def get_stock_quotes_snapshot():
    snap = db.get_snapshot("stock_quotes_top_45_plus_commodities")
    if snap and snap["payload"]:
        return snap["payload"]
    snapshot_stock_quotes()
    snap = db.get_snapshot("stock_quotes_top_45_plus_commodities")
    return snap["payload"] if snap else []


def paginate(items, page, per_page):
    total = len(items)
    pages = max(1, math.ceil(total / per_page))
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], total, pages, page


def get_crypto_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in get_crypto_quotes_snapshot()}
    if symbol not in light_map:
        return None

    asset = dict(light_map[symbol])
    candles = fetch_crypto_candles(symbol, interval="15m", limit=60) or []
    brain = ava_hypothesis_engine(candles)
    feats = extract_market_features(candles)

    signal_meta = {
        "signal": brain["signal"],
        "confidence": brain["confidence"],
        "rsi": round(feats["rsi"], 2) if feats else 50.0,
        "momentum": round(feats["move_5"], 2) if feats else 0.0,
        "summary": brain["reasoning"]
    }

    forecast = {
        "trend": brain["forecast_trend"],
        "projected_change": brain["projected_change"],
        "confidence_band": "92%" if brain["confidence"] > 0.70 else "88%",
        "summary": f"AVA forecast regime: {brain['forecast_trend'].lower()} with {brain['trend_strength'].lower()} conviction."
    }

    trend = {
        "state": brain["trend_state"],
        "strength": brain["trend_strength"],
        "read": brain["regime"],
        "summary": f"Dominant factors: {', '.join(brain['dominant_factors'])}."
    }

    asset.update({
        "price_display": fmt_price(asset["price"], asset["symbol"]),
        "change_display": fmt_change(asset["change"]),
        "detail_candles": render_candles_from_ohlc(candles) if candles else fallback_candles_html(99),
        "signal_meta": signal_meta,
        "forecast": forecast,
        "trend_data": trend,
        "signal": brain["signal"],
        "ava_brain": brain,
    })
    return asset


def get_stock_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in get_stock_quotes_snapshot()}
    if symbol not in light_map:
        return None

    asset = dict(light_map[symbol])
    candles = fetch_stock_candles(symbol, period="6mo", interval="1d") or []
    brain = ava_hypothesis_engine(candles)
    feats = extract_market_features(candles)

    signal_meta = {
        "signal": brain["signal"],
        "confidence": brain["confidence"],
        "rsi": round(feats["rsi"], 2) if feats else 50.0,
        "momentum": round(feats["move_5"], 2) if feats else 0.0,
        "summary": brain["reasoning"]
    }

    forecast = {
        "trend": brain["forecast_trend"],
        "projected_change": brain["projected_change"],
        "confidence_band": "92%" if brain["confidence"] > 0.70 else "88%",
        "summary": f"AVA forecast regime: {brain['forecast_trend'].lower()} with {brain['trend_strength'].lower()} conviction."
    }

    trend = {
        "state": brain["trend_state"],
        "strength": brain["trend_strength"],
        "read": brain["regime"],
        "summary": f"Dominant factors: {', '.join(brain['dominant_factors'])}."
    }

    asset.update({
        "price_display": fmt_price(asset["price"], asset["symbol"]),
        "change_display": fmt_change(asset["change"]),
        "detail_candles": render_candles_from_ohlc(candles) if candles else fallback_candles_html(101),
        "signal_meta": signal_meta,
        "forecast": forecast,
        "trend_data": trend,
        "signal": brain["signal"],
        "ava_brain": brain,
    })
    return asset


def get_web_user():
    return db.get_user_by_session(request.cookies.get("session_token"))


@app.before_request
def load_user():
    g.user = get_web_user()


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
        if request.cookies.get("admin_auth") != "1":
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
    return is_paid_active() and current_tier() in ["basic", "pro", "elite"]


def can_access_forecast():
    return is_paid_active() and current_tier() in ["pro", "elite"]


def can_access_trends():
    return is_paid_active() and current_tier() in ["pro", "elite"]


def nav_layout(title, content):
    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
        <a href="/forecast">Forecast</a>
        <a href="/trends">Trends</a>
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

    <div class="footer">AVA Markets © 2026 — crypto, stocks, premium signals, trends, and forecasts.</div>
  </div>
</body>
</html>
    """, title=title, content=content, css=CSS, user=g.get("user"))


@app.route("/")
def home():
    crypto_list = get_crypto_quotes_snapshot()
    stock_list = get_stock_quotes_snapshot()

    featured_crypto = dict(crypto_list[0]) if crypto_list else {"symbol": "BTC", "name": "Bitcoin", "price": 0, "change": 0, "dir": "up", "signal": "HOLD"}
    featured_stock = dict(stock_list[0]) if stock_list else {"symbol": "AAPL", "name": "Apple", "price": 0, "change": 0, "dir": "up", "signal": "HOLD"}

    featured_crypto["price_display"] = fmt_price(featured_crypto["price"], featured_crypto["symbol"])
    featured_crypto["change_display"] = fmt_change(featured_crypto["change"])
    featured_stock["price_display"] = fmt_price(featured_stock["price"], featured_stock["symbol"])
    featured_stock["change_display"] = fmt_change(featured_stock["change"])

    crypto_candles = fetch_crypto_candles(featured_crypto["symbol"], interval="15m", limit=40)
    stock_candles = fetch_stock_candles(featured_stock["symbol"], period="3mo", interval="1d")

    content = render_template_string("""
    <section class="hero">
      <div class="hero-card">
        <div class="badge">AVA Markets Brain v1</div>
        <h1>Fast cached crypto, global stocks, metals, and premium signal intelligence.</h1>
        <p>
          AVA uses cached market snapshots for speed and deeper detail-page analysis for signal,
          trend, and forecast intelligence.
        </p>
        <div class="btns">
          <a class="btn btn-primary" href="/crypto">Explore Crypto</a>
          <a class="btn btn-secondary" href="/stocks">Explore Stocks</a>
          <a class="btn btn-secondary" href="/pricing">View Pricing</a>
        </div>
      </div>

      <div class="card">
        <div class="badge">Featured {{ featured_crypto.symbol }}</div>
        <div style="font-size:2.4rem;font-weight:800;">{{ featured_crypto.price_display }}
          <span class="{{ 'up' if featured_crypto.dir == 'up' else 'down' }}" style="font-size:1rem;">{{ featured_crypto.change_display }}</span>
        </div>
        <p>Fast snapshot quote with live-style candle preview.</p>
        <div style="margin:16px 0 18px;">
          {% if signals %}
            <span class="signal {{ 'signal-buy' if featured_crypto.signal == 'BUY' else 'signal-hold' if featured_crypto.signal == 'HOLD' else 'signal-sell' }}">{{ featured_crypto.signal }}</span>
          {% else %}
            <span class="signal signal-locked">Signal Locked</span>
          {% endif %}
        </div>
        <div class="candle-box">{{ crypto_candles|safe }}</div>
      </div>
    </section>

    <section class="section">
      <h2 class="section-title">Featured stock / commodity</h2>
      <div class="card">
        <h2><a href="/stocks/{{ featured_stock.symbol }}">{{ featured_stock.symbol }}</a> — {{ featured_stock.name }}</h2>
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
    return nav_layout("AVA Markets", content)


@app.route("/crypto")
def crypto():
    page = int(request.args.get("page", 1) or 1)
    search = (request.args.get("q") or "").strip().lower()

    assets = [dict(a) for a in get_crypto_quotes_snapshot()]
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
        signal_html = f'<span class="signal {sig_class}">{a["signal"]}</span>' if unlocked else '<span class="signal signal-locked">Locked</span>'
        rows += f"""
        <tr>
          <td class="asset-name"><strong><a href="/crypto/{a['symbol']}">{a['symbol']}</a></strong><span>{a['name']}</span></td>
          <td>{a['price_display']}</td>
          <td class="{'up' if a['dir']=='up' else 'down'}">{a['change_display']}</td>
          <td>{signal_html}</td>
          <td>{"Unlocked" if unlocked else "Basic+"}</td>
        </tr>
        """

    featured = page_items[0] if page_items else assets[0]
    featured_candles = fetch_crypto_candles(featured["symbol"], interval="15m", limit=40)

    pagination = ""
    if current > 1:
        pagination += f'<a class="page-link" href="/crypto?page={current-1}&q={search}">Previous</a>'
    pagination += f'<span class="page-link">Page {current} / {pages}</span>'
    if current < pages:
        pagination += f'<a class="page-link" href="/crypto?page={current+1}&q={search}">Next</a>'

    content = f"""
    <section class="section">
      <h1>Crypto</h1>
      <p class="section-sub">Top 100 crypto snapshot cache. List pages stay light and fast.</p>

      <form method="GET" style="margin-bottom:20px;">
        <input class="search-box" type="text" name="q" placeholder="Search crypto symbol or name..." value="{search}">
      </form>

      <div class="card" style="margin-bottom:24px;">
        <div class="badge">Featured Market</div>
        <h2><a href="/crypto/{featured['symbol']}">{featured['symbol']}</a> — {featured['name']}</h2>
        <p>{featured['price_display']} <span class="{'up' if featured['dir']=='up' else 'down'}">{featured['change_display']}</span></p>
        <div class="candle-box">{render_candles_from_ohlc(featured_candles) if featured_candles else fallback_candles_html(3)}</div>
      </div>

      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>24h</th><th>Signal</th><th>Access</th></tr>
          {rows}
        </table>
      </div>

      <div class="pagination">{pagination}</div>
    </section>
    """
    return nav_layout("Crypto - AVA Markets", content)


@app.route("/stocks")
def stocks():
    page = int(request.args.get("page", 1) or 1)
    search = (request.args.get("q") or "").strip().lower()

    assets = [dict(a) for a in get_stock_quotes_snapshot()]
    for a in assets:
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])

    if search:
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_STOCKS)
    unlocked = can_access_signals()

    rows = ""
    for a in page_items:
        sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[a["signal"]]
        signal_html = f'<span class="signal {sig_class}">{a["signal"]}</span>' if unlocked else '<span class="signal signal-locked">Locked</span>'
        rows += f"""
        <tr>
          <td class="asset-name"><strong><a href="/stocks/{a['symbol']}">{a['symbol']}</a></strong><span>{a['name']}</span></td>
          <td>{a['price_display']}</td>
          <td class="{'up' if a['dir']=='up' else 'down'}">{a['change_display']}</td>
          <td>{signal_html}</td>
          <td>{"Unlocked" if unlocked else "Basic+"}</td>
        </tr>
        """

    featured = page_items[0] if page_items else assets[0]
    featured_candles = fetch_stock_candles(featured["symbol"], period="3mo", interval="1d")

    pagination = ""
    if current > 1:
        pagination += f'<a class="page-link" href="/stocks?page={current-1}&q={search}">Previous</a>'
    pagination += f'<span class="page-link">Page {current} / {pages}</span>'
    if current < pages:
        pagination += f'<a class="page-link" href="/stocks?page={current+1}&q={search}">Next</a>'

    content = f"""
    <section class="section">
      <h1>Stocks + Commodities</h1>
      <p class="section-sub">Top 45 stocks plus gold, silver, platinum, oil, and diamonds proxy from cached snapshots.</p>

      <form method="GET" style="margin-bottom:20px;">
        <input class="search-box" type="text" name="q" placeholder="Search stock or commodity..." value="{search}">
      </form>

      <div class="card" style="margin-bottom:24px;">
        <div class="badge">Featured Asset</div>
        <h2><a href="/stocks/{featured['symbol']}">{featured['symbol']}</a> — {featured['name']}</h2>
        <p>{featured['price_display']} <span class="{'up' if featured['dir']=='up' else 'down'}">{featured['change_display']}</span></p>
        <div class="candle-box">{render_candles_from_ohlc(featured_candles) if featured_candles else fallback_candles_html(4)}</div>
      </div>

      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>Signal</th><th>Access</th></tr>
          {rows}
        </table>
      </div>

      <div class="pagination">{pagination}</div>
    </section>
    """
    return nav_layout("Stocks - AVA Markets", content)


@app.route("/crypto/<symbol>")
def crypto_detail(symbol):
    asset = get_crypto_detail(symbol)
    if not asset:
        abort(404)

    unlocked_signals = can_access_signals()
    unlocked_forecast = can_access_forecast()
    unlocked_trends = can_access_trends()

    sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[asset["signal"]]
    signal_html = f'<span class="signal {sig_class}">{asset["signal"]}</span>' if unlocked_signals else '<span class="signal signal-locked">Locked</span>'

    content = render_template_string("""
    <section class="section">
      <h1>{{ asset.symbol }} — {{ asset.name }}</h1>
      <p class="section-sub">Detail page loads AVA Brain v1 for signal, trend, and forecast intelligence.</p>

      <div class="detail-grid">
        <div class="card">
          <div class="badge">Live Market</div>
          <h2>{{ asset.price_display }} <span class="{{ 'up' if asset.dir == 'up' else 'down' }}">{{ asset.change_display }}</span></h2>
          <div style="margin:12px 0 18px;">{{ signal_html|safe }}</div>
          <div class="candle-box">{{ asset.detail_candles|safe }}</div>
        </div>

        <div class="mini-grid">
          <div class="metric-card">
            <h3>Signal</h3>
            <p>{% if unlocked_signals %}{{ asset.signal_meta.signal }}{% else %}Locked{% endif %}</p>
            <p>Confidence: {% if unlocked_signals %}{{ (asset.signal_meta.confidence * 100)|round(0) }}%{% else %}Basic+{% endif %}</p>
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

      <div class="market-grid" style="margin-top:24px;">
        <div class="price-card">
          <h3>AVA Reasoning</h3>
          <p>{% if unlocked_signals %}{{ asset.signal_meta.summary }}{% else %}Upgrade to Basic or higher to unlock signal reasoning.{% endif %}</p>
        </div>
        <div class="price-card">
          <h3>Trend Read</h3>
          <p>{% if unlocked_trends %}{{ asset.trend_data.summary }}{% else %}Upgrade to Pro or Elite to unlock trend intelligence.{% endif %}</p>
        </div>
        <div class="price-card">
          <h3>Forecast Read</h3>
          <p>{% if unlocked_forecast %}{{ asset.forecast.summary }}{% else %}Upgrade to Pro or Elite to unlock forecast intelligence.{% endif %}</p>
        </div>
      </div>
    </section>
    """, asset=asset, signal_html=signal_html, unlocked_signals=unlocked_signals, unlocked_forecast=unlocked_forecast, unlocked_trends=unlocked_trends)
    return nav_layout(f"{asset['symbol']} - AVA Markets", content)


@app.route("/stocks/<symbol>")
def stock_detail(symbol):
    asset = get_stock_detail(symbol)
    if not asset:
        abort(404)

    unlocked_signals = can_access_signals()
    unlocked_forecast = can_access_forecast()
    unlocked_trends = can_access_trends()

    sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[asset["signal"]]
    signal_html = f'<span class="signal {sig_class}">{asset["signal"]}</span>' if unlocked_signals else '<span class="signal signal-locked">Locked</span>'

    content = render_template_string("""
    <section class="section">
      <h1>{{ asset.symbol }} — {{ asset.name }}</h1>
      <p class="section-sub">Detail page loads AVA Brain v1 for signal, trend, and forecast intelligence.</p>

      <div class="detail-grid">
        <div class="card">
          <div class="badge">Live Market</div>
          <h2>{{ asset.price_display }} <span class="{{ 'up' if asset.dir == 'up' else 'down' }}">{{ asset.change_display }}</span></h2>
          <div style="margin:12px 0 18px;">{{ signal_html|safe }}</div>
          <div class="candle-box">{{ asset.detail_candles|safe }}</div>
        </div>

        <div class="mini-grid">
          <div class="metric-card">
            <h3>Signal</h3>
            <p>{% if unlocked_signals %}{{ asset.signal_meta.signal }}{% else %}Locked{% endif %}</p>
            <p>Confidence: {% if unlocked_signals %}{{ (asset.signal_meta.confidence * 100)|round(0) }}%{% else %}Basic+{% endif %}</p>
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

      <div class="market-grid" style="margin-top:24px;">
        <div class="price-card">
          <h3>AVA Reasoning</h3>
          <p>{% if unlocked_signals %}{{ asset.signal_meta.summary }}{% else %}Upgrade to Basic or higher to unlock signal reasoning.{% endif %}</p>
        </div>
        <div class="price-card">
          <h3>Trend Read</h3>
          <p>{% if unlocked_trends %}{{ asset.trend_data.summary }}{% else %}Upgrade to Pro or Elite to unlock trend intelligence.{% endif %}</p>
        </div>
        <div class="price-card">
          <h3>Forecast Read</h3>
          <p>{% if unlocked_forecast %}{{ asset.forecast.summary }}{% else %}Upgrade to Pro or Elite to unlock forecast intelligence.{% endif %}</p>
        </div>
      </div>
    </section>
    """, asset=asset, signal_html=signal_html, unlocked_signals=unlocked_signals, unlocked_forecast=unlocked_forecast, unlocked_trends=unlocked_trends)
    return nav_layout(f"{asset['symbol']} - AVA Markets", content)


@app.route("/forecast")
def forecast():
    unlocked = can_access_forecast()
    sample = get_crypto_detail("BTC") or {}
    f = sample.get("forecast", {"trend": "Stable", "projected_change": "+0.00%", "confidence_band": "90%", "summary": "Not enough data."})

    cards = """
      <div class="price-card"><h3>Trend</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Projected Change</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Confidence Band</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
    """
    if unlocked:
        cards = f"""
          <div class="price-card"><h3>Trend</h3><div style="font-size:2rem;font-weight:800;" class="up">{f['trend']}</div><p>{f['summary']}</p></div>
          <div class="price-card"><h3>Projected Change</h3><div style="font-size:2rem;font-weight:800;" class="up">{f['projected_change']}</div><p>Estimated move over the next periods.</p></div>
          <div class="price-card"><h3>Confidence Band</h3><div style="font-size:2rem;font-weight:800;">{f['confidence_band']}</div><p>Forecast confidence band.</p></div>
        """
    content = f"""
    <section class="section">
      <h1>Forecast</h1>
      <p class="section-sub">Forecast intelligence is generated from AVA Brain v1 detail analysis.</p>
      <div class="market-grid">{cards}</div>
    </section>
    """
    return nav_layout("Forecast - AVA Markets", content)


@app.route("/trends")
def trends():
    unlocked = can_access_trends()
    sample = get_crypto_detail("BTC") or {}
    t = sample.get("trend_data", {"state": "Neutral", "strength": "Low", "read": "Mixed", "summary": "Not enough data."})

    cards = """
      <div class="price-card"><h3>Trend State</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Trend Strength</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
      <div class="price-card"><h3>Trend Read</h3><div style="font-size:2rem;font-weight:800;">Locked</div><p>Pro+</p></div>
    """
    if unlocked:
        cards = f"""
          <div class="price-card"><h3>Trend State</h3><div style="font-size:2rem;font-weight:800;" class="up">{t['state']}</div><p>{t['summary']}</p></div>
          <div class="price-card"><h3>Trend Strength</h3><div style="font-size:2rem;font-weight:800;">{t['strength']}</div><p>Directional magnitude.</p></div>
          <div class="price-card"><h3>Trend Read</h3><div style="font-size:2rem;font-weight:800;">{t['read']}</div><p>Primary trend driver.</p></div>
        """
    content = f"""
    <section class="section">
      <h1>Trends</h1>
      <p class="section-sub">Trend intelligence is generated from AVA Brain v1 detail analysis.</p>
      <div class="market-grid">{cards}</div>
    </section>
    """
    return nav_layout("Trends - AVA Markets", content)


@app.route("/pricing")
def pricing():
    content = render_template_string("""
    <section class="section">
      <h1>Pricing</h1>
      <p class="section-sub">Simple pricing for signals, forecasts, and trend intelligence.</p>
      <div class="market-grid">
        <div class="price-card">
          <h3>Free</h3>
          <div style="font-size:2.4rem;font-weight:800;">$0</div>
          <p>Browse crypto + stocks • candles • market movement</p>
        </div>

        <div class="price-card">
          <h3>Basic</h3>
          <div style="font-size:2.4rem;font-weight:800;">$19</div>
          <p>Unlock crypto + stock signals</p>
          {% if user %}
            <form method="POST" action="/checkout/basic"><button class="btn btn-primary" type="submit">Choose Basic</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Pro</h3>
          <div style="font-size:2.4rem;font-weight:800;">$49</div>
          <p>Everything in Basic • forecast • trends</p>
          {% if user %}
            <form method="POST" action="/checkout/pro"><button class="btn btn-primary" type="submit">Choose Pro</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Elite</h3>
          <div style="font-size:2.4rem;font-weight:800;">$149</div>
          <p>Everything in Pro • highest access</p>
          {% if user %}
            <form method="POST" action="/checkout/elite"><button class="btn btn-primary" type="submit">Choose Elite</button></form>
          {% endif %}
        </div>
      </div>

      {% if not user %}
      <div class="btns">
        <a class="btn btn-primary" href="/register">Create Account</a>
        <a class="btn btn-secondary" href="/login">Login</a>
      </div>
      {% endif %}
    </section>
    """, user=g.get("user"))
    return nav_layout("Pricing - AVA Markets", content)


@app.route("/register", methods=["GET", "POST"])
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
              <input type="password" name="password" placeholder="Password (min 6 chars)" required>
              <button type="submit">Register</button>
            </form>
          </div>
        </div>
        """
        return nav_layout("Register - AVA Markets", content)

    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()

    if not email or len(password) < 6:
        return nav_layout("Register Error", '<div class="form-shell"><div class="form-card"><div class="error">Email and password (min 6 chars) required.</div><a href="/register">Try again</a></div></div>')

    user = db.create_user(email, password)
    if not user:
        return nav_layout("Register Error", '<div class="form-shell"><div class="form-card"><div class="error">Email already registered.</div><a href="/login">Login instead</a></div></div>')

    token = db.create_session(user["id"])
    resp = make_response(redirect("/dashboard"))
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", max_age=30 * 86400)
    return resp


@app.route("/login", methods=["GET", "POST"])
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
        return nav_layout("Login - AVA Markets", content)

    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()

    user = db.verify_user(email, password)
    if not user:
        return nav_layout("Login Error", '<div class="form-shell"><div class="form-card"><div class="error">Invalid credentials.</div><a href="/login">Try again</a></div></div>')

    token = db.create_session(user["id"])
    resp = make_response(redirect("/dashboard"))
    resp.set_cookie("session_token", token, httponly=True, samesite="Lax", max_age=30 * 86400)
    return resp


@app.route("/logout")
def logout():
    token = request.cookies.get("session_token")
    if token:
        db.delete_session(token)
    resp = make_response(redirect("/"))
    resp.delete_cookie("session_token")
    return resp


@app.route("/dashboard")
@require_login
def dashboard():
    user = g.user
    recent = db.get_recent_predictions(user["id"], limit=10)

    rows = ""
    for r in recent:
        rows += f"""
        <tr>
          <td>{r['prediction_type']}</td>
          <td>{r['confidence'] if r['confidence'] is not None else '—'}</td>
          <td>{r['created_at']}</td>
        </tr>
        """
    if not rows:
        rows = "<tr><td colspan='3'>No predictions yet.</td></tr>"

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
          <p><strong>Email:</strong> {user['email']}</p>
          <p><strong>Tier:</strong> <span class="tier">{user['tier']}</span></p>
          <p><strong>Subscription:</strong> {user['subscription_status']}</p>
        </div>

        <div class="dashboard-card">
          <h3>Access</h3>
          <p>Signals: {"Unlocked" if can_access_signals() else "Locked"}</p>
          <p>Forecast: {"Unlocked" if can_access_forecast() else "Locked"}</p>
          <p>Trends: {"Unlocked" if can_access_trends() else "Locked"}</p>
        </div>

        <div class="dashboard-card">
          <h3>API Key</h3>
          <div class="key">{user['api_key']}</div>
        </div>
      </div>

      <div class="btns">
        {billing_button}
        <a class="btn btn-secondary" href="/logout">Logout</a>
      </div>

      <div class="section" style="padding-top:30px;">
        <h2 class="section-title">Recent Predictions</h2>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Type</th><th>Confidence</th><th>Created</th></tr>
            {rows}
          </table>
        </div>
      </div>
    </section>
    """
    return nav_layout("Dashboard - AVA Markets", content)


@app.route("/checkout/<tier>", methods=["POST"])
@require_login
def checkout(tier):
    tier = tier.lower()
    if tier not in ["basic", "pro", "elite"]:
        return redirect("/pricing")

    success_url = f"{Config.DOMAIN}/dashboard?checkout=success"
    cancel_url = f"{Config.DOMAIN}/pricing?checkout=cancel"

    session = sm.create_checkout(g.user["id"], g.user["email"], tier, success_url, cancel_url)
    if session and getattr(session, "url", None):
        return redirect(session.url)

    db.update_user(g.user["id"], tier=tier, subscription_status="active")
    db.log_payment(g.user["id"], "manual", f"local_{tier}_{int(time.time())}", Config.TIERS[tier]["price"], "succeeded")
    return redirect("/dashboard")


@app.route("/billing/portal", methods=["POST"])
@require_login
def billing_portal():
    if not g.user.get("stripe_customer_id"):
        return redirect("/dashboard")
    session = sm.create_portal(g.user["stripe_customer_id"], f"{Config.DOMAIN}/dashboard")
    if session and getattr(session, "url", None):
        return redirect(session.url)
    return redirect("/dashboard")


@app.route("/webhook/stripe", methods=["POST"])
def stripe_webhook():
    payload = request.data
    sig = request.headers.get("Stripe-Signature")

    if stripe and Config.STRIPE_WEBHOOK_SECRET:
        try:
            event = stripe.Webhook.construct_event(payload, sig, Config.STRIPE_WEBHOOK_SECRET)
        except Exception as e:
            logger.error(f"Bad Stripe signature: {e}")
            return jsonify({"error": "Bad signature"}), 400
    else:
        try:
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8")
            event = json.loads(payload)
        except Exception as e:
            logger.error(f"Bad payload: {e}")
            return jsonify({"error": "Bad payload"}), 400

    etype = event.get("type", "")
    obj = event.get("data", {}).get("object", {})

    if etype == "checkout.session.completed":
        meta = obj.get("metadata", {})
        uid = int(meta.get("user_id", 0))
        tier = meta.get("tier", "basic")
        if uid:
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
def admin_login():
    if request.cookies.get("admin_auth") == "1":
        return redirect("/admin")

    error = ""
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == Config.ADMIN_USERNAME and password == Config.ADMIN_PASSWORD:
            resp = make_response(redirect("/admin"))
            resp.set_cookie("admin_auth", "1", httponly=True, samesite="Lax", max_age=60 * 60 * 12)
            return resp
        error = "Invalid admin credentials."

    content = f"""
    <div class="form-shell">
      <div class="form-card">
        <h1>Admin Login</h1>
        <p>Restricted access.</p>
        {'<div class="error">' + error + '</div>' if error else ''}
        <form method="POST">
          <input type="text" name="username" placeholder="Username" required>
          <input type="password" name="password" placeholder="Password" required>
          <button type="submit">Enter Admin</button>
        </form>
      </div>
    </div>
    """
    return nav_layout("Admin Login - AVA Markets", content)


@app.route("/admin/logout")
def admin_logout():
    resp = make_response(redirect("/"))
    resp.delete_cookie("admin_auth")
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
          <td>{u['id']}</td>
          <td>{u['email']}</td>
          <td>{u['tier']}</td>
          <td>{u['subscription_status']}</td>
          <td>{u['created_at']}</td>
        </tr>
        """

    payment_rows = ""
    for p in payments:
        payment_rows += f"""
        <tr>
          <td>{p.get('email') or '-'}</td>
          <td>{p['provider']}</td>
          <td>{p['payment_id']}</td>
          <td>{p['amount']}</td>
          <td>{p['status']}</td>
          <td>{p['created_at']}</td>
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
    return nav_layout("Admin - AVA Markets", content)


@app.errorhandler(404)
def not_found(e):
    return nav_layout("404", "<section class='section'><div class='card'><h1>404</h1><p>Page not found.</p></div></section>"), 404


@app.errorhandler(500)
def server_error(e):
    logger.exception("Unhandled server error")
    return nav_layout("500", "<section class='section'><div class='card'><h1>500</h1><p>Internal server error.</p></div></section>"), 500


if __name__ == "__main__":
    snapshot_crypto_quotes()
    snapshot_stock_quotes()
    start_snapshot_worker()
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)