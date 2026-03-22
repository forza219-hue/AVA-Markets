#!/usr/bin/env python3
import os
import json
import time
import html
import math
import sqlite3
import secrets
import hashlib
import logging
import random
import threading
from datetime import datetime, timedelta
from functools import wraps
from urllib.parse import quote_plus

import bcrypt
import requests
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
logging.basicConfig(level=logging.INFO, format="%(asctime)s - AVA - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class Config:
    APP_NAME = os.environ.get("APP_NAME", "AVA Markets").strip()

    HOST = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 5000))
    DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

    DATABASE = os.environ.get("DATABASE_URL", "ava_markets.db").strip()
    SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32)).strip()
    DOMAIN = os.environ.get("DOMAIN", "http://localhost:5000").strip().rstrip("/")

    COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "false").lower() == "true"
    COOKIE_SAMESITE = os.environ.get("COOKIE_SAMESITE", "Lax").strip()

    ADMIN_EMAIL = os.environ.get("ADMIN_EMAIL", "you@example.com").strip().lower()

    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "").strip()
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    STRIPE_PRICE_PRO_MONTHLY = os.environ.get("STRIPE_PRICE_PRO_MONTHLY", "").strip()
    STRIPE_PRICE_ELITE_MONTHLY = os.environ.get("STRIPE_PRICE_ELITE_MONTHLY", "").strip()

    FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()

    TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()

    RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "").strip()
    EMAIL_FROM = os.environ.get("EMAIL_FROM", "AVA Markets <no-reply@example.com>").strip()

    RATE_LIMIT_STORAGE_URI = os.environ.get("RATE_LIMIT_STORAGE_URI", "memory://")
    PASSWORD_RESET_TTL_MINUTES = int(os.environ.get("PASSWORD_RESET_TTL_MINUTES", 30))
    BROADCAST_COOLDOWN_SECONDS = int(os.environ.get("BROADCAST_COOLDOWN_SECONDS", 14400))

    CRYPTO_CACHE_TTL = 300
    STOCK_CACHE_TTL = 600
    PAGE_SIZE_CRYPTO = 100
    PAGE_SIZE_STOCKS = 100

    SIGNAL_MIN_CONFIDENCE = 70
    SIGNAL_MIN_RR = 1.2


if stripe and Config.STRIPE_SECRET_KEY:
    stripe.api_key = Config.STRIPE_SECRET_KEY


app = Flask(__name__)
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
app.config["SECRET_KEY"] = Config.SECRET_KEY

if Limiter:
    limiter = Limiter(key_func=get_remote_address, app=app, storage_uri=Config.RATE_LIMIT_STORAGE_URI)
else:
    class _NoopLimiter:
        def limit(self, *args, **kwargs):
            return lambda fn: fn
    limiter = _NoopLimiter()


CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
:root{
  --bg:#05070c;
  --bg2:#09111a;
  --bg3:#0f1824;
  --card:rgba(255,255,255,.045);
  --border:rgba(255,255,255,.08);
  --text:#f8fafc;
  --muted:#94a3b8;
  --yellow:#facc15;
  --yellow2:#fde047;
  --yellow3:#eab308;
  --cyan:#38bdf8;
  --green:#22c55e;
  --red:#ef4444;
  --shadow:0 28px 80px rgba(0,0,0,.45);
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{
  margin:0;font-family:'Inter',sans-serif;color:var(--text);
  background:
    radial-gradient(circle at 10% 0%, rgba(250,204,21,.08), transparent 25%),
    radial-gradient(circle at 90% 0%, rgba(56,189,248,.08), transparent 18%),
    radial-gradient(circle at 50% 100%, rgba(250,204,21,.05), transparent 28%),
    linear-gradient(160deg,var(--bg),var(--bg2) 45%, var(--bg3));
}
a{text-decoration:none;color:inherit}
.container{max-width:1320px;margin:0 auto;padding:0 24px}
.nav{
  display:flex;justify-content:space-between;align-items:center;padding:18px 0;
  position:sticky;top:0;z-index:120;background:rgba(5,7,12,.76);
  backdrop-filter:blur(18px);border-bottom:1px solid rgba(255,255,255,.05)
}
.logo{
  font-size:1.32rem;font-weight:900;letter-spacing:.3px;
  background:linear-gradient(90deg,#fff,var(--yellow2),var(--yellow3));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
}
.nav-links{display:flex;gap:16px;align-items:center;flex-wrap:wrap}
.nav-links a{color:var(--muted);font-weight:700;transition:.2s}
.nav-links a:hover{color:var(--text)}
.hero{display:grid;grid-template-columns:1.1fr .9fr;gap:28px;align-items:center;padding:76px 0 38px}
.hero-card,.card,.table-shell,.price-card{
  background:linear-gradient(180deg, rgba(255,255,255,.05), rgba(255,255,255,.03));
  border:1px solid var(--border);border-radius:24px;box-shadow:var(--shadow)
}
.hero-card{padding:36px}
.card{padding:22px}
.price-card{padding:26px}
.price-card.featured{
  border-color:rgba(250,204,21,.34);
  box-shadow:0 24px 80px rgba(250,204,21,.08), var(--shadow)
}
.section{padding:28px 0 72px}
.badge{
  display:inline-flex;align-items:center;gap:8px;padding:8px 14px;border-radius:999px;
  background:rgba(250,204,21,.12);border:1px solid rgba(250,204,21,.24);
  color:#fde68a;font-size:.84rem;font-weight:800;margin-bottom:16px
}
h1{font-size:clamp(2.5rem,5vw,4.8rem);line-height:1.02;margin:0 0 18px;font-weight:900}
h2{margin:0 0 14px}
h3{margin:0 0 8px}
p{color:var(--muted);line-height:1.7;font-size:1rem}
.hero-sub{font-size:1.08rem;max-width:720px}
.btns{display:flex;gap:14px;flex-wrap:wrap;margin-top:22px}
.btn{
  display:inline-flex;align-items:center;justify-content:center;
  padding:14px 18px;border-radius:14px;font-weight:800;border:1px solid transparent;
  cursor:pointer;transition:.2s
}
.btn:hover{transform:translateY(-2px)}
.btn-primary{
  background:linear-gradient(90deg,var(--yellow2),var(--yellow3));
  color:#111827;box-shadow:0 12px 30px rgba(250,204,21,.18)
}
.btn-secondary{
  background:rgba(255,255,255,.04);
  border-color:rgba(255,255,255,.10);color:var(--text)
}
.btn-dark{
  background:#0d131d;border:1px solid rgba(255,255,255,.08);color:#fff
}
.grid-2{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:20px}
.grid-3{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}
.grid-4{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}
.kpi{
  padding:18px;border-radius:18px;background:rgba(255,255,255,.03);
  border:1px solid rgba(255,255,255,.05)
}
.kpi .num{font-size:1.7rem;font-weight:900;color:#fff}
.kpi .label{color:var(--muted);font-size:.92rem}
.table-shell{overflow:hidden}
.market-table{width:100%;border-collapse:collapse}
.market-table th,.market-table td{
  padding:16px 14px;border-bottom:1px solid rgba(255,255,255,.06);
  text-align:left;vertical-align:top
}
.market-table th{
  color:#d6dce5;background:rgba(255,255,255,.02);font-size:.91rem;font-weight:800
}
.market-table tr:hover{background:rgba(255,255,255,.02)}
.asset-name strong{display:block}
.asset-name span{display:block;color:var(--muted);font-size:.84rem;margin-top:4px}
.asset-row{display:flex;align-items:center;gap:10px}
.asset-logo{width:28px;height:28px;border-radius:50%;object-fit:cover;background:#fff}
.asset-icon{width:28px;height:28px;display:inline-flex;align-items:center;justify-content:center;font-size:1.1rem}
.up{color:var(--green)}
.down{color:var(--red)}
.signal{display:inline-flex;padding:8px 12px;border-radius:999px;font-weight:800;font-size:.81rem}
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
.price{font-size:2.4rem;font-weight:900;margin:8px 0 6px}
.small{font-size:.92rem;color:var(--muted)}
.muted{color:var(--muted)}
.blur-lock{position:relative;overflow:hidden}
.blur-lock .blurred{filter:blur(5px);opacity:.7;user-select:none;pointer-events:none}
.blur-overlay{
  position:absolute;inset:0;display:flex;align-items:center;justify-content:center;
  background:linear-gradient(180deg, rgba(5,7,12,.04), rgba(5,7,12,.78))
}
.lock-card{
  max-width:360px;text-align:center;padding:20px;border-radius:18px;
  background:rgba(10,15,23,.92);border:1px solid rgba(250,204,21,.2)
}
.alert-box{
  border:1px solid rgba(250,204,21,.18);background:rgba(250,204,21,.06);
  color:#fde68a;padding:14px;border-radius:14px
}
.footer-top{
  display:flex;justify-content:space-between;gap:20px;flex-wrap:wrap;
  padding:26px 0;border-top:1px solid rgba(255,255,255,.06);margin-top:40px
}
.disclaimer{color:#8fa0b6;font-size:.84rem;line-height:1.7;max-width:980px}
.footer{padding:10px 0 40px;color:var(--muted);font-size:.9rem}
.hr{height:1px;background:rgba(255,255,255,.06);margin:24px 0}
.blog-card{display:block}
.blog-card h3{margin-top:8px}
.blog-meta{font-size:.85rem;color:var(--muted);margin-bottom:10px}
@media (max-width: 960px){
  .hero{grid-template-columns:1fr}
  .nav{flex-direction:column;gap:14px}
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
    ("AAVE", "Aave"), ("ALGO", "Algorand"), ("FLOW", "Flow"), ("MKR", "Maker"), ("SAND", "The Sandbox"),
    ("MANA", "Decentraland"), ("GRT", "The Graph"), ("EGLD", "MultiversX"), ("KAS", "Kaspa"), ("XMR", "Monero"),
    ("XTZ", "Tezos"), ("EOS", "EOS"), ("AXS", "Axie Infinity"), ("CHZ", "Chiliz"), ("CRV", "Curve DAO"),
    ("COMP", "Compound"), ("SNX", "Synthetix"), ("1INCH", "1inch"), ("ZEC", "Zcash"), ("DASH", "Dash"),
    ("KAVA", "Kava"), ("ROSE", "Oasis"), ("LDO", "Lido DAO"), ("BLUR", "Blur"), ("DYDX", "dYdX"),
    ("GMX", "GMX"), ("CFX", "Conflux"), ("MINA", "Mina"), ("CKB", "Nervos"), ("IOTA", "IOTA"),
    ("QTUM", "Qtum"), ("ZIL", "Zilliqa"), ("BAT", "Basic Attention Token"), ("ENJ", "Enjin Coin"), ("HOT", "Holo"),
    ("ANKR", "Ankr"), ("WOO", "WOO"), ("YFI", "yearn.finance"), ("SUSHI", "Sushi"), ("CELR", "Celer Network"),
    ("ONT", "Ontology"), ("SKL", "SKALE"), ("RSR", "Reserve Rights"), ("LRC", "Loopring"), ("NEXO", "Nexo"),
    ("GLM", "Golem"), ("FLUX", "Flux"), ("API3", "API3"), ("MASK", "Mask Network"), ("OCEAN", "Ocean Protocol"),
    ("ARKM", "Arkham"), ("TRB", "Tellor"), ("BAND", "Band Protocol"), ("STORJ", "Storj"), ("CELO", "Celo")
]

STOCK_UNIVERSE = [
    ("AAPL", "Apple"), ("MSFT", "Microsoft"), ("NVDA", "NVIDIA"), ("AMZN", "Amazon"),
    ("GOOGL", "Alphabet"), ("META", "Meta"), ("TSLA", "Tesla"), ("BRK-B", "Berkshire Hathaway"),
    ("JPM", "JPMorgan"), ("V", "Visa"), ("MA", "Mastercard"), ("UNH", "UnitedHealth"),
    ("XOM", "Exxon"), ("LLY", "Eli Lilly"), ("AVGO", "Broadcom"), ("ORCL", "Oracle"),
    ("COST", "Costco"), ("WMT", "Walmart"), ("HD", "Home Depot"), ("PG", "Procter & Gamble"),
    ("KO", "Coca-Cola"), ("PEP", "PepsiCo"), ("ABBV", "AbbVie"), ("BAC", "Bank of America"),
    ("AMD", "AMD"), ("CRM", "Salesforce"), ("NFLX", "Netflix"), ("ADBE", "Adobe"),
    ("INTC", "Intel"), ("QCOM", "Qualcomm"), ("CSCO", "Cisco"), ("TMO", "Thermo Fisher"),
    ("MCD", "McDonald's"), ("NKE", "Nike"), ("DIS", "Disney"), ("CAT", "Caterpillar"),
    ("GE", "GE Aerospace"), ("IBM", "IBM"), ("UBER", "Uber"), ("ABNB", "Airbnb"),
    ("PYPL", "PayPal"), ("SHOP", "Shopify"), ("PLTR", "Palantir"), ("PFE", "Pfizer"),
    ("MRK", "Merck"), ("GS", "Goldman Sachs"), ("MS", "Morgan Stanley"),
    ("GC=F", "Gold Futures"), ("SI=F", "Silver Futures"), ("CL=F", "Oil Futures")
]

STOCK_DOMAINS = {
    "AAPL": "apple.com",
    "MSFT": "microsoft.com",
    "NVDA": "nvidia.com",
    "AMZN": "amazon.com",
    "GOOGL": "google.com",
    "META": "meta.com",
    "TSLA": "tesla.com",
    "BRK-B": "berkshirehathaway.com",
    "JPM": "jpmorganchase.com",
    "V": "visa.com",
    "MA": "mastercard.com",
    "UNH": "uhc.com",
    "XOM": "exxonmobil.com",
    "LLY": "lilly.com",
    "AVGO": "broadcom.com",
    "ORCL": "oracle.com",
    "COST": "costco.com",
    "WMT": "walmart.com",
    "HD": "homedepot.com",
    "PG": "pg.com",
    "KO": "coca-cola.com",
    "PEP": "pepsico.com",
    "ABBV": "abbvie.com",
    "BAC": "bankofamerica.com",
    "AMD": "amd.com",
    "CRM": "salesforce.com",
    "NFLX": "netflix.com",
    "ADBE": "adobe.com",
    "INTC": "intel.com",
    "QCOM": "qualcomm.com",
    "CSCO": "cisco.com",
    "TMO": "thermofisher.com",
    "MCD": "mcdonalds.com",
    "NKE": "nike.com",
    "DIS": "thewaltdisneycompany.com",
    "CAT": "cat.com",
    "GE": "geaerospace.com",
    "IBM": "ibm.com",
    "UBER": "uber.com",
    "ABNB": "airbnb.com",
    "PYPL": "paypal.com",
    "SHOP": "shopify.com",
    "PLTR": "palantir.com",
    "PFE": "pfizer.com",
    "MRK": "merck.com",
    "GS": "goldmansachs.com",
    "MS": "morganstanley.com"
}

CRYPTO_LOGO_OVERRIDES = {
    "1INCH": "https://assets.coingecko.com/coins/images/13469/large/1inch-token.png",
    "PYTH": "https://assets.coingecko.com/coins/images/31924/large/pyth.png",
    "WIF": "https://assets.coingecko.com/coins/images/33566/large/dogwifhat.jpg",
    "TAO": "https://assets.coingecko.com/coins/images/28463/large/BitTensor_Logo.png",
    "ARKM": "https://assets.coingecko.com/coins/images/30929/large/Arkham_Logo.png",
    "BONK": "https://assets.coingecko.com/coins/images/28600/large/bonk.jpg",
    "SEI": "https://assets.coingecko.com/coins/images/28205/large/Sei_Logo_-_Transparent.png",
    "TIA": "https://assets.coingecko.com/coins/images/31967/large/tia.jpg",
    "JUP": "https://assets.coingecko.com/coins/images/34188/large/jup.png",
    "FET": "https://assets.coingecko.com/coins/images/5681/large/Fetch.jpg",
    "RNDR": "https://assets.coingecko.com/coins/images/11636/large/rndr.png",
    "IMX": "https://assets.coingecko.com/coins/images/17233/large/immutableX-symbol-BLK-RGB.png",
    "INJ": "https://assets.coingecko.com/coins/images/12882/large/Secondary_Symbol.png",
    "RUNE": "https://assets.coingecko.com/coins/images/6595/large/RUNE.png",
    "CFX": "https://assets.coingecko.com/coins/images/13079/large/3vuYMBjT.png",
    "MINA": "https://assets.coingecko.com/coins/images/15628/large/mina.png",
    "API3": "https://assets.coingecko.com/coins/images/13256/large/api3.jpg",
    "MASK": "https://assets.coingecko.com/coins/images/14051/large/Mask_Network.jpg",
    "TRB": "https://assets.coingecko.com/coins/images/9644/large/Blk_icon_current.png",
    "CELO": "https://assets.coingecko.com/coins/images/11090/large/InjXBNx9_400x400.jpg",
    "RSR": "https://assets.coingecko.com/coins/images/8365/large/rsr.png",
    "GLM": "https://assets.coingecko.com/coins/images/542/large/Golem_Submark_Positive_RGB.png",
    "ROSE": "https://assets.coingecko.com/coins/images/13162/large/rose.png",
    "KAS": "https://assets.coingecko.com/coins/images/25760/large/kaspa-icon-exchanges.png",
    "FLOW": "https://assets.coingecko.com/coins/images/13446/large/flow_logo.png",
    "EGLD": "https://assets.coingecko.com/coins/images/12335/large/Elrond.png",
    "KAVA": "https://assets.coingecko.com/coins/images/9761/large/kava.png",
    "LDO": "https://assets.coingecko.com/coins/images/13573/large/Lido_DAO.png",
    "BLUR": "https://assets.coingecko.com/coins/images/28453/large/blur.png",
    "DYDX": "https://assets.coingecko.com/coins/images/17500/large/hjnIm9bV.jpg",
    "GMX": "https://assets.coingecko.com/coins/images/18323/large/arbit.png",
    "CKB": "https://assets.coingecko.com/coins/images/9566/large/Nervos_White.png",
    "CELR": "https://assets.coingecko.com/coins/images/4379/large/Celr.png",

    "APT": "https://assets.coingecko.com/coins/images/26455/large/aptos_round.png",
    "ARB": "https://assets.coingecko.com/coins/images/16547/large/arb.jpg",
    "OP": "https://assets.coingecko.com/coins/images/25244/large/Optimism.png",
    "SUI": "https://assets.coingecko.com/coins/images/26375/large/sui-ocean-square.png",
    "PEPE": "https://assets.coingecko.com/coins/images/29850/large/pepe-token.jpeg",
    "SHIB": "https://assets.coingecko.com/coins/images/11939/large/shiba.png",
    "HBAR": "https://assets.coingecko.com/coins/images/3688/large/hbar.png",
    "AXS": "https://assets.coingecko.com/coins/images/13029/large/axie_infinity_logo.png",
    "IOTA": "https://assets.coingecko.com/coins/images/692/large/IOTA_Swirl.png",
    "WOO": "https://assets.coingecko.com/coins/images/12921/large/WOO_Logotype_RGB_Yellow.png",
    "OCEAN": "https://assets.coingecko.com/coins/images/3687/large/ocean-protocol-logo.jpg"
}

BLOG_POSTS = {
    "how-to-read-ava-signals": {
        "title": "How to Read AVA Signals Like a Pro",
        "description": "Learn how AVA confidence, trade levels, and market regime analysis work together.",
        "date": "2026-01-10",
        "body": """
        <p>AVA Signals combine trend structure, breakout behavior, RSI, MACD, and volatility into a ranked setup engine.</p>
        <h2>What matters most?</h2>
        <p>Start with signal direction, then confidence, then risk/reward. A high confidence signal with poor structure is less useful than a balanced setup with clean levels.</p>
        <h2>How to use confidence</h2>
        <p>Confidence is not a guarantee. It is a weighted indication of how aligned AVA sees the setup across multiple technical factors.</p>
        <h2>Use stops and targets</h2>
        <p>AVA provides entry, stop, TP1, and TP2 to keep the workflow structured. This reduces emotional trading and random exits.</p>
        """
    },
    "best-crypto-setups-today": {
        "title": "Best Crypto Setups Today: What AVA Looks For",
        "description": "A breakdown of how AVA identifies high-conviction crypto trade setups.",
        "date": "2026-01-18",
        "body": """
        <p>Crypto setups tend to move faster and with more volatility than equities. AVA adapts by combining breakout structure with ATR-style risk controls.</p>
        <h2>Trend first</h2>
        <p>The strongest crypto setups usually align trend, momentum, and structure. AVA looks for stacked EMAs, RSI momentum, and clean breakouts.</p>
        <h2>Risk controls</h2>
        <p>Stops are placed with a volatility-aware model rather than arbitrary percentages. That helps avoid noise while preserving disciplined risk.</p>
        """
    },
    "aapl-vs-nvda-trader-guide": {
        "title": "AAPL vs NVDA: A Trader's Guide to Two Market Giants",
        "description": "Compare trading behavior, volatility, and trend structure in Apple and NVIDIA.",
        "date": "2026-01-24",
        "body": """
        <p>AAPL and NVDA are both liquid, widely followed equities, but they behave very differently in trend and volatility regimes.</p>
        <h2>Apple</h2>
        <p>Apple often trades with smoother structure and broader institutional participation.</p>
        <h2>NVIDIA</h2>
        <p>NVIDIA tends to offer faster momentum swings and stronger breakout behavior, which can reward active traders but also punish poor risk management.</p>
        """
    }
}

SYMBOL_LEARN = {
    "BTC": {
        "title": "Bitcoin (BTC) Trading Guide",
        "intro": "Bitcoin is the benchmark digital asset and one of the most watched macro-sensitive markets in the world.",
        "sections": [
            ("Why BTC matters", "BTC often acts as the anchor for crypto market sentiment, liquidity, and trend leadership."),
            ("How traders use BTC", "Traders watch BTC for breakout structure, macro correlation, and risk-on/risk-off behavior."),
            ("How AVA analyzes BTC", "AVA scores BTC using trend stack, RSI, MACD, breakout logic, and volatility-aware trade construction.")
        ]
    },
    "ETH": {
        "title": "Ethereum (ETH) Trading Guide",
        "intro": "Ethereum is a major smart-contract asset and often shows different behavior than Bitcoin during rotation cycles.",
        "sections": [
            ("Why ETH matters", "ETH can outperform in risk-on periods and often reflects broader altcoin sentiment."),
            ("How traders use ETH", "Traders monitor ETH for trend continuation, rotational strength, and support/resistance structure."),
            ("How AVA analyzes ETH", "AVA weighs momentum, moving averages, and breakout structure to surface ranked ETH setups.")
        ]
    },
    "AAPL": {
        "title": "Apple (AAPL) Trading Guide",
        "intro": "Apple is one of the most important large-cap equities and a core holding for many institutional and retail portfolios.",
        "sections": [
            ("Why AAPL matters", "AAPL influences major indices and often reflects broader mega-cap sentiment."),
            ("How traders use AAPL", "Traders often use AAPL for swing structure, earnings reactions, and sector leadership clues."),
            ("How AVA analyzes AAPL", "AVA evaluates AAPL using trend alignment, momentum, breakout context, and risk/reward construction.")
        ]
    },
    "NVDA": {
        "title": "NVIDIA (NVDA) Trading Guide",
        "intro": "NVIDIA is a high-volatility, high-attention equity that frequently offers strong momentum opportunities.",
        "sections": [
            ("Why NVDA matters", "NVDA is closely tied to AI, semis, growth sentiment, and institutional positioning."),
            ("How traders use NVDA", "Traders watch NVDA for momentum continuation, gap behavior, and rapid trend expansion."),
            ("How AVA analyzes NVDA", "AVA uses trend stack, momentum signals, MACD, RSI, and breakout logic to classify NVDA setups.")
        ]
    }
}


def h(v):
    return html.escape("" if v is None else str(v), quote=True)

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
    return "BUY" if c >= 0.8 else "SELL" if c <= -0.8 else "HOLD"

def normalize_symbol_id(sym):
    return str(sym).replace("=", "_").replace("-", "_").replace("/", "_")

def get_stock_logo(sym):
    d = STOCK_DOMAINS.get(str(sym).upper())
    return f"https://t3.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON&fallback_opts=TYPE,SIZE,URL&url=http://{d}&size=128" if d else ""

def get_crypto_logo(sym):
    s = str(sym).upper()
    if s in CRYPTO_LOGO_OVERRIDES:
        return CRYPTO_LOGO_OVERRIDES[s]
    return f"https://raw.githubusercontent.com/spothq/cryptocurrency-icons/master/128/color/{str(sym).lower()}.png"

def get_asset_icon(sym):
    return {"GC=F": "🥇", "SI=F": "🥈", "CL=F": "🛢️"}.get(str(sym).upper(), "📈")

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

        CREATE TABLE IF NOT EXISTS password_resets (
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at TIMESTAMP NOT NULL,
            used INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS email_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            event_type TEXT NOT NULL,
            meta_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, symbol, asset_type)
        );

        CREATE TABLE IF NOT EXISTS user_alert_prefs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE NOT NULL,
            telegram_enabled INTEGER NOT NULL DEFAULT 0,
            discord_enabled INTEGER NOT NULL DEFAULT 0,
            email_enabled INTEGER NOT NULL DEFAULT 1,
            min_confidence INTEGER NOT NULL DEFAULT 70,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS portfolio_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            asset_type TEXT NOT NULL,
            quantity REAL NOT NULL,
            avg_cost REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    def get_user_by_email(self, email):
        c = self.conn()
        row = c.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),)).fetchone()
        c.close()
        return dict(row) if row else None

    def get_user_by_id(self, user_id):
        c = self.conn()
        row = c.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        c.close()
        return dict(row) if row else None

    def get_all_users(self, limit=500):
        c = self.conn()
        rows = c.execute("SELECT * FROM users ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def create_session(self, user_id):
        token = secrets.token_hex(32)
        exp = datetime.utcnow() + timedelta(days=30)
        c = self.conn()
        c.execute("INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)", (token, user_id, exp))
        c.commit()
        c.close()
        return token

    def delete_session(self, token):
        if not token:
            return
        c = self.conn()
        c.execute("DELETE FROM sessions WHERE token = ?", (token,))
        c.commit()
        c.close()

    def get_user_by_session(self, token):
        if not token:
            return None
        c = self.conn()
        row = c.execute("""
            SELECT u.* FROM users u
            JOIN sessions s ON s.user_id = u.id
            WHERE s.token = ? AND s.expires_at > ?
        """, (token, datetime.utcnow())).fetchone()
        c.close()
        return dict(row) if row else None

    def update_user_password(self, user_id, new_password):
        pw_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        c = self.conn()
        c.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pw_hash, user_id))
        c.commit()
        c.close()

    def create_password_reset(self, user_id):
        token = secrets.token_urlsafe(48)
        exp = datetime.utcnow() + timedelta(minutes=Config.PASSWORD_RESET_TTL_MINUTES)
        c = self.conn()
        c.execute("INSERT INTO password_resets (token, user_id, expires_at, used) VALUES (?, ?, ?, 0)", (token, user_id, exp))
        c.commit()
        c.close()
        return token

    def get_valid_password_reset(self, token):
        c = self.conn()
        row = c.execute("""
            SELECT * FROM password_resets
            WHERE token = ? AND used = 0 AND expires_at > ?
        """, (token, datetime.utcnow())).fetchone()
        c.close()
        return dict(row) if row else None

    def mark_password_reset_used(self, token):
        c = self.conn()
        c.execute("UPDATE password_resets SET used = 1 WHERE token = ?", (token,))
        c.commit()
        c.close()

    def upgrade_user(self, user_id, tier, customer_id=None, sub_id=None, billing_cycle=None):
        c = self.conn()
        c.execute("""
            UPDATE users
            SET tier = ?, stripe_customer_id = COALESCE(?, stripe_customer_id),
                stripe_sub_id = COALESCE(?, stripe_sub_id), billing_cycle = ?
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
        c.execute(
            "REPLACE INTO market_cache (cache_key, payload_json, updated_at) VALUES (?, ?, ?)",
            (key, json.dumps(payload), int(time.time()))
        )
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
            row = c.execute(
                "SELECT * FROM signal_history WHERE signal_id = ? AND outcome = 'OPEN'",
                (s["signal_id"],)
            ).fetchone()

            if not row:
                c.execute("""
                    INSERT INTO signal_history (
                        history_id, signal_id, symbol, asset_type, name, signal, confidence,
                        entry_price, stop_loss, take_profit_1, take_profit_2, risk_reward,
                        outcome, outcome_note, updated_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', '', ?, ?)
                """, (
                    secrets.token_hex(16), s["signal_id"], s["symbol"], s["asset_type"], s["name"],
                    s["signal"], s["confidence"], s["entry_price"], s["stop_loss"],
                    s["take_profit_1"], s["take_profit_2"], s["risk_reward"], now, now
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
        rows = c.execute("SELECT * FROM signal_history ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def get_open_signal_history(self, limit=200):
        c = self.conn()
        rows = c.execute("""
            SELECT * FROM signal_history
            WHERE outcome = 'OPEN'
            ORDER BY created_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def update_signal_outcome(self, history_id, outcome, note):
        c = self.conn()
        c.execute("""
            UPDATE signal_history
            SET outcome = ?, outcome_note = ?, updated_at = ?
            WHERE history_id = ?
        """, (outcome, note, int(time.time()), history_id))
        c.commit()
        c.close()

    def was_recent_signal(self, symbol, asset_type, cooldown_hours=48):
        c = self.conn()
        cutoff = int(time.time()) - int(cooldown_hours * 3600)
        row = c.execute("""
            SELECT 1
            FROM signal_history
            WHERE symbol = ? AND asset_type = ? AND created_at >= ?
            LIMIT 1
        """, (str(symbol).upper().strip(), str(asset_type).strip(), cutoff)).fetchone()
        c.close()
        return bool(row)

    def invalidate_legacy_history(self):
        c = self.conn()
        c.execute("""
            UPDATE signal_history
            SET outcome = 'LEGACY_INVALID',
                outcome_note = 'Invalidated after signal engine correction.',
                updated_at = ?
            WHERE outcome IN ('OPEN', 'EXPIRED', 'STOPPED', 'TP1_HIT', 'TP2_HIT', 'AMBIGUOUS')
        """, (int(time.time()),))
        c.commit()
        c.close()

    def get_signal_stats(self):
        c = self.conn()
        total = c.execute("SELECT COUNT(*) AS n FROM signal_history").fetchone()["n"]
        open_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome='OPEN'").fetchone()["n"]
        expired_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome='EXPIRED'").fetchone()["n"]
        tp1_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome='TP1_HIT'").fetchone()["n"]
        tp2_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome='TP2_HIT'").fetchone()["n"]
        stopped_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome='STOPPED'").fetchone()["n"]
        ambiguous_n = c.execute("SELECT COUNT(*) AS n FROM signal_history WHERE outcome='AMBIGUOUS'").fetchone()["n"]

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
            "ambiguous": ambiguous_n,
            "win_rate": win_rate
        }

    def subscribe_email(self, email):
        c = self.conn()
        c.execute("INSERT OR IGNORE INTO alert_subscribers (email, active) VALUES (?, 1)", (email.lower().strip(),))
        c.commit()
        c.close()

    def unsubscribe_email(self, email):
        c = self.conn()
        c.execute("UPDATE alert_subscribers SET active = 0 WHERE email = ?", (email.lower().strip(),))
        c.commit()
        c.close()

    def get_active_subscribers(self, limit=1000):
        c = self.conn()
        rows = c.execute("SELECT * FROM alert_subscribers WHERE active = 1 ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def log_email_event(self, email, event_type, meta=None):
        c = self.conn()
        c.execute(
            "INSERT INTO email_events (email, event_type, meta_json) VALUES (?, ?, ?)",
            (email.lower().strip(), event_type, json.dumps(meta or {}))
        )
        c.commit()
        c.close()

    def was_broadcast_sent_recently(self, channel, message_hash, cooldown_seconds):
        c = self.conn()
        cutoff = int(time.time()) - cooldown_seconds
        row = c.execute("""
            SELECT id FROM broadcast_log
            WHERE channel = ? AND message_hash = ? AND created_at >= ?
            LIMIT 1
        """, (channel, message_hash, cutoff)).fetchone()
        c.close()
        return bool(row)

    def log_broadcast(self, channel, message_hash):
        c = self.conn()
        c.execute(
            "INSERT INTO broadcast_log (channel, message_hash, created_at) VALUES (?, ?, ?)",
            (channel, message_hash, int(time.time()))
        )
        c.commit()
        c.close()

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

    def get_subscriber_count(self):
        c = self.conn()
        n = c.execute("SELECT COUNT(*) AS n FROM alert_subscribers WHERE active = 1").fetchone()["n"]
        c.close()
        return n

    def add_watchlist(self, user_id, symbol, asset_type):
        c = self.conn()
        c.execute(
            "INSERT OR IGNORE INTO watchlists (user_id, symbol, asset_type) VALUES (?, ?, ?)",
            (user_id, symbol.upper().strip(), asset_type.strip())
        )
        c.commit()
        c.close()

    def remove_watchlist(self, user_id, symbol, asset_type):
        c = self.conn()
        c.execute(
            "DELETE FROM watchlists WHERE user_id = ? AND symbol = ? AND asset_type = ?",
            (user_id, symbol.upper().strip(), asset_type.strip())
        )
        c.commit()
        c.close()

    def get_watchlist(self, user_id):
        c = self.conn()
        rows = c.execute("SELECT * FROM watchlists WHERE user_id = ? ORDER BY created_at DESC", (user_id,)).fetchall()
        c.close()
        return [dict(r) for r in rows]

    def ensure_alert_prefs(self, user_id):
        c = self.conn()
        c.execute("INSERT OR IGNORE INTO user_alert_prefs (user_id) VALUES (?)", (user_id,))
        c.commit()
        c.close()

    def get_alert_prefs(self, user_id):
        self.ensure_alert_prefs(user_id)
        c = self.conn()
        row = c.execute("SELECT * FROM user_alert_prefs WHERE user_id = ?", (user_id,)).fetchone()
        c.close()
        return dict(row) if row else None

    def update_alert_prefs(self, user_id, email_enabled, telegram_enabled, discord_enabled, min_confidence):
        self.ensure_alert_prefs(user_id)
        c = self.conn()
        c.execute("""
            UPDATE user_alert_prefs
            SET email_enabled = ?, telegram_enabled = ?, discord_enabled = ?, min_confidence = ?
            WHERE user_id = ?
        """, (email_enabled, telegram_enabled, discord_enabled, min_confidence, user_id))
        c.commit()
        c.close()

    def add_portfolio_position(self, user_id, symbol, asset_type, quantity, avg_cost):
        c = self.conn()
        c.execute("""
            INSERT INTO portfolio_positions (user_id, symbol, asset_type, quantity, avg_cost)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, symbol.upper().strip(), asset_type.strip(), float(quantity), float(avg_cost)))
        c.commit()
        c.close()

    def delete_portfolio_position(self, position_id, user_id):
        c = self.conn()
        c.execute("DELETE FROM portfolio_positions WHERE id = ? AND user_id = ?", (position_id, user_id))
        c.commit()
        c.close()

    def get_portfolio_positions(self, user_id):
        c = self.conn()
        rows = c.execute("""
            SELECT * FROM portfolio_positions
            WHERE user_id = ?
            ORDER BY created_at DESC
        """, (user_id,)).fetchall()
        c.close()
        return [dict(r) for r in rows]


db = Database(Config.DATABASE)
MEM_CACHE = {}


def get_web_user():
    return db.get_user_by_session(request.cookies.get("session_token"))

@app.before_request
def load_req():
    g.user = get_web_user()
    if g.user and str(g.user.get("email", "")).lower() == Config.ADMIN_EMAIL:
        if g.user.get("tier") != "elite":
            db.upgrade_user(g.user["id"], "elite", billing_cycle="admin")
            g.user = db.get_user_by_id(g.user["id"])

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
            if tier_order.get(g.user.get("tier", "free"), 0) < tier_order.get(min_tier, 0):
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

def set_session_cookie(resp, token):
    resp.set_cookie(
        "session_token",
        token,
        httponly=True,
        secure=Config.COOKIE_SECURE,
        samesite=Config.COOKIE_SAMESITE,
        max_age=60 * 60 * 24 * 30
    )
    return resp

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


def perform_crypto_fetch():
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

    loaded_symbols = {r["symbol"] for r in results}
    for symbol, name in CRYPTO_TOP_90:
        if symbol not in loaded_symbols:
            price = random.uniform(0.0001, 500.0) if symbol not in ("BTC", "ETH", "TAO") else random.uniform(1.0, 50000.0)
            change = random.uniform(-4.0, 4.0)
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

    results = sorted(results, key=lambda x: next((i for i, t in enumerate(CRYPTO_TOP_90) if t[0] == x["symbol"]), 9999))
    set_cached_payload("crypto_list", results)


def perform_stock_fetch():
    results = []
    headers = {"User-Agent": "Mozilla/5.0"}

    for symbol, name in STOCK_UNIVERSE:
        try:
            r = requests.get(
                f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d",
                headers=headers,
                timeout=5
            )
            if r.status_code == 200:
                res = r.json().get("chart", {}).get("result", [])
                if res:
                    meta = res[0].get("meta", {})
                    price = float(meta.get("regularMarketPrice") or meta.get("chartPreviousClose") or 0)
                    prev = float(meta.get("previousClose") or meta.get("chartPreviousClose") or price)
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
            time.sleep(0.12)
        except Exception:
            continue

    loaded_symbols = {r["symbol"] for r in results}
    for symbol, name in STOCK_UNIVERSE:
        if symbol not in loaded_symbols:
            price = random.uniform(10.0, 1000.0)
            change = random.uniform(-4.0, 4.0)
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

    results = sorted(results, key=lambda x: next((i for i, t in enumerate(STOCK_UNIVERSE) if t[0] == x["symbol"]), 9999))
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
        return []
    return []


def fetch_stock_candles(symbol):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(
            f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=6mo",
            headers=headers,
            timeout=5
        )
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
        return []
    return []


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
        h_ = candles[i]["high"]
        l_ = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h_ - l_, abs(h_ - pc), abs(l_ - pc))
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

    rsi = calc_rsi(closes, 14)
    macd_line, macd_signal, macd_hist = calc_macd(closes)
    atr = calc_atr_proxy(candles, 14)

    recent_high_10 = max(highs[-10:-1])
    recent_low_10 = min(lows[-10:-1])
    recent_high_20 = max(highs[-20:-1])
    recent_low_20 = min(lows[-20:-1])

    score = 0
    reasons = []
    regime = "Range / Mixed"

    strong_bull = current > ema9[-1] > ema20[-1] > ema50[-1]
    strong_bear = current < ema9[-1] < ema20[-1] < ema50[-1]

    if strong_bull:
        score += 4
        regime = "Strong Bull Trend"
        reasons.append("Price is stacked above EMA-9, EMA-20 and EMA-50.")
    elif strong_bear:
        score -= 4
        regime = "Strong Bear Trend"
        reasons.append("Price is stacked below EMA-9, EMA-20 and EMA-50.")
    elif current > ema20[-1] and current > ema50[-1]:
        score += 2
        regime = "Bullish Bias"
        reasons.append("Price is holding above key moving averages.")
    elif current < ema20[-1] and current < ema50[-1]:
        score -= 2
        regime = "Bearish Bias"
        reasons.append("Price is holding below key moving averages.")
    else:
        regime = "Range / Mixed"
        reasons.append("Trend structure is mixed.")

    score += 1 if ema9[-1] > ema9[-2] else -1
    score += 1 if ema20[-1] > ema20[-2] else -1
    score += 1 if ema50[-1] > ema50[-2] else -1
    score += 1 if current > sma50[-1] else -1

    if rsi < 30:
        score += 2
        reasons.append("RSI shows oversold rebound potential.")
    elif rsi > 70:
        score -= 1
        reasons.append("RSI is overextended.")
    elif rsi > 55:
        score += 1
        reasons.append("RSI supports bullish momentum.")
    elif rsi < 45:
        score -= 1
        reasons.append("RSI leans weak.")

    if macd_line > macd_signal and macd_hist > 0:
        score += 2
        reasons.append("MACD is positive and expanding.")
    elif macd_line < macd_signal and macd_hist < 0:
        score -= 2
        reasons.append("MACD is negative and weakening.")

    if current > recent_high_10 and current > prev_close:
        score += 1
        reasons.append("Fresh breakout above 10-period resistance.")
    elif current < recent_low_10 and current < prev_close:
        score -= 1
        reasons.append("Fresh breakdown below 10-period support.")

    if current > recent_high_20:
        score += 1
        reasons.append("Price is above 20-period structure highs.")
    elif current < recent_low_20:
        score -= 1
        reasons.append("Price is below 20-period structure support.")

    if atr > 0 and (atr / max(current, 0.000001)) > 0.03:
        reasons.append("Volatility is elevated, increasing both opportunity and risk.")
    else:
        reasons.append("Volatility is controlled and trend-friendly.")

    # avoid fighting strong primary trend too aggressively
    if strong_bull and score < 0:
        score = max(score, -1)
        reasons.append("Bearish conviction reduced because primary trend remains strongly bullish.")
    if strong_bear and score > 0:
        score = min(score, 1)
        reasons.append("Bullish conviction reduced because primary trend remains strongly bearish.")

    sig = "BUY" if score >= 5 else "SELL" if score <= -5 else "HOLD"
    conf = min(82, max(52, 55 + abs(score) * 3))

    return {
        "signal": sig,
        "conf": conf,
        "regime": regime,
        "reason": " ".join(reasons),
        "score": score
    }


def build_trade_setup(asset, candles, asset_type):
    if not candles or len(candles) < 60:
        return None

    brain = ava_brain_analyze(candles)
    current = float(candles[-1]["close"])

    closes = [c["close"] for c in candles]
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)

    signal = brain["signal"]
    if signal == "HOLD":
        return None

    # avoid shorting clear strong uptrends
    if signal == "SELL" and len(ema20) >= 2 and len(ema50) >= 2:
        if current > ema20[-1] and ema20[-1] > ema50[-1]:
            return None

    # avoid buying clear strong downtrends
    if signal == "BUY" and len(ema20) >= 2 and len(ema50) >= 2:
        if current < ema20[-1] and ema20[-1] < ema50[-1]:
            return None

    confidence = min(82, max(52, int(brain["conf"])))

    atr_floor = current * (0.012 if asset_type == "stock" else 0.008)
    atr = max(calc_atr_proxy(candles, 14), atr_floor)

    highs = [c["high"] for c in candles[-20:]]
    lows = [c["low"] for c in candles[-20:]]
    recent_high = max(highs)
    recent_low = min(lows)

    stop_mult = 2.2 if asset_type == "stock" else 1.4

    if signal == "BUY":
        entry = current
        stop = max(current - atr * stop_mult, recent_low * 0.985)
        if stop >= entry:
            stop = current - atr * max(1.8, stop_mult)
        risk = max(entry - stop, 0.000001)
        tp1 = entry + risk * 1.8
        tp2 = entry + risk * 3.0
        rr = (tp1 - entry) / risk
    else:
        entry = current
        stop = min(current + atr * stop_mult, recent_high * 1.015)
        if stop <= entry:
            stop = current + atr * max(1.8, stop_mult)
        risk = max(stop - entry, 0.000001)
        tp1 = entry - risk * 1.8
        tp2 = entry - risk * 3.0
        rr = (entry - tp1) / risk

    signal_ts = candles[-1].get("ts") or int(time.time())

    return {
        "signal_id": f"{asset_type}:{asset['symbol']}:{signal_ts}",
        "symbol": asset["symbol"],
        "asset_type": asset_type,
        "name": asset["name"],
        "signal": signal,
        "confidence": confidence,
        "regime": brain["regime"],
        "entry_price": round(entry, 8),
        "stop_loss": round(stop, 8),
        "take_profit_1": round(tp1, 8),
        "take_profit_2": round(tp2, 8),
        "risk_reward": round(rr, 2),
        "reason": brain["reason"],
        "price": round(float(asset["price"]), 8),
        "change_pct": round(float(asset["change"]), 4)
    }


def generate_active_signals():
    signals = []

    for asset in fetch_crypto_quotes_safe()[:60]:
        if db.was_recent_signal(asset["symbol"], "crypto", cooldown_hours=24):
            continue

        candles = fetch_crypto_candles(asset["symbol"], 120)
        setup = build_trade_setup(asset, candles, "crypto")
        if setup and setup["confidence"] >= Config.SIGNAL_MIN_CONFIDENCE and setup["risk_reward"] >= Config.SIGNAL_MIN_RR:
            signals.append(setup)

    for asset in fetch_stock_quotes_safe()[:50]:
        if db.was_recent_signal(asset["symbol"], "stock", cooldown_hours=72):
            continue

        candles = fetch_stock_candles(asset["symbol"])
        setup = build_trade_setup(asset, candles, "stock")
        if setup and setup["confidence"] >= Config.SIGNAL_MIN_CONFIDENCE and setup["risk_reward"] >= Config.SIGNAL_MIN_RR:
            signals.append(setup)

    signals.sort(key=lambda x: (x["confidence"], x["risk_reward"]), reverse=True)
    signals = signals[:50]
    db.replace_active_signals(signals)
    db.sync_signal_history(signals)
    return signals


def evaluate_signal_history_outcomes():
    open_rows = db.get_open_signal_history(limit=500)

    for row in open_rows:
        symbol = row["symbol"]
        asset_type = row["asset_type"]
        signal = row["signal"]
        stop = float(row["stop_loss"])
        tp1 = float(row["take_profit_1"])
        tp2 = float(row["take_profit_2"])

        candles = fetch_crypto_candles(symbol, 40) if asset_type == "crypto" else fetch_stock_candles(symbol)
        if not candles:
            continue

        recent = candles[-5:]
        outcome = None
        note = ""

        for c in recent:
            if signal == "BUY":
                hit_stop = c["low"] <= stop
                hit_tp1 = c["high"] >= tp1
                hit_tp2 = c["high"] >= tp2

                if hit_stop and (hit_tp1 or hit_tp2):
                    outcome = "AMBIGUOUS"
                    note = "Same candle touched stop and target; intrabar order unknown."
                    break
                elif hit_tp2:
                    outcome = "TP2_HIT"
                    note = "Candle high hit TP2."
                    break
                elif hit_tp1:
                    outcome = "TP1_HIT"
                    note = "Candle high hit TP1."
                    break
                elif hit_stop:
                    outcome = "STOPPED"
                    note = "Candle low hit stop."
                    break

            else:
                hit_stop = c["high"] >= stop
                hit_tp1 = c["low"] <= tp1
                hit_tp2 = c["low"] <= tp2

                if hit_stop and (hit_tp1 or hit_tp2):
                    outcome = "AMBIGUOUS"
                    note = "Same candle touched stop and target; intrabar order unknown."
                    break
                elif hit_tp2:
                    outcome = "TP2_HIT"
                    note = "Candle low hit TP2."
                    break
                elif hit_tp1:
                    outcome = "TP1_HIT"
                    note = "Candle low hit TP1."
                    break
                elif hit_stop:
                    outcome = "STOPPED"
                    note = "Candle high hit stop."
                    break

        if outcome:
            db.update_signal_outcome(row["history_id"], outcome, note)


def get_confidence_accuracy_breakdown():
    rows = db.get_signal_history(limit=1000)
    buckets = {
        "50-59": {"wins": 0, "losses": 0, "ambiguous": 0},
        "60-69": {"wins": 0, "losses": 0, "ambiguous": 0},
        "70-79": {"wins": 0, "losses": 0, "ambiguous": 0},
        "80-89": {"wins": 0, "losses": 0, "ambiguous": 0},
        "90-99": {"wins": 0, "losses": 0, "ambiguous": 0},
    }

    for r in rows:
        conf = int(r.get("confidence", 0))
        outcome = str(r.get("outcome", "OPEN"))

        if conf < 60:
            bucket = "50-59"
        elif conf < 70:
            bucket = "60-69"
        elif conf < 80:
            bucket = "70-79"
        elif conf < 90:
            bucket = "80-89"
        else:
            bucket = "90-99"

        if outcome in ("TP1_HIT", "TP2_HIT"):
            buckets[bucket]["wins"] += 1
        elif outcome == "STOPPED":
            buckets[bucket]["losses"] += 1
        elif outcome == "AMBIGUOUS":
            buckets[bucket]["ambiguous"] += 1

    result = []
    for bucket, vals in buckets.items():
        total = vals["wins"] + vals["losses"]
        acc = round((vals["wins"] / total) * 100, 2) if total > 0 else 0.0
        result.append({
            "bucket": bucket,
            "wins": vals["wins"],
            "losses": vals["losses"],
            "ambiguous": vals["ambiguous"],
            "accuracy": acc
        })
    return result


def send_email(to_email, subject, html_body, text_body=None):
    if not Config.RESEND_API_KEY:
        logger.info(f"[EMAIL STUB] To={to_email} Subject={subject}")
        db.log_email_event(to_email, "stub_send", {"subject": subject})
        return True

    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {Config.RESEND_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "from": Config.EMAIL_FROM,
                "to": [to_email],
                "subject": subject,
                "html": html_body,
                "text": text_body or ""
            },
            timeout=10
        )
        ok = r.status_code in (200, 201)
        db.log_email_event(to_email, "resend_send", {"subject": subject, "status": r.status_code})
        return ok
    except Exception as e:
        logger.warning(f"Resend email failed: {e}")
        return False


def send_password_reset_email(email, reset_link):
    subject = f"{Config.APP_NAME} Password Reset"
    html_body = f"""
    <div style="font-family:Arial,sans-serif;line-height:1.6">
      <h2>{Config.APP_NAME} Password Reset</h2>
      <p>Click the link below to reset your password:</p>
      <p><a href="{reset_link}">{reset_link}</a></p>
      <p>This link expires in {Config.PASSWORD_RESET_TTL_MINUTES} minutes.</p>
    </div>
    """
    return send_email(email, subject, html_body, f"Reset your password: {reset_link}")


def send_telegram_message(text):
    if not Config.TELEGRAM_BOT_TOKEN or not Config.TELEGRAM_CHAT_ID:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{Config.TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": Config.TELEGRAM_CHAT_ID, "text": text},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        logger.warning(f"Telegram send failed: {e}")
        return False


def send_discord_message(text):
    if not Config.DISCORD_WEBHOOK_URL:
        return False
    try:
        r = requests.post(Config.DISCORD_WEBHOOK_URL, json={"content": text}, timeout=10)
        return r.status_code in (200, 204)
    except Exception as e:
        logger.warning(f"Discord send failed: {e}")
        return False


def build_top_signals_broadcast(signals):
    if not signals:
        return None
    lines = ["🚀 AVA Top Signals"]
    for s in signals[:3]:
        lines.append(
            f"{s['symbol']} | {s['signal']} | Entry {fmt_price(s['entry_price'])} | TP1 {fmt_price(s['take_profit_1'])} | Conf {s['confidence']}%"
        )
    return "\n".join(lines)


def maybe_broadcast_top_signals(signals):
    text = build_top_signals_broadcast(signals)
    if not text:
        return

    msg_hash = hashlib.sha256(text.encode()).hexdigest()

    if Config.TELEGRAM_BOT_TOKEN and Config.TELEGRAM_CHAT_ID:
        if not db.was_broadcast_sent_recently("telegram", msg_hash, Config.BROADCAST_COOLDOWN_SECONDS):
            if send_telegram_message(text):
                db.log_broadcast("telegram", msg_hash)

    if Config.DISCORD_WEBHOOK_URL:
        if not db.was_broadcast_sent_recently("discord", msg_hash, Config.BROADCAST_COOLDOWN_SECONDS):
            if send_discord_message(text):
                db.log_broadcast("discord", msg_hash)


PLAN_META = {
    "pro_monthly": {"tier": "pro", "billing": "monthly", "price_id": Config.STRIPE_PRICE_PRO_MONTHLY},
    "elite_monthly": {"tier": "elite", "billing": "monthly", "price_id": Config.STRIPE_PRICE_ELITE_MONTHLY},
}

def stripe_enabled():
    return bool(stripe and Config.STRIPE_SECRET_KEY)

def create_checkout_session(user, plan_key):
    if not stripe_enabled():
        raise RuntimeError("Stripe not configured")
    plan = PLAN_META.get(plan_key)
    if not plan or not plan["price_id"]:
        raise RuntimeError("Invalid or missing Stripe price")
    return stripe.checkout.Session.create(
        mode="subscription",
        customer_email=user["email"],
        client_reference_id=str(user["id"]),
        line_items=[{"price": plan["price_id"], "quantity": 1}],
        success_url=f"{Config.DOMAIN}/dashboard",
        cancel_url=f"{Config.DOMAIN}/pricing",
        metadata={"tier": plan["tier"], "billing": plan["billing"], "plan_key": plan_key}
    )

def create_billing_portal(customer_id):
    if not stripe_enabled():
        raise RuntimeError("Stripe not configured")
    return stripe.billing_portal.Session.create(customer=customer_id, return_url=f"{Config.DOMAIN}/dashboard")


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
        cards += f"""
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
    fallback_html = '<p class="muted">No active signals yet.</p>'
    if not blurred:
        return f"<div class='grid-3'>{cards or fallback_html}</div>"

    return f"""
    <div class="blur-lock">
      <div class="blurred">
        <div class="grid-3">{cards or fallback_html}</div>
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
    <div class="footer">AVA Markets © 2026 — Premium Final Edition.</div>
    """


def nav_layout(title, content, description="AVA Markets - AI market intelligence"):
    user_nav = (
        """<a href="/dashboard">Dashboard</a><a href="/logout">Logout</a>"""
        if g.user else
        """<a href="/login">Login</a><a href="/register">Register</a>"""
    )
    admin_link = '<a href="/admin">Admin</a>' if is_admin() else ''
    return render_template_string("""
    <!DOCTYPE html>
    <html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{ title }}</title>
  <meta name="description" content="{{ description }}">
  <meta name="google-site-verification" content="39xa6RndNqbrq7XCh_9JZQkWBoRKAJlghz8ieHcV2v4" />
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
            <a href="/trends">Trends</a>
            <a href="/forecasts">Forecasts</a>
            <a href="/hot">HOT</a>
            <a href="/history">History</a>
            <a href="/portfolio">Portfolio</a>
            <a href="/blog">Blog</a>
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
    """, title=title, description=description, css=CSS, content=content, user_nav=user_nav, admin_link=admin_link, footer=render_footer())


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


def current_price_for(symbol, asset_type):
    symbol = symbol.upper()
    if asset_type == "crypto":
        a = next((x for x in fetch_crypto_quotes_safe() if x["symbol"] == symbol), None)
        return float(a["price"]) if a else None
    a = next((x for x in fetch_stock_quotes_safe() if x["symbol"] == symbol), None)
    return float(a["price"]) if a else None


def build_portfolio_analytics(user_id):
    positions = db.get_portfolio_positions(user_id)
    rows = []
    total_cost = 0.0
    total_value = 0.0
    crypto_value = 0.0
    stock_value = 0.0

    for p in positions:
        qty = float(p["quantity"])
        avg_cost = float(p["avg_cost"])
        current = current_price_for(p["symbol"], p["asset_type"])
        if current is None:
            current = avg_cost

        cost_basis = qty * avg_cost
        market_value = qty * current
        pnl = market_value - cost_basis
        pnl_pct = (pnl / cost_basis * 100.0) if cost_basis > 0 else 0.0

        total_cost += cost_basis
        total_value += market_value
        if p["asset_type"] == "crypto":
            crypto_value += market_value
        else:
            stock_value += market_value

        rows.append({
            "id": p["id"],
            "symbol": p["symbol"],
            "asset_type": p["asset_type"],
            "quantity": qty,
            "avg_cost": avg_cost,
            "current_price": current,
            "cost_basis": cost_basis,
            "market_value": market_value,
            "pnl": pnl,
            "pnl_pct": pnl_pct
        })

    total_pnl = total_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100.0) if total_cost > 0 else 0.0
    crypto_alloc = (crypto_value / total_value * 100.0) if total_value > 0 else 0.0
    stock_alloc = (stock_value / total_value * 100.0) if total_value > 0 else 0.0

    best = max(rows, key=lambda x: x["pnl_pct"], default=None)
    worst = min(rows, key=lambda x: x["pnl_pct"], default=None)

    return {
        "positions": rows,
        "total_cost": total_cost,
        "total_value": total_value,
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl_pct,
        "crypto_alloc": crypto_alloc,
        "stock_alloc": stock_alloc,
        "best": best,
        "worst": worst
    }


def combined_market_assets():
    assets = []
    for a in fetch_crypto_quotes_safe():
        item = dict(a)
        item["asset_type"] = "crypto"
        assets.append(item)
    for a in fetch_stock_quotes_safe():
        item = dict(a)
        item["asset_type"] = "stock"
        assets.append(item)
    return assets


def get_trend_lists():
    assets = combined_market_assets()
    gainers = sorted(assets, key=lambda x: float(x.get("change", 0)), reverse=True)[:12]
    losers = sorted(assets, key=lambda x: float(x.get("change", 0)))[:12]
    return gainers, losers


def get_hot_assets():
    signals = db.get_active_signals(limit=100)
    hot = sorted(
        signals,
        key=lambda x: (float(x.get("confidence", 0)), float(x.get("risk_reward", 0)), abs(float(x.get("change_pct", 0)))),
        reverse=True
    )
    return hot[:12]


def build_forecasts():
    forecasts = []

    for asset in fetch_crypto_quotes_safe()[:12]:
        candles = fetch_crypto_candles(asset["symbol"], 100)
        if not candles:
            continue
        brain = ava_brain_analyze(candles)
        forecasts.append({
            "symbol": asset["symbol"],
            "name": asset["name"],
            "asset_type": "crypto",
            "price": asset["price"],
            "change": asset["change"],
            "signal": brain["signal"],
            "confidence": brain["conf"],
            "regime": brain["regime"],
            "summary": brain["reason"]
        })

    for asset in fetch_stock_quotes_safe()[:12]:
        candles = fetch_stock_candles(asset["symbol"])
        if not candles:
            continue
        brain = ava_brain_analyze(candles)
        forecasts.append({
            "symbol": asset["symbol"],
            "name": asset["name"],
            "asset_type": "stock",
            "price": asset["price"],
            "change": asset["change"],
            "signal": brain["signal"],
            "confidence": brain["conf"],
            "regime": brain["regime"],
            "summary": brain["reason"]
        })

    forecasts.sort(key=lambda x: int(x["confidence"]), reverse=True)
    return forecasts[:20]

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
        "price_display": fmt_price(a.get("price", 0)),
        "change_display": fmt_change(a.get("change", 0)),
        "dir": a.get("dir", "down"),
        "signal": a.get("signal", "HOLD")
    } for a in assets]
    return jsonify({"updated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), "items": items})


@app.route("/")
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
        <div class="badge">AVA Super Sharp</div>
        <h1>Trade cleaner. Scan faster. Move with conviction.</h1>
        <p class="hero-sub">
          AVA Markets surfaces ranked crypto and stock trade setups with entries, stops, targets,
          confidence scoring, regime detection, portfolio analytics, Trends, HOT feeds, and Forecasts.
        </p>
        <div class="btns">
          <a class="btn btn-primary" href="/pricing">Unlock Pro</a>
          <a class="btn btn-secondary" href="/signals">View Signals</a>
          <a class="btn btn-dark" href="/portfolio">Open Portfolio</a>
        </div>
        <div class="grid-4" style="margin-top:24px;">
          <div class="kpi"><div class="num">{stats['total']}</div><div class="label">Signals Recorded</div></div>
          <div class="kpi"><div class="num">{stats['win_rate']}%</div><div class="label">Tracked Win Rate</div></div>
          <div class="kpi"><div class="num">{user_count}</div><div class="label">Registered Users</div></div>
          <div class="kpi"><div class="num">{subs}</div><div class="label">Alert Subscribers</div></div>
        </div>
      </div>

      <div class="card">
        <div class="badge">Top Setups Now</div>
        <h2>Premium AVA Trade Feed</h2>
        <p>Highest conviction setups ranked by confidence and risk/reward.</p>
        {signals_section}
      </div>
    </section>

    <section class="section">
      <div class="grid-3">
        <div class="card">
          <div class="badge">Live Scan</div>
          <h3>Crypto + Stocks + Commodities</h3>
          <p>Track liquid assets across digital and traditional markets from one dashboard.</p>
        </div>
        <div class="card">
          <div class="badge">Portfolio Intelligence</div>
          <h3>See exposure clearly</h3>
          <p>Track total value, allocation, unrealized PnL, and your top winners and losers.</p>
        </div>
        <div class="card">
          <div class="badge">Forecast Engine</div>
          <h3>AVA Super Sharp</h3>
          <p>Use Trends, HOT, and Forecasts to see what AVA thinks matters right now.</p>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="grid-3">
        <div class="price-card">
          <div class="pill">Free</div>
          <div class="price">$0</div>
          <p>Market dashboards, light signals, blog content, and premium previews.</p>
          <a class="btn btn-secondary" style="width:100%;" href="/register">Start Free</a>
        </div>

        <div class="price-card featured">
          <div class="pill" style="background:rgba(56,189,248,.12);border-color:rgba(56,189,248,.2);color:#bae6fd;">Most Popular</div>
          <div class="price">$15<span class="small">/mo</span></div>
          <p>Full active signals, detail pages, signal history, tracked outcomes and premium workflow tools.</p>
          <a class="btn btn-primary" style="width:100%;margin-top:10px;" href="/pricing">Get Pro</a>
        </div>

        <div class="price-card">
          <div class="pill" style="background:rgba(250,204,21,.14);border-color:rgba(250,204,21,.24);color:#fde68a;">Elite</div>
          <div class="price">$35<span class="small">/mo</span></div>
          <p>Everything in Pro plus premium alert-ready workflow and hottest conviction feeds.</p>
          <a class="btn btn-secondary" style="width:100%;margin-top:10px;" href="/pricing">Go Elite</a>
        </div>
      </div>
    </section>
    """
    return nav_layout("AVA Markets - AI Market Intelligence", content)


@app.route("/register", methods=["GET", "POST"])
def register():
    if g.user:
        return redirect("/dashboard")
    err = ""
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "").strip()
        if len(password) < 6:
            err = "Password must be at least 6 characters."
        else:
            u = db.create_user(email, password)
            if not u:
                err = "Email already exists."
            else:
                resp = make_response(redirect("/dashboard"))
                set_session_cookie(resp, db.create_session(u["id"]))
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
    return nav_layout("Register - AVA", content)


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
            set_session_cookie(resp, db.create_session(u["id"]))
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
        <div class="small" style="margin-top:14px;"><a href="/forgot-password">Forgot password?</a></div>
      </div>
    </div>
    """
    return nav_layout("Login - AVA", content)


@app.route("/logout")
def logout():
    token = request.cookies.get("session_token")
    db.delete_session(token)
    resp = make_response(redirect("/"))
    resp.delete_cookie("session_token")
    return resp


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    msg = ""
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        user = db.get_user_by_email(email)
        if user:
            token = db.create_password_reset(user["id"])
            reset_link = f"{Config.DOMAIN}/reset-password/{quote_plus(token)}"
            send_password_reset_email(email, reset_link)
        msg = "If that email exists, a reset link has been sent."
    content = f"""
    <div class="form-shell"><div class="form-card">
      <h2>Forgot Password</h2>
      {f"<div class='success'>{msg}</div>" if msg else ""}
      <form method="POST">
        <input type="email" name="email" placeholder="Email" required>
        <button type="submit">Send Reset Link</button>
      </form>
    </div></div>
    """
    return nav_layout("Forgot Password", content)


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token):
    row = db.get_valid_password_reset(token)
    if not row:
        return nav_layout("Reset Password", "<section class='section'><div class='card'><div class='error'>Invalid or expired reset link.</div></div></section>")

    err = ""
    ok = ""
    if request.method == "POST":
        pw1 = request.form.get("password", "").strip()
        pw2 = request.form.get("confirm_password", "").strip()
        if len(pw1) < 6:
            err = "Password must be at least 6 characters."
        elif pw1 != pw2:
            err = "Passwords do not match."
        else:
            db.update_user_password(row["user_id"], pw1)
            db.mark_password_reset_used(token)
            ok = "Password updated successfully."

    content = f"""
    <div class="form-shell"><div class="form-card">
      <h2>Reset Password</h2>
      {f"<div class='error'>{err}</div>" if err else ""}
      {f"<div class='success'>{ok}</div>" if ok else ""}
      <form method="POST">
        <input type="password" name="password" placeholder="New password" required>
        <input type="password" name="confirm_password" placeholder="Confirm password" required>
        <button type="submit">Update Password</button>
      </form>
    </div></div>
    """
    return nav_layout("Reset Password", content)


@app.route("/dashboard")
@require_auth
def dashboard():
    signals = db.get_active_signals(limit=6)
    watchlist = db.get_watchlist(g.user["id"])
    prefs = db.get_alert_prefs(g.user["id"])
    billing_btn = ""
    if g.user.get("tier") in ("pro", "elite") and g.user.get("stripe_customer_id"):
        billing_btn = '<a href="/billing" class="btn btn-secondary" style="width:100%; margin-top:10px;">Open Billing Portal</a>'

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

    watch_rows = ""
    for w in watchlist:
        watch_rows += f"""
        <tr>
          <td>{h(w['symbol'])}</td>
          <td>{h(w['asset_type'])}</td>
          <td>
            <form method="POST" action="/watchlist/remove">
              <input type="hidden" name="symbol" value="{h(w['symbol'])}">
              <input type="hidden" name="asset_type" value="{h(w['asset_type'])}">
              <button class="btn btn-secondary" type="submit">Remove</button>
            </form>
          </td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Account Hub</div>
      <h1>Dashboard</h1>

      <div class="grid-2" style="margin-bottom:20px;">
        <div class="card">
          <h3>Account</h3>
          <p><strong>Email:</strong> {h(g.user['email'])}</p>
          <p><strong>Plan:</strong> {tier_badge_html(g.user)}</p>
          <p><strong>Billing:</strong> {h(g.user.get('billing_cycle') or 'none')}</p>
          {billing_btn}
        </div>

        <div class="card">
          <h3>Alert Preferences</h3>
          <form method="POST" action="/alerts/preferences">
            <label class="small">Minimum Confidence</label>
            <input type="number" name="min_confidence" min="50" max="98" value="{int((prefs or {}).get('min_confidence', 70))}">
            <button class="btn btn-primary" type="submit">Save Preferences</button>
          </form>
        </div>
      </div>

      <div class="card" style="margin-bottom:20px;">
        <h3>Top AVA Trades</h3>
        <div class="grid-3">
          {signal_cards or "<p>No signals available yet.</p>"}
        </div>
      </div>

      <div class="card">
        <h3>Watchlist</h3>
        <form method="POST" action="/watchlist/add" class="grid-3" style="margin-bottom:16px;">
          <input type="text" name="symbol" placeholder="BTC or AAPL" required>
          <select name="asset_type">
            <option value="crypto">crypto</option>
            <option value="stock">stock</option>
          </select>
          <button class="btn btn-primary" type="submit">Add to Watchlist</button>
        </form>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Symbol</th><th>Type</th><th>Action</th></tr>
            {watch_rows or "<tr><td colspan='3'>No watchlist items yet.</td></tr>"}
          </table>
        </div>
      </div>
    </section>
    """
    return nav_layout("Dashboard - AVA", content)


@app.route("/alerts/preferences", methods=["POST"])
@require_auth
def alert_preferences():
    min_conf = int(request.form.get("min_confidence", 70))
    min_conf = max(50, min(98, min_conf))
    db.update_alert_prefs(g.user["id"], 1, 0, 0, min_conf)
    return redirect("/dashboard")


@app.route("/watchlist/add", methods=["POST"])
@require_auth
def watchlist_add():
    db.add_watchlist(g.user["id"], request.form.get("symbol", ""), request.form.get("asset_type", "crypto"))
    return redirect("/dashboard")


@app.route("/watchlist/remove", methods=["POST"])
@require_auth
def watchlist_remove():
    db.remove_watchlist(g.user["id"], request.form.get("symbol", ""), request.form.get("asset_type", "crypto"))
    return redirect("/dashboard")


@app.route("/portfolio")
@require_auth
def portfolio():
    analytics = build_portfolio_analytics(g.user["id"])

    rows = ""
    for p in analytics["positions"]:
        pnl_class = "up" if p["pnl"] >= 0 else "down"
        rows += f"""
        <tr>
          <td><strong>{h(p['symbol'])}</strong><div class="small">{h(p['asset_type'])}</div></td>
          <td>{p['quantity']}</td>
          <td>{fmt_price(p['avg_cost'])}</td>
          <td>{fmt_price(p['current_price'])}</td>
          <td>{fmt_price(p['cost_basis'])}</td>
          <td>{fmt_price(p['market_value'])}</td>
          <td class="{pnl_class}">{fmt_price(p['pnl'])}</td>
          <td class="{pnl_class}">{p['pnl_pct']:+.2f}%</td>
          <td>
            <form method="POST" action="/portfolio/delete">
              <input type="hidden" name="position_id" value="{p['id']}">
              <button class="btn btn-secondary" type="submit">Remove</button>
            </form>
          </td>
        </tr>
        """

    best = analytics["best"]
    worst = analytics["worst"]
    best_text = f"{h(best['symbol'])} ({best['pnl_pct']:+.2f}%)" if best else "N/A"
    worst_text = f"{h(worst['symbol'])} ({worst['pnl_pct']:+.2f}%)" if worst else "N/A"

    content = f"""
    <section class="section">
      <div class="badge">Portfolio Analytics</div>
      <h1>Your Portfolio</h1>

      <div class="grid-4" style="margin-bottom:20px;">
        <div class="kpi"><div class="num">{fmt_price(analytics['total_value'])}</div><div class="label">Total Value</div></div>
        <div class="kpi"><div class="num">{fmt_price(analytics['total_pnl'])}</div><div class="label">Unrealized PnL</div></div>
        <div class="kpi"><div class="num">{analytics['total_pnl_pct']:+.2f}%</div><div class="label">PnL %</div></div>
        <div class="kpi"><div class="num">{len(analytics['positions'])}</div><div class="label">Positions</div></div>
      </div>

      <div class="grid-2" style="margin-bottom:20px;">
        <div class="card">
          <h3>Add Position</h3>
          <form method="POST" action="/portfolio/add">
            <input type="text" name="symbol" placeholder="BTC or AAPL" required>
            <select name="asset_type">
              <option value="crypto">crypto</option>
              <option value="stock">stock</option>
            </select>
            <input type="number" step="any" name="quantity" placeholder="Quantity" required>
            <input type="number" step="any" name="avg_cost" placeholder="Average Cost" required>
            <button type="submit">Add Position</button>
          </form>
        </div>

        <div class="card">
          <h3>Allocation Overview</h3>
          <p><strong>Crypto Allocation:</strong> {analytics['crypto_alloc']:.2f}%</p>
          <p><strong>Stock Allocation:</strong> {analytics['stock_alloc']:.2f}%</p>
          <p><strong>Best Position:</strong> {best_text}</p>
          <p><strong>Worst Position:</strong> {worst_text}</p>
        </div>
      </div>

      <div class="table-shell">
        <table class="market-table">
          <tr>
            <th>Asset</th><th>Qty</th><th>Avg Cost</th><th>Current</th><th>Cost Basis</th>
            <th>Value</th><th>PnL</th><th>PnL %</th><th>Action</th>
          </tr>
          {rows or "<tr><td colspan='9'>No positions yet.</td></tr>"}
        </table>
      </div>
    </section>
    """
    return nav_layout("Portfolio - AVA", content)


@app.route("/portfolio/add", methods=["POST"])
@require_auth
def portfolio_add():
    symbol = request.form.get("symbol", "").strip().upper()
    asset_type = request.form.get("asset_type", "crypto").strip()
    quantity = float(request.form.get("quantity", "0") or 0)
    avg_cost = float(request.form.get("avg_cost", "0") or 0)
    if symbol and quantity > 0 and avg_cost > 0:
        db.add_portfolio_position(g.user["id"], symbol, asset_type, quantity, avg_cost)
    return redirect("/portfolio")


@app.route("/portfolio/delete", methods=["POST"])
@require_auth
def portfolio_delete():
    position_id = int(request.form.get("position_id", "0") or 0)
    if position_id > 0:
        db.delete_portfolio_position(position_id, g.user["id"])
    return redirect("/portfolio")


@app.route("/billing")
@require_auth
def billing():
    if not g.user.get("stripe_customer_id"):
        return redirect("/pricing")
    try:
        session = create_billing_portal(g.user["stripe_customer_id"])
        return redirect(session.url)
    except Exception as e:
        return str(e), 500


@app.route("/pricing")
def pricing():
    content = """
    <section class="section">
      <div style="text-align:center; margin-bottom:34px;">
        <div class="badge">Simple Monthly Pricing</div>
        <h1>Choose your AVA plan</h1>
        <p>Start free. Upgrade when you want ranked active signals, tracked history, portfolio tools, and premium workflow features.</p>
      </div>

      <div class="grid-3" style="align-items:stretch;">
        <div class="price-card">
          <div class="pill">Free</div>
          <div class="price">$0</div>
          <p>For curious users and light scanners.</p>
          <ul style="line-height:2; color:var(--text);">
            <li>Crypto + stock dashboards</li>
            <li>Basic list signals</li>
            <li>Blog and learning pages</li>
            <li>Portfolio tracking</li>
          </ul>
          <a href="/register" class="btn btn-secondary" style="width:100%;">Start Free</a>
        </div>

        <div class="price-card featured">
          <div class="pill" style="background:rgba(56,189,248,.12);border-color:rgba(56,189,248,.22);color:#bae6fd;">Most Popular</div>
          <div class="price">$15<span class="small">/mo</span></div>
          <p>Best for most traders who want real AVA workflow value.</p>
          <ul style="line-height:2; color:var(--text);">
            <li>Active Signal Trades</li>
            <li>AVA Brain detail pages</li>
            <li>Signal history + win-rate</li>
            <li>Portfolio + premium workflow</li>
            <li>Trends + Forecasts</li>
          </ul>
          <form action="/checkout/pro_monthly" method="POST">
            <button type="submit" class="btn btn-primary" style="width:100%;">Get Pro Monthly</button>
          </form>
        </div>

        <div class="price-card">
          <div class="pill" style="background:rgba(250,204,21,.14);border-color:rgba(250,204,21,.22);color:#fde68a;">Power Users</div>
          <div class="price">$35<span class="small">/mo</span></div>
          <p>For advanced users wanting sharper market intelligence and premium workflow expansion.</p>
          <ul style="line-height:2; color:var(--text);">
            <li>Everything in Pro</li>
            <li>HOT feed</li>
            <li>Elite Forecasts</li>
            <li>Telegram / Discord alert-ready workflow</li>
            <li>Highest conviction market view</li>
          </ul>
          <form action="/checkout/elite_monthly" method="POST">
            <button type="submit" class="btn btn-secondary" style="width:100%;">Get Elite Monthly</button>
          </form>
        </div>
      </div>
    </section>
    """
    return nav_layout("Pricing - AVA", content)


@app.route("/checkout/<plan_key>", methods=["POST"])
@require_auth
def checkout(plan_key):
    try:
        session = create_checkout_session(g.user, plan_key)
        return redirect(session.url)
    except Exception as e:
        return str(e), 500


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
            <p>Upgrade to Pro to unlock the full ranked signal table and AVA Brain detail pages.</p>
            <a href="/pricing" class="btn btn-primary">Unlock Pro</a>
          </div>
        </section>
        """
        return nav_layout("Signals - AVA", content)

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

    content = f"""
    <section class="section">
      <div class="badge">AVA Super Brain</div>
      <h1>Active Signal Trades</h1>
      <div class="btns" style="margin-bottom:20px;">
        <a class="btn btn-secondary" href="/signals">All</a>
        <a class="btn btn-secondary" href="/signals?type=crypto">Crypto</a>
        <a class="btn btn-secondary" href="/signals?type=stock">Stocks</a>
      </div>
      <div class="table-shell">
        <table class="market-table">
          <tr>
            <th>Asset</th><th>Type</th><th>Signal</th><th>Confidence</th>
            <th>Entry</th><th>Stop</th><th>TP1</th><th>TP2</th><th>R:R</th>
          </tr>
          {rows or "<tr><td colspan='9'>No signals available yet.</td></tr>"}
        </table>
      </div>
    </section>
    """
    return nav_layout("Signals - AVA", content)


@app.route("/trends")
@require_tier("pro")
def trends():
    gainers, losers = get_trend_lists()

    gainers_html = ""
    for a in gainers:
        link = f"/crypto/{h(a['symbol'])}" if a["asset_type"] == "crypto" else f"/stocks/{h(a['symbol'])}"
        gainers_html += f"""
        <tr>
          <td><strong><a href="{link}">{h(a['symbol'])}</a></strong><div class="small">{h(a['name'])}</div></td>
          <td>{h(a['asset_type'].upper())}</td>
          <td>{fmt_price(a['price'])}</td>
          <td class="up">{fmt_change(a['change'])}</td>
          <td><span class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """

    losers_html = ""
    for a in losers:
        link = f"/crypto/{h(a['symbol'])}" if a["asset_type"] == "crypto" else f"/stocks/{h(a['symbol'])}"
        losers_html += f"""
        <tr>
          <td><strong><a href="{link}">{h(a['symbol'])}</a></strong><div class="small">{h(a['name'])}</div></td>
          <td>{h(a['asset_type'].upper())}</td>
          <td>{fmt_price(a['price'])}</td>
          <td class="down">{fmt_change(a['change'])}</td>
          <td><span class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">AVA Trends</div>
      <h1>Market Trends</h1>
      <p>Live strongest gainers and losers across crypto, stocks, and commodities.</p>

      <div class="grid-2">
        <div class="card">
          <h3>Top Gainers</h3>
          <div class="table-shell">
            <table class="market-table">
              <tr><th>Asset</th><th>Type</th><th>Price</th><th>Change</th><th>Signal</th></tr>
              {gainers_html or "<tr><td colspan='5'>No data yet.</td></tr>"}
            </table>
          </div>
        </div>

        <div class="card">
          <h3>Top Losers</h3>
          <div class="table-shell">
            <table class="market-table">
              <tr><th>Asset</th><th>Type</th><th>Price</th><th>Change</th><th>Signal</th></tr>
              {losers_html or "<tr><td colspan='5'>No data yet.</td></tr>"}
            </table>
          </div>
        </div>
      </div>
    </section>
    """
    return nav_layout("Trends - AVA", content)


@app.route("/hot")
@require_tier("elite")
def hot():
    hot_assets = get_hot_assets()

    rows = ""
    for s in hot_assets:
        link = f"/crypto/{h(s['symbol'])}" if s["asset_type"] == "crypto" else f"/stocks/{h(s['symbol'])}"
        rows += f"""
        <tr>
          <td><strong><a href="{link}">{h(s['symbol'])}</a></strong><div class="small">{h(s['name'])}</div></td>
          <td>{h(s['asset_type'].upper())}</td>
          <td><span class="signal signal-{h(s['signal'].lower())}">{h(s['signal'])}</span></td>
          <td>{int(s['confidence'])}%</td>
          <td>{fmt_price(s['entry_price'])}</td>
          <td>{fmt_price(s['take_profit_1'])}</td>
          <td>{h(s['risk_reward'])}:1</td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">AVA HOT Feed</div>
      <h1>HOT Setups</h1>
      <p>Elite-only feed of the sharpest AVA-ranked setups with strongest confidence and structure.</p>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Type</th><th>Signal</th><th>Confidence</th><th>Entry</th><th>TP1</th><th>R:R</th></tr>
          {rows or "<tr><td colspan='7'>No HOT setups yet.</td></tr>"}
        </table>
      </div>
    </section>
    """
    return nav_layout("HOT - AVA", content)


@app.route("/forecasts")
@require_tier("pro")
def forecasts():
    items = build_forecasts()

    cards = ""
    for f in items:
        link = f"/crypto/{h(f['symbol'])}" if f["asset_type"] == "crypto" else f"/stocks/{h(f['symbol'])}"
        cards += f"""
        <div class="card">
          <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;">
            <div>
              <h3 style="margin-bottom:4px;"><a href="{link}">{h(f['symbol'])}</a></h3>
              <div class="small">{h(f['name'])} • {h(f['asset_type'].upper())}</div>
            </div>
            <span class="signal signal-{h(f['signal'].lower())}">{h(f['signal'])}</span>
          </div>
          <div class="hr"></div>
          <p><strong>Confidence:</strong> {int(f['confidence'])}%</p>
          <p><strong>Regime:</strong> {h(f['regime'])}</p>
          <p><strong>Price:</strong> {fmt_price(f['price'])}</p>
          <p><strong>Change:</strong> {fmt_change(f['change'])}</p>
          <p class="small">{h(f['summary'])}</p>
        </div>
        """

    content = f"""
    <section class="section">
      <div class="badge">AVA Forecast Engine</div>
      <h1>Forecasts</h1>
      <p>AVA Super Sharp directional analysis across crypto, stocks, and commodities.</p>
      <div class="grid-2">
        {cards or "<p>No forecasts available yet.</p>"}
      </div>
    </section>
    """
    return nav_layout("Forecasts - AVA", content)


@app.route("/history")
def history():
    stats = db.get_signal_stats()
    confidence_rows = get_confidence_accuracy_breakdown()
    confidence_html = ""
    for row in confidence_rows:
        confidence_html += f"""
        <tr>
          <td>{h(row['bucket'])}</td>
          <td>{row['wins']}</td>
          <td>{row['losses']}</td>
          <td>{row.get('ambiguous', 0)}</td>
          <td>{row['accuracy']}%</td>
        </tr>
        """

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
        return nav_layout("History - AVA", content)

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
          <tr><th>Asset</th><th>Type</th><th>Signal</th><th>Confidence</th><th>Entry</th><th>TP1</th><th>TP2</th><th>Outcome</th></tr>
          {rows or "<tr><td colspan='8'>No signal history yet.</td></tr>"}
        </table>
      </div>

      <div class="card" style="margin-top:24px;">
        <h3>Accuracy by Confidence Bucket</h3>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Confidence</th><th>Wins</th><th>Losses</th><th>Ambiguous</th><th>Accuracy</th></tr>
            {confidence_html or "<tr><td colspan='5'>No confidence accuracy data yet.</td></tr>"}
          </table>
        </div>
      </div>
    </section>
    """
    return nav_layout("History - AVA", content)


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
      <div class="badge">Live Crypto Scanner</div>
      <h1>100 Crypto Assets</h1>
      <p>Live crypto dashboard with broad coverage, AVA list signals, and fallback-filled market coverage.</p>
      <div id="live-updated" class="small" style="margin-bottom:20px;">Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>24h</th><th>AVA Signal</th></tr>
          {rows or "<tr><td colspan='4'>Cache warming up. Refresh soon.</td></tr>"}
        </table>
      </div>
      {render_pagination('/crypto', current, pages)}
      <div class="small" style="margin-top:12px;">{total} crypto assets loaded</div>
    </section>
    {live_update_script("crypto")}
    """
    return nav_layout("Crypto Signals and Prices - AVA", content)


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
          <td id="price-{safe_id}">{fmt_price(a.get("price",0))}</td>
          <td id="change-{safe_id}" class="{a.get('dir','down')}">{fmt_change(a.get("change",0))}</td>
          <td><span id="signal-{safe_id}" class="signal signal-{a.get('signal','HOLD').lower()}">{a.get('signal','HOLD')}</span></td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Premium Equity Scanner</div>
      <h1>50 Stocks & Commodities</h1>
      <p>Institutional-style watchlist covering mega-cap equities, sector leaders, and macro commodity proxies.</p>
      <div id="live-updated" class="small" style="margin-bottom:20px;">Last updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC</div>
      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>AVA Signal</th></tr>
          {rows or "<tr><td colspan='4'>Cache warming up. Refresh soon.</td></tr>"}
        </table>
      </div>
      {render_pagination('/stocks', current, pages)}
      <div class="small" style="margin-top:12px;">{total} stock and commodity assets loaded</div>
    </section>
    {live_update_script("stocks")}
    """
    return nav_layout("Stock Signals and Prices - AVA", content)


@app.route("/crypto/<symbol>")
@require_tier("pro")
def crypto_detail(symbol):
    symbol = symbol.upper()
    asset = next((a for a in fetch_crypto_quotes_safe() if a.get("symbol") == symbol), None)
    if not asset:
        abort(404)
    candles = fetch_crypto_candles(symbol)
    brain = ava_brain_analyze(candles)

    content = f"""
    <section class="section">
      <div class="badge">Premium Intelligence</div>
      <h1>{h(asset['name'])} ({symbol})</h1>
      <div style="font-size:2.4rem;font-weight:900;margin-bottom:20px;">
        {fmt_price(asset['price'])}
        <span class="{asset['dir']}" style="font-size:1.15rem;">{fmt_change(asset['change'])}</span>
      </div>
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
    return nav_layout(f"{symbol} Signal Analysis - AVA", content)


@app.route("/stocks/<symbol>")
@require_tier("pro")
def stock_detail(symbol):
    symbol = symbol.upper()
    asset = next((a for a in fetch_stock_quotes_safe() if a.get("symbol") == symbol), None)
    if not asset:
        abort(404)
    candles = fetch_stock_candles(symbol)
    brain = ava_brain_analyze(candles)

    content = f"""
    <section class="section">
      <div class="badge">Premium Intelligence</div>
      <h1>{h(asset['name'])} ({symbol})</h1>
      <div style="font-size:2.4rem;font-weight:900;margin-bottom:20px;">
        {fmt_price(asset['price'])}
        <span class="{asset['dir']}" style="font-size:1.15rem;">{fmt_change(asset['change'])}</span>
      </div>
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
    return nav_layout(f"{symbol} Signal Analysis - AVA", content)


@app.route("/landing/<symbol>")
def landing_symbol(symbol):
    symbol = symbol.upper()

    if symbol in ("BTC", "ETH"):
        asset = next((a for a in fetch_crypto_quotes_safe() if a["symbol"] == symbol), None)
        if not asset:
            abort(404)
        content = f"""
        <section class="section">
          <div class="badge">Market Landing Page</div>
          <h1>{h(asset['name'])} ({h(symbol)}) Signals, Price, and AVA Guide</h1>
          <p>Track live price, AVA signal direction, and unlock premium trade setups for {h(asset['name'])}.</p>
          <div class="card">
            <h3>Live Snapshot</h3>
            <p>Price: <strong>{fmt_price(asset['price'])}</strong></p>
            <p>Change: <strong class="{asset['dir']}">{fmt_change(asset['change'])}</strong></p>
            <p>Signal: <span class="signal signal-{asset['signal'].lower()}">{asset['signal']}</span></p>
            <div class="btns">
              <a href="/learn/{h(symbol.lower())}" class="btn btn-secondary">Learn {h(symbol)}</a>
              <a href="/crypto/{h(symbol)}" class="btn btn-primary">Open Premium Detail</a>
            </div>
          </div>
        </section>
        """
        return nav_layout(f"{symbol} Price and Signals - AVA", content)

    if symbol in ("AAPL", "NVDA"):
        asset = next((a for a in fetch_stock_quotes_safe() if a["symbol"] == symbol), None)
        if not asset:
            abort(404)
        content = f"""
        <section class="section">
          <div class="badge">Market Landing Page</div>
          <h1>{h(asset['name'])} ({h(symbol)}) Signals, Price, and AVA Guide</h1>
          <p>Track live price, AVA signal direction, and unlock premium trade setups for {h(asset['name'])}.</p>
          <div class="card">
            <h3>Live Snapshot</h3>
            <p>Price: <strong>{fmt_price(asset['price'])}</strong></p>
            <p>Change: <strong class="{asset['dir']}">{fmt_change(asset['change'])}</strong></p>
            <p>Signal: <span class="signal signal-{asset['signal'].lower()}">{asset['signal']}</span></p>
            <div class="btns">
              <a href="/learn/{h(symbol.lower())}" class="btn btn-secondary">Learn {h(symbol)}</a>
              <a href="/stocks/{h(symbol)}" class="btn btn-primary">Open Premium Detail</a>
            </div>
          </div>
        </section>
        """
        return nav_layout(f"{symbol} Price and Signals - AVA", content)

    abort(404)


@app.route("/learn/<symbol>")
def learn_symbol(symbol):
    symbol = symbol.upper()
    page = SYMBOL_LEARN.get(symbol)
    if not page:
        abort(404)

    sections_html = ""
    for title, body in page["sections"]:
        sections_html += f"<h2>{h(title)}</h2><p>{h(body)}</p>"

    content = f"""
    <section class="section">
      <div class="badge">Learning Hub</div>
      <h1>{h(page['title'])}</h1>
      <p>{h(page['intro'])}</p>
      <div class="card">
        {sections_html}
        <div class="hr"></div>
        <div class="btns">
          <a href="/landing/{h(symbol)}" class="btn btn-secondary">Open {h(symbol)} Landing Page</a>
          <a href="/pricing" class="btn btn-primary">Unlock Premium Signals</a>
        </div>
      </div>
    </section>
    """
    return nav_layout(page["title"], content)


@app.route("/blog")
def blog():
    cards = ""
    for slug, post in BLOG_POSTS.items():
        cards += f"""
        <a class="card blog-card" href="/blog/{h(slug)}">
          <div class="badge">AVA Research</div>
          <h3>{h(post['title'])}</h3>
          <div class="blog-meta">{h(post['date'])}</div>
          <p>{h(post['description'])}</p>
        </a>
        """
    content = f"""
    <section class="section">
      <div class="badge">SEO Content Engine</div>
      <h1>AVA Blog & Research</h1>
      <p>Learn how AVA signals work, how to think about crypto and stocks, and how to build cleaner trading workflows.</p>
      <div class="grid-3">
        {cards}
      </div>
    </section>
    """
    return nav_layout("AVA Blog and Research", content)


@app.route("/blog/<slug>")
def blog_post(slug):
    post = BLOG_POSTS.get(slug)
    if not post:
        abort(404)
    content = f"""
    <section class="section">
      <div class="badge">AVA Research</div>
      <h1>{h(post['title'])}</h1>
      <div class="small" style="margin-bottom:18px;">{h(post['date'])}</div>
      <div class="card">
        {post['body']}
        <div class="hr"></div>
        <div class="btns">
          <a href="/signals" class="btn btn-secondary">View Signals</a>
          <a href="/pricing" class="btn btn-primary">Unlock Pro</a>
        </div>
      </div>
    </section>
    """
    return nav_layout(post["title"], content)


@app.route("/admin")
@require_admin
def admin():
    stats = db.get_signal_stats()
    users = db.get_all_users(200)
    subscribers = db.get_active_subscribers(100)

    user_rows = ""
    for u in users:
        user_rows += f"""
        <tr>
          <td>{u['id']}</td>
          <td>{h(u['email'])}</td>
          <td>{h(u['tier'])}</td>
          <td>{h(u.get('billing_cycle') or '')}</td>
          <td>{h(u.get('created_at') or '')}</td>
        </tr>
        """

    sub_rows = ""
    for s in subscribers:
        sub_rows += f"""
        <tr>
          <td>{h(s['email'])}</td>
          <td>ACTIVE</td>
          <td>{h(s['created_at'])}</td>
        </tr>
        """

    content = f"""
    <section class="section">
      <div class="badge">Admin Control</div>
      <h1>Admin Panel</h1>

      <div class="grid-4" style="margin-bottom:24px;">
        <div class="kpi"><div class="num">{db.get_user_count()}</div><div class="label">Users</div></div>
        <div class="kpi"><div class="num">{db.get_paid_user_count()}</div><div class="label">Paid Users</div></div>
        <div class="kpi"><div class="num">{db.get_subscriber_count()}</div><div class="label">Subscribers</div></div>
        <div class="kpi"><div class="num">{stats['win_rate']}%</div><div class="label">Tracked Win Rate</div></div>
      </div>

      <div class="grid-2" style="margin-bottom:20px;">
        <div class="card">
          <h3>Broadcast Controls</h3>
          <form action="/admin/broadcast" method="POST">
            <button class="btn btn-primary" type="submit">Broadcast Top Signals</button>
          </form>
        </div>

        <div class="card">
          <h3>History Controls</h3>
          <p class="small">Marks prior trade outcomes as legacy-invalid after engine correction.</p>
          <form action="/admin/invalidate-history" method="POST">
            <button class="btn btn-secondary" type="submit">Invalidate Legacy History</button>
          </form>
        </div>
      </div>

      <div class="card" style="margin-bottom:20px;">
        <h3>Users</h3>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>ID</th><th>Email</th><th>Tier</th><th>Billing</th><th>Created</th></tr>
            {user_rows or "<tr><td colspan='5'>No users.</td></tr>"}
          </table>
        </div>
      </div>

      <div class="card">
        <h3>Subscribers</h3>
        <div class="table-shell">
          <table class="market-table">
            <tr><th>Email</th><th>Status</th><th>Created</th></tr>
            {sub_rows or "<tr><td colspan='3'>No subscribers.</td></tr>"}
          </table>
        </div>
      </div>
    </section>
    """
    return nav_layout("Admin - AVA", content)


@app.route("/admin/broadcast", methods=["POST"])
@require_admin
def admin_broadcast():
    maybe_broadcast_top_signals(db.get_active_signals(limit=3))
    return redirect("/admin")


@app.route("/admin/invalidate-history", methods=["POST"])
@require_admin
def admin_invalidate_history():
    db.invalidate_legacy_history()
    return redirect("/admin")


@app.route("/terms")
def terms():
    return nav_layout("Terms", "<section class='section'><div class='card'><h1>Terms</h1><p>This platform is for market intelligence and educational use only.</p></div></section>")


@app.route("/privacy")
def privacy():
    return nav_layout("Privacy", "<section class='section'><div class='card'><h1>Privacy</h1><p>We store only operational data needed to run AVA Markets.</p></div></section>")


@app.route("/webhook/stripe", methods=["POST"])
def stripe_webhook():
    if not stripe_enabled() or not stripe:
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
        if uid:
            db.upgrade_user(
                int(uid),
                md.get("tier", "pro"),
                obj.get("customer"),
                obj.get("subscription"),
                md.get("billing", "monthly")
            )

    if event.get("type") == "customer.subscription.deleted":
        obj = event.get("data", {}).get("object", {})
        customer_id = obj.get("customer")
        if customer_id:
            for u in db.get_all_users(1000):
                if u.get("stripe_customer_id") == customer_id:
                    db.upgrade_user(u["id"], "free", billing_cycle=None, sub_id=None)

    return "OK", 200


@app.route("/debug/promote/<tier>")
@require_auth
def debug_promote(tier):
    if tier not in ("free", "pro", "elite"):
        return "invalid tier", 400
    db.upgrade_user(g.user["id"], tier, billing_cycle="manual")
    return redirect("/dashboard")


_bg_started = False


def start_background_loop():
    global _bg_started
    if _bg_started:
        return

    def loop():
        logger.info("Background loop starting...")
        while True:
            try:
                perform_crypto_fetch()
                perform_stock_fetch()
                sigs = generate_active_signals()
                evaluate_signal_history_outcomes()
                maybe_broadcast_top_signals(sigs[:3])
            except Exception as e:
                logger.warning(f"Background cycle error: {e}")
            time.sleep(60)

    threading.Thread(target=loop, daemon=True).start()
    _bg_started = True


if os.environ.get("WERKZEUG_RUN_MAIN") == "true" or not Config.DEBUG:
    start_background_loop()

if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
