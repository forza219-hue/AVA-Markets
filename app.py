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
import requests
import yfinance as yf
from datetime import datetime, timedelta
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
    DATABASE = os.environ.get("DATABASE_URL", "ava_markets_core.db")
    SECRET_KEY = os.environ.get("SECRET_KEY", secrets.token_hex(32))
    DOMAIN = os.environ.get("DOMAIN", f"http://localhost:{PORT}")

    STRIPE_SECRET_KEY = os.environ.get("STRIPE_SECRET_KEY", "")
    STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    ADMIN_USERNAME = os.environ.get("ADMIN_USERNAME", "admin").strip()
    ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "AvaAdmin2024!").strip()

    CONTACT_EMAIL = os.environ.get("CONTACT_EMAIL", "hello@avamarkets.com")

    TIERS = {
        "free": {"price": 0},
        "basic": {"price": 9},
        "pro": {"price": 29},
        "elite": {"price": 79},
        "enterprise": {"price": None},
    }

    PAGE_SIZE_CRYPTO = 25
    PAGE_SIZE_STOCKS = 20


if stripe and Config.STRIPE_SECRET_KEY:
    stripe.api_key = Config.STRIPE_SECRET_KEY

app = Flask(__name__)
app.config["SECRET_KEY"] = Config.SECRET_KEY


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
.asset-row{
  display:flex;align-items:center;gap:10px;
}
.asset-logo{
  width:24px;height:24px;border-radius:50%;object-fit:cover;
  background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.08);
  flex-shrink:0;
}
.asset-icon{
  width:24px;height:24px;display:inline-flex;align-items:center;justify-content:center;
  font-size:1rem;flex-shrink:0;
}
.asset-feature{
  display:flex;align-items:center;gap:12px;
}
.asset-feature-logo{
  width:32px;height:32px;border-radius:50%;object-fit:cover;
  background:rgba(255,255,255,.06);
  border:1px solid rgba(255,255,255,.08);
}
.asset-feature-icon{
  width:32px;height:32px;display:inline-flex;align-items:center;justify-content:center;
  font-size:1.25rem;
}
.asset-subtitle{
  color:var(--muted);font-size:.92rem;margin-top:4px;
}
.featured-shell{
  display:flex;flex-direction:column;gap:10px;
}
.live-stamp{
  color:var(--muted);font-size:.88rem;margin-top:10px
}
ul{color:var(--muted)}
@media (max-width: 900px){
  .hero,.detail-grid,.mini-grid{grid-template-columns:1fr}
  .nav{flex-direction:column;gap:14px}
  .nav-links{justify-content:center}
}
"""


CRYPTO_TOP_90 = [
    ("BTC", "Bitcoin"),
    ("ETH", "Ethereum"),
    ("BNB", "BNB"),
    ("SOL", "Solana"),
    ("XRP", "XRP"),
    ("DOGE", "Dogecoin"),
    ("ADA", "Cardano"),
    ("AVAX", "Avalanche"),
    ("LINK", "Chainlink"),
    ("DOT", "Polkadot"),
    ("MATIC", "Polygon"),
    ("LTC", "Litecoin"),
    ("BCH", "Bitcoin Cash"),
    ("ATOM", "Cosmos"),
    ("UNI", "Uniswap"),
    ("NEAR", "NEAR Protocol"),
    ("APT", "Aptos"),
    ("ARB", "Arbitrum"),
    ("OP", "Optimism"),
    ("SUI", "Sui"),
    ("PEPE", "Pepe"),
    ("SHIB", "Shiba Inu"),
    ("TRX", "TRON"),
    ("ETC", "Ethereum Classic"),
    ("XLM", "Stellar"),
    ("HBAR", "Hedera"),
    ("ICP", "Internet Computer"),
    ("FIL", "Filecoin"),
    ("INJ", "Injective"),
    ("RNDR", "Render"),
    ("TAO", "Bittensor"),
    ("IMX", "Immutable"),
    ("SEI", "Sei"),
    ("TIA", "Celestia"),
    ("JUP", "Jupiter"),
    ("PYTH", "Pyth Network"),
    ("BONK", "Bonk"),
    ("WIF", "dogwifhat"),
    ("FET", "Fetch.ai"),
    ("RUNE", "THORChain"),
    ("AAVE", "Aave"),
    ("MKR", "Maker"),
    ("ALGO", "Algorand"),
    ("VET", "VeChain"),
    ("EGLD", "MultiversX"),
    ("THETA", "Theta Network"),
    ("SAND", "The Sandbox"),
    ("MANA", "Decentraland"),
    ("AXS", "Axie Infinity"),
    ("GRT", "The Graph"),
    ("FLOW", "Flow"),
    ("KAS", "Kaspa"),
    ("KAVA", "Kava"),
    ("DYDX", "dYdX"),
    ("WLD", "Worldcoin"),
    ("ARKM", "Arkham"),
    ("STRK", "Starknet"),
    ("ENA", "Ethena"),
    ("ONDO", "Ondo"),
    ("JASMY", "JasmyCoin"),
    ("LDO", "Lido DAO"),
    ("CRV", "Curve DAO Token"),
    ("SNX", "Synthetix"),
    ("COMP", "Compound"),
    ("1INCH", "1inch"),
    ("BAT", "Basic Attention Token"),
    ("ZEC", "Zcash"),
    ("DASH", "Dash"),
    ("CHZ", "Chiliz"),
    ("ROSE", "Oasis"),
    ("QTUM", "Qtum"),
    ("IOTA", "IOTA"),
    ("ZIL", "Zilliqa"),
    ("KSM", "Kusama"),
    ("GMT", "STEPN"),
    ("BLUR", "Blur"),
    ("ACE", "Fusionist"),
    ("NEO", "NEO"),
    ("CFX", "Conflux"),
    ("FTM", "Fantom"),
    ("GALA", "Gala"),
    ("LRC", "Loopring"),
    ("ENS", "Ethereum Name Service"),
    ("SXP", "Solar"),
    ("HOT", "Holo"),
    ("ANKR", "Ankr"),
    ("ICX", "ICON"),
    ("SC", "Siacoin"),
    ("CKB", "Nervos Network"),
    ("MASK", "Mask Network"),
    ("YFI", "yearn.finance"),
    ("WOO", "WOO"),
    ("SKL", "SKALE"),
]

CRYPTO_NAME_MAP = {symbol: name for symbol, name in CRYPTO_TOP_90}
CRYPTO_ALLOWED_SYMBOLS = set(CRYPTO_NAME_MAP.keys())

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

REQUESTS_TIMEOUT = int(os.environ.get("REQUESTS_TIMEOUT", "12"))
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
TWELVEDATA_API_KEY = os.environ.get("TWELVEDATA_API_KEY", "").strip()

COINGECKO_IDS = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "BNB": "binancecoin",
    "SOL": "solana",
    "XRP": "ripple",
    "DOGE": "dogecoin",
    "ADA": "cardano",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "DOT": "polkadot",
    "MATIC": "matic-network",
    "LTC": "litecoin",
    "BCH": "bitcoin-cash",
    "ATOM": "cosmos",
    "UNI": "uniswap",
    "NEAR": "near",
    "APT": "aptos",
    "ARB": "arbitrum",
    "OP": "optimism",
    "SUI": "sui",
    "PEPE": "pepe",
    "SHIB": "shiba-inu",
    "TRX": "tron",
    "ETC": "ethereum-classic",
    "XLM": "stellar",
    "HBAR": "hedera-hashgraph",
    "ICP": "internet-computer",
    "FIL": "filecoin",
    "INJ": "injective-protocol",
    "RNDR": "render-token",
    "TAO": "bittensor",
    "IMX": "immutable-x",
    "SEI": "sei-network",
    "TIA": "celestia",
    "JUP": "jupiter-exchange-solana",
    "PYTH": "pyth-network",
    "BONK": "bonk",
    "WIF": "dogwifcoin",
    "FET": "fetch-ai",
    "RUNE": "thorchain",
    "AAVE": "aave",
    "MKR": "maker",
    "ALGO": "algorand",
    "VET": "vechain",
    "EGLD": "elrond-erd-2",
    "THETA": "theta-token",
    "SAND": "the-sandbox",
    "MANA": "decentraland",
    "AXS": "axie-infinity",
    "GRT": "the-graph",
    "FLOW": "flow",
    "KAS": "kaspa",
    "KAVA": "kava",
    "DYDX": "dydx-chain",
    "WLD": "worldcoin-wld",
    "ARKM": "arkham",
    "STRK": "starknet",
    "ENA": "ethena",
    "ONDO": "ondo-finance",
    "JASMY": "jasmycoin",
    "LDO": "lido-dao",
    "CRV": "curve-dao-token",
    "SNX": "havven",
    "COMP": "compound-governance-token",
    "1INCH": "1inch",
    "BAT": "basic-attention-token",
    "ZEC": "zcash",
    "DASH": "dash",
    "CHZ": "chiliz",
    "ROSE": "oasis-network",
    "QTUM": "qtum",
    "IOTA": "iota",
    "ZIL": "zilliqa",
    "KSM": "kusama",
    "GMT": "stepn",
    "BLUR": "blur",
    "ACE": "fusionist",
    "NEO": "neo",
    "CFX": "conflux-token",
    "FTM": "fantom",
    "GALA": "gala",
    "LRC": "loopring",
    "ENS": "ethereum-name-service",
    "SXP": "solar",
    "HOT": "holotoken",
    "ANKR": "ankr",
    "ICX": "icon",
    "SC": "siacoin",
    "CKB": "nervos-network",
    "MASK": "mask-network",
    "YFI": "yearn-finance",
    "WOO": "woo-network",
    "SKL": "skale",
}

KRAKEN_PAIRS = {
    "BTC": "XBTUSD",
    "ETH": "ETHUSD",
    "SOL": "SOLUSD",
    "XRP": "XRPUSD",
    "DOGE": "DOGEUSD",
    "ADA": "ADAUSD",
    "AVAX": "AVAXUSD",
    "DOT": "DOTUSD",
    "LINK": "LINKUSD",
    "LTC": "LTCUSD",
    "BCH": "BCHUSD",
    "ATOM": "ATOMUSD",
    "UNI": "UNIUSD",
    "TRX": "TRXUSD",
    "ETC": "ETCUSD",
    "XLM": "XLMUSD",
    "AAVE": "AAVEUSD",
    "COMP": "COMPUSD",
    "MKR": "MKRUSD",
    "ZEC": "ZECUSD",
    "DASH": "DASHUSD",
    "KSM": "KSMUSD",
}

KRAKEN_OHLC_MAP = {
    "BTC": "XXBTZUSD",
    "ETH": "XETHZUSD",
    "SOL": "SOLUSD",
    "XRP": "XXRPZUSD",
    "DOGE": "XDGUSD",
    "ADA": "ADAUSD",
    "AVAX": "AVAXUSD",
    "DOT": "DOTUSD",
    "LINK": "LINKUSD",
    "LTC": "XLTCZUSD",
    "BCH": "BCHUSD",
    "ATOM": "ATOMUSD",
    "UNI": "UNIUSD",
    "TRX": "TRXUSD",
    "ETC": "XETCZUSD",
    "XLM": "XXLMZUSD",
}


def get_stock_logo(symbol):
    domain = STOCK_DOMAINS.get(symbol.upper())
    if domain:
        return f"https://logo.clearbit.com/{domain}"
    return None


def get_crypto_logo(symbol):
    return f"https://cryptoicons.org/api/icon/{symbol.lower()}/200"


def get_asset_icon(symbol):
    mapping = {"GC=F": "🥇", "SI=F": "🥈", "PL=F": "🔘", "CL=F": "🛢️", "SIG": "💎"}
    return mapping.get(symbol.upper(), "📈")


FALLBACK_STOCKS = [
    {"symbol": "AAPL", "name": "Apple", "price": 211.42, "change": 1.12, "dir": "up", "signal": "BUY", "logo": get_stock_logo("AAPL"), "icon": get_asset_icon("AAPL")},
    {"symbol": "MSFT", "name": "Microsoft", "price": 428.36, "change": 0.73, "dir": "up", "signal": "BUY", "logo": get_stock_logo("MSFT"), "icon": get_asset_icon("MSFT")},
    {"symbol": "NVDA", "name": "NVIDIA", "price": 924.80, "change": 2.09, "dir": "up", "signal": "BUY", "logo": get_stock_logo("NVDA"), "icon": get_asset_icon("NVDA")},
    {"symbol": "GC=F", "name": "Gold Futures", "price": 2345.20, "change": 0.35, "dir": "up", "signal": "HOLD", "logo": None, "icon": get_asset_icon("GC=F")},
    {"symbol": "CL=F", "name": "Oil Futures", "price": 81.12, "change": -0.72, "dir": "down", "signal": "SELL", "logo": None, "icon": get_asset_icon("CL=F")},
]

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

        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            asset_type TEXT NOT NULL,
            symbol TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, asset_type, symbol)
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
            UNIQUE(snapshot_id, horizon)
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

    def get_open_outcomes(self, limit=250):
        c = self.conn()
        rows = c.execute("""
            SELECT s.*
            FROM signal_snapshots s
            WHERE NOT EXISTS (
                SELECT 1 FROM signal_outcomes o
                WHERE o.snapshot_id = s.id AND o.horizon = '24h'
            )
            ORDER BY s.id DESC
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
    interval_map = {
        "15m": 15,
        "1h": 60,
        "4h": 240,
    }

    pair = KRAKEN_OHLC_MAP.get(symbol.upper())
    kraken_interval = interval_map.get(interval, 15)

    if not pair:
        return None

    try:
        r = requests.get(
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": pair, "interval": kraken_interval},
            timeout=REQUESTS_TIMEOUT
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
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4])
            })

        return candles or None
    except Exception as e:
        logger.warning(f"fetch_crypto_candles failed for {symbol} via Kraken: {e}")
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
    except Exception:
        return None


def fetch_crypto_multi_timeframe(symbol):
    return {
        "15m": fetch_crypto_candles(symbol, interval="15m", limit=80),
        "1h": fetch_crypto_candles(symbol, interval="1h", limit=80),
        "4h": fetch_crypto_candles(symbol, interval="4h", limit=80),
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
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr = max(h - l, abs(h - pc), abs(l - pc))
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


def fetch_crypto_from_coingecko():
    symbol_to_id = {s: COINGECKO_IDS[s] for s, _ in CRYPTO_TOP_90 if s in COINGECKO_IDS}
    if not symbol_to_id:
        return []

    ids_needed = list(symbol_to_id.values())
    reverse_ids = {v: k for k, v in symbol_to_id.items()}
    results = []
    chunk_size = 40

    for i in range(0, len(ids_needed), chunk_size):
        chunk = ids_needed[i:i + chunk_size]
        r = requests.get(
            "https://api.coingecko.com/api/v3/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ",".join(chunk),
                "order": "market_cap_desc",
                "per_page": len(chunk),
                "page": 1,
                "sparkline": "false",
                "price_change_percentage": "24h"
            },
            timeout=REQUESTS_TIMEOUT,
            headers={"accept": "application/json"}
        )
        r.raise_for_status()
        data = r.json()

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
                "logo": get_crypto_logo(symbol),
            })

        time.sleep(0.15)

    payload_map = {item["symbol"]: item for item in results}
    ordered = [payload_map[symbol] for symbol, _ in CRYPTO_TOP_90 if symbol in payload_map]
    return ordered


def fetch_crypto_from_kraken():
    if not KRAKEN_PAIRS:
        return []

    reverse_pairs = {v: k for k, v in KRAKEN_PAIRS.items()}
    pair_list = list(KRAKEN_PAIRS.values())
    results = []
    chunk_size = 15

    for i in range(0, len(pair_list), chunk_size):
        chunk = pair_list[i:i + chunk_size]
        r = requests.get(
            "https://api.kraken.com/0/public/Ticker",
            params={"pair": ",".join(chunk)},
            timeout=REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        payload = r.json()

        if payload.get("error"):
            raise Exception(str(payload["error"]))

        raw = payload.get("result", {})
        for result_key, value in raw.items():
            symbol = None
            for pair_code, sym in reverse_pairs.items():
                if result_key == pair_code or pair_code in result_key:
                    symbol = sym
                    break

            if not symbol:
                continue

            try:
                price = float(value["c"][0])
                open_price = float(value["o"])
                change = pct_change(price, open_price)
            except Exception:
                continue

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

        time.sleep(0.08)

    deduped = {}
    for item in results:
        deduped[item["symbol"]] = item

    ordered = [deduped[symbol] for symbol, _ in CRYPTO_TOP_90 if symbol in deduped]
    return ordered


def fetch_crypto_quotes_safe():
    try:
        data = fetch_crypto_from_coingecko()
        if data:
            return data
    except Exception as e:
        logger.warning(f"fetch_crypto_quotes_safe CoinGecko failed: {e}")

    try:
        data = fetch_crypto_from_kraken()
        if data:
            return data
    except Exception as e:
        logger.warning(f"fetch_crypto_quotes_safe Kraken failed: {e}")

    logger.warning("fetch_crypto_quotes_safe failed: all crypto providers unavailable")
    return []


def normalize_stock_symbol_for_twelvedata(symbol):
    mapping = {
        "BRK-B": "BRK.B",
    }
    return mapping.get(symbol.upper(), symbol.upper())


def normalize_stock_symbol_for_finnhub(symbol):
    mapping = {
        "BRK-B": "BRK.B",
    }
    return mapping.get(symbol.upper(), symbol.upper())


def fetch_stock_quotes_from_twelvedata():
    if not TWELVEDATA_API_KEY:
        raise Exception("TWELVEDATA_API_KEY not set")

    results = []

    for original_symbol, _name in STOCK_UNIVERSE:
        if "=" in original_symbol:
            continue

        api_symbol = normalize_stock_symbol_for_twelvedata(original_symbol)

        r = requests.get(
            "https://api.twelvedata.com/quote",
            params={
                "symbol": api_symbol,
                "apikey": TWELVEDATA_API_KEY,
            },
            timeout=REQUESTS_TIMEOUT
        )
        r.raise_for_status()
        item = r.json()

        if isinstance(item, dict) and item.get("code"):
            continue

        try:
            price = float(item.get("close") or 0)
            percent_change_raw = item.get("percent_change")
            if percent_change_raw in [None, ""]:
                change = 0.0
            else:
                change = float(str(percent_change_raw).replace("%", ""))
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

        time.sleep(0.12)

    return results


def fetch_stock_quotes_from_finnhub():
    if not FINNHUB_API_KEY:
        raise Exception("FINNHUB_API_KEY not set")

    payload = []

    for symbol, name in STOCK_UNIVERSE:
        if "=" in symbol:
            continue

        api_symbol = normalize_stock_symbol_for_finnhub(symbol)

        r = requests.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": api_symbol, "token": FINNHUB_API_KEY},
            timeout=REQUESTS_TIMEOUT
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
            "name": STOCK_NAME_MAP.get(symbol, name),
            "price": current_price,
            "change": change,
            "dir": "up" if change >= 0 else "down",
            "signal": compute_light_signal(change),
            "logo": get_stock_logo(symbol),
            "icon": get_asset_icon(symbol),
        })

        time.sleep(0.12)

    return payload


def fetch_stock_quotes_safe():
    try:
        payload = fetch_stock_quotes_from_twelvedata()
        if payload:
            existing = {x["symbol"] for x in payload}
            for item in FALLBACK_STOCKS:
                if item["symbol"] not in existing:
                    payload.append(dict(item))
            return payload
    except Exception as e:
        logger.warning(f"fetch_stock_quotes_safe failed via Twelve Data: {e}")

    try:
        payload = fetch_stock_quotes_from_finnhub()
        if payload:
            existing = {x["symbol"] for x in payload}
            for item in FALLBACK_STOCKS:
                if item["symbol"] not in existing:
                    payload.append(dict(item))
            return payload
    except Exception as e:
        logger.warning(f"fetch_stock_quotes_safe failed via Finnhub: {e}")

    return FALLBACK_STOCKS.copy()


def paginate(items, page, per_page):
    total = len(items)
    pages = max(1, math.ceil(total / per_page)) if total > 0 else 1
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], total, pages, page


def get_crypto_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in fetch_crypto_quotes_safe()}
    if symbol not in light_map:
        return None

    asset = dict(light_map[symbol])
    tf_map = fetch_crypto_multi_timeframe(symbol)
    primary_candles = tf_map.get("15m") or []
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


def get_stock_detail(symbol):
    symbol = symbol.upper()
    light_map = {a["symbol"]: a for a in fetch_stock_quotes_safe()}
    if symbol not in light_map:
        return None

    asset = dict(light_map[symbol])
    tf_map = fetch_stock_multi_timeframe(symbol)
    primary_candles = tf_map.get("1d") or []
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


def get_latest_price(asset_type, symbol):
    if asset_type == "crypto":
        light = {a["symbol"]: a for a in fetch_crypto_quotes_safe()}
    else:
        light = {a["symbol"]: a for a in fetch_stock_quotes_safe()}
    item = light.get(symbol.upper())
    return item["price"] if item else None


def parse_sqlite_dt(v):
    try:
        return datetime.fromisoformat(str(v).replace(" ", "T"))
    except Exception:
        return None


def refresh_signal_outcomes():
    rows = db.get_open_outcomes(limit=200)
    now = datetime.now()
    for row in rows:
        created = parse_sqlite_dt(row["created_at"])
        if not created:
            continue
        age_hours = (now - created).total_seconds() / 3600.0
        price_now = get_latest_price(row["asset_type"], row["symbol"])
        if not price_now:
            continue

        if age_hours >= 1:
            ret, outcome = evaluate_signal_outcome(row["signal"], row["price_at_signal"], price_now)
            if ret is not None:
                db.insert_signal_outcome(row["id"], "1h", price_now, ret, outcome)

        if age_hours >= 24:
            ret, outcome = evaluate_signal_outcome(row["signal"], row["price_at_signal"], price_now)
            if ret is not None:
                db.insert_signal_outcome(row["id"], "24h", price_now, ret, outcome)

        if age_hours >= 24 * 7:
            ret, outcome = evaluate_signal_outcome(row["signal"], row["price_at_signal"], price_now)
            if ret is not None:
                db.insert_signal_outcome(row["id"], "7d", price_now, ret, outcome)


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
setInterval(refreshCryptoList, 15000);
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
        interval = 15000 if asset_type == "crypto" else 45000
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
  <meta name="google-site-verification" content="39xa6RndNqbrq7XCh_9JZQkWBoRKAJlghz8ieHcV2v4" />
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
        "/",
        "/crypto",
        "/stocks",
        "/forecast",
        "/trends",
        "/performance",
        "/signal-changes",
        "/pricing",
        "/login",
        "/register",
    ]

    crypto_assets = fetch_crypto_quotes_safe()[:50]
    stock_assets = fetch_stock_quotes_safe()[:50]

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
    <loc>{u['loc']}</loc>
    <lastmod>{u['lastmod']}</lastmod>
    <changefreq>{u['changefreq']}</changefreq>
    <priority>{u['priority']}</priority>
  </url>
        """)

    xml.append("</urlset>")
    resp = make_response("".join(xml))
    resp.headers["Content-Type"] = "application/xml; charset=utf-8"
    return resp


@app.route("/api/live/crypto-list")
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
        "symbol": "AAPL",
        "name": "Apple",
        "price": 0.0,
        "change": 0.0,
        "dir": "up",
        "signal": "HOLD",
        "logo": get_stock_logo("AAPL"),
        "icon": get_asset_icon("AAPL"),
    }

    featured_crypto["price_display"] = fmt_price(featured_crypto["price"], featured_crypto["symbol"])
    featured_crypto["change_display"] = fmt_change(featured_crypto["change"])
    featured_stock["price_display"] = fmt_price(featured_stock["price"], featured_stock["symbol"])
    featured_stock["change_display"] = fmt_change(featured_stock["change"])

    crypto_candles = None
    if featured_crypto["symbol"] != "N/A":
        crypto_candles = fetch_crypto_candles(featured_crypto["symbol"], interval="15m", limit=40)

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
            <h2 style="margin:0;"><a href="/stocks/{{ featured_stock.symbol }}">{{ featured_stock.symbol }}</a> — {{ featured_stock.name }}</h2>
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

    what_is_block = """
    <div class="card" style="margin-top:24px;">
      <h2>What is AVA Markets?</h2>
      <p>
        AVA Markets is a market intelligence platform that helps users monitor crypto, stocks,
        and commodities through live pricing, technical signals, trend analysis, and forecast views.
        The platform is designed to make multi-timeframe market analysis easier to understand.
      </p>
    </div>
    """

    why_users_block = """
    <div class="card" style="margin-top:24px;">
      <h2>Why users search for AVA Markets</h2>
      <p>
        Traders and market watchers use AVA Markets to find crypto signals, stock signals,
        market forecasts, buy-hold-sell views, trend analysis, and transparent performance data
        without the clutter of traditional charting platforms.
      </p>
    </div>
    """

    full_content = content + seo_block + popular_links + what_is_block + why_users_block

    return nav_layout(
        "Crypto Signals, Stock Signals, Trends & Forecasts - AVA Markets",
        full_content,
        meta_description="AVA Markets is a live-updating crypto and stock signals platform with market forecasts, trend analysis, buy-hold-sell views, and transparent performance tracking.",
        json_ld_override=homepage_json_ld()
    )

@app.route("/crypto")
def crypto():
    page = int(request.args.get("page", 1) or 1)
    search = (request.args.get("q") or "").strip().lower()

    assets = [dict(a) for a in fetch_crypto_quotes_safe()]
    for a in assets:
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])

    if search:
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]

    if not assets:
        assets = []

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_CRYPTO)
    unlocked = can_access_signals()

    rows = ""
    for a in page_items:
        sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[a["signal"]]
        signal_html = f'<span id="signal-{a["symbol"]}" class="signal {sig_class}">{a["signal"]}</span>' if unlocked else '<span class="signal signal-locked">Locked</span>'
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">
              <img class="asset-logo" src="{a.get('logo', '')}" alt="{a['symbol']}" onerror="this.style.display='none'">
              <a href="/crypto/{a['symbol']}">{a['symbol']}</a>
            </strong>
            <span>{a['name']}</span>
          </td>
          <td id="price-{a['symbol']}">{a['price_display']}</td>
          <td id="change-{a['symbol']}" class="{'up' if a['dir']=='up' else 'down'}">{a['change_display']}</td>
          <td>{signal_html}</td>
          <td>{"Unlocked" if unlocked else "Basic+"}</td>
        </tr>
        """

    pagination = ""
    if current > 1:
        pagination += f'<a class="page-link" href="/crypto?page={current-1}&q={search}">Previous</a>'
    pagination += f'<span class="page-link">Page {current} / {pages}</span>'
    if current < pages:
        pagination += f'<a class="page-link" href="/crypto?page={current+1}&q={search}">Next</a>'

    content = f"""
    <section class="section">
      <h1>Crypto</h1>
      <p class="section-sub">Top crypto assets with live-updating previews and premium multi-timeframe detail pages.</p>
      <div id="live-updated-crypto" class="live-stamp">Last updated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC</div>

      <div class="card" style="margin-bottom:24px;">
        <h2>Crypto market signals and trend analysis</h2>
        <p>
          Browse crypto market signals across major assets including Bitcoin, Ethereum, Solana,
          BNB, XRP, and more. AVA Markets combines live price movement, signal previews,
          multi-timeframe analysis, and forecast intelligence to help users understand
          crypto market structure more clearly.
        </p>
      </div>

      <form method="GET" style="margin-bottom:20px;">
        <input class="search-box" type="text" name="q" placeholder="Search crypto symbol or name..." value="{search}">
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

    crypto_breadcrumbs = build_breadcrumbs([
        {"name": "Home", "url": Config.DOMAIN + "/"},
        {"name": "Crypto", "url": Config.DOMAIN + "/crypto"}
    ])

    return nav_layout(
        "Crypto Market Signals - AVA Markets",
        content,
        extra_head=live_update_script("crypto_list"),
        meta_description="Browse live-updating crypto markets on AVA Markets with price movement previews, premium multi-timeframe signals, and detailed asset intelligence.",
        json_ld_override=crypto_breadcrumbs
    )


@app.route("/stocks")
def stocks():
    page = int(request.args.get("page", 1) or 1)
    search = (request.args.get("q") or "").strip().lower()

    assets = [dict(a) for a in fetch_stock_quotes_safe()]
    for a in assets:
        a["price_display"] = fmt_price(a["price"], a["symbol"])
        a["change_display"] = fmt_change(a["change"])

    if search:
        assets = [a for a in assets if search in a["symbol"].lower() or search in a["name"].lower()]

    if not assets:
        assets = [dict(x) for x in FALLBACK_STOCKS]
        for a in assets:
            a["price_display"] = fmt_price(a["price"], a["symbol"])
            a["change_display"] = fmt_change(a["change"])

    page_items, total, pages, current = paginate(assets, page, Config.PAGE_SIZE_STOCKS)
    unlocked = can_access_signals()

    rows = ""
    for a in page_items:
        safe_id = a["symbol"].replace("=", "_").replace("-", "_")
        media = (
            f'<img class="asset-logo" src="{a.get("logo","")}" alt="{a["symbol"]}" onerror="this.style.display=\'none\'">'
            if a.get("logo") else
            f'<span class="asset-icon">{a.get("icon","📈")}</span>'
        )
        sig_class = {"BUY": "signal-buy", "HOLD": "signal-hold", "SELL": "signal-sell"}[a["signal"]]
        signal_html = f'<span id="signal-{safe_id}" class="signal {sig_class}">{a["signal"]}</span>' if unlocked else '<span class="signal signal-locked">Locked</span>'
        rows += f"""
        <tr>
          <td class="asset-name">
            <strong class="asset-row">
              {media}
              <a href="/stocks/{a['symbol']}">{a['symbol']}</a>
            </strong>
            <span>{a['name']}</span>
          </td>
          <td id="price-{safe_id}">{a['price_display']}</td>
          <td id="change-{safe_id}" class="{'up' if a['dir']=='up' else 'down'}">{a['change_display']}</td>
          <td>{signal_html}</td>
          <td>{"Unlocked" if unlocked else "Basic+"}</td>
        </tr>
        """

    pagination = ""
    if current > 1:
        pagination += f'<a class="page-link" href="/stocks?page={current-1}&q={search}">Previous</a>'
    pagination += f'<span class="page-link">Page {current} / {pages}</span>'
    if current < pages:
        pagination += f'<a class="page-link" href="/stocks?page={current+1}&q={search}">Next</a>'

    content = f"""
    <section class="section">
      <h1>Stocks + Commodities</h1>
      <p class="section-sub">Global market list with live-updating previews and premium multi-timeframe detail analysis.</p>
      <div id="live-updated-stocks" class="live-stamp">Last updated: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")} UTC</div>

      <div class="card" style="margin-bottom:24px;">
        <h2>Stock signals, commodity signals, and trend intelligence</h2>
        <p>
          Explore stock market signals and commodity trend analysis across major names such as
          Apple, Microsoft, NVIDIA, Tesla, Amazon, and global assets like gold and oil.
          AVA Markets helps users inspect live pricing, technical signal structure, trend regime,
          and forecast views in one dashboard.
        </p>
      </div>

      <form method="GET" style="margin-bottom:20px;">
        <input class="search-box" type="text" name="q" placeholder="Search stock or commodity..." value="{search}">
      </form>

      <div class="table-shell">
        <table class="market-table">
          <tr><th>Asset</th><th>Price</th><th>1D</th><th>Preview Signal</th><th>Access</th></tr>
          {rows}
        </table>
      </div>

      <div class="pagination">{pagination}</div>
      {legal_disclaimer_html()}
    </section>
    """

    stock_breadcrumbs = build_breadcrumbs([
        {"name": "Home", "url": Config.DOMAIN + "/"},
        {"name": "Stocks", "url": Config.DOMAIN + "/stocks"}
    ])

    return nav_layout(
        "Stocks and Commodities Signals - AVA Markets",
        content,
        extra_head=live_update_script("stock_list"),
        meta_description="Explore stocks and commodities on AVA Markets with live-updating prices, market previews, premium multi-timeframe signals, trend analysis, and forecasts.",
        json_ld_override=stock_breadcrumbs
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
    signal_html = f'<span id="detail-signal" class="signal {sig_class}">{asset["signal"]}</span>' if unlocked_signals else '<span class="signal signal-locked">Locked</span>'

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

      <div class="card" style="margin-bottom:24px;">
        <p>
          AVA Markets tracks {{ asset.name }} ({{ asset.symbol }}) with live price updates,
          multi-timeframe crypto signals, trend regime analysis, and forecast intelligence.
          Users can inspect short-term, intraday, and swing signal structure for clearer
          technical market context.
        </p>
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

    crypto_breadcrumbs = build_breadcrumbs([
        {"name": "Home", "url": Config.DOMAIN + "/"},
        {"name": "Crypto", "url": Config.DOMAIN + "/crypto"},
        {"name": asset["symbol"], "url": f"{Config.DOMAIN}/crypto/{asset['symbol']}"}
    ])

    return nav_layout(
        f"{asset['symbol']} Price, Signal & Forecast - AVA Markets",
        content,
        extra_head=live_update_script("detail", asset["symbol"], "crypto"),
        meta_description=f"Track {asset['name']} ({asset['symbol']}) on AVA Markets with live price updates, multi-timeframe signal analysis, trend regime view, and forecast intelligence.",
        canonical_url=f"{Config.DOMAIN}/crypto/{asset['symbol']}",
        og_type="article",
        json_ld_override=[asset_json_ld("crypto", asset), crypto_breadcrumbs]
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
    signal_html = f'<span id="detail-signal" class="signal {sig_class}">{asset["signal"]}</span>' if unlocked_signals else '<span class="signal signal-locked">Locked</span>'

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

      <div class="card" style="margin-bottom:24px;">
        <p>
          AVA Markets tracks {{ asset.name }} ({{ asset.symbol }}) with live price updates,
          multi-timeframe stock and commodity signal analysis, trend regime detection,
          and forecast intelligence for short-term and macro market context.
        </p>
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

    stock_breadcrumbs = build_breadcrumbs([
        {"name": "Home", "url": Config.DOMAIN + "/"},
        {"name": "Stocks", "url": Config.DOMAIN + "/stocks"},
        {"name": asset["symbol"], "url": f"{Config.DOMAIN}/stocks/{asset['symbol']}"}
    ])

    return nav_layout(
        f"{asset['symbol']} Price, Signal & Forecast - AVA Markets",
        content,
        extra_head=live_update_script("detail", asset["symbol"], "stock"),
        meta_description=f"Track {asset['name']} ({asset['symbol']}) on AVA Markets with live price updates, multi-timeframe signal analysis, trend regime view, and forecast intelligence.",
        canonical_url=f"{Config.DOMAIN}/stocks/{asset['symbol']}",
        og_type="article",
        json_ld_override=[asset_json_ld("stock", asset), stock_breadcrumbs]
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
          <div class="price-card"><h3>Trend</h3><div style="font-size:2rem;font-weight:800;" class="up">{f['trend']}</div><p>{f['summary']}</p></div>
          <div class="price-card"><h3>Projected View</h3><div style="font-size:2rem;font-weight:800;" class="up">{f['projected_change']}</div><p>Multi-timeframe projection signal output.</p></div>
          <div class="price-card"><h3>Confidence Band</h3><div style="font-size:2rem;font-weight:800;">{f['confidence_band']}</div><p>Indicator agreement level, not guaranteed outcome probability.</p></div>
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
          <div class="price-card"><h3>Trend State</h3><div style="font-size:2rem;font-weight:800;" class="up">{t['state']}</div><p>{t['summary']}</p></div>
          <div class="price-card"><h3>Trend Strength</h3><div style="font-size:2rem;font-weight:800;">{t['strength']}</div><p>Directional magnitude from the multi-timeframe model.</p></div>
          <div class="price-card"><h3>Regime Read</h3><div style="font-size:2rem;font-weight:800;">{t['read']}</div><p>Current market structure classification.</p></div>
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
        table_rows += f"""
        <tr>
          <td>{r['asset_type']}</td>
          <td>{r['symbol']}</td>
          <td>{r['timeframe']}</td>
          <td>{r['signal_type']}</td>
          <td>{r['signal']}</td>
          <td>{float(r['confidence'] or 0) * 100:.0f}%</td>
          <td class="{outcome_class}">{r['outcome']}</td>
          <td class="{'up' if float(r['return_pct'] or 0) >= 0 else 'down'}">{float(r['return_pct'] or 0):+.2f}%</td>
          <td>{r['horizon']}</td>
          <td>{r['created_at']}</td>
        </tr>
        """

    best_assets_html = "".join(f"<li>{sym}: {ret:+.2f}% avg</li>" for sym, ret in summary["best_assets"]) or "<li>No evaluated signals yet.</li>"

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
          <td>{r['asset_type']}</td>
          <td>{r['symbol']}</td>
          <td>{r['timeframe']}</td>
          <td>{r['signal_type']}</td>
          <td>{r.get('old_signal') or '-'}</td>
          <td>{r.get('new_signal') or '-'}</td>
          <td>{r['changed_at']}</td>
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
    content = render_template_string("""
    <section class="section">
      <h1>Pricing</h1>
      <p class="section-sub">Launch pricing designed to reduce friction while proving trust and retention.</p>
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
          {% if user %}
            <form method="POST" action="/checkout/basic"><button class="btn btn-primary" type="submit">Choose Basic</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Pro</h3>
          <div style="font-size:2.4rem;font-weight:800;">$29</div>
          <p>Everything in Basic • forecasts • trend regime views • better decision context</p>
          {% if user %}
            <form method="POST" action="/checkout/pro"><button class="btn btn-primary" type="submit">Choose Pro</button></form>
          {% endif %}
        </div>

        <div class="price-card">
          <h3>Elite</h3>
          <div style="font-size:2.4rem;font-weight:800;">$79</div>
          <p>Everything in Pro • highest access tier • positioned for serious users</p>
          {% if user %}
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

      <div class="card" style="margin-top:24px;">
        <h2>Pricing FAQ</h2>
        <p><strong>What’s included in Free?</strong><br>Free includes browsing market pages, candles, and movement previews.</p>
        <p><strong>What’s included in Basic?</strong><br>Basic unlocks multi-timeframe crypto and stock signals.</p>
        <p><strong>What’s included in Pro?</strong><br>Pro includes everything in Basic plus forecast and trend intelligence.</p>
        <p><strong>How does Enterprise work?</strong><br>Enterprise is custom for API, team, and advanced commercial use cases.</p>
      </div>

      {{ disclaimer|safe }}
    </section>
    """, user=g.get("user"), contact_email=Config.CONTACT_EMAIL, disclaimer=legal_disclaimer_html())

    pricing_faq = faq_json_ld([
        ("What does the Free plan include?", "The Free plan includes browsing crypto, stocks, commodities, candle views, and market movement previews."),
        ("What does Basic unlock?", "Basic unlocks multi-timeframe signal access for crypto and stocks."),
        ("What does Pro unlock?", "Pro unlocks everything in Basic plus forecast views and trend intelligence."),
        ("Is Enterprise pricing fixed?", "No. Enterprise is custom and designed for API access, teams, and advanced commercial use cases.")
    ])

    return nav_layout(
        "Pricing - AVA Markets",
        content,
        meta_description="View AVA Markets pricing for free market browsing, Basic signal access, Pro forecast and trend intelligence, Elite access, and custom Enterprise plans.",
        json_ld_override=pricing_faq
    )


@app.route("/watchlist/add/<asset_type>/<symbol>", methods=["POST"])
@require_login
def watchlist_add(asset_type, symbol):
    if asset_type not in ["crypto", "stock"]:
        return redirect("/dashboard")
    db.add_watchlist(g.user["id"], asset_type, symbol)
    return redirect(request.referrer or "/dashboard")


@app.route("/watchlist/remove/<asset_type>/<symbol>", methods=["POST"])
@require_login
def watchlist_remove(asset_type, symbol):
    if asset_type not in ["crypto", "stock"]:
        return redirect("/dashboard")
    db.remove_watchlist(g.user["id"], asset_type, symbol)
    return redirect(request.referrer or "/dashboard")


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
        return nav_layout(
            "Register - AVA Markets",
            content,
            meta_description="Create your AVA Markets account.",
            robots_content="noindex, nofollow"
        )

    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()

    if not email or len(password) < 6:
        return nav_layout(
            "Register Error",
            '<div class="form-shell"><div class="form-card"><div class="error">Email and password (min 6 chars) required.</div><a href="/register">Try again</a></div></div>',
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
        return nav_layout(
            "Login - AVA Markets",
            content,
            meta_description="Log in to your AVA Markets account.",
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
    watchlist = db.get_watchlist(user["id"])

    watch_rows = ""
    for item in watchlist[:20]:
        href = f"/crypto/{item['symbol']}" if item["asset_type"] == "crypto" else f"/stocks/{item['symbol']}"
        watch_rows += f"""
        <tr>
          <td>{item['asset_type']}</td>
          <td><a href="{href}">{item['symbol']}</a></td>
          <td>{item['created_at']}</td>
          <td>
            <form method="POST" action="/watchlist/remove/{item['asset_type']}/{item['symbol']}">
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
        meta_description="Your AVA Markets account dashboard.",
        robots_content="noindex, nofollow"
    )


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
        username = (request.form.get("username") or "").strip()
        password = (request.form.get("password") or "").strip()

        if username == Config.ADMIN_USERNAME and password == Config.ADMIN_PASSWORD:
            resp = make_response(redirect("/admin"))
            resp.set_cookie("admin_auth", "1", httponly=True, samesite="Lax", max_age=60 * 60 * 12)
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
        meta_description="Restricted admin login for AVA Markets.",
        robots_content="noindex, nofollow"
    )


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
    return nav_layout(
        "Admin - AVA Markets",
        content,
        meta_description="Restricted admin panel.",
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


try:
    refresh_signal_outcomes()
except Exception as e:
    logger.warning(f"Initial outcome refresh failed: {e}")


if __name__ == "__main__":
    app.run(host=Config.HOST, port=Config.PORT, debug=Config.DEBUG)
