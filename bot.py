# main.py
import os
import time
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List
from itertools import combinations
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading

import aiohttp
import ccxt.async_support as ccxt
from web3 import Web3
from web3.middleware import geth_poa_middleware
from telegram import Bot, Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ---------------- tiny HTTP server so Render keeps the service alive -------------
def run_server():
    server = HTTPServer(('', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# ================== CONFIG (ENV) ==================
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
USER_ID = int(os.environ.get("USER_ID", "5009858379"))

# initial capital (will be updated by morning prompt or /capital)
MY_CAPITAL_USD = float(os.environ.get("MY_CAPITAL_USD", 50.0))

MIN_VOLUME_24H = float(os.environ.get("MIN_VOLUME_24H", 500000))   # USD
MIN_EFF_SPREAD_PERCENT = float(os.environ.get("MIN_EFF_SPREAD_PERCENT", 2.0))
CHECK_INTERVAL_SECONDS = int(os.environ.get("CHECK_INTERVAL_SECONDS", 300))
FUNDING_CHECK_INTERVAL = int(os.environ.get("FUNDING_CHECK_INTERVAL", 300))

# Fees & slippage (defaults; can tune via env)
CEX_FEE_DEFAULT_PCT = float(os.environ.get("CEX_FEE_DEFAULT_PCT", 0.1))
CEX_FEES_OVERRIDE = {
    "bybit": float(os.environ.get("CEX_FEE_BYBIT_PCT", 0.055)),
    "mexc": float(os.environ.get("CEX_FEE_MEXC_PCT", 0.2)),
    "bitget": float(os.environ.get("CEX_FEE_BITGET_PCT", 0.1)),
}
DEX_FEE_PCT = float(os.environ.get("DEX_FEE_PCT", 0.3))
EST_SLIPPAGE_PCT = float(os.environ.get("EST_SLIPPAGE_PCT", 0.3))

ETH_RPC = os.environ.get("ETH_RPC", "https://eth.llamarpc.com")
BSC_RPC = os.environ.get("BSC_RPC", "https://bsc-dataseed.binance.org")
GAS_UPDATE_INTERVAL = int(os.environ.get("GAS_UPDATE_INTERVAL", 3600))
GAS_UNITS_SWAP = int(os.environ.get("GAS_UNITS_SWAP", 200000))
ETH_GWEI_FALLBACK = float(os.environ.get("ETH_GWEI_FALLBACK", 30.0))
BNB_GWEI_FALLBACK = float(os.environ.get("BNB_GWEI_FALLBACK", 5.0))
ETH_PRICE_FALLBACK = float(os.environ.get("ETH_PRICE_FALLBACK", 4619.0))
BNB_PRICE_FALLBACK = float(os.environ.get("BNB_PRICE_FALLBACK", 550.0))

# DEX router addresses
UNISWAP_ROUTER = Web3.to_checksum_address("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
PANCAKE_ROUTER = Web3.to_checksum_address("0x10ED43C718714eb63d5aA57B78B54704E256024E")

# CoinGecko memecoin mapping (add overrides if needed)
COINGECKO_IDS_OVERRIDE = {"TRUMP": "maga"}

# ================== LOGGING & BOT ==================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("arb-bot")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN must be set in ENV")
bot = Bot(token=TELEGRAM_TOKEN)

# ================== GLOBAL STATE ==================
# enabled exchanges (toggle via commands)
ENABLED_EXCHANGES: Dict[str, bool] = {"bybit": True, "mexc": True, "bitget": True}

# CCXT async clients (no api keys needed for public data)
EXCHANGES: Dict[str, ccxt.Exchange] = {
    "bybit": ccxt.bybit({"enableRateLimit": True}),
    "mexc": ccxt.mexc({"enableRateLimit": True}),
    "bitget": ccxt.bitget({"enableRateLimit": True}),
}

# web3
w3_eth = Web3(Web3.HTTPProvider(ETH_RPC))
w3_bsc = Web3(Web3.HTTPProvider(BSC_RPC))
try:
    w3_bsc.middleware_onion.inject(geth_poa_middleware, layer=0)
except Exception:
    pass

# ROUTER ABI (getAmountsOut)
ROUTER_ABI = [{
    "inputs": [{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"address[]","name":"path","type":"address[]"}],
    "name":"getAmountsOut",
    "outputs":[{"internalType":"uint256[]","name":"amounts","type":"uint256[]"}],
    "stateMutability":"view",
    "type":"function"
}]

TOKEN_ABI = [{"inputs":[],"name":"decimals","outputs":[{"internalType":"uint8","name":"","type":"uint8"}],"stateMutability":"view","type":"function"}]

# minimal token map
TOKEN_MAP = {
    "WETH": {"eth": Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), "decimals": 18},
    "USDT": {"eth": Web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7"), "decimals": 6},
    "USDC": {"eth": Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"), "decimals": 6},
    "WBNB": {"bsc": Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"), "decimals": 18},
    "BUSD": {"bsc": Web3.to_checksum_address("0xe9e7cea3dedca5984780bafc599bd69add087d56"), "decimals": 18},
    "USDT_BSC": {"bsc": Web3.to_checksum_address("0x55d398326f99059fF775485246999027B3197955"), "decimals": 18},
}

# caches
COINGECKE_API = "https://api.coingecko.com/api/v3"
COINGECKO_SYMBOL_TO_ID: Dict[str, str] = {}
TOKEN_ADDR_CACHE: Dict[Tuple[str,str], Optional[Tuple[str,int]]] = {}
SIGNAL_CACHE: Dict[str, float] = {}
GAS_FEES_USD = {"ETH": None, "BNB": None}
LAST_GAS_UPDATE = 0

# ================== HELPERS ==================
def pct_between(a: float, b: float) -> Optional[float]:
    if a is None or b is None or (a + b) == 0:
        return None
    return abs((a - b) / ((a + b) / 2) * 100)

def effective_after_costs(raw_pct: float, is_cex_cex: bool, ex_name: str, chain_for_gas: str = "ETH") -> float:
    cex_fee = CEX_FEES_OVERRIDE.get(ex_name.lower(), CEX_FEE_DEFAULT_PCT)
    gas_usd = GAS_FEES_USD.get(chain_for_gas, 0.0) or 0.0
    gas_pct = (gas_usd / MY_CAPITAL_USD) * 100 if MY_CAPITAL_USD and gas_usd else 0.0
    if is_cex_cex:
        fees = (2 * cex_fee) + EST_SLIPPAGE_PCT
    else:
        fees = cex_fee + DEX_FEE_PCT + EST_SLIPPAGE_PCT + gas_pct
    return raw_pct - fees

async def safe_send_html(text: str, dedup_key: Optional[str] = None, dedup_seconds: int = 1800):
    now = time.time()
    if dedup_key:
        last = SIGNAL_CACHE.get(dedup_key)
        if last and (now - last) < dedup_seconds:
            logger.debug("skip duplicate signal %s", dedup_key)
            return
    try:
        await bot.send_message(chat_id=USER_ID, text=text, parse_mode="HTML")
        if dedup_key:
            SIGNAL_CACHE[dedup_key] = now
        logger.info("Sent TG message (%s)", dedup_key or "no-key")
    except Exception as e:
        logger.error("TG send error: %s", e)

# ---------------- CoinGecko helpers ----------------
async def init_coingecko(session: aiohttp.ClientSession):
    global COINGECKO_SYMBOL_TO_ID
    if COINGECKO_SYMBOL_TO_ID:
        return
    url = f"{COINGECKE_API}/coins/list"
    try:
        async with session.get(url, timeout=30) as r:
            if r.status == 200:
                data = await r.json()
                mapping = {}
                for item in data:
                    sym = (item.get("symbol") or "").lower()
                    if sym and sym not in mapping:
                        mapping[sym] = item.get("id")
                # overrides
                for k,v in COINGECKO_IDS_OVERRIDE.items():
                    mapping[k.lower()] = v
                COINGECKO_SYMBOL_TO_ID = mapping
                logger.info("CoinGecko list loaded")
            else:
                COINGECKO_SYMBOL_TO_ID = {}
                logger.warning("CoinGecko list fetch failed %s", r.status)
    except Exception as e:
        COINGECKO_SYMBOL_TO_ID = {}
        logger.warning("CoinGecko init error: %s", e)

async def get_coin_id(symbol: str) -> Optional[str]:
    return COINGECKO_SYMBOL_TO_ID.get(symbol.lower())

async def fetch_token_address(session: aiohttp.ClientSession, symbol: str, chain: str) -> Optional[Tuple[str,int]]:
    key = (symbol.upper(), chain.upper())
    if key in TOKEN_ADDR_CACHE:
        return TOKEN_ADDR_CACHE[key]
    # check TOKEN_MAP
    tinfo = TOKEN_MAP.get(symbol.upper())
    if tinfo and chain.lower() in tinfo:
        addr = tinfo.get(chain.lower())
        decimals = tinfo.get("decimals", 18)
        TOKEN_ADDR_CACHE[key] = (addr, int(decimals))
        return TOKEN_ADDR_CACHE[key]
    coin_id = await get_coin_id(symbol) or COINGECKO_IDS_OVERRIDE.get(symbol.upper(), symbol.lower())
    url = f"{COINGECKE_API}/coins/{coin_id}"
    try:
        async with session.get(url, timeout=30) as r:
            if r.status != 200:
                TOKEN_ADDR_CACHE[key] = None
                return None
            info = await r.json()
            plat_key = "ethereum" if chain.upper() == "ETH" else "binance-smart-chain"
            addr = info.get("platforms", {}).get(plat_key)
            if not addr:
                TOKEN_ADDR_CACHE[key] = None
                return None
            addr = Web3.to_checksum_address(addr)
            w3 = w3_eth if chain.upper() == "ETH" else w3_bsc
            try:
                contract = w3.eth.contract(address=addr, abi=TOKEN_ABI)
                decimals = await asyncio.to_thread(contract.functions.decimals.call)
            except Exception:
                decimals = 18
            TOKEN_ADDR_CACHE[key] = (addr, int(decimals))
            await asyncio.sleep(0.08)
            return TOKEN_ADDR_CACHE[key]
    except Exception as e:
        logger.debug("fetch_token_address error %s: %s", symbol, e)
        TOKEN_ADDR_CACHE[key] = None
        return None

# ---------------- DEX price ----------------
def get_router_contract(w3: Web3, router_addr: str):
    return w3.eth.contract(address=router_addr, abi=ROUTER_ABI)

async def get_dex_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    try:
        base, quote = symbol.split("/")
    except Exception:
        return None
    chain = "BSC" if ("BNB" in symbol or "BUSD" in symbol) else "ETH"
    router_addr = PANCAKE_ROUTER if chain == "BSC" else UNISWAP_ROUTER
    w3 = w3_bsc if chain == "BSC" else w3_eth
    base_info = await fetch_token_address(session, base, "BSC" if chain=="BSC" else "ETH")
    quote_sym = quote if chain=="ETH" else (quote + "_BSC")
    quote_info = await fetch_token_address(session, quote_sym, "BSC" if chain=="BSC" else "ETH")
    if not base_info or not quote_info:
        return None
    base_addr, base_dec = base_info
    quote_addr, quote_dec = quote_info
    router = get_router_contract(w3, router_addr)
    amount_in = int(1 * (10 ** base_dec))
    def call_router():
        return router.functions.getAmountsOut(amount_in, [base_addr, quote_addr]).call()
    try:
        amounts = await asyncio.to_thread(call_router)
        if not amounts or len(amounts) < 2:
            return None
        amount_out = amounts[-1]
        price = amount_out / (10 ** quote_dec)
        await asyncio.sleep(0.06)
        return float(price)
    except Exception as e:
        logger.debug("DEX call error for %s: %s", symbol, e)
        return None

# ---------------- gas estimate ----------------
async def update_gas_fees(session: aiohttp.ClientSession):
    global LAST_GAS_UPDATE, GAS_FEES_USD
    now = time.time()
    if LAST_GAS_UPDATE and (now - LAST_GAS_UPDATE) < GAS_UPDATE_INTERVAL and GAS_FEES_USD["ETH"] is not None:
        return
    eth_gwei = None; bnb_gwei = None; eth_price = None; bnb_price = None
    try:
        async with session.get(f"{COINGECKE_API}/simple/price?ids=ethereum,binancecoin&vs_currencies=usd") as r:
            if r.status == 200:
                d = await r.json()
                eth_price = d.get("ethereum", {}).get("usd", ETH_PRICE_FALLBACK)
                bnb_price = d.get("binancecoin", {}).get("usd", BNB_PRICE_FALLBACK)
    except Exception:
        pass
    eth_gwei = eth_gwei or ETH_GWEI_FALLBACK
    bnb_gwei = bnb_gwei or BNB_GWEI_FALLBACK
    eth_price = eth_price or ETH_PRICE_FALLBACK
    bnb_price = bnb_price or BNB_PRICE_FALLBACK
    GAS_FEES_USD["ETH"] = eth_gwei * 1e-9 * GAS_UNITS_SWAP * eth_price
    GAS_FEES_USD["BNB"] = bnb_gwei * 1e-9 * GAS_UNITS_SWAP * bnb_price
    LAST_GAS_UPDATE = now
    logger.info("Gas est: ETH $%.2f, BNB $%.2f", GAS_FEES_USD["ETH"], GAS_FEES_USD["BNB"])

# ---------------- snapshots ----------------
async def build_tickers_snapshot() -> Dict[str, Dict[str, dict]]:
    out: Dict[str, Dict[str, dict]] = {}
    for name, client in EXCHANGES.items():
        if not ENABLED_EXCHANGES.get(name, False):
            continue
        try:
            tks = await client.fetch_tickers()
            if tks:
                # filter USDT spot pairs
                out[name] = {s:t for s,t in tks.items() if s.endswith("/USDT")}
            else:
                out[name] = {}
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug("fetch_tickers %s error: %s", name, e)
            out[name] = {}
    return out

async def build_funding_snapshot() -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for name, client in EXCHANGES.items():
        if not ENABLED_EXCHANGES.get(name, False):
            continue
        try:
            if client.has.get("fetchFundingRates"):
                rates = await client.fetch_funding_rates()
                if not rates or len(rates) < 10:
                    logger.debug("Low/no funding rates for %s, skip", name)
                    continue
                out[name] = rates
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug("fetch_funding_rates %s error: %s", name, e)
    return out

# ---------------- STRATEGY CHECKS ----------------
async def check_cex_cex(tickers_by_ex: Dict[str,Dict[str,dict]]):
    logger.info("Check CEX↔CEX")
    names = list(tickers_by_ex.keys())
    for i in range(len(names)):
        for j in range(i+1, len(names)):
            a = names[i]; b = names[j]
            common = set(tickers_by_ex[a].keys()) & set(tickers_by_ex[b].keys())
            for sym in common:
                ta = tickers_by_ex[a].get(sym); tb = tickers_by_ex[b].get(sym)
                if not ta or not tb: continue
                qa = ta.get("quoteVolume",0); qb = tb.get("quoteVolume",0)
                if qa < MIN_VOLUME_24H or qb < MIN_VOLUME_24H: continue
                pa = ta.get("last"); pb = tb.get("last")
                if pa is None or pb is None: continue
                raw = pct_between(pa,pb)
                eff = effective_after_costs(raw, True, a)
                if eff >= MIN_EFF_SPREAD_PERCENT:
                    profit = (eff/100.0)*MY_CAPITAL_USD
                    direction = f"Купить → {a}, Продать → {b}" if pa < pb else f"Купить → {b}, Продать → {a}"
                    msg = (
                        f"🟢 <b>SPOT ARB</b>\n<code>{sym}</code>\n"
                        f"raw: <b>{raw:.2f}%</b>  eff: <b>{eff:.2f}%</b>\n"
                        f"{a}: <code>{pa:.6f}</code>\n{b}: <code>{pb:.6f}</code>\n"
                        f"Объем(min): <b>{min(qa,qb)/1000:.1f}k</b> USDT\n"
                        f"Направление: {direction}\n"
                        f"Прогноз прибыли (на {MY_CAPITAL_USD}$): <b>${profit:.2f}</b>"
                    )
                    await safe_send_html(msg, dedup_key=f"spot_{sym}_{a}_{b}")

async def check_funding():
    logger.info("Check FUNDING")
    funding = await build_funding_snapshot()
    if not funding:
        return
    exs = list(funding.keys())
    for i in range(len(exs)):
        for j in range(i+1, len(exs)):
            a = exs[i]; b = exs[j]
            common = set(funding[a].keys()) & set(funding[b].keys())
            for perp in common:
                try:
                    ra = float(funding[a][perp].get("fundingRate",0))*100
                    rb = float(funding[b][perp].get("fundingRate",0))*100
                    spread = abs(ra - rb)
                    # convert to percent and compare to threshold (we require eff >= MIN_EFF_SPREAD_PERCENT)
                    # approximate: use spread as raw_pct for funding check but require higher than 0.02 (2bp) for notifying funding deltas
                    if spread*1.0 < 0.05:  # require at least 0.05% funding diff to consider
                        continue
                    # profit estimation (rough)
                    profit = (spread/100.0) * MY_CAPITAL_USD
                    if profit < 0.5:
                        continue
                    msg = (
                        f"🟣 <b>FUNDING ARB</b>\n<code>{perp}</code>\n"
                        f"{a}: {ra:.4f}%\n{b}: {rb:.4f}%\n"
                        f"Δ funding: <b>{spread:.3f}%</b>\n"
                        f"Прогноз прибыли (на {MY_CAPITAL_USD}$): <b>${profit:.2f}</b>"
                    )
                    await safe_send_html(msg, dedup_key=f"fund_{perp}_{a}_{b}")
                except Exception:
                    continue

async def check_cex_dex(tickers_by_ex: Dict[str,Dict[str,dict]], session: aiohttp.ClientSession, dex_limit: int = 50):
    logger.info("Check CEX↔DEX")
    # collect candidate symbols by highest volume
    cand: Dict[str, float] = {}
    for ex_name, tdict in tickers_by_ex.items():
        for sym, t in tdict.items():
            qvol = t.get("quoteVolume") or (t.get("baseVolume") or 0) * (t.get("last") or 0)
            if qvol and qvol >= MIN_VOLUME_24H:
                cand[sym] = max(cand.get(sym,0), float(qvol))
    top = sorted(cand.items(), key=lambda x: x[1], reverse=True)[:dex_limit]
    for sym, vol in top:
        # check each exchange price vs dex
        for ex_name, tdict in tickers_by_ex.items():
            tk = tdict.get(sym)
            if not tk: continue
            cex_price = tk.get("last")
            if cex_price is None: continue
            dex_price = await get_dex_price(session, sym)
            if dex_price is None: continue
            raw = pct_between(float(cex_price), float(dex_price))
            if raw is None: continue
            chain = "BNB" if ("BNB" in sym or "BUSD" in sym) else "ETH"
            eff = effective_after_costs(raw, False, ex_name, chain_for_gas=chain)
            if eff >= MIN_EFF_SPREAD_PERCENT:
                profit = (eff/100.0)*MY_CAPITAL_USD
                dex_name = "PancakeSwap" if chain=="BNB" else "Uniswap"
                msg = (
                    f"🔵 <b>{ex_name.upper()}↔DEX</b>\n<code>{sym}</code>\n"
                    f"raw: <b>{raw:.2f}%</b> eff: <b>{eff:.2f}%</b>\n"
                    f"{ex_name}: <code>{float(cex_price):.6f}</code>\n{dex_name}: <code>{float(dex_price):.6f}</code>\n"
                    f"Объем: <b>{vol/1000:.1f}k</b> USDT\n"
                    f"Прогноз прибыли (на {MY_CAPITAL_USD}$): <b>${profit:.2f}</b>"
                )
                await safe_send_html(msg, dedup_key=f"dex_{sym}_{ex_name}")

# ---------------- Telegram command handlers ----------------
async def cmd_capital(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global MY_CAPITAL_USD
    if not context.args:
        await update.message.reply_text("Формат: /capital 100")
        return
    try:
        val = float(context.args[0])
        MY_CAPITAL_USD = val
        await update.message.reply_text(f"✅ Капитал обновлён: ${val}")
    except Exception:
        await update.message.reply_text("Неверный формат. Пример: /capital 100")

async def cmd_enable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /enable bybit")
        return
    ex = context.args[0].lower()
    if ex in ENABLED_EXCHANGES:
        ENABLED_EXCHANGES[ex] = True
        await update.message.reply_text(f"✅ Биржа {ex} включена")
    else:
        await update.message.reply_text(f"Неизвестная биржа: {ex}")

async def cmd_disable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Использование: /disable bybit")
        return
    ex = context.args[0].lower()
    if ex in ENABLED_EXCHANGES:
        ENABLED_EXCHANGES[ex] = False
        await update.message.reply_text(f"⛔ Биржа {ex} отключена")
    else:
        await update.message.reply_text(f"Неизвестная биржа: {ex}")

async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ex_status = "\n".join([f"{k}: {'ON' if v else 'OFF'}" for k,v in ENABLED_EXCHANGES.items()])
    await update.message.reply_text(f"📊 Капитал: ${MY_CAPITAL_USD}\nАктивные биржи:\n{ex_status}")

# ---------------- scheduler: morning capital prompt ----------------
async def morning_prompt():
    # sends at 08:00 every day (server local time)
    while True:
        now = datetime.now()
        if now.hour == 8 and now.minute == 0:
            try:
                await safe_send_html("☀️ <b>Доброе утро!</b>\nПожалуйста, отправь актуальный капитал командой /capital <amount> (в $).")
            except Exception as e:
                logger.debug("morning send error: %s", e)
            # wait 61 seconds to avoid multiple sends in the same minute
            await asyncio.sleep(61)
        await asyncio.sleep(20)

# ---------------- main loop and bot startup ----------------
async def run_telegram_app():
    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("capital", cmd_capital))
    app.add_handler(CommandHandler("enable", cmd_enable))
    app.add_handler(CommandHandler("disable", cmd_disable))
    app.add_handler(CommandHandler("status", cmd_status))
    await app.initialize()
    await app.start()
    # start polling in background
    await app.updater.start_polling()
    return app

async def arb_loop():
    async with aiohttp.ClientSession() as session:
        # init coingecko & gas once
        await init_coingecko(session)
        await update_gas_fees(session)
        last_funding = 0
        while True:
            try:
                tickers = await build_tickers_snapshot()
                # funding snapshot (less frequent)
                if time.time() - last_funding > FUNDING_CHECK_INTERVAL:
                    funding = await build_funding_snapshot()
                    last_funding = time.time()
                else:
                    funding = {}
                # run checks concurrently
                await asyncio.gather(
                    check_cex_cex(tickers),
                    check_cex_dex(tickers, session),
                    check_funding()
                )
            except Exception as e:
                logger.error("arb loop error: %s", e)
            await asyncio.sleep(CHECK_INTERVAL_SECONDS)

async def main():
    logger.info("Starting arb monitor (signals only).")
    # load markets
    await asyncio.gather(*(client.load_markets() for client in EXCHANGES.values()))
    # start telegram app + arb loop + morning prompt
    tg_app = await run_telegram_app()
    await bot.send_message(chat_id=USER_ID, text="✅ Бот запущен: мониторинг сигналов (SPOT / FUNDING / CEX↔DEX).")
    await asyncio.gather(arb_loop(), morning_prompt())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
