import os
import time
import asyncio
import ccxt
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List
from itertools import combinations
import aiohttp
from http.server import HTTPServer, SimpleHTTPRequestHandler

# Фиктивный сервер на порту 10000 для Render Web Service
def run_server():
    server = HTTPServer(('', 10000), SimpleHTTPRequestHandler)
    server.serve_forever()

import threading
threading.Thread(target=run_server, daemon=True).start()
import ccxt.async_support as ccxtgit 
from web3 import Web3
from web3.middleware import geth_poa_middleware
from telegram import Bot

# ========== CONFIG (ENV) ==========
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")  # Уже активен
CHAT_ID = int(os.environ.get("CHAT_ID", "5009858379"))
BYBIT_KEY = os.environ.get("BYBIT_KEY", "FH8adJ1udxzCpm49r1")
MEXC_KEY = os.environ.get("MEXC_KEY", "mx0vglszjOH7FJdFIb")
BITGET_KEY = os.environ.get("BITGET_KEY", "bg_67eee458caa2e940880b61dd0e05a8a7")
MIN_VOLUME = float(os.environ.get("MIN_VOLUME", 500000))
MIN_SPREAD_PERCENT = float(os.environ.get("MIN_SPREAD_PERCENT", 2.0))
MIN_PROFIT_USD = float(os.environ.get("MIN_PROFIT_USD", 0.5))
MY_CAPITAL_USD = float(os.environ.get("MY_CAPITAL_USD", 50.0))
CHECK_INTERVAL = int(os.environ.get("CHECK_INTERVAL", 300))
FUNDING_CHECK_INTERVAL = int(os.environ.get("FUNDING_CHECK_INTERVAL", 300))
DEX_CANDIDATES_LIMIT = int(os.environ.get("DEX_CANDIDATES_LIMIT", 50))
CEX_FEE_DEFAULT_PCT = float(os.environ.get("CEX_FEE_DEFAULT_PCT", 0.1))
CEX_FEES_OVERRIDE = {
    "bybit": float(os.environ.get("CEX_FEE_BYBIT_PCT", 0.055)),
    "mexc": float(os.environ.get("CEX_FEE_MEXC_PCT", 0.2)),
    "bitget": float(os.environ.get("CEX_FEE_BITGET_PCT", 0.1)),
}
DEX_FEE_PCT = float(os.environ.get("DEX_FEE_PCT", 0.3))
SLIPPAGE_PCT = float(os.environ.get("SLIPPAGE_PCT", 0.3))
ETH_RPC = os.environ.get("ETH_RPC", "https://eth.llamarpc.com")
BSC_RPC = os.environ.get("BSC_RPC", "https://bsc-dataseed.binance.org")
GAS_UPDATE_INTERVAL = int(os.environ.get("GAS_UPDATE_INTERVAL", 3600))
GAS_UNITS_SWAP = int(os.environ.get("GAS_UNITS_SWAP", 200000))
ETH_GWEI_FALLBACK = float(os.environ.get("ETH_GWEI_FALLBACK", 30.0))
BNB_GWEI_FALLBACK = float(os.environ.get("BNB_GWEI_FALLBACK", 5.0))
ETH_PRICE_FALLBACK = float(os.environ.get("ETH_PRICE_FALLBACK", 4619.0))
BNB_PRICE_FALLBACK = float(os.environ.get("BNB_PRICE_FALLBACK", 550.0))
# Раскомментируй, если нужны точные газы (опционально)
# ETHERSCAN_KEY = os.environ.get("ETHERSCAN_KEY")
# BSCSCAN_KEY = os.environ.get("BSCSCAN_KEY")
COINGECKO_IDS_OVERRIDE = {"TRUMP": "maga"}

# ========== LOGGING & BOT ==========
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("arb-bot")
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_TOKEN is not set in environment variables!")
bot = Bot(token=TELEGRAM_TOKEN)

# ========== EXCHANGES & WEB3 =========
def make_client(id_name: str, key: Optional[str], secret: Optional[str]):
    params = {"enableRateLimit": True}
    if key and secret:
        params.update({"apiKey": key, "secret": secret})
    if id_name == "bybit":
        return ccxt.bybit(params)
    if id_name == "mexc":
        return ccxt.mexc(params)
    if id_name == "bitget":
        return ccxt.bitget(params)
    raise ValueError("unknown exchange")
# Раскомментируй следующие строки
BYBIT_KEY = os.environ.get("BYBIT_KEY")
BYBIT_SECRET = os.environ.get("BYBIT_SECRET")
MEXC_KEY = os.environ.get("MEXC_KEY")
MEXC_SECRET = os.environ.get("MEXC_SECRET")
BITGET_KEY = os.environ.get("BITGET_KEY")
BITGET_SECRET = os.environ.get("BITGET_SECRET")

if not (BYBIT_KEY and BYBIT_SECRET and MEXC_KEY and MEXC_SECRET and BITGET_KEY and BITGET_SECRET):
    raise ValueError("One or more exchange API keys/secrets are not set!")
EXCHANGES = {
    "bybit": make_client("bybit", BYBIT_KEY, BYBIT_SECRET),
    "mexc": make_client("mexc", MEXC_KEY, MEXC_SECRET),
    "bitget": make_client("bitget", BITGET_KEY, BITGET_SECRET),
}
# Комментируем публичный вариант
# EXCHANGES = {
#     "bybit": ccxt.bybit({"enableRateLimit": True}),
#     "mexc": ccxt.mexc({"enableRateLimit": True}),
#     "bitget": ccxt.bitget({"enableRateLimit": True}),
# }
w3_eth = Web3(Web3.HTTPProvider(ETH_RPC))
w3_bsc = Web3(Web3.HTTPProvider(BSC_RPC))
try:
    w3_bsc.middleware_onion.inject(geth_poa_middleware, layer=0)
except:
    pass
UNISWAP_ROUTER = Web3.to_checksum_address("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D")
PANCAKE_ROUTER = Web3.to_checksum_address("0x10ED43C718714eb63d5aA57B78B54704E256024E")
ROUTER_ABI = [{
    "inputs": [{"internalType": "uint256", "name": "amountIn", "type": "uint256"}, {"internalType": "address[]", "name": "path", "type": "address[]"}],
    "name": "getAmountsOut",
    "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}],
    "stateMutability": "view",
    "type": "function"
}]
TOKEN_ABI = [{"inputs": [], "name": "decimals", "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}], "stateMutability": "view", "type": "function"}]
TOKEN_MAP = {
    "WETH": {"eth": Web3.to_checksum_address("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), "decimals": 18},
    "USDT": {"eth": Web3.to_checksum_address("0xdAC17F958D2ee523a2206206994597C13D831ec7"), "decimals": 6},
    "USDC": {"eth": Web3.to_checksum_address("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"), "decimals": 6},
    "WBNB": {"bsc": Web3.to_checksum_address("0xBB4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"), "decimals": 18},
    "BUSD": {"bsc": Web3.to_checksum_address("0xe9e7cea3dedca5984780bafc599bd69add087d56"), "decimals": 18},
    "USDT_BSC": {"bsc": Web3.to_checksum_address("0x55d398326f99059fF775485246999027B3197955"), "decimals": 18},
}
COINGECKO_SYMBOL_TO_ID: Dict[str, str] = {}
TOKEN_ADDR_CACHE: Dict[Tuple[str, str], Optional[Tuple[str, int]]] = {}
SIGNAL_CACHE: Dict[str, float] = {}
SIGNALS_24H = 0
LAST_RESET = datetime.now()
LAST_GAS_UPDATE = 0
GAS_FEES_USD = {"ETH": None, "BNB": None}
MIN_SPREAD = MIN_SPREAD_PERCENT  # Фиксируем на MIN_SPREAD_PERCENT
# ========== HELPERS ==========
def pct(a: float, b: float) -> Optional[float]:
    if a is None or b is None or (a + b) == 0:
        return None
    return abs((a - b) / ((a + b) / 2) * 100)

def effective_after_costs(raw_pct: float, is_cex_cex: bool, chain: str, ex_name: str, avg_price: float) -> float:
    cex_fee = CEX_FEES_OVERRIDE.get(ex_name.lower(), CEX_FEE_DEFAULT_PCT)
    gas_usd = GAS_FEES_USD.get(chain) or 0.0
    gas_pct = (gas_usd / MY_CAPITAL_USD) * 100 if gas_usd and MY_CAPITAL_USD else 0.0
    fees = (2 * cex_fee + SLIPPAGE_PCT) if is_cex_cex else (cex_fee + DEX_FEE_PCT + SLIPPAGE_PCT + gas_pct)
    return raw_pct - fees

async def tg_send(msg: str, key: str, dedup_seconds: int = 1800):
    now = time.time()
    if key in SIGNAL_CACHE and (now - SIGNAL_CACHE[key]) < dedup_seconds:
        logger.debug(f"Skip duplicate signal {key}")
        return
    try:
        await bot.send_message(chat_id=CHAT_ID, text=msg)
        SIGNAL_CACHE[key] = now
        global SIGNALS_24H
        SIGNALS_24H += 1
        if len(SIGNAL_CACHE) > 1000:
            oldest = sorted(SIGNAL_CACHE.items(), key=lambda x: x[1])[:200]
            for k, _ in oldest:
                SIGNAL_CACHE.pop(k, None)
        logger.info(f"Sent TG alert {key}")
    except Exception as e:
        logger.error(f"TG send error: {e}")

async def update_min_spread():
    global MIN_SPREAD, SIGNALS_24H, LAST_RESET
    now = datetime.now()
    if now - LAST_RESET > timedelta(hours=24):
        SIGNALS_24H = 0
        LAST_RESET = now
    # Отключаем адаптивность, фиксируем MIN_SPREAD на MIN_SPREAD_PERCENT
    MIN_SPREAD = MIN_SPREAD_PERCENT
    logger.info(f"Min spread fixed at {MIN_SPREAD}%")

async def init_coingecko(session: aiohttp.ClientSession):
    global COINGECKO_SYMBOL_TO_ID
    if COINGECKO_SYMBOL_TO_ID:
        return
    url = "https://api.coingecko.com/api/v3/coins/list"
    try:
        async with session.get(url, timeout=30) as r:
            if r.status == 200:
                data = await r.json()
                mapping = {}
                for item in data:
                    sym = item.get("symbol", "").lower()
                    if sym and sym not in mapping:
                        mapping[sym] = COINGECKO_IDS_OVERRIDE.get(sym.upper(), item.get("id"))
                COINGECKO_SYMBOL_TO_ID = mapping
                logger.info(f"CoinGecko list loaded ({len(mapping)} symbols)")
            else:
                COINGECKO_SYMBOL_TO_ID = {}
                logger.warning(f"CoinGecko list fetch failed status {r.status}")
    except Exception as e:
        COINGECKO_SYMBOL_TO_ID = {}
        logger.warning(f"CoinGecko init error: {e}")

async def get_coin_id(symbol: str) -> Optional[str]:
    return COINGECKO_SYMBOL_TO_ID.get(symbol.lower())

async def fetch_token_address(session: aiohttp.ClientSession, symbol: str, chain: str) -> Optional[Tuple[str, int]]:
    key = (symbol.upper(), chain)
    if key in TOKEN_ADDR_CACHE:
        return TOKEN_ADDR_CACHE[key]
    if symbol.upper() in TOKEN_MAP and chain in TOKEN_MAP[symbol.upper()]:
        addr = TOKEN_MAP[symbol.upper()][chain]
        decimals = TOKEN_MAP[symbol.upper()].get("decimals", 18)
        TOKEN_ADDR_CACHE[key] = (addr, decimals)
        return TOKEN_ADDR_CACHE[key]
    coin_id = await get_coin_id(symbol) or COINGECKO_IDS_OVERRIDE.get(symbol.upper(), symbol.lower())
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}"
    try:
        async with session.get(url, timeout=30) as r:
            if r.status != 200:
                TOKEN_ADDR_CACHE[key] = None
                return None
            info = await r.json()
            plat_key = "ethereum" if chain == "ETH" else "binance-smart-chain"
            addr = info.get("platforms", {}).get(plat_key)
            if not addr:
                TOKEN_ADDR_CACHE[key] = None
                return None
            addr = Web3.to_checksum_address(addr)
            w3 = w3_eth if chain == "ETH" else w3_bsc
            try:
                contract = w3.eth.contract(address=addr, abi=TOKEN_ABI)
                decimals = await asyncio.to_thread(contract.functions.decimals.call)
            except:
                decimals = 18
            TOKEN_ADDR_CACHE[key] = (addr, int(decimals))
            await asyncio.sleep(0.08)
            return TOKEN_ADDR_CACHE[key]
    except Exception as e:
        logger.debug(f"fetch_token_address error for {symbol}: {e}")
        TOKEN_ADDR_CACHE[key] = None
        return None

async def get_dex_price(session: aiohttp.ClientSession, symbol: str) -> Optional[float]:
    try:
        base, quote = symbol.split("/")
    except:
        return None
    chain = "BSC" if base.upper() in COINGECKO_IDS_OVERRIDE or "BNB" in symbol or "BUSD" in symbol else "ETH"
    router_addr = PANCAKE_ROUTER if chain == "BSC" else UNISWAP_ROUTER
    w3 = w3_bsc if chain == "BSC" else w3_eth
    base_info = await fetch_token_address(session, base, chain)
    quote_info = await fetch_token_address(session, quote if chain == "ETH" else (quote + "_BSC"), chain)
    if not base_info or not quote_info:
        return None
    base_addr, base_dec = base_info
    quote_addr, quote_dec = quote_info
    router = w3.eth.contract(address=router_addr, abi=ROUTER_ABI)
    amount_in = int(1 * (10 ** base_dec))
    def call_router():
        return router.functions.getAmountsOut(amount_in, [base_addr, quote_addr]).call()
    try:
        amounts = await asyncio.to_thread(call_router)
        if not amounts or len(amounts) < 2:
            return None
        price = amounts[-1] / (10 ** quote_dec)
        await asyncio.sleep(0.06)
        return float(price)
    except Exception as e:
        logger.debug(f"get_dex_price error for {symbol}: {e}")
        return None

async def update_gas_fees(session: aiohttp.ClientSession):
    global GAS_FEES_USD, LAST_GAS_UPDATE
    now = time.time()
    if LAST_GAS_UPDATE and (now - LAST_GAS_UPDATE) < GAS_UPDATE_INTERVAL and GAS_FEES_USD["ETH"] is not None:
        return
    eth_gwei = bnb_gwei = eth_price = bnb_price = None
    try:
        async with session.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum,binancecoin&vs_currencies=usd") as r:
            if r.status == 200:
                data = await r.json()
                eth_price = data.get("ethereum", {}).get("usd", ETH_PRICE_FALLBACK)
                bnb_price = data.get("binancecoin", {}).get("usd", BNB_PRICE_FALLBACK)
        # url = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_KEY}" if ETHERSCAN_KEY else "https://api.etherscan.io/api?module=gastracker&action=gasoracle"
        # async with session.get(url, timeout=15) as r:
        #     if r.status == 200:
        #         jd = await r.json()
        #         if jd.get("status") == "1":
        #             eth_gwei = float(jd["result"]["SafeGasPrice"])
        # url2 = f"https://api.bscscan.com/api?module=gastracker&action=gasoracle&apikey={BSCSCAN_KEY}" if BSCSCAN_KEY else "https://api.bscscan.com/api?module=gastracker&action=gasoracle"
        # async with session.get(url2, timeout=15) as r:
        #     if r.status == 200:
        #         jd = await r.json()
        #         if jd.get("status") == "1":
        #             bnb_gwei = float(jd["result"]["SafeGasPrice"])
    except Exception as e:
        logger.debug(f"Gas update failed: {e}")
    eth_gwei = eth_gwei or ETH_GWEI_FALLBACK
    bnb_gwei = bnb_gwei or BNB_GWEI_FALLBACK
    eth_price = eth_price or ETH_PRICE_FALLBACK
    bnb_price = bnb_price or BNB_PRICE_FALLBACK
    GAS_FEES_USD["ETH"] = eth_gwei * 1e-9 * GAS_UNITS_SWAP * eth_price
    GAS_FEES_USD["BNB"] = bnb_gwei * 1e-9 * GAS_UNITS_SWAP * bnb_price
    LAST_GAS_UPDATE = now
    logger.info(f"Updated gas: ETH ${GAS_FEES_USD['ETH']:.2f}, BNB ${GAS_FEES_USD['BNB']:.2f}")

async def build_tickers_snapshot() -> Dict[str, Dict[str, dict]]:
    out: Dict[str, Dict[str, dict]] = {}
    for name, client in EXCHANGES.items():
        try:
            # Поддержка спотов и фьючерсов
            tks = await client.fetch_tickers(params={"type": "spot"})  # Спот
            futs = await client.fetch_tickers(params={"type": "future"})  # Фьючерсы
            out[name] = {**{s: t for s, t in tks.items() if s.endswith("/USDT")}, **{s: t for s, t in futs.items() if s.endswith(":USDT")}}
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug(f"fetch_tickers {name} error: {e}")
            out[name] = {}
    return out

async def build_funding_snapshot() -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for name, client in EXCHANGES.items():
        try:
            if client.has.get("fetchFundingRates"):
                rates = await client.fetch_funding_rates(params={"type": "future"})
                if not rates or len(rates) < 10:
                    logger.debug(f"Low/no funding rates for {name}, skip")
                    continue
                out[name] = rates
            await asyncio.sleep(0.2)
        except Exception as e:
            logger.debug(f"fetch_funding_rates {name} error: {e}")
    return out

async def check_cex_cex(tickers_by_ex: Dict[str, Dict[str, dict]]):
    logger.info("Checking CEX-CEX")
    symbol_exs: Dict[str, List[str]] = {}
    for ex_name, tdict in tickers_by_ex.items():
        for sym in tdict.keys():
            symbol_exs.setdefault(sym, []).append(ex_name)
    candidates = [s for s, exs in symbol_exs.items() if len(exs) >= 2]
    for sym in candidates:
        prices = {}
        vols = {}
        for ex_name in symbol_exs[sym]:
            tk = tickers_by_ex.get(ex_name, {}).get(sym)
            if not tk:
                continue
            last = tk.get("last")
            qvol = tk.get("quoteVolume") or (tk.get("baseVolume") or 0) * (last or 0)
            if last is None or qvol is None:
                continue
            prices[ex_name] = float(last)
            vols[ex_name] = float(qvol)
        if len(prices) < 2:
            continue
        best = None
        ex_list = list(prices.items())
        for i in range(len(ex_list)):
            for j in range(i + 1, len(ex_list)):
                a_name, a_price = ex_list[i]
                b_name, b_price = ex_list[j]
                raw = pct(a_price, b_price)
                if raw is None:
                    continue
                vol_min = min(vols.get(a_name, 0), vols.get(b_name, 0))
                if vol_min < MIN_VOLUME:
                    continue
                avg_price = (a_price + b_price) / 2
                effective = effective_after_costs(raw, True, "ETH", a_name, avg_price)
                est_profit = (effective / 100.0) * MY_CAPITAL_USD
                if effective >= MIN_SPREAD and est_profit >= MIN_PROFIT_USD:
                    if best is None or effective > best[0]:
                        best = (effective, raw, a_name, a_price, b_name, b_price, vol_min, est_profit)
        if best:
            eff, rawp, a_name, a_price, b_name, b_price, vol_min, est_profit = best
            direction = f"купить на {a_name}, продать на {b_name}" if a_price < b_price else f"купить на {b_name}, продать на {a_name}"
            msg = (f"🟢 SPOT ARB\n{sym} | raw {rawp:.2f}% | eff {eff:.2f}%\n"
                   f"{a_name}: {a_price:.6f}\n{b_name}: {b_price:.6f}\n"
                   f"Объем(min): {vol_min/1000:.1f}k USDT\nНаправление: {direction}\n"
                   f"Кругов: {max(1, int(rawp/1.5))}\nВремя: 10-30 мин\n"
                   f"Прогноз прибыли (на {MY_CAPITAL_USD}$): ${est_profit:.2f}")
            await tg_send(msg, f"cex_{sym}_{a_name}_{b_name}")
            await asyncio.sleep(0.25)

async def check_funding():
    logger.info("Checking FUNDING arb (priority)")
    funding = await build_funding_snapshot()
    if not funding:
        return
    ex_names = list(funding.keys())
    for i in range(len(ex_names)):
        for j in range(i + 1, len(ex_names)):
            exa, exb = ex_names[i], ex_names[j]
            rates_a = funding.get(exa, {})
            rates_b = funding.get(exb, {})
            common = set(rates_a.keys()) & set(rates_b.keys())
            for perp in common:
                try:
                    r_a = float(rates_a[perp].get("fundingRate", 0)) * 100
                    r_b = float(rates_b[perp].get("fundingRate", 0)) * 100
                    spread = abs(r_a - r_b)
                    if spread < MIN_SPREAD:
                        continue
                    spot_sym = perp.replace(":USDT", "/USDT") if ":USDT" in perp else perp.replace("PERP", "/USDT")
                    try:
                        tick_a = await EXCHANGES[exa].fetch_ticker(perp, params={"category": "linear"})
                        tick_b = await EXCHANGES[exb].fetch_ticker(perp, params={"category": "linear"})
                        perp_vol_a = tick_a.get("quoteVolume", 0)
                        perp_vol_b = tick_b.get("quoteVolume", 0)
                    except:
                        perp_vol_a = perp_vol_b = 0
                    try:
                        spot_a = await EXCHANGES[exa].fetch_ticker(spot_sym)
                        spot_b = await EXCHANGES[exb].fetch_ticker(spot_sym)
                        vol_a = spot_a.get("quoteVolume", 0)
                        vol_b = spot_b.get("quoteVolume", 0)
                    except:
                        vol_a = vol_b = 0
                    vol_min = min(perp_vol_a or vol_a or 0, perp_vol_b or vol_b or 0)
                    if vol_min < MIN_VOLUME:
                        continue
                    profit = (spread / 100.0) * MY_CAPITAL_USD
                    if profit < MIN_PROFIT_USD:
                        continue
                    dir_a = "лонг⬆️" if r_a < 0 else "шорт⬇️"
                    dir_b = "лонг⬆️" if r_b < 0 else "шорт⬇️"
                    msg = (f"🟣 FUNDING ARB\n{perp} | Δ {spread:.3f}%\n"
                           f"{exa}: {r_a:.4f}% {dir_a}\n{exb}: {r_b:.4f}% {dir_b}\n"
                           f"Объем(min): {vol_min/1000:.0f}k USDT\nКругов: {max(1, int(spread/1.5))}\n"
                           f"Время: 4-8 часов\nПрогноз прибыли (на {MY_CAPITAL_USD}$): ${profit:.2f}")
                    await tg_send(msg, f"fund_{perp}_{exa}_{exb}")
                    await asyncio.sleep(0.3)
                except:
                    continue

async def check_cex_dex(tickers_by_ex: Dict[str, Dict[str, dict]], session: aiohttp.ClientSession):
    logger.info("Checking CEX-DEX (priority)")
    candidates = {}
    for ex_name, tdict in tickers_by_ex.items():
        for sym, t in tdict.items():
            qvol = t.get("quoteVolume") or (t.get("baseVolume") or 0) * (t.get("last") or 0)
            if qvol and qvol >= MIN_VOLUME:
                candidates[sym] = max(candidates.get(sym, 0), float(qvol))
    top_symbols = sorted(candidates.items(), key=lambda x: x[1], reverse=True)[:DEX_CANDIDATES_LIMIT]
    for sym, vol in top_symbols:
        for ex_name, tdict in tickers_by_ex.items():
            tk = tdict.get(sym)
            if not tk:
                continue
            cex_price = tk.get("last")
            if cex_price is None:
                continue
            p_dex = await get_dex_price(session, sym)
            if p_dex is None:
                continue
            raw = pct(float(cex_price), float(p_dex))
            if raw is None:
                continue
            chain = "BSC" if sym.split("/")[0].upper() in COINGECKO_IDS_OVERRIDE or "BNB" in sym or "BUSD" in sym else "ETH"
            effective = effective_after_costs(raw, False, chain, ex_name, (float(cex_price) + float(p_dex)) / 2)
            est_profit = (effective / 100.0) * MY_CAPITAL_USD
            if effective < MIN_SPREAD or est_profit < MIN_PROFIT_USD:
                continue
            dex_name = "PancakeSwap" if chain == "BNB" else "Uniswap"
            msg = (f"🔵 {ex_name.upper()}↔DEX ARB\n{sym} | raw {raw:.2f}% | eff {effective:.2f}%\n"
                   f"{ex_name}: {float(cex_price):.6f}\n{dex_name}: {float(p_dex):.6f}\n"
                   f"Объем: {vol/1000:.1f}k USDT\nКругов: {max(1, int(raw/1.5))}\n"
                   f"Время: 15-45 мин (с газом)\nПрогноз прибыли (на {MY_CAPITAL_USD}$): ${est_profit:.2f}")
            await tg_send(msg, f"dex_{sym}_{ex_name}")
            await asyncio.sleep(0.25)

async def main():
    logger.info("Starting arb scanner")
    await asyncio.gather(*(client.load_markets() for client in EXCHANGES.values()))
    await bot.send_message(chat_id=CHAT_ID, text="✅ Бот запущен: $50, 500k vol, ≥0.5$ profit, TRUMP-ready")
    last_gas = 0
    async with aiohttp.ClientSession() as session:
        await init_coingecko(session)
        while True:
            try:
                await update_min_spread()
                if time.time() - last_gas > GAS_UPDATE_INTERVAL:
                    await update_gas_fees(session)
                    last_gas = time.time()
                tickers = await build_tickers_snapshot()
                # Приоритет: FUNDING, CEX-DEX, потом CEX-CEX
                await asyncio.gather(
                    check_funding(),  # Фьючерсы — первый приоритет
                    check_cex_dex(tickers, session),  # CEX-DEX — второй приоритет
                    check_cex_cex(tickers),  # CEX-CEX — третий
                )
            except Exception as e:
                logger.error(f"Main loop error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")