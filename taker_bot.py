"""
taker_bot.py
FR自動売買ボット
  - funding_log.csvの直近2回が taker_ok=True かつ FR > MIN_FR_1H → エントリー
  - taker_ok=False になったら → 決済
  - HL SHORT + MEXC LONG (delta neutral)

コスト: HL taker 0.035%×2 + MEXC taker 0.04%×2 = 往復0.15%
"""

import csv
import json
import os
import time
import urllib.request
import urllib.parse
from collections import defaultdict
from datetime import datetime, timezone

import ccxt
import tweepy
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

# ── 設定 ────────────────────────────────────────────────────────
TRADE_SIZE_USD  = float(os.environ.get("TRADE_SIZE_USD", "50"))   # 1ポジションUSDT
MAX_POSITIONS   = int(os.environ.get("MAX_POSITIONS", "3"))        # 最大同時ポジション数
MIN_FR_1H       = 0.0012  # エントリー最小FR閾値: 0.12%/h（往復コスト0.17%÷損益分岐1.5h）

DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
FUNDING_CSV  = os.path.join(DATA_DIR, "funding_log.csv")
STATE_FILE   = os.path.join(DATA_DIR, "taker_state.json")

HL_PRIVATE_KEY  = os.environ["HL_PRIVATE_KEY"]
MEXC_API_KEY    = os.environ["MEXC_API_KEY"]
MEXC_API_SECRET = os.environ["MEXC_API_SECRET"]
TG_TOKEN        = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT         = os.environ.get("TELEGRAM_CHAT_ID", "")
X_API_KEY       = os.environ.get("X_API_KEY", "")
X_API_SECRET    = os.environ.get("X_API_SECRET", "")
X_ACCESS_TOKEN  = os.environ.get("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET", "")


# ── Telegram ─────────────────────────────────────────────────────
def tg(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        return
    try:
        url  = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": msg}).encode()
        urllib.request.urlopen(url, data=data, timeout=10)
    except Exception as e:
        print(f"[TG error] {e}")


# ── X (Twitter) 投稿 ─────────────────────────────────────────────
def post_x(text: str):
    if not all([X_API_KEY, X_API_SECRET, X_ACCESS_TOKEN, X_ACCESS_SECRET]):
        return
    try:
        client = tweepy.Client(
            consumer_key=X_API_KEY,
            consumer_secret=X_API_SECRET,
            access_token=X_ACCESS_TOKEN,
            access_token_secret=X_ACCESS_SECRET,
        )
        client.create_tweet(text=text)
        print("[X] 投稿完了")
    except Exception as e:
        print(f"[X error] {e}")


# ── State管理 ─────────────────────────────────────────────────────
def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"positions": {}}


def save_state(state: dict):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── Funding CSV ───────────────────────────────────────────────────
def get_latest_signals(n: int = 2) -> dict:
    """各銘柄の直近n回のシグナルを返す {coin: [{'ts','fr','taker'}, ...]}"""
    rows_by_coin: dict = defaultdict(list)
    with open(FUNDING_CSV) as f:
        for row in csv.DictReader(f):
            if row["timestamp_utc"] == "timestamp_utc":
                continue
            rows_by_coin[row["coin"]].append({
                "ts":    row["timestamp_utc"],
                "fr":    float(row["funding_rate_1h"]),
                "taker": row["taker_ok"] == "True",
            })
    result = {}
    for coin, rows in rows_by_coin.items():
        rows.sort(key=lambda x: x["ts"])
        result[coin] = rows[-n:]
    return result


# ── HL ───────────────────────────────────────────────────────────
def hl_open_short(exchange: Exchange, info: Info, coin: str, size_usd: float) -> dict:
    mids  = info.all_mids()
    price = float(mids.get(coin, 0))
    if price == 0:
        raise ValueError(f"HL価格取得失敗: {coin}")
    sz = round(size_usd / price, 6)
    resp = exchange.market_open(coin, is_buy=False, sz=sz, slippage=0.02)
    status = resp.get("response", {}).get("data", {}).get("statuses", [{}])[0]
    if "error" in status:
        raise RuntimeError(f"HL open error: {status['error']}")
    filled_price = float(status.get("filled", {}).get("avgPx", price))
    return {"size_coin": sz, "entry_price": filled_price}


def hl_close_short(exchange: Exchange, coin: str) -> dict:
    resp = exchange.market_close(coin, slippage=0.02)
    status = resp.get("response", {}).get("data", {}).get("statuses", [{}])[0]
    if "error" in status:
        raise RuntimeError(f"HL close error: {status['error']}")
    filled_price = float(status.get("filled", {}).get("avgPx", 0))
    return {"close_price": filled_price}


# ── MEXC ─────────────────────────────────────────────────────────
def get_mexc() -> ccxt.mexc:
    return ccxt.mexc({
        "apiKey": MEXC_API_KEY,
        "secret": MEXC_API_SECRET,
        "options": {"defaultType": "swap"},
    })


def mexc_open_long(mexc: ccxt.mexc, coin: str, size_usd: float) -> dict:
    symbol  = f"{coin}/USDT:USDT"
    market  = mexc.market(symbol)
    ticker  = mexc.fetch_ticker(symbol)
    price   = float(ticker["ask"] or ticker["last"])
    cs      = float(market.get("contractSize") or 1)
    # contracts = USDT / (price per coin × coins per contract)
    contracts = size_usd / (price * cs)
    # 最小1枚、precision=1（整数）
    contracts = max(1, round(contracts))
    order = mexc.create_market_buy_order(symbol, contracts)
    avg_price = float(order.get("average") or order.get("price") or price)
    return {"contracts": contracts, "entry_price": avg_price, "contract_size": cs}


def mexc_close_long(mexc: ccxt.mexc, coin: str, contracts: int) -> dict:
    symbol = f"{coin}/USDT:USDT"
    order  = mexc.create_market_sell_order(
        symbol, contracts, params={"reduceOnly": True}
    )
    avg_price = float(order.get("average") or order.get("price") or 0)
    return {"close_price": avg_price}


# ── メインロジック ────────────────────────────────────────────────
def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== taker_bot {ts} ===")

    state     = load_state()
    positions = state["positions"]

    wallet   = Account.from_key(HL_PRIVATE_KEY)
    info     = Info(skip_ws=True)
    exchange = Exchange(wallet)
    mexc     = get_mexc()
    mexc.load_markets()

    signals = get_latest_signals(n=2)

    # ── 決済チェック（先に行う）────────────────────────────────
    for coin in list(positions.keys()):
        sig = signals.get(coin, [])
        if not sig or sig[-1]["taker"]:
            continue   # まだtaker継続中

        print(f"[EXIT] {coin}  taker_ok=False → 決済")
        pos = positions[coin]

        hl_ok, mexc_ok = False, False
        try:
            hl_close_short(exchange, coin)
            hl_ok = True
        except Exception as e:
            print(f"  HL close error: {e}")
            tg(f"⚠️ EXIT HL ERROR: {coin}\n{e}")

        try:
            mexc_close_long(mexc, coin, int(pos["mexc_contracts"]))
            mexc_ok = True
        except Exception as e:
            print(f"  MEXC close error: {e}")
            tg(f"⚠️ EXIT MEXC ERROR: {coin}\n{e}")

        if hl_ok and mexc_ok:
            opened = datetime.strptime(pos["opened_at"], "%Y-%m-%d %H:%M:%S")
            now_dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            dur_h  = (now_dt - opened).total_seconds() / 3600
            est_fr = pos["fr_at_entry"] * dur_h * pos["size_usd"]
            est_cost = 0.0017 * pos["size_usd"]   # 往復0.17%（HL 0.09% + MEXC 0.08%）
            net = est_fr - est_cost

            del positions[coin]
            save_state(state)

            tg(
                f"🔴 EXIT: {coin}\n"
                f"保有: {dur_h:.1f}h\n"
                f"推定FR収益: ${est_fr:.2f}\n"
                f"手数料: ${est_cost:.2f}\n"
                f"推定net: ${net:.2f}"
            )
            post_x(
                f"🔴 FR Arb 決済 #{coin}\n"
                f"保有時間: {dur_h:.1f}h\n"
                f"推定収益: ${est_fr:.2f} / net: ${net:.2f}\n"
                f"HL SHORT × MEXC LONG\n"
                f"#MindRaid #FRArb #仮想通貨 #ClaudeCode"
            )
            print(f"  → 決済完了  推定net: ${net:.2f}")

    # ── エントリーチェック ──────────────────────────────────────
    for coin, rows in signals.items():
        if coin in positions:
            continue
        if len(positions) >= MAX_POSITIONS:
            print(f"MAX_POSITIONS ({MAX_POSITIONS}) 到達 → スキップ")
            break
        if len(rows) < 2:
            continue

        # 直近2回ともtaker_ok=True かつ FR閾値超え
        if not all(r["taker"] for r in rows):
            continue
        avg_fr = sum(abs(r["fr"]) for r in rows) / len(rows)
        if avg_fr < MIN_FR_1H:
            continue

        print(f"[ENTRY] {coin}  avg_FR={avg_fr:.4%}/h  size=${TRADE_SIZE_USD}")

        # HL SHORT
        try:
            hl_res = hl_open_short(exchange, info, coin, TRADE_SIZE_USD)
        except Exception as e:
            print(f"  HL open error: {e}")
            tg(f"⚠️ ENTRY HL ERROR: {coin}\n{e}")
            continue

        time.sleep(1)

        # MEXC LONG
        try:
            mx_res = mexc_open_long(mexc, coin, TRADE_SIZE_USD)
        except Exception as e:
            print(f"  MEXC open error → HL rollback: {e}")
            tg(f"⚠️ ENTRY MEXC ERROR: {coin}\n{e}\nHL rollback中...")
            try:
                hl_close_short(exchange, coin)
                tg(f"  HL rollback完了")
            except Exception as re:
                tg(f"  HL rollback失敗: {re}")
            continue

        positions[coin] = {
            "opened_at":     ts,
            "fr_at_entry":   avg_fr,
            "size_usd":      TRADE_SIZE_USD,
            "hl_size_coin":  hl_res["size_coin"],
            "hl_entry_price": hl_res["entry_price"],
            "mexc_contracts": mx_res["contracts"],
            "mexc_contract_size": mx_res["contract_size"],
            "mexc_entry_price": mx_res["entry_price"],
        }
        save_state(state)

        tg(
            f"🟢 ENTRY: {coin}\n"
            f"FR: {avg_fr:.4%}/h\n"
            f"Size: ${TRADE_SIZE_USD}\n"
            f"HL SHORT @ {hl_res['entry_price']:.6f}  ({hl_res['size_coin']} coins)\n"
            f"MEXC LONG @ {mx_res['entry_price']:.6f}  ({mx_res['contracts']} contracts)"
        )
        print(f"  → エントリー完了")

    # 現在のポジション一覧
    if positions:
        print(f"\n現在のポジション: {list(positions.keys())}")
    else:
        print("\n現在ポジションなし")


if __name__ == "__main__":
    main()
