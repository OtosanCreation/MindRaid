"""
taker_bot.py
FR自動売買ボット（双方向対応）
  - FR < -MIN_FR_1H → HL SHORT + MEXC LONG  (ネガティブFR: LONGが受取)
  - FR >  MIN_FR_1H → HL LONG  + MEXC SHORT (ポジティブFR: SHORTが受取)
  - FR が EXIT閾値を下回ったら → 決済

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

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import ccxt
import tweepy
from eth_account import Account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info

# ── 設定 ────────────────────────────────────────────────────────
TRADE_SIZE_USD  = float(os.environ.get("TRADE_SIZE_USD", "90"))   # 1ポジションUSDT
MAX_POSITIONS   = int(os.environ.get("MAX_POSITIONS", "2"))        # 最大同時ポジション数
MIN_FR_1H       = 0.0010  # エントリー最小FR閾値: 0.10%/h（トライアルフェーズ、サンプル収集優先）
EXIT_FR_1H      = 0.0002  # 決済FR閾値: 0.02%/h（MAKERレート相当、コスト回収前）
EXIT_FR_RECOVERED = 0.0001  # コスト回収済み後の決済閾値: 0.01%/h（最後まで搾り取る）

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
GMAIL_ADDRESS   = os.environ.get("GMAIL_ADDRESS", "")
GMAIL_PASSWORD  = os.environ.get("GMAIL_APP_PASSWORD", "")


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


# ── Gmail通知 ────────────────────────────────────────────────────
def send_gmail(subject: str, body: str):
    if not GMAIL_ADDRESS or not GMAIL_PASSWORD:
        return
    try:
        msg = MIMEMultipart()
        msg["From"]    = GMAIL_ADDRESS
        msg["To"]      = GMAIL_ADDRESS
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(GMAIL_ADDRESS, GMAIL_PASSWORD)
            smtp.send_message(msg)
        print("[Gmail] 送信完了")
    except Exception as e:
        print(f"[Gmail error] {e}")


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
def get_sz_decimals(info: Info) -> dict:
    """各コインのszDecimals（注文サイズの有効桁数）を返す {coin: int}"""
    meta = info.meta()
    return {a["name"]: a["szDecimals"] for a in meta["universe"]}


def hl_open_short(exchange: Exchange, info: Info, coin: str, size_usd: float,
                  sz_decimals_map: dict = None) -> dict:
    mids  = info.all_mids()
    price = float(mids.get(coin, 0))
    if price == 0:
        raise ValueError(f"HL価格取得失敗: {coin}")
    decimals = (sz_decimals_map or {}).get(coin, 6)
    sz = round(size_usd / price, decimals)
    if sz <= 0:
        raise ValueError(f"注文サイズが0: {coin} price={price} decimals={decimals}")
    resp = exchange.market_open(coin, is_buy=False, sz=sz, slippage=0.02)
    status = resp.get("response", {}).get("data", {}).get("statuses", [{}])[0]
    if "error" in status:
        raise RuntimeError(f"HL open error: {status['error']}")
    filled_price = float(status.get("filled", {}).get("avgPx", price))
    return {"size_coin": sz, "entry_price": filled_price}


def hl_close_short(exchange: Exchange, coin: str) -> dict:
    resp = exchange.market_close(coin, slippage=0.02)
    if resp is None:
        raise RuntimeError("HL close failed: response is None")
    status = resp.get("response", {}).get("data", {}).get("statuses", [{}])[0]
    if "error" in status:
        raise RuntimeError(f"HL close error: {status['error']}")
    filled_price = float(status.get("filled", {}).get("avgPx", 0))
    return {"close_price": filled_price}


def hl_open_long(exchange: Exchange, info: Info, coin: str, size_usd: float,
                 sz_decimals_map: dict = None) -> dict:
    mids  = info.all_mids()
    price = float(mids.get(coin, 0))
    if price == 0:
        raise ValueError(f"HL価格取得失敗: {coin}")
    decimals = (sz_decimals_map or {}).get(coin, 6)
    sz = round(size_usd / price, decimals)
    if sz <= 0:
        raise ValueError(f"注文サイズが0: {coin} price={price} decimals={decimals}")
    resp = exchange.market_open(coin, is_buy=True, sz=sz, slippage=0.02)
    status = resp.get("response", {}).get("data", {}).get("statuses", [{}])[0]
    if "error" in status:
        raise RuntimeError(f"HL open error: {status['error']}")
    filled_price = float(status.get("filled", {}).get("avgPx", price))
    return {"size_coin": sz, "entry_price": filled_price}


def hl_close_long(exchange: Exchange, coin: str) -> dict:
    resp = exchange.market_close(coin, slippage=0.02)
    if resp is None:
        raise RuntimeError("HL close failed: response is None")
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


def mexc_open_short(mexc: ccxt.mexc, coin: str, size_usd: float) -> dict:
    symbol    = f"{coin}/USDT:USDT"
    market    = mexc.market(symbol)
    ticker    = mexc.fetch_ticker(symbol)
    price     = float(ticker["bid"] or ticker["last"])
    cs        = float(market.get("contractSize") or 1)
    contracts = max(1, round(size_usd / (price * cs)))
    order     = mexc.create_market_sell_order(symbol, contracts)
    avg_price = float(order.get("average") or order.get("price") or price)
    return {"contracts": contracts, "entry_price": avg_price, "contract_size": cs}


def mexc_close_short(mexc: ccxt.mexc, coin: str, contracts: int) -> dict:
    symbol = f"{coin}/USDT:USDT"
    order  = mexc.create_market_buy_order(
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

    sz_decimals_map = get_sz_decimals(info)

    signals = get_latest_signals(n=2)

    # ── 決済チェック（先に行う）────────────────────────────────
    for coin in list(positions.keys()):
        sig = signals.get(coin, [])
        if not sig:
            continue

        pos        = positions[coin]
        current_fr = abs(sig[-1]["fr"])
        opened     = datetime.strptime(pos["opened_at"], "%Y-%m-%d %H:%M:%S")
        now_dt     = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        dur_h      = (now_dt - opened).total_seconds() / 3600
        est_fr_now = abs(pos["fr_at_entry"]) * dur_h * pos["size_usd"]
        cost       = 0.0017 * pos["size_usd"]
        cost_recovered = est_fr_now >= cost

        # コスト回収済みなら閾値を下げてギリギリまで粘る
        exit_threshold = EXIT_FR_RECOVERED if cost_recovered else EXIT_FR_1H

        if current_fr >= exit_threshold:
            print(f"[HOLD] {coin}  FR={current_fr*100:.4f}%/h  保有継続"
                  f"{'（コスト回収済）' if cost_recovered else ''}")
            continue   # まだFRが稼げる → 保有継続

        print(f"[EXIT] {coin}  FR={current_fr*100:.4f}%/h < {exit_threshold*100:.4f}% → 決済")

        direction = pos.get("direction", "short_fr")  # 旧stateとの後方互換
        side_label = "HL SHORT × MEXC LONG" if direction == "short_fr" else "HL LONG × MEXC SHORT"

        hl_ok, mexc_ok = False, False
        try:
            if direction == "short_fr":
                hl_close_short(exchange, coin)
            else:
                hl_close_long(exchange, coin)
            hl_ok = True
        except Exception as e:
            print(f"  HL close error: {e}")
            tg(f"⚠️ EXIT HL ERROR: {coin}\n{e}")

        try:
            if direction == "short_fr":
                mexc_close_long(mexc, coin, int(pos["mexc_contracts"]))
            else:
                mexc_close_short(mexc, coin, int(pos["mexc_contracts"]))
            mexc_ok = True
        except Exception as e:
            print(f"  MEXC close error: {e}")
            tg(f"⚠️ EXIT MEXC ERROR: {coin}\n{e}")

        if hl_ok and mexc_ok:
            est_fr   = est_fr_now
            est_cost = cost
            net      = est_fr - est_cost

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
                f"{side_label}\n"
                f"#MindRaid #FRArb #仮想通貨 #ClaudeCode"
            )
            send_gmail(
                subject=f"[MindRaid] EXIT: {coin}  net ${net:.2f}",
                body=(
                    f"FR Arb 決済\n\n"
                    f"銘柄: {coin}\n"
                    f"方向: {side_label}\n"
                    f"保有時間: {dur_h:.1f}h\n"
                    f"推定FR収益: ${est_fr:.2f}\n"
                    f"手数料: ${est_cost:.2f}\n"
                    f"推定net: ${net:.2f}\n"
                    f"時刻: {ts} UTC"
                )
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
        avg_fr_raw = sum(r["fr"] for r in rows) / len(rows)
        avg_fr     = abs(avg_fr_raw)
        if avg_fr < MIN_FR_1H:
            continue

        # FR方向で発注サイドを決定
        direction  = "short_fr" if avg_fr_raw < 0 else "long_fr"
        side_label = "HL SHORT × MEXC LONG" if direction == "short_fr" else "HL LONG × MEXC SHORT"

        print(f"[ENTRY] {coin}  avg_FR={avg_fr_raw:.4%}/h  {side_label}  size=${TRADE_SIZE_USD}")

        # HL発注
        try:
            if direction == "short_fr":
                hl_res = hl_open_short(exchange, info, coin, TRADE_SIZE_USD, sz_decimals_map)
            else:
                hl_res = hl_open_long(exchange, info, coin, TRADE_SIZE_USD, sz_decimals_map)
        except Exception as e:
            print(f"  HL open error: {e}")
            tg(f"⚠️ ENTRY HL ERROR: {coin}\n{e}")
            continue

        time.sleep(1)

        # MEXC発注
        try:
            if direction == "short_fr":
                mx_res = mexc_open_long(mexc, coin, TRADE_SIZE_USD)
            else:
                mx_res = mexc_open_short(mexc, coin, TRADE_SIZE_USD)
        except Exception as e:
            print(f"  MEXC open error → HL rollback: {e}")
            tg(f"⚠️ ENTRY MEXC ERROR: {coin}\n{e}\nHL rollback中...")
            try:
                if direction == "short_fr":
                    hl_close_short(exchange, coin)
                else:
                    hl_close_long(exchange, coin)
                tg(f"  HL rollback完了")
            except Exception as re:
                tg(f"  HL rollback失敗: {re}")
            continue

        positions[coin] = {
            "direction":     direction,
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
            f"方向: {side_label}\n"
            f"FR: {avg_fr_raw:.4%}/h\n"
            f"Size: ${TRADE_SIZE_USD}\n"
            f"HL @ {hl_res['entry_price']:.6f}  ({hl_res['size_coin']} coins)\n"
            f"MEXC @ {mx_res['entry_price']:.6f}  ({mx_res['contracts']} contracts)"
        )
        send_gmail(
            subject=f"[MindRaid] ENTRY: {coin}  {side_label}",
            body=(
                f"FR Arb エントリー\n\n"
                f"銘柄: {coin}\n"
                f"方向: {side_label}\n"
                f"FR: {avg_fr_raw:.4%}/h\n"
                f"サイズ: ${TRADE_SIZE_USD}\n"
                f"HL @ {hl_res['entry_price']:.6f}\n"
                f"MEXC @ {mx_res['entry_price']:.6f}\n"
                f"時刻: {ts} UTC"
            )
        )
        print(f"  → エントリー完了")

    # 現在のポジション一覧
    if positions:
        print(f"\n現在のポジション: {list(positions.keys())}")
    else:
        print("\n現在ポジションなし")


if __name__ == "__main__":
    main()
