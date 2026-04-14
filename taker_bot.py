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


def get_hl_open_coins(info: Info, address: str) -> set:
    """HLで実際に開いているポジションのコイン名セットを返す（state.jsonの代替確認）"""
    try:
        user_state = info.user_state(address)
        result = set()
        for pos in user_state.get("assetPositions", []):
            p    = pos.get("position", {})
            coin = p.get("coin")
            szi  = float(p.get("szi", 0))
            if coin and szi != 0:
                result.add(coin)
        return result
    except Exception as e:
        print(f"[WARN] HL実ポジション取得失敗: {e}")
        return set()


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


def hl_force_close(exchange: Exchange, info: Info, coin: str, address: str,
                   sz_decimals_map: dict = None) -> dict:
    """market_close失敗時のフォールバック: user_stateから実サイズを取得してIOC指値でクローズ"""
    user_state = info.user_state(address)
    pos_map = {p["position"]["coin"]: p["position"]
               for p in user_state.get("assetPositions", [])}
    if coin not in pos_map or float(pos_map[coin]["szi"]) == 0:
        return {"close_price": 0.0}  # すでに決済済み

    szi      = float(pos_map[coin]["szi"])  # 正=LONG, 負=SHORT
    is_buy   = szi < 0                      # SHORTを閉じるにはBUY
    decimals = (sz_decimals_map or {}).get(coin, 6)
    sz       = round(abs(szi), decimals)

    mids  = info.all_mids()
    price = float(mids.get(coin, 0))
    if price == 0:
        raise ValueError(f"HL価格取得失敗: {coin}")

    # 3%スリッページのIOC指値（確実にフィルされるよう広め）
    limit_px = round(price * (1.03 if is_buy else 0.97), 6)

    resp = exchange.order(
        coin, is_buy, sz, limit_px,
        order_type={"limit": {"tif": "Ioc"}},
        reduce_only=True,
    )
    if resp is None:
        raise RuntimeError("HL force close: order response is None")
    status = resp.get("response", {}).get("data", {}).get("statuses", [{}])[0]
    if "error" in status:
        raise RuntimeError(f"HL force close error: {status['error']}")
    filled_price = float(status.get("filled", {}).get("avgPx", price))
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


def get_mexc_open_coins(mexc: ccxt.mexc) -> set:
    """MEXCで実際に開いているポジションのコイン名セットを返す"""
    try:
        positions = mexc.fetch_positions()
        result = set()
        for p in positions:
            contracts = float(p.get("contracts") or 0)
            if contracts > 0:
                # symbol = "XXX/USDT:USDT" → coin = "XXX"
                sym = p.get("symbol", "")
                coin_name = sym.split("/")[0] if "/" in sym else sym
                if coin_name:
                    result.add(coin_name)
        return result
    except Exception as e:
        print(f"[WARN] MEXC実ポジション取得失敗: {e}")
        return set()


def mexc_force_close(mexc: ccxt.mexc, coin: str, direction: str) -> dict:
    """mexc close失敗時のフォールバック: fetch_positionsから実サイズを取得してクローズ"""
    symbol    = f"{coin}/USDT:USDT"
    positions = mexc.fetch_positions([symbol])
    pos = next(
        (p for p in positions
         if p.get("symbol") == symbol and float(p.get("contracts") or 0) > 0),
        None
    )
    if pos is None:
        return {"close_price": 0.0}  # すでに決済済み

    contracts = int(float(pos["contracts"]))
    # short_fr: MEXC LONG を閉じる = SELL
    # long_fr:  MEXC SHORT を閉じる = BUY
    if direction == "short_fr":
        order = mexc.create_market_sell_order(symbol, contracts, params={"reduceOnly": True})
    else:
        order = mexc.create_market_buy_order(symbol, contracts, params={"reduceOnly": True})
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
    hl_open_coins   = get_hl_open_coins(info, wallet.address)
    print(f"HL実ポジション: {hl_open_coins or 'なし'}")

    signals = get_latest_signals(n=2)

    # ── 決済チェック（先に行う）────────────────────────────────
    for coin in list(positions.keys()):
        pos = positions[coin]

        # ── dangerポジション：FR閾値監視 → 閾値割れでHLのみ決済 ──
        if pos.get("status") == "danger":
            sig = signals.get(coin, [])
            if not sig:
                tg(f"🚨 危険ポジション保有中: {coin}（FRデータなし）\nHL手動確認してください")
                continue

            current_fr = abs(sig[-1]["fr"])
            opened     = datetime.strptime(pos["opened_at"], "%Y-%m-%d %H:%M:%S")
            now_dt     = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            dur_h      = (now_dt - opened).total_seconds() / 3600

            if current_fr >= EXIT_FR_1H:
                print(f"[DANGER HOLD] {coin}  FR={current_fr*100:.4f}%/h  保有継続（HL裸注意）")
                tg(f"⚠️ 危険ポジション保有中: {coin}  FR={current_fr*100:.4f}%/h\nHL裸ショート・MEXCヘッジなし")
                continue

            print(f"[DANGER EXIT] {coin}  FR={current_fr*100:.4f}%/h < {EXIT_FR_1H*100:.4f}% → HL決済試行")
            direction = pos.get("direction", "short_fr")

            # HL実ポジション事前チェック
            if coin not in hl_open_coins:
                print(f"  HL {coin}: ポジションなし → stateから削除")
                del positions[coin]
                save_state(state)
                tg(f"🧹 DANGERゴースト削除: {coin}\nHLにポジションなし\nstate.jsonをクリーンアップしました")
                continue

            hl_ok = False
            for attempt in range(1, 4):
                try:
                    if direction == "short_fr":
                        hl_close_short(exchange, coin)
                    else:
                        hl_close_long(exchange, coin)
                    hl_ok = True
                    break
                except Exception as e:
                    print(f"  HL danger close attempt {attempt}/3: {e}")
                    open_coins = get_hl_open_coins(info, wallet.address)
                    if coin not in open_coins:
                        print(f"  HL {coin} ポジション消滅確認 → 決済済みとみなす")
                        hl_ok = True
                        break
                    if attempt < 3:
                        time.sleep(2)
            # 3回失敗 → force_closeを最終手段として試行
            if not hl_ok:
                try:
                    hl_force_close(exchange, info, coin, wallet.address, sz_decimals_map)
                    print(f"  HL force close成功")
                    tg(f"⚠️ HL 強制決済実行: {coin}\nmarket_close 3回失敗のためIOC指値注文で強制クローズしました")
                    hl_ok = True
                except Exception as fe:
                    print(f"  HL force close失敗: {fe}")
            if hl_ok:
                del positions[coin]
                save_state(state)
                tg(f"✅ DANGER EXIT完了: {coin}\nHL裸ポジションを決済しました\n保有: {dur_h:.1f}h")
                send_gmail(
                    subject=f"[MindRaid] DANGER EXIT: {coin}",
                    body=f"危険ポジション（HL裸）を自動決済しました。\n銘柄: {coin}\n保有時間: {dur_h:.1f}h\n時刻: {ts} UTC"
                )
            else:
                tg(f"🚨 DANGER EXIT失敗: {coin}\n3回試行しても決済できません\n手動でHL確認してください")
            continue

        # ── 通常ポジション：決済判断 ──────────────────────────
        sig = signals.get(coin, [])
        if not sig:
            continue

        current_fr = abs(sig[-1]["fr"])
        opened     = datetime.strptime(pos["opened_at"], "%Y-%m-%d %H:%M:%S")
        now_dt     = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        dur_h      = (now_dt - opened).total_seconds() / 3600
        est_fr_now = abs(pos["fr_at_entry"]) * dur_h * pos["size_usd"]
        cost       = 0.0017 * pos["size_usd"]
        cost_recovered = est_fr_now >= cost

        exit_threshold = EXIT_FR_RECOVERED if cost_recovered else EXIT_FR_1H

        if current_fr >= exit_threshold:
            print(f"[HOLD] {coin}  FR={current_fr*100:.4f}%/h  保有継続"
                  f"{'（コスト回収済）' if cost_recovered else ''}")
            continue

        print(f"[EXIT] {coin}  FR={current_fr*100:.4f}%/h < {exit_threshold*100:.4f}% → 決済")

        direction  = pos.get("direction", "short_fr")
        side_label = "HL SHORT × MEXC LONG" if direction == "short_fr" else "HL LONG × MEXC SHORT"

        # ── 実ポジション事前チェック ──
        hl_has_pos   = coin in hl_open_coins
        mexc_coins   = get_mexc_open_coins(mexc)
        mexc_has_pos = coin in mexc_coins

        # 両方ともポジションなし → state.jsonのゴースト → 掃除して終了
        if not hl_has_pos and not mexc_has_pos:
            print(f"  {coin}: HL/MEXC両方ポジションなし → stateから削除")
            del positions[coin]
            save_state(state)
            tg(f"🧹 ゴーストポジション削除: {coin}\nHL/MEXC両方にポジションなし\nstate.jsonをクリーンアップしました")
            continue

        # HL決済
        hl_ok = False
        if not hl_has_pos:
            print(f"  HL {coin}: ポジションなし → スキップ")
            hl_ok = True
        else:
            for attempt in range(1, 4):
                try:
                    if direction == "short_fr":
                        hl_close_short(exchange, coin)
                    else:
                        hl_close_long(exchange, coin)
                    hl_ok = True
                    break
                except Exception as e:
                    print(f"  HL close attempt {attempt}/3: {e}")
                    open_coins = get_hl_open_coins(info, wallet.address)
                    if coin not in open_coins:
                        print(f"  HL {coin} ポジション消滅確認 → 決済済みとみなす")
                        hl_ok = True
                        break
                    if attempt < 3:
                        time.sleep(2)
            # 3回失敗 → force_closeを最終手段として試行
            if not hl_ok:
                try:
                    hl_force_close(exchange, info, coin, wallet.address, sz_decimals_map)
                    print(f"  HL force close成功")
                    tg(f"⚠️ HL 強制決済実行: {coin}\nmarket_close 3回失敗のためIOC指値注文で強制クローズしました")
                    hl_ok = True
                except Exception as fe:
                    tg(f"⚠️ EXIT HL ERROR: {coin}\n3回失敗 + 強制決済も失敗\n手動確認してください")
                    print(f"  HL force close失敗: {fe}")

        # MEXC決済
        mexc_ok = False
        if not mexc_has_pos:
            print(f"  MEXC {coin}: ポジションなし → スキップ")
            mexc_ok = True
        else:
            for attempt in range(1, 4):
                try:
                    if direction == "short_fr":
                        mexc_close_long(mexc, coin, int(pos["mexc_contracts"]))
                    else:
                        mexc_close_short(mexc, coin, int(pos["mexc_contracts"]))
                    mexc_ok = True
                    break
                except Exception as e:
                    err_str = str(e)
                    print(f"  MEXC close attempt {attempt}/3: {err_str}")
                    # すでに決済済み（ポジションなし）は成功とみなす
                    if "nonexistent or closed" in err_str or "Position is nonexistent" in err_str:
                        print(f"  MEXC {coin} ポジションなし確認 → 決済済みとみなす")
                        mexc_ok = True
                        break
                    if attempt < 3:
                        time.sleep(2)
            # 3回失敗 → force_closeを最終手段として試行
            if not mexc_ok:
                try:
                    mexc_force_close(mexc, coin, direction)
                    print(f"  MEXC force close成功")
                    tg(f"⚠️ MEXC 強制決済実行: {coin}\n通常クローズ 3回失敗のため実ポジション取得して強制クローズしました")
                    mexc_ok = True
                except Exception as mfe:
                    tg(f"⚠️ EXIT MEXC ERROR: {coin}\n3回失敗 + 強制決済も失敗\n手動確認してください")
                    print(f"  MEXC force close失敗: {mfe}")

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
        else:
            # 片方失敗 → stateは残したまま次スキャンで再試行
            tg(
                f"⚠️ EXIT 部分失敗: {coin}\n"
                f"HL: {'✅' if hl_ok else '❌'}  MEXC: {'✅' if mexc_ok else '❌'}\n"
                f"次のスキャンで再試行します"
            )

    # ── エントリーチェック ──────────────────────────────────────
    for coin, rows in signals.items():
        if coin in positions:
            if positions[coin].get("status") == "danger":
                print(f"[DANGER] {coin} 危険ポジションフラグあり → スキップ")
                tg(f"🚨 危険ポジション未解決: {coin}\nHL手動確認・決済してください")
            continue
        # state.jsonになくてもHL実ポジションがあればスキップ（二重エントリー防止）
        if coin in hl_open_coins:
            print(f"[SKIP] {coin} HL実ポジションあり（state未記録）→ エントリースキップ")
            tg(f"⚠️ {coin} HLにポジションあり（state未記録）\n手動確認してください")
            continue
        if len(positions) >= MAX_POSITIONS:
            print(f"MAX_POSITIONS ({MAX_POSITIONS}) 到達 → スキップ")
            break
        if len(rows) < 2:
            continue

        # 直近2回とも個別にFR閾値超え かつ 平均FR閾値超え
        if not all(abs(r["fr"]) >= MIN_FR_1H for r in rows):
            continue
        avg_fr_raw = sum(r["fr"] for r in rows) / len(rows)
        avg_fr     = abs(avg_fr_raw)
        if avg_fr < MIN_FR_1H:
            continue

        # FR方向で発注サイドを決定
        direction  = "short_fr" if avg_fr_raw < 0 else "long_fr"
        side_label = "HL SHORT × MEXC LONG" if direction == "short_fr" else "HL LONG × MEXC SHORT"

        print(f"[ENTRY] {coin}  avg_FR={avg_fr_raw:.4%}/h  {side_label}  size=${TRADE_SIZE_USD}")

        # HL レバレッジを1xに設定してから発注
        try:
            exchange.update_leverage(1, coin, is_cross=True)
        except Exception as e:
            print(f"  [WARN] HL leverage set failed: {e}")

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
                positions[coin] = {"status": "danger", "opened_at": ts, "reason": str(re)}
                save_state(state)
                tg(f"🚨 危険ポジション記録: {coin}\nHL裸ポジションあり。手動で確認・決済してください")
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
