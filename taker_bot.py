"""
taker_bot.py
FR自動売買ボット（双方向対応、HL × Lighter / MEXC 両対応）
  - EXCHANGE_MODE="LIGHTER" で Lighter DEX と ARB（デフォルト）
  - EXCHANGE_MODE="MEXC" で旧 MEXC フロー（互換用・コメントアウトされていない）
  - net_short_1h = HL_FR_1h - COUNTER_FR_1h
  - net_long_1h  = COUNTER_FR_1h - HL_FR_1h
  - 優位側の net FR/h が閾値を下回ったら決済

コスト (LIGHTER モード): HL taker 0.035%×2 + Lighter taker 0%×2 = 往復0.07%
コスト (MEXC モード)   : HL taker 0.035%×2 + MEXC taker 0.04%×2 = 往復0.15%
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

# ── .env ロード（ローカル実行用）────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Kill switch（STOP ファイルが存在したら即 exit）──────────────
import os as _os, sys as _sys
if _os.path.exists(_os.path.expanduser("~/MindRaid/STOP")):
    print("[KILL-SWITCH] STOP file present, exiting without action.")
    _sys.exit(0)

# ── 設定 ────────────────────────────────────────────────────────
EXCHANGE_MODE   = os.environ.get("EXCHANGE_MODE", "LIGHTER").upper()  # "LIGHTER" or "MEXC"
TRADE_SIZE_USD  = float(os.environ.get("TRADE_SIZE_USD", "100"))  # 1ポジションUSD
MAX_POSITIONS   = int(os.environ.get("MAX_POSITIONS", "4"))        # 最大同時ポジション数
MIN_FR_1H       = 0.00005  # エントリー最小 net FR閾値: 0.005%/h（修正後 true 1h レート基準 / 12hでコスト回収見込み）
MIN_HOLD_H      = 6       # 最低保有時間: FRフリップ以外はこれ未満で決済しない
MAX_LEG_FR      = 0.005   # 片側FR上限: HL/Lighter どちらか |FR|>0.5%/h の銘柄はスパイク扱いで見送り
EXIT_FR_1H      = -1e9    # 旧: 閾値割れEXIT は廃止。フリップ(<0)のみEXIT。互換のため定数は残す
EXIT_FR_RECOVERED = -1e9  # 同上
# MAX_ENTRY_SPREAD: LIGHTER では手数料0のため閾値を小さくしても良いが、
# 価格スリッページ自体のリスクは同じなので 0.15% を維持
MAX_ENTRY_SPREAD = 0.0015   # エントリー時許容スプレッド: 0.15%（不利側。超えたらロールバック）

DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
FUNDING_CSV  = os.path.join(DATA_DIR, "funding_log.csv")
MEXC_FUNDING_CSV    = os.path.join(DATA_DIR, "mexc_funding_log.csv")
LIGHTER_FUNDING_CSV = os.path.join(DATA_DIR, "lighter_funding_log.csv")
STATE_FILE   = os.path.join(DATA_DIR, "taker_state.json")
TRADES_CSV   = os.path.join(DATA_DIR, "trades.csv")

TRADE_FIELDS = [
    "trade_id", "coin", "direction",
    "opened_at_utc", "closed_at_utc", "duration_h",
    "size_usd", "hl_size_coin", "counter_size_coin",
    "entry_hl_fr_1h", "entry_counter_fr_1h", "entry_net_fr_1h",
    "entry_hl_px", "entry_counter_px", "entry_spread",
    "exit_hl_fr_1h", "exit_counter_fr_1h", "exit_net_fr_1h",
    "est_funding_usd", "est_cost_usd", "est_net_usd",
    "actual_hl_funding_usd", "actual_lighter_funding_usd", "actual_total_funding_usd",
    "exit_reason",
]


def fetch_hl_actual_funding(info, main_addr: str, coin: str, opened_at_utc: str, closed_at_utc: str) -> float:
    """HL の userFunding API から期間内の実 funding 受取額（USD）を取得。"""
    try:
        start_ms = int(datetime.strptime(opened_at_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
        end_ms   = int(datetime.strptime(closed_at_utc, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp() * 1000)
        data = info.post("/info", {"type": "userFunding", "user": main_addr, "startTime": start_ms, "endTime": end_ms})
        total = 0.0
        for item in data or []:
            d = item.get("delta", {})
            if d.get("type") == "funding" and d.get("coin") == coin:
                total += float(d.get("usdc", 0))
        return total
    except Exception as e:
        print(f"[WARN] HL 実 funding 取得失敗: {e}")
        return 0.0


def fetch_lighter_actual_funding(coin: str, opened_at_utc: str, closed_at_utc: str,
                                  size_usd: float, direction: str) -> float:
    """
    Lighter は per-account funding 履歴 API がないので、
    lighter_funding_log.csv から期間内の FR を合計して USD 換算で推定する。
    """
    try:
        start = datetime.strptime(opened_at_utc, "%Y-%m-%d %H:%M:%S")
        end   = datetime.strptime(closed_at_utc, "%Y-%m-%d %H:%M:%S")
        total_fr_accum = 0.0  # 期間内 FR の累計（時間加重平均 × 保有時間相当）
        sample_count = 0
        with open(LIGHTER_FUNDING_CSV) as f:
            for row in csv.DictReader(f):
                ts = datetime.strptime(row["timestamp_utc"], "%Y-%m-%d %H:%M:%S")
                if start <= ts <= end and row["coin"] == coin:
                    total_fr_accum += float(row["funding_rate_1h"])
                    sample_count += 1
        if sample_count == 0:
            return 0.0
        # スキャン間隔は約 30 分 = 0.5 時間
        avg_fr = total_fr_accum / sample_count
        duration_h = (end - start).total_seconds() / 3600
        # direction: short_fr = HL SHORT × Lighter LONG
        #   → Lighter long は FR 正で「払う」（FR 負で受取）
        # direction: long_fr  = HL LONG × Lighter SHORT
        #   → Lighter short は FR 正で「受取」
        sign = -1 if direction == "short_fr" else 1
        return avg_fr * duration_h * size_usd * sign
    except Exception as e:
        print(f"[WARN] Lighter 実 funding 計算失敗: {e}")
        return 0.0


def check_losing_streak(state: dict, tg_func, n: int = 3) -> None:
    """trades.csv の末尾 n 件が全て est_net_usd < 0 なら Telegram で通知。
    同じ末尾 trade_id で二重通知しないよう state に記録。"""
    try:
        if not os.path.exists(TRADES_CSV):
            return
        with open(TRADES_CSV) as f:
            rows = list(csv.DictReader(f))
        if len(rows) < n:
            return
        last = rows[-n:]
        nets = []
        for r in last:
            v = r.get("est_net_usd", "")
            if v == "" or v is None:
                return  # 不完全データはスキップ
            try:
                nets.append(float(v))
            except ValueError:
                return
        if all(net < 0 for net in nets):
            last_id = last[-1]["trade_id"]
            if state.get("last_streak_alert_trade_id") == last_id:
                return  # すでに通知済み
            total_loss = sum(nets)
            msg_lines = [f"⚠️ {n}連敗検知！"]
            for r, net in zip(last, nets):
                msg_lines.append(f"  {r['coin']} {r['direction']}  net=${net:+.3f}  ({r['duration_h']}h)")
            msg_lines.append(f"合計: ${total_loss:+.3f}")
            msg_lines.append("パラメータ見直しを検討してください（閾値/サイズ/銘柄除外等）")
            tg_func("\n".join(msg_lines))
            state["last_streak_alert_trade_id"] = last_id
            save_state(state)
            print(f"  [streak] {n}連敗通知送信: 合計 ${total_loss:+.3f}")
    except Exception as e:
        print(f"[WARN] 連敗チェック失敗: {e}")


def log_trade_record(pos: dict, coin: str, closed_at: str, duration_h: float,
                     exit_hl_fr: float, exit_counter_fr: float, exit_net_fr: float,
                     est_funding: float, est_cost: float, est_net: float,
                     exit_reason: str,
                     actual_hl_funding: float = None,
                     actual_lighter_funding: float = None):
    """確定したトレードを data/trades.csv に1行追記する（FB用ログ）。"""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        file_exists = os.path.exists(TRADES_CSV)
        trade_id = f"{pos.get('opened_at','').replace(' ','T').replace(':','')}-{coin}"
        row = {
            "trade_id":            trade_id,
            "coin":                coin,
            "direction":           pos.get("direction", ""),
            "opened_at_utc":       pos.get("opened_at", ""),
            "closed_at_utc":       closed_at,
            "duration_h":          round(duration_h, 3),
            "size_usd":            pos.get("size_usd", ""),
            "hl_size_coin":        pos.get("hl_size_coin", ""),
            "counter_size_coin":   pos.get("counter_size_coin", ""),
            "entry_hl_fr_1h":      pos.get("entry_hl_fr_1h", ""),
            "entry_counter_fr_1h": pos.get("entry_mexc_fr_1h", pos.get("entry_counter_fr_1h", "")),
            "entry_net_fr_1h":     pos.get("entry_net_fr_1h", ""),
            "entry_hl_px":         pos.get("hl_entry_price", ""),
            "entry_counter_px":    pos.get("counter_entry_price", ""),
            "entry_spread":        pos.get("entry_spread", ""),
            "exit_hl_fr_1h":       round(exit_hl_fr, 8) if exit_hl_fr is not None else "",
            "exit_counter_fr_1h":  round(exit_counter_fr, 8) if exit_counter_fr is not None else "",
            "exit_net_fr_1h":      round(exit_net_fr, 8) if exit_net_fr is not None else "",
            "est_funding_usd":     round(est_funding, 4) if est_funding is not None else "",
            "est_cost_usd":        round(est_cost, 4) if est_cost is not None else "",
            "est_net_usd":         round(est_net, 4) if est_net is not None else "",
            "actual_hl_funding_usd":     round(actual_hl_funding, 4) if actual_hl_funding is not None else "",
            "actual_lighter_funding_usd": round(actual_lighter_funding, 4) if actual_lighter_funding is not None else "",
            "actual_total_funding_usd":  (
                round((actual_hl_funding or 0) + (actual_lighter_funding or 0), 4)
                if (actual_hl_funding is not None or actual_lighter_funding is not None) else ""
            ),
            "exit_reason":         exit_reason,
        }
        with open(TRADES_CSV, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=TRADE_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        print(f"  [trade_log] {trade_id} 記録完了 → {TRADES_CSV}")
    except Exception as e:
        print(f"[WARN] trade record 書き込み失敗: {e}")

HL_PRIVATE_KEY  = os.environ["HL_PRIVATE_KEY"]
HL_WALLET_ADDRESS = os.environ.get("HL_WALLET_ADDRESS", "")  # メインウォレット（API Wallet 使用時に必須）
# MEXC 鍵は MEXC モード時のみ必須（LIGHTER モード時は空でも可）
MEXC_API_KEY    = os.environ.get("MEXC_API_KEY", "")
MEXC_API_SECRET = os.environ.get("MEXC_API_SECRET", "")
TG_TOKEN        = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT         = os.environ.get("TELEGRAM_CHAT_ID", "")
X_API_KEY       = os.environ.get("X_API_KEY", "")
X_API_SECRET    = os.environ.get("X_API_SECRET", "")
X_ACCESS_TOKEN  = os.environ.get("X_ACCESS_TOKEN", "")
X_ACCESS_SECRET = os.environ.get("X_ACCESS_TOKEN_SECRET", "")
GMAIL_ADDRESS   = os.environ.get("GMAIL_USER", "") or os.environ.get("GMAIL_ADDRESS", "")
GMAIL_PASSWORD  = os.environ.get("GMAIL_PASS", "") or os.environ.get("GMAIL_APP_PASSWORD", "")
EMAIL_TO        = os.environ.get("EMAIL_TO", "") or GMAIL_ADDRESS

# ── Lighter クライアントは LIGHTER モード時のみ import ────────────
lighter_client = None
if EXCHANGE_MODE == "LIGHTER":
    try:
        import lighter_client as _lc
        lighter_client = _lc
    except ImportError as e:
        print(f"[FATAL] EXCHANGE_MODE=LIGHTER なのに lighter_client を import できませんでした: {e}")
        raise


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
        msg["To"]      = EMAIL_TO
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
def get_latest_hl_signals(n: int = 2) -> dict:
    """HL各銘柄の直近n回のシグナルを返す {coin: [{'ts','fr','taker'}, ...]}"""
    if not os.path.exists(FUNDING_CSV):
        print(f"[WARN] funding CSV が見つかりません: {FUNDING_CSV}")
        return {}

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


def get_latest_net_signals(n: int = 2) -> dict:
    """
    HL × Counter (Lighter or MEXC) の直近n回からネットFRシグナルを返す。
    net_short_1h = HL_SHORT受取 + COUNTER_LONG受取  = hl_fr_1h - counter_fr_1h
    net_long_1h  = HL_LONG受取  + COUNTER_SHORT受取 = counter_fr_1h - hl_fr_1h

    EXCHANGE_MODE に応じて Counter CSV（LIGHTER_FUNDING_CSV or MEXC_FUNDING_CSV）を使用。
    戻り値の dict には mexc_fr_1h / counter_fr_1h の両キーを含める
    （旧コード互換のため mexc_fr_1h も保持）。
    """
    if not os.path.exists(FUNDING_CSV):
        print(f"[WARN] HL funding CSV が見つかりません: {FUNDING_CSV}")
        return {}

    counter_csv = LIGHTER_FUNDING_CSV if EXCHANGE_MODE == "LIGHTER" else MEXC_FUNDING_CSV
    counter_label = "Lighter" if EXCHANGE_MODE == "LIGHTER" else "MEXC"
    if not os.path.exists(counter_csv):
        print(f"[WARN] {counter_label} funding CSV が見つかりません: {counter_csv}")
        return {}

    hl_rows_by_coin: dict = defaultdict(list)
    ct_rows_by_coin: dict = defaultdict(list)

    with open(FUNDING_CSV) as f:
        for row in csv.DictReader(f):
            coin = row.get("coin", "")
            ts = row.get("timestamp_utc", "")
            if not coin or not ts:
                continue
            try:
                hl_rate_1h = float(row["funding_rate_1h"])
            except Exception:
                continue
            hl_rows_by_coin[coin].append({"ts": ts, "hl_fr_1h": hl_rate_1h})

    with open(counter_csv) as f:
        for row in csv.DictReader(f):
            coin = row.get("coin", "")
            ts = row.get("timestamp_utc", "")
            if not coin or not ts:
                continue
            try:
                ct_rate_1h = float(row["funding_rate_1h"])
            except Exception:
                continue
            ct_rows_by_coin[coin].append({"ts": ts, "counter_fr_1h": ct_rate_1h})

    result = {}
    for coin, hl_rows in hl_rows_by_coin.items():
        ct_rows = ct_rows_by_coin.get(coin, [])
        if not ct_rows:
            continue

        hl_by_ts = {r["ts"]: float(r["hl_fr_1h"]) for r in hl_rows}
        ct_by_ts = {r["ts"]: float(r["counter_fr_1h"]) for r in ct_rows}
        common_ts = sorted(set(hl_by_ts.keys()) & set(ct_by_ts.keys()))
        if not common_ts:
            continue

        combined = []
        for ts in common_ts[-n:]:
            hl_fr_1h = hl_by_ts[ts]
            counter_fr_1h = ct_by_ts[ts]
            net_short = hl_fr_1h - counter_fr_1h
            combined.append({
                "ts": ts,
                "hl_fr_1h": hl_fr_1h,
                "counter_fr_1h": counter_fr_1h,
                # 旧コード互換（"mexc_fr_1h" キーも保持）
                "mexc_fr_1h": counter_fr_1h,
                "net_short_1h": net_short,
                "net_long_1h": -net_short,
            })
        result[coin] = combined

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
        return None  # API失敗は「不明」として扱う（Noneと空setを区別）


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


# ── Counter-Exchange 抽象化レイヤー ───────────────────────────────
# EXCHANGE_MODE に応じて Lighter / MEXC を切り替える統一 API
# state["positions"][coin]["exchange"] で開いたポジションの取引所を記録。
# 既存の MEXC ポジションは後方互換（exchange キーなし=mexc とみなす）

def counter_init():
    """Counter-exchange クライアントを初期化。"""
    if EXCHANGE_MODE == "LIGHTER":
        return None  # lighter_client はモジュールレベル関数のためクライアント不要
    else:
        mexc = get_mexc()
        mexc.load_markets()
        return mexc


def counter_get_open_coins(client) -> set:
    """Counter-exchange の実ポジション銘柄セットを返す（None=API失敗）。"""
    if EXCHANGE_MODE == "LIGHTER":
        positions = lighter_client.get_positions()
        if positions is None:
            return None
        return {p["symbol"] for p in positions}
    else:
        return get_mexc_open_coins(client)


def counter_open_long(client, coin: str, size_usd: float) -> dict:
    """Counter-exchange で LONG を開く。戻り値は normalize 済み。
    返却 dict: {size_coin, entry_price, exchange, [contracts, contract_size]}
    """
    if EXCHANGE_MODE == "LIGHTER":
        # 発注前に 1x cross に設定（失敗しても発注自体は続行）
        try:
            lighter_client.set_leverage(symbol=coin, leverage=1, cross_margin=True)
        except Exception as e:
            print(f"  [WARN] Lighter set_leverage failed: {e}")
        res = lighter_client.place_order(symbol=coin, side="buy", size_usd=size_usd)
        if res is None:
            raise RuntimeError(f"Lighter open long failed: {coin}")
        return {
            "size_coin":   res["size_coin"],
            "entry_price": res["entry_price"],
            "exchange":    "lighter",
        }
    else:
        res = mexc_open_long(client, coin, size_usd)
        return {
            "size_coin":     res["contracts"] * res["contract_size"],
            "entry_price":   res["entry_price"],
            "contracts":     res["contracts"],
            "contract_size": res["contract_size"],
            "exchange":      "mexc",
        }


def counter_open_short(client, coin: str, size_usd: float) -> dict:
    if EXCHANGE_MODE == "LIGHTER":
        try:
            lighter_client.set_leverage(symbol=coin, leverage=1, cross_margin=True)
        except Exception as e:
            print(f"  [WARN] Lighter set_leverage failed: {e}")
        res = lighter_client.place_order(symbol=coin, side="sell", size_usd=size_usd)
        if res is None:
            raise RuntimeError(f"Lighter open short failed: {coin}")
        return {
            "size_coin":   res["size_coin"],
            "entry_price": res["entry_price"],
            "exchange":    "lighter",
        }
    else:
        res = mexc_open_short(client, coin, size_usd)
        return {
            "size_coin":     res["contracts"] * res["contract_size"],
            "entry_price":   res["entry_price"],
            "contracts":     res["contracts"],
            "contract_size": res["contract_size"],
            "exchange":      "mexc",
        }


def counter_close(client, coin: str, direction: str, pos_state: dict) -> dict:
    """Counter-exchange のポジションをクローズ。state の情報を元に適切に処理。
    direction: "short_fr"=Counterは LONG 保有、"long_fr"=Counterは SHORT 保有
    """
    exchange = pos_state.get("exchange", "mexc")  # 後方互換: 未記載なら mexc
    if exchange == "lighter":
        size_coin = float(pos_state.get("counter_size_coin") or pos_state.get("size_coin") or 0)
        close_side = "sell" if direction == "short_fr" else "buy"
        res = lighter_client.close_position(symbol=coin, side=close_side, size_coin=size_coin)
        if res is None:
            raise RuntimeError(f"Lighter close failed: {coin}")
        return res
    else:
        contracts = int(pos_state.get("mexc_contracts", 0))
        if direction == "short_fr":
            return mexc_close_long(client, coin, contracts)
        else:
            return mexc_close_short(client, coin, contracts)


def counter_force_close(client, coin: str, direction: str, pos_state: dict = None) -> dict:
    """Counter-exchange のポジションを強制クローズ。"""
    exchange = (pos_state or {}).get("exchange") or (
        "lighter" if EXCHANGE_MODE == "LIGHTER" else "mexc"
    )
    if exchange == "lighter":
        res = lighter_client.force_close_position(symbol=coin)
        # ポジションが既になければ None が返る
        return res or {"close_price": 0.0}
    else:
        return mexc_force_close(client, coin, direction)


def counter_label() -> str:
    return "Lighter" if EXCHANGE_MODE == "LIGHTER" else "MEXC"


# ── MEXC ─────────────────────────────────────────────────────────
def get_mexc() -> ccxt.mexc:
    return ccxt.mexc({
        "apiKey": MEXC_API_KEY,
        "secret": MEXC_API_SECRET,
        "options": {"defaultType": "swap"},
    })


def _mexc_coin_from_symbol(symbol: str) -> str:
    if not symbol:
        return ""
    if "/" in symbol:
        return symbol.split("/")[0]
    if "_" in symbol:
        return symbol.split("_")[0]
    return symbol


def _mexc_position_side(pos: dict) -> str:
    """返り値: 'long' | 'short' | ''"""
    info = pos.get("info", {}) if isinstance(pos.get("info"), dict) else {}
    raw = (
        pos.get("side")
        or pos.get("positionSide")
        or info.get("positionSide")
        or info.get("holdSide")
        or ""
    )
    raw_l = str(raw).lower()
    if "long" in raw_l or raw_l == "buy":
        return "long"
    if "short" in raw_l or raw_l == "sell":
        return "short"
    try:
        c = float(pos.get("contracts") or 0)
        if c < 0:
            return "short"
    except Exception:
        pass
    return ""


def _mexc_position_contracts(pos: dict) -> float:
    info = pos.get("info", {}) if isinstance(pos.get("info"), dict) else {}
    raw = pos.get("contracts")
    if raw is None:
        raw = info.get("vol") or info.get("positionQty") or 0
    try:
        return abs(float(raw or 0))
    except Exception:
        return 0.0


def _mexc_position_symbol(pos: dict) -> str:
    info = pos.get("info", {}) if isinstance(pos.get("info"), dict) else {}
    sym = pos.get("symbol") or info.get("symbol") or ""
    return str(sym)


def _mexc_create_open_market(mexc: ccxt.mexc, symbol: str, is_buy: bool,
                             contracts: int, position_side: str):
    """口座モード差異を吸収して成行オープンを試行する。"""
    candidates = []
    if position_side:
        candidates.append({"positionSide": position_side.upper()})
        candidates.append({"holdSide": position_side.lower()})
    candidates.append({})

    last_err = None
    for params in candidates:
        try:
            if is_buy:
                return mexc.create_market_buy_order(symbol, contracts, params=params)
            return mexc.create_market_sell_order(symbol, contracts, params=params)
        except Exception as e:
            last_err = e
    raise last_err


def _mexc_create_reduce_only_market(mexc: ccxt.mexc, symbol: str, is_buy: bool,
                                    contracts: int, position_side: str):
    """口座モード差異を吸収してreduceOnly成行を試行する。"""
    candidates = []
    if position_side:
        candidates.append({"reduceOnly": True, "positionSide": position_side.upper()})
        candidates.append({"reduceOnly": True, "holdSide": position_side.lower()})
    candidates.append({"reduceOnly": True})

    last_err = None
    for params in candidates:
        try:
            if is_buy:
                return mexc.create_market_buy_order(symbol, contracts, params=params)
            return mexc.create_market_sell_order(symbol, contracts, params=params)
        except Exception as e:
            last_err = e
    raise last_err


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
    order = _mexc_create_open_market(mexc, symbol, is_buy=True, contracts=contracts, position_side="long")
    avg_price = float(order.get("average") or order.get("price") or price)
    return {"contracts": contracts, "entry_price": avg_price, "contract_size": cs}


def mexc_close_long(mexc: ccxt.mexc, coin: str, contracts: int) -> dict:
    symbol = f"{coin}/USDT:USDT"
    order = _mexc_create_reduce_only_market(
        mexc, symbol, is_buy=False, contracts=contracts, position_side="long"
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
    order     = _mexc_create_open_market(mexc, symbol, is_buy=False, contracts=contracts, position_side="short")
    avg_price = float(order.get("average") or order.get("price") or price)
    return {"contracts": contracts, "entry_price": avg_price, "contract_size": cs}


def mexc_close_short(mexc: ccxt.mexc, coin: str, contracts: int) -> dict:
    symbol = f"{coin}/USDT:USDT"
    order = _mexc_create_reduce_only_market(
        mexc, symbol, is_buy=True, contracts=contracts, position_side="short"
    )
    avg_price = float(order.get("average") or order.get("price") or 0)
    return {"close_price": avg_price}


def get_mexc_open_coins(mexc: ccxt.mexc) -> set:
    """MEXCで実際に開いているポジションのコイン名セットを返す"""
    try:
        positions = mexc.fetch_positions()
        result = set()
        for p in positions:
            contracts = _mexc_position_contracts(p)
            if contracts > 0:
                # symbol = "XXX/USDT:USDT" or "XXX_USDT" → coin = "XXX"
                sym = _mexc_position_symbol(p)
                coin_name = _mexc_coin_from_symbol(sym)
                if coin_name:
                    result.add(coin_name)
        return result
    except Exception as e:
        print(f"[WARN] MEXC実ポジション取得失敗: {e}")
        return None  # API失敗は「不明」として扱う


def mexc_force_close(mexc: ccxt.mexc, coin: str, direction: str) -> dict:
    """mexc close失敗時のフォールバック: fetch_positionsから実サイズを取得してクローズ"""
    symbol    = f"{coin}/USDT:USDT"
    positions = mexc.fetch_positions([symbol])
    expected_side = "long" if direction == "short_fr" else "short"
    alt_symbol = symbol.replace("/", "_").replace(":USDT", "")
    same_symbol = [
        p for p in positions
        if _mexc_position_symbol(p) in {symbol, alt_symbol}
        and _mexc_position_contracts(p) > 0
    ]
    if not same_symbol:
        pos = None
    else:
        same_side = [p for p in same_symbol if _mexc_position_side(p) == expected_side]
        candidates = same_side or same_symbol
        pos = max(candidates, key=_mexc_position_contracts)
    if pos is None:
        return {"close_price": 0.0}  # すでに決済済み

    contracts = int(max(1, round(_mexc_position_contracts(pos))))
    pos_side = _mexc_position_side(pos) or expected_side
    # LONGを閉じる = SELL, SHORTを閉じる = BUY
    is_buy = pos_side == "short"
    order = _mexc_create_reduce_only_market(
        mexc, symbol, is_buy=is_buy, contracts=contracts, position_side=pos_side
    )
    avg_price = float(order.get("average") or order.get("price") or 0)
    return {"close_price": avg_price}


# ── メインロジック ────────────────────────────────────────────────
def main():
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"=== taker_bot {ts} ===")

    state     = load_state()
    positions = state["positions"]

    print(f"=== EXCHANGE_MODE = {EXCHANGE_MODE} ===")

    wallet   = Account.from_key(HL_PRIVATE_KEY)
    info     = Info(skip_ws=True)
    # API Wallet 使用時は main wallet address を渡す（メインアカウントに対して発注するため）
    main_addr = HL_WALLET_ADDRESS or wallet.address
    exchange = Exchange(wallet, account_address=main_addr)
    counter_client = counter_init()   # Lighter は None, MEXC は ccxt client
    # 旧コード互換のため mexc 変数も残す（MEXC モード時のみ）
    mexc = counter_client if EXCHANGE_MODE == "MEXC" else None

    # ── Lighter 署名ガード（HL 発注前に check_client で鍵不一致を検知）──
    if EXCHANGE_MODE == "LIGHTER":
        sign_err = lighter_client.check_signer_valid()
        if sign_err is not None:
            print(f"[WARN] Lighter 署名鍵不一致を検知 → 自律修復を試みます\n{sign_err}")
            tg(f"⚠️ Lighter との接続キーがずれています\n自動で修復します（取引は一時停止）...")
            try:
                import asyncio as _asyncio
                import system_setup as _ss
                _asyncio.run(_ss.setup_force())
                sign_err2 = lighter_client.check_signer_valid()
                if sign_err2 is None:
                    print("[OK] 自律修復成功 → 処理を継続します")
                    tg("✅ 接続キーの自動修復に成功しました。取引を再開します。")
                else:
                    raise RuntimeError(sign_err2)
            except Exception as e:
                msg = (
                    f"🚨 接続キーの自動修復に失敗しました。今回の取引はスキップします。\n{e}\n"
                    "手動対応: python3 system_setup.py --force を実行してください。"
                )
                print(f"[ABORT] {msg}")
                tg(msg)
                send_gmail(
                    subject="[MindRaid] 🚨 Lighter 署名鍵 自律修復失敗",
                    body=f"{msg}\n時刻: {ts} UTC",
                )
                return
        print("[OK] Lighter signer check_client: 一致")

    sz_decimals_map = get_sz_decimals(info)
    hl_open_coins   = get_hl_open_coins(info, main_addr)
    if hl_open_coins is None:
        print("[ABORT] HL実ポジション取得失敗 → 安全のため全処理スキップ")
        tg("🚨 HL のポジション情報が取得できませんでした\n安全のため今回の取引はすべてスキップします")
        send_gmail(
            subject="[MindRaid] taker_bot ABORT: HL API失敗",
            body=f"HL実ポジション取得失敗のためスキップ。\n時刻: {ts} UTC"
        )
        return
    print(f"HL実ポジション: {hl_open_coins or 'なし'}")

    hl_signals = get_latest_hl_signals(n=2)
    signals    = get_latest_net_signals(n=2)

    # ── 決済チェック（先に行う）────────────────────────────────
    hold_lines: list[str] = []  # HOLD サマリー（ループ後にまとめて Telegram 送信）
    for coin in list(positions.keys()):
        pos = positions[coin]

        # ── dangerポジション：FR閾値監視 → 閾値割れでHLのみ決済 ──
        if pos.get("status") == "danger":
            sig = hl_signals.get(coin, [])
            if not sig:
                tg(f"🚨 {coin}: FRデータが取れないため状態確認できません\nHLを直接確認してください")
                continue

            current_fr = abs(sig[-1]["fr"])
            opened     = datetime.strptime(pos["opened_at"], "%Y-%m-%d %H:%M:%S")
            now_dt     = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
            dur_h      = (now_dt - opened).total_seconds() / 3600

            if current_fr >= EXIT_FR_1H:
                print(f"[DANGER HOLD] {coin}  FR={current_fr*100:.4f}%/h  保有継続（HL裸注意）")
                tg(f"⚠️ {coin}: 片側だけポジションが残っています（HL のみ）\nFR={current_fr*100:.4f}%/h  まだ決済基準内なので様子見中")
                continue

            print(f"[DANGER EXIT] {coin}  FR={current_fr*100:.4f}%/h < {EXIT_FR_1H*100:.4f}% → HL決済試行")
            direction = pos.get("direction", "short_fr")

            # HL実ポジション事前チェック
            if coin not in hl_open_coins:
                print(f"  HL {coin}: ポジションなし → stateから削除")
                del positions[coin]
                save_state(state)
                tg(f"🧹 {coin}: HL にポジションが見つかりません。管理データから削除しました。")
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
                    open_coins = get_hl_open_coins(info, main_addr)
                    if open_coins is not None and coin not in open_coins:
                        print(f"  HL {coin} ポジション消滅確認 → 決済済みとみなす")
                        hl_ok = True
                        break
                    if open_coins is None:
                        print("  HL実ポジション再確認失敗 → この試行では判定保留")
                    if attempt < 3:
                        time.sleep(2)
            # 3回失敗 → force_closeを最終手段として試行
            if not hl_ok:
                try:
                    hl_force_close(exchange, info, coin, main_addr, sz_decimals_map)
                    print(f"  HL force close成功")
                    tg(f"⚠️ {coin}: 通常の決済ができなかったため強制決済しました（HL）")
                    hl_ok = True
                except Exception as fe:
                    print(f"  HL force close失敗: {fe}")
            if hl_ok:
                log_trade_record(
                    pos, coin, ts, dur_h,
                    exit_hl_fr=current_fr, exit_counter_fr=None, exit_net_fr=None,
                    est_funding=None, est_cost=None, est_net=None,
                    exit_reason="danger",
                )
                del positions[coin]
                save_state(state)
                tg(f"✅ {coin}: 片側ポジションを決済しました\n保有時間: {dur_h:.1f}h")
                send_gmail(
                    subject=f"[MindRaid] DANGER EXIT: {coin}",
                    body=f"危険ポジション（HL裸）を自動決済しました。\n銘柄: {coin}\n保有時間: {dur_h:.1f}h\n時刻: {ts} UTC"
                )
            else:
                tg(f"🚨 {coin}: 決済を3回試みましたが失敗しました\nHL を直接確認してください")
                send_gmail(
                    subject=f"[MindRaid] 🚨 DANGER EXIT FAILED: {coin}",
                    body=f"危険ポジション（HL裸）の決済が失敗しました。手動でHL確認・決済してください。\n\n銘柄: {coin}\n時刻: {ts} UTC"
                )
            continue

        # ── 通常ポジション：決済判断 ──────────────────────────
        sig = signals.get(coin, [])
        if not sig:
            print(f"[SKIP] {coin} ネットFRデータ不足（HL/{counter_label()}両方の最新データが未取得）")
            tg(f"⚠️ {coin}: FRデータが揃っていないため今回の決済判断をスキップします")
            continue

        # 取引所ラベル（ポジションを開いた取引所を表示）
        pos_exchange = pos.get("exchange", "mexc")  # 後方互換
        counter_name = "Lighter" if pos_exchange == "lighter" else "MEXC"

        direction  = pos.get("direction", "short_fr")
        side_label = (f"HL SHORT × {counter_name} LONG" if direction == "short_fr"
                      else f"HL LONG × {counter_name} SHORT")

        last_row = sig[-1]
        current_net_fr = float(last_row["net_short_1h"] if direction == "short_fr" else last_row["net_long_1h"])
        hl_fr_now      = float(last_row["hl_fr_1h"])
        counter_fr_now = float(last_row.get("counter_fr_1h", last_row.get("mexc_fr_1h", 0)))
        opened     = datetime.strptime(pos["opened_at"], "%Y-%m-%d %H:%M:%S")
        now_dt     = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        dur_h      = (now_dt - opened).total_seconds() / 3600
        entry_net_fr_1h = float(pos.get("entry_net_fr_1h", pos.get("fr_at_entry", 0)))
        # 実 funding ベースで現時点の推定収益を算出（entry FR × 時間は過大推定になる）
        try:
            _hl_fund_now = fetch_hl_actual_funding(info, main_addr, coin, pos.get("opened_at", ""), ts)
        except Exception:
            _hl_fund_now = 0.0
        if pos_exchange == "lighter":
            try:
                _lt_fund_now = fetch_lighter_actual_funding(
                    coin, pos.get("opened_at", ""), ts,
                    pos.get("size_usd", TRADE_SIZE_USD), direction
                )
            except Exception:
                _lt_fund_now = 0.0
        else:
            _lt_fund_now = 0.0
        est_fr_now = _hl_fund_now + _lt_fund_now
        # コスト: Lighter なら 0.07% (HL x2 のみ)、MEXC なら 0.17% (HL + MEXC)
        cost_rate  = 0.0007 if pos_exchange == "lighter" else 0.0017
        cost       = cost_rate * pos["size_usd"]
        cost_recovered = est_fr_now >= cost

        # 新ロジック: MIN_HOLD_H 未満は無条件ホールド、以降は net_fr が負に転じた時のみ EXIT
        # 旧の EXIT_FR_1H / EXIT_FR_RECOVERED しきい値は廃止（コスト負けの主因だった）
        if dur_h < MIN_HOLD_H:
            exit_threshold = -1e9  # 初期 6h は絶対にホールド
        else:
            exit_threshold = 0.0   # 以降はフリップ(<0)のみ EXIT

        # ── 表示用：方向別の受取額に変換（計算値 current_net_fr には影響なし）──
        # short_fr: HL SHORT → +hl_fr 受取 / Counter LONG → -counter_fr 受取
        # long_fr : HL LONG  → -hl_fr 受取 / Counter SHORT → +counter_fr 受取
        if direction == "short_fr":
            hl_earn      = +hl_fr_now
            counter_earn = -counter_fr_now
        else:  # long_fr
            hl_earn      = -hl_fr_now
            counter_earn = +counter_fr_now

        if current_net_fr >= exit_threshold:
            cost_tag = "手数料回収済み" if cost_recovered else "手数料まだ回収中"
            if dur_h < MIN_HOLD_H:
                hold_reason = f"最低保有 {MIN_HOLD_H}h 未満 → ホールド確定（残り {MIN_HOLD_H - dur_h:.1f}h）"
            else:
                hold_reason = "net FR がまだプラス → ホールド継続（マイナス転落で決済）"
            print(
                f"[HOLD] {coin}  netFR={current_net_fr*100:+.4f}%/h  "
                f"(HL={hl_fr_now*100:+.4f}%/h, {counter_name}={counter_fr_now*100:+.4f}%/h) 保有継続"
            )
            # ── 表示用：プラス転換までの推定時間（現 FR 継続仮定。計算・判定には未使用）──
            net_now = est_fr_now - cost
            if net_now >= 0:
                be_line = "   ✅ 既に黒字（プラス転換済み）"
            else:
                remaining   = -net_now  # まだ必要な額（正）
                hourly_earn = current_net_fr * pos["size_usd"]  # 現 FR が続く仮定の時給($/h)
                if hourly_earn <= 0:
                    be_line = "   📍 プラスまでの時間: 未確定（現 FR ≤ 0）"
                else:
                    be_hours = remaining / hourly_earn
                    # MAX_HOLD は未定義のため 72h を暫定閾値として使用
                    if be_hours > 72:
                        be_line = f"   ⚠ MAX_HOLD(72h) 超過見込み: プラスまで ~{be_hours:.1f}h"
                    elif be_hours > 24:
                        be_line = f"   ⚠ プラスまで ~{be_hours:.1f}h（長時間注意）"
                    else:
                        be_line = f"   📍 プラスまで あと ~{be_hours:.1f}h"

            hold_lines.append(
                f"🔵 {coin}  持ち続ける（{cost_tag}）\n"
                f"   {hold_reason}\n"
                f"   net FR {current_net_fr*100:+.4f}%/h　(HL受取 {hl_earn*100:+.4f}%/h ＋ {counter_name}受取 {counter_earn*100:+.4f}%/h)\n"
                f"   保有 {dur_h:.1f}h　収益 ${est_fr_now:.2f} − 手数料 ${cost:.2f} = 手取り ${est_fr_now - cost:+.2f}\n"
                f"{be_line}"
            )
            continue

        # EXIT 条件: MIN_HOLD_H 経過後に net FR が負転落
        print(
            f"[EXIT] {coin}  netFR={current_net_fr*100:+.4f}%/h "
            f"(HL={hl_fr_now*100:+.4f}%/h, {counter_name}={counter_fr_now*100:+.4f}%/h) "
            f"→ フリップ決済 (保有 {dur_h:.1f}h)"
        )
        tg(
            f"🔴 {coin} 決済します\n"
            f"理由: net FR がマイナスに転落（{current_net_fr*100:+.4f}%/h）\n"
            f"HL受取 {hl_earn*100:+.4f}%/h ＋ {counter_name}受取 {counter_earn*100:+.4f}%/h\n"
            f"保有時間: {dur_h:.1f}h　収益 ${est_fr_now:.2f} − 手数料 ${cost:.2f} = 手取り ${est_fr_now - cost:+.2f}"
        )

        # ── 実ポジション事前チェック ──
        hl_has_pos = coin in hl_open_coins
        # ポジションを開いた取引所で実ポジション確認（モード異なる場合も正しい取引所を見る）
        if pos_exchange == "lighter":
            counter_open_coins_now = lighter_client.get_positions()
            if counter_open_coins_now is None:
                print(f"  [SKIP] {coin}: Lighter API失敗 → 今回は決済判断スキップ")
                tg(f"⚠️ {coin}: Lighter のポジション情報が取得できませんでした\n次のスキャンで再確認します")
                continue
            counter_coins = {p["symbol"] for p in counter_open_coins_now}
        else:
            # MEXC ポジションの決済（EXCHANGE_MODE が LIGHTER でも旧 MEXC ポジションは処理可能）
            if mexc is None:
                mexc = get_mexc()
                mexc.load_markets()
            counter_coins = get_mexc_open_coins(mexc)
            if counter_coins is None:
                print(f"  [SKIP] {coin}: MEXC API失敗 → 今回は決済判断スキップ")
                tg(f"⚠️ {coin} MEXC実ポジション取得失敗\n次のスキャンで再試行します")
                continue
        counter_has_pos = coin in counter_coins

        # 両方ともポジションなし → state.jsonのゴースト → 掃除して終了
        if not hl_has_pos and not counter_has_pos:
            print(f"  {coin}: HL/{counter_name}両方ポジションなし → stateから削除")
            del positions[coin]
            save_state(state)
            tg(f"🧹 {coin}: HL・{counter_name} 両方にポジションが見つかりません\n管理データから削除しました")
            continue

        # 片側のみポジションなし → 手動決済された可能性を通知
        if hl_has_pos and not counter_has_pos:
            tg(f"⚠️ {coin}: {counter_name} 側のポジションが見つかりません（手動決済された？）\nHL 側だけ決済します")
        elif not hl_has_pos and counter_has_pos:
            tg(f"⚠️ {coin}: HL 側のポジションが見つかりません（手動決済された？）\n{counter_name} 側だけ決済します")

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
                    open_coins = get_hl_open_coins(info, main_addr)
                    if open_coins is not None and coin not in open_coins:
                        print(f"  HL {coin} ポジション消滅確認 → 決済済みとみなす")
                        hl_ok = True
                        break
                    if open_coins is None:
                        print("  HL実ポジション再確認失敗 → この試行では判定保留")
                    if attempt < 3:
                        time.sleep(2)
            # 3回失敗 → force_closeを最終手段として試行
            if not hl_ok:
                try:
                    hl_force_close(exchange, info, coin, main_addr, sz_decimals_map)
                    print(f"  HL force close成功")
                    tg(f"⚠️ {coin}: 通常決済が3回失敗したため強制決済しました（HL）")
                    hl_ok = True
                except Exception as fe:
                    tg(f"🚨 {coin}: HL の決済が完全に失敗しました\nHL を直接確認してください")
                    send_gmail(
                        subject=f"[MindRaid] ⚠️ EXIT HL FAILED: {coin}",
                        body=f"HL決済が完全に失敗しました。手動で確認・決済してください。\n\n銘柄: {coin}\n時刻: {ts} UTC\nエラー: {fe}"
                    )
                    print(f"  HL force close失敗: {fe}")

        # Counter-Exchange 決済（Lighter or MEXC）
        counter_ok = False
        # ポジションを開いた取引所に応じたクライアントを準備
        if pos_exchange == "lighter":
            ct_client = None
        else:
            if mexc is None:
                mexc = get_mexc()
                mexc.load_markets()
            ct_client = mexc

        if not counter_has_pos:
            print(f"  {counter_name} {coin}: ポジションなし → スキップ")
            counter_ok = True
        else:
            for attempt in range(1, 4):
                try:
                    counter_close(ct_client, coin, direction, pos)
                    counter_ok = True
                    break
                except Exception as e:
                    err_str = str(e)
                    print(f"  {counter_name} close attempt {attempt}/3: {err_str}")
                    if "nonexistent or closed" in err_str or "Position is nonexistent" in err_str:
                        print(f"  {counter_name} {coin} ポジションなし確認 → 決済済みとみなす")
                        counter_ok = True
                        break
                    if attempt < 3:
                        time.sleep(2)
            # 3回失敗 → force_closeを最終手段として試行
            if not counter_ok:
                try:
                    counter_force_close(ct_client, coin, direction, pos)
                    print(f"  {counter_name} force close成功")
                    tg(f"⚠️ {coin}: 通常決済が3回失敗したため強制決済しました（{counter_name}）")
                    counter_ok = True
                except Exception as mfe:
                    tg(f"🚨 {coin}: {counter_name} の決済が完全に失敗しました\n{counter_name} を直接確認してください")
                    send_gmail(
                        subject=f"[MindRaid] ⚠️ EXIT {counter_name} FAILED: {coin}",
                        body=f"{counter_name}決済が完全に失敗しました。手動で確認・決済してください。\n\n銘柄: {coin}\n時刻: {ts} UTC\nエラー: {mfe}"
                    )
                    print(f"  {counter_name} force close失敗: {mfe}")

        if hl_ok and counter_ok:
            est_fr   = est_fr_now
            est_cost = cost
            net      = est_fr - est_cost

            # 実 funding 取得（HL は API、Lighter は CSV から推算）
            actual_hl_fund = fetch_hl_actual_funding(
                info, main_addr, coin, pos.get("opened_at", ""), ts
            )
            actual_lt_fund = fetch_lighter_actual_funding(
                coin, pos.get("opened_at", ""), ts,
                pos.get("size_usd", TRADE_SIZE_USD), pos.get("direction", "short_fr")
            ) if pos.get("exchange") == "lighter" else None

            log_trade_record(
                pos, coin, ts, dur_h,
                exit_hl_fr=hl_fr_now, exit_counter_fr=counter_fr_now, exit_net_fr=current_net_fr,
                est_funding=est_fr, est_cost=est_cost, est_net=net,
                exit_reason="normal",
                actual_hl_funding=actual_hl_fund,
                actual_lighter_funding=actual_lt_fund,
            )

            del positions[coin]
            save_state(state)

            # 3連敗チェック（同じ末尾 trade_id では二重通知しない）
            check_losing_streak(state, tg, n=3)

            tg(
                f"🔴 {coin} 決済完了\n"
                f"理由: net FR がマイナスに転落（{current_net_fr*100:+.4f}%/h）\n"
                f"保有時間: {dur_h:.1f}h\n"
                f"HL {hl_fr_now*100:+.4f}%/h / {counter_name} {counter_fr_now*100:+.4f}%/h\n"
                f"FR収益: ${est_fr:.2f}　手数料: ${est_cost:.2f}　手取り: ${net:.2f}\n"
                f"\n🔍 HL と {counter_name} 両取引所でポジションが実際にクローズされているか直接確認してください。"
            )
            post_x(
                f"🔴 FR Arb 決済 #{coin}\n"
                f"保有時間: {dur_h:.1f}h\n"
                f"現在net FR: {current_net_fr:+.4%}/h\n"
                f"推定収益: ${est_fr:.2f} / net: ${net:.2f}\n"
                f"{side_label}\n"
                f"#FRArb #仮想通貨"
            )
            send_gmail(
                subject=f"[MindRaid] EXIT: {coin}  net ${net:.2f}",
                body=(
                    f"FR Arb 決済\n\n"
                    f"銘柄: {coin}\n"
                    f"方向: {side_label}\n"
                    f"保有時間: {dur_h:.1f}h\n"
                    f"現在net FR: {current_net_fr:+.4%}/h\n"
                    f"現在HL FR: {hl_fr_now:+.4%}/h\n"
                    f"現在{counter_name} FR: {counter_fr_now:+.4%}/h\n"
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
                f"現在net FR: {current_net_fr:+.4%}/h\n"
                f"HL: {'✅' if hl_ok else '❌'}  {counter_name}: {'✅' if counter_ok else '❌'}\n"
                f"次のスキャンで再試行します\n"
                f"\n🚨 至急: HL と {counter_name} 両取引所の板を直接確認し、片側裸ポジションの有無をチェックしてください。必要なら手動でクローズを。"
            )

    # ── HOLD サマリー通知は廃止（ENTRY/EXIT 通知のみで十分） ──────
    # if hold_lines:
    #     tg("📊 保有ポジション状況\n" + "\n".join(hold_lines))

    # ── エントリーチェック ──────────────────────────────────────
    # まず候補を抽出して netFR 降順でソート（MAX_POSITIONS 到達時に最優位を取りこぼさない）
    entry_candidates = []  # [(avg_net_fr_1h, coin, rows, direction, selected_net)]
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
        if len(rows) < 2:
            continue

        # 直近2回の「ネットFR/h」で方向を決定
        avg_short_net_1h = sum(r["net_short_1h"] for r in rows) / len(rows)
        direction  = "short_fr" if avg_short_net_1h >= 0 else "long_fr"
        selected_net = [
            float(r["net_short_1h"] if direction == "short_fr" else r["net_long_1h"])
            for r in rows
        ]
        if not all(v >= MIN_FR_1H for v in selected_net):
            continue
        avg_net_fr_1h = sum(selected_net) / len(selected_net)
        if avg_net_fr_1h < MIN_FR_1H:
            continue
        # スパイク除去: 片側 FR が |0.5%/h| 超の銘柄は平均回帰しやすく負けやすい
        avg_hl_fr_entry = sum(float(r["hl_fr_1h"]) for r in rows) / len(rows)
        avg_ctr_fr_entry = sum(float(r.get("counter_fr_1h", r.get("mexc_fr_1h", 0))) for r in rows) / len(rows)
        if abs(avg_hl_fr_entry) > MAX_LEG_FR or abs(avg_ctr_fr_entry) > MAX_LEG_FR:
            print(f"[SKIP] {coin} 片側FRスパイク (HL={avg_hl_fr_entry*100:+.3f}%/h, ctr={avg_ctr_fr_entry*100:+.3f}%/h, 上限={MAX_LEG_FR*100:.2f}%/h)")
            continue
        entry_candidates.append((avg_net_fr_1h, coin, rows, direction, selected_net))

    entry_candidates.sort(key=lambda x: x[0], reverse=True)
    if entry_candidates:
        print(f"[CANDIDATES] netFR降順: " + ", ".join(
            f"{c[1]}({c[0]*100:.4f}%/h)" for c in entry_candidates[:5]
        ))

    for avg_net_fr_1h, coin, rows, direction, selected_net in entry_candidates:
        if len(positions) >= MAX_POSITIONS:
            print(f"MAX_POSITIONS ({MAX_POSITIONS}) 到達 → スキップ（残候補: {[c[1] for c in entry_candidates if c[0] <= avg_net_fr_1h and c[1] != coin]}）")
            break

        avg_hl_fr_1h      = sum(float(r["hl_fr_1h"]) for r in rows) / len(rows)
        avg_counter_fr_1h = sum(float(r.get("counter_fr_1h", r.get("mexc_fr_1h", 0))) for r in rows) / len(rows)
        counter_name_enter = counter_label()
        side_label = (f"HL SHORT × {counter_name_enter} LONG" if direction == "short_fr"
                      else f"HL LONG × {counter_name_enter} SHORT")

        print(
            f"[ENTRY] {coin}  netFR={avg_net_fr_1h:.4%}/h "
            f"(HL={avg_hl_fr_1h:+.4%}/h, {counter_name_enter}={avg_counter_fr_1h:+.4%}/h)  "
            f"{side_label}  size=${TRADE_SIZE_USD}"
        )

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
            tg(
                f"⚠️ ENTRY HL ERROR: {coin}\n{e}\n"
                f"\n🔍 HL の板でポジションが誤って残っていないか直接確認してください。"
            )
            continue

        time.sleep(1)

        # Counter-Exchange 発注（Lighter または MEXC）
        try:
            if direction == "short_fr":
                ct_res = counter_open_long(counter_client, coin, TRADE_SIZE_USD)
            else:
                ct_res = counter_open_short(counter_client, coin, TRADE_SIZE_USD)
        except Exception as e:
            print(f"  {counter_name_enter} open error → HL rollback: {e}")
            tg(
                f"⚠️ ENTRY {counter_name_enter} ERROR: {coin}\n{e}\nHL rollback中...\n"
                f"\n🔍 {counter_name_enter} の板でポジションが誤って約定していないか直接確認してください。"
            )
            hl_rb_ok = False
            try:
                if direction == "short_fr":
                    hl_close_short(exchange, coin)
                else:
                    hl_close_long(exchange, coin)
                hl_rb_ok = True
            except Exception as re:
                print(f"  HL rollback例外: {re}")
                try:
                    hl_force_close(exchange, info, coin, main_addr, sz_decimals_map)
                    hl_rb_ok = True
                except Exception as fe:
                    print(f"  HL force rollback失敗: {fe}")
            # API応答に関わらず実ポジションで確認
            import time as _time; _time.sleep(2)
            hl_verify = get_hl_open_coins(info, main_addr)
            if hl_verify is not None and coin in hl_verify:
                hl_rb_ok = False
                print(f"  [検証失敗] HL {coin} ポジションまだ残存")
            if hl_rb_ok:
                tg(
                    f"  HL rollback完了（実ポジション消滅確認）\n"
                    f"🔍 念のため HL と {counter_name_enter} 両取引所の板でポジションが残っていないか直接確認してください。"
                )
            else:
                tg(
                    f"  HL rollback失敗: {coin} HL裸ポジション残存\n"
                    f"\n🚨 至急: HL の板を直接確認し、手動で裸ポジションをクローズしてください。"
                )
                positions[coin] = {
                    "status": "danger", "opened_at": ts,
                    "reason": f"{counter_name_enter.lower()}_error_hl_rollback_failed"
                }
                save_state(state)
                send_gmail(
                    subject=f"[MindRaid] 🚨 HL裸ポジション: {coin}",
                    body=f"{counter_name_enter}発注失敗後のHLロールバックが失敗。手動でHL確認・決済してください。\n\n銘柄: {coin}\n時刻: {ts} UTC"
                )
                tg(f"🚨 危険ポジション記録: {coin}\nHL裸ポジションあり。手動で確認・決済してください")
            continue

        # ── スプレッドチェック（不利側超過でロールバック）──
        hl_px = float(hl_res["entry_price"])
        ct_px = float(ct_res["entry_price"])
        # short_fr: HL売→Counter買。Counter買値 > HL売値 が不利（即時損失）
        # long_fr : HL買→Counter売。Counter売値 < HL買値 が不利
        if direction == "short_fr":
            unfavorable_ratio = (ct_px - hl_px) / hl_px
        else:
            unfavorable_ratio = (hl_px - ct_px) / hl_px
        spread_pct = unfavorable_ratio * 100
        print(f"  スプレッド: HL={hl_px} {counter_name_enter}={ct_px} 不利側={spread_pct:+.4f}%")

        # abs() でどちら向きの異常スプレッドも弾く（価格スケールバグ等のデータエラー防御）
        if abs(unfavorable_ratio) > MAX_ENTRY_SPREAD:
            print(f"  [ROLLBACK] スプレッド過大 ({spread_pct:.4f}% > {MAX_ENTRY_SPREAD*100:.2f}%) → 両脚クローズ")
            tg(
                f"⚠️ ENTRY 見送り: {coin}\n"
                f"スプレッド {spread_pct:+.4f}% > 許容 {MAX_ENTRY_SPREAD*100:.2f}%\n"
                f"HL {'売' if direction=='short_fr' else '買'}: {hl_px}\n"
                f"{counter_name_enter} {'買' if direction=='short_fr' else '売'}: {ct_px}\n"
                f"両脚ロールバック中..."
            )
            hl_rb_ok = False
            try:
                if direction == "short_fr":
                    hl_close_short(exchange, coin)
                else:
                    hl_close_long(exchange, coin)
                hl_rb_ok = True
            except Exception as e:
                print(f"  HL rollback失敗: {e}")
                try:
                    hl_force_close(exchange, info, coin, main_addr, sz_decimals_map)
                    hl_rb_ok = True
                except Exception as fe:
                    print(f"  HL force rollback失敗: {fe}")

            # Counter-exchange rollback: 今開いたポジションを対象とする仮 state を構築
            rollback_pos_state = {
                "exchange": ct_res["exchange"],
                "counter_size_coin": ct_res.get("size_coin"),
                "mexc_contracts": ct_res.get("contracts"),
            }
            ct_rb_ok = False
            try:
                counter_force_close(counter_client, coin, direction, rollback_pos_state)
                ct_rb_ok = True
            except Exception as e:
                print(f"  {counter_name_enter} rollback失敗: {e}")

            # API応答に関わらず実ポジションで最終確認
            import time as _time2; _time2.sleep(2)
            hl_verify2 = get_hl_open_coins(info, main_addr)
            if hl_verify2 is not None and coin in hl_verify2:
                hl_rb_ok = False
                print(f"  [スプレッド検証失敗] HL {coin} ポジションまだ残存")

            if hl_rb_ok and ct_rb_ok:
                tg(
                    f"✅ ロールバック完了: {coin}\nエントリー見送りました\n"
                    f"🔍 念のため HL と {counter_name_enter} 両取引所の板にポジションが残っていないか直接確認してください。"
                )
            else:
                tg(
                    f"🚨 ロールバック失敗: {coin}\n"
                    f"HL: {'✅' if hl_rb_ok else '❌'}  {counter_name_enter}: {'✅' if ct_rb_ok else '❌'}\n"
                    f"手動で確認・決済してください\n"
                    f"\n🚨 至急: 両取引所の板を直接確認し、残存ポジションを手動クローズしてください。"
                )
                send_gmail(
                    subject=f"[MindRaid] 🚨 ROLLBACK FAILED: {coin}",
                    body=f"スプレッド過大によるロールバックが失敗しました。手動確認してください。\n\n"
                         f"銘柄: {coin}\nHL rollback: {'OK' if hl_rb_ok else 'FAIL'}\n"
                         f"{counter_name_enter} rollback: {'OK' if ct_rb_ok else 'FAIL'}\n時刻: {ts} UTC"
                )
                positions[coin] = {
                    "status": "danger",
                    "opened_at": ts,
                    "reason": "rollback_failed_spread_check"
                }
                save_state(state)
            continue

        # ── state 記録（EXCHANGE_MODE に応じてフィールドを保存）──
        state_entry = {
            "exchange":        ct_res["exchange"],    # "lighter" or "mexc"
            "direction":       direction,
            "opened_at":       ts,
            "fr_at_entry":     avg_net_fr_1h,  # 互換用
            "entry_net_fr_1h": avg_net_fr_1h,
            "entry_hl_fr_1h":  avg_hl_fr_1h,
            "entry_counter_fr_1h": avg_counter_fr_1h,
            # 旧コード互換フィールド
            "entry_mexc_fr_1h": avg_counter_fr_1h,
            "size_usd":        TRADE_SIZE_USD,
            "hl_size_coin":    hl_res["size_coin"],
            "hl_entry_price":  hl_res["entry_price"],
            "counter_size_coin":  ct_res["size_coin"],
            "counter_entry_price": ct_res["entry_price"],
            "entry_spread":    unfavorable_ratio,
        }
        # MEXC 固有フィールド
        if ct_res["exchange"] == "mexc":
            state_entry["mexc_contracts"]     = ct_res["contracts"]
            state_entry["mexc_contract_size"] = ct_res["contract_size"]
            state_entry["mexc_entry_price"]   = ct_res["entry_price"]
        positions[coin] = state_entry
        save_state(state)

        # Telegram 通知（取引所に応じて size 表示を変更）
        if ct_res["exchange"] == "mexc":
            ct_size_str = f"({ct_res['contracts']} contracts)"
        else:
            ct_size_str = f"({ct_res['size_coin']} coins)"

        tg(
            f"🟢 {coin} エントリー完了\n"
            f"戦略: {side_label}\n"
            f"net FR: {avg_net_fr_1h:.4%}/h\n"
            f"HL {avg_hl_fr_1h:+.4%}/h / {counter_name_enter} {avg_counter_fr_1h:+.4%}/h\n"
            f"サイズ: ${TRADE_SIZE_USD}\n"
            f"HL約定価格: {hl_res['entry_price']:.6f}  {counter_name_enter}約定価格: {ct_res['entry_price']:.6f}\n"
            f"\n🔍 HL と {counter_name_enter} 両取引所でポジションが実際に建っているか直接確認してください。"
        )
        post_x(
            f"🟢 FR Arb エントリー #{coin}\n"
            f"方向: {side_label}\n"
            f"net FR: {avg_net_fr_1h:.4%}/h\n"
            f"#FRArb #仮想通貨"
        )
        send_gmail(
            subject=f"[MindRaid] ENTRY: {coin}  {side_label}",
            body=(
                f"FR Arb エントリー\n\n"
                f"銘柄: {coin}\n"
                f"方向: {side_label}\n"
                f"net FR: {avg_net_fr_1h:.4%}/h\n"
                f"HL FR: {avg_hl_fr_1h:+.4%}/h\n"
                f"{counter_name_enter} FR: {avg_counter_fr_1h:+.4%}/h\n"
                f"サイズ: ${TRADE_SIZE_USD}\n"
                f"HL @ {hl_res['entry_price']:.6f}\n"
                f"{counter_name_enter} @ {ct_res['entry_price']:.6f}\n"
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
