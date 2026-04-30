"""
daily_equity_report.py
毎日 23:00 JST (14:00 UTC) に Telegram で口座残高サマリーを送る。

内容:
  - HL Total Equity（perps + spot ETH）と前日比
  - HL USDC only（ETH除外）と前日比
  - Lighter Total Equity と前日比
  - 実トレード実力（ETH除外合計）と前日比
  - 通算N・勝敗（当日 / 前日）

スナップショットは data/equity_snapshots.json に日次で追記。
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

import urllib.parse
import urllib.request

from hyperliquid.info import Info

DATA_DIR = Path(__file__).parent / "data"
SNAP_PATH = DATA_DIR / "equity_snapshots.json"
TRADES_CSV = DATA_DIR / "trades.csv"

# 戦略切替リスタート基準日 (N=0 開始点)。この日時以降の close のみ集計。
# 2026-04-24: BE-hold + 閾値0.015% + レジームストップ + FilterA/B 投入、手動で全ポジ解消。
RESTART_BASELINE_UTC = datetime(2026, 4, 24, 0, 0, 0, tzinfo=timezone.utc)

TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "")
HL_ADDR = os.environ.get("HL_WALLET_ADDRESS", "")


def tg_send(msg: str):
    if not TG_TOKEN or not TG_CHAT:
        print("[WARN] TELEGRAM env が無い")
        return
    url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"}).encode()
    with urllib.request.urlopen(url, data=data, timeout=10) as r:
        print(json.loads(r.read()).get("ok"))


def fetch_hl_equity(addr: str) -> dict:
    """
    HL Unified Account の Total Equity を画面表示と合わせて取得。
      Total = USDC_spot_total + 非USDC_spot_USD + perps_unrealized_PnL
      USDC_only = USDC_spot_total + perps_unrealized_PnL  （ETH除外）
    """
    info = Info(skip_ws=True)
    mids = info.all_mids()

    # perps の unrealized PnL 合計
    us = info.user_state(addr)
    upnl = 0.0
    for pos in us.get("assetPositions", []):
        p = pos.get("position", {})
        upnl += float(p.get("unrealizedPnl") or 0)

    # spot balances
    usdc_total = 0.0
    non_usdc_value = 0.0
    try:
        spot = info.spot_user_state(addr)
        for bal in spot.get("balances", []):
            coin = bal.get("coin", "")
            total = float(bal.get("total", 0))
            if total <= 0:
                continue
            if coin == "USDC":
                usdc_total += total
                continue
            # UETH など U接頭辞を剝がして mid を引く
            lookup = coin[1:] if coin.startswith("U") and coin != "USDC" else coin
            px = float(mids.get(lookup) or 0)
            non_usdc_value += total * px
    except Exception as e:
        print(f"[WARN] spot state 失敗: {e}")

    return {
        "usdc_only": usdc_total + upnl,
        "spot_value": non_usdc_value,
        "total": usdc_total + non_usdc_value + upnl,
    }


def fetch_lighter_equity() -> float:
    """Lighter Total Equity = collateral + sum(unrealized_pnl)"""
    import lighter_client
    col = lighter_client.get_balance() or 0.0
    positions = lighter_client.get_positions() or []
    upnl = sum(p.get("unrealized_pnl", 0) for p in positions)
    return float(col) + float(upnl)


def load_snapshots() -> list:
    if SNAP_PATH.exists():
        with open(SNAP_PATH) as f:
            return json.load(f)
    return []


def save_snapshots(snaps: list):
    DATA_DIR.mkdir(exist_ok=True)
    with open(SNAP_PATH, "w") as f:
        json.dump(snaps, f, indent=2)


def count_trades_until(end_dt: datetime) -> tuple[int, int, int]:
    """trades.csv から end_dt までのクローズ済み (N, 勝, 敗) を返す"""
    if not TRADES_CSV.exists():
        return 0, 0, 0
    n = wins = losses = 0
    with open(TRADES_CSV) as f:
        for row in csv.DictReader(f):
            closed = row.get("closed_at_utc", "")
            if not closed:
                continue
            try:
                closed_dt = datetime.strptime(closed, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if closed_dt > end_dt:
                continue
            if closed_dt < RESTART_BASELINE_UTC:
                continue
            n += 1
            try:
                net = float(row.get("est_net_usd", 0))
            except Exception:
                net = 0
            if net >= 0:
                wins += 1
            else:
                losses += 1
    return n, wins, losses


def fmt_diff(v: float) -> str:
    return f"({v:+.2f})"


def main():
    now_utc = datetime.now(timezone.utc)
    now_jst = now_utc + timedelta(hours=9)
    date_str = now_jst.strftime("%Y-%m-%d %H:%M JST")

    hl = fetch_hl_equity(HL_ADDR)
    lt = fetch_lighter_equity()

    current = {
        "date_utc": now_utc.strftime("%Y-%m-%d"),
        "timestamp_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "hl_total": round(hl["total"], 2),
        "hl_usdc": round(hl["usdc_only"], 2),
        "hl_spot": round(hl["spot_value"], 2),
        "lighter_total": round(lt, 2),
    }

    # クローズ trades
    n_now, w_now, l_now = count_trades_until(now_utc)
    current["n"] = n_now
    current["wins"] = w_now
    current["losses"] = l_now

    # 前日スナップ（同じ日付があれば除外）
    snaps = load_snapshots()
    prev = None
    for s in reversed(snaps):
        if s.get("date_utc") != current["date_utc"]:
            prev = s
            break

    def d(k):
        return round(current[k] - prev[k], 2) if prev else 0.0

    # 実トレード実力 = HL USDC + Lighter Total
    current["real_trade"] = round(current["hl_usdc"] + current["lighter_total"], 2)
    prev_real = round(prev["hl_usdc"] + prev["lighter_total"], 2) if prev else 0

    # 前日の N
    if prev:
        prev_line = f"前日: N={prev['n']} ({prev['wins']}勝{prev['losses']}敗)"
    else:
        prev_line = "前日: データなし（初回）"

    diff_hl_total = d("hl_total") if prev else 0
    diff_hl_usdc = d("hl_usdc") if prev else 0
    diff_lt = d("lighter_total") if prev else 0
    diff_real = round(current["real_trade"] - prev_real, 2) if prev else 0

    msg = (
        f"📊 <b>MindRaid Daily Report</b> {date_str}\n"
        f"\n"
        f"💼 <b>Hyperliquid</b>\n"
        f"  Total Equity: ${current['hl_total']:.2f} {fmt_diff(diff_hl_total)}\n"
        f"  USDC only (ETH除外): ${current['hl_usdc']:.2f} {fmt_diff(diff_hl_usdc)}\n"
        f"\n"
        f"💼 <b>Lighter</b>\n"
        f"  Total Equity: ${current['lighter_total']:.2f} {fmt_diff(diff_lt)}\n"
        f"\n"
        f"📈 <b>実トレード実力（ETH除外）</b>\n"
        f"  合計: ${current['real_trade']:.2f} {fmt_diff(diff_real)}\n"
        f"\n"
        f"🎯 通算 N={n_now} ({w_now}勝{l_now}敗)\n"
        f"    {prev_line}"
    )

    print(msg)
    tg_send(msg)

    # スナップ追記（同じ日付があれば上書き）
    snaps = [s for s in snaps if s.get("date_utc") != current["date_utc"]]
    snaps.append(current)
    save_snapshots(snaps)
    print(f"[OK] snapshot 保存: {SNAP_PATH}")


if __name__ == "__main__":
    main()
