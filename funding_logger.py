"""
funding_logger.py
BTC / ETH / SOL の predictedFunding (HlPerp) を取得して CSV に追記する。
crontab から毎時呼び出す想定。

CSV: /Users/lotusfamily/MindRaid/data/funding_log.csv
列 : timestamp_utc, coin, funding_rate_1h, funding_rate_8h, funding_rate_24h,
     interval_hours, taker_ok, maker_ok

MEXC CSV: /Users/lotusfamily/MindRaid/data/mexc_funding_log.csv
列 : timestamp_utc, coin, funding_rate_1h, next_settle_time
"""

import csv
import os
import urllib.request
import json
from datetime import datetime, timezone

from hyperliquid.info import Info

COINS       = ["BTC", "ETH", "SOL", "DOGE", "XRP"]
MEXC_COINS  = ["ETH", "BTC", "SOL"]   # MEXCで取得する銘柄
MEXC_SYMBOLS = {"ETH": "ETH_USDT", "BTC": "BTC_USDT", "SOL": "SOL_USDT"}
VENUE       = "HlPerp"
TAKER_RT    = 0.00035 * 2   # 往復 taker 0.07%
MAKER_RT    = 0.00010 * 2   # 往復 maker 0.02%
DATA_DIR    = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH    = os.path.join(DATA_DIR, "funding_log.csv")
MEXC_CSV_PATH = os.path.join(DATA_DIR, "mexc_funding_log.csv")
FIELDNAMES  = [
    "timestamp_utc", "coin",
    "funding_rate_1h", "funding_rate_8h", "funding_rate_24h",
    "interval_hours", "taker_ok", "maker_ok",
]
MEXC_FIELDNAMES = [
    "timestamp_utc", "coin",
    "funding_rate_1h", "next_settle_time",
]


def fetch_funding(info: Info) -> list[dict]:
    raw = info.post("/info", {"type": "predictedFundings"})
    result = []
    for item in raw:
        coin, venues = item[0], item[1]
        if coin not in COINS:
            continue
        for venue_name, data in venues:
            if venue_name != VENUE:
                continue
            rate     = float(data["fundingRate"])
            interval = int(data["fundingIntervalHours"])
            rate_1h  = rate / interval
            rate_8h  = rate_1h * 8
            rate_24h = rate_1h * 24
            result.append({
                "coin":             coin,
                "funding_rate_1h":  rate_1h,
                "funding_rate_8h":  rate_8h,
                "funding_rate_24h": rate_24h,
                "interval_hours":   interval,
                "taker_ok":         abs(rate_1h) > TAKER_RT,
                "maker_ok":         abs(rate_1h) > MAKER_RT,
            })
    return result


def append_csv(rows: list[dict], ts: str) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    write_header = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({"timestamp_utc": ts, **row})


def fetch_mexc_funding() -> list[dict]:
    result = []
    for coin in MEXC_COINS:
        symbol = MEXC_SYMBOLS[coin]
        url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{symbol}"
        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
            if not data.get("success"):
                continue
            d = data["data"]
            # MEXCのfundingRateは8h換算なので1hに変換
            rate_8h = float(d["fundingRate"])
            rate_1h = rate_8h / 8
            next_settle = d.get("nextSettleTime", "")
            result.append({
                "coin":             coin,
                "funding_rate_1h":  rate_1h,
                "next_settle_time": next_settle,
            })
        except Exception as e:
            print(f"[MEXC] {coin} 取得失敗: {e}")
    return result


def append_mexc_csv(rows: list[dict], ts: str) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    write_header = not os.path.exists(MEXC_CSV_PATH)
    with open(MEXC_CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MEXC_FIELDNAMES)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({"timestamp_utc": ts, **row})


def main() -> None:
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    info = Info(skip_ws=True)

    # HL
    rows = fetch_funding(info)
    append_csv(rows, ts)
    print("--- Hyperliquid ---")
    for r in rows:
        flag = "◀ TAKER" if r["taker_ok"] else ("◁ maker" if r["maker_ok"] else "     -")
        print(f"[{ts}] {r['coin']:<4}  1h={r['funding_rate_1h']:+.6f}  "
              f"8h={r['funding_rate_8h']:+.6f}  {flag}")
    print(f"→ Appended {len(rows)} rows to {CSV_PATH}")

    # MEXC
    print("--- MEXC ---")
    mexc_rows = fetch_mexc_funding()
    if mexc_rows:
        append_mexc_csv(mexc_rows, ts)
        for r in mexc_rows:
            # HL ETHと比較表示
            hl_eth = next((x for x in rows if x["coin"] == r["coin"]), None)
            diff = ""
            if hl_eth:
                spread = r["funding_rate_1h"] - hl_eth["funding_rate_1h"]
                diff = f"  spread={spread:+.6f}"
            print(f"[{ts}] {r['coin']:<4}  1h={r['funding_rate_1h']:+.6f}{diff}")
        print(f"→ Appended {len(mexc_rows)} rows to {MEXC_CSV_PATH}")
    else:
        print("→ MEXC データ取得なし")


if __name__ == "__main__":
    main()
