"""
fetch_cex.py
Hyperliquid の predictedFundings から BybitPerp レートを取得して CSV に追記する。
BTC / ETH / SOL を対象。精算間隔 8h。
GitHub Actions から毎時呼び出す想定。

※ Bybit / Binance の API は GitHub Actions の US サーバーから地理的にブロックされるため
  Hyperliquid が集約している BybitPerp レートを使用する。

CSV: data/cex_log.csv
列 : timestamp_utc, coin, exchange, funding_rate_8h, funding_rate_1h, funding_rate_24h
"""

import csv
import os
from datetime import datetime, timezone

from hyperliquid.info import Info

COINS        = ["BTC", "ETH", "SOL", "DOGE", "XRP"]
TARGET_VENUE = "BybitPerp"
DATA_DIR     = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH     = os.path.join(DATA_DIR, "cex_log.csv")
FIELDNAMES   = [
    "timestamp_utc", "coin", "exchange",
    "funding_rate_8h", "funding_rate_1h", "funding_rate_24h",
]


def fetch_bybit_funding(info: Info) -> list[dict]:
    raw    = info.post("/info", {"type": "predictedFundings"})
    result = []
    for item in raw:
        coin, venues = item[0], item[1]
        if coin not in COINS:
            continue
        for venue_name, data in venues:
            if venue_name != TARGET_VENUE:
                continue
            rate_8h  = float(data["fundingRate"])
            interval = int(data["fundingIntervalHours"])   # BybitPerp = 8h
            rate_1h  = rate_8h / interval
            rate_24h = rate_1h * 24
            result.append({
                "coin":             coin,
                "exchange":         "BybitPerp",
                "funding_rate_8h":  rate_8h,
                "funding_rate_1h":  rate_1h,
                "funding_rate_24h": rate_24h,
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


def main() -> None:
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    info = Info(skip_ws=True)
    rows = fetch_bybit_funding(info)
    append_csv(rows, ts)
    for r in rows:
        print(f"[{ts}] {r['coin']:<4}  1h={r['funding_rate_1h']:+.6f}  "
              f"8h={r['funding_rate_8h']:+.6f}")
    print(f"→ Appended {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
