"""
fetch_cex.py
Binance の予測ファンディングレートを CCXT 経由で取得して CSV に追記する。
BTC / ETH / SOL を対象。精算間隔 8h。
GitHub Actions から毎時呼び出す想定。

CSV: data/cex_log.csv
列 : timestamp_utc, coin, exchange, funding_rate_8h, funding_rate_1h, funding_rate_24h
"""

import csv
import os
from datetime import datetime, timezone

import ccxt

COINS = {
    "BTC": "BTC/USDT:USDT",
    "ETH": "ETH/USDT:USDT",
    "SOL": "SOL/USDT:USDT",
}
INTERVAL_HOURS = 8
DATA_DIR  = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH  = os.path.join(DATA_DIR, "cex_log.csv")
FIELDNAMES = [
    "timestamp_utc", "coin", "exchange",
    "funding_rate_8h", "funding_rate_1h", "funding_rate_24h",
]


def fetch_binance_funding() -> list[dict]:
    exchange = ccxt.binance()
    symbols  = list(COINS.values())
    rates    = exchange.fetch_funding_rates(symbols)
    result   = []
    for coin, symbol in COINS.items():
        data    = rates.get(symbol, {})
        rate_8h = data.get("fundingRate")
        if rate_8h is None:
            continue
        rate_1h  = rate_8h / INTERVAL_HOURS
        rate_24h = rate_1h * 24
        result.append({
            "coin":             coin,
            "exchange":         "Binance",
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
    rows = fetch_binance_funding()
    append_csv(rows, ts)
    for r in rows:
        print(f"[{ts}] {r['coin']:<4}  1h={r['funding_rate_1h']:+.6f}  "
              f"8h={r['funding_rate_8h']:+.6f}")
    print(f"→ Appended {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
