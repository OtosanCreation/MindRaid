"""
funding_logger.py
BTC / ETH / SOL の predictedFunding (HlPerp) を取得して CSV に追記する。
crontab から毎時呼び出す想定。

CSV: /Users/lotusfamily/MindRaid/data/funding_log.csv
列 : timestamp_utc, coin, funding_rate_1h, funding_rate_8h, funding_rate_24h,
     interval_hours, taker_ok, maker_ok
"""

import csv
import os
from datetime import datetime, timezone

from hyperliquid.info import Info

COINS       = ["BTC", "ETH", "SOL"]
VENUE       = "HlPerp"
TAKER_RT    = 0.00035 * 2   # 往復 taker 0.07%
MAKER_RT    = 0.00010 * 2   # 往復 maker 0.02%
DATA_DIR    = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH    = os.path.join(DATA_DIR, "funding_log.csv")
FIELDNAMES  = [
    "timestamp_utc", "coin",
    "funding_rate_1h", "funding_rate_8h", "funding_rate_24h",
    "interval_hours", "taker_ok", "maker_ok",
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


def main() -> None:
    ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    info = Info(skip_ws=True)
    rows = fetch_funding(info)
    append_csv(rows, ts)
    for r in rows:
        flag = "◀ TAKER" if r["taker_ok"] else ("◁ maker" if r["maker_ok"] else "     -")
        print(f"[{ts}] {r['coin']:<4}  1h={r['funding_rate_1h']:+.6f}  "
              f"8h={r['funding_rate_8h']:+.6f}  {flag}")
    print(f"→ Appended {len(rows)} rows to {CSV_PATH}")


if __name__ == "__main__":
    main()
