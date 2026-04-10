"""
funding_logger.py
HL全銘柄（229）と MEXC共通銘柄（約191）の predictedFunding を毎時取得してCSVに追記。

HL CSV:   data/funding_log.csv
  列: timestamp_utc, coin, funding_rate_1h, funding_rate_8h, funding_rate_24h,
      interval_hours, taker_ok, maker_ok

MEXC CSV: data/mexc_funding_log.csv
  列: timestamp_utc, coin, funding_rate_1h, next_settle_time
"""

import csv
import os
import urllib.request
import json
from datetime import datetime, timezone

from hyperliquid.info import Info

VENUE     = "HlPerp"
TAKER_RT  = 0.00045 * 2   # 往復 taker 0.09%（HL実績: 0.045%/side）
MAKER_RT  = 0.00015 * 2   # 往復 maker 0.03%（HL実績: 0.015%/side）
DATA_DIR  = os.path.join(os.path.dirname(__file__), "data")
CSV_PATH      = os.path.join(DATA_DIR, "funding_log.csv")
MEXC_CSV_PATH = os.path.join(DATA_DIR, "mexc_funding_log.csv")
FIELDNAMES = [
    "timestamp_utc", "coin",
    "funding_rate_1h", "funding_rate_8h", "funding_rate_24h",
    "interval_hours", "taker_ok", "maker_ok",
]
MEXC_FIELDNAMES = [
    "timestamp_utc", "coin",
    "funding_rate_1h", "next_settle_time",
]


def fetch_hl_funding(info: Info) -> list[dict]:
    raw = info.post("/info", {"type": "predictedFundings"})
    result = []
    for item in raw:
        coin, venues = item[0], item[1]
        for venue_name, data in venues:
            if venue_name != VENUE:
                continue
            rate     = float(data["fundingRate"])
            interval = int(data["fundingIntervalHours"])
            rate_1h  = rate / interval
            result.append({
                "coin":            coin,
                "funding_rate_1h": rate_1h,
                "funding_rate_8h": rate_1h * 8,
                "funding_rate_24h": rate_1h * 24,
                "interval_hours":  interval,
                "taker_ok":        abs(rate_1h) > TAKER_RT,
                "maker_ok":        abs(rate_1h) > MAKER_RT,
            })
    return result


def fetch_mexc_coins():
    """MEXCで取扱中の_USDT無期限銘柄セットを返す"""
    url = "https://contract.mexc.com/api/v1/contract/detail"
    with urllib.request.urlopen(url, timeout=15) as r:
        data = json.loads(r.read())
    return {d["symbol"].replace("_USDT", "") for d in data["data"] if d["symbol"].endswith("_USDT")}


def fetch_one_mexc(coin: str):
    url = f"https://contract.mexc.com/api/v1/contract/funding_rate/{coin}_USDT"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            data = json.loads(r.read())
        if not data.get("success"):
            return None
        d = data["data"]
        rate_8h = float(d["fundingRate"])
        return {
            "coin":            coin,
            "funding_rate_1h": rate_8h / 8,
            "next_settle_time": d.get("nextSettleTime", ""),
        }
    except Exception as e:
        print(f"[MEXC] {coin} 取得失敗: {e}")
        return None


def fetch_mexc_funding(target_coins):
    """HL×MEXC共通銘柄を順次取得"""
    results = []
    for coin in target_coins:
        r = fetch_one_mexc(coin)
        if r:
            results.append(r)
    return results


def append_csv(rows: list[dict], ts: str) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    write_header = not os.path.exists(CSV_PATH)
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({"timestamp_utc": ts, **row})


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

    # ── HL 全銘柄 ──────────────────────────────────────
    hl_rows = fetch_hl_funding(info)
    append_csv(hl_rows, ts)
    hl_coins = {r["coin"] for r in hl_rows}
    taker_hits = [r for r in hl_rows if r["taker_ok"]]
    maker_hits  = [r for r in hl_rows if r["maker_ok"]]
    print(f"--- HL ({len(hl_rows)}銘柄) ---")
    print(f"  taker超え: {len(taker_hits)}銘柄  maker超え: {len(maker_hits)}銘柄")
    print(f"  → {CSV_PATH} に追記")

    # ── MEXC HL共通銘柄 ────────────────────────────────
    print("--- MEXC ---")
    mexc_all = fetch_mexc_coins()
    common   = sorted(hl_coins & mexc_all)
    print(f"  HL×MEXC共通: {len(common)}銘柄 → 取得中...")
    mexc_rows = fetch_mexc_funding(common)
    if mexc_rows:
        append_mexc_csv(mexc_rows, ts)
        print(f"  取得成功: {len(mexc_rows)}銘柄")
        # スプレッド上位5件表示
        hl_map = {r["coin"]: r["funding_rate_1h"] for r in hl_rows}
        spreads = []
        for r in mexc_rows:
            hl_r = hl_map.get(r["coin"], 0)
            spreads.append((r["coin"], r["funding_rate_1h"], hl_r, r["funding_rate_1h"] - hl_r))
        spreads.sort(key=lambda x: abs(x[3]), reverse=True)
        print("  スプレッド上位5:")
        for coin, mx, hl, sp in spreads[:5]:
            print(f"    {coin:<10} MEXC={mx:+.5f}  HL={hl:+.5f}  spread={sp:+.5f}")
        print(f"  → {MEXC_CSV_PATH} に追記")
    else:
        print("  → データ取得なし")


if __name__ == "__main__":
    main()
