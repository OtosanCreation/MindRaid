"""
arb_dashboard.py
HL vs MEXC のリアルタイムアービトラージダッシュボード
"""

import os
import time
from datetime import datetime, timezone
import ccxt
from hyperliquid.info import Info

COINS = ["BTC", "ETH", "SOL"]
REFRESH = 10  # 秒

def load_env():
    env = {}
    path = os.path.expanduser("~/.env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    return env

def fetch_hl(info):
    raw = info.post("/info", {"type": "predictedFundings"})
    result = {}
    prices_raw = info.all_mids()
    for coin in COINS:
        result[coin] = {"hl_fr": None, "hl_price": None}
        if coin in prices_raw:
            result[coin]["hl_price"] = float(prices_raw[coin])
    for item in raw:
        coin, venues = item[0], item[1]
        if coin not in COINS:
            continue
        for venue_name, data in venues:
            if venue_name == "HlPerp":
                rate = float(data["fundingRate"])
                interval = int(data["fundingIntervalHours"])
                result[coin]["hl_fr"] = rate / interval
    return result

def fetch_mexc(exchange):
    result = {}
    for coin in COINS:
        symbol = f"{coin}/USDT:USDT"
        try:
            ticker = exchange.fetch_ticker(symbol)
            fr_info = exchange.fetch_funding_rate(symbol)
            result[coin] = {
                "mexc_price": ticker["last"],
                "mexc_fr": fr_info.get("fundingRate"),
            }
        except Exception as e:
            result[coin] = {"mexc_price": None, "mexc_fr": None}
    return result

def pct(v):
    if v is None:
        return "  N/A   "
    sign = "+" if v >= 0 else ""
    return f"{sign}{v*100:.4f}%"

def fmt_price(v):
    if v is None:
        return "      N/A"
    return f"{v:>12,.2f}"

def display(hl_data, mexc_data):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("\033[2J\033[H", end="")  # 画面クリア
    print("=" * 70)
    print(f"  🔥 アービトラージ ダッシュボード    更新: {now}")
    print("=" * 70)
    print(f"  {'銘柄':<6} {'HL価格':>12}  {'HL FR(1h)':>10}  {'MEXC価格':>12}  {'MEXC FR(1h)':>10}  {'鞘':>10}")
    print("-" * 70)

    spreads = []
    for coin in COINS:
        hl = hl_data.get(coin, {})
        mx = mexc_data.get(coin, {})
        hl_price = hl.get("hl_price")
        mx_price = mx.get("mexc_price")
        hl_fr = hl.get("hl_fr")
        mx_fr = mx.get("mexc_fr")

        spread = None
        direction = ""
        if hl_price and mx_price:
            spread = hl_price - mx_price
            direction = "HL売り→MEXC買い" if spread > 0 else "MEXC買い→HL売り"
            spreads.append((coin, abs(spread), direction))

        spread_str = f"{spread:+.2f}" if spread is not None else "N/A"
        print(f"  {coin:<6} {fmt_price(hl_price)}  {pct(hl_fr):>10}  {fmt_price(mx_price)}  {pct(mx_fr):>10}  {spread_str:>10}")

    print("=" * 70)
    if spreads:
        spreads.sort(key=lambda x: x[1], reverse=True)
        print(f"  📊 鞘ランキング（大きい順）")
        for i, (coin, sp, direction) in enumerate(spreads, 1):
            print(f"  {i}位 {coin}: {sp:+.2f}pts  方向: {direction}")
    print("=" * 70)
    print(f"  次の更新まで {REFRESH}秒 ... (Ctrl+C で終了)")

def main():
    env = load_env()
    api_key = env.get("MEXC_API_KEY", "")
    secret = env.get("MEXC_SECRET", "")

    exchange = ccxt.mexc({
        "apiKey": api_key,
        "secret": secret,
    })
    info = Info(skip_ws=True)

    while True:
        try:
            hl_data = fetch_hl(info)
            mexc_data = fetch_mexc(exchange)
            display(hl_data, mexc_data)
        except Exception as e:
            print(f"エラー: {e}")
        time.sleep(REFRESH)

if __name__ == "__main__":
    main()
