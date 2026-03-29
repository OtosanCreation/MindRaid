"""
post_pnl_x.py
FR Arb 日次収益サマリーを X (Twitter) に投稿する。
GitHub Actions から1日1回呼び出す想定。
"""

import csv
import os
import tweepy
from datetime import datetime, timezone

PNL_PATH = os.path.join(os.path.dirname(__file__), "data", "pnl_log.csv")


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
    for key in ["X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_TOKEN_SECRET"]:
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


def read_latest_pnl():
    if not os.path.exists(PNL_PATH):
        return None
    rows = []
    with open(PNL_PATH, newline="") as f:
        rows = list(csv.DictReader(f))
    return rows[-1] if rows else None


def calc_days_open(row: dict) -> int:
    try:
        opened = datetime.strptime(row["logged_at_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return max(1, int((datetime.now(timezone.utc) - opened).total_seconds() / 86400))
    except Exception:
        return 1


def build_tweet(row: dict, today_str: str) -> str:
    coin     = row.get("coin", "ETH")
    size     = row.get("size", "?")
    actual   = float(row.get("hl_funding_actual_usd", 0))
    theory   = float(row.get("total_funding_theoretical_usd", 0) or 0)
    accuracy = row.get("accuracy_pct", "")
    apy      = float(row.get("annualized_apy_pct", 0) or 0)
    net_pnl  = float(row.get("net_pnl_usd", 0))
    fees     = float(row.get("entry_fees_usd", 0))

    acc_str = f"{float(accuracy):.1f}%" if accuracy else "N/A"
    sign    = "+" if actual >= 0 else ""

    lines = [
        f"📊 FR Arb 日次実績 [{today_str} UTC]",
        f"{coin} {size} @ Hyperliquid",
        "",
        f"実FR収益(24h): {sign}${actual:.4f}",
        f"理論値比:      {acc_str}",
        f"APY:           {apy:.1f}%",
        f"Net PnL:       ${net_pnl:+.4f}（手数料${fees:.2f}込）",
        "",
        "※データ提供のみ。投資判断はご自身で。",
        "#FundingRate #アービトラージ #仮想通貨 #Hyperliquid",
    ]
    return "\n".join(lines)


def post_tweet(tweet_text: str):
    env = load_env()
    client = tweepy.Client(
        consumer_key=env["X_API_KEY"],
        consumer_secret=env["X_API_SECRET"],
        access_token=env["X_ACCESS_TOKEN"],
        access_token_secret=env["X_ACCESS_TOKEN_SECRET"],
    )
    response = client.create_tweet(text=tweet_text)
    return response.data["id"]


def main():
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    row = read_latest_pnl()
    if not row:
        print("❌ pnl_log.csv にデータがありません")
        return

    tweet_text = build_tweet(row, today_str)
    print("--- Tweet preview ---")
    print(tweet_text)
    print("---------------------")

    tweet_id = post_tweet(tweet_text)
    print(f"✅ 投稿完了: https://x.com/logiQ_Alpha/status/{tweet_id}")


if __name__ == "__main__":
    main()
