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


def read_pnl_summary():
    """
    直近24時間分のFundingを合計し、最新行のNet PnL・APYと合わせて返す。
    戻り値: dict or None
    """
    if not os.path.exists(PNL_PATH):
        return None
    rows = []
    with open(PNL_PATH, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return None

    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - 24 * 3600

    # 直近24h のFundingを合計
    funding_24h = 0.0
    theory_24h  = 0.0
    for r in rows:
        try:
            ts = datetime.strptime(r["logged_at_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            if ts >= cutoff:
                funding_24h += float(r.get("hl_funding_actual_usd", 0))
                theory_24h  += float(r.get("total_funding_theoretical_usd", 0) or 0)
        except Exception:
            pass

    latest = rows[-1]
    return {
        "coin":        latest.get("coin", "ETH"),
        "size":        latest.get("size", "?"),
        "funding_24h": funding_24h,
        "theory_24h":  theory_24h,
        "apy":         float(latest.get("annualized_apy_pct", 0) or 0),
        "net_pnl":     float(latest.get("net_pnl_usd", 0)),
        "fees":        float(latest.get("entry_fees_usd", 0)),
    }


def build_tweet(summary: dict, today_str: str) -> str:
    coin        = summary["coin"]
    size        = summary["size"]
    actual      = summary["funding_24h"]
    theory      = summary["theory_24h"]
    apy         = summary["apy"]
    net_pnl     = summary["net_pnl"]
    fees        = summary["fees"]

    accuracy    = (actual / theory * 100) if theory != 0 else None

    acc_str = f"{accuracy:.1f}%" if accuracy is not None else "N/A"
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

    summary = read_pnl_summary()
    if not summary:
        print("❌ pnl_log.csv にデータがありません")
        return

    tweet_text = build_tweet(summary, today_str)
    print("--- Tweet preview ---")
    print(tweet_text)
    print("---------------------")

    tweet_id = post_tweet(tweet_text)
    print(f"✅ 投稿完了: https://x.com/logiQ_Alpha/status/{tweet_id}")


if __name__ == "__main__":
    main()
