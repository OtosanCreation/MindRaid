"""
post_tweet.py
ダッシュボード画像 + Funding Rate サマリーを X (Twitter) に投稿する。
GitHub Actions から毎時呼び出す想定。
"""

import os
import tweepy
from datetime import datetime, timezone
from hyperliquid.info import Info

TAKER_RT = 0.00035 * 2
MAKER_RT = 0.00010 * 2
IMG_PATH = os.path.join(os.path.dirname(__file__), "images", "dashboard.png")


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
    # GitHub Actions の環境変数もフォールバックで読む
    for key in ["X_API_KEY", "X_API_SECRET", "X_ACCESS_TOKEN", "X_ACCESS_TOKEN_SECRET"]:
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


def fetch_top(n=3):
    info = Info(skip_ws=True)
    raw = info.post("/info", {"type": "predictedFundings"})
    rows = []
    for item in raw:
        coin = item[0]
        for venue_name, data in item[1]:
            if venue_name == "HlPerp":
                rate = float(data["fundingRate"]) / int(data["fundingIntervalHours"])
                rows.append({"coin": coin, "rate": rate})
                break
    total = len(rows)
    taker_n = sum(1 for r in rows if abs(r["rate"]) > TAKER_RT)
    long_top  = sorted(rows, key=lambda r: r["rate"])[:n]
    short_top = sorted(rows, key=lambda r: r["rate"], reverse=True)[:n]
    return total, taker_n, long_top, short_top


def build_tweet(total, taker_n, long_top, short_top, now_str):
    def pct(v):
        sign = "+" if v >= 0 else ""
        return f"{sign}{v*100:.4f}%"

    lines = [
        f"⚡ Hyperliquid Funding Rate [{now_str} UTC]",
        "",
        "🟢 LONG受取 TOP3（FR最小）",
    ]
    for i, r in enumerate(long_top, 1):
        tag = " ◀TAKER" if abs(r["rate"]) > TAKER_RT else ""
        lines.append(f"{i}. {r['coin']}  {pct(r['rate'])}{tag}")

    lines += ["", "🔴 SHORT受取 TOP3（FR最大）"]
    for i, r in enumerate(short_top, 1):
        tag = " ◀TAKER" if abs(r["rate"]) > TAKER_RT else ""
        lines.append(f"{i}. {r['coin']}  {pct(r['rate'])}{tag}")

    lines += [
        "",
        f"TAKER超え: {taker_n}銘柄 / {total}銘柄",
        "",
        "#Hyperliquid #FundingRate #仮想通貨 #アービトラージ",
    ]
    return "\n".join(lines)


def post(tweet_text, img_path):
    env = load_env()
    api_key    = env["X_API_KEY"]
    api_secret = env["X_API_SECRET"]
    acc_token  = env["X_ACCESS_TOKEN"]
    acc_secret = env["X_ACCESS_TOKEN_SECRET"]

    # v1.1 API（画像アップロード用）
    auth = tweepy.OAuth1UserHandler(api_key, api_secret, acc_token, acc_secret)
    api_v1 = tweepy.API(auth)

    # v2 API（ツイート投稿用）
    client = tweepy.Client(
        consumer_key=api_key,
        consumer_secret=api_secret,
        access_token=acc_token,
        access_token_secret=acc_secret,
    )

    # 画像アップロード
    media = api_v1.media_upload(filename=img_path)
    media_id = media.media_id

    # ツイート投稿
    response = client.create_tweet(text=tweet_text, media_ids=[media_id])
    return response.data["id"]


def main():
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    print("Fetching funding data …")
    total, taker_n, long_top, short_top = fetch_top()

    tweet_text = build_tweet(total, taker_n, long_top, short_top, now_str)
    print("--- Tweet preview ---")
    print(tweet_text)
    print("---------------------")

    if not os.path.exists(IMG_PATH):
        print(f"⚠ 画像が見つかりません: {IMG_PATH}")
        print("  先に generate_image.py を実行してください")
        return

    tweet_id = post(tweet_text, IMG_PATH)
    print(f"✅ ツイート投稿完了: https://x.com/logiQ_Alpha/status/{tweet_id}")


if __name__ == "__main__":
    main()
