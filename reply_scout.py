"""
reply_scout.py
X をキーワード検索してリプ候補をGmailに送信する。
毎朝 8:00 JST に GitHub Actions から呼び出す想定。

必要なSecrets:
  X_API_KEY, X_API_SECRET（既存）
  GMAIL_ADDRESS, GMAIL_APP_PASSWORD（新規追加が必要）
"""

import base64
import json
import os
import smtplib
import urllib.request
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import tweepy

# ── 設定 ──────────────────────────────────────────────────────
KEYWORDS = ["Hyperliquid", "FundingRate", "仮想通貨 アービトラージ"]
EXCLUDE_WORDS = ["簡単に稼げる", "誰でも", "保証"]
TOP_PER_KEYWORD = 2
SENT_IDS_PATH = os.path.join(os.path.dirname(__file__), "data", "sent_ids.json")
RETENTION_DAYS = 7


# ── 環境変数 ──────────────────────────────────────────────────
def load_env() -> dict:
    env = {}
    path = os.path.expanduser("~/.env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip()
    for key in ["X_API_KEY", "X_API_SECRET", "GMAIL_ADDRESS", "GMAIL_APP_PASSWORD"]:
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


# ── Bearer Token 取得（既存の API_KEY/SECRET から動的に取得）──
def get_bearer_token(api_key: str, api_secret: str) -> str:
    credentials = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
    req = urllib.request.Request(
        "https://api.twitter.com/oauth2/token",
        data=b"grant_type=client_credentials",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())["access_token"]


# ── 送信済みID管理 ─────────────────────────────────────────────
def load_sent_ids() -> dict:
    if os.path.exists(SENT_IDS_PATH):
        with open(SENT_IDS_PATH) as f:
            return json.load(f)
    return {}


def save_sent_ids(sent_ids: dict) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    cleaned = {k: v for k, v in sent_ids.items() if v >= cutoff}
    os.makedirs(os.path.dirname(SENT_IDS_PATH), exist_ok=True)
    with open(SENT_IDS_PATH, "w") as f:
        json.dump(cleaned, f, indent=2, ensure_ascii=False)


# ── X 検索 ────────────────────────────────────────────────────
def search_tweets(client: tweepy.Client, keyword: str, sent_ids: dict) -> list[dict]:
    exclude_query = " ".join(f'-"{w}"' for w in EXCLUDE_WORDS)
    query = f"{keyword} {exclude_query} -is:retweet"

    try:
        resp = client.search_recent_tweets(
            query=query,
            max_results=10,  # 最小値は10（API制限）、実質5件相当を上位で取得
            sort_order="relevancy",
            tweet_fields=["public_metrics", "created_at", "author_id"],
            expansions=["author_id"],
            user_fields=["username", "name"],
            start_time=datetime.now(timezone.utc) - timedelta(hours=24),
        )
    except Exception as e:
        print(f"[{keyword}] 検索失敗: {e}")
        return []

    if not resp.data:
        return []

    users = {u.id: (u.username, u.name) for u in (resp.includes.get("users") or [])}

    results = []
    for tweet in resp.data:
        if str(tweet.id) in sent_ids:
            continue
        m = tweet.public_metrics or {}
        # impression_countは自分のツイート以外0になることがあるため複合スコアで補完
        impression = m.get("impression_count", 0)
        score = impression if impression > 0 else (m.get("like_count", 0) * 10 + m.get("retweet_count", 0) * 5)
        username, name = users.get(tweet.author_id, ("unknown", ""))
        results.append({
            "id": str(tweet.id),
            "text": tweet.text,
            "username": username,
            "name": name,
            "url": f"https://x.com/{username}/status/{tweet.id}",
            "impressions": impression,
            "likes": m.get("like_count", 0),
            "retweets": m.get("retweet_count", 0),
            "score": score,
            "keyword": keyword,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:TOP_PER_KEYWORD]


# ── メール送信 ────────────────────────────────────────────────
def build_body(candidates: list[dict], date_str: str) -> str:
    lines = [f"今日のリプ候補 {date_str}", "=" * 50, ""]
    for i, t in enumerate(candidates, 1):
        lines.append(f"【{i}】@{t['username']}（{t['name']}）")
        lines.append(f"キーワード: {t['keyword']}")
        lines.append(f"閲覧数: {t['impressions']:,}  いいね: {t['likes']:,}  RT: {t['retweets']:,}")
        lines.append(f"URL: {t['url']}")
        lines.append("")
        lines.append(t["text"])
        lines.append("-" * 50)
        lines.append("")
    return "\n".join(lines)


def send_gmail(subject: str, body: str, env: dict) -> None:
    sender = env["GMAIL_ADDRESS"]
    password = env["GMAIL_APP_PASSWORD"]

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = sender
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)


# ── メイン ────────────────────────────────────────────────────
def main() -> None:
    env = load_env()
    bearer_token = get_bearer_token(env["X_API_KEY"], env["X_API_SECRET"])
    client = tweepy.Client(bearer_token=bearer_token)

    sent_ids = load_sent_ids()
    candidates = []
    seen_ids = set()

    for keyword in KEYWORDS:
        tweets = search_tweets(client, keyword, sent_ids)
        for t in tweets:
            if t["id"] not in seen_ids:
                candidates.append(t)
                seen_ids.add(t["id"])
        print(f"[{keyword}] {len(tweets)}件取得")

    if not candidates:
        print("候補なし。終了。")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    for t in candidates:
        sent_ids[t["id"]] = now_iso
    save_sent_ids(sent_ids)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    subject = f"今日のリプ候補【{date_str}】"
    body = build_body(candidates, date_str)

    send_gmail(subject, body, env)
    print(f"✓ {len(candidates)}件をGmailに送信: {subject}")


if __name__ == "__main__":
    main()
