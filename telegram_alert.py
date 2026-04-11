"""
telegram_alert.py
Funding Rate が閾値を超えた銘柄を Telegram に即通知する。
GitHub Actions から毎時呼び出す想定。

閾値:
  TAKER超え: |1h rate| > 0.070%  → 必ず通知
  MAKER超え: |1h rate| > 0.020%  → 件数のみサマリー
"""

import os
import urllib.request
import urllib.parse
import json
from datetime import datetime, timezone
from hyperliquid.info import Info

TAKER_RT  = 0.00035 * 2   # 0.070%
MAKER_RT  = 0.00010 * 2   # 0.020%
ENTRY_FR  = 0.0012         # エントリー閾値: 0.12%/h（taker_bot.pyと同値）


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
    for key in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


def fetch_data():
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
    return rows


def send_message(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
    }).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def build_message(rows, now_str):
    sorted_long  = sorted([r for r in rows if r["rate"] < 0], key=lambda r: r["rate"])
    sorted_short = sorted([r for r in rows if r["rate"] > 0], key=lambda r: r["rate"], reverse=True)
    maker_total = sum(1 for r in rows if abs(r["rate"]) > MAKER_RT)

    def pct(v):
        sign = "+" if v >= 0 else ""
        return f"{sign}{v*100:.4f}%"

    def label(rate, threshold):
        return "TAKER超え" if abs(rate) > threshold else "参考"

    lines = [
        f"<b>⚡ MindRaid Alert</b>  {now_str} UTC",
        f"Hyperliquid {len(rows)}銘柄 | MAKER超え: {maker_total}銘柄 | エントリー閾値: {ENTRY_FR*100:.2f}%/h",
        "",
    ]

    top_long = sorted_long[:3]
    if top_long:
        lines.append("🟢 <b>HL SHORT機会 TOP3</b>（FR ネガティブ）")
        for r in top_long:
            tag = " ✅エントリー圏" if abs(r["rate"]) >= ENTRY_FR else ""
            lines.append(f"  {r['coin']}  <code>{pct(r['rate'])}</code>/h  (8h: {pct(r['rate']*8)}){tag}")
        lines.append("")

    top_short = sorted_short[:3]
    if top_short:
        lines.append("🔴 <b>HL LONG機会 TOP3</b>（FR ポジティブ）")
        for r in top_short:
            tag = " ✅エントリー圏" if r["rate"] >= ENTRY_FR else ""
            lines.append(f"  {r['coin']}  <code>{pct(r['rate'])}</code>/h  (8h: {pct(r['rate']*8)}){tag}")
        lines.append("")

    lines.append("※投資助言ではありません")
    return "\n".join(lines)


def main():
    env = load_env()
    token   = env["TELEGRAM_BOT_TOKEN"]
    chat_id = env["TELEGRAM_CHAT_ID"]

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    print("Fetching funding data …")
    rows = fetch_data()

    msg = build_message(rows, now_str)
    print("--- Message preview ---")
    print(msg)
    print("-----------------------")

    result = send_message(token, chat_id, msg)
    if result.get("ok"):
        print(f"✅ Telegram送信完了")
    else:
        print(f"❌ エラー: {result}")


if __name__ == "__main__":
    main()
