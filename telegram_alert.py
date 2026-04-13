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

STATE_FILE = os.path.join(os.path.dirname(__file__), "data", "taker_state.json")


def load_positions() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f).get("positions", {})
    return {}


def build_position_section(positions: dict, mids: dict, now_str: str) -> list:
    if not positions:
        return []

    lines = ["📊 <b>現在のポジション</b>"]
    now_dt = datetime.strptime(now_str, "%Y-%m-%d %H:%M")

    for coin, pos in positions.items():
        if pos.get("status") == "danger":
            lines.append(f"  🚨 {coin}  危険ポジション（HL裸）手動確認要")
            continue

        direction   = pos.get("direction", "short_fr")
        side        = "SHORT" if direction == "short_fr" else "LONG"
        entry_price = float(pos.get("hl_entry_price", 0))
        size_coin   = float(pos.get("hl_size_coin", 0))
        size_usd    = float(pos.get("size_usd", 0))
        fr_entry    = float(pos.get("fr_at_entry", 0))
        opened_at   = pos.get("opened_at", "")

        current_price = float(mids.get(coin, entry_price))

        # 価格損益
        if direction == "short_fr":
            price_pnl = (entry_price - current_price) * size_coin
        else:
            price_pnl = (current_price - entry_price) * size_coin

        # FR推定収益
        try:
            opened_dt = datetime.strptime(opened_at, "%Y-%m-%d %H:%M:%S")
            dur_h = (now_dt - opened_dt).total_seconds() / 3600
        except Exception:
            dur_h = 0
        est_fr  = fr_entry * dur_h * size_usd
        est_net = price_pnl + est_fr

        sign = "+" if est_net >= 0 else ""
        lines.append(
            f"  {'🟢' if direction == 'short_fr' else '🔴'} {coin} HL {side}"
            f"  ${size_usd:.0f}  {dur_h:.1f}h"
            f"\n    価格損益: <code>{'+' if price_pnl>=0 else ''}{price_pnl:.2f}$</code>"
            f"  FR収益: <code>+{est_fr:.2f}$</code>"
            f"  推定net: <code>{sign}{est_net:.2f}$</code>"
        )

    lines.append("")
    return lines

TAKER_RT  = 0.00035 * 2   # 0.070%
MAKER_RT  = 0.00010 * 2   # 0.020%
ENTRY_FR  = 0.0010         # エントリー閾値: 0.10%/h（taker_bot.pyと同値）


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


def fetch_data(info: Info):
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


def build_message(rows, now_str, positions=None, mids=None):
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

    entry_candidates = []

    top_long = sorted_long[:3]
    if top_long:
        lines.append("🟢 <b>HL SHORT機会 TOP3</b>（FR ネガティブ）")
        for r in top_long:
            if abs(r["rate"]) >= ENTRY_FR:
                tag = " ⚡エントリー圏"
                entry_candidates.append((r["coin"], r["rate"], "SHORT"))
            else:
                tag = ""
            lines.append(f"  {r['coin']}  <code>{pct(r['rate'])}</code>/h  (8h: {pct(r['rate']*8)}){tag}")
        lines.append("")

    top_short = sorted_short[:3]
    if top_short:
        lines.append("🔴 <b>HL LONG機会 TOP3</b>（FR ポジティブ）")
        for r in top_short:
            if r["rate"] >= ENTRY_FR:
                tag = " ⚡エントリー圏"
                entry_candidates.append((r["coin"], r["rate"], "LONG"))
            else:
                tag = ""
            lines.append(f"  {r['coin']}  <code>{pct(r['rate'])}</code>/h  (8h: {pct(r['rate']*8)}){tag}")
        lines.append("")

    if entry_candidates:
        lines.append("🎯 <b>次のスキャンで継続ならエントリー予定</b>")
        for coin, rate, side in entry_candidates:
            lines.append(f"  → {coin}  HL {side}  {pct(rate)}/h")
        lines.append("")

    pos_lines = build_position_section(positions or {}, mids or {}, now_str)
    lines.extend(pos_lines)

    lines.append("※投資助言ではありません")
    return "\n".join(lines)


def main():
    env = load_env()
    token   = env["TELEGRAM_BOT_TOKEN"]
    chat_id = env["TELEGRAM_CHAT_ID"]

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    print("Fetching funding data …")
    info = Info(skip_ws=True)
    rows = fetch_data(info)
    mids = info.all_mids()
    positions = load_positions()

    msg = build_message(rows, now_str, positions=positions, mids=mids)
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
