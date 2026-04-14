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
import ccxt


def fetch_hl_positions(address: str) -> list:
    """HL APIから実際のオープンポジションを取得"""
    if not address:
        return []
    try:
        info = Info(skip_ws=True)
        user_state = info.user_state(address)
        result = []
        for pos in user_state.get("assetPositions", []):
            p   = pos.get("position", {})
            szi = float(p.get("szi", 0))
            if szi == 0:
                continue
            result.append({
                "coin":          p.get("coin"),
                "side":          "SHORT" if szi < 0 else "LONG",
                "size":          abs(szi),
                "entry_px":      float(p.get("entryPx") or 0),
                "mark_px":       float(p.get("positionValue", 0)) / abs(szi) if szi != 0 else 0,
                "unrealized_pnl": float(p.get("unrealizedPnl") or 0),
                "funding":       float(p.get("cumFunding", {}).get("sinceOpen", 0)),
            })
        return result
    except Exception as e:
        print(f"[WARN] HL実ポジション取得失敗: {e}")
        return []


def fetch_mexc_positions(api_key: str, api_secret: str) -> list:
    """MEXC APIから実際のオープンポジションを取得"""
    if not api_key or not api_secret:
        return []
    try:
        mexc = ccxt.mexc({
            "apiKey": api_key,
            "secret": api_secret,
            "options": {"defaultType": "swap"},
        })
        positions = mexc.fetch_positions()
        result = []
        for pos in positions:
            contracts = float(pos.get("contracts") or 0)
            if contracts == 0:
                continue
            entry_px  = float(pos.get("entryPrice") or 0)
            mark_px   = float(pos.get("markPrice") or 0)
            cont_size = float(pos.get("contractSize") or 1)
            side      = pos.get("side", "").upper()
            pnl       = float(pos.get("unrealizedPnl") or 0)
            # ccxtがPNLを返さない場合、手動計算
            if pnl == 0 and entry_px > 0 and mark_px > 0:
                if side == "LONG":
                    pnl = (mark_px - entry_px) * contracts * cont_size
                elif side == "SHORT":
                    pnl = (entry_px - mark_px) * contracts * cont_size
            result.append({
                "coin":          pos["symbol"].split("/")[0],
                "side":          side,
                "contracts":     contracts,
                "entry_price":   entry_px,
                "unrealized_pnl": pnl,
            })
        return result
    except Exception as e:
        print(f"[WARN] MEXC実ポジション取得失敗: {e}")
        return []


def build_position_section(hl_positions: list, mexc_positions: list) -> list:
    lines = ["📊 <b>現在のポジション</b>"]

    if not hl_positions and not mexc_positions:
        lines.append("  HL / MEXC: ポジションなし")
        lines.append("")
        return lines

    # HLポジション
    if hl_positions:
        for p in hl_positions:
            pnl  = p["unrealized_pnl"]
            fund = p["funding"]
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"  {'🟢' if p['side']=='SHORT' else '🔴'} HL {p['coin']} {p['side']}"
                f"  {p['size']:.4f}枚 @ ${p['entry_px']:.4f}"
                f"\n    未実現PNL: <code>{sign}{pnl:.2f}$</code>"
                f"  累計FR: <code>{'+' if fund>=0 else ''}{fund:.2f}$</code>"
            )
    else:
        lines.append("  HL: ポジションなし")

    # MEXCポジション
    if mexc_positions:
        for p in mexc_positions:
            pnl  = p["unrealized_pnl"]
            sign = "+" if pnl >= 0 else ""
            lines.append(
                f"  {'🔴' if p['side']=='LONG' else '🟢'} MEXC {p['coin']} {p['side']}"
                f"  {p['contracts']:.0f}枚 @ ${p['entry_price']:.4f}"
                f"\n    未実現PNL: <code>{sign}{pnl:.2f}$</code>"
            )
    else:
        lines.append("  MEXC: ポジションなし")

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


def build_message(rows, now_str, hl_positions=None, mexc_positions=None):
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

    # 既存ポジションの銘柄セット（エントリー予定から除外するため）
    open_coins = {p["coin"] for p in (hl_positions or [])}

    entry_candidates = []

    top_long = sorted_long[:3]
    if top_long:
        lines.append("🟢 <b>HL SHORT機会 TOP3</b>（FR ネガティブ）")
        for r in top_long:
            if abs(r["rate"]) >= ENTRY_FR:
                if r["coin"] in open_coins:
                    tag = " ⚡保有中"
                else:
                    tag = " ⚡エントリー圏"
                    entry_candidates.append((r["coin"], r["rate"], "SHORT"))
            else:
                tag = ""
            lines.append(f"  {r['coin']}  <code>{pct(r['rate'])}</code>/h{tag}")
        lines.append("")

    top_short = sorted_short[:3]
    if top_short:
        lines.append("🔴 <b>HL LONG機会 TOP3</b>（FR ポジティブ）")
        for r in top_short:
            if r["rate"] >= ENTRY_FR:
                if r["coin"] in open_coins:
                    tag = " ⚡保有中"
                else:
                    tag = " ⚡エントリー圏"
                    entry_candidates.append((r["coin"], r["rate"], "LONG"))
            else:
                tag = ""
            lines.append(f"  {r['coin']}  <code>{pct(r['rate'])}</code>/h{tag}")
        lines.append("")

    if entry_candidates:
        lines.append("🎯 <b>次のスキャンで継続ならエントリー予定</b>")
        for coin, rate, side in entry_candidates:
            lines.append(f"  → {coin}  HL {side}  {pct(rate)}/h")
        lines.append("")

    pos_lines = build_position_section(hl_positions or [], mexc_positions or [])
    lines.extend(pos_lines)

    return "\n".join(lines)


def main():
    env = load_env()
    token   = env["TELEGRAM_BOT_TOKEN"]
    chat_id = env["TELEGRAM_CHAT_ID"]

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    print("Fetching funding data …")
    info = Info(skip_ws=True)
    rows = fetch_data(info)

    hl_address    = os.environ.get("HL_WALLET_ADDRESS", "")
    mexc_api_key  = os.environ.get("MEXC_API_KEY", "")
    mexc_api_sec  = os.environ.get("MEXC_API_SECRET", "")
    hl_positions   = fetch_hl_positions(hl_address)
    mexc_positions = fetch_mexc_positions(mexc_api_key, mexc_api_sec)

    msg = build_message(rows, now_str, hl_positions=hl_positions, mexc_positions=mexc_positions)
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
