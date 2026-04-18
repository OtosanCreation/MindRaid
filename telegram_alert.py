"""
telegram_alert.py
Funding Rate が閾値を超えた銘柄を Telegram に即通知する。
GitHub Actions から毎時呼び出す想定。

閾値:
  TAKER超え: |1h rate| > 0.070%  → 必ず通知
  MAKER超え: |1h rate| > 0.020%  → 件数のみサマリー
"""

import os
import csv
import urllib.request
import urllib.parse
import json
from collections import defaultdict
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
        mexc.load_markets()
        positions = mexc.fetch_positions()
        result = []
        for pos in positions:
            contracts = float(pos.get("contracts") or 0)
            if contracts == 0:
                continue
            symbol    = pos["symbol"]
            coin      = symbol.split("/")[0]
            entry_px  = float(pos.get("entryPrice") or 0)
            side      = pos.get("side", "").upper()
            # contract_size を market info から取得（posの値は信頼しない）
            try:
                market    = mexc.market(symbol)
                cont_size = float(market.get("contractSize") or 1)
            except Exception:
                cont_size = float(pos.get("contractSize") or 1)
            # mark_pxを取得（posの値がNullならtickerから）
            mark_px = float(pos.get("markPrice") or 0)
            if mark_px == 0:
                try:
                    ticker = mexc.fetch_ticker(symbol)
                    mark_px = float(ticker.get("last") or ticker.get("close") or 0)
                except Exception as te:
                    print(f"[WARN] MEXC {coin} ticker取得失敗: {te}")
            # PNLを常に手動計算（ccxtのunrealizedPnlは信頼しない）
            if entry_px > 0 and mark_px > 0:
                if side == "LONG":
                    pnl = (mark_px - entry_px) * contracts * cont_size
                elif side == "SHORT":
                    pnl = (entry_px - mark_px) * contracts * cont_size
                else:
                    pnl = float(pos.get("unrealizedPnl") or 0)
            else:
                pnl = float(pos.get("unrealizedPnl") or 0)
                print(f"[WARN] MEXC {coin} PNL計算不可: entry={entry_px} mark={mark_px}")
            result.append({
                "coin":          coin,
                "side":          side,
                "contracts":     contracts,
                "entry_price":   entry_px,
                "unrealized_pnl": pnl,
            })
        return result
    except Exception as e:
        print(f"[WARN] MEXC実ポジション取得失敗: {e}")
        return []


PNL_SNAPSHOT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "pnl_snapshot.json")


def load_pnl_snapshot() -> dict:
    try:
        if os.path.exists(PNL_SNAPSHOT_PATH):
            with open(PNL_SNAPSHOT_PATH) as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_pnl_snapshot(snap: dict) -> None:
    try:
        os.makedirs(os.path.dirname(PNL_SNAPSHOT_PATH), exist_ok=True)
        with open(PNL_SNAPSHOT_PATH, "w") as f:
            json.dump(snap, f)
    except Exception as e:
        print(f"[WARN] PNLスナップショット保存失敗: {e}")


def check_stuck_pnl(counter_positions: list) -> list:
    """前回と完全一致するPNLを検知して警告を返す"""
    prev = load_pnl_snapshot()
    warnings = []
    current = {}
    for p in counter_positions:
        coin = p.get("coin") or p.get("symbol", "")
        side = p.get("side", "")
        key  = f"{COUNTER_NAME}:{coin}:{side}"
        pnl_val = round(float(p.get("unrealized_pnl", 0)), 4)
        current[key] = pnl_val
        if key in prev and prev[key] == pnl_val and pnl_val != 0:
            warnings.append(f"⚠️ {coin} のPNLが前回と完全一致（${pnl_val:.2f}）— 計算固着の疑い")
        elif key in prev and pnl_val == 0:
            warnings.append(f"⚠️ {coin} のPNLが $0.00 — 計算失敗の可能性")
    save_pnl_snapshot(current)
    return warnings


def fetch_lighter_positions() -> list:
    """Lighter の実ポジションを取得"""
    try:
        import lighter_client
        return lighter_client.get_positions()  # [{symbol, side, size, entry_price, unrealized_pnl}]
    except Exception as e:
        print(f"[WARN] Lighter実ポジション取得失敗: {e}")
        return []


def build_position_section(hl_positions: list, counter_positions: list) -> list:
    lines = ["📊 <b>現在のポジション</b>"]

    if not hl_positions and not counter_positions:
        lines.append(f"  HL / {COUNTER_NAME}: ポジションなし")
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

    # Counter（Lighter or MEXC）ポジション
    if counter_positions:
        for p in counter_positions:
            pnl  = float(p.get("unrealized_pnl", 0))
            sign = "+" if pnl >= 0 else ""
            coin = p.get("coin") or p.get("symbol", "")
            side = p.get("side", "")
            size = p.get("size") or p.get("contracts", 0)
            entry= p.get("entry_price") or p.get("entry_px", 0)
            lines.append(
                f"  {'🔴' if side=='LONG' else '🟢'} {COUNTER_NAME} {coin} {side}"
                f"  {float(size):.4f}枚 @ ${float(entry):.4f}"
                f"\n    未実現PNL: <code>{sign}{pnl:.2f}$</code>"
            )
    else:
        lines.append(f"  {COUNTER_NAME}: ポジションなし")

    lines.append("")
    return lines

TAKER_RT     = 0.00035 * 2   # 0.070%
MAKER_RT     = 0.00010 * 2   # 0.020%
ENTRY_FR     = 0.0004         # エントリー閾値: 0.04%/h
EXCHANGE_MODE = os.environ.get("EXCHANGE_MODE", "LIGHTER").upper()
DATA_DIR     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
HL_FUNDING_CSV      = os.path.join(DATA_DIR, "funding_log.csv")
MEXC_FUNDING_CSV    = os.path.join(DATA_DIR, "mexc_funding_log.csv")
LIGHTER_FUNDING_CSV = os.path.join(DATA_DIR, "lighter_funding_log.csv")
COUNTER_CSV  = LIGHTER_FUNDING_CSV if EXCHANGE_MODE == "LIGHTER" else MEXC_FUNDING_CSV
COUNTER_NAME = "Lighter" if EXCHANGE_MODE == "LIGHTER" else "MEXC"


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


def load_latest_net_rates() -> dict:
    """
    CSVから coin ごとの最新共通timestampのFRを読み、net FR/h を返す。
    EXCHANGE_MODE に応じて Lighter または MEXC の CSV を使用。
    net_short_1h = hl_fr_1h - counter_fr_1h  (HL売 × Counter買)
    net_long_1h  = counter_fr_1h - hl_fr_1h  (HL買 × Counter売)
    """
    if not os.path.exists(HL_FUNDING_CSV) or not os.path.exists(COUNTER_CSV):
        return {}

    hl = defaultdict(dict)
    ct = defaultdict(dict)

    with open(HL_FUNDING_CSV) as f:
        for row in csv.DictReader(f):
            coin = row.get("coin", "")
            ts   = row.get("timestamp_utc", "")
            if not coin or not ts:
                continue
            try:
                hl[coin][ts] = float(row["funding_rate_1h"])
            except Exception:
                continue

    with open(COUNTER_CSV) as f:
        for row in csv.DictReader(f):
            coin = row.get("coin", "")
            ts   = row.get("timestamp_utc", "")
            if not coin or not ts:
                continue
            try:
                ct[coin][ts] = float(row["funding_rate_1h"])
            except Exception:
                continue

    out = {}
    for coin, hl_by_ts in hl.items():
        ct_by_ts = ct.get(coin, {})
        if not ct_by_ts:
            continue
        common_ts = sorted(set(hl_by_ts.keys()) & set(ct_by_ts.keys()))
        if not common_ts:
            continue
        ts        = common_ts[-1]
        hl_rate   = hl_by_ts[ts]
        ct_rate   = ct_by_ts[ts]
        short_net = hl_rate - ct_rate
        out[coin] = {
            "ts":            ts,
            "hl_fr_1h":      hl_rate,
            "counter_fr_1h": ct_rate,
            "net_short_1h":  short_net,
            "net_long_1h":   -short_net,
        }
    return out


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


def build_message(rows, now_str, hl_positions=None, counter_positions=None, net_rates=None):
    sorted_long  = sorted([r for r in rows if r["rate"] < 0], key=lambda r: r["rate"])
    sorted_short = sorted([r for r in rows if r["rate"] > 0], key=lambda r: r["rate"], reverse=True)
    maker_total = sum(1 for r in rows if abs(r["rate"]) > MAKER_RT)
    net_rates = net_rates or {}

    def pct(v):
        sign = "+" if v >= 0 else ""
        return f"{sign}{v*100:.4f}%"

    def label(rate, threshold):
        return "TAKER超え" if abs(rate) > threshold else "参考"

    lines = [
        f"<b>⚡ MindRaid Alert</b>  {now_str} UTC",
        f"対象 {len(rows)}銘柄 (HL∩Lighter) | MAKER超え: {maker_total}銘柄 | "
        f"エントリー判定: net FR (HL-{COUNTER_NAME}) ≥ {ENTRY_FR*100:.2f}%/h",
        f"🖥 <i>取引エンジン: ローカル運用中（GitHubはデータ収集のみ）</i>",
        "",
    ]

    # 既存ポジションの銘柄セット（エントリー予定から除外するため）
    open_coins = {p["coin"] for p in (hl_positions or [])} | {p.get("coin", p.get("symbol","")) for p in (counter_positions or [])}

    entry_candidates = []

    top_long = sorted_long[:3]
    if top_long:
        lines.append("🟢 <b>HL SHORT機会 TOP3</b>（FR ネガティブ）")
        for r in top_long:
            coin = r["coin"]
            net = net_rates.get(coin)
            if net:
                short_net = float(net["net_short_1h"])
                long_net = float(net["net_long_1h"])
                best_side = "SHORT" if short_net >= long_net else "LONG"
                best_net = max(short_net, long_net)
                net_txt = (
                    f" net短:<code>{pct(short_net)}</code>/h"
                    f" net長:<code>{pct(long_net)}</code>/h"
                )
                if coin in open_coins:
                    tag = " ⚡保有中"
                elif best_net >= ENTRY_FR:
                    tag = f" ⚡netエントリー圏(HL {best_side})"
                else:
                    tag = " ⚪net不足"
            else:
                net_txt = " net: <code>N/A</code>"
                tag = " ⚪net不明"
            lines.append(f"  {coin}  <code>{pct(r['rate'])}</code>/h |{net_txt}{tag}")
        lines.append("")

    top_short = sorted_short[:3]
    if top_short:
        lines.append("🔴 <b>HL LONG機会 TOP3</b>（FR ポジティブ）")
        for r in top_short:
            coin = r["coin"]
            net = net_rates.get(coin)
            if net:
                short_net = float(net["net_short_1h"])
                long_net = float(net["net_long_1h"])
                best_side = "SHORT" if short_net >= long_net else "LONG"
                best_net = max(short_net, long_net)
                net_txt = (
                    f" net短:<code>{pct(short_net)}</code>/h"
                    f" net長:<code>{pct(long_net)}</code>/h"
                )
                if coin in open_coins:
                    tag = " ⚡保有中"
                elif best_net >= ENTRY_FR:
                    tag = f" ⚡netエントリー圏(HL {best_side})"
                else:
                    tag = " ⚪net不足"
            else:
                net_txt = " net: <code>N/A</code>"
                tag = " ⚪net不明"
            lines.append(f"  {coin}  <code>{pct(r['rate'])}</code>/h |{net_txt}{tag}")
        lines.append("")

    # 次スキャンで継続なら候補（実際のtaker_botと同じく net FR 優位側で判定）
    for r in rows:
        coin = r["coin"]
        if coin in open_coins:
            continue
        net = net_rates.get(coin)
        if not net:
            continue
        short_net = float(net["net_short_1h"])
        long_net = float(net["net_long_1h"])
        best_side = "SHORT" if short_net >= long_net else "LONG"
        best_net = max(short_net, long_net)
        if best_net >= ENTRY_FR:
            entry_candidates.append((coin, best_side, best_net, short_net, long_net))

    entry_candidates.sort(key=lambda x: x[2], reverse=True)
    if entry_candidates:
        lines.append("🎯 <b>次のスキャンで継続ならエントリー予定</b>")
        for coin, side, best_net, short_net, long_net in entry_candidates[:5]:
            lines.append(
                f"  → {coin}  HL {side}  net:<code>{pct(best_net)}</code>/h"
                f" (short:<code>{pct(short_net)}</code> / long:<code>{pct(long_net)}</code>)"
            )
        lines.append("")
    else:
        lines.append("ℹ️ <b>net FR基準のエントリー候補なし</b>")
        lines.append("")

    pos_lines = build_position_section(hl_positions or [], counter_positions or [])
    lines.extend(pos_lines)

    stuck_warnings = check_stuck_pnl(counter_positions or [])
    if stuck_warnings:
        lines.extend(stuck_warnings)
        lines.append("")

    return "\n".join(lines)


def main():
    env = load_env()
    token   = env["TELEGRAM_BOT_TOKEN"]
    chat_id = env["TELEGRAM_CHAT_ID"]

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    print("Fetching funding data …")
    info = Info(skip_ws=True)
    rows = fetch_data(info)

    # Lighter 未対応銘柄を除外（ヘッジ不可＝取引対象外）
    if EXCHANGE_MODE == "LIGHTER":
        try:
            import lighter_client
            lighter_markets = lighter_client.get_markets() or {}
            before = len(rows)
            rows = [r for r in rows if r["coin"] in lighter_markets]
            print(f"[filter] Lighter対応銘柄のみに絞り込み: {before} → {len(rows)}")
        except Exception as e:
            print(f"[WARN] Lighter market 取得失敗、フィルタ無し: {e}")

    net_rates = load_latest_net_rates()

    hl_address = os.environ.get("HL_WALLET_ADDRESS", "")
    hl_positions = fetch_hl_positions(hl_address)

    if EXCHANGE_MODE == "LIGHTER":
        counter_positions = fetch_lighter_positions()
    else:
        mexc_api_key = os.environ.get("MEXC_API_KEY", "")
        mexc_api_sec = os.environ.get("MEXC_API_SECRET", "")
        counter_positions = fetch_mexc_positions(mexc_api_key, mexc_api_sec)

    msg = build_message(
        rows,
        now_str,
        hl_positions=hl_positions,
        counter_positions=counter_positions,
        net_rates=net_rates,
    )
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
