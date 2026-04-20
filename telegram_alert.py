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
try:
    from taker_bot import fetch_lighter_actual_funding, fetch_hl_actual_funding
    _ACTUAL_FUNDING_AVAILABLE = True
except Exception:
    _ACTUAL_FUNDING_AVAILABLE = False


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


HL_COST_RT   = 0.00035 * 2   # HL taker 往復 0.07%（open + close）
LIGHTER_COST_RT = 0.0         # Lighter は maker = 0%


def load_taker_state() -> dict:
    """data/taker_state.json から保有ポジション情報を読む。"""
    path = os.path.join(DATA_DIR, "taker_state.json")
    try:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f).get("positions", {})
    except Exception:
        pass
    return {}


def calc_position_stats(state_pos: dict, now: datetime, coin: str = None, hl_address: str = None) -> dict:
    """
    1ポジション分の損益・BE を計算。
    coin/hl_address が渡された場合は実際の funding API から取得（正確値）。
    なければ entry_net_fr_1h × 保有時間で推定。
    """
    try:
        opened_at  = datetime.strptime(state_pos["opened_at"], "%Y-%m-%d %H:%M:%S")
        hours_held = (now - opened_at).total_seconds() / 3600
        net_fr     = float(state_pos.get("entry_net_fr_1h", 0))
        size_usd   = float(state_pos.get("size_usd", 90))
        direction  = state_pos.get("direction", "short_fr")

        # Lighter モードは HL 側のみ taker 手数料
        total_cost_usd = HL_COST_RT * size_usd

        # 実際の funding API で取得できる場合はそちらを優先
        if _ACTUAL_FUNDING_AVAILABLE and coin and hl_address:
            try:
                _info = Info(skip_ws=True)
                _now_str = now.strftime("%Y-%m-%d %H:%M:%S")
                _hl_fund = fetch_hl_actual_funding(_info, hl_address, coin, state_pos["opened_at"], _now_str)
                _lt_fund = fetch_lighter_actual_funding(coin, state_pos["opened_at"], _now_str, size_usd, direction)
                est_funding_usd = _hl_fund + _lt_fund
            except Exception:
                est_funding_usd = net_fr * hours_held * size_usd
        else:
            est_funding_usd = net_fr * hours_held * size_usd

        est_net_usd = est_funding_usd - total_cost_usd

        if net_fr > 0:
            be_hours       = total_cost_usd / (net_fr * size_usd)
            be_remaining_h = be_hours - hours_held
            be_eta         = opened_at + __import__("datetime").timedelta(hours=be_hours)
            be_eta_str     = be_eta.strftime("%H:%M UTC")
        else:
            be_hours = be_remaining_h = None
            be_eta_str = "計算不可"

        return {
            "hours_held":     hours_held,
            "est_funding_usd": est_funding_usd,
            "total_cost_usd": total_cost_usd,
            "est_net_usd":    est_net_usd,
            "be_hours":       be_hours,
            "be_remaining_h": be_remaining_h,
            "be_eta_str":     be_eta_str,
        }
    except Exception as e:
        return {}


EXIT_FR_1H        = 0.0002   # コスト未回収時の決済閾値
EXIT_FR_RECOVERED = 0.0001   # コスト回収済み後の決済閾値


def build_position_section(hl_positions: list, counter_positions: list, now: datetime = None, hl_address: str = None) -> list:
    lines = ["📊 <b>今持ってるポジション</b>"]
    now = now or datetime.utcnow()

    if not hl_positions and not counter_positions:
        lines.append("  今は何も持っていません")
        lines.append("")
        return lines

    state_positions = load_taker_state()
    net_rates = load_latest_net_rates()
    hl_by_coin = {p["coin"]: p for p in (hl_positions or [])}
    ct_by_coin = {(p.get("coin") or p.get("symbol","")): p for p in (counter_positions or [])}
    all_coins  = sorted(set(list(hl_by_coin.keys()) + list(ct_by_coin.keys())))

    def s(v): return ("+" if v >= 0 else "") + f"{v:.2f}"

    for coin in all_coins:
        hl = hl_by_coin.get(coin)
        ct = ct_by_coin.get(coin)
        sp = state_positions.get(coin, {})
        stats = calc_position_stats(sp, now, coin=coin, hl_address=hl_address) if sp else {}
        nr = net_rates.get(coin)

        if stats:
            hl_pnl_v  = hl["unrealized_pnl"] if hl else 0.0
            ct_pnl_v  = float(ct.get("unrealized_pnl", 0)) if ct else 0.0
            price_pnl = hl_pnl_v + ct_pnl_v
            est_net   = stats["est_net_usd"]
            held_h    = stats["hours_held"]
            be_rem    = stats["be_remaining_h"]
            cost_recovered = stats["est_funding_usd"] >= stats["total_cost_usd"]
            exit_thr  = EXIT_FR_RECOVERED if cost_recovered else EXIT_FR_1H

            if cost_recovered:
                cost_line = "手数料: 回収済み ✅"
            elif be_rem is not None and be_rem > 0:
                cost_line = f"手数料回収まで: あと <code>{be_rem:.1f}h</code>"
            elif be_rem is not None and be_rem <= 0:
                cost_line = "手数料: 未回収（FR変動で想定外れ）"
            else:
                cost_line = ""

            # net FR と EXIT 閾値比較
            if nr:
                direction = sp.get("direction", "short_fr")
                current_net = float(nr["net_short_1h"] if direction == "short_fr" else nr["net_long_1h"])
                ratio = current_net / exit_thr if exit_thr > 0 else 0
                hold_exit = "保有継続 ✅" if current_net >= exit_thr else "決済対象 🔴"
                thr_note = "（手数料回収後なので緩め）" if cost_recovered else "（回収前）"
                fr_line = (
                    f"net FR <code>{current_net*100:+.4f}%/h</code>"
                    f"  決済基準 <code>{exit_thr*100:.4f}%/h</code>{thr_note} の <code>{ratio:.1f}倍</code> → {hold_exit}"
                )
            else:
                fr_line = "net FR: データなし"

            lines.append(
                f"  💰 {coin}  保有 <code>{held_h:.1f}h</code>  {cost_line}\n"
                f"    {fr_line}\n"
                f"    FR収益: <code>{s(stats['est_funding_usd'])}$</code>"
                f"  手数料: <code>-{stats['total_cost_usd']:.2f}$</code>"
                f"  手取り: <code>{s(est_net)}$</code>\n"
                f"    価格変動の影響: <code>{s(price_pnl)}$</code>（ほぼゼロが正常）"
            )
        else:
            if hl:
                lines.append(f"  💰 {coin}  HL {hl['side']}  含み益: <code>{s(hl['unrealized_pnl'])}$</code>")
        lines.append("")

    return lines

TAKER_RT     = 0.00035 * 2   # 0.070%
MAKER_RT     = 0.00010 * 2   # 0.020%
ENTRY_FR     = 0.00005        # エントリー閾値: 0.005%/h（8x修正後バックテスト最適値）
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


def build_message(rows, now_str, hl_positions=None, counter_positions=None, net_rates=None, now: datetime = None, hl_address: str = None):
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
        f"<b>⚡ MindRaid</b>  {now_str} UTC",
        f"スキャン対象 {len(rows)}銘柄 | 稼ぎ率が基準（{ENTRY_FR*100:.3f}%/h）を超えてる銘柄: {maker_total}銘柄",
        "",
    ]

    # 既存ポジションの銘柄セット（エントリー予定から除外するため）
    open_coins = {p["coin"] for p in (hl_positions or [])} | {p.get("coin", p.get("symbol","")) for p in (counter_positions or [])}

    entry_candidates = []

    def format_top_row_by_net(coin, side_net, hl_rate, ct_rate, open_coins):
        held = "  ← 今持ってる" if coin in open_coins else ""
        flag = "⚡" if side_net >= ENTRY_FR else " "
        ratio = side_net / ENTRY_FR if ENTRY_FR > 0 else 0
        return (
            f"  {flag} {coin}  net FR <code>{pct(side_net)}</code>/h（基準{ENTRY_FR*100:.3f}%の{ratio:.1f}倍）"
            f"  HL <code>{pct(hl_rate)}</code> / {COUNTER_NAME} <code>{pct(ct_rate)}</code>{held}"
        )

    short_ops = []
    long_ops  = []
    for coin, nr in net_rates.items():
        short_ops.append((coin, float(nr["net_short_1h"]), float(nr["hl_fr_1h"]), float(nr["counter_fr_1h"])))
        long_ops.append((coin, float(nr["net_long_1h"]), float(nr["hl_fr_1h"]), float(nr["counter_fr_1h"])))
    short_ops.sort(key=lambda x: x[1], reverse=True)
    long_ops.sort(key=lambda x: x[1], reverse=True)

    if short_ops:
        lines.append(f"🟢 <b>net FR TOP3（売り戦略 / 基準 {ENTRY_FR*100:.3f}%/h以上でエントリー）</b>")
        for coin, sn, hr, cr in short_ops[:3]:
            lines.append(format_top_row_by_net(coin, sn, hr, cr, open_coins))
        lines.append("")

    if long_ops:
        lines.append(f"🔴 <b>net FR TOP3（買い戦略 / 基準 {ENTRY_FR*100:.3f}%/h以上でエントリー）</b>")
        for coin, ln, hr, cr in long_ops[:3]:
            lines.append(format_top_row_by_net(coin, ln, hr, cr, open_coins))
        lines.append("")

    # D/E ブロック（エントリー予告・ポジションスナップショット）は taker_bot の ENTRY/HOLD 通知で代替のため削除済み

    # Lighter は PNL 計算が信頼できるため stuck チェック不要（MEXC 専用）
    if EXCHANGE_MODE != "LIGHTER":
        stuck_warnings = check_stuck_pnl(counter_positions or [])
        if stuck_warnings:
            lines.extend(stuck_warnings)
            lines.append("")

    return "\n".join(lines)


def main():
    env = load_env()
    token   = env["TELEGRAM_BOT_TOKEN"]
    chat_id = env["TELEGRAM_CHAT_ID"]

    now_dt  = datetime.now(timezone.utc).replace(tzinfo=None)  # naive UTC
    now_str = now_dt.strftime("%Y-%m-%d %H:%M")
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
        now=now_dt,
        hl_address=hl_address,
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
