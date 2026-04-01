"""
pnl_logger.py
実際のFR収益・手数料・スリッページを集計し理論値と比較してCSVに追記する。
毎時 cron から呼び出す想定。

取得元:
  - HL : /info {"type": "userFunding", "user": ADDRESS}
  - MEXC: ccxt fetch_funding_history (未実装時は手入力列で対応)
  - 理論値: data/funding_log.csv の直近レートから計算

CSV: data/pnl_log.csv
"""

import csv
import os
import sys
import time
from datetime import datetime, timezone

from hyperliquid.info import Info

DATA_DIR   = os.path.join(os.path.dirname(__file__), "data")
POS_PATH   = os.path.join(DATA_DIR, "positions.csv")
PNL_PATH   = os.path.join(DATA_DIR, "pnl_log.csv")
FUND_PATH  = os.path.join(DATA_DIR, "funding_log.csv")

PNL_FIELDS = [
    "logged_at_utc", "position_id", "coin", "size",
    "period_hours",
    "hl_funding_actual_usd",
    "mexc_funding_actual_usd",
    "total_funding_actual_usd",
    "total_funding_theoretical_usd",
    "accuracy_pct",
    "entry_fees_usd",
    "slippage_usd",
    "net_pnl_usd",
    "annualized_apy_pct",
    "hl_wallet",
]


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
    for key in ["HL_WALLET_ADDRESS", "MEXC_API_KEY", "MEXC_SECRET",
                "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


# ── ポジション読み込み ─────────────────────────────────────────

def read_open_positions() -> list[dict]:
    if not os.path.exists(POS_PATH):
        return []
    with open(POS_PATH, newline="") as f:
        return [r for r in csv.DictReader(f) if r["status"] == "open"]


# ── 前回ログ情報の取得 ────────────────────────────────────────

def get_last_log_for_position(position_id: str) -> tuple:
    """
    (last_logged_at_utc_str, cumulative_funding_usd) を返す。
    CSV にログがなければ (None, 0.0)。

    - last_logged_at_utc_str: 前回ログのタイムスタンプ（次回のsince_msに使う）
    - cumulative_funding_usd: これまでの累計実績FR収益
    """
    if not os.path.exists(PNL_PATH):
        return None, 0.0

    last_time = None
    cumulative = 0.0
    with open(PNL_PATH, newline="") as f:
        for row in csv.DictReader(f):
            if row.get("position_id") == position_id:
                last_time = row.get("logged_at_utc")
                try:
                    cumulative += float(row.get("total_funding_actual_usd", 0))
                except ValueError:
                    pass

    return last_time, cumulative


# ── HL 実際のFR取得 ───────────────────────────────────────────

def fetch_hl_funding_since(info: Info, wallet: str, since_ms: int) -> list[dict]:
    """
    HL の userFunding エンドポイントから since_ms 以降の資金調達履歴を取得。
    戻り値: [{"coin": str, "usdc": float, "time": int}, ...]
    """
    raw = info.post("/info", {
        "type":      "userFunding",
        "user":      wallet,
        "startTime": since_ms,
    })
    results = []
    if not raw:
        return results
    for item in raw:
        delta = item.get("delta", {})
        results.append({
            "coin": delta.get("coin", ""),
            "usdc": float(delta.get("usdc", 0)),
            "time": int(item.get("time", 0)),
        })
    return results


# ── 理論値計算 ────────────────────────────────────────────────

def calc_theoretical(coin: str, size: float, hours: float, entry_price: float) -> float:
    """
    funding_log.csv の直近レートからFR収益理論値をUSD換算で計算。
    """
    if not os.path.exists(FUND_PATH) or entry_price == 0:
        return 0.0

    rows = []
    with open(FUND_PATH, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("coin") == coin:
                rows.append(r)

    if not rows:
        return 0.0

    # 直近24件固定で平均レートを算出（period_hoursで切ると短期間時に1件しか使わず暴れるため）
    lookback = min(len(rows), 24)
    recent = rows[-lookback:]
    avg_rate_1h = sum(float(r["funding_rate_1h"]) for r in recent) / len(recent)

    # 理論収益 USD = avg_rate_1h × hours × size × entry_price
    return avg_rate_1h * hours * size * entry_price


# ── スリッページ計算 ──────────────────────────────────────────

def calc_slippage(pos: dict) -> float:
    try:
        s = float(pos["short_entry_price"])
        l = float(pos["long_entry_price"])
        size = float(pos["size"])
        return abs(s - l) * size / 2
    except (ValueError, KeyError):
        return 0.0


# ── PnL CSV 追記 ─────────────────────────────────────────────

def append_pnl(row: dict) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    write_header = not os.path.exists(PNL_PATH)
    with open(PNL_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=PNL_FIELDS)
        if write_header:
            writer.writeheader()
        writer.writerow(row)


# ── メイン ────────────────────────────────────────────────────

def main() -> None:
    env    = load_env()
    wallet = env.get("HL_WALLET_ADDRESS", "")
    if not wallet:
        print("❌ HL_WALLET_ADDRESS が未設定 (~/.env に追加してください)")
        sys.exit(1)

    info   = Info(skip_ws=True)
    now    = datetime.now(timezone.utc)
    now_ms = int(now.timestamp() * 1000)
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    open_positions = read_open_positions()
    if not open_positions:
        print(f"[{now_str}] オープンポジションなし → スキップ")
        return

    print(f"[{now_str}] オープンポジション {len(open_positions)}件 を処理中...")

    for pos in open_positions:
        pid   = pos["position_id"]
        coin  = pos["coin"]
        size  = float(pos["size"])

        # 開設からの経過時間
        opened = datetime.strptime(pos["opened_at_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        hours_total = (now - opened).total_seconds() / 3600

        # 前回ログ情報を取得
        last_logged_str, prev_cumulative = get_last_log_for_position(pid)

        # 前回ログ時刻以降のFRを取得（初回はポジション開設時刻から）
        if last_logged_str:
            last_logged_dt = datetime.strptime(last_logged_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            since_ms = int(last_logged_dt.timestamp() * 1000)
            period_hours = (now - last_logged_dt).total_seconds() / 3600
        else:
            since_ms = int(opened.timestamp() * 1000)
            period_hours = hours_total

        # HL 実際のFR収益（今回期間分のみ）
        hl_history = fetch_hl_funding_since(info, wallet, since_ms)
        hl_funding_usd = sum(
            item["usdc"] for item in hl_history
            if item["coin"] == coin
        )

        # MEXC は現時点では手動入力のため 0
        mexc_funding_usd = 0.0

        # 今期間の実績合計
        period_actual = hl_funding_usd + mexc_funding_usd

        # 累計実績 FR（過去ログ + 今期間）
        cumulative_funding = prev_cumulative + period_actual

        # エントリー価格
        try:
            entry_price = (float(pos["short_entry_price"]) + float(pos["long_entry_price"])) / 2
        except (ValueError, KeyError):
            entry_price = 0.0

        # 今期間の理論値
        theoretical_usd = calc_theoretical(coin, size, period_hours, entry_price)

        # 精度
        accuracy = (period_actual / theoretical_usd * 100) if theoretical_usd != 0 else None

        # 手数料（入場時の1回限り）
        try:
            entry_fees = float(pos["short_entry_fee_usd"]) + float(pos["long_entry_fee_usd"])
        except (ValueError, KeyError):
            entry_fees = 0.0

        # スリッページ
        slippage = calc_slippage(pos)

        # Net PnL = 累計FR収益 - 入場手数料（1回限り）
        net_pnl = cumulative_funding - entry_fees

        # 年率 APY（今期間のFRから計算）
        notional = size * entry_price if entry_price else 0.0
        if notional > 0 and period_hours > 0:
            hourly_rate = period_actual / notional / period_hours
            annualized_apy = hourly_rate * 24 * 365 * 100
        else:
            annualized_apy = 0.0

        row = {
            "logged_at_utc":                now_str,
            "position_id":                  pid,
            "coin":                         coin,
            "size":                         size,
            "period_hours":                 round(period_hours, 2),
            "hl_funding_actual_usd":        round(hl_funding_usd, 6),
            "mexc_funding_actual_usd":      round(mexc_funding_usd, 6),
            "total_funding_actual_usd":     round(period_actual, 6),
            "total_funding_theoretical_usd": round(theoretical_usd, 6),
            "accuracy_pct":                 round(accuracy, 2) if accuracy is not None else "",
            "entry_fees_usd":               round(entry_fees, 4),
            "slippage_usd":                 round(slippage, 4),
            "net_pnl_usd":                  round(net_pnl, 6),
            "annualized_apy_pct":           round(annualized_apy, 2),
            "hl_wallet":                    wallet[:8] + "...",
        }

        append_pnl(row)

        acc_str = f"{accuracy:.1f}%" if accuracy is not None else "N/A"
        print(f"  [{pid}] {coin} {size} ETH | "
              f"期間FR: ${period_actual:+.4f} | 累計FR: ${cumulative_funding:+.4f} | "
              f"理論: ${theoretical_usd:+.4f} | 精度: {acc_str} | "
              f"Net PnL: ${net_pnl:+.4f} | APY: {annualized_apy:.2f}%")

    print(f"→ {len(open_positions)}件 追記完了: {PNL_PATH}")


if __name__ == "__main__":
    main()
