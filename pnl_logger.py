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

def calc_theoretical(coin: str, size: float, hours: float) -> float:
    """
    funding_log.csv の直近レートからFR収益理論値を計算。
    size: ETH枚数, hours: 保有時間数
    """
    if not os.path.exists(FUND_PATH):
        return 0.0

    rows = []
    with open(FUND_PATH, newline="") as f:
        for r in csv.DictReader(f):
            if r.get("coin") == coin:
                rows.append(r)

    if not rows:
        return 0.0

    # 直近 N レコードの平均レート (1h)
    lookback = min(len(rows), int(hours) + 1)
    recent = rows[-lookback:]
    avg_rate_1h = sum(float(r["funding_rate_1h"]) for r in recent) / len(recent)

    # 理論収益 = avg_rate_1h × hours × notional
    # notional の価格: funding_log には価格がないので size のみで%計算 → USDはエントリ価格が必要
    # ここでは rate × hours × size を返す（呼び出し元でエントリ価格を掛ける）
    return avg_rate_1h * hours * size  # unit: ETH×rate (dimensionless)


# ── スリッページ計算 ──────────────────────────────────────────

def calc_slippage(pos: dict) -> float:
    """
    理想価格 = (short_entry + long_entry) / 2
    実スリッページ = |short_entry - long_entry| × size / 2
    デルタニュートラルなら両側のスリッページが打ち消し合うためゼロに近い。
    参考値として記録する。
    """
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

    # HL の直近 24h 資金調達履歴をまとめて取得
    since_24h_ms = now_ms - 24 * 3600 * 1000
    hl_history   = fetch_hl_funding_since(info, wallet, since_24h_ms)

    for pos in open_positions:
        pid   = pos["position_id"]
        coin  = pos["coin"]
        size  = float(pos["size"])

        # 開設からの経過時間
        opened = datetime.strptime(pos["opened_at_utc"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        hours_total = (now - opened).total_seconds() / 3600

        # HL 実際のFR収益 (直近24h 分)
        hl_funding_usd = sum(
            item["usdc"] for item in hl_history
            if item["coin"] == coin
        )

        # MEXC は現時点では手動入力のため 0 (将来 ccxt で自動化)
        mexc_funding_usd = 0.0

        total_actual = hl_funding_usd + mexc_funding_usd

        # 理論値 (エントリー価格を使って USD 換算)
        try:
            entry_price = (float(pos["short_entry_price"]) + float(pos["long_entry_price"])) / 2
        except (ValueError, KeyError):
            entry_price = 0.0
        theoretical_rate_eth = calc_theoretical(coin, size, min(hours_total, 24))
        theoretical_usd = theoretical_rate_eth * entry_price  # 直近24h理論値

        # 精度 (%)
        accuracy = (total_actual / theoretical_usd * 100) if theoretical_usd != 0 else None

        # 手数料
        try:
            entry_fees = float(pos["short_entry_fee_usd"]) + float(pos["long_entry_fee_usd"])
        except (ValueError, KeyError):
            entry_fees = 0.0

        # スリッページ
        slippage = calc_slippage(pos)

        # Net PnL (直近24h FR - 手数料)
        net_pnl = total_actual - entry_fees

        # 年率 APY
        notional = size * entry_price if entry_price else 0.0
        if notional > 0 and hours_total > 0:
            daily_rate = total_actual / notional / (min(hours_total, 24) / 24)
            annualized_apy = daily_rate * 365 * 100
        else:
            annualized_apy = 0.0

        row = {
            "logged_at_utc":                now_str,
            "position_id":                  pid,
            "coin":                         coin,
            "size":                         size,
            "period_hours":                 round(min(hours_total, 24), 2),
            "hl_funding_actual_usd":        round(hl_funding_usd, 6),
            "mexc_funding_actual_usd":      round(mexc_funding_usd, 6),
            "total_funding_actual_usd":     round(total_actual, 6),
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
              f"実FR: ${total_actual:+.4f} | 理論: ${theoretical_usd:+.4f} | "
              f"精度: {acc_str} | APY: {annualized_apy:.2f}%")

    print(f"→ {len(open_positions)}件 追記完了: {PNL_PATH}")


if __name__ == "__main__":
    main()
