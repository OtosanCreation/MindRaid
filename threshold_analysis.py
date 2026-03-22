"""
threshold_analysis.py
過去2週間の HlPerp fundingHistory から BTC/ETH/SOL の
閾値超え頻度を集計する。

閾値（1h レート、絶対値）
  BREAKEVEN : 0.011%  = 往復 maker 手数料の損益分岐
  PROFIT    : 0.015%  = 現実的に利益が出る水準
"""

import time
from collections import defaultdict
from datetime import datetime, timezone

from hyperliquid.info import Info

# ── 設定 ──────────────────────────────────────────────
COINS     = ["BTC", "ETH", "SOL"]
DAYS      = 14
BREAKEVEN = 0.00011   # 0.011% / h
PROFIT    = 0.00015   # 0.015% / h
# ──────────────────────────────────────────────────────


def ts_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def pct(v: float) -> str:
    return f"{v * 100:+.4f}%"


def bar(n: int, total: int, width: int = 30) -> str:
    filled = round(width * n / total) if total else 0
    return "█" * filled + "░" * (width - filled)


def analyze(coin: str, records: list[dict]) -> dict:
    rates = [float(r["fundingRate"]) for r in records]
    abs_rates = [abs(r) for r in rates]
    n = len(rates)

    pos = [r for r in rates if r > 0]   # longs pay → ショートが受け取り
    neg = [r for r in rates if r < 0]   # shorts pay → ロングが受け取り

    # 閾値超え（どちら向きでも受け取れる機会）
    be_any   = [r for r in rates if abs(r) >= BREAKEVEN]
    pr_any   = [r for r in rates if abs(r) >= PROFIT]

    # 向き別
    be_long  = [r for r in rates if r <= -BREAKEVEN]   # ロング受取
    be_short = [r for r in rates if r >= BREAKEVEN]    # ショート受取
    pr_long  = [r for r in rates if r <= -PROFIT]
    pr_short = [r for r in rates if r >= PROFIT]

    # 連続超え（連続して何時間続くか）の最大値
    def max_streak(lst, pred):
        best = cur = 0
        for v in lst:
            if pred(v):
                cur += 1
                best = max(best, cur)
            else:
                cur = 0
        return best

    max_streak_be = max_streak(rates, lambda r: abs(r) >= BREAKEVEN)
    max_streak_pr = max_streak(rates, lambda r: abs(r) >= PROFIT)

    # 時間帯別（UTC hour）の頻度
    hourly_be = defaultdict(int)
    for r_dict in records:
        if abs(float(r_dict["fundingRate"])) >= BREAKEVEN:
            h = ts_to_dt(r_dict["time"]).hour
            hourly_be[h] += 1

    return {
        "coin": coin,
        "n": n,
        "mean":   sum(abs_rates) / n if n else 0,
        "max":    max(abs_rates) if abs_rates else 0,
        "pos_hrs": len(pos),
        "neg_hrs": len(neg),
        # 損益分岐
        "be_total":  len(be_any),
        "be_long":   len(be_long),
        "be_short":  len(be_short),
        "be_streak": max_streak_be,
        # 利益水準
        "pr_total":  len(pr_any),
        "pr_long":   len(pr_long),
        "pr_short":  len(pr_short),
        "pr_streak": max_streak_pr,
        # 時間帯
        "hourly_be": hourly_be,
        # 生データ（度数分布用）
        "rates": rates,
    }


def print_report(stats: list[dict], days: int) -> None:
    total_hours = days * 24
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'='*68}")
    print(f"  Funding Threshold Analysis  (HlPerp, {days}d)  —  {now}")
    print(f"  BREAKEVEN threshold : {pct(BREAKEVEN)} / h")
    print(f"  PROFIT    threshold : {pct(PROFIT)} / h")
    print(f"  参照時間数          : {total_hours} h (実データ: {stats[0]['n']} h)")
    print(f"{'='*68}")

    for s in stats:
        n = s["n"]
        print(f"\n  ┌─ {s['coin']} ─────────────────────────────────────────────")
        print(f"  │  平均 |rate|  : {pct(s['mean'])}  最大: {pct(s['max'])}")
        print(f"  │  rate > 0 (long pays) : {s['pos_hrs']:4d} h  "
              f"rate < 0 (short pays): {s['neg_hrs']:4d} h")

        print(f"  │")
        print(f"  │  ── 損益分岐 (≥ {pct(BREAKEVEN)}) ──")
        print(f"  │  合計   : {s['be_total']:4d} / {n} h  "
              f"= {s['be_total']/n*100:5.1f}%  "
              f"≈ {s['be_total']/days:.1f} h/day")
        print(f"  │  {bar(s['be_total'], n)}  {s['be_total']:4d}h")
        print(f"  │    ロング受取: {s['be_long']:4d} h  ショート受取: {s['be_short']:4d} h")
        print(f"  │  最長連続    : {s['be_streak']} h")

        print(f"  │")
        print(f"  │  ── 利益水準 (≥ {pct(PROFIT)}) ──")
        print(f"  │  合計   : {s['pr_total']:4d} / {n} h  "
              f"= {s['pr_total']/n*100:5.1f}%  "
              f"≈ {s['pr_total']/days:.1f} h/day")
        print(f"  │  {bar(s['pr_total'], n)}  {s['pr_total']:4d}h")
        print(f"  │    ロング受取: {s['pr_long']:4d} h  ショート受取: {s['pr_short']:4d} h")
        print(f"  │  最長連続    : {s['pr_streak']} h")

        # 度数分布（簡易ヒストグラム）
        print(f"  │")
        print(f"  │  ── |rate| 度数分布 ──")
        buckets = [
            ("< 0.005%",  0,        0.00005),
            ("0.005~0.01%", 0.00005, 0.0001),
            ("0.01~0.011%", 0.0001,  BREAKEVEN),
            (f"0.011~0.015% [BE]", BREAKEVEN, PROFIT),
            (f"≥ 0.015% [PR]",     PROFIT,    float("inf")),
        ]
        for label, lo, hi in buckets:
            cnt = sum(1 for r in s["rates"] if lo <= abs(r) < hi)
            mark = " ←" if lo >= BREAKEVEN else ""
            print(f"  │  {label:<20} {bar(cnt, n, 20)} {cnt:4d} h{mark}")

        print(f"  └{'─'*63}")

    # 3銘柄合計サマリ
    print(f"\n  ── 3銘柄合計サマリ (損益分岐超え機会) ──")
    print(f"  {'銘柄':<6} {'BE超/day':>9} {'PR超/day':>9} {'BE%':>7} {'PR%':>7}")
    print(f"  {'-'*44}")
    for s in stats:
        n = s["n"]
        print(f"  {s['coin']:<6} {s['be_total']/days:>8.1f}h {s['pr_total']/days:>8.1f}h "
              f"{s['be_total']/n*100:>6.1f}% {s['pr_total']/n*100:>6.1f}%")

    print(f"\n  ── 注記 ──")
    print(f"  ・1 機会 = 1h。entryとexitは同 period 内と想定（最小ケース）")
    print(f"  ・実際は entry/exit 各1h かかるため実効機会は BE_total - 2 程度")
    print(f"  ・レートは 1h ごとに変動するため streak（連続時間）が重要な指標")
    print(f"{'='*68}")


def main():
    info = Info(skip_ws=True)
    start_ms = int((time.time() - DAYS * 86400) * 1000)

    all_stats = []
    for coin in COINS:
        print(f"Fetching {coin} fundingHistory ({DAYS}d)…")
        records = info.funding_history(coin, start_ms)
        all_stats.append(analyze(coin, records))

    print_report(all_stats, DAYS)


if __name__ == "__main__":
    main()
