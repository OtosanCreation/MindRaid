"""
funding_scan.py
Hyperliquid の predictedFundings を取得し、
「手数料を超える機会」を一覧表示するスクリプト。

データ構造:
  [ [coin, [ [venue, {fundingRate, nextFundingTime, fundingIntervalHours}], ... ]], ... ]
  venue: "HlPerp" (1h), "BinPerp" (4h), "BybitPerp" (4h)

手数料の目安（Hyperliquid）
  Taker: 0.035%  (0.00035)  ← 片道
  Maker: 0.010%  (0.00010)  ← 片道
  往復 taker: 0.07%  往復 maker: 0.02%
"""

from hyperliquid.info import Info
from datetime import datetime, timezone

# ── 設定 ──────────────────────────────────────────
TAKER_FEE        = 0.00035
MAKER_FEE        = 0.00010
ROUND_TRIP_TAKER = TAKER_FEE * 2    # 0.0007  (片道 entry + exit)
ROUND_TRIP_MAKER = MAKER_FEE * 2    # 0.0002

# 注目する venue（HlPerp が Hyperliquid 本体）
TARGET_VENUE = "HlPerp"
# ──────────────────────────────────────────────────


def pct(rate: float) -> str:
    sign = "+" if rate >= 0 else ""
    return f"{sign}{rate * 100:.4f}%"


def to_hourly(rate: float, interval_h: int) -> float:
    """任意の period レートを 1h レートに正規化"""
    return rate / interval_h


def main():
    info = Info(skip_ws=True)

    print("Fetching predictedFundings …")
    raw = info.post("/info", {"type": "predictedFundings"})
    # raw: [ [coin, [ [venue, {fundingRate, nextFundingTime, fundingIntervalHours}], ... ]], ... ]

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # ─── HlPerp 行だけ抽出 ────────────────────────
    rows = []
    for item in raw:
        coin = item[0]
        venues = item[1]  # list of [venue_name, data_dict]
        for venue_name, data in venues:
            if venue_name == TARGET_VENUE:
                rate     = float(data["fundingRate"])
                interval = int(data["fundingIntervalHours"])   # HlPerp = 1
                rows.append({
                    "coin":     coin,
                    "rate":     rate,
                    "interval": interval,
                    "rate_1h":  to_hourly(rate, interval),
                    "rate_8h":  to_hourly(rate, interval) * 8,   # 比較用
                    "rate_24h": to_hourly(rate, interval) * 24,
                })
                break

    # ─── 統計 ─────────────────────────────────────
    total = len(rows)

    # HlPerp は 1h ごとに精算 → 比較は「1 期間 = 1h」の rate で
    taker_long  = [r for r in rows if r["rate"] < -ROUND_TRIP_TAKER]
    taker_short = [r for r in rows if r["rate"] >  ROUND_TRIP_TAKER]
    maker_long  = [r for r in rows if r["rate"] < -ROUND_TRIP_MAKER]
    maker_short = [r for r in rows if r["rate"] >  ROUND_TRIP_MAKER]

    print(f"\n{'='*65}")
    print(f"  Hyperliquid Predicted Funding  ({TARGET_VENUE})  —  {now}")
    print(f"{'='*65}")
    print(f"  取得銘柄数             : {total}")
    print(f"  精算間隔 (HlPerp)      : 1h")
    print(f"  往復手数料 taker       : {pct(ROUND_TRIP_TAKER)}")
    print(f"  往復手数料 maker       : {pct(ROUND_TRIP_MAKER)}")
    print()

    taker_total = len(taker_long) + len(taker_short)
    maker_total = len(maker_long) + len(maker_short)

    print(f"【Taker 超え】 |1h rate| > {pct(ROUND_TRIP_TAKER)}")
    print(f"  ロング受取  (rate<0) : {len(taker_long):3d} 銘柄")
    print(f"  ショート受取 (rate>0) : {len(taker_short):3d} 銘柄")
    print(f"  合計                  : {taker_total:3d} 銘柄 / この 1 期間")
    print(f"  → 1日換算             : 約 {taker_total * 24} 機会 (24期間×同数と仮定)")
    print()

    print(f"【Maker 超え】 |1h rate| > {pct(ROUND_TRIP_MAKER)}")
    print(f"  ロング受取  (rate<0) : {len(maker_long):3d} 銘柄")
    print(f"  ショート受取 (rate>0) : {len(maker_short):3d} 銘柄")
    print(f"  合計                  : {maker_total:3d} 銘柄 / この 1 期間")
    print()

    # ─── TOP 15 ロング受取 ─────────────────────────
    print("── TOP 15 ロング受取 (funding が最も負 → ロング保有で受け取り) ──")
    print(f"  {'銘柄':<10} {'1h rate':>10}  {'8h換算':>10}  {'24h換算':>10}  メモ")
    print(f"  {'-'*60}")
    for r in sorted(rows, key=lambda x: x["rate"])[:15]:
        t = " ◀ taker超" if r["rate"] < -ROUND_TRIP_TAKER else (" ◁ maker超" if r["rate"] < -ROUND_TRIP_MAKER else "")
        print(f"  {r['coin']:<10} {pct(r['rate']):>10}  {pct(r['rate_8h']):>10}  {pct(r['rate_24h']):>10} {t}")

    print()

    # ─── TOP 15 ショート受取 ───────────────────────
    print("── TOP 15 ショート受取 (funding が最も正 → ショート保有で受け取り) ──")
    print(f"  {'銘柄':<10} {'1h rate':>10}  {'8h換算':>10}  {'24h換算':>10}  メモ")
    print(f"  {'-'*60}")
    for r in sorted(rows, key=lambda x: x["rate"], reverse=True)[:15]:
        t = " ◀ taker超" if r["rate"] > ROUND_TRIP_TAKER else (" ◁ maker超" if r["rate"] > ROUND_TRIP_MAKER else "")
        print(f"  {r['coin']:<10} {pct(r['rate']):>10}  {pct(r['rate_8h']):>10}  {pct(r['rate_24h']):>10} {t}")

    print()
    print(f"{'='*65}")
    print("  凡例:")
    print("  ◀ taker超 : 往復 taker 手数料を差し引いても 1 期間でプラス")
    print("  ◁ maker超 : 指値 (maker) なら 1 期間でプラス")
    print("  ※ delta-neutral 戦略想定（現物ロング + perp ショート 等）")
    print(f"{'='*65}")


if __name__ == "__main__":
    main()
