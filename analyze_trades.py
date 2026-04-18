"""
analyze_trades.py
data/trades.csv を読んで FB 用の集計を出す。
- 全体: N、平均 net、中央値、勝率、合計 net
- entry_net_fr バケット別: bucket, N, 平均 net, 勝率
- 保有時間バケット別: bucket, N, 平均 net, 勝率
- exit_reason 別
- 直近 N 件
"""
import csv
import os
import statistics
from collections import defaultdict

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
TRADES_CSV = os.path.join(DATA_DIR, "trades.csv")


def load_trades():
    if not os.path.exists(TRADES_CSV):
        return []
    with open(TRADES_CSV) as f:
        return list(csv.DictReader(f))


def to_float(s, default=0.0):
    try:
        return float(s) if s not in ("", None) else default
    except ValueError:
        return default


def summarize(trades, label):
    if not trades:
        print(f"  {label}: 0件")
        return
    nets = [to_float(t["est_net_usd"]) for t in trades if t["est_net_usd"] != ""]
    if not nets:
        print(f"  {label}: N={len(trades)}  (net未記録)")
        return
    win = sum(1 for n in nets if n > 0)
    print(f"  {label}: N={len(nets)}  平均net=${statistics.mean(nets):+.3f}  "
          f"中央値=${statistics.median(nets):+.3f}  勝率={win/len(nets)*100:.1f}%  "
          f"合計=${sum(nets):+.2f}")


def bucket_entry_net_fr(v):
    v = to_float(v) * 100  # %/h
    if v < 0.05: return "<0.05%"
    if v < 0.10: return "0.05-0.10%"
    if v < 0.20: return "0.10-0.20%"
    if v < 0.50: return "0.20-0.50%"
    return ">0.50%"


def bucket_duration(v):
    v = to_float(v)
    if v < 1: return "<1h"
    if v < 4: return "1-4h"
    if v < 12: return "4-12h"
    if v < 24: return "12-24h"
    return ">24h"


def main():
    trades = load_trades()
    print(f"\n=== Trade 集計 ({TRADES_CSV}) ===")
    print(f"総トレード: {len(trades)}件\n")

    if not trades:
        print("データなし。trades.csv が空または存在しません。")
        return

    print("[全体]")
    summarize(trades, "ALL")
    print()

    print("[entry_net_fr_1h バケット別]")
    by_fr = defaultdict(list)
    for t in trades:
        by_fr[bucket_entry_net_fr(t["entry_net_fr_1h"])].append(t)
    for b in ["<0.05%", "0.05-0.10%", "0.10-0.20%", "0.20-0.50%", ">0.50%"]:
        summarize(by_fr[b], b)
    print()

    print("[保有時間バケット別]")
    by_dur = defaultdict(list)
    for t in trades:
        by_dur[bucket_duration(t["duration_h"])].append(t)
    for b in ["<1h", "1-4h", "4-12h", "12-24h", ">24h"]:
        summarize(by_dur[b], b)
    print()

    print("[exit_reason 別]")
    by_reason = defaultdict(list)
    for t in trades:
        by_reason[t.get("exit_reason", "")].append(t)
    for r, items in by_reason.items():
        summarize(items, r or "(空)")
    print()

    print("[直近10件]")
    for t in trades[-10:]:
        dur = to_float(t["duration_h"])
        net = to_float(t["est_net_usd"])
        act = to_float(t.get("actual_total_funding_usd", ""))
        efr = to_float(t["entry_net_fr_1h"]) * 100
        act_str = f" 実funding=${act:+.3f}" if t.get("actual_total_funding_usd") else ""
        print(f"  {t['opened_at_utc']:<20} {t['coin']:<8} {t['direction']:<10} "
              f"entry_net={efr:+.4f}%/h  dur={dur:.1f}h  est_net=${net:+.3f}{act_str}  ({t['exit_reason']})")

    # 推定精度（est_funding vs actual_funding）
    pairs = [(to_float(t["est_funding_usd"]), to_float(t.get("actual_total_funding_usd","")))
             for t in trades if t.get("actual_total_funding_usd") and t["est_funding_usd"]]
    if pairs:
        diffs = [a - e for e, a in pairs]
        print(f"\n[推定精度] N={len(pairs)}  est→actual 差分 平均=${statistics.mean(diffs):+.4f}  "
              f"中央値=${statistics.median(diffs):+.4f}")


if __name__ == "__main__":
    main()
