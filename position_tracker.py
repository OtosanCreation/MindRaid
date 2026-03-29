"""
position_tracker.py
デルタニュートラル両建てポジションの手動記録 CLI。

使い方:
  # ポジション開設 (HL short, MEXC long の場合)
  python position_tracker.py open ETH 0.5 HL MEXC \
      --short-price 2000.50 --long-price 2001.20 \
      --short-fee 0.70 --long-fee 0.40 \
      --notes "Phase3テスト開始"

  # ポジション決済
  python position_tracker.py close POS-001 \
      --short-price 1980.00 --long-price 1980.80 \
      --short-fee 0.70 --long-fee 0.40

  # 一覧表示
  python position_tracker.py status
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timezone

DATA_DIR  = os.path.join(os.path.dirname(__file__), "data")
POS_PATH  = os.path.join(DATA_DIR, "positions.csv")

FIELDNAMES = [
    "position_id", "opened_at_utc", "closed_at_utc",
    "coin", "size",
    "short_exchange", "long_exchange",
    "short_entry_price", "long_entry_price",
    "short_entry_fee_usd", "long_entry_fee_usd",
    "short_close_price", "long_close_price",
    "short_close_fee_usd", "long_close_fee_usd",
    "status", "notes",
]


# ── ユーティリティ ────────────────────────────────────────────

def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def read_positions() -> list[dict]:
    if not os.path.exists(POS_PATH):
        return []
    with open(POS_PATH, newline="") as f:
        return list(csv.DictReader(f))


def write_positions(rows: list[dict]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(POS_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)


def next_position_id(rows: list[dict]) -> str:
    nums = []
    for r in rows:
        pid = r.get("position_id", "")
        if pid.startswith("POS-") and pid[4:].isdigit():
            nums.append(int(pid[4:]))
    n = max(nums) + 1 if nums else 1
    return f"POS-{n:03d}"


# ── サブコマンド ───────────────────────────────────────────────

def cmd_open(args):
    rows = read_positions()
    pid  = next_position_id(rows)
    ts   = now_utc()

    # 証拠金計算
    short_notional = float(args.size) * float(args.short_price)
    long_notional  = float(args.size) * float(args.long_price)

    new_row = {
        "position_id":       pid,
        "opened_at_utc":     ts,
        "closed_at_utc":     "",
        "coin":              args.coin.upper(),
        "size":              args.size,
        "short_exchange":    args.short_ex.upper(),
        "long_exchange":     args.long_ex.upper(),
        "short_entry_price": args.short_price,
        "long_entry_price":  args.long_price,
        "short_entry_fee_usd": args.short_fee,
        "long_entry_fee_usd":  args.long_fee,
        "short_close_price": "",
        "long_close_price":  "",
        "short_close_fee_usd": "",
        "long_close_fee_usd":  "",
        "status":            "open",
        "notes":             args.notes or "",
    }
    rows.append(new_row)
    write_positions(rows)

    total_entry_fee = float(args.short_fee) + float(args.long_fee)
    print(f"\n✅ ポジション開設: {pid}")
    print(f"   {args.coin.upper()} {args.size} ETH  ({ts} UTC)")
    print(f"   SHORT: {args.short_ex.upper()}  @ ${float(args.short_price):,.2f}  fee=${float(args.short_fee):.2f}")
    print(f"   LONG : {args.long_ex.upper()}   @ ${float(args.long_price):,.2f}  fee=${float(args.long_fee):.2f}")
    print(f"   想定証拠金: ${(short_notional + long_notional)/2:,.2f}  入場費合計: ${total_entry_fee:.2f}")
    print(f"   → {POS_PATH}\n")


def cmd_close(args):
    rows = read_positions()
    target = None
    for r in rows:
        if r["position_id"] == args.position_id:
            target = r
            break
    if target is None:
        print(f"❌ ポジションが見つかりません: {args.position_id}")
        sys.exit(1)
    if target["status"] == "closed":
        print(f"⚠️  既にクローズ済み: {args.position_id}")
        sys.exit(1)

    ts = now_utc()
    target["closed_at_utc"]     = ts
    target["short_close_price"] = args.short_price
    target["long_close_price"]  = args.long_price
    target["short_close_fee_usd"] = args.short_fee
    target["long_close_fee_usd"]  = args.long_fee
    target["status"]            = "closed"

    # PnL 計算
    size          = float(target["size"])
    short_pnl     = (float(target["short_entry_price"]) - float(args.short_price)) * size
    long_pnl      = (float(args.long_price) - float(target["long_entry_price"])) * size
    total_fees    = (
        float(target["short_entry_fee_usd"]) +
        float(target["long_entry_fee_usd"]) +
        float(args.short_fee) +
        float(args.long_fee)
    )
    price_pnl     = short_pnl + long_pnl
    net_price_pnl = price_pnl - total_fees

    write_positions(rows)

    print(f"\n✅ ポジション決済: {args.position_id}")
    print(f"   決済時刻: {ts} UTC")
    print(f"   SHORT PnL: ${short_pnl:+.2f}  LONG PnL: ${long_pnl:+.2f}")
    print(f"   価格差PnL: ${price_pnl:+.2f}  手数料合計: -${total_fees:.2f}")
    print(f"   価格差 net PnL: ${net_price_pnl:+.2f}")
    print(f"   ※ FR収益は pnl_log.csv を参照\n")


def cmd_status(args):
    rows = read_positions()
    if not rows:
        print("ポジションなし")
        return

    open_rows   = [r for r in rows if r["status"] == "open"]
    closed_rows = [r for r in rows if r["status"] == "closed"]

    print("\n" + "=" * 65)
    print(f"  MindRaid Phase 3 — ポジション一覧   {now_utc()} UTC")
    print("=" * 65)

    if open_rows:
        print(f"\n  [オープン中] {len(open_rows)}件")
        print(f"  {'ID':<10} {'コイン':<5} {'サイズ':<7} {'SHORT':<6} {'LONG':<6} {'開設日時'}")
        print("  " + "-" * 58)
        for r in open_rows:
            print(f"  {r['position_id']:<10} {r['coin']:<5} {r['size']:<7} "
                  f"{r['short_exchange']:<6} {r['long_exchange']:<6} {r['opened_at_utc']}")

    if closed_rows:
        print(f"\n  [決済済み] {len(closed_rows)}件")
        print(f"  {'ID':<10} {'コイン':<5} {'サイズ':<7} 開設日時              決済日時")
        print("  " + "-" * 58)
        for r in closed_rows:
            print(f"  {r['position_id']:<10} {r['coin']:<5} {r['size']:<7} "
                  f"{r['opened_at_utc']}  {r['closed_at_utc']}")

    print("=" * 65 + "\n")


# ── エントリーポイント ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="デルタニュートラルポジション管理")
    sub    = parser.add_subparsers(dest="cmd", required=True)

    # open
    p_open = sub.add_parser("open", help="ポジション開設")
    p_open.add_argument("coin",      help="銘柄 (例: ETH)")
    p_open.add_argument("size",      type=float, help="サイズ (ETH換算, 例: 0.5)")
    p_open.add_argument("short_ex",  help="SHORT取引所 (例: HL)")
    p_open.add_argument("long_ex",   help="LONG取引所 (例: MEXC)")
    p_open.add_argument("--short-price", required=True, type=float, dest="short_price")
    p_open.add_argument("--long-price",  required=True, type=float, dest="long_price")
    p_open.add_argument("--short-fee",   required=True, type=float, dest="short_fee",
                        help="入場手数料 USD (片道)")
    p_open.add_argument("--long-fee",    required=True, type=float, dest="long_fee",
                        help="入場手数料 USD (片道)")
    p_open.add_argument("--notes", default="", help="メモ")

    # close
    p_close = sub.add_parser("close", help="ポジション決済")
    p_close.add_argument("position_id", help="ポジションID (例: POS-001)")
    p_close.add_argument("--short-price", required=True, type=float, dest="short_price")
    p_close.add_argument("--long-price",  required=True, type=float, dest="long_price")
    p_close.add_argument("--short-fee",   required=True, type=float, dest="short_fee")
    p_close.add_argument("--long-fee",    required=True, type=float, dest="long_fee")

    # status
    sub.add_parser("status", help="ポジション一覧")

    args = parser.parse_args()

    if args.cmd == "open":
        cmd_open(args)
    elif args.cmd == "close":
        cmd_close(args)
    elif args.cmd == "status":
        cmd_status(args)


if __name__ == "__main__":
    main()
