"""
phase3_report.py
Phase 3 の累積 PnL・理論値 vs 実績・手数料分析をターミナルに表示する。
オプションで Telegram にも送信する。

使い方:
  python phase3_report.py            # ターミナル表示のみ
  python phase3_report.py --telegram  # Telegram にも送信
"""

import argparse
import csv
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

DATA_DIR  = os.path.join(os.path.dirname(__file__), "data")
POS_PATH  = os.path.join(DATA_DIR, "positions.csv")
PNL_PATH  = os.path.join(DATA_DIR, "pnl_log.csv")


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
    for key in ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]:
        if key not in env and os.environ.get(key):
            env[key] = os.environ[key]
    return env


# ── データ読み込み ────────────────────────────────────────────

def read_csv(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


# ── レポート生成 ──────────────────────────────────────────────

def build_report(positions: list[dict], pnl_logs: list[dict]) -> str:
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    open_pos   = [p for p in positions if p["status"] == "open"]
    closed_pos = [p for p in positions if p["status"] == "closed"]

    lines = []
    lines.append("=" * 62)
    lines.append(f"  MindRaid Phase 3 レポート  {now_str}")
    lines.append("=" * 62)

    # ── ポジションサマリー
    lines.append(f"\n  ポジション: オープン {len(open_pos)}件 / 決済済み {len(closed_pos)}件\n")

    if open_pos:
        lines.append(f"  {'ID':<10} {'コイン':<5} {'サイズ':<7} {'SHORT':<6} {'LONG':<6} 開設日時")
        lines.append("  " + "-" * 55)
        for p in open_pos:
            opened = p["opened_at_utc"]
            # 経過時間
            try:
                dt = datetime.strptime(opened, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=timezone.utc)
                elapsed_h = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
                elapsed_str = f"({elapsed_h:.1f}h経過)"
            except Exception:
                elapsed_str = ""
            lines.append(f"  {p['position_id']:<10} {p['coin']:<5} {p['size']:<7} "
                         f"{p['short_exchange']:<6} {p['long_exchange']:<6} {opened} {elapsed_str}")

    # ── PnL 集計
    if not pnl_logs:
        lines.append("\n  PnLデータなし (pnl_logger.py を実行してください)")
    else:
        # ポジション別集計 (最新レコードを使用)
        by_pos: dict[str, list[dict]] = {}
        for row in pnl_logs:
            pid = row["position_id"]
            by_pos.setdefault(pid, []).append(row)

        lines.append(f"\n  {'ID':<10} {'コイン':<5} {'実FR (USD)':<14} "
                     f"{'理論値 (USD)':<14} {'精度':<8} {'APY':<8} {'Net PnL'}")
        lines.append("  " + "-" * 62)

        total_actual      = 0.0
        total_theoretical = 0.0
        total_net_pnl     = 0.0
        total_fees        = 0.0

        for pid, rows in by_pos.items():
            # 累積実FR / 理論値
            cum_actual      = sum(float(r["total_funding_actual_usd"])      for r in rows)
            cum_theoretical = sum(float(r["total_funding_theoretical_usd"]) for r in rows if r["total_funding_theoretical_usd"])
            cum_net_pnl     = sum(float(r["net_pnl_usd"])                   for r in rows)
            cum_fees        = sum(float(r["entry_fees_usd"])                 for r in rows[:1])  # 入場手数料は1回
            last            = rows[-1]
            coin            = last["coin"]
            size            = last["size"]
            apy             = float(last["annualized_apy_pct"]) if last["annualized_apy_pct"] else 0.0
            acc             = (cum_actual / cum_theoretical * 100) if cum_theoretical else None
            acc_str         = f"{acc:.1f}%" if acc is not None else "N/A"

            lines.append(f"  {pid:<10} {coin:<5} ${cum_actual:<13.4f} "
                         f"${cum_theoretical:<13.4f} {acc_str:<8} {apy:.2f}%   ${cum_net_pnl:+.4f}")

            total_actual      += cum_actual
            total_theoretical += cum_theoretical
            total_net_pnl     += cum_net_pnl
            total_fees        += cum_fees

        lines.append("  " + "-" * 62)
        total_acc = (total_actual / total_theoretical * 100) if total_theoretical else None
        total_acc_str = f"{total_acc:.1f}%" if total_acc is not None else "N/A"
        lines.append(f"  {'合計':<16} ${total_actual:<13.4f} ${total_theoretical:<13.4f} "
                     f"{total_acc_str:<8}          ${total_net_pnl:+.4f}")

        # ── 費用・スリッページ分析
        lines.append(f"\n  手数料合計 (入場): ${total_fees:.4f} USD")
        total_slip = sum(float(r["slippage_usd"]) for r in pnl_logs[:len(by_pos)])
        lines.append(f"  スリッページ推定:  ${total_slip:.4f} USD")

        # ── 収益 breakdown
        lines.append(f"\n  収益ブレークダウン:")
        lines.append(f"    FR収益 (実績):    ${total_actual:+.4f}")
        lines.append(f"    FR収益 (理論):    ${total_theoretical:+.4f}")
        lines.append(f"    理論 vs 実績:     {total_acc_str}")
        lines.append(f"    手数料:           -${total_fees:.4f}")
        lines.append(f"    Net PnL:          ${total_net_pnl:+.4f}")

    lines.append("\n" + "=" * 62)
    lines.append("  ※ 投資助言ではありません / 実験・検証目的")
    lines.append("=" * 62 + "\n")

    return "\n".join(lines)


# ── Telegram 送信 ─────────────────────────────────────────────

def send_telegram(token: str, chat_id: str, text: str) -> None:
    # Telegram は 4096文字制限 → 必要なら分割
    max_len = 4000
    chunks = [text[i:i+max_len] for i in range(0, len(text), max_len)]
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    for chunk in chunks:
        data = urllib.parse.urlencode({
            "chat_id":    chat_id,
            "text":       f"<pre>{chunk}</pre>",
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(url, data=data)
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            if not result.get("ok"):
                print(f"❌ Telegram エラー: {result}")


# ── エントリーポイント ─────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Phase 3 PnL レポート")
    parser.add_argument("--telegram", action="store_true", help="Telegram にも送信")
    args = parser.parse_args()

    positions = read_csv(POS_PATH)
    pnl_logs  = read_csv(PNL_PATH)

    report = build_report(positions, pnl_logs)
    print(report)

    if args.telegram:
        env = load_env()
        token   = env.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = env.get("TELEGRAM_CHAT_ID", "")
        if not token or not chat_id:
            print("❌ TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID が未設定")
            return
        send_telegram(token, chat_id, report)
        print("✅ Telegram 送信完了")


if __name__ == "__main__":
    main()
