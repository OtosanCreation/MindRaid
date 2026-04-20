"""
emergency_close_all.py — 全オープンポジション強制クローズ用の one-shot スクリプト

【背景】
Lighter CSV の単位バグ（8h rate を 1h 列名で保存）が判明し、Phase 6 のエントリー
条件と BE 推定が実態と乖離していたため、全ポジションを一度クローズして
再起動する方針。

【安全機構】
デフォルトでは DRY RUN モードで動き、**API コールは一切行わない**。
実際にクローズしたいときだけ、ユーザーが手動でファイル末尾の
  EXECUTE_CLOSE = False
を
  EXECUTE_CLOSE = True
に変更してから `python3 emergency_close_all.py` を再実行する。

【使い方】
1) まず launchctl で taker_bot のスケジューラを止める（README 参照）
2) `python3 emergency_close_all.py` で DRY RUN し、対象ポジを確認
3) 問題なければ EXECUTE_CLOSE = True に書き換えて再実行
4) クローズ完了後、Lighter & HL の残ポジを mobile / Web で最終確認

【禁止】
- Claude / bot 側から自動実行しないこと（金銭移動は必ずユーザー手動）
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone

# === DRY RUN ガード ========================================================
# デフォルト False。True に書き換えてから再実行すると実際にクローズする。
EXECUTE_CLOSE = False
# ===========================================================================

HERE = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(HERE, "data", "taker_state.json")


def banner(title: str) -> None:
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def load_positions() -> dict:
    if not os.path.exists(STATE_FILE):
        print(f"[FATAL] state file not found: {STATE_FILE}")
        sys.exit(1)
    with open(STATE_FILE) as f:
        state = json.load(f)
    return state.get("positions", {})


def dry_run_preview(positions: dict) -> None:
    banner("DRY RUN: クローズ対象ポジション一覧")
    if not positions:
        print("  (オープンポジションなし)")
        return
    for coin, pos in positions.items():
        exch = pos.get("exchange", "mexc")
        direction = pos.get("direction", "?")
        size_usd = pos.get("size_usd", 0)
        hl_sz = pos.get("hl_size_coin", 0)
        ctr_sz = pos.get("counter_size_coin", 0)
        opened = pos.get("opened_at", "?")
        # direction = "short_fr" → HL は SHORT、Lighter は LONG
        # direction = "long_fr"  → HL は LONG、 Lighter は SHORT
        hl_side = "SHORT" if direction == "short_fr" else "LONG"
        ctr_side = "LONG" if direction == "short_fr" else "SHORT"
        print(
            f"  {coin:5s}  dir={direction:9s} exchange={exch:7s} "
            f"size_usd={size_usd:>6.1f}  opened={opened}"
        )
        print(f"         HL leg  : close {hl_side} size_coin={hl_sz}")
        print(f"         {exch:7s}: close {ctr_side} size_coin={ctr_sz}")
    print()


def close_one(coin: str, pos: dict, exchange, info, sz_decimals_map, main_addr):
    """1 ポジション（HL + Counter）を閉じる。EXECUTE_CLOSE=True のときのみ呼ぶ。"""
    import lighter_client
    import taker_bot as tb

    direction = pos.get("direction", "short_fr")
    exch_name = pos.get("exchange", "lighter")

    print(f"\n  >>> {coin} close start (dir={direction}, exchange={exch_name})")

    # --- HL side -----------------------------------------------------------
    try:
        if direction == "short_fr":
            hl_res = tb.hl_close_short(exchange, coin)
        else:
            hl_res = tb.hl_close_long(exchange, coin)
        print(f"      HL close OK:  {hl_res}")
    except Exception as e:
        print(f"      HL close FAILED: {e}  → force close フォールバック")
        try:
            hl_res = tb.hl_force_close(exchange, info, coin, main_addr, sz_decimals_map)
            print(f"      HL force close OK: {hl_res}")
        except Exception as e2:
            print(f"      [FATAL] HL force close も失敗: {e2}")

    # 少し待って Lighter 側へ（fill 反映安定のため）
    time.sleep(1.0)

    # --- Counter (Lighter) side -------------------------------------------
    try:
        if exch_name == "lighter":
            res = lighter_client.force_close_position(symbol=coin)
            print(f"      Lighter close OK: {res}")
        else:
            print(f"      [SKIP] exchange={exch_name} (MEXC 経路は手動対応)")
    except Exception as e:
        print(f"      [FATAL] Lighter close 失敗: {e}")


def execute_close(positions: dict) -> None:
    banner("LIVE CLOSE: API コールを実行します")
    # 遅延 import（DRY RUN 時には HL / Lighter SDK を起動しない）
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass

    HL_PRIVATE_KEY = os.environ.get("HL_PRIVATE_KEY", "")
    HL_WALLET_ADDRESS = os.environ.get("HL_WALLET_ADDRESS", "")
    if not HL_PRIVATE_KEY:
        print("[FATAL] HL_PRIVATE_KEY が .env に設定されていません")
        sys.exit(1)

    wallet = Account.from_key(HL_PRIVATE_KEY)
    info = Info(skip_ws=True)
    main_addr = HL_WALLET_ADDRESS or wallet.address
    exchange = Exchange(wallet, account_address=main_addr)

    import taker_bot as tb
    sz_decimals_map = tb.get_sz_decimals(info)

    for coin, pos in positions.items():
        close_one(coin, pos, exchange, info, sz_decimals_map, main_addr)
        time.sleep(2.0)  # 連続発注を避ける

    banner("CLOSE 完了 — 最終確認を mobile / Web で行ってください")


def main() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts} UTC] emergency_close_all.py")
    print(f"          EXECUTE_CLOSE = {EXECUTE_CLOSE}")
    print()

    positions = load_positions()
    dry_run_preview(positions)

    if not EXECUTE_CLOSE:
        print("[DRY RUN ONLY] 実際にクローズするには、このファイルの")
        print("  EXECUTE_CLOSE = False")
        print("を True に書き換えてから再実行してください。")
        print()
        print("チェックリスト（実行前）:")
        print("  [ ] launchctl で taker_bot スケジューラを止めた")
        print("      例:  launchctl unload ~/Library/LaunchAgents/com.mindraid.*.plist")
        print("  [ ] .env に HL_PRIVATE_KEY / LIGHTER_* が設定されている")
        print("  [ ] HL / Lighter どちらも手動で残高と API 鍵を確認済み")
        print("  [ ] 対象ポジション数: 4（上記リストと一致？）")
        return

    # === LIVE CLOSE (ユーザーが手動で EXECUTE_CLOSE = True に書き換え済み) ===
    print("[LIVE] 5 秒後にクローズ処理を開始します。中断は Ctrl-C。")
    for i in range(5, 0, -1):
        print(f"  ...{i}")
        time.sleep(1)
    execute_close(positions)


if __name__ == "__main__":
    main()
