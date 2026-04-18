"""
lighter_client.py — Lighter DEX クライアント（HL×Lighter FR Arb 用）

【設計方針】
- Lighter SDK は async だが、taker_bot.py との互換性のため
  全関数を同期ラッパーとして公開する（asyncio.run() でラップ）
- 失敗時はすべて None を返し、Telegram にエラー通知を送る
- 3 回リトライ（1 秒間隔）

【必要な .env 変数】
  LIGHTER_ETH_PRIVATE_KEY, LIGHTER_API_PRIVATE_KEY, LIGHTER_ACCOUNT_INDEX
"""

import os
import time
import json
import asyncio
import logging
from typing import Optional, Dict, List, Tuple

import lighter
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── 設定 ────────────────────────────────────────────────────────────────────
LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
RETRY_COUNT = 3
RETRY_INTERVAL = 1.0   # 秒

MARKETS_CACHE_PATH = os.path.join(os.path.dirname(__file__), "data", "lighter_markets.json")

# ─── Telegram 通知（循環 import を避けるためインライン実装）────────────────────
def _send_telegram_error(msg: str):
    try:
        import requests
        token = os.getenv("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
        if token and chat_id:
            requests.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                json={"chat_id": chat_id, "text": f"[LighterClient ERROR]\n{msg}"},
                timeout=5,
            )
    except Exception:
        pass


# ─── 内部ヘルパー ─────────────────────────────────────────────────────────────
def _get_account_index() -> int:
    return int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))


def _get_api_private_key() -> str:
    key = os.getenv("LIGHTER_API_PRIVATE_KEY", "")
    if not key:
        raise ValueError("LIGHTER_API_PRIVATE_KEY が .env に設定されていません")
    return key


def _get_eth_private_key() -> str:
    key = os.getenv("LIGHTER_ETH_PRIVATE_KEY", "")
    if not key:
        raise ValueError("LIGHTER_ETH_PRIVATE_KEY が .env に設定されていません")
    if not key.startswith("0x"):
        key = "0x" + key
    return key


def _run(coro):
    """asyncio コルーチンを同期的に実行するヘルパー。"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, coro)
                return future.result()
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        return asyncio.run(coro)


def _retry(func, *args, **kwargs):
    """最大 RETRY_COUNT 回リトライする。最終的に失敗したら None を返す。"""
    last_err = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            last_err = e
            logger.warning(f"[LighterClient] 試行 {attempt}/{RETRY_COUNT} 失敗: {e}")
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_INTERVAL)
    msg = f"全 {RETRY_COUNT} 回失敗: {last_err}"
    logger.error(f"[LighterClient] {msg}")
    _send_telegram_error(msg)
    return None


# ─── マーケット情報キャッシュ ─────────────────────────────────────────────────
_markets_cache: Optional[Dict[str, dict]] = None  # symbol → {market_id, ...}


async def _fetch_markets_async() -> Dict[str, dict]:
    """Lighter のパープ市場一覧を取得して symbol → market_id の dict を返す。"""
    cfg = lighter.Configuration(host=LIGHTER_URL)
    async with lighter.ApiClient(configuration=cfg) as api_client:
        order_api = lighter.OrderApi(api_client)
        books = await order_api.order_books()
    result = {}
    for ob in books.order_books:
        if ob.market_type == "perp" and ob.status == "active":
            result[ob.symbol] = {
                "market_id": ob.market_id,
                "min_base_amount": float(ob.min_base_amount),
                "supported_size_decimals": ob.supported_size_decimals,
            }
    return result


def get_markets() -> Optional[Dict[str, dict]]:
    """パープ市場一覧を返す（キャッシュあり）。"""
    global _markets_cache
    if _markets_cache is not None:
        return _markets_cache

    # キャッシュファイルから読む
    if os.path.exists(MARKETS_CACHE_PATH):
        try:
            with open(MARKETS_CACHE_PATH, "r") as f:
                _markets_cache = json.load(f)
            return _markets_cache
        except Exception:
            pass

    # API から取得
    result = _retry(lambda: _run(_fetch_markets_async()))
    if result is not None:
        _markets_cache = result
        os.makedirs(os.path.dirname(MARKETS_CACHE_PATH), exist_ok=True)
        with open(MARKETS_CACHE_PATH, "w") as f:
            json.dump(result, f, indent=2)
    return result


def get_market_id(symbol: str) -> Optional[int]:
    """シンボル名 (例: 'ETH') から market_id を返す。見つからなければ None。"""
    markets = get_markets()
    if markets is None:
        return None
    info = markets.get(symbol)
    return info["market_id"] if info else None


# ─── パブリック API ────────────────────────────────────────────────────────────

def test_connection() -> bool:
    """
    Lighter API への接続テスト（認証不要）。
    成功時 True、失敗時 False。
    """
    async def _test():
        cfg = lighter.Configuration(host=LIGHTER_URL)
        async with lighter.ApiClient(configuration=cfg) as api_client:
            root_api = lighter.RootApi(api_client)
            status = await root_api.status()
            return status.status == 200

    try:
        result = _run(_test())
        if result:
            logger.info("[LighterClient] test_connection: OK")
        else:
            logger.error("[LighterClient] test_connection: 異常ステータス")
        return bool(result)
    except Exception as e:
        msg = f"test_connection 失敗: {e}"
        logger.error(f"[LighterClient] {msg}")
        _send_telegram_error(msg)
        return False


def get_balance() -> Optional[float]:
    """
    Lighter の USDC 証拠金残高（コラテラル）を返す。
    失敗時 None。
    """
    async def _get():
        cfg = lighter.Configuration(host=LIGHTER_URL)
        async with lighter.ApiClient(configuration=cfg) as api_client:
            account_api = lighter.AccountApi(api_client)
            result = await account_api.account(
                by="index",
                value=str(_get_account_index()),
            )
            return float(result.accounts[0].collateral)

    return _retry(lambda: _run(_get()))


def get_funding_rates() -> Optional[Dict[str, float]]:
    """
    Lighter のパープ FR を取得する。
    戻り値: { "ETH": 0.0001, "BTC": -0.00005, ... } （1 時間あたりレート）
    失敗時 None。
    """
    async def _get():
        cfg = lighter.Configuration(host=LIGHTER_URL)
        async with lighter.ApiClient(configuration=cfg) as api_client:
            funding_api = lighter.FundingApi(api_client)
            rates = await funding_api.funding_rates()
        result = {}
        for r in rates.funding_rates:
            if r.exchange == "lighter":
                result[r.symbol] = float(r.rate)
        return result

    return _retry(lambda: _run(_get()))


def get_positions() -> Optional[List[dict]]:
    """
    オープン中のポジション一覧を返す。
    戻り値: [
        {
            "symbol": "ETH",
            "market_id": 3,
            "side": "long",          # "long" or "short"
            "size": 0.05,            # コイン数
            "entry_price": 1800.0,
            "unrealized_pnl": 1.23,
            "position_value": 90.0,
        },
        ...
    ]
    失敗時 None。
    """
    async def _get():
        cfg = lighter.Configuration(host=LIGHTER_URL)
        async with lighter.ApiClient(configuration=cfg) as api_client:
            account_api = lighter.AccountApi(api_client)
            result = await account_api.account(
                by="index",
                value=str(_get_account_index()),
            )
            positions = []
            for p in result.accounts[0].positions:
                size = abs(float(p.position))
                if size <= 0:
                    continue  # 0枚のゴーストポジションをスキップ
                positions.append({
                    "symbol": p.symbol,
                    "market_id": p.market_id,
                    "side": "long" if p.sign >= 0 else "short",
                    "size": size,
                    "entry_price": float(p.avg_entry_price),
                    "unrealized_pnl": float(p.unrealized_pnl),
                    "position_value": float(p.position_value),
                })
            return positions

    return _retry(lambda: _run(_get()))


def place_order(
    symbol: str,
    side: str,           # "buy" or "sell"
    size_usd: float,
    max_slippage: float = 0.003,  # 0.3%
) -> Optional[dict]:
    """
    成行（Market）注文を発注する。
    size_usd を現在の best price で換算してコイン数を計算。

    戻り値: {"tx_hash": "...", "symbol": "ETH", "side": "buy", "size_coin": 0.05}
    失敗時 None。
    """
    async def _place():
        market_id = get_market_id(symbol)
        if market_id is None:
            raise ValueError(f"マーケットが見つかりません: {symbol}")

        markets = get_markets()
        min_base = markets[symbol]["min_base_amount"]
        decimals = markets[symbol]["supported_size_decimals"]

        is_ask = (side == "sell")

        api_priv = _get_api_private_key()
        client = lighter.SignerClient(
            url=LIGHTER_URL,
            account_index=_get_account_index(),
            api_private_keys={0: api_priv},
        )

        # best price を取得して USD → コイン数を計算
        best_price = await client.get_best_price(market_index=market_id, is_ask=is_ask)
        price_float = best_price / 1e6   # Lighter は price を 1e6 スケールで保持

        size_coin = size_usd / price_float
        size_coin = max(size_coin, min_base)
        size_coin = round(size_coin, decimals)

        # base_amount は整数コイン数（empirical: *10**decimals を掛けると10倍オーバー）
        # max_slippage は小数（SDK source: ideal_price * (1 + max_slippage * sign)）
        base_amount_int = max(int(round(size_coin)), 1)

        # client_order_index は時刻ベースのユニーク ID
        client_order_index = int(time.time() * 1000) % (2**31)

        _, resp, err = await client.create_market_order_limited_slippage(
            market_index=market_id,
            client_order_index=client_order_index,
            base_amount=base_amount_int,
            max_slippage=max_slippage,
            is_ask=is_ask,
        )
        if err:
            raise Exception(f"create_market_order 失敗: {err}")

        return {
            "tx_hash": resp.tx_hash if resp else None,
            "symbol": symbol,
            "side": side,
            "size_coin": size_coin,
            "entry_price": price_float,
        }

    return _retry(lambda: _run(_place()))


def close_position(symbol: str, side: str, size_coin: float) -> Optional[dict]:
    """
    ポジションを成行でクローズする（reduce_only=True）。
    side: クローズしたいポジションの向き（"long" を閉じるなら "sell"、"short" を閉じるなら "buy"）

    戻り値: {"tx_hash": "...", "symbol": "ETH"}
    失敗時 None。
    """
    async def _close():
        market_id = get_market_id(symbol)
        if market_id is None:
            raise ValueError(f"マーケットが見つかりません: {symbol}")

        markets = get_markets()
        decimals = markets[symbol]["supported_size_decimals"]

        is_ask = (side == "sell")

        api_priv = _get_api_private_key()
        client = lighter.SignerClient(
            url=LIGHTER_URL,
            account_index=_get_account_index(),
            api_private_keys={0: api_priv},
        )

        client_order_index = int(time.time() * 1000) % (2**31)

        base_amount_int = max(int(round(size_coin)), 1)

        _, resp, err = await client.create_market_order_limited_slippage(
            market_index=market_id,
            client_order_index=client_order_index,
            base_amount=base_amount_int,
            max_slippage=0.005,  # 0.5% 小数で渡す
            is_ask=is_ask,
            reduce_only=True,
        )
        if err:
            raise Exception(f"close_position 失敗: {err}")

        return {
            "tx_hash": resp.tx_hash if resp else None,
            "symbol": symbol,
        }

    return _retry(lambda: _run(_close()))


def force_close_position(symbol: str) -> Optional[dict]:
    """
    ポジションを確認して成行でクローズする（緊急用）。
    ポジションが存在しない場合は None を返す。
    """
    positions = get_positions()
    if positions is None:
        _send_telegram_error(f"force_close_position: get_positions 失敗 ({symbol})")
        return None

    target = next((p for p in positions if p["symbol"] == symbol), None)
    if target is None:
        logger.info(f"[LighterClient] force_close_position: {symbol} のポジションなし")
        return None

    close_side = "sell" if target["side"] == "long" else "buy"
    return close_position(
        symbol=symbol,
        side=close_side,
        size_coin=target["size"],
    )


# ─── スタンドアロン実行（接続テスト）─────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("=" * 50)
    print("Lighter クライアント 接続テスト")
    print("=" * 50)

    # 1. 接続テスト
    print("\n[1] test_connection()")
    ok = test_connection()
    print(f"     結果: {'✅ OK' if ok else '❌ NG'}")

    # 2. 残高確認
    print("\n[2] get_balance()")
    balance = get_balance()
    print(f"     残高: {balance} USDC" if balance is not None else "     残高取得失敗")

    # 3. FR 確認（HL と共通銘柄数）
    print("\n[3] get_funding_rates()")
    rates = get_funding_rates()
    if rates:
        print(f"     Lighter パープ FR 銘柄数: {len(rates)}")
        # HL の主要銘柄との共通銘柄確認
        hl_major = {"BTC", "ETH", "SOL", "AVAX", "LINK", "ARB", "OP", "MATIC", "DOGE", "PEPE"}
        common = hl_major & set(rates.keys())
        print(f"     HL 主要銘柄との共通: {sorted(common)}")
        # FR サンプル
        sample = list(rates.items())[:5]
        print(f"     FR サンプル (1h): {sample}")
    else:
        print("     FR 取得失敗")

    # 4. ポジション確認
    print("\n[4] get_positions()")
    positions = get_positions()
    if positions is not None:
        if positions:
            for p in positions:
                print(f"     {p['symbol']}: {p['side']} {p['size']} @ {p['entry_price']}")
        else:
            print("     オープンポジションなし")
    else:
        print("     ポジション取得失敗")

    # 5. マーケット一覧確認
    print("\n[5] get_markets()")
    markets = get_markets()
    if markets:
        print(f"     パープ市場数: {len(markets)}")
    else:
        print("     マーケット取得失敗")

    print("\n" + "=" * 50)
    print("テスト完了")
