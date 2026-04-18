"""
dry_run_test.py
価格スケーリング／base_amount スケーリングが正しいか、実発注せず検証する。
複数銘柄で best_price → price_float → size_coin → base_amount_int を計算し、
期待値と比較する。
"""
import asyncio
import os
import lighter
from dotenv import load_dotenv

load_dotenv()

LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"
SIZE_USD = 100.0

SYMBOLS = ["ETH", "BTC", "XMR", "YZY", "KAITO", "EIGEN", "STABLE", "IP"]


async def main():
    import lighter_client as lc
    markets = lc.get_markets()
    api_priv = lc._get_api_private_key()
    acct = lc._get_account_index()

    client = lighter.SignerClient(
        url=LIGHTER_URL,
        account_index=acct,
        api_private_keys={0: api_priv},
    )

    print(f"{'SYM':<10} {'size_dec':<10} {'price_dec':<10} {'raw_bid':<14} {'raw_ask':<14} {'bid_float':<14} {'ask_float':<14} {'size_coin':<14} {'base_amt_int':<14}")
    print("=" * 140)
    for sym in SYMBOLS:
        if sym not in markets:
            print(f"{sym}: not on Lighter")
            continue
        m = markets[sym]
        mid = m["market_id"]
        sd = m["supported_size_decimals"]
        pd = m["supported_price_decimals"]

        try:
            raw_bid = await client.get_best_price(market_index=mid, is_ask=False)
            raw_ask = await client.get_best_price(market_index=mid, is_ask=True)
        except Exception as e:
            print(f"{sym}: price fetch failed: {e}")
            continue

        bid_float = raw_bid / (10 ** pd)
        ask_float = raw_ask / (10 ** pd)
        mid_px = (bid_float + ask_float) / 2

        size_coin = SIZE_USD / mid_px
        min_base = m["min_base_amount"]
        size_coin = max(size_coin, min_base)
        size_coin = round(size_coin, sd)
        base_amount_int = max(int(round(size_coin * (10 ** sd))), 1)

        # SDK が解釈する実コイン数
        actual_coins = base_amount_int / (10 ** sd)
        actual_usd = actual_coins * mid_px

        print(f"{sym:<10} {sd:<10} {pd:<10} {raw_bid:<14} {raw_ask:<14} {bid_float:<14.6f} {ask_float:<14.6f} {size_coin:<14.6f} {base_amount_int:<14}  → actual {actual_coins:.4f} coin = ${actual_usd:.2f}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
