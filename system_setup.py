"""
system_setup.py — Lighter API キー初回登録スクリプト

【使用タイミング】
- 初回セットアップ時
- API キーを再生成したいとき（セキュリティリセット等）

【必要な .env 変数】
- LIGHTER_ETH_PRIVATE_KEY: Lighter UI の Export Key で取得した秘密鍵
- LIGHTER_ACCOUNT_INDEX: 722838（固定）

【実行方法】
  python system_setup.py

【完了後】
.env の LIGHTER_API_PRIVATE_KEY が更新されます。
"""

import os
import sys
import asyncio
import lighter
from dotenv import load_dotenv

load_dotenv()

LIGHTER_URL = "https://mainnet.zklighter.elliot.ai"


def update_env_value(key: str, value: str):
    """
    .env ファイルの特定キーの値を上書きする。
    """
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        print(f"[ERROR] .env ファイルが見つかりません: {env_path}")
        return False

    with open(env_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    updated = False
    new_lines = []
    for line in lines:
        if line.startswith(f"{key}="):
            new_lines.append(f"{key}={value}\n")
            updated = True
        else:
            new_lines.append(line)

    if not updated:
        new_lines.append(f"{key}={value}\n")

    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(new_lines)

    return True


def log_key_rotation(pubkey: str, tx_hash: str):
    """鍵 rotation を logs/key_rotations.log に追記（監査用）。"""
    import datetime
    log_dir = os.path.join(os.path.dirname(__file__), "logs")
    os.makedirs(log_dir, exist_ok=True)
    path = os.path.join(log_dir, "key_rotations.log")
    ts = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"{ts} UTC  pubkey={pubkey}  tx={tx_hash}\n")


def check_already_valid() -> bool:
    """既存の .env の鍵がオンチェーン鍵と一致していれば True。"""
    try:
        import lighter_client
        err = lighter_client.check_signer_valid()
        return err is None
    except Exception:
        return False


async def setup():
    # Idempotency: 既に一致していれば rotation しない（誤発動で .env と chain が乖離するのを防ぐ）
    force = "--force" in sys.argv
    if not force and check_already_valid():
        print("[SKIP] .env の LIGHTER_API_PRIVATE_KEY は既にオンチェーン登録鍵と一致しています。")
        print("       強制再登録したい場合は:  python system_setup.py --force")
        return

    eth_private_key = os.getenv("LIGHTER_ETH_PRIVATE_KEY", "")
    account_index_str = os.getenv("LIGHTER_ACCOUNT_INDEX", "")

    if not eth_private_key:
        print("[ERROR] LIGHTER_ETH_PRIVATE_KEY が .env に設定されていません")
        sys.exit(1)

    if not eth_private_key.startswith("0x"):
        eth_private_key = "0x" + eth_private_key

    if not account_index_str:
        print("[ERROR] LIGHTER_ACCOUNT_INDEX が .env に設定されていません")
        sys.exit(1)

    account_index = int(account_index_str)
    print(f"[INFO] Account Index: {account_index}")
    print(f"[INFO] ETH Key: {eth_private_key[:8]}...{eth_private_key[-6:]}")

    # Step 1: 新しい API キーペアを生成
    print("\n[1/3] 新しい Lighter API キーペアを生成中...")
    api_private_key, api_public_key, err = lighter.create_api_key()
    if err:
        print(f"[ERROR] API キー生成失敗: {err}")
        sys.exit(1)
    print(f"      Public Key: {api_public_key}")
    print(f"      Private Key: {api_private_key[:10]}...{api_private_key[-6:]}")

    # Step 2: 未登録のキーで一時的な SignerClient を作成して change_api_key を実行
    print("\n[2/3] ETH 署名で API キーをオンチェーン登録中...")
    try:
        client = lighter.SignerClient(
            url=LIGHTER_URL,
            account_index=account_index,
            api_private_keys={0: api_private_key},
        )
        result, err = await client.change_api_key(
            eth_private_key=eth_private_key,
            new_pubkey=api_public_key,
            api_key_index=0,
        )
        if err:
            print(f"[ERROR] change_api_key 失敗: {err}")
            sys.exit(1)
        print(f"      TX Hash: {result.tx_hash}")
        print(f"      Code: {result.code}")
        log_key_rotation(api_public_key, result.tx_hash)
    except Exception as e:
        print(f"[ERROR] 例外発生: {e}")
        sys.exit(1)

    # Step 3: .env に書き込み
    print("\n[3/3] .env を更新中...")
    ok = update_env_value("LIGHTER_API_PRIVATE_KEY", api_private_key)
    if ok:
        print("      LIGHTER_API_PRIVATE_KEY を .env に保存しました ✅")
    else:
        print("[WARN] .env の自動更新に失敗しました。手動で以下を設定してください:")
        print(f"       LIGHTER_API_PRIVATE_KEY={api_private_key}")

    print("\n✅ セットアップ完了！")
    print("   次のステップ: python lighter_client.py を実行して接続確認")


if __name__ == "__main__":
    asyncio.run(setup())
