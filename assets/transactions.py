import os
from datetime import datetime, timezone
from typing import List, Dict

import pandas as pd
from dagster import asset, AssetExecutionContext, MaterializeResult, MetadataValue
from web3 import Web3
from dotenv import load_dotenv

# For local dev: load .env (in prod, rely on real env vars)
load_dotenv()

def _get_w3() -> Web3:
    rpc_url = os.environ["ETHEREUM_RPC_URL"]  # raises KeyError if missing → fail fast
    return Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))

@asset(
    group_name="ethereum",
    name="ethereum_transactions_recent",
    compute_kind="Web3.py",
    description="Fetch transactions from the last N blocks and write them to Parquet.",
)
def ethereum_transactions_recent(context: AssetExecutionContext) -> MaterializeResult:
    """
    Learning-friendly asset:
      - Reads ETHEREUM_RPC_URL from env
      - Fetches last N blocks (N from ETH_TX_RECENT_BLOCKS, default=10)
      - Writes Parquet to data/ethereum/transactions/
      - Emits handy metadata (counts, preview, path)
    """
    w3 = _get_w3()

    n_blocks = int(os.getenv("ETH_TX_RECENT_BLOCKS", "10"))
    latest_block = w3.eth.block_number
    start_block = max(0, latest_block - n_blocks + 1)

    context.log.info(f"Fetching transactions from block {start_block} to {latest_block}…")

    rows: List[Dict] = []

    for block_num in range(start_block, latest_block + 1):
        block = w3.eth.get_block(block_num, full_transactions=True)
        ts_iso = datetime.fromtimestamp(block.timestamp, tz=timezone.utc).isoformat()

        for tx in block.transactions:
            # tx is AttributeDict; getattr handles EIP-1559 fields missing on old txs
            rows.append(
                {
                    "block_number": block_num,
                    "block_timestamp": ts_iso,
                    "hash": tx.hash.hex(),
                    "from": tx["from"],
                    "to": tx.to,
                    "value_wei": int(tx.value),
                    "gas": int(tx.gas),
                    "max_fee_per_gas": getattr(tx, "maxFeePerGas", None),
                    "max_priority_fee_per_gas": getattr(tx, "maxPriorityFeePerGas", None),
                    "nonce": int(tx.nonce),
                    "transaction_index": int(tx.transactionIndex),
                }
            )

        if block_num % 5 == 0 or block_num == latest_block:
            context.log.info(f"Processed up to block {block_num} (rows: {len(rows)})")

    # Save
    df = pd.DataFrame(rows)
    os.makedirs("data/ethereum/transactions", exist_ok=True)
    out_path = f"data/ethereum/transactions/tx_recent_until_{latest_block}.parquet"
    df.to_parquet(out_path, index=False)

    # Show a tiny preview table in Dagster UI
    preview_md = "(no rows)" if df.empty else df.head(10).to_markdown()

    return MaterializeResult(
        metadata={
            "blocks_scanned": MetadataValue.int(n_blocks),
            "latest_block": MetadataValue.int(latest_block),
            "num_transactions": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(preview_md),
            "file": MetadataValue.path(out_path),
        }
    )