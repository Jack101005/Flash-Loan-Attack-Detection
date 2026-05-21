"""
enrich_test_data.py — One-time script to add block_number and block_timestamp
to data/test_data.csv by querying the Alchemy RPC for each tx_hash.

Output: data/test_data_enriched.csv

Usage:
    python ingestion/enrich_test_data.py
    python ingestion/enrich_test_data.py --input data/test_data.csv --output data/test_data_enriched.csv

Requires: ALCHEMY_RPC_URL in .env (or environment)
"""

import argparse
import csv
import os
import sys
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

RPC_URL = os.getenv("ALCHEMY_RPC_URL")
if not RPC_URL:
    print("[error] ALCHEMY_RPC_URL not set. Add it to .env")
    sys.exit(1)


def rpc_call(method: str, params: list) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    resp = requests.post(RPC_URL, json=payload, timeout=15)
    resp.raise_for_status()
    return resp.json()


def get_block_info(tx_hash: str) -> tuple[int | None, int | None]:
    """
    Returns (block_number, block_timestamp) for a tx_hash.
    Returns (None, None) if not found or an error occurs.
    """
    try:
        tx_resp = rpc_call("eth_getTransactionByHash", [tx_hash])
        tx_data = tx_resp.get("result")
        if not tx_data or tx_data.get("blockNumber") is None:
            return None, None

        block_number = int(tx_data["blockNumber"], 16)

        block_resp = rpc_call("eth_getBlockByNumber", [tx_data["blockNumber"], False])
        block_data = block_resp.get("result")
        if not block_data:
            return block_number, None

        block_timestamp = int(block_data["timestamp"], 16)
        return block_number, block_timestamp

    except Exception as e:
        print(f"  [error] {tx_hash[:14]}...: {e}")
        return None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",  default="data/test_data.csv")
    parser.add_argument("--output", default="data/test_data_enriched.csv")
    args = parser.parse_args()

    root = Path(__file__).parent.parent
    input_path  = root / args.input
    output_path = root / args.output

    if not input_path.exists():
        print(f"[error] Input file not found: {input_path}")
        sys.exit(1)

    rows = []
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    out_fields = fieldnames + ["block_number", "block_timestamp"]
    enriched = []
    total = len(rows)

    print(f"[enrich] Processing {total} transactions from {input_path.name}...")
    print(f"[enrich] RPC: {RPC_URL[:40]}...")

    for i, row in enumerate(rows, 1):
        tx_hash = row["tx_hash"].strip()
        print(f"  [{i}/{total}] {tx_hash[:20]}...", end=" ", flush=True)

        block_number, block_timestamp = get_block_info(tx_hash)

        row["block_number"]    = block_number if block_number is not None else ""
        row["block_timestamp"] = block_timestamp if block_timestamp is not None else ""

        if block_timestamp:
            print(f"block={block_number} ts={block_timestamp} ✓")
        else:
            print("not found (pending or non-existent)")

        enriched.append(row)
        time.sleep(0.1)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(enriched)

    found = sum(1 for r in enriched if r["block_timestamp"])
    print(f"\n[enrich] Done. {found}/{total} transactions enriched → {output_path}")


if __name__ == "__main__":
    main()
