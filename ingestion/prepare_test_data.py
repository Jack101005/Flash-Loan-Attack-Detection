"""
prepare_test_data.py

Reads Etherscan CSV exports and enriches each row by calling
eth_getTransactionByHash on a public Ethereum RPC to get the input calldata.

Usage:
    1. Place your Etherscan CSVs in etherscan_exports/
    2. Run: python prepare_test_data.py
    3. Output: data/test_data.csv

The script auto-detects column names (handles both standard and advanced
Etherscan export formats) and strips whitespace from hashes.
"""

import csv
import json
import time
from pathlib import Path
from urllib import request, error
from collections import Counter

# --- Config ---
RPC_URLS = [
    "https://eth.llamarpc.com",
    "https://rpc.ankr.com/eth",
    "https://cloudflare-eth.com",
    "https://ethereum-rpc.publicnode.com",
    "https://eth.drpc.org",
]

# Edit these to match your file locations
INPUT_FILES = [
    ("etherscan_exports/aav3.csv",     "Aave V3"),
    ("etherscan_exports/balencer.csv", "Balancer V2"),
    ("etherscan_exports/uniswap.csv",  "Uniswap V3"),
]
OUTPUT_FILE = "data/test_data_large.csv"
MAX_PER_FILE = None        # No cap — process every hash in each export file
REQUEST_DELAY_SEC = 0.2    # Be polite to free RPCs


def rpc_call(url: str, method: str, params: list) -> dict | None:
    payload = json.dumps({
        "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
    }).encode("utf-8")
    req = request.Request(url, data=payload, headers={
        "Content-Type": "application/json",
        "User-Agent": "flash-loan-detector/1.0",
    })
    try:
        with request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return None


def fetch_transaction(tx_hash: str, working_url: list) -> dict | None:
    if working_url[0]:
        resp = rpc_call(working_url[0], "eth_getTransactionByHash", [tx_hash])
        if resp and resp.get("result"):
            return resp["result"]
        working_url[0] = None

    for url in RPC_URLS:
        resp = rpc_call(url, "eth_getTransactionByHash", [tx_hash])
        if resp and resp.get("result"):
            working_url[0] = url
            return resp["result"]
    return None


def read_etherscan_hashes(csv_path: Path, max_rows: int | None) -> list[str]:
    """Extract tx hashes from any Etherscan CSV format. max_rows=None reads all rows."""
    hashes = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if max_rows is not None and len(hashes) >= max_rows:
                break
            h = row.get("Transaction Hash") or row.get("Txhash") or ""
            h = h.strip()
            if h:
                hashes.append(h)
    return hashes


def probe_rpcs() -> str | None:
    print("[probe] Testing RPC endpoints...")
    for url in RPC_URLS:
        resp = rpc_call(url, "eth_blockNumber", [])
        if resp and "result" in resp:
            print(f"  OK:   {url}")
            return url
        else:
            print(f"  FAIL: {url}")
    return None


def main():
    Path("data").mkdir(exist_ok=True)

    working = probe_rpcs()
    if not working:
        print(
            "\n[error] All RPC endpoints failed.\n"
            "   Fix: Add your Alchemy key as the first entry in RPC_URLS:\n"
            "         'wss://eth-mainnet.g.alchemy.com/v2/oHvcR-UluOgwiToSONbcy'"
        )
        return

    working_cache = [working]
    all_rows = []
    seen = set()

    fieldnames = ["tx_hash", "from", "to", "input", "value", "gas",
                  "gas_price", "nonce", "block_number", "protocol_label"]

    out_f = open(OUTPUT_FILE, "w", encoding="utf-8", newline="")
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    writer.writeheader()
    out_f.flush()

    try:
        for csv_path_str, label in INPUT_FILES:
            csv_path = Path(csv_path_str)
            if not csv_path.exists():
                print(f"[skip] {csv_path} not found")
                continue

            hashes = read_etherscan_hashes(csv_path, MAX_PER_FILE)
            print(f"\n[{label}] {len(hashes)} hashes from {csv_path.name}")

            for i, tx_hash in enumerate(hashes, 1):
                if tx_hash in seen:
                    continue
                seen.add(tx_hash)

                print(f"  [{i}/{len(hashes)}] {tx_hash[:14]}... ", end="", flush=True)
                tx = fetch_transaction(tx_hash, working_cache)

                if tx is None:
                    print("FAILED")
                    continue

                selector = (tx.get("input") or "0x")[:10]
                print(f"OK  selector={selector}")

                row = {
                    "tx_hash": tx.get("hash", ""),
                    "from": tx.get("from", ""),
                    "to": tx.get("to", ""),
                    "input": tx.get("input", ""),
                    "value": tx.get("value", "0x0"),
                    "gas": tx.get("gas", "0x0"),
                    "gas_price": tx.get("gasPrice", "0x0"),
                    "nonce": tx.get("nonce", "0x0"),
                    "block_number": tx.get("blockNumber") or "",
                    "protocol_label": label,
                }
                all_rows.append(row)
                writer.writerow(row)
                out_f.flush()

                time.sleep(REQUEST_DELAY_SEC)
    finally:
        out_f.close()

    if not all_rows:
        print("\n[error] No rows fetched.")
        return

    print(f"\n{'='*60}")
    print(f"[done] Wrote {len(all_rows)} transactions to {OUTPUT_FILE}")

    selectors = Counter(r["input"][:10] for r in all_rows)
    print(f"\n[selector distribution]")
    for sel, cnt in selectors.most_common():
        print(f"  {sel}  x  {cnt}")

    # Show flash loan vs noise breakdown
    flash_sels = {"0xab9c4b5d", "0x42b0b77c", "0x5c38449e", "0x490e6cbc"}
    flash_count = sum(1 for r in all_rows if r["input"][:10] in flash_sels)
    noise_count = len(all_rows) - flash_count
    print(f"\n  Flash loans: {flash_count}")
    print(f"  Other:       {noise_count}")


if __name__ == "__main__":
    main()