"""
export_detections.py — Export detected flash loans to CSV

Runs the same two-pass filter + ABI decoding logic as listener.py,
but reads directly from data/test_data.csv (no WebSocket needed)
and writes results to data/detected_flash_loans.csv.

This CSV serves as the handoff artifact for Person 3 (Backend Logic)
so they can test their decoding, graph construction, and cycle detection
without needing Kafka or the listener running.

The output schema matches the raw_txns Kafka message format from the
technical spec (Section 1.1), plus decoded fields.

Usage:
    python export_detections.py
    python export_detections.py --input data/test_data.csv --output data/detected_flash_loans.csv
"""

import csv
import json
import os
import sys
import argparse
import time

from web3 import Web3

# Add ingestion/ to path for config imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "ingestion"))

from config import WATCHLIST, SELECTORS


# ──────────────────────────────────────────────────────────────
# ABI loading
# ──────────────────────────────────────────────────────────────
def load_abi(filename):
    for path in [
        os.path.join(SCRIPT_DIR, "abis", filename),
        os.path.join(SCRIPT_DIR, "ingestion", "abis", filename),
    ]:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    return []


w3 = Web3()

AAVE_ABI = load_abi("aave_v3_pool.json") or [
    {"name": "flashLoan", "type": "function", "inputs": [
        {"name": "receiverAddress", "type": "address"},
        {"name": "assets", "type": "address[]"},
        {"name": "amounts", "type": "uint256[]"},
        {"name": "interestRateModes", "type": "uint256[]"},
        {"name": "onBehalfOf", "type": "address"},
        {"name": "params", "type": "bytes"},
        {"name": "referralCode", "type": "uint16"},
    ], "outputs": []},
    {"name": "flashLoanSimple", "type": "function", "inputs": [
        {"name": "receiverAddress", "type": "address"},
        {"name": "asset", "type": "address"},
        {"name": "amount", "type": "uint256"},
        {"name": "params", "type": "bytes"},
        {"name": "referralCode", "type": "uint16"},
    ], "outputs": []},
]

BALANCER_ABI = load_abi("balancer_v2_vault.json") or [
    {"name": "flashLoan", "type": "function", "inputs": [
        {"name": "recipient", "type": "address"},
        {"name": "tokens", "type": "address[]"},
        {"name": "amounts", "type": "uint256[]"},
        {"name": "userData", "type": "bytes"},
    ], "outputs": []},
]

UNISWAP_ABI = load_abi("uniswap_v3_pool.json") or [
    {"name": "flash", "type": "function", "inputs": [
        {"name": "recipient", "type": "address"},
        {"name": "amount0", "type": "uint256"},
        {"name": "amount1", "type": "uint256"},
        {"name": "data", "type": "bytes"},
    ], "outputs": []},
]

aave_contract = w3.eth.contract(abi=AAVE_ABI)
balancer_contract = w3.eth.contract(abi=BALANCER_ABI)
uniswap_contract = w3.eth.contract(abi=UNISWAP_ABI)

DECODERS = {
    "Aave V3 flashLoan":       aave_contract,
    "Aave V3 flashLoanSimple": aave_contract,
    "Balancer V2 flashLoan":   balancer_contract,
    "Uniswap V3 flash":        uniswap_contract,
}


# ──────────────────────────────────────────────────────────────
# Main export logic
# ──────────────────────────────────────────────────────────────
def process_transactions(input_path: str, output_path: str):
    # Read input
    with open(input_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"[export] Read {len(rows)} transactions from {input_path}")

    # Output fields — matches raw_txns Kafka schema + decoded fields
    output_fields = [
        "tx_hash",
        "from",
        "to",
        "input",
        "value",
        "gas",
        "gas_price",
        "timestamp",
        "source",
        # Detection metadata
        "protocol",
        "selector",
        "function_name",
        # Decoded parameters (JSON strings for flexibility)
        "decoded_assets",
        "decoded_amounts",
        "decoded_modes",
        "decode_success",
    ]

    detected = []
    ignored = 0

    for row in rows:
        tx_hash = row.get("tx_hash", "").strip()
        to_addr = row.get("to", "").strip().lower()
        input_hex = row.get("input", "0x").strip()
        selector = input_hex[:10]

        # Pass 1: Contract address
        if to_addr not in WATCHLIST:
            ignored += 1
            continue

        # Pass 2: Function selector
        if selector not in SELECTORS:
            ignored += 1
            continue

        protocol = SELECTORS[selector]
        pool = WATCHLIST[to_addr]

        # Build base output (Kafka message schema)
        out = {
            "tx_hash": tx_hash,
            "from": row.get("from", ""),
            "to": row.get("to", ""),
            "input": input_hex,
            "value": row.get("value", "0x0"),
            "gas": row.get("gas", "0x0"),
            "gas_price": row.get("gas_price", "0x0"),
            "timestamp": time.time(),
            "source": "ethereum_mainnet",
            "protocol": protocol,
            "selector": selector,
            "function_name": "",
            "decoded_assets": "",
            "decoded_amounts": "",
            "decoded_modes": "",
            "decode_success": "false",
        }

        # Decode parameters
        decoder = DECODERS.get(protocol)
        if decoder:
            try:
                func, decoded = decoder.decode_function_input(input_hex)
                out["function_name"] = func.fn_name
                out["decode_success"] = "true"

                if "assets" in decoded:
                    # Aave V3 flashLoan (multi-asset)
                    out["decoded_assets"] = json.dumps(list(decoded["assets"]))
                    out["decoded_amounts"] = json.dumps(
                        [str(a) for a in decoded["amounts"]]
                    )
                    if "interestRateModes" in decoded:
                        out["decoded_modes"] = json.dumps(
                            [int(m) for m in decoded["interestRateModes"]]
                        )
                elif "asset" in decoded:
                    # Aave V3 flashLoanSimple
                    out["decoded_assets"] = json.dumps([decoded["asset"]])
                    out["decoded_amounts"] = json.dumps([str(decoded["amount"])])
                elif "tokens" in decoded:
                    # Balancer V2 flashLoan
                    out["decoded_assets"] = json.dumps(list(decoded["tokens"]))
                    out["decoded_amounts"] = json.dumps(
                        [str(a) for a in decoded["amounts"]]
                    )
                elif "amount0" in decoded:
                    # Uniswap V3 flash
                    out["decoded_assets"] = json.dumps(["token0", "token1"])
                    out["decoded_amounts"] = json.dumps(
                        [str(decoded["amount0"]), str(decoded["amount1"])]
                    )

            except Exception as e:
                out["decode_success"] = "false"
                out["function_name"] = f"DECODE_ERROR: {e}"

        detected.append(out)

    # Write output
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(detected)

    # Summary
    print(f"\n{'='*60}")
    print(f"  Export Summary")
    print(f"{'='*60}")
    print(f"  Total transactions:     {len(rows)}")
    print(f"  Ignored (filtered):     {ignored}")
    print(f"  Flash loans detected:   {len(detected)}")

    decode_ok = sum(1 for d in detected if d["decode_success"] == "true")
    decode_fail = len(detected) - decode_ok
    print(f"  Decoded successfully:   {decode_ok}")
    print(f"  Decode failed:          {decode_fail}")

    # Protocol breakdown
    from collections import Counter
    protocols = Counter(d["protocol"] for d in detected)
    print(f"\n  By protocol:")
    for proto, cnt in protocols.most_common():
        print(f"    {proto:30s}  x {cnt}")

    # Show a few decoded examples
    decoded_examples = [d for d in detected if d["decode_success"] == "true"]
    if decoded_examples:
        print(f"\n  Sample decoded detections:")
        for d in decoded_examples[:3]:
            print(f"    {d['function_name']:20s}  "
                  f"assets={d['decoded_assets'][:60]}  "
                  f"amounts={d['decoded_amounts'][:40]}")

    print(f"\n  Output written to: {output_path}")
    print(f"{'='*60}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export detected flash loans to CSV for Person 3"
    )
    parser.add_argument(
        "--input", default="data/test_data.csv",
        help="Input CSV (default: data/test_data.csv)"
    )
    parser.add_argument(
        "--output", default="data/detected_flash_loans.csv",
        help="Output CSV (default: data/detected_flash_loans.csv)"
    )
    args = parser.parse_args()

    process_transactions(args.input, args.output)