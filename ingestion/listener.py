"""
listener.py — Flash Loan Mempool Listener (Stage 1: Ingestion)

Subscribes to newPendingTransactions via WebSocket, applies two-pass
filtering (contract address + function selector), decodes flash loan
parameters, and prints structured detection output.

Features:
  - Two-pass filter: contract address + function selector
  - ABI decoding for Aave V3, Balancer V2, Uniswap V3
  - In-memory deduplication buffer
  - Auto-reconnect with exponential backoff (max 5 retries)
  - Session statistics

Works identically against:
  - Mock server:  ws://localhost:8765   (for development/testing)
  - Alchemy:      wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

Usage:
    python listener.py                          # default: ws://localhost:8765
    python listener.py --url wss://your-rpc     # custom endpoint
    python listener.py --max-retries 10         # custom retry limit

Requires: pip install web3 websockets
"""

import asyncio
import json
import time
import argparse
import os
from web3 import AsyncWeb3, WebSocketProvider
from web3.exceptions import TransactionNotFound
from config import WATCHLIST, SELECTORS


# ──────────────────────────────────────────────────────────────
# ABI loading — from abis/ directory or inline fallback
# ──────────────────────────────────────────────────────────────
def load_abi(filename: str) -> list:
    """
    Load ABI from abis/ directory. Checks two locations:
      1. Next to this script:  ingestion/abis/<filename>
      2. Project root:         abis/<filename>
    Returns empty list if not found in either location.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, "abis", filename),          # ingestion/abis/
        os.path.join(script_dir, "..", "abis", filename),     # project root abis/
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r") as f:
                abi = json.load(f)
                print(f"[listener] Loaded ABI: {path}")
                return abi

    print(f"[listener] Warning: {filename} not found in abis/, using inline ABI")
    return []


# Load ABIs from files (preferred) or use inline fallback
AAVE_V3_ABI = load_abi("aave_v3_pool.json") or [
    {
        "name": "flashLoan", "type": "function",
        "inputs": [
            {"name": "receiverAddress", "type": "address"},
            {"name": "assets", "type": "address[]"},
            {"name": "amounts", "type": "uint256[]"},
            {"name": "interestRateModes", "type": "uint256[]"},
            {"name": "onBehalfOf", "type": "address"},
            {"name": "params", "type": "bytes"},
            {"name": "referralCode", "type": "uint16"},
        ], "outputs": [],
    },
    {
        "name": "flashLoanSimple", "type": "function",
        "inputs": [
            {"name": "receiverAddress", "type": "address"},
            {"name": "asset", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "params", "type": "bytes"},
            {"name": "referralCode", "type": "uint16"},
        ], "outputs": [],
    },
]

BALANCER_V2_ABI = load_abi("balancer_v2_vault.json") or [
    {
        "name": "flashLoan", "type": "function",
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "tokens", "type": "address[]"},
            {"name": "amounts", "type": "uint256[]"},
            {"name": "userData", "type": "bytes"},
        ], "outputs": [],
    },
]

UNISWAP_V3_ABI = load_abi("uniswap_v3_pool.json") or [
    {
        "name": "flash", "type": "function",
        "inputs": [
            {"name": "recipient", "type": "address"},
            {"name": "amount0", "type": "uint256"},
            {"name": "amount1", "type": "uint256"},
            {"name": "data", "type": "bytes"},
        ], "outputs": [],
    },
]


# ──────────────────────────────────────────────────────────────
# Stats tracking
# ──────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total_seen = 0
        self.filtered_address = 0
        self.filtered_selector = 0
        self.detected = 0
        self.decode_errors = 0
        self.duplicates = 0
        self.reconnections = 0
        self.start_time = time.time()

    def summary(self):
        elapsed = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"  Session Summary ({elapsed:.1f}s)")
        print(f"{'='*60}")
        print(f"  Total tx hashes received:   {self.total_seen}")
        print(f"  Passed address filter:      {self.filtered_address}")
        print(f"  Passed selector filter:     {self.filtered_selector}")
        print(f"  Flash loans detected:       {self.detected}")
        print(f"  Decode errors:              {self.decode_errors}")
        print(f"  Duplicates skipped:         {self.duplicates}")
        print(f"  Reconnections:              {self.reconnections}")
        print(f"{'='*60}\n")


# ──────────────────────────────────────────────────────────────
# Core listener session — one WebSocket connection lifecycle
# ──────────────────────────────────────────────────────────────
async def _run_session(wss_url: str, stats: Stats, seen_hashes: set):
    """
    Run a single WebSocket session. Returns normally on connection
    drop (so the caller can reconnect). Raises KeyboardInterrupt
    on Ctrl+C.
    """
    DEDUP_MAX_SIZE = 10_000

    async with AsyncWeb3(WebSocketProvider(wss_url)) as w3:
        # Build contract objects for ABI decoding
        aave_contract = w3.eth.contract(abi=AAVE_V3_ABI)
        balancer_contract = w3.eth.contract(abi=BALANCER_V2_ABI)
        uniswap_contract = w3.eth.contract(abi=UNISWAP_V3_ABI)

        # Map protocol names to their decoder contracts
        decoders = {
            "Aave V3 flashLoan":       aave_contract,
            "Aave V3 flashLoanSimple": aave_contract,
            "Balancer V2 flashLoan":   balancer_contract,
            "Uniswap V3 flash":        uniswap_contract,
        }

        await w3.eth.subscribe("newPendingTransactions")
        print(f"[listener] Connected to {wss_url}")
        print(f"[listener] Watching {len(WATCHLIST)} contracts, {len(SELECTORS)} selectors")
        print(f"[listener] Listening for flash loans...\n")

        async for msg in w3.socket.process_subscriptions():
            tx_hash = msg["result"]
            stats.total_seen += 1

            # --- Deduplication ---
            tx_hash_str = tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)
            if tx_hash_str in seen_hashes:
                stats.duplicates += 1
                continue
            seen_hashes.add(tx_hash_str)
            if len(seen_hashes) > DEDUP_MAX_SIZE:
                seen_hashes.clear()

            # --- Fetch full transaction ---
            try:
                tx = await w3.eth.get_transaction(tx_hash)
            except TransactionNotFound:
                continue
            except Exception:
                continue

            if not tx or not tx.get("to"):
                continue

            # --- Filter 1: Contract address ---
            to_addr = tx["to"].lower()
            if to_addr not in WATCHLIST:
                continue
            stats.filtered_address += 1
            protocol_pool = WATCHLIST[to_addr]

            # --- Filter 2: Function selector ---
            input_data = tx.get("input", "0x")
            if isinstance(input_data, bytes):
                input_hex = "0x" + input_data.hex()
            else:
                input_hex = str(input_data)

            selector = input_hex[:10]
            if selector not in SELECTORS:
                continue
            stats.filtered_selector += 1

            protocol_method = SELECTORS[selector]
            stats.detected += 1

            # --- Build output (per technical spec) ---
            out_data = {
                "tx_hash": tx_hash_str,
                "from": tx.get("from", ""),
                "to": tx["to"],
                "input": input_hex,
                "value": str(tx.get("value", 0)),
                "gas": str(tx.get("gas", 0)),
                "gas_price": str(tx.get("gasPrice", 0)),
                "timestamp": time.time(),
                "source": "ethereum_mainnet",
            }

            print(f"{'!'*60}")
            print(f"  FLASH LOAN DETECTED  #{stats.detected}")
            print(f"{'!'*60}")
            print(f"  Protocol:  {protocol_method}")
            print(f"  Pool:      {protocol_pool}")
            print(f"  Tx Hash:   {tx_hash_str}")
            print(f"  From:      {tx.get('from', 'unknown')}")
            print(f"  Selector:  {selector}")

            # --- Decode flash loan parameters ---
            decoder = decoders.get(protocol_method)
            if decoder:
                try:
                    func, decoded = decoder.decode_function_input(input_hex)
                    if "assets" in decoded:
                        print(f"  Function:  {func.fn_name}")
                        print(f"  Assets:    {decoded['assets']}")
                        print(f"  Amounts:   {decoded['amounts']}")
                        if "interestRateModes" in decoded:
                            print(f"  Modes:     {decoded['interestRateModes']}")
                    elif "asset" in decoded:
                        print(f"  Function:  {func.fn_name}")
                        print(f"  Asset:     {decoded['asset']}")
                        print(f"  Amount:    {decoded['amount']}")
                    elif "tokens" in decoded:
                        print(f"  Function:  {func.fn_name}")
                        print(f"  Tokens:    {decoded['tokens']}")
                        print(f"  Amounts:   {decoded['amounts']}")
                    elif "amount0" in decoded:
                        print(f"  Function:  {func.fn_name}")
                        print(f"  Amount0:   {decoded['amount0']}")
                        print(f"  Amount1:   {decoded['amount1']}")
                except Exception as e:
                    stats.decode_errors += 1
                    print(f"  Decode:    FAILED ({e})")

            print()

            # TODO: Kafka producer — produce out_data to topic 'raw_txns'
            # producer.produce('raw_txns', key=tx_hash_str, value=json.dumps(out_data))


# ──────────────────────────────────────────────────────────────
# Reconnect wrapper with exponential backoff
# ──────────────────────────────────────────────────────────────
async def log_mempool(wss_url: str, max_retries: int = 5):
    """
    Run the listener with automatic reconnection on WebSocket drops.

    Reconnect strategy (per PDF spec Section 1.1):
      - Exponential backoff: 1s, 2s, 4s, 8s, 16s
      - Max retries: 5 (configurable)
      - On successful reconnect: retry counter resets to 0
      - After max retries exhausted: alert and exit

    Gap timestamps are logged so any missed transactions during
    disconnection can be identified later.
    """
    stats = Stats()
    seen_hashes: set = set()
    retry_count = 0
    BASE_DELAY = 1.0  # Starting backoff delay in seconds

    while True:
        prev_seen = stats.total_seen

        try:
            await _run_session(wss_url, stats, seen_hashes)

            # _run_session returned normally — server closed cleanly.
            # In production, this could be Alchemy dropping the connection
            # gracefully, so we should still reconnect.
            gap_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"\n[listener] Server closed connection at {gap_time}")

            # If we processed messages, this was a working connection
            if stats.total_seen > prev_seen:
                retry_count = 0

        except KeyboardInterrupt:
            print("\n[listener] Stopped by user.")
            break

        except Exception as e:
            # Connection dropped unexpectedly
            gap_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            print(f"\n[listener] Connection lost at {gap_time}: "
                  f"{type(e).__name__}: {e}")

            # If we processed messages before the drop, reset retry counter
            if stats.total_seen > prev_seen:
                retry_count = 0

        # --- Reconnect with exponential backoff ---
        retry_count += 1
        stats.reconnections += 1

        if retry_count > max_retries:
            print(f"[listener] ALERT: Max retries ({max_retries}) exhausted. "
                  f"Exiting.")
            print(f"[listener] Transactions may have been missed during gaps.")
            break

        # Exponential backoff: 1s, 2s, 4s, 8s, 16s, ...
        delay = BASE_DELAY * (2 ** (retry_count - 1))
        print(f"[listener] Reconnecting in {delay:.0f}s "
              f"(attempt {retry_count}/{max_retries})...")

        try:
            await asyncio.sleep(delay)
        except KeyboardInterrupt:
            print("\n[listener] Stopped by user during reconnect wait.")
            break

        print(f"[listener] Attempting reconnection...")

    stats.summary()


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Flash Loan Mempool Listener")
    parser.add_argument(
        "--url",
        default="ws://localhost:8765",
        help="WebSocket RPC URL (default: ws://localhost:8765 for mock server)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=5,
        help="Max reconnection attempts before exiting (default: 5)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(log_mempool(args.url, args.max_retries))
    except KeyboardInterrupt:
        print("\n[listener] Stopped by user.")