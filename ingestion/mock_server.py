"""
mock_server.py — Mock Ethereum Node (JSON-RPC 2.0 over WebSocket)

Simulates an Alchemy/Infura WebSocket endpoint so that listener.py
(using web3.py's AsyncWeb3 + WebSocketProvider) works identically
to production — just change the URL.

Protocol flow:
  1. Client sends:  eth_subscribe("newPendingTransactions")
     Server replies: {"jsonrpc":"2.0","id":N,"result":"0xSUB_ID"}

  2. Server pushes:  {"jsonrpc":"2.0","method":"eth_subscription",
                      "params":{"subscription":"0xSUB_ID","result":"0xTX_HASH"}}
     (one per transaction from test_data.csv, with a delay between each)

  3. Client sends:  eth_getTransactionByHash("0xTX_HASH")
     Server replies: {"jsonrpc":"2.0","id":N,"result":{...full tx object...}}

Usage:
    python mock_server.py                      # default: data/test_data.csv
    python mock_server.py --data my_data.csv   # custom data file
    python mock_server.py --delay 1.0          # 1 second between txs
    python mock_server.py --loop               # repeat dataset forever

Requires: pip install websockets
"""

import asyncio
import argparse
import csv
import json
import secrets
import sys
from pathlib import Path

try:
    import websockets
except ImportError:
    print("Error: websockets not installed. Run: pip install websockets")
    sys.exit(1)


def load_transactions(csv_path: str) -> tuple[dict, dict]:
    """
    Load test_data_enriched.csv (or test_data.csv as fallback) into:
      - txns: {tx_hash: full tx object} for O(1) lookup
      - block_timestamps: {block_number_int: unix_timestamp_int}
    """
    txns = {}
    block_timestamps = {}
    path = Path(csv_path)
    if not path.exists():
        print(f"[error] {csv_path} not found!")
        sys.exit(1)

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tx_hash = row["tx_hash"].strip()
            if not tx_hash or not tx_hash.startswith("0x"):
                continue

            block_number_raw = row.get("block_number", "").strip()
            block_ts_raw     = row.get("block_timestamp", "").strip()

            block_number_int = int(block_number_raw) if block_number_raw else None
            block_ts_int     = int(block_ts_raw)     if block_ts_raw     else None
            block_number_hex = hex(block_number_int) if block_number_int else None

            if block_number_int and block_ts_int:
                block_timestamps[block_number_int] = block_ts_int

            # Build a transaction object matching what eth_getTransactionByHash returns
            txns[tx_hash] = {
                "hash": tx_hash,
                "from": row.get("from", "0x" + "0" * 40),
                "to": row.get("to", "0x" + "0" * 40),
                "input": row.get("input", "0x"),
                "value": row.get("value", "0x0"),
                "gas": row.get("gas", "0x7a120"),
                "gasPrice": row.get("gas_price", "0x6fc23ac00"),
                "nonce": row.get("nonce", "0x0"),
                "blockHash": ("0x" + "0" * 64) if block_number_hex else None,
                "blockNumber": block_number_hex,
                "transactionIndex": "0x0" if block_number_hex else None,
                "type": "0x2",
                "chainId": "0x1",
                "v": "0x0",
                "r": "0x" + "0" * 64,
                "s": "0x" + "0" * 64,
                "maxFeePerGas": row.get("gas_price", "0x6fc23ac00"),
                "maxPriorityFeePerGas": "0x59682f00",
            }

    enriched_count = sum(1 for t in txns.values() if t["blockNumber"])
    print(f"[mock] Loaded {len(txns)} transactions ({enriched_count} with block timestamps) from {csv_path}")
    return txns, block_timestamps


class MockEthereumNode:
    """Handles JSON-RPC 2.0 requests and subscription management."""

    def __init__(self, transactions: dict, block_timestamps: dict, delay: float, loop: bool):
        self.transactions = transactions
        self.block_timestamps = block_timestamps  # {block_number_int: unix_timestamp_int}
        self.tx_hashes = list(transactions.keys())
        self.delay = delay
        self.loop = loop
        self.subscriptions = {}  # ws -> {sub_id: sub_type}

    async def handle_connection(self, websocket):
        """Handle a single WebSocket client connection."""
        client_id = id(websocket)
        self.subscriptions[client_id] = {}
        print(f"[mock] Client connected (id={client_id})")

        # We need two concurrent tasks:
        # 1. Listen for incoming JSON-RPC requests (eth_subscribe, eth_getTransactionByHash)
        # 2. Push subscription notifications once subscribed
        push_task = None

        try:
            async for raw_msg in websocket:
                try:
                    msg = json.loads(raw_msg)
                except json.JSONDecodeError:
                    print(f"[mock] Invalid JSON from client: {raw_msg[:100]}")
                    continue

                method = msg.get("method", "")
                req_id = msg.get("id")
                params = msg.get("params", [])

                if method == "eth_subscribe":
                    response, sub_id = self._handle_subscribe(req_id, params, client_id)
                    await websocket.send(json.dumps(response))
                    print(f"[mock] Subscribed: {params[0]} -> sub_id={sub_id}")

                    # Start pushing transactions
                    if push_task is None or push_task.done():
                        push_task = asyncio.create_task(
                            self._push_transactions(websocket, sub_id)
                        )

                elif method == "eth_unsubscribe":
                    response = self._handle_unsubscribe(req_id, params, client_id)
                    await websocket.send(json.dumps(response))

                elif method == "eth_getTransactionByHash":
                    response = self._handle_get_transaction(req_id, params)
                    await websocket.send(json.dumps(response))

                elif method == "eth_chainId":
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0", "id": req_id, "result": "0x1"
                    }))

                elif method == "eth_getBlockByNumber":
                    response = self._handle_get_block(req_id, params)
                    await websocket.send(json.dumps(response))

                elif method == "eth_blockNumber":
                    latest = max(self.block_timestamps.keys(), default=25000000)
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0", "id": req_id, "result": hex(latest)
                    }))

                elif method == "net_version":
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0", "id": req_id, "result": "1"
                    }))

                elif method == "web3_clientVersion":
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0", "id": req_id,
                        "result": "MockEthNode/v1.0.0/flash-loan-detector"
                    }))

                else:
                    # Unknown method — return empty result (don't error)
                    print(f"[mock] Unknown method: {method}")
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0", "id": req_id, "result": None
                    }))

        except websockets.exceptions.ConnectionClosed:
            print(f"[mock] Client disconnected (id={client_id})")
        finally:
            if push_task and not push_task.done():
                push_task.cancel()
            self.subscriptions.pop(client_id, None)

    def _handle_subscribe(self, req_id, params, client_id):
        """Handle eth_subscribe — return subscription ID."""
        sub_type = params[0] if params else "newPendingTransactions"
        sub_id = "0x" + secrets.token_hex(16)
        self.subscriptions[client_id][sub_id] = sub_type

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": sub_id,
        }, sub_id

    def _handle_unsubscribe(self, req_id, params, client_id):
        """Handle eth_unsubscribe."""
        sub_id = params[0] if params else ""
        removed = sub_id in self.subscriptions.get(client_id, {})
        if removed:
            del self.subscriptions[client_id][sub_id]
        return {"jsonrpc": "2.0", "id": req_id, "result": removed}

    def _handle_get_block(self, req_id, params):
        """Handle eth_getBlockByNumber — return block with timestamp."""
        block_number_hex = params[0] if params else "0x0"
        try:
            block_number_int = int(block_number_hex, 16)
        except ValueError:
            block_number_int = 0

        timestamp = self.block_timestamps.get(block_number_int, 0)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "number": block_number_hex,
                "timestamp": hex(timestamp) if timestamp else "0x0",
                "hash": "0x" + "0" * 64,
                "parentHash": "0x" + "0" * 64,
                "transactions": [],
            } if timestamp else None,
        }

    def _handle_get_transaction(self, req_id, params):
        """Handle eth_getTransactionByHash — return full tx object."""
        tx_hash = params[0] if params else ""
        tx = self.transactions.get(tx_hash)

        if tx:
            print(f"[mock] -> eth_getTransactionByHash({tx_hash[:14]}...) = FOUND")
        else:
            print(f"[mock] -> eth_getTransactionByHash({tx_hash[:14]}...) = NOT FOUND")

        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": tx,  # None if not found (valid JSON-RPC response)
        }

    async def _push_transactions(self, websocket, sub_id):
        """Push transaction hashes as subscription notifications."""
        iteration = 0
        while True:
            iteration += 1
            print(f"\n[mock] === Starting dataset replay (iteration {iteration}) ===")

            for i, tx_hash in enumerate(self.tx_hashes, 1):
                notification = {
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {
                        "subscription": sub_id,
                        "result": tx_hash,
                    },
                }
                try:
                    await websocket.send(json.dumps(notification))
                    label = self.transactions[tx_hash].get("input", "0x")[:10]
                    print(f"[mock] [{i}/{len(self.tx_hashes)}] Pushed {tx_hash[:14]}... (selector={label})")
                    await asyncio.sleep(self.delay)
                except websockets.exceptions.ConnectionClosed:
                    print("[mock] Client disconnected during push")
                    return

            if not self.loop:
                print("\n[mock] === All transactions sent. Server stays alive for queries. ===")
                # Keep alive — don't return, just stop pushing
                await asyncio.Future()  # Block forever
                return

            print(f"\n[mock] === Loop mode: waiting 5s before next replay ===")
            await asyncio.sleep(5)


async def main():
    parser = argparse.ArgumentParser(description="Mock Ethereum WebSocket Node")
    parser.add_argument("--data", default="data/test_data_enriched.csv",
                        help="Path to enriched CSV (default: data/test_data_enriched.csv)")
    parser.add_argument("--host", default="localhost",
                        help="Host to bind (default: localhost)")
    parser.add_argument("--port", type=int, default=8765,
                        help="Port to bind (default: 8765)")
    parser.add_argument("--delay", type=float, default=2.0,
                        help="Seconds between each transaction push (default: 2.0)")
    parser.add_argument("--loop", action="store_true",
                        help="Loop the dataset forever (default: play once)")
    args = parser.parse_args()

    transactions, block_timestamps = load_transactions(args.data)
    if not transactions:
        print("[error] No transactions loaded. Check your CSV file.")
        sys.exit(1)

    node = MockEthereumNode(transactions, block_timestamps, args.delay, args.loop)

    async with websockets.serve(node.handle_connection, args.host, args.port):
        print(f"[mock] Mock Ethereum Node running on ws://{args.host}:{args.port}")
        print(f"[mock] Delay: {args.delay}s between transactions")
        print(f"[mock] Loop:  {'ON' if args.loop else 'OFF'}")
        print(f"[mock] Ready — start listener.py to connect\n")
        await asyncio.Future()  # Run forever


if __name__ == "__main__":
    asyncio.run(main())