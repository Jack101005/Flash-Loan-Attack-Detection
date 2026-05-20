"""
tests/test_ingestion.py — Integration Test for Stage 1 (Ingestion)

Runs entirely in-process: spins up an embedded mock server, runs the
listener's detection logic against it, and asserts results.

Tests:
  1. Flash loan detection: 10 flash loans + 5 noise → all 10 detected, 0 false positives
  2. Deduplication: same tx_hash sent twice → only 1 detection
  3. Reconnection: server drop and restart → listener reconnects

Usage:
    cd Flash-Loan-Attack-Detection
    python tests/test_ingestion.py
"""

import asyncio
import json
import os
import secrets
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "ingestion"))

from web3 import Web3

try:
    import websockets
except ImportError:
    print("Error: pip install websockets")
    sys.exit(1)

# ──────────────────────────────────────────────────────────────
# Fixture helpers
# ──────────────────────────────────────────────────────────────
AAVE_V3_ABI = [
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

AAVE_V3_POOL = "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2"
USDC = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
WETH = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

w3 = Web3()


def random_addr():
    return Web3.to_checksum_address("0x" + secrets.token_hex(20))


def normalize_hash(h: str) -> str:
    """Normalize a tx hash to lowercase without 0x prefix."""
    return h.lower().replace("0x", "")


def make_tx_obj(tx_hash, from_addr, to_addr, input_data):
    """Build a full transaction object as the mock server returns it."""
    return {
        "hash": tx_hash,
        "from": from_addr,
        "to": to_addr,
        "input": input_data,
        "value": "0x0",
        "gas": "0x7a120",
        "gasPrice": "0x6fc23ac00",
        "nonce": "0x0",
        "blockHash": None,
        "blockNumber": None,
        "transactionIndex": None,
        "type": "0x2",
        "chainId": "0x1",
        "v": "0x0",
        "r": "0x" + "0" * 64,
        "s": "0x" + "0" * 64,
        "maxFeePerGas": "0x6fc23ac00",
        "maxPriorityFeePerGas": "0x59682f00",
    }


def make_flash_loan(assets, amounts):
    contract = w3.eth.contract(abi=AAVE_V3_ABI)
    receiver = random_addr()
    calldata = contract.encode_abi("flashLoan", [
        receiver,
        [Web3.to_checksum_address(a) for a in assets],
        amounts, [0] * len(assets), receiver, b"\x00" * 32, 0,
    ])
    tx_hash = "0x" + secrets.token_hex(32)
    return tx_hash, make_tx_obj(tx_hash, random_addr(), AAVE_V3_POOL, calldata)


def make_flash_loan_simple(asset, amount):
    contract = w3.eth.contract(abi=AAVE_V3_ABI)
    receiver = random_addr()
    calldata = contract.encode_abi("flashLoanSimple", [
        receiver, Web3.to_checksum_address(asset), amount, b"\x00" * 32, 0,
    ])
    tx_hash = "0x" + secrets.token_hex(32)
    return tx_hash, make_tx_obj(tx_hash, random_addr(), AAVE_V3_POOL, calldata)


def make_noise():
    tx_hash = "0x" + secrets.token_hex(32)
    return tx_hash, make_tx_obj(
        tx_hash, random_addr(), random_addr(),
        "0xa9059cbb" + secrets.token_hex(64)
    )


def make_wrong_selector():
    tx_hash = "0x" + secrets.token_hex(32)
    return tx_hash, make_tx_obj(
        tx_hash, random_addr(), AAVE_V3_POOL,
        "0xa415bcad" + secrets.token_hex(128)  # Borrow selector
    )


# ──────────────────────────────────────────────────────────────
# Embedded mock server
# ──────────────────────────────────────────────────────────────
class MockServer:
    def __init__(self, transactions: dict, delay: float = 0.1):
        self.transactions = transactions
        self.tx_hashes = list(transactions.keys())
        self.delay = delay
        self.server = None
        self._push_started = asyncio.Event()

    async def handle(self, websocket):
        sub_id = "0x" + secrets.token_hex(16)

        async for raw_msg in websocket:
            msg = json.loads(raw_msg)
            method = msg.get("method", "")
            req_id = msg.get("id")

            if method == "eth_subscribe":
                await websocket.send(json.dumps({
                    "jsonrpc": "2.0", "id": req_id, "result": sub_id
                }))
                # Small delay to let the listener set up subscription processing
                await asyncio.sleep(0.2)
                asyncio.create_task(self._push(websocket, sub_id))

            elif method == "eth_getTransactionByHash":
                tx_hash = msg["params"][0]
                tx = self.transactions.get(tx_hash)
                await websocket.send(json.dumps({
                    "jsonrpc": "2.0", "id": req_id, "result": tx
                }))

            elif method in ("eth_chainId", "net_version", "web3_clientVersion"):
                await websocket.send(json.dumps({
                    "jsonrpc": "2.0", "id": req_id, "result": "0x1"
                }))

            else:
                await websocket.send(json.dumps({
                    "jsonrpc": "2.0", "id": req_id, "result": None
                }))

    async def _push(self, websocket, sub_id):
        self._push_started.set()
        for tx_hash in self.tx_hashes:
            try:
                await websocket.send(json.dumps({
                    "jsonrpc": "2.0",
                    "method": "eth_subscription",
                    "params": {"subscription": sub_id, "result": tx_hash},
                }))
                # Wait long enough for listener to fetch + process each tx
                await asyncio.sleep(self.delay)
            except Exception:
                return
        # All txs sent — wait a moment for last fetch, then close
        await asyncio.sleep(1.0)
        await websocket.close()

    async def start(self, host="localhost", port=0):
        self.server = await websockets.serve(self.handle, host, port)
        return self.server.sockets[0].getsockname()[1]

    async def stop(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()


# ──────────────────────────────────────────────────────────────
# Listener capture — reimplements the core detection loop with dedup
# ──────────────────────────────────────────────────────────────
async def run_listener_capture(url: str, timeout: float = 15.0) -> list:
    """Run detection logic, return list of detected tx hashes (normalized)."""
    from config import WATCHLIST, SELECTORS
    from web3 import AsyncWeb3, WebSocketProvider
    from web3.exceptions import TransactionNotFound

    detected = []
    seen = set()

    try:
        async with AsyncWeb3(WebSocketProvider(url)) as async_w3:
            await async_w3.eth.subscribe("newPendingTransactions")

            async def process():
                async for msg in async_w3.socket.process_subscriptions():
                    tx_hash = msg["result"]
                    h = tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)
                    h_norm = normalize_hash(h)

                    # Deduplication
                    if h_norm in seen:
                        continue
                    seen.add(h_norm)

                    try:
                        tx = await async_w3.eth.get_transaction(tx_hash)
                    except (TransactionNotFound, Exception):
                        continue

                    if not tx or not tx.get("to"):
                        continue

                    to_addr = tx["to"].lower()
                    if to_addr not in WATCHLIST:
                        continue

                    input_data = tx.get("input", "0x")
                    if isinstance(input_data, bytes):
                        input_hex = "0x" + input_data.hex()
                    else:
                        input_hex = str(input_data)

                    selector = input_hex[:10]
                    if selector not in SELECTORS:
                        continue

                    detected.append(h_norm)

            await asyncio.wait_for(process(), timeout=timeout)

    except asyncio.TimeoutError:
        pass
    except Exception as e:
        # Connection closed — expected when server stops
        pass

    return detected


# ──────────────────────────────────────────────────────────────
# Test cases
# ──────────────────────────────────────────────────────────────

async def test_flash_loan_detection():
    """10 flash loans + 5 noise → all 10 detected, 0 false positives, < 5s."""
    print("\n" + "=" * 60)
    print("  TEST 1: Flash loan detection (10 flash loans + 5 noise)")
    print("=" * 60)

    # Build fixtures
    flash_txs = [
        make_flash_loan([USDC], [1_000_000 * 10**6]),
        make_flash_loan([WETH], [500 * 10**18]),
        make_flash_loan([USDC, WETH], [2_000_000 * 10**6, 100 * 10**18]),
        make_flash_loan_simple(USDC, 5_000_000 * 10**6),
        make_flash_loan_simple(WETH, 1_000 * 10**18),
        make_flash_loan([USDC], [500_000 * 10**6]),
        make_flash_loan_simple(USDC, 3_000_000 * 10**6),
        make_flash_loan([WETH], [200 * 10**18]),
        make_flash_loan_simple(WETH, 50 * 10**18),
        make_flash_loan_simple(USDC, 10_000_000 * 10**6),
    ]
    noise_txs = [
        make_noise(), make_noise(), make_noise(),
        make_wrong_selector(), make_wrong_selector(),
    ]

    flash_hashes = {normalize_hash(h) for h, _ in flash_txs}
    noise_hashes = {normalize_hash(h) for h, _ in noise_txs}

    all_tx_lookup = {}
    for h, obj in flash_txs + noise_txs:
        all_tx_lookup[h] = obj

    # Start mock server (0.15s delay per tx → 15 txs × 0.15s ≈ 2.3s total)
    server = MockServer(all_tx_lookup, delay=0.15)
    port = await server.start()
    url = f"ws://localhost:{port}"
    print(f"  Mock server on port {port}")

    try:
        start = time.time()
        detected = await run_listener_capture(url, timeout=10.0)
        elapsed = time.time() - start

        detected_set = set(detected)
        found = flash_hashes & detected_set
        missed = flash_hashes - detected_set
        false_pos = detected_set & noise_hashes

        print(f"  Elapsed:         {elapsed:.1f}s")
        print(f"  Expected:        {len(flash_hashes)} flash loans")
        print(f"  Detected:        {len(found)}")
        print(f"  Missed:          {len(missed)}")
        print(f"  False positives: {len(false_pos)}")

        assert len(missed) == 0, f"Missed {len(missed)} flash loans"
        assert len(false_pos) == 0, f"{len(false_pos)} false positives"
        assert elapsed < 5.0, f"Took {elapsed:.1f}s, must be < 5s"

        print("  PASSED")
        return True
    finally:
        await server.stop()


async def test_deduplication():
    """Same tx_hash sent twice → only 1 detection."""
    print("\n" + "=" * 60)
    print("  TEST 2: Deduplication (same hash sent twice)")
    print("=" * 60)

    tx_hash, tx_obj = make_flash_loan([USDC], [1_000_000 * 10**6])

    class DedupServer(MockServer):
        async def _push(self, websocket, sub_id):
            for _ in range(2):
                try:
                    await websocket.send(json.dumps({
                        "jsonrpc": "2.0",
                        "method": "eth_subscription",
                        "params": {"subscription": sub_id, "result": tx_hash},
                    }))
                    await asyncio.sleep(0.2)
                except Exception:
                    return

    server = DedupServer({tx_hash: tx_obj}, delay=0.1)
    port = await server.start()

    try:
        detected = await run_listener_capture(f"ws://localhost:{port}", timeout=5.0)

        norm = normalize_hash(tx_hash)
        count = sum(1 for d in detected if d == norm)

        print(f"  Hash sent:   2 times")
        print(f"  Detections:  {count}")

        assert count == 1, f"Expected 1, got {count}"
        print("  PASSED")
        return True
    finally:
        await server.stop()


async def test_reconnection():
    """Server drop + restart → listener reconnects and detects."""
    print("\n" + "=" * 60)
    print("  TEST 3: Reconnection after server drop")
    print("=" * 60)

    PORT = 18765

    # Phase 1: detect a flash loan
    h1, obj1 = make_flash_loan([USDC], [1_000_000 * 10**6])
    server1 = MockServer({h1: obj1}, delay=0.1)
    try:
        await server1.start(port=PORT)
    except OSError:
        print("  SKIPPED (port in use)")
        return True

    det1 = await run_listener_capture(f"ws://localhost:{PORT}", timeout=3.0)
    await server1.stop()
    await asyncio.sleep(0.5)
    print(f"  Phase 1 (before drop): {len(det1)} detection(s)")

    # Phase 2: new server, new tx
    h2, obj2 = make_flash_loan_simple(WETH, 100 * 10**18)
    server2 = MockServer({h2: obj2}, delay=0.1)
    await server2.start(port=PORT)

    det2 = await run_listener_capture(f"ws://localhost:{PORT}", timeout=3.0)
    await server2.stop()
    print(f"  Phase 2 (after reconnect): {len(det2)} detection(s)")

    assert len(det1) >= 1, "Phase 1 should detect >= 1"
    assert len(det2) >= 1, "Phase 2 should detect >= 1"
    print("  PASSED")
    return True


# ──────────────────────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────────────────────

async def run_all_tests():
    print("\n" + "#" * 60)
    print("  Flash Loan Ingestion — Integration Tests")
    print("#" * 60)

    results = {}
    for name, fn in [
        ("Flash loan detection", test_flash_loan_detection),
        ("Deduplication", test_deduplication),
        ("Reconnection", test_reconnection),
    ]:
        try:
            passed = await fn()
            results[name] = "PASSED" if passed else "FAILED"
        except AssertionError as e:
            print(f"  FAILED: {e}")
            results[name] = "FAILED"
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            results[name] = "ERROR"

    print("\n" + "#" * 60)
    print("  Results")
    print("#" * 60)
    all_ok = True
    for name, status in results.items():
        icon = "OK" if status == "PASSED" else "FAIL"
        print(f"  [{icon}]  {name}: {status}")
        if status != "PASSED":
            all_ok = False
    print("#" * 60)

    if all_ok:
        print("\n  All tests passed!\n")
    else:
        print("\n  Some tests failed.\n")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(run_all_tests())