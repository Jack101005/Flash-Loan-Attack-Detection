"""
listener_mp.py — Multi-Process Flash Loan Mempool Listener (Stage 1, parallel)

Throughput-oriented variant of listener.py. The single-process listener is
bottlenecked on per-transaction RPC round-trips: for every pending hash it does
`get_transaction(hash)` (and `get_block` for hits), all inside one event loop on
one connection. This version fans that work out across N worker processes.

Two modes:

  LIVE mode (default) — same as listener.py's data source:
      Mempool (ONE WebSocket subscription)
          │
          ▼
      Feeder process ──► multiprocessing.Queue (tx hashes) ──► N worker processes
                                                                │ each has its own
                                                                │ Web3 connection
                                                                │ + Kafka producer
                                                                ▼
                                                         Kafka topic raw_txns

  OFFLINE mode (--offline) — no WebSocket, no mock server, no Docker required:
      data/test_data_enriched.csv
          │
          ▼
      Feeder process ──► multiprocessing.Queue (tx rows) ──► N worker processes
                                                              │ same filter/decode
                                                              │ logic as live mode,
                                                              │ no RPC calls needed
                                                              ▼
                                                         Kafka topic raw_txns
                                                         (or stdout with --no-kafka)

Why one subscription/reader, many fetchers/workers:
  - The feed itself is cheap (hashes, or CSV rows). Duplicating it would make
    every worker see every item and do the work N times — strictly worse.
  - The EXPENSIVE part in live mode is the per-hash `get_transaction` round-trip.
    In offline mode there's no RPC, but the fan-out structure (and therefore the
    filter/decode/produce code path) is identical — so offline mode is a faithful
    test of everything except the network fetch.

Concurrency design notes (read before modifying):
  - `--no-kafka` MUST require neither `confluent_kafka` nor a running broker.
    `broker.kafka_producer` (which imports confluent_kafka) is therefore only
    ever imported INSIDE `if use_kafka:` blocks — never at module level, never
    unconditionally inside a worker function.
  - Per-item counters (`detected`, `filtered_out`, `kafka_sent`, ...) are NOT
    accumulated via `Manager().dict()["x"] += 1` in worker hot loops. That
    pattern is read-RPC + increment + write-RPC across process boundaries and
    is NOT atomic — under 4-way contention with no per-item delay it loses
    35-55% of increments (confirmed empirically). Instead, each worker keeps
    PURELY LOCAL counters and writes ONE final dict to a shared results map
    after draining its STOP sentinel; the parent sums these after join().
  - A `Barrier(n_workers + 1)` synchronizes feeder + all workers before the
    first item is enqueued. Without it, a zero-delay feeder can drain the
    queue before slow-starting worker processes are ready to pull from it,
    leaving some workers permanently idle for short runs.

Dedup (live mode) is shared across workers via a Manager dict so two workers
never fetch the same hash. Each worker owns its own Web3 + Kafka producer
(neither object is picklable / fork-safe, so they are created INSIDE the worker
after fork).

Output schema is identical to listener.py — same 9 fields, same raw_txns topic —
so the PySpark streaming job consumes both without changes:
    {tx_hash, from, to, input, value, gas, gas_price, timestamp, source}

Usage:
    # Offline — no Docker, no mock server, no network at all
    python ingestion/listener_mp.py --offline --no-kafka
    python ingestion/listener_mp.py --offline --workers 4 --no-kafka
    python ingestion/listener_mp.py --offline --workers 4          # needs Kafka up

    # Offline throughput sweep (mirrors benchmarks/run_benchmark.py)
    python ingestion/listener_mp.py --offline --benchmark --no-kafka

    # Live — requires mock server or real RPC
    python ingestion/listener_mp.py                              # 4 workers, .env URLs
    python ingestion/listener_mp.py --url ws://localhost:8765    # mock server
    python ingestion/listener_mp.py --workers 8                  # 8 fetcher processes
    python ingestion/listener_mp.py --no-kafka                   # print-only
    python ingestion/listener_mp.py --duration 60                # auto-stop after 60s

Requires: pip install web3 websockets confluent-kafka python-dotenv
"""

import argparse
import asyncio
import csv
import json
import multiprocessing as mp
import os
import sys
import time

# Real-world flash-loan / MEV `input` calldata can exceed Python's default
# 131072-byte csv field limit (multicall batches, encoded swap routes).
# Raise it before any CSV is read via load_csv_rows().
csv.field_size_limit(100_000_000)

# Allow running directly (python ingestion/listener_mp.py) while importing the
# broker/ sibling package and the local config module.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass

from config import WATCHLIST, SELECTORS

# Sentinel placed on the queue once per worker to signal clean shutdown.
_STOP = "__STOP__"

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_CSV = os.path.join(PROJECT_ROOT, "data", "test_data_enriched.csv")


# ──────────────────────────────────────────────────────────────────────────────
# ABI loading (shared by all workers — plain data, safe to load per process)
# ──────────────────────────────────────────────────────────────────────────────
def load_abi(filename: str) -> list:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(script_dir, "abis", filename),
        os.path.join(script_dir, "..", "abis", filename),
    ]
    for path in candidates:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    return []


AAVE_V3_ABI = load_abi("aave_v3_pool.json") or [
    {"name": "flashLoan", "type": "function", "inputs": [
        {"name": "receiverAddress", "type": "address"},
        {"name": "assets", "type": "address[]"},
        {"name": "amounts", "type": "uint256[]"},
        {"name": "interestRateModes", "type": "uint256[]"},
        {"name": "onBehalfOf", "type": "address"},
        {"name": "params", "type": "bytes"},
        {"name": "referralCode", "type": "uint16"}], "outputs": []},
    {"name": "flashLoanSimple", "type": "function", "inputs": [
        {"name": "receiverAddress", "type": "address"},
        {"name": "asset", "type": "address"},
        {"name": "amount", "type": "uint256"},
        {"name": "params", "type": "bytes"},
        {"name": "referralCode", "type": "uint16"}], "outputs": []},
]
BALANCER_V2_ABI = load_abi("balancer_v2_vault.json") or [
    {"name": "flashLoan", "type": "function", "inputs": [
        {"name": "recipient", "type": "address"},
        {"name": "tokens", "type": "address[]"},
        {"name": "amounts", "type": "uint256[]"},
        {"name": "userData", "type": "bytes"}], "outputs": []},
]
UNISWAP_V3_ABI = load_abi("uniswap_v3_pool.json") or [
    {"name": "flash", "type": "function", "inputs": [
        {"name": "recipient", "type": "address"},
        {"name": "amount0", "type": "uint256"},
        {"name": "amount1", "type": "uint256"},
        {"name": "data", "type": "bytes"}], "outputs": []},
]


# ──────────────────────────────────────────────────────────────────────────────
# Shared filter + decode (used by both live and offline workers)
# ──────────────────────────────────────────────────────────────────────────────
def build_decoders():
    """Build ABI decoder contracts. Pure data — safe to call in any process."""
    from web3 import Web3
    w3 = Web3()
    aave = w3.eth.contract(abi=AAVE_V3_ABI)
    balancer = w3.eth.contract(abi=BALANCER_V2_ABI)
    uniswap = w3.eth.contract(abi=UNISWAP_V3_ABI)
    return {
        "Aave V3 flashLoan": aave,
        "Aave V3 flashLoanSimple": aave,
        "Balancer V2 flashLoan": balancer,
        "Uniswap V3 flash": uniswap,
    }


def filter_and_decode(tx: dict, decoders: dict):
    """Apply the two-pass filter (address → selector) and optionally decode.

    `tx` must have at least: to, input, from, value, gas, gas_price/gasPrice.
    Returns (out_data, protocol_method, assets_str) or (None, None, None) if
    the tx is not a watched flash loan call.

    Note: a successful selector match (protocol_method set) does NOT guarantee
    decode_function_input succeeds — some real-world calldata may not match the
    ABI's expected tuple layout (e.g. a different overload, a proxy/multicall
    wrapper, or malformed test data). On decode failure, assets_str records the
    failure reason but the tx is still counted as `detected` and still produced
    to Kafka with the raw input_hex — decode is enrichment, not a filter gate.
    """
    to = tx.get("to")
    if not to:
        return None, None, None
    to_addr = to.lower()
    if to_addr not in WATCHLIST:
        return None, None, None

    input_data = tx.get("input", "0x")
    input_hex = ("0x" + input_data.hex()) if isinstance(input_data, bytes) else str(input_data)
    selector = input_hex[:10]
    if selector not in SELECTORS:
        return None, None, None

    protocol_method = SELECTORS[selector]

    assets_str = ""
    decoder = decoders.get(protocol_method)
    if decoder:
        try:
            func, decoded = decoder.decode_function_input(input_hex)
            if "assets" in decoded:
                assets_str = f" assets={decoded['assets']}"
            elif "asset" in decoded:
                assets_str = f" asset={decoded['asset']}"
            elif "tokens" in decoded:
                assets_str = f" tokens={decoded['tokens']}"
        except Exception as e:
            assets_str = f" decode=FAILED({type(e).__name__})"

    return input_hex, protocol_method, assets_str


# ════════════════════════════════════════════════════════════════════════════
# OFFLINE MODE — feeder reads CSV rows, no network/WebSocket/mock server at all
# ════════════════════════════════════════════════════════════════════════════
def load_csv_rows(csv_path: str) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def offline_feeder_main(rows, hash_queue, n_workers, ready_barrier, delay: float):
    """Wait for all workers to be ready, then push CSV rows + STOP sentinels.

    One row = one simulated 'mempool hash'. The barrier prevents a fast feeder
    from draining the queue before slow-starting worker processes connect.
    """
    ready_barrier.wait()
    for row in rows:
        hash_queue.put(row)
        if delay > 0:
            time.sleep(delay)
    for _ in range(n_workers):
        hash_queue.put(_STOP)


def offline_worker_main(worker_id, hash_queue, ready_barrier, results, use_kafka, quiet):
    """Pull CSV rows, build a tx-shaped dict, filter/decode/produce.

    No Web3 connection, no RPC calls — `get_transaction` is replaced by reading
    the row's own fields, which is the whole point of offline mode.

    Keeps PURELY LOCAL counters (no Manager IPC in the hot loop) and writes ONE
    final dict to `results[worker_id]` after STOP — see module docstring for why.
    """
    decoders = build_decoders()

    kafka_producer = None
    produce_message = flush_producer = None
    if use_kafka:
        try:
            from broker.kafka_producer import create_producer, produce_message, flush_producer
            kafka_producer = create_producer()
        except Exception as e:
            if not quiet:
                print(f"[w{worker_id}] kafka init failed ({e}) — print-only")

    local = {"handled": 0, "detected": 0, "filtered_out": 0,
             "kafka_sent": 0, "kafka_failures": 0}

    ready_barrier.wait()

    while True:
        row = hash_queue.get()
        if row == _STOP:
            break
        local["handled"] += 1

        tx = {
            "to": row.get("to", ""),
            "from": row.get("from", ""),
            "input": row.get("input", "0x"),
            "value": row.get("value", "0x0"),
            "gas": row.get("gas", "0x0"),
            "gasPrice": row.get("gas_price", "0x0"),
        }

        input_hex, protocol_method, assets_str = filter_and_decode(tx, decoders)
        if input_hex is None:
            local["filtered_out"] += 1
            continue

        local["detected"] += 1

        try:
            timestamp = float(row.get("block_timestamp") or time.time())
        except (ValueError, TypeError):
            timestamp = time.time()

        # Hex-string fields from the CSV (e.g. "0x2dc6c0") -> decimal strings,
        # matching what listener.py's live path produces via str(tx.get("gas",0)).
        def hex_to_dec_str(v, default="0"):
            try:
                return str(int(str(v), 16)) if str(v).startswith("0x") else str(v)
            except (ValueError, TypeError):
                return default

        out = {
            "tx_hash": row.get("tx_hash", ""),
            "from": tx["from"],
            "to": tx["to"],
            "input": input_hex,
            "value": hex_to_dec_str(tx["value"]),
            "gas": hex_to_dec_str(tx["gas"]),
            "gas_price": hex_to_dec_str(tx["gasPrice"]),
            "timestamp": timestamp,
            "source": "offline_csv_replay",
        }

        if not quiet:
            print(f"[w{worker_id}] DETECTED {protocol_method} {out['tx_hash'][:18]}...{assets_str}")

        if kafka_producer:
            try:
                produce_message(kafka_producer, "raw_txns", out["tx_hash"], out)
                local["kafka_sent"] += 1
            except Exception as e:
                local["kafka_failures"] += 1
                if not quiet:
                    print(f"[w{worker_id}] kafka send failed: {e}")

    if kafka_producer and flush_producer:
        flush_producer(kafka_producer)

    local["pid"] = os.getpid()
    results[worker_id] = local  # SINGLE write per worker -> no increment race
    if not quiet:
        print(f"[w{worker_id}] stopped (handled {local['handled']} rows, "
              f"detected {local['detected']})")


def run_offline(csv_path: str, n_workers: int, use_kafka: bool,
                delay: float, quiet: bool = False) -> dict:
    """Run one offline pass over the CSV with n_workers fan-out. Returns stats."""
    rows = load_csv_rows(csv_path)

    mgr = mp.Manager()
    results = mgr.dict()
    hash_queue: mp.Queue = mgr.Queue(maxsize=10_000)
    # +1 for the feeder itself.
    ready_barrier = mgr.Barrier(n_workers + 1)

    if not quiet:
        print("=" * 60)
        print("  OFFLINE MULTI-PROCESS LISTENER (CSV replay)")
        print(f"  Source:   {csv_path}")
        print(f"  Rows:     {len(rows)}")
        print(f"  Workers:  {n_workers}")
        print(f"  Kafka:    {'enabled' if use_kafka else 'disabled (print-only)'}")
        print("=" * 60)

    workers = []
    for i in range(n_workers):
        p = mp.Process(target=offline_worker_main,
                       args=(i + 1, hash_queue, ready_barrier, results, use_kafka, quiet),
                       daemon=True)
        p.start()
        workers.append(p)

    feeder = mp.Process(target=offline_feeder_main,
                        args=(rows, hash_queue, n_workers, ready_barrier, delay),
                        daemon=True)
    feeder.start()

    start = time.time()
    feeder.join(timeout=120)
    for p in workers:
        p.join(timeout=120)
    elapsed = time.time() - start

    per_worker = [dict(results.get(i + 1, {"handled": 0, "detected": 0,
                  "filtered_out": 0, "kafka_sent": 0, "kafka_failures": 0, "pid": None}))
                  for i in range(n_workers)]

    totals = {"handled": 0, "detected": 0, "filtered_out": 0,
              "kafka_sent": 0, "kafka_failures": 0}
    for w in per_worker:
        for k in totals:
            totals[k] += w.get(k, 0)

    stats = {
        "rows": len(rows),
        "detected": totals["detected"],
        "filtered_out": totals["filtered_out"],
        "kafka_sent": totals["kafka_sent"],
        "kafka_failures": totals["kafka_failures"],
        "elapsed_sec": round(elapsed, 3),
        "throughput": round(totals["detected"] / elapsed, 2) if elapsed > 0 else 0,
        "n_workers": n_workers,
        "worker_loads": [w.get("handled", 0) for w in per_worker],
        "worker_pids": [w.get("pid") for w in per_worker],
    }

    if not quiet:
        print(f"\n{'=' * 60}")
        print(f"  OFFLINE SUMMARY ({elapsed:.2f}s, {n_workers} workers)")
        print(f"  Rows read:       {stats['rows']}")
        print(f"  Rows handled:    {sum(stats['worker_loads'])} (should equal rows)")
        print(f"  Flash loans:     {stats['detected']}")
        print(f"  Filtered out:    {stats['filtered_out']}")
        print(f"  detected + filtered_out = {stats['detected'] + stats['filtered_out']} "
              f"(should equal rows)")
        print(f"  Kafka sent:      {stats['kafka_sent']}")
        print(f"  Kafka failures:  {stats['kafka_failures']}")
        print(f"  Throughput:      {stats['throughput']} detections/sec")
        for i, (load, pid) in enumerate(zip(stats["worker_loads"], stats["worker_pids"])):
            print(f"  worker {i+1}: {load} rows handled (pid {pid})")
        print(f"  distinct PIDs:   {len(set(stats['worker_pids']))} "
              f"(proves real parallel processes)")
        print(f"{'=' * 60}\n")

    mgr.shutdown()
    return stats


def run_offline_benchmark(csv_path: str, use_kafka: bool, delay: float):
    """Sweep worker counts 1/2/4/8 over the same CSV, print a comparison table.

    Mirrors benchmarks/run_benchmark.py's single-vs-Spark table, but for the
    ingestion (listener) layer instead of the processing (Spark) layer.
    """
    print("=" * 72)
    print("  OFFLINE LISTENER BENCHMARK — worker count sweep")
    print(f"  Source: {csv_path}")
    print("=" * 72)

    results = []
    for n in (1, 2, 4, 8):
        r = run_offline(csv_path, n_workers=n, use_kafka=use_kafka, delay=delay, quiet=True)
        results.append(r)
        ok = "OK" if (r["detected"] + r["filtered_out"] == r["rows"]
                       and sum(r["worker_loads"]) == r["rows"]) else "MISMATCH"
        print(f"  workers={n:<2}  detected={r['detected']:<3} filtered={r['filtered_out']:<3} "
              f"[{ok}]  time={r['elapsed_sec']:<6}s  throughput={r['throughput']:<6} det/sec  "
              f"loads={r['worker_loads']}")

    baseline = results[0]["elapsed_sec"]
    print(f"\n{'Workers':<10}{'Time(s)':<10}{'Speedup':<10}")
    print("-" * 30)
    for r in results:
        speedup = round(baseline / r["elapsed_sec"], 2) if r["elapsed_sec"] > 0 else 0
        print(f"{r['n_workers']:<10}{r['elapsed_sec']:<10}{speedup}x")

    print(f"\n  Note: with {results[0]['detected']} detections total and an artificial")
    print(f"  feeder delay of {delay}s/row, speedup is bounded by feeder pacing, not")
    print(f"  fetch latency — this offline mode tests the fan-out MECHANISM and the")
    print(f"  filter/decode/produce code path, not RPC-bound throughput. For a")
    print(f"  realistic RPC-latency simulation, use --rpc-delay (live mode).")


# ════════════════════════════════════════════════════════════════════════════
# LIVE MODE — feeder subscribes to mempool, workers fetch via their own Web3
# ════════════════════════════════════════════════════════════════════════════
async def _feed(wss_url: str, hash_queue: mp.Queue, stop_event,
                seen: dict, counters: dict, max_retries: int):
    """Subscribe to newPendingTransactions and enqueue each unseen hash.

    Does NO fetching — that is the workers' job. Reconnects with exponential
    backoff (1, 2, 4, 8, 16s) just like the single-process listener.
    """
    from web3 import AsyncWeb3, WebSocketProvider

    DEDUP_MAX = 50_000
    retry = 0

    while not stop_event.is_set():
        try:
            async with AsyncWeb3(WebSocketProvider(wss_url)) as w3:
                await w3.eth.subscribe("newPendingTransactions")
                print(f"[feeder] connected {wss_url} — streaming hashes")
                retry = 0
                async for msg in w3.socket.process_subscriptions():
                    if stop_event.is_set():
                        break
                    tx_hash = msg["result"]
                    h = tx_hash.hex() if isinstance(tx_hash, bytes) else str(tx_hash)
                    counters["seen"] += 1
                    # Shared dedup: skip if any worker already has this hash queued.
                    if h in seen:
                        counters["dups"] += 1
                        continue
                    seen[h] = 1
                    if len(seen) > DEDUP_MAX:
                        seen.clear()
                    hash_queue.put(h)
        except Exception as e:
            if stop_event.is_set():
                break
            retry += 1
            if retry > max_retries:
                print(f"[feeder] max retries reached ({max_retries}) — stopping. {e}")
                stop_event.set()
                break
            delay = 2 ** (retry - 1)
            print(f"[feeder] disconnected ({type(e).__name__}) — retry {retry}/{max_retries} in {delay}s")
            await asyncio.sleep(delay)

    # Tell every worker to finish: one sentinel each.
    for _ in range(counters["n_workers"]):
        hash_queue.put(_STOP)
    print("[feeder] done — sentinels sent")


def feeder_main(wss_url, hash_queue, stop_event, seen, counters, max_retries):
    try:
        asyncio.run(_feed(wss_url, hash_queue, stop_event, seen, counters, max_retries))
    except KeyboardInterrupt:
        stop_event.set()


async def _work(worker_id: int, wss_url: str, hash_queue: mp.Queue,
                stop_event, results: dict, use_kafka: bool,
                rpc_delay: float):
    """Pull hashes off the queue, fetch the full tx, filter, decode, produce.

    Web3 and the Kafka producer are created HERE (after fork) because neither is
    safe to pickle / share across processes.

    rpc_delay: if > 0, sleep this long before each get_transaction call. This
    simulates real RPC latency on top of the mock server's instant responses —
    use it to see the multi-process speedup that real-world latency would give.

    Keeps PURELY LOCAL counters and writes ONE final dict to `results[worker_id]`
    on exit — see module docstring (same rationale as offline mode).
    """
    from web3 import AsyncWeb3, WebSocketProvider
    from web3.exceptions import TransactionNotFound

    kafka_producer = None
    produce_message = flush_producer = None
    if use_kafka:
        try:
            from broker.kafka_producer import create_producer, produce_message, flush_producer
            kafka_producer = create_producer()
        except Exception as e:
            print(f"[w{worker_id}] kafka init failed ({e}) — print-only")

    local = {"handled": 0, "detected": 0, "kafka_sent": 0, "kafka_failures": 0}

    async with AsyncWeb3(WebSocketProvider(wss_url)) as w3:
        decoders = build_decoders()
        print(f"[w{worker_id}] ready (kafka={'on' if kafka_producer else 'off'}, "
              f"rpc_delay={rpc_delay}s)")

        loop = asyncio.get_event_loop()
        while True:
            h = await loop.run_in_executor(None, hash_queue.get)
            if h == _STOP:
                break
            local["handled"] += 1

            if rpc_delay > 0:
                await asyncio.sleep(rpc_delay)

            try:
                tx = await w3.eth.get_transaction(h)
            except TransactionNotFound:
                continue
            except Exception:
                continue
            if not tx or not tx.get("to"):
                continue

            input_hex, protocol_method, assets_str = filter_and_decode(dict(tx), decoders)
            if input_hex is None:
                continue

            local["detected"] += 1

            block_timestamp = time.time()
            block_number = tx.get("blockNumber")
            if block_number is not None:
                try:
                    block = await w3.eth.get_block(block_number)
                    block_timestamp = block["timestamp"]
                except Exception:
                    pass

            out = {
                "tx_hash": h,
                "from": tx.get("from", ""),
                "to": tx["to"],
                "input": input_hex,
                "value": str(tx.get("value", 0)),
                "gas": str(tx.get("gas", 0)),
                "gas_price": str(tx.get("gasPrice", 0)),
                "timestamp": block_timestamp,
                "source": "ethereum_mainnet",
            }

            print(f"[w{worker_id}] DETECTED {protocol_method} {h[:18]}...{assets_str}")

            if kafka_producer:
                try:
                    produce_message(kafka_producer, "raw_txns", h, out)
                    local["kafka_sent"] += 1
                except Exception as e:
                    local["kafka_failures"] += 1
                    print(f"[w{worker_id}] kafka send failed: {e}")

    if kafka_producer and flush_producer:
        flush_producer(kafka_producer)

    local["pid"] = os.getpid()
    results[worker_id] = local
    print(f"[w{worker_id}] stopped (handled {local['handled']}, detected {local['detected']})")


def worker_main(worker_id, wss_url, hash_queue, stop_event, results, use_kafka, rpc_delay):
    try:
        asyncio.run(_work(worker_id, wss_url, hash_queue, stop_event, results, use_kafka, rpc_delay))
    except KeyboardInterrupt:
        pass


def run_live(wss_url: str, n_workers: int, use_kafka: bool,
             max_retries: int, duration: float | None, rpc_delay: float):
    mgr = mp.Manager()
    seen = mgr.dict()
    results = mgr.dict()
    counters = mgr.dict({"seen": 0, "dups": 0, "n_workers": n_workers})
    hash_queue: mp.Queue = mgr.Queue(maxsize=10_000)
    stop_event = mgr.Event()

    print("=" * 60)
    print("  MULTI-PROCESS LISTENER (live)")
    print(f"  URL:      {wss_url}")
    print(f"  Workers:  {n_workers}")
    print(f"  Kafka:    {'enabled' if use_kafka else 'disabled (print-only)'}")
    if rpc_delay > 0:
        print(f"  RPC delay: {rpc_delay}s/fetch (simulated)")
    print("=" * 60)

    workers = []
    for i in range(n_workers):
        p = mp.Process(target=worker_main,
                       args=(i + 1, wss_url, hash_queue, stop_event, results, use_kafka, rpc_delay),
                       daemon=True)
        p.start()
        workers.append(p)

    feeder = mp.Process(target=feeder_main,
                        args=(wss_url, hash_queue, stop_event, seen, counters, max_retries),
                        daemon=True)
    feeder.start()

    start = time.time()
    try:
        while feeder.is_alive():
            feeder.join(timeout=1.0)
            if duration and (time.time() - start) >= duration:
                print(f"\n[main] duration {duration}s reached — stopping")
                stop_event.set()
                break
    except KeyboardInterrupt:
        print("\n[main] interrupted — stopping")
        stop_event.set()

    feeder.join(timeout=10)
    for p in workers:
        p.join(timeout=10)
        if p.is_alive():
            p.terminate()

    elapsed = time.time() - start

    per_worker = [dict(results.get(i + 1, {"handled": 0, "detected": 0,
                  "kafka_sent": 0, "kafka_failures": 0, "pid": None}))
                  for i in range(n_workers)]
    totals = {"handled": 0, "detected": 0, "kafka_sent": 0, "kafka_failures": 0}
    for w in per_worker:
        for k in totals:
            totals[k] += w.get(k, 0)

    print(f"\n{'=' * 60}")
    print(f"  SUMMARY ({elapsed:.1f}s, {n_workers} workers)")
    print(f"  Hashes seen:        {counters['seen']}")
    print(f"  Duplicates skipped: {counters['dups']}")
    print(f"  Flash loans:        {totals['detected']}")
    print(f"  Kafka sent:         {totals['kafka_sent']}")
    print(f"  Kafka failures:     {totals['kafka_failures']}")
    if elapsed > 0:
        print(f"  Throughput:         {totals['detected'] / elapsed:.2f} detections/sec")
    for i, w in enumerate(per_worker):
        print(f"  worker {i+1}: {w.get('handled', 0)} handled, "
              f"{w.get('detected', 0)} detected (pid {w.get('pid')})")
    print(f"{'=' * 60}\n")


# ════════════════════════════════════════════════════════════════════════════
# Entry point
# ════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Multi-process flash loan listener")

    # Mode selection
    parser.add_argument("--offline", action="store_true",
                        help="Replay data/test_data_enriched.csv directly — "
                             "no WebSocket, no mock server, no network at all")
    parser.add_argument("--benchmark", action="store_true",
                        help="With --offline: sweep workers=1/2/4/8 and print "
                             "a speedup table (no mock server needed)")

    # Shared
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of parallel worker processes (default: 4)")
    parser.add_argument("--no-kafka", action="store_true",
                        help="Disable Kafka — print detections only. Requires "
                             "neither confluent_kafka nor a running broker.")
    parser.add_argument("--data", default=DEFAULT_CSV,
                        help=f"CSV path for --offline (default: {DEFAULT_CSV})")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="Offline: artificial per-row feeder delay (seconds)")

    # Live-only
    parser.add_argument("--url", default=None,
                        help="WebSocket RPC URL (default: $ETH_WSS_PRIMARY or ws://localhost:8765)")
    parser.add_argument("--max-retries", type=int, default=5,
                        help="Feeder reconnection attempts before exit (default: 5)")
    parser.add_argument("--duration", type=float, default=None,
                        help="Auto-stop after N seconds (useful for benchmarking)")
    parser.add_argument("--rpc-delay", type=float, default=0.0,
                        help="Live: artificial per-fetch delay (s) to simulate real RPC latency")

    args = parser.parse_args()

    # 'spawn' is safest cross-platform (required on macOS for asyncio + web3).
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        pass

    if args.offline:
        if args.benchmark:
            run_offline_benchmark(args.data, use_kafka=not args.no_kafka, delay=args.delay)
        else:
            run_offline(args.data, n_workers=args.workers,
                        use_kafka=not args.no_kafka, delay=args.delay)
    else:
        url = args.url or os.getenv("ETH_WSS_PRIMARY") or "ws://localhost:8765"
        run_live(url, args.workers, use_kafka=not args.no_kafka,
                max_retries=args.max_retries, duration=args.duration,
                rpc_delay=args.rpc_delay)
