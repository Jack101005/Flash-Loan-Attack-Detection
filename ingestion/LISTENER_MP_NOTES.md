# listener_mp.py — Design, Race Conditions, and Benchmark Log

This file documents:
1. **Why listener_mp.py exists** and the architectural choices made
2. **Three bugs found and fixed** during validation
3. **The full benchmark log** — 60-row and 865-row offline sweeps
4. **What to do next** (the live `--rpc-delay` path not yet run)

---

## 1. Purpose and architecture

`listener_mp.py` is a multi-process variant of `listener.py`. It addresses
the same ingestion bottleneck from a different angle:

**The bottleneck:** for each pending tx hash from the mempool subscription,
`get_transaction` is an RPC round-trip over a WebSocket — typically 50–200 ms
per call. A single-process listener serializes these, so throughput is bounded
by roughly `1 / rpc_latency`. At 100 ms per call, that is 10 tx/s maximum.

**The approach:** separate the subscription (cheap, single point of truth) from
the per-transaction fetching (expensive, embarrassingly parallel):

```
mempool subscription (feeder)
        │
        │  (raw tx hashes over multiprocessing.Queue)
        │
   ┌────▼────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ Worker 1 │  │  Worker 2   │  │  Worker 3   │  │  Worker 4   │
   │ get_tx() │  │  get_tx()   │  │  get_tx()   │  │  get_tx()   │
   │ decode() │  │  decode()   │  │  decode()   │  │  decode()   │
   │ produce()│  │  produce()  │  │  produce()  │  │  produce()  │
   └──────────┘  └─────────────┘  └─────────────┘  └─────────────┘
```

**Why NOT N independent subscriptions?**
Each worker would receive every mempool hash and issue its own `get_transaction`
RPC for every hash. With N workers, every item is fetched N times — strictly
worse than a single process. The correct model is: one reader, N fetchers.

**Workers are created post-fork** (on macOS/Linux: `fork`; on Windows: `spawn`).
Each worker creates its own `Web3` connection and `confluent_kafka.Producer`
instance post-fork, because neither is picklable and neither should be shared
across process boundaries.

---

## 2. Bugs found and fixed during validation

### Bug 1 — Unconditional `confluent_kafka` import with `--no-kafka`

**Symptom:** `python listener_mp.py --offline --no-kafka` raised
`ModuleNotFoundError: No module named 'confluent_kafka'` even with `--no-kafka`
specified, because `from confluent_kafka import Producer` appeared at the
module top-level.

**Fix:** moved the import inside the `if use_kafka:` guard inside each worker's
`_worker_main()`. The `--no-kafka` flag now truly requires zero additional
packages beyond Python stdlib + `web3`.

### Bug 2 — Non-atomic `Manager().dict()["x"] += 1` counter

**Symptom:** running `python listener_mp.py --offline --no-kafka --workers 4`
against the 60-row CSV gave `detected + filtered_out < 60` — counts summed
to 42, 48, 50 (varying run-to-run) instead of exactly 60. Data was being
silently dropped.

**Root cause:** `Manager().dict()["detected"] += 1` is NOT atomic across
process boundaries. Each increment is:
1. Worker sends `get("detected")` RPC to the Manager process
2. Manager process returns the current value
3. Worker adds 1 locally
4. Worker sends `set("detected", new_value)` RPC to the Manager process

Under N-way contention, steps 1–4 from different workers interleave:
Workers A and B both read value 5, both compute 6, both write 6 → one
increment is lost. Confirmed in isolation:

```python
# 4 processes × 50 increments = 200 expected
# Actual results: 90, 107, 113, 122 — 39–55% of increments lost
```

**Fix:** each worker accumulates plain local dict counters (zero IPC in the
hot loop) and writes them **once** to a `Manager().dict()` keyed by worker ID
**after** draining its `_STOP` sentinel and before exiting. The parent
process calls `worker.join()` on each worker, then reads each worker's
result dict and sums them. This pattern has zero contention and is
theoretically correct.

**Result:** `detected=35`, `filtered_out=25`, `detected + filtered_out == 60`,
`sum(worker_loads) == 60`, 4 distinct PIDs — exact and reproducible across
every run.

### Bug 3 — Feeder drains queue before workers are ready (`Barrier`)

**Symptom:** without the barrier, running with N workers would occasionally
produce wildly unbalanced loads: `[9, 2, 0, 0]` for 4 workers on 11 items.
Workers 3 and 4 got nothing because the feeder had already pushed all items
before they finished spawning and called `Queue.get()`.

**Root cause:** on macOS with `spawn` start method, each `mp.Process` must
re-import the module and initialize all module-level state (including Web3
ABI decoders) before executing. This can take tens of milliseconds. With
zero per-item delay in offline mode, the feeder can enqueue all 60 items
before even one worker has called `Queue.get()` for the first time.

**Fix:** `multiprocessing.Barrier(n_workers + 1)`. Each worker calls
`barrier.wait()` after all its initialization is complete (Web3 connected,
Kafka producer created, etc.). The feeder calls `barrier.wait()` before
pushing the first item. This ensures all workers are ready before any
work enters the queue.

**After the barrier:** work distribution across workers still varies
run-to-run (OS scheduling determines which worker wins each `Queue.get()`
race). Only the totals are guaranteed: `sum(worker_loads) == rows`.

---

## 3. Benchmark log

### How to run

```bash
# No Docker, no mock server, no network required
python ingestion/listener_mp.py --offline --benchmark --no-kafka

# For the 865-row dataset specifically:
python ingestion/listener_mp.py --offline --benchmark --no-kafka \
  --data data/test_data_large.csv
```

> **`--data data/test_data_large.csv` requires `csv.field_size_limit`.**
> Real on-chain MEV calldata in `input` fields routinely exceeds Python's
> default 128 KB CSV field limit (`_csv.Error: field larger than field
> limit (131072)`). `listener_mp.py` now sets
> `csv.field_size_limit(100_000_000)` near the top of its imports.
> This is safe — it only affects CSV parsing, not memory usage in general.

### 60-row results (`data/test_data_enriched.csv`)

Single pass, 4 workers:

```
============================================================
  OFFLINE SUMMARY (0.92s, 4 workers)
  Rows read:       60
  Rows handled:    60
  Flash loans:     35
  Filtered out:    25
  detected + filtered_out = 60
  Throughput:      37.98 detections/sec
  worker 1: 17 rows handled (pid 2893)
  worker 2: 11 rows handled (pid 2894)
  worker 3: 19 rows handled (pid 2895)
  worker 4: 13 rows handled (pid 2896)
  distinct PIDs:   4
============================================================
```

Worker-count sweep:

```
workers   detected  filtered  ok?   time(s)   throughput(det/s)   loads
-------   --------  --------  ---   -------   -----------------   ------
1         35        25        [OK]  0.407     86.08               [60]
2         35        25        [OK]  0.423     82.69               [29, 31]
4         35        25        [OK]  0.448     78.13               [16, 12, 13, 19]
8         35        25        [OK]  0.483     72.48               [7, 6, 8, 9, 5, 8, 8, 9]

Speedup vs 1 worker: 0.96× / 0.91× / 0.84× — negative
```

**Interpretation:** `mp.Process` spawn cost (~tens of ms per worker with
`spawn`) dominates sub-millisecond-per-row work. Adding workers increases
fixed overhead without increasing parallelism on meaningful work.

### 865-row results (`data/test_data_large.csv`)

Source: real Etherscan exports — `etherscan_exports/aav3.csv`,
`balencer.csv`, `uniswap.csv` — enriched by `ingestion/prepare_test_data.py`.

Worker-count sweep:

```
workers   detected  filtered  ok?   time(s)   throughput(det/s)   loads (sample)
-------   --------  --------  ---   -------   -----------------   ------
1         429       436       [OK]  0.674     636.6               [865]
2         429       436       [OK]  0.282     1519.0              [440, 425]     ← best
4         429       436       [OK]  0.298     1439.6              [227, 207, 222, 209]
8         429       436       [OK]  0.392     1094.5              [110, 114, 105, 110, 104, 105, 116, 101]

Speedup vs 1 worker: 2.39× / 2.26× / 1.72×
```

**Interpretation:** the same `mp.Process` spawn cost is amortized over ~14×
more work (865 rows vs 60). Per-item compute (ABI decode + dict building) now
dominates spawn cost, so workers provide real parallelism. 2 workers is the
sweet spot: gains taper past 2 because per-worker payload shrinks faster than
spawn overhead, following the classic Amdahl's Law curve.

Note: these are single-sample timings. Run-to-run variance is ±5–15% due to
OS scheduling and Python GIL interactions. The speedup trend is reliable;
the exact values are not. Recommend 3–5 trials and averaging for a formal
result.

---

## 4. What to do next (live `--rpc-delay` path)

The offline benchmark tests **correctness** and **Amdahl amortization** on
in-memory data. The realistic positive case for multi-process ingestion is
the **live RPC path**, where each item requires a `get_transaction` WebSocket
call (50–200 ms of real network latency). Even the 60-row dataset would show
positive speedup at 100 ms/call: 4 concurrent fetches vs 1 serial fetch.

The `--rpc-delay` flag exists to simulate this without needing a real node:

```bash
# Terminal 1: mock server with 100ms per-transaction delay
python ingestion/mock_server.py --delay 0.1 --loop --data data/test_data_large.csv

# Terminal 2: 1 worker vs 4 workers (expected: ~4× speedup)
python ingestion/listener_mp.py --url ws://localhost:8765 --rpc-delay 0.1 --workers 1
python ingestion/listener_mp.py --url ws://localhost:8765 --rpc-delay 0.1 --workers 4
```

**This has not yet been run.** Expected result: 4 workers at 100 ms/fetch
produce ~4× throughput improvement vs 1 worker (fetch-bound, spawn cost
negligible). The natural next step for whoever picks up `listener_mp.py`.
