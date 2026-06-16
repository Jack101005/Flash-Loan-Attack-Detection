# Benchmark & Fault Tolerance Demo

This directory contains scripts that demonstrate **why distributed processing
is necessary** by comparing a single-process Python pipeline against the
distributed Spark + Kafka pipeline on the same dataset — and, separately,
a single-process ingestion listener against a multi-process one.

---

## Files

| File | Purpose |
|------|---------|
| `single_process_job.py` | Baseline single-process pipeline (same logic as Spark job) |
| `run_benchmark.py` | Measures throughput (tx/sec) for single-process vs Spark 1/2/4 workers |
| `fault_tolerance_demo.py` | Live demo: crash single-process mid-batch, then crash a Spark worker — shows recovery difference |
| `benchmark_results.csv` | Output from `run_benchmark.py --mode all` (auto-generated) |
| `../ingestion/listener_mp.py` | Multi-process ingestion listener — `--offline --benchmark` sweeps workers=1/2/4/8 over the CSV (see "Ingestion-layer benchmark" below) |
| `../ingestion/LISTENER_MP_NOTES.md` | Design rationale, race conditions found/fixed, and the full benchmark log for `listener_mp.py` |

---

## Prerequisites

```powershell
# Docker must be running with the full stack
docker compose up -d --build

# Python deps (from project root venv)
pip install python-dotenv pymongo certifi redis requests
```

---

## Throughput Benchmark

### Single-process only (no Docker required)

```powershell
python benchmarks/run_benchmark.py --mode single
```

> **Price lookups: live vs offline.** By default `single_process_job.py`
> fetches historical prices from Redis → CoinGecko. With Redis down (no
> Docker), every WETH/WBTC lookup falls through to CoinGecko's rate-limited
> public API **sequentially** — a 35-tx run can take **4+ minutes** and the
> numbers are not reproducible (rate limits vary).
>
> For throughput benchmarking, use `--offline-prices` (static fallback prices,
> no network). This isolates the **parallelizable compute** (decode → enrich →
> score → write) from external API latency. `run_benchmark.py` passes this flag
> automatically for the single-process baseline.
>
> ```powershell
> python benchmarks/single_process_job.py --offline-prices   # ~12s
> python benchmarks/single_process_job.py                     # ~4min (live prices, Redis down)
> ```
>
> This is itself a distributed-systems lesson: with cold price lookups, adding
> Spark workers does **not** help — they all contend for the same CoinGecko
> per-IP rate limit. The fix is a **shared distributed cache (Redis)**, which
> every Spark worker reads in parallel. Warm the cache first, then parallelism pays off.

### Spark with specific worker count (Docker required)

```powershell
python benchmarks/run_benchmark.py --mode spark --workers 1
python benchmarks/run_benchmark.py --mode spark --workers 2
python benchmarks/run_benchmark.py --mode spark --workers 4
```

### Full comparison: single-process + all Spark configs

```powershell
python benchmarks/run_benchmark.py --mode all
```

### Expected output

```
════════════════════════════════════════════════════════════════════════
  BENCHMARK RESULTS — Single-Process vs Distributed Spark
════════════════════════════════════════════════════════════════════════
  Mode                   Workers    Records    Time(s)    tx/sec     Speedup
  ────────────────────────────────────────────────────────────────────
  single_process         1          30         28.40      1.06       1.00x
  spark_1w               1          30         35.20      0.85       0.80x
  spark_2w               2          30         18.10      1.66       1.57x
  spark_4w               4          30         10.30      2.91       2.75x
════════════════════════════════════════════════════════════════════════
```

> **Why does Spark 1 worker look slower than single-process?**
> Spark has ~15 seconds of fixed overhead (JVM startup, Kafka roundtrip,
> micro-batch scheduling). For tiny datasets this overhead dominates.
> At sustained load (thousands of transactions), the Spark numbers improve
> and the 4-worker config pulls ahead significantly.

---

## Ingestion-layer benchmark (listener_mp.py)

The benchmarks above measure the **processing** layer (Spark vs single
process, reading from Kafka). This section measures the **ingestion**
layer — `ingestion/listener.py` (single process) vs
`ingestion/listener_mp.py` (multi-process: one feeder + N worker processes).

### Run it (no Docker, no mock server)

```powershell
# Single pass, 4 workers, prints detections to stdout
python ingestion/listener_mp.py --offline --no-kafka --workers 4

# Worker-count sweep: 1, 2, 4, 8
python ingestion/listener_mp.py --offline --benchmark --no-kafka
```

`--offline` replays `data/test_data_enriched.csv` directly (no WebSocket,
no mock server). `--no-kafka` additionally requires neither
`confluent_kafka` nor a running broker — these two flags together give a
zero-dependency test.

### Actual results — two datasets

#### 60-row CSV (`data/test_data_enriched.csv`)

**Single pass, 4 workers:**

```
============================================================
  OFFLINE SUMMARY (0.92s, 4 workers)
  Rows read:       60
  Rows handled:    60 (should equal rows)
  Flash loans:     35
  Filtered out:    25
  detected + filtered_out = 60 (should equal rows)
  Kafka sent:      0
  Kafka failures:  0
  Throughput:      37.98 detections/sec
  worker 1: 17 rows handled (pid 2893)
  worker 2: 11 rows handled (pid 2894)
  worker 3: 19 rows handled (pid 2895)
  worker 4: 13 rows handled (pid 2896)
  distinct PIDs:   4 (proves real parallel processes)
============================================================
```

**Worker-count sweep (60 rows, this machine):**

```
========================================================================
  OFFLINE LISTENER BENCHMARK — worker count sweep
========================================================================
  workers=1   detected=35  filtered=25  [OK]  time=0.407 s  throughput=86.08  det/sec  loads=[60]
  workers=2   detected=35  filtered=25  [OK]  time=0.423 s  throughput=82.69  det/sec  loads=[29, 31]
  workers=4   detected=35  filtered=25  [OK]  time=0.448 s  throughput=78.13  det/sec  loads=[16, 12, 13, 19]
  workers=8   detected=35  filtered=25  [OK]  time=0.483 s  throughput=72.48  det/sec  loads=[7, 6, 8, 9, 5, 8, 8, 9]

Workers   Time(s)   Speedup
------------------------------
1         0.407     1.0x
2         0.423     0.96x
4         0.448     0.91x
8         0.483     0.84x
```

#### 865-row CSV (`data/test_data_large.csv`)

Produced by `ingestion/prepare_test_data.py` from real Etherscan exports
(aav3.csv, balencer.csv, uniswap.csv). 429 detected / 436 filtered.

> **Note:** real MEV calldata `input` fields can exceed Python's default
> 128 KB CSV field limit. Running this benchmark on the 60-row dataset
> first will not expose this; it surfaces only with real on-chain data.
> `listener_mp.py` now adds `csv.field_size_limit(100_000_000)` near its
> imports to handle this transparently.

**Worker-count sweep (865 rows, this machine):**

```
workers=1   detected=429  filtered=436  [OK]  time=0.674 s  throughput= 636.6 det/sec  loads=[865]
workers=2   detected=429  filtered=436  [OK]  time=0.282 s  throughput=1519.0 det/sec  loads=[440, 425]
workers=4   detected=429  filtered=436  [OK]  time=0.298 s  throughput=1439.6 det/sec  loads=[227, 207, 222, 209]
workers=8   detected=429  filtered=436  [OK]  time=0.392 s  throughput=1094.5 det/sec  loads=[110, 114, 105, 110, 104, 105, 116, 101]

Workers   Time(s)   Speedup
------------------------------
1         0.674     1.00x
2         0.282     2.39x  ← best
4         0.298     2.26x
8         0.392     1.72x
```

### How to interpret this

**Correctness, not speed, is the headline result here.** Across both
datasets and all worker counts, `detected + filtered_out == rows` holds,
`[OK]` confirms `sum(worker_loads) == rows`, and the detected/filtered
split is stable. This is the result of fixing a real bug found during
development: a shared `Manager().dict()["x"] += 1` counter pattern lost
35–55% of increments under contention (confirmed in isolation: 4×50=200
expected increments, got 90–122). The fix — per-worker local counters,
written once and summed by the parent — is what makes these totals exact
and reproducible. See `ingestion/LISTENER_MP_NOTES.md` for the full
before/after.

**60-row CSV: negative speedup is expected.** With effectively zero
per-item cost (dict build + ABI decode, both sub-millisecond), the fixed
cost of spawning each `mp.Process` (tens of milliseconds via `spawn`,
since each worker re-imports the module and rebuilds Web3 ABI decoders)
dominates. 8 workers means paying that spawn cost 8 times to parallelize
under half a second of actual work — net loss.

**865-row CSV: positive speedup proves Amdahl amortization, not
multi-process being "fast enough".** The spawn overhead is identical
(same machine, same Python, same `mp.Process` spawn method). What changed
is the amount of work each worker actually does: ~14× more rows means the
same fixed overhead is amortized over ~14× more payload, flipping the
curve. 2 workers gives 2.39× speedup; gains taper past 2 workers as
per-worker share shrinks. This is the classic Amdahl’s Law shape: the
serial (spawn) fraction eventually dominates again at high worker count.

**The real-world positive case (live RPC mode) has not yet been run.**
With per-item cost at 50–200 ms (`get_transaction` RPC over WebSocket),
the amortization is immediate — even 60 rows would show positive speedup.
The `--rpc-delay` flag in live mode simulates this:

```powershell
# Mock server in one terminal
python ingestion/mock_server.py --delay 0.1 --loop

# Listener with simulated 100ms-per-fetch RPC latency, in another
python ingestion/listener_mp.py --url ws://localhost:8765 --rpc-delay 0.1 --workers 1
python ingestion/listener_mp.py --url ws://localhost:8765 --rpc-delay 0.1 --workers 4
```

**This has not been run yet.** The expected result is that 4 workers at
100ms/fetch should noticeably outperform 1 worker, because 4 fetches now
happen concurrently instead of serially — the inverse of the offline
result above, for the inverse reason (per-item cost now dominates spawn
cost). This is the natural next step for whoever picks up
`listener_mp.py`.

---

## Fault Tolerance Demo

### Scenario 1 — Single-process crash

```powershell
python benchmarks/fault_tolerance_demo.py --scenario single
```

**What you will observe:**
1. Job starts processing transactions (0.25s delay per tx so you can watch)
2. After tx #10, job simulates a crash — prints the "SIMULATED CRASH" message
3. MongoDB has 0 records (pending batch of 10 was in memory, never written)
4. Press Enter → job restarts from **row 0** (no checkpoint)
5. Final MongoDB count shows all records — but everything was re-processed

**Key point:** the 10 in-flight docs were permanently lost. At real scale,
a crash mid-batch means minutes or hours of work must be redone.

---

### Scenario 2 — Spark worker crash (requires Docker)

```powershell
python benchmarks/fault_tolerance_demo.py --scenario spark
```

**What you will observe:**
1. Transactions flow into Kafka via mock_server + listener
2. Spark processes the first micro-batch (~10s) → records appear in MongoDB
3. Script kills one Spark worker container (`docker kill <container>`)
4. Spark master detects the failure via heartbeat timeout (~10s)
5. Master reassigns the unfinished task to another worker
6. Worker resumes from the **last committed Kafka offset** (checkpoint)
7. MongoDB count continues to grow — no records lost

**Key point:** Spark never re-reads transactions it already committed.
The checkpoint guarantees exactly-once delivery (combined with the
idempotent MongoDB upsert).

---

### Both scenarios + comparison table

```powershell
python benchmarks/fault_tolerance_demo.py
```

---

## Why Single-Process Cannot Recover

```
single_process_job.py state exists only in RAM:

  pending_docs = []          ← list of dicts about to be written
  for i, row in enumerate(rows):   ← loop index not persisted
      doc = process_transaction(row)
      pending_docs.append(doc)
      if len(pending_docs) >= batch_size:
          write_to_mongo(pending_docs)  ← if crash happens HERE...
          pending_docs.clear()          ← ...these docs are GONE
```

On restart: `pending_docs` is empty, loop starts at `i=0`. No way to
know which rows were already processed.

---

## Why Spark Recovers

```
Spark checkpoint directory structure:

/tmp/spark-checkpoints/flash-loan-detection/
├── commits/        ← which micro-batches completed
├── offsets/        ← Kafka offset at start of each micro-batch
└── sources/0/      ← partition state per source

On worker crash:
  1. Spark master detects missing heartbeat
  2. Looks up the last committed offset in offsets/
  3. Re-schedules the batch task to a live worker
  4. Worker reads from that offset (not from "earliest")
  5. MongoDB upsert (UpdateOne) is idempotent — re-writing is safe
```

The combination of **Kafka offset checkpoint** + **idempotent MongoDB
upsert** is what makes Spark's recovery reliable. Without idempotent
writes, re-processing could create duplicates.

---

## How to Interpret the Throughput Numbers

| Observation | Explanation |
|-------------|-------------|
| Spark 1 worker slower than single-process | Fixed JVM/Kafka overhead dominates for small batches |
| Spark 2 workers ~1.5x speedup | 2 partitions processed in parallel — network I/O shared |
| Spark 4 workers ~3x speedup | 4 partitions × 4 workers — optimal for `raw_txns` topic |
| Adding worker 5+ gives no benefit | `raw_txns` has 4 partitions — extra workers are idle |
| Speedup is sub-linear (not exactly 4x) | Redis/CoinGecko API calls are the shared bottleneck |
| listener_mp.py 1 worker faster than 8 (60-row CSV, offline) | Same root cause as row 1 above, one layer down: `mp.Process` spawn cost dominates a sub-ms-per-row workload. |
| listener_mp.py 2 workers at 2.39× (865-row CSV, offline) | Amdahl amortization: same spawn overhead, ~14× more work — per-item compute cost now dominates spawn cost, so workers help. Gains taper at 4+ workers as per-worker share shrinks. See "Ingestion-layer benchmark" above. |

To improve Spark's advantage further:
- Increase Kafka partitions to 8 and workers to 8
- Pre-warm the Redis price cache so workers don't wait on CoinGecko
- Use `--loops 3` in `run_benchmark.py` to average over multiple runs

To see `listener_mp.py`'s advantage (rather than its overhead), run the
live-mode `--rpc-delay` test described in "Ingestion-layer benchmark" above —
this has not yet been run.
