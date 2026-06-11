# Benchmark & Fault Tolerance Demo

This directory contains scripts that demonstrate **why distributed processing
is necessary** by comparing a single-process Python pipeline against the
distributed Spark + Kafka pipeline on the same dataset.

---

## Files

| File | Purpose |
|------|---------|
| `single_process_job.py` | Baseline single-process pipeline (same logic as Spark job) |
| `run_benchmark.py` | Measures throughput (tx/sec) for single-process vs Spark 1/2/4 workers |
| `fault_tolerance_demo.py` | Live demo: crash single-process mid-batch, then crash a Spark worker — shows recovery difference |
| `benchmark_results.csv` | Output from `run_benchmark.py --mode all` (auto-generated) |

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

To improve Spark's advantage further:
- Increase Kafka partitions to 8 and workers to 8
- Pre-warm the Redis price cache so workers don't wait on CoinGecko
- Use `--loops 3` in `run_benchmark.py` to average over multiple runs
