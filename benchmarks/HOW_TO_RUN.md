# How to Run the Benchmark & Fault Tolerance Demo

Work through the sections in order. Sections 1–2 require **no Docker**.
Sections 3–5 require **Docker Desktop running**.

---

## Prerequisites

```powershell
# Activate the project venv first — every command in this guide assumes it
cd D:\ThirdYear\second_semester\Flash-Loan-Attack-Detection
ngan\Scripts\activate

# Verify required packages
python -c "import pymongo, certifi, dotenv, redis, requests; print('all OK')"
# If any are missing:
pip install python-dotenv pymongo certifi redis requests
```

Check your `.env` in the project root contains:

```env
ETH_WSS_PRIMARY=wss://eth-mainnet.g.alchemy.com/v2/<YOUR_KEY>
ETH_WSS_FALLBACK=wss://ethereum-rpc.publicnode.com
MONGODB_URI=mongodb+srv://<user>:<pass>@cluster0...
MONGODB_FLASHLOAN_NAME=flash_loan_detection
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASS=redis_dev_pass
```

---

## Section 1 — Single-process baseline (no Docker)

This runs the pipeline as a plain Python script: one process, one thread,
reads CSV directly. No Kafka. No Spark.

### 1a. Normal run

```powershell
python benchmarks/single_process_job.py --offline-prices
```

**Expected output:**
```
============================================================
  SINGLE-PROCESS PIPELINE
  Source:     ...data/test_data_enriched.csv
  Batch size: 10
  Prices:     OFFLINE (static fallback)
============================================================

[single] Loaded 60 rows from CSV
[single]   1/60  0xabc123...  HIGH    $ 5,000,000
[single]   2/60  0xdef456...  MEDIUM  $   250,000
...

============================================================
  SINGLE-PROCESS COMPLETE
  Input rows:  60
  Processed:   35        ← 35 flash loans detected
  Written:     35        ← 35 written to MongoDB
  Errors:      25        ← 25 non-flash-loan rows (expected)
  Time:        ~12s
  Throughput:  ~3 tx/sec
============================================================
```

**What to check:**
- `Written` == `Processed` (35). If `Written: 0`, your `MONGODB_URI` is not
  loading — make sure `.env` is in the project root and has the correct value.
- `Errors: 25` is normal — those rows are not flash loans.

### 1b. Quiet mode (for benchmarking — suppresses per-tx lines)

```powershell
python benchmarks/single_process_job.py --offline-prices --quiet
```

### 1c. Live price mode (slow — needs Docker/Redis or CoinGecko calls)

```powershell
python benchmarks/single_process_job.py
```

Takes ~4–10 minutes without Redis because every WETH/WBTC price lookup calls
the rate-limited CoinGecko public API sequentially. Use `--offline-prices` for
all throughput comparisons. Use live mode only to show the real pipeline works
end-to-end.

---

## Section 2 — Fault tolerance demo: single-process (no Docker)

This is **the most important demo** for the report. It proves that a
single-process pipeline has no checkpoint and loses state on crash.

### Step 1 — Run the crash scenario

```powershell
python benchmarks/single_process_job.py --crash-after 10 --delay 0.3
```

Watch the output carefully:

```
[single]   1/60  0x...  HIGH    $5,000,000
[single]   2/60  0x...  HIGH    $3,000,000
...
[single]  10/60  0x...  MEDIUM  $  500,000

[single] ⚡ SIMULATED CRASH after 10 transactions
[single]    10 docs were pending (in-memory, NOT written)
[single]    These records are LOST — no checkpoint exists
[single]    Restart will re-process from row 0
```

**What it shows:**
- The 10 docs were sitting in the `pending` list in RAM
- The batch hadn't hit `batch_size=10` limit yet — so nothing was written
- Crash = all 10 lost

**Check MongoDB count** — open another terminal:
```powershell
python -c "
import os, certifi
from dotenv import load_dotenv
from pymongo import MongoClient
load_dotenv('.env'); load_dotenv('backend/.env')
c = MongoClient(os.getenv('MONGODB_URI'), tls=True, tlsCAFile=certifi.where())
print('records after crash:', c['flash_loan_detection']['transactions_benchmark'].count_documents({}))
"
```
Expected: **0** (nothing was flushed before the crash)

### Step 2 — Restart (watch it start from row 0)

```powershell
python benchmarks/single_process_job.py --offline-prices --delay 0.1
```

Notice it starts again from row 1/60 — not row 11. There is no "resume from".
The `Written: 35` at the end shows it had to redo everything.

### Step 3 — Run the full automated demo

```powershell
python benchmarks/fault_tolerance_demo.py --scenario single
```

This runs both steps above automatically with pauses and prints the summary box.

**Expected summary at end:**
```
┌──────────────────────────────────────────────────────────┐
│  SINGLE-PROCESS FAULT TOLERANCE RESULT                   │
│  Before crash:    ~10 tx processed (batch not flushed)   │
│  MongoDB records:   0 (batch was lost)                   │
│  After restart:    35 records (full re-process)          │
│  ❌ No checkpoint — restart always starts from tx 0      │
│  ❌ In-flight batch (10 docs) was lost on crash          │
│  ❌ Wasted work: all rows re-processed from scratch      │
└──────────────────────────────────────────────────────────┘
```

---

## Section 3 — Start Docker stack

All remaining sections require Docker Desktop running.

```powershell
# Start the full stack (Kafka + Spark + Redis)
docker compose up -d --build

# Wait ~30 seconds, then verify everything is up
docker compose ps
```

All services should show `Up` or `running`. Key ones to check:
- `kafka-1`, `kafka-2`, `kafka-3`
- `spark-master`, `spark-worker` (should show 4 replicas)
- `processing-job`
- `redis`

**Verify Spark has 4 workers:**

Open http://localhost:8080 — under Workers you should see 4 entries.

---

## Section 4 — Throughput benchmark: single-process vs Spark

### 4a. Full comparison (recommended)

```powershell
python benchmarks/run_benchmark.py --mode all
```

This runs sequentially:
1. Single-process baseline (offline prices, ~12s)
2. Spark with 1 worker (scales down, waits, produces txs, measures)
3. Spark with 2 workers
4. Spark with 4 workers

Total runtime: **~10–15 minutes**

**Expected output:**
```
════════════════════════════════════════════════════════════════════════
  BENCHMARK RESULTS — Single-Process vs Distributed Spark
════════════════════════════════════════════════════════════════════════
  Mode                   Workers    Records    Time(s)    tx/sec     Speedup
  ────────────────────────────────────────────────────────────────────
  single_process         1          35         12.00      2.92       1.00x
  spark_1w               1          30         38.00      0.79       0.27x
  spark_2w               2          30         22.00      1.36       0.47x
  spark_4w               4          30         14.00      2.14       0.73x
════════════════════════════════════════════════════════════════════════
```

> **Why does Spark look slower than single-process here?**
>
> Our dataset has only 35 transactions — Spark's JVM startup + Kafka
> roundtrip is a ~15s fixed overhead that dominates at this scale.
> At sustained load (thousands of tx), Spark's parallel processing wins.
> To demonstrate this, run `--loops 3` to replay the dataset 3x:
>
> ```powershell
> python benchmarks/run_benchmark.py --mode all --loops 3
> ```

### 4b. Individual mode (test one config at a time)

```powershell
python benchmarks/run_benchmark.py --mode single
python benchmarks/run_benchmark.py --mode spark --workers 1
python benchmarks/run_benchmark.py --mode spark --workers 2
python benchmarks/run_benchmark.py --mode spark --workers 4
```

### 4c. Results are saved automatically

After `--mode all`, check:
```
benchmarks/benchmark_results.csv
```

---

## Section 5 — Fault tolerance demo: Spark (requires Docker)

### What this proves

Spark commits the Kafka offset to disk at the end of every micro-batch
(`/tmp/spark-checkpoints/flash-loan-detection`). When a worker dies, Spark
master reassigns the unfinished task to a healthy worker, which resumes from
the last committed offset. No transactions lost.

### Run it

```powershell
python benchmarks/fault_tolerance_demo.py --scenario spark
```

**What you will observe:**

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  SCENARIO 2 — Spark (Checkpoint-Based Fault Tolerance)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

[demo] MongoDB 'transactions' cleared. Initial count: 0
[demo] Restarting Spark processing-job (fresh checkpoint)...
[demo] Starting mock Ethereum node + ingestion listener...
[demo] Waiting for first Spark micro-batch to complete (~25s)...

[demo] After first micro-batch: 12 records in MongoDB

[demo] ⚡ KILLING one Spark worker container!
[demo] Killed container: abc123def456
[demo] Worker killed at 21:15:33

[demo] Watching MongoDB for 60 seconds...
[demo] t+ 3s  MongoDB: 12 records (waiting...)
[demo] t+ 6s  MongoDB: 12 records (waiting...)
[demo] t+15s  MongoDB: 20 records (growing ✓)
[demo] t+30s  MongoDB: 30 records (growing ✓)
...

┌──────────────────────────────────────────────────────────┐
│  SPARK FAULT TOLERANCE RESULT                            │
│  Before worker kill:    12 records in MongoDB            │
│  After recovery:        35 records in MongoDB            │
│  New records written:   23                               │
│  ✅ Spark detected worker failure automatically          │
│  ✅ Remaining workers continued from checkpoint          │
│  ✅ Kafka offset preserved — no transactions re-read     │
│  ✅ Processing resumed without manual intervention       │
└──────────────────────────────────────────────────────────┘
```

**Key evidence to point to:**
- MongoDB count continued growing *after* the kill (Spark recovered)
- The count did not reset to 0 (Spark did not re-read from the beginning)
- No manual intervention was needed

### Verify the checkpoint manually

```powershell
# Check the checkpoint directory exists inside the processing-job container
docker exec flash-loan-attack-detection-processing-job-1 ls /tmp/spark-checkpoints/flash-loan-detection/
```

Expected output:
```
commits/   metadata   offsets/   sources/
```

The `offsets/` directory holds the Kafka offset Spark resumes from.

---

## Section 6 — Both scenarios end-to-end

```powershell
python benchmarks/fault_tolerance_demo.py
```

Runs single-process crash (§2) then Spark recovery (§5) back-to-back,
with a final comparison table printed at the end:

```
╔═══════════════════════════════════════════════════════════════╗
║     FAULT TOLERANCE: Single-Process vs Distributed Spark     ║
╠════════════════════════════╦══════════════════════════════════╣
║ Property                   ║ Single-Process │ Spark           ║
╠════════════════════════════╬══════════════════════════════════╣
║ Crash recovery             ║ ❌ Restart row 0  ✅ Checkpoint  ║
║ In-flight data on crash    ║ ❌ Lost           ✅ Preserved    ║
║ Worker failure handling    ║ ❌ Full stop       ✅ Auto-retry  ║
║ State persistence          ║ ❌ None           ✅ Kafka offset ║
║ Manual restart needed      ║ ❌ Yes            ✅ No           ║
║ Duplicate processing risk  ║ ❌ High           ✅ Low (upsert) ║
╚════════════════════════════╩══════════════════════════════════╝
```

---

## Troubleshooting

| Problem | What to check |
|---------|--------------|
| `FileNotFoundError` on CSV | Make sure you're running from the project root (`D:\ThirdYear\second_semester\Flash-Loan-Attack-Detection`), not from inside `benchmarks/` |
| `Written: 0` with no error | `MONGODB_URI` not loaded — run `python -c "from dotenv import load_dotenv; load_dotenv('.env'); import os; print(os.getenv('MONGODB_URI'))"` and confirm it prints your URI |
| `ModuleNotFoundError` | Venv not activated — run `ngan\Scripts\activate` first |
| Spark benchmark hangs at "waiting for records" | `processing-job` may not be running — check `docker compose logs processing-job` |
| Spark benchmark `scale failed` | Docker Compose not running or `docker compose` not on PATH |
| Worker kill has no effect | The container name may differ — run `docker ps --filter name=spark-worker` to find the real names |
| Spark records don't grow after worker kill | Need ≥3 remaining workers to absorb the task — verify 3+ workers are still up at http://localhost:8080 |
