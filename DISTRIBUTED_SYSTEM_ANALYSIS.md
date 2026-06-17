# Distributed System Analysis — Flash Loan Attack Detection

This document maps each core characteristic of a distributed system to
concrete evidence found in this codebase, and provides a practical
demonstration plan for validating each characteristic through experiments,
benchmarks, and live system behaviour — not just theory.

---

## Table of contents

1. [What kind of distributed system is this?](#1-what-kind-of-distributed-system-is-this)
2. [Characteristic 1 — Concurrency](#2-characteristic-1--concurrency)
3. [Characteristic 2 — Message passing (no shared memory)](#3-characteristic-2--message-passing-no-shared-memory)
4. [Characteristic 3 — Fault tolerance](#4-characteristic-3--fault-tolerance)
5. [Characteristic 4 — Scalability](#5-characteristic-4--scalability)
6. [Characteristic 5 — Location transparency](#6-characteristic-5--location-transparency)
7. [Characteristic 6 — No single point of failure](#7-characteristic-6--no-single-point-of-failure)
8. [CAP theorem position](#8-cap-theorem-position)
9. [Demonstration quick-reference](#9-demonstration-quick-reference)

---

## 1. What kind of distributed system is this?

This project is a **fault-tolerant, publish-subscribe event streaming
pipeline** — commonly called a **Kappa Architecture**.

| Distributed system concept | Role in this project |
|---|---|
| Publish-subscribe messaging | Apache Kafka (`raw_txns` topic) |
| Distributed stream processor | PySpark Structured Streaming (1 master + 4 workers) |
| Distributed cache | Redis (price cache, TTL-based LRU) |
| Serving store | MongoDB Atlas |
| Push-based event delivery | FastAPI SSE → React `EventSource` |

**Why Kappa, not Lambda?**
Lambda Architecture has two processing paths: a real-time stream path and
a historical batch reprocessing path. This system has only one path —
`streaming_job.py` handles all detection. There is no separate batch layer.
This is the defining characteristic of Kappa Architecture.

**Full pipeline data flow:**
```
mock_server.py (WebSocket)
    │
    ▼
listener.py / listener_mp.py      ← ingestion layer (Stage 1)
    │  produces to
    ▼
Kafka topic: raw_txns             ← message broker (Stage 2)
  [3 brokers · 4 partitions · replication factor 3]
    │  consumed by
    ▼
PySpark Structured Streaming      ← stream processing (Stage 3)
  [spark-master + 4 spark-workers · 10s micro-batches]
    │               │
    ▼               ▼
Redis           MongoDB Atlas     ← storage (Stage 4)
(price cache)   (transactions)
                    │
                    ▼
              FastAPI :8000        ← backend API (Stage 5)
                    │  SSE stream
                    ▼
              React :5173          ← dashboard (Stage 6)
```

---

## 2. Characteristic 1 — Concurrency

### Definition
Multiple computations execute simultaneously rather than sequentially.

### Where it appears in the codebase

**Spark layer — partition-level parallelism**

`docker-compose.yml` starts four independent `spark-worker` replicas.
The Kafka topic `raw_txns` has 4 partitions. Each micro-batch assigns
one partition per worker, so 4 workers execute their UDF chains
(`decode_flash_loan_udf → symbol_udf → price_udf → confidence_udf`)
at the same time, writing results to MongoDB via `foreachPartition`.

```yaml
# docker-compose.yml — four independent worker containers
spark-worker-1:
  image: flash-loan-attack-detection-spark-worker
spark-worker-2:
  image: flash-loan-attack-detection-spark-worker
spark-worker-3:
  image: flash-loan-attack-detection-spark-worker
spark-worker-4:
  image: flash-loan-attack-detection-spark-worker
```

**Ingestion layer — multi-process fan-out**

`ingestion/listener_mp.py` splits work across OS processes:
- 1 feeder process holds the mempool subscription (or reads the CSV)
- N worker processes each own a separate Web3 connection and Kafka producer
- Work is distributed via `multiprocessing.Queue` — no shared state in the hot path

```
feeder process (PID A) ──► Queue ──► worker-0 (PID B)  → Kafka
                                  ──► worker-1 (PID C)  → Kafka
                                  ──► worker-2 (PID D)  → Kafka
                                  ──► worker-3 (PID E)  → Kafka
```

### How to demonstrate it

**Experiment:** Run `listener_mp.py` in offline benchmark mode, then capture
the Spark UI task timeline.

```bash
# Show concurrent ingestion: 4 distinct PIDs each processing ~15 rows
python ingestion/listener_mp.py --offline --no-kafka --workers 4

# Show Spark concurrency: tasks overlapping in the timeline
# Open http://localhost:4040 → Stages → look for 4 overlapping task bars
```

**What to measure:**
- `listener_mp.py` output: 4 distinct PIDs, sum of worker loads == 60
- Spark UI → Stages tab: screenshot showing 4 tasks with overlapping
  start/end timestamps in the same micro-batch

**Expected result (60-row CSV, 4 workers, `--no-kafka`):**
```
Worker PIDs: [12341, 12342, 12343, 12344]   ← 4 distinct processes
Worker loads: [15, 15, 15, 15]              ← ~equal distribution
detected=35  filtered_out=25  total=60
```

**Important caveat to report honestly:**
The offline benchmark shows *negative* speedup for the 60-row CSV
(1 worker: 0.407 s; 8 workers: 0.483 s). `mp.Process` spawn cost
(each worker re-imports modules and rebuilds ABI decoders) dominates
when per-item work is sub-millisecond. Speedup turns *positive* at
865 rows (workers=2 gives 2.39× speedup: 1519 tx/s vs 637 tx/s).
Report this honestly — it demonstrates understanding of Amdahl's Law:
fixed overhead amortizes over more work.

---

## 3. Characteristic 2 — Message passing (no shared memory)

### Definition
Components communicate only by sending messages through a channel.
No component reads another component's memory directly.

### Where it appears in the codebase

Every stage boundary in the pipeline is a message channel, not a
function call or shared variable:

| From | Channel | To |
|---|---|---|
| `listener.py` | Kafka topic `raw_txns` | `streaming_job.py` |
| `streaming_job.py` | MongoDB write | `FastAPI /stream/detections` |
| `FastAPI` | HTTP SSE stream | React `EventSource` |

The Spark job cannot call `listener.py`. The frontend cannot query Spark
directly. Every component is isolated — it reads from its input channel
and writes to its output channel. This is pure message passing.

**Kafka as the durable message channel:**

```python
# broker/kafka_producer.py — listener writes to the channel
produce_message(producer, topic="raw_txns", key=tx_hash, value=tx_json)

# processing/streaming_job.py — Spark reads from the channel
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "raw_txns") \
    .option("startingOffsets", "earliest") \
    .load()
```

**SSE as the push channel from backend to frontend:**

```python
# backend/Main.py — FastAPI pushes events, never shares state
async def detection_stream():
    while True:
        detections = get_recent_detections(limit=50)
        if fingerprint_changed(detections):
            yield f"event: detections\ndata: {json.dumps(detections)}\n\n"
        else:
            yield ": heartbeat\n\n"
        await asyncio.sleep(1)
```

### How to demonstrate it

**Experiment:** Trace a single `tx_hash` through every stage visually.

```
Step 1: Start full stack + mock_server.py
Step 2: Open Kafka UI at http://localhost:8082
        → Topic: raw_txns → Messages
        → Note a specific tx_hash (e.g. 0xabc123...)

Step 3: Open MongoDB (via MongoDBCompass or pymongo shell)
        → DB: flash_loan_detection → collection: transactions
        → Find the same tx_hash

Step 4: Open React dashboard at http://localhost:5173
        → Find the same tx_hash in the live detections table

Step 5: Open browser DevTools → Network tab → filter by "stream"
        → Click the /stream/detections SSE connection
        → Watch event frames arriving with the same tx_hash
```

**What to measure:**
- Consumer lag in Kafka UI (messages in `raw_txns` minus committed offset
  of `flash_loan_detectors` group) — should be near zero at steady state
- Messages/sec rate in Kafka UI topic details
- SSE event count in DevTools Network tab over 60 seconds

**Expected result:**
The same `tx_hash` visible in Kafka UI, MongoDB, and the React table —
demonstrating 3 independent components that never share memory but
successfully pass the same transaction from source to display.

---

## 4. Characteristic 3 — Fault tolerance

### Definition
The system continues operating correctly after one or more of its
components fail, with no data loss and automatic recovery.

### Where it appears in the codebase

**Kafka — broker replication**

Topic `raw_txns` has `replication_factor=3`. Every partition is stored
on all 3 brokers. When a broker dies, ZooKeeper triggers leader election
for that broker's partitions. A follower from the ISR (in-sync replica)
set is promoted to leader in seconds. No messages are lost because the
idempotent producer (`enable.idempotence=True`) deduplicates any retried
sends by `(producer_id, partition, sequence_number)`.

```python
# broker/kafka_producer.py — idempotent producer config
producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "acks": "all",                                  # all ISR replicas must confirm
    "enable.idempotence": True,                     # dedup retried produces
    "retries": 2147483647,                          # retry until success
    "max.in.flight.requests.per.connection": 5,
}
```

**Spark — checkpoint-based recovery**

`streaming_job.py` uses a persistent checkpoint directory. Every committed
micro-batch writes the processed Kafka offsets to disk. If a Spark worker
crashes mid-batch, the Spark master re-assigns that partition's task to
a surviving worker, resuming from the last committed offset — not from the
beginning of the topic.

```python
# processing/streaming_job.py — checkpoint location
CHECKPOINT_DIR = "/tmp/spark-checkpoints/flash-loan-detection"

mongo_query = df_enriched.writeStream \
    .foreachBatch(write_to_mongo) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime="10 seconds") \
    .start()
```

**Ingestion — RPC failover and reconnect**

`ingestion/listener.py` accepts a primary and fallback WebSocket URL.
On a dropped connection, it retries with exponential backoff (1s, 2s,
4s, 8s, 16s, max 5 attempts). If the primary provider is unhealthy
(connection drops before receiving any transaction), it rotates to
`ETH_WSS_FALLBACK`. A session that received ≥1 transaction keeps its
URL on retry, since the provider is considered healthy.

### How to demonstrate it

**Experiment A — kill a Kafka broker while ingesting:**

```bash
# Terminal 1: full stack running, listener producing to Kafka

# Terminal 2: watch MongoDB document count
watch -n1 'python -c "
import os; from pymongo import MongoClient
c = MongoClient(os.environ[\"MONGODB_URI\"])
print(c.flash_loan_detection.transactions.count_documents({}))
"'

# Terminal 3: kill broker-2 while ingestion is running
docker stop flash-loan-attack-detection-kafka-2-1

# Observe: MongoDB count continues rising. No gap. No duplicate.
# Kafka UI: ISR count for affected partitions drops from 3 → 2, then leader
# re-election completes and producing resumes (typically < 30 s).

# Restart the broker
docker start flash-loan-attack-detection-kafka-2-1
# Kafka UI: ISR recovers to 3.
```

**Experiment B — kill a Spark worker while streaming:**

```bash
# Terminal 1: streaming_job.py running (check http://localhost:4040)

# Terminal 2: count MongoDB docs before kill
BEFORE=$(python -c "
import os; from pymongo import MongoClient
c = MongoClient(os.environ['MONGODB_URI'])
print(c.flash_loan_detection.transactions.count_documents({}))
")

# Kill one worker
docker stop flash-loan-attack-detection-spark-worker-2-1

# Spark UI: workers count drops from 4 → 3.
# Spark master re-assigns the failed task from last checkpoint offset.
# MongoDB count continues increasing.

AFTER=$(python -c "
import os; from pymongo import MongoClient
c = MongoClient(os.environ['MONGODB_URI'])
print(c.flash_loan_detection.transactions.count_documents({}))
")
echo "Before: $BEFORE  After: $AFTER"   # AFTER > BEFORE, no gap
```

**Experiment C — use the existing fault tolerance demo script:**

```bash
# Scenario 1: single-process loses all pending work on crash
python benchmarks/fault_tolerance_demo.py --scenario single

# Scenario 2: Spark recovers from checkpoint, no data lost
python benchmarks/fault_tolerance_demo.py --scenario spark
```

**What to measure:**
- MongoDB document count before vs after broker/worker kill (should be
  equal or greater, never less)
- Time to recovery (seconds from `docker stop` to Kafka UI showing stable ISR)
- `fault_tolerance_demo.py` side-by-side table: single-process = 0 docs
  after crash; Spark = all docs preserved

**Expected result:**
```
                    Single-process    Spark (checkpoint)
Docs before crash:       10                10
Crash at tx #11
Docs after restart:       0                10    ← Spark recovers from checkpoint
Data loss:              YES                NO
Recovery action:    manual restart    automatic re-assignment
```

---

## 5. Characteristic 4 — Scalability

### Definition
Adding more processing nodes increases throughput proportionally,
without redesigning the system.

### Where it appears in the codebase

**Horizontal scaling — Spark workers**

The only change needed to scale is a single Docker Compose flag:

```bash
docker compose up -d --scale spark-worker=8
```

No code changes. No config changes. The Spark master automatically
distributes micro-batch tasks across the new workers.

**The partition ceiling:**
Maximum useful parallelism = number of Kafka partitions. `raw_txns` has
4 partitions. Adding a 5th Spark worker gives it nothing to do in any
given micro-batch (only 4 tasks exist per batch). To scale beyond 4,
increase `KAFKA_NUM_PARTITIONS` in `docker-compose.yml` first.

```yaml
# docker-compose.yml — the ceiling parameter
KAFKA_NUM_PARTITIONS: 4   # increase this to unlock more Spark parallelism
```

**Ingestion scaling — `listener_mp.py --workers N`**

```bash
python ingestion/listener_mp.py --offline --workers 2    # 2 processes
python ingestion/listener_mp.py --offline --workers 4    # 4 processes
python ingestion/listener_mp.py --offline --workers 8    # 8 processes
```

### How to demonstrate it

**Experiment:** Run `run_benchmark.py --mode all` and plot the speedup curve.

```bash
# Runs single-process, Spark 1-worker, 2-worker, 4-worker on same dataset
python benchmarks/run_benchmark.py --mode all

# Output: benchmark_results.csv
# Columns: mode, workers, total_time_s, tx_per_sec, speedup
```

**What to measure:**
- `tx/sec` at 1, 2, and 4 Spark workers
- Speedup ratio vs 1-worker baseline
- Whether throughput plateaus at 4 workers (partition-bound ceiling)

**Expected result shape:**
```
Workers    tx/sec     Speedup
-------    ------     -------
   1         X         1.0×
   2        ~2X        ~2.0×
   4        ~4X        ~4.0×
   8        ~4X        ~4.0×    ← plateau: partition ceiling at 4
```

The plateau at 5+ workers is not a bug — it is evidence that you
understand *why* the system scales the way it does (partition-bound
parallelism). To demonstrate the ceiling shifting, temporarily set
`KAFKA_NUM_PARTITIONS: 8` and `--scale spark-worker=8` and show
throughput continues rising.

**Ingestion benchmark result (already validated):**
```
Dataset: 865-row test_data_large.csv
Workers=1:   ~637 tx/s  (baseline)
Workers=2:   ~1519 tx/s (2.39× speedup)
Workers=4:   ~1143 tx/s (gains taper as spawn overhead amortizes)
```

---

## 6. Characteristic 5 — Location transparency

### Definition
Components address each other by logical name rather than physical IP
address. Moving or renaming a component requires no code changes.

### Where it appears in the codebase

Every inter-component address in this system is a hostname, not an IP.
No Python file contains a hardcoded IP address.

```bash
# Verify: zero hardcoded IPs in all Python source files
grep -r "9092\|6379\|27017" --include="*.py" .
# Expected output: zero matches in src files
# (only .env and docker-compose.yml use these ports, via env vars)
```

**Docker bridge network — `broker_net`:**

```yaml
# docker-compose.yml — all services on a named bridge network
networks:
  broker_net:
    driver: bridge
```

Docker's embedded DNS resolves `kafka-1`, `kafka-2`, `kafka-3`, `redis`,
`spark-master` to their current container IPs. If a container restarts
with a new IP, the hostname still resolves. The application code never
sees the IP.

**Environment-variable addressed services:**

```python
# broker/kafka_producer.py
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS",
                             "kafka-1:9092,kafka-2:9092,kafka-3:9092")

# processing/streaming_job.py
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
MONGO_URI  = os.getenv("MONGODB_URI")

# backend/Main.py
MONGO_URI = os.getenv("MONGODB_URI")
```

**MongoDB Atlas** is a cloud endpoint — the application connects via a
URI string, not an IP address. The actual physical host (AWS, GCP,
or Azure depending on Atlas region) is completely transparent to the code.

### How to demonstrate it

**Experiment A — static grep proof:**

```bash
grep -rn "[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}" \
     --include="*.py" --include="*.ts" --include="*.tsx" .
# Expected: zero IP addresses in application source files
```

**Experiment B — rename a service without touching code:**

```bash
# 1. In docker-compose.yml, rename the redis service:
#    redis: → cache:

# 2. In .env, update:
#    REDIS_HOST=cache

# 3. Restart
docker compose down && docker compose up -d

# 4. System works identically. Zero Python files were changed.
```

**What to show:**
- The `.env` file listing all connection strings as hostnames
- The `docker-compose.yml` services block showing no IP addresses
- The grep output: zero IP hits in `*.py`

---

## 7. Characteristic 6 — No single point of failure

### Definition
No individual component's failure stops the pipeline. The system
degrades gracefully and recovers automatically.

### Where it appears in the codebase

| Component killed | What happens | Recovery mechanism |
|---|---|---|
| `kafka-2` (one broker) | ISR for affected partitions drops from 3→2; new leader elected | ZooKeeper leader election; producing resumes |
| `spark-worker-2` (one worker) | Spark master drops worker; re-assigns its partition tasks | Checkpoint replay from last committed Kafka offset |
| `redis` (price cache) | `price_udf` Redis lookup fails; falls back to CoinGecko | Try/except in `price_udf`; then static price fallback |
| `FastAPI` backend | React shows "disconnected" state; `EventSource` retries | Browser `EventSource` auto-reconnects on `onerror` |

**Kafka ISR guarantee:**

With `replication_factor=3`, the cluster tolerates losing any 1 broker:
- `kafka-1` killed → `kafka-2` or `kafka-3` takes over as partition leader
- `kafka-2` killed → `kafka-1` or `kafka-3` takes over
- `kafka-3` killed → `kafka-1` or `kafka-2` takes over

With 2 of 3 brokers alive, the ISR is still valid and `acks="all"`
continues to be satisfiable.

**React `EventSource` auto-reconnect:**

```typescript
// frontend/src/pages/HomePage.tsx
const es = new EventSource(`${API_URL}/stream/detections`);
es.onerror = () => {
    // Browser automatically retries the SSE connection with backoff
    // No user action required
};
```

### How to demonstrate it

**Live kill sequence (strongest possible demonstration):**

```bash
# 1. Full stack running. React dashboard showing live detections.
#    Screen-record or share screen for the audience.

# 2. Kill broker-2 — dashboard must not stop
docker stop flash-loan-attack-detection-kafka-2-1
# Kafka UI: ISR drops to 2/3. Dashboard: still receiving detections.

# 3. Kill a Spark worker — dashboard must not stop
docker stop flash-loan-attack-detection-spark-worker-2-1
# Spark UI: 3 workers. Dashboard: still receiving detections.

# 4. Restore both
docker start flash-loan-attack-detection-kafka-2-1
docker start flash-loan-attack-detection-spark-worker-2-1
# Kafka UI: ISR returns to 3/3. Spark UI: 4 workers.
```

**What to measure:**
- React dashboard uptime percentage during the kill sequence (should be 100%)
- Time for Kafka ISR to recover from 2 → 3 after broker restart
- Time for Spark worker count to recover from 3 → 4
- MongoDB document count: must be monotonically increasing through all kills

**Contrast with single-process baseline:**

```bash
# Single-process job has a hard SPOF: the process itself
python benchmarks/single_process_job.py --crash-after 10

# After the crash, check MongoDB
python -c "
import os; from pymongo import MongoClient
c = MongoClient(os.environ['MONGODB_URI'])
print(c.flash_loan_detection.transactions_benchmark.count_documents({}))
"
# Result: 0 — all pending work was lost
```

---

## 8. CAP theorem position

This system makes a **CP (Consistency + Partition Tolerance)** choice.

**Consistency preference in Kafka:**
`acks="all"` means the Kafka producer waits for all in-sync replicas to
confirm a write before considering it successful. If a network partition
isolates a broker, the remaining brokers refuse to accept writes until
the partition heals or the ISR is reconfigured. **Consistency is preferred
over availability.**

**Why not AP?**
An AP Kafka configuration would set `acks=1` (leader only), accepting
the risk of data loss if the leader fails before replication. This system
does not do that.

**Implication:**
In a severe partition event (e.g. only 1 of 3 brokers reachable,
`min.insync.replicas=2`), the producer will block and throw a timeout
exception rather than accept a potentially non-replicated write. The
pipeline pauses rather than corrupts data.

---

## 9. Demonstration quick-reference

| # | Characteristic | Command / action | Evidence to capture |
|---|---|---|---|
| 1 | Concurrency | `python ingestion/listener_mp.py --offline --no-kafka --workers 4` | PID table showing 4 distinct processes + Spark UI task timeline screenshot |
| 2 | Message passing | Run full stack; trace one `tx_hash` across Kafka UI → MongoDB → React dashboard | Screenshot of same hash at each stage + DevTools SSE event frames |
| 3 | Fault tolerance | `docker stop flash-loan-attack-detection-kafka-2-1` during live ingestion | MongoDB doc count before == after. Compare vs `fault_tolerance_demo.py --scenario single` |
| 4 | Scalability | `python benchmarks/run_benchmark.py --mode all` | `benchmark_results.csv` speedup curve: 1×→2×→4×, plateau at 4 workers |
| 5 | Transparency | `grep -r "9092\|6379" --include="*.py" .` + rename `redis:` to `cache:` in compose | Zero IP grep hits; system runs unchanged after rename |
| 6 | No SPOF | `docker stop kafka-2` then `docker stop spark-worker-2` while dashboard is live | Screen recording of dashboard staying alive during both kills |

**Strongest overall evidence (ranked):**
1. Screen recording of the no-SPOF kill sequence — system survives failure visibly
2. The `tx_hash` end-to-end trace — concretely shows message passing across 5 stages
3. `run_benchmark.py` speedup curve — quantitative, with a meaningful plateau to explain
4. `fault_tolerance_demo.py` side-by-side table — single-process loses data, Spark does not
5. `listener_mp.py` PID table — shows concurrency numerically with exact counts
