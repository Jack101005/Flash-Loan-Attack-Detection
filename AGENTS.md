# Flash-Loan Attack Detection — Codex Project Brief

Read this file at the start of every conversation. It tells Codex the
current state of the project, what every file does, and how the system
actually works. **This file supersedes the original spec PDF and any
earlier version of AGENTS.md.**

---

## Project summary

A real-time distributed system that monitors the Ethereum mempool for
flash loan transactions. Built by a 5-person team as a university project.

**Pipeline (in order):**
```
Mock Ethereum Node (WebSocket)
    ↓
listener.py  ──► Kafka (3 brokers, 4 partitions, topic: raw_txns)
                        ↓
              PySpark Structured Streaming
              (1 master + 4 workers, 10 s micro-batches)
                 ↙               ↘
         Redis                  MongoDB Atlas
     (price cache)            (detections collection: "transactions")
                                       ↓
                              FastAPI backend  (port 8000)
                                       ↓
                              React frontend  (port 5173)
                          via SSE /stream/detections
```

**Tech stack:** Python, Web3.py, Apache Kafka (confluent-kafka),
PySpark Structured Streaming, Redis, MongoDB Atlas, FastAPI, React (Vite),
React Flow, Tailwind CSS, Docker Compose.

---

## Team responsibilities

| Person | Role | Stage |
|---|---|---|
| Person 1 | System Architect & DevOps | Docker, Kafka, Spark infra |
| Person 2 (Ngan) | Blockchain Data Engineer | Stage 1 — Ingestion, Stage 2 — Kafka |
| Person 3 | Backend Logic & Streaming | Stage 3 — PySpark streaming job |
| Person 4 | Database & State Management | Redis price cache, MongoDB |
| Person 5 | Frontend, API & Integration | FastAPI, React dashboard, SSE |

---

## Current project state (update this section as work progresses)

### Stage 1 — Ingestion ✅ COMPLETE
- `ingestion/listener.py` — WebSocket listener, two-pass filter (contract
  address → function selector), ABI decoding, auto-reconnect with
  exponential backoff (max 5 retries, doubling delay starting at 1 s),
  in-memory dedup set (cap 10 000 hashes), Kafka produce with fallback
  log, `--no-kafka` flag for offline mode, session stats summary.
  **RPC failover:** accepts a list of WebSocket URLs (`ETH_WSS_PRIMARY` +
  `ETH_WSS_FALLBACK` from `.env`). Rotates to the next URL only when a
  session fails before receiving any transactions (unhealthy provider
  signal). A session that received ≥1 tx keeps its URL on retry.
  Override via `--url` / `--fallback-url` CLI flags.
- `ingestion/listener_mp.py` — 🆕 Multi-process variant of `listener.py`
  for ingestion throughput scaling. **Status: structurally validated
  end-to-end on the offline CSV path; not yet load-tested against real
  RPC latency (live mode).** See `ingestion/LISTENER_MP_NOTES.md` for full
  design rationale, the two race conditions found and fixed during
  validation, and benchmark results. Quick summary:
  - **Architecture:** one feeder process holds the single mempool
    subscription (live mode) or reads the CSV (offline mode) and fans
    items out via a `multiprocessing.Queue` to N worker processes. Each
    worker owns its own Web3 connection + Kafka producer (created
    post-fork — neither is picklable). One subscription/reader, many
    fetchers — duplicating the subscription would make every worker
    re-fetch every item.
  - **`--offline` mode:** replays `data/test_data_enriched.csv` directly —
    no WebSocket, no mock server, no Docker, no network at all. Add
    `--no-kafka` to also skip `confluent_kafka` entirely (zero deps).
  - **`--offline --benchmark`:** sweeps workers=1/2/4/8 over the CSV and
    prints a speedup table.
  - Fixed during validation: (1) `--no-kafka` previously still imported
    `confluent_kafka` transitively via an unconditional module-level
    import — now nested inside `if use_kafka:`; (2) shared
    `Manager().dict()["x"] += 1` counters lost 35–55% of increments
    under 4-way contention (non-atomic read-RPC + increment + write-RPC
    across process boundaries) — replaced with per-worker local counters,
    written once each to a shared results map and summed by the parent;
    (3) added a `Barrier(n_workers + 1)` so a zero-delay feeder cannot
    drain the queue before slow-starting workers are ready.
  - **Validated result (60-row CSV, 4 workers, `--no-kafka`):**
    `detected=35`, `filtered_out=25`, `detected + filtered_out == rows`,
    `sum(worker_loads) == rows`, 4 distinct PIDs. Counts are now exact
    and reproducible across runs (work *distribution* across workers
    still varies run-to-run due to OS scheduling — that's expected).
  - **Benchmark finding (offline, no per-item delay):** speedup is
    *negative* — 1 worker is fastest, 8 workers is ~16% slower. This
    mirrors the Spark "1 worker slower than single-process" finding in
    `BENCHMARK.md`: with in-memory CSV rows, `mp.Process` spawn cost
    (tens of ms per worker via `spawn`) dominates a sub-millisecond
    per-row workload. The positive case (multi-process pulls ahead) is
    expected only under realistic RPC latency — use `--rpc-delay` in
    live mode to demonstrate this; not yet run.
- `ingestion/mock_server.py` — JSON-RPC 2.0 mock Ethereum node; serves
  `eth_subscribe`, `eth_getTransactionByHash`, `eth_getBlockByNumber`
  over WebSocket. Supports `--delay`, `--loop`, `--data` flags.
- `ingestion/config.py` — `WATCHLIST` (3 contracts) + `SELECTORS`
  (4 selectors). See Key Constants below.
- `data/test_data_enriched.csv` — enriched dataset, **60 rows total**
  (corrected from the previously-documented 35 — see Known Issues #8).
  Of the 60: **35 rows match a watched contract + selector** (`detected` —
  these are the "35 flash loan transactions" referenced elsewhere in the
  docs/benchmarks), **25 rows are filtered out** (general Aave selectors
  not in `SELECTORS` such as `supply`/`withdraw`/`borrow`/`repay`, one
  empty-`input` Uniswap row, and one Uniswap row whose `input` is an
  ASCII phishing-link payload). Includes `block_timestamp` used by mock
  server for historical price lookups.
- `abis/aave_v3_pool.json` — Aave V3 Pool ABI (flashLoan + flashLoanSimple)
- `abis/balancer_v2_vault.json` — Balancer V2 Vault ABI (flashLoan)
- `abis/uniswap_v3_pool.json` — Uniswap V3 Pool ABI (flash)

### Stage 2 — Kafka ✅ COMPLETE
- 3-broker cluster (`kafka-1`, `kafka-2`, `kafka-3`) in Docker.
  Topic `raw_txns`: 4 partitions, replication factor 3, 1-hour retention.
  Partition default set via `KAFKA_NUM_PARTITIONS: 4` to prevent race-
  condition auto-create at 1 partition.
- `broker/kafka_producer.py` — `create_producer()`, `produce_message()`,
  `flush_producer()`, `ensure_topic()`. Bootstrap: 3 brokers,
  `acks="all"`, `replication_factor=3`.
  **Idempotent producer:** `enable.idempotence=True`, `retries=2147483647`,
  `max.in.flight.requests.per.connection=5`. Broker deduplicates retried
  produces by (PID, partition, seq) — prevents duplicate messages in
  `raw_txns` on network blips or broker leader elections.
- `broker/kafka_consumer.py` — `create_consumer()`, `consume_messages()`
  iterator, `start_consumer_loop()` for lightweight non-Spark consumers.
  Manual offset commit for exactly-once semantics.
- `broker/consumer_groups.py` — group registry (`flash_loan_detectors`,
  `alerting_group`, `debug_consumer`), `describe_group_lag()`,
  `reset_group_to_earliest()`.
- **Note:** `alerting_group` exists in code but no Telegram service was
  implemented. Telegram was scoped out of the final build.

### Stage 3 — PySpark Streaming ✅ COMPLETE
- `processing/streaming_job.py` — PySpark Structured Streaming job
  submitted via `spark-submit` to `spark://spark-master:7077`.
- **Detection method:** selector match + USD notional threshold scoring.
  There is no graph construction, no DFS, no cycle detection. The original
  spec's NetworkX approach was replaced entirely.
- Pipeline inside the job:
  1. `readStream` from Kafka topic `raw_txns` (earliest offsets)
  2. Parse JSON → extract `selector` (first 10 chars of `input`)
  3. Filter rows where `selector` ∈ PROTOCOL_BY_SELECTOR
  4. `decode_flash_loan_udf` — raw calldata → `assets[]` + `amounts[]`
  5. `symbol_udf` — token address → symbol (USDC, USDT, WETH, WBTC, USDe, DAI)
  6. `human_amount_udf` — raw uint256 → human-readable float (decimals-aware)
  7. `price_udf` — Redis hist cache → CoinGecko history API → static fallback.
     Key pattern: `hist_price:{SYMBOL}:{DD-MM-YYYY}`. TTL: 86 400 s.
     **Limitation:** CoinGecko returns daily average, not spot price at tx time.
  8. `confidence_udf` — thresholds: HIGH ≥ $1 M, MEDIUM ≥ $100 K, else LOW
  9. `write_to_mongo` via `foreachBatch → foreachPartition` — distributed
     bulk upsert to MongoDB Atlas (no driver-side collect).
- Trigger: console sink every 5 s, MongoDB sink every 10 s.
- Checkpoint: `/tmp/spark-checkpoints/flash-loan-detection` (Docker volume).
- **Known limitation:** Uniswap V3 flash() decode produces literal strings
  `"token0"` / `"token1"` as asset labels (not real addresses), so symbol
  lookup returns `UNKNOWN`, price = 0, and confidence = LOW for all
  Uniswap flash loans. This is a stub — not yet fixed.

### Stage 4 — Storage & State ✅ COMPLETE
- `storage/mongo_store.py` — singleton `MongoClient`, `init_indexes()`,
  `transactions_collection()`, `get_recent_detections()`, `save_detection()`.
  MongoDB Atlas via TLS + certifi.
- **Collection name:** `transactions` (inside DB `flash_loan_detection`).
- **⚠️ Known ordering issue:** `get_recent_detections()` sorts by
  `detected_at DESCENDING`, but the Spark writer (`_write_partition_to_mongo`)
  never writes `detected_at` — it writes `processed_at` (Unix int). The
  `detected_at` field is only written by `save_detection()`, which is not
  called by Spark. Dashboard ordering may therefore be non-deterministic.
  Fix: change sort field to `processed_at` or `timestamp` in `mongo_store.py`.
- Redis is used by the Spark job for historical price caching only.
  There is no separate price-feed service (it was removed).
  Redis is authenticated (`REDIS_PASS` env var required).

### Stage 5 — Backend API ✅ COMPLETE
- `backend/Main.py` — FastAPI app. Endpoints:
  - `GET  /`                  — liveness check
  - `GET  /health/db`         — MongoDB ping
  - `POST /decode`            — look up a tx hash in MongoDB, return
                                structured `DecodeResponse`
  - `GET  /stream/detections` — SSE endpoint; async generator polls
                                MongoDB every 1 s; sends `event: detections`
                                when fingerprint changes, `: heartbeat` otherwise.
- CORS: all origins allowed (`allow_origins=["*"]`).
- `init_indexes()` called on startup.
- Run: `uvicorn backend.Main:app --host 0.0.0.0 --port 8000 --reload`
  or inside `backend/`: `uv run uvicorn Main:app --reload --port 8000`
- **Note:** `cycle_path` is returned in both `/decode` and `/stream/detections`
  but is always `[]` — the Spark job never computes a cycle path.

### Stage 6 — Frontend Dashboard ✅ COMPLETE
- `frontend/` — React + Vite + TypeScript + Tailwind CSS.
- `src/pages/HomePage.tsx` — connects to SSE `/stream/detections`;
  renders KPI cards + live detections table + ReactFlow topology panel.
- `src/pages/DecodePage.tsx` — manual tx hash lookup via `POST /decode`.
- `src/components/dashboard/KpiCards.tsx` — 4 cards: Active Alerts,
  High Confidence, Medium Confidence, Max Borrowed.
- `src/components/dashboard/LiveDetectionsTable.tsx` — scrollable table,
  click-to-select a transaction.
- `src/components/dashboard/TransactionGraph.tsx` — ReactFlow panel;
  renders a simple 2-node graph (Sender → Pool) for the selected tx.
  **Note:** this is a cosmetic topology view, not a cycle graph. `cycle_path`
  is not used here.
- `src/hooks/useDecoder.ts` + `src/services/decoderService.ts` — decode page logic.
- Run: `cd frontend && npm install && npm run dev`
- API URL hardcoded as `http://localhost:8000` in `HomePage.tsx`.
  `decoderService.ts` reads `VITE_API_URL` env var with same fallback.

### Benchmarks & Fault Tolerance Demo ✅ COMPLETE
- `benchmarks/single_process_job.py` — Baseline pipeline: same decode/enrich/
  score/write logic as `streaming_job.py` but one Python process, no Spark,
  reads CSV directly. Supports `--crash-after N` to simulate a mid-batch crash
  (pending docs lost, no checkpoint). Writes to `transactions_benchmark` collection.
- `benchmarks/run_benchmark.py` — Throughput comparison: runs single-process and
  Spark (1/2/4 workers) on the same dataset, prints tx/sec + speedup table, saves
  `benchmark_results.csv`. Use `--mode all` for full comparison.
- `benchmarks/fault_tolerance_demo.py` — Two-scenario live demo:
  - Scenario 1 (single): crash after 10 tx, show 0 records written, restart from row 0
  - Scenario 2 (spark): kill one worker container, show Spark recovers from checkpoint
  - Prints side-by-side comparison table explaining why Spark recovers and single-process does not
- `benchmarks/BENCHMARK.md` — Methodology, expected outputs, interpretation guide.
  Now also covers the `listener_mp.py` offline ingestion benchmark — see
  the "Ingestion-layer benchmark" section.

### Alerting — ❌ NOT IMPLEMENTED
- Telegram bot was in the original spec but was scoped out.
- `alerting_group` consumer group exists in `consumer_groups.py` but
  nothing consumes it.

---

## ⚠️ Known issues / open TODOs

| # | Issue | File | Fix |
|---|---|---|---|
| 1 | `get_recent_detections()` sorts by `detected_at` but Spark writes `processed_at` | `storage/mongo_store.py` | Change sort key to `processed_at` or `timestamp` |
| 2 | 2 malformed `tx_hash` rows in test CSV (91-char and 41-char) cause 35 Kafka → 33 MongoDB gap | `data/test_data_enriched.csv` | Fix or remove those 2 rows; add validation in `decode_flash_loan_udf` |
| 3 | Uniswap V3 flash() decode produces literal `"token0"`/`"token1"` — always LOW confidence | `processing/streaming_job.py` | Resolve real token addresses from contract or event logs |
| 4 | CoinGecko history API returns daily average price, not spot price at tx timestamp | `processing/streaming_job.py` | Upgrade to CoinGecko Pro range endpoint or Chainlink oracle |
| 5 | `cycle_path` field in API responses is always `[]` | `backend/Main.py`, frontend | Either remove the field or implement real cycle detection |
| 6 | `API_URL` in `HomePage.tsx` is hardcoded — breaks if backend port changes | `frontend/src/pages/HomePage.tsx` | Move to `VITE_API_URL` env var (already done in `decoderService.ts`) |
| 7 | SSE poll interval is 1 s in code; docs/reports say 3 s | `backend/Main.py` | Update docs or change `asyncio.sleep(1)` to match intent |
| 8 | 12 of the 35 "detected" rows in `data/test_data_enriched.csv` (the `0xab9c4b5d` / Aave V3 `flashLoan` rows, NOT `flashLoanSimple`) fail ABI decode with `InvalidPointer` against the real Aave V3 ABI — decode is enrichment-only so detection/Kafka produce still succeeds, but `assets=[...]` is replaced with `decode=FAILED(InvalidPointer)` in logs for these rows. The other 23 detected rows (2× `flashLoan`, 19× `flashLoanSimple`, 4× Uniswap `flash`) decode cleanly, which rules out an ABI/config problem — points to malformed/synthetic calldata for just those 12 rows | `data/test_data_enriched.csv` | Identify the 12 affected `tx_hash` values (selector `0xab9c4b5d`, decode=FAILED) and either fix their `input` field against real mainnet calldata or document them as intentionally-synthetic test rows |

---

## File map (what every file does)

```
Flash-Loan-Attack-Detection/
│
├── AGENTS.md                        ← THIS FILE (read at session start)
├── README.md                        ← Full setup guide with port reference
├── requirements.txt                 ← Root Python deps (web3, confluent-kafka, etc.)
├── docker-compose.yml               ← Full stack orchestration (root-level, ACTIVE)
├── .env                             ← Secrets: ALCHEMY_RPC_URL, ETH_WSS_PRIMARY,
│                                       ETH_WSS_FALLBACK, REDIS_PASS,
│                                       MONGODB_URI, MONGODB_FLASHLOAN_NAME
├── clear_mongo.py                   ← Dev util: wipe MongoDB transactions collection
│
├── abis/                            ← Ethereum contract ABIs
│   ├── aave_v3_pool.json            ← flashLoan (0xab9c4b5d) + flashLoanSimple (0x42b0b77c)
│   ├── balancer_v2_vault.json       ← flashLoan (0x5c38449e)
│   └── uniswap_v3_pool.json         ← flash (0x490e6cbc)
│
├── ingestion/                       ← Stage 1+2 (Person 2 — COMPLETE)
│   ├── config.py                    ← WATCHLIST addresses + SELECTORS dict
│   ├── listener.py                  ← Main WebSocket listener + Kafka produce
│   ├── listener_mp.py               ← 🆕 Multi-process listener (live + --offline modes)
│   ├── LISTENER_MP_NOTES.md         ← 🆕 Design rationale, race conditions, benchmark log
│   ├── mock_server.py               ← Local JSON-RPC 2.0 mock Ethereum node
│   └── data/
│       └── detected_flash_loans.csv ← detection output sample (NOT the test input)
│
├── broker/                          ← Stage 2 (Person 2 — COMPLETE)
│   ├── kafka_producer.py            ← create_producer(), produce_message(), ensure_topic()
│   ├── kafka_consumer.py            ← create_consumer(), consume_messages(), start_consumer_loop()
│   └── consumer_groups.py           ← group registry, describe_group_lag(), reset helper
│
├── processing/                      ← Stage 3 (Person 3 — COMPLETE)
│   ├── streaming_job.py             ← PySpark Structured Streaming job (main logic)
│   ├── Dockerfile                   ← Driver image (spark-submit entry point)
│   ├── Dockerfile.worker            ← Worker image (4 replicas)
│   └── test_distributed_write.py   ← Dev test for MongoDB distributed write
│
├── storage/                         ← Stage 4 (Person 4 — COMPLETE)
│   ├── mongo_store.py               ← MongoClient singleton, collection helpers,
│   │                                   init_indexes(), get_recent_detections()
│   └── Dockerfile                   ← (present but not used in current compose)
│
├── backend/                         ← Stage 5 (Person 5 — COMPLETE)
│   ├── Main.py                      ← FastAPI app: /, /health/db, /decode, /stream/detections
│   ├── pyproject.toml               ← uv-managed Python deps for backend
│   ├── uv.lock                      ← Locked dependency graph
│   └── README.md                    ← Backend-specific setup (uv workflow)
│
├── frontend/                        ← Stage 6 (Person 5 — COMPLETE)
│   ├── src/
│   │   ├── pages/
│   │   │   ├── HomePage.tsx         ← Main dashboard (SSE + KPI + table + graph)
│   │   │   └── DecodePage.tsx       ← Manual tx hash decode page
│   │   ├── components/
│   │   │   ├── dashboard/
│   │   │   │   ├── KpiCards.tsx         ← 4 KPI stat cards
│   │   │   │   ├── LiveDetectionsTable.tsx ← Scrollable detections table
│   │   │   │   └── TransactionGraph.tsx ← ReactFlow 2-node topology panel
│   │   │   ├── decode/
│   │   │   │   ├── DecodeForm.tsx
│   │   │   │   └── DecodeResult.tsx
│   │   │   └── layout/DashboardLayout.tsx
│   │   ├── hooks/useDecoder.ts      ← Decode page state logic
│   │   └── services/decoderService.ts ← POST /decode fetch wrapper
│   ├── package.json
│   └── vite.config.ts
│
├── benchmarks/                      ← Distributed vs non-distributed comparison
│   ├── single_process_job.py        ← Baseline: same pipeline logic, no Spark/Kafka
│   ├── run_benchmark.py             ← Throughput: single-process vs Spark 1/2/4 workers
│   ├── fault_tolerance_demo.py      ← Crash demo: single-process loses state, Spark recovers
│   ├── BENCHMARK.md                 ← How to run + interpret results (Spark + listener_mp)
│   └── benchmark_results.csv        ← Auto-generated by run_benchmark.py
│
├── data/                            ← ACTIVE test dataset location
│   └── test_data_enriched.csv       ← 60 rows total: 35 detected (flash loan calls
│                                       matching WATCHLIST+SELECTORS) + 25 filtered out
│                                       (general Aave selectors, empty-input row,
│                                       phishing-link row). Use data/, NOT ingestion/data/.
│
└── ngan/                            ← Python virtualenv (vendor packages — IGNORE)
```

---

## Key constants (refer to these, don't guess)

**Watched contract addresses (WATCHLIST in `ingestion/config.py`):**
- `0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2` → Aave V3 Pool
- `0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8` → Uniswap V3 USDC/WETH Pool
- `0xba12222222228d8ba445958a75a0704d566bf2c8` → Balancer V2 Vault

**Flash loan function selectors (SELECTORS in `ingestion/config.py`):**
- `0xab9c4b5d` → Aave V3 flashLoan
- `0x42b0b77c` → Aave V3 flashLoanSimple
- `0x5c38449e` → Balancer V2 flashLoan
- `0x490e6cbc` → Uniswap V3 flash

**Kafka:**
- Topic: `raw_txns` (4 partitions, replication 3, retention 1 hour)
  ⚠️ The README mentions `raw_transactions` in one place — that is wrong.
  The actual topic name used everywhere in code is **`raw_txns`**.
- Consumer groups: `flash_loan_detectors` (Spark), `alerting_group`
  (unused), `debug_consumer` (dev)
- Host bootstrap: `localhost:9094,localhost:9095,localhost:9096`
- Docker bootstrap: `kafka-1:9092,kafka-2:9092,kafka-3:9092`

**MongoDB Atlas:**
- DB: `flash_loan_detection` (env: `MONGODB_FLASHLOAN_NAME`)
- Collection: `transactions`
- Fields written by Spark: `tx_hash`, `protocol`, `from`, `pool`,
  `token`, `amount_human`, `amount_usd`, `confidence`, `timestamp`,
  `batch_id`, `processed_at`
- Fields expected by `mongo_store.py` indexes (NOT written by Spark):
  `block_number`, `detected_at`, `risk_level` — these indexes exist
  but are never populated by the streaming job.

**Redis:**
- Host: `localhost:6379` (from host) / `redis` (Docker)
- Password: `redis_dev_pass` (env: `REDIS_PASS`) — **required**
- Key pattern: `hist_price:{SYMBOL}:{DD-MM-YYYY}` (e.g. `hist_price:WETH:16-04-2026`)
- TTL: 86 400 s (1 day)
- Supported symbols for price lookup: WETH, WBTC (stablecoins return 1.0 directly)

**Confidence scoring (streaming_job.py):**
- HIGH   : amount_usd ≥ $1 000 000
- MEDIUM : amount_usd ≥ $100 000
- LOW    : amount_usd < $100 000 (or price unavailable)

---

## How to run locally

```powershell
# ── Step 1: Create .env at project root ──────────────────────────────────────
# ALCHEMY_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<KEY>
# ETH_WSS_PRIMARY=wss://eth-mainnet.g.alchemy.com/v2/<KEY>   # primary RPC
# ETH_WSS_FALLBACK=wss://ethereum-rpc.publicnode.com          # failover RPC
# REDIS_HOST=localhost
# REDIS_PORT=6379
# REDIS_PASS=redis_dev_pass
# MONGODB_URI=mongodb+srv://...
# MONGODB_FLASHLOAN_NAME=flash_loan_detection

# ── Step 2: Start all infrastructure + Spark job ─────────────────────────────
cd Flash-Loan-Attack-Detection
docker compose up -d --build
# Wait ~30s for brokers to elect leaders and Spark to register workers

# ── Step 3: Start the mock Ethereum node ─────────────────────────────────────
# Terminal A
python ingestion/mock_server.py
# Options: --delay 2.0 --loop --data data/test_data_enriched.csv

# ── Step 4: Start the ingestion listener ─────────────────────────────────────
# Terminal B
python ingestion/listener.py                          # uses ETH_WSS_PRIMARY + ETH_WSS_FALLBACK from .env
python ingestion/listener.py --url ws://localhost:8765  # mock server override
python ingestion/listener.py --no-kafka               # print-only, no Docker needed

# ── Step 4b: Or use the multi-process listener ───────────────────────────────
python ingestion/listener_mp.py --offline --no-kafka --workers 4   # no Docker, no mock server
python ingestion/listener_mp.py --offline --benchmark --no-kafka   # worker-count sweep
python ingestion/listener_mp.py --url ws://localhost:8765          # live, mock server
python ingestion/listener_mp.py --url ws://localhost:8765 --rpc-delay 0.1  # simulate RPC latency

# ── Step 5: Start the backend API ────────────────────────────────────────────
# Terminal C
uvicorn backend.Main:app --host 0.0.0.0 --port 8000 --reload
# or inside backend/: uv run uvicorn Main:app --reload --port 8000

# ── Step 6: Start the frontend ───────────────────────────────────────────────
# Terminal D
cd frontend && npm install && npm run dev

# ── Kafka smoke tests ─────────────────────────────────────────────────────────
python broker/kafka_producer.py          # send 1 test message
python broker/kafka_consumer.py          # consume from earliest (debug_consumer group)
python broker/consumer_groups.py         # print lag for all groups

# ── Clear all MongoDB detections ──────────────────────────────────────────────
python clear_mongo.py
```

---

## Service URLs (when full stack is running)

| Service | URL | Notes |
|---|---|---|
| React Dashboard | http://localhost:5173 | Flash loan alerts + decode page |
| FastAPI Swagger | http://localhost:8000/docs | API explorer |
| Kafka UI | http://localhost:8082 | Browse topics, messages, consumer lag |
| Spark Master UI | http://localhost:8080 | Cluster overview, 4 workers |
| Spark App UI | http://localhost:4040 | Streaming job details |
| RedisInsight | http://localhost:5540 | Redis key browser (add DB: host=redis, port=6379, pass=redis_dev_pass) |

---

## Kafka / Spark mental model (quick reference)

**Why 3 brokers + replication 3?** Every partition is stored on all 3
brokers (leader + 2 followers). If 1 broker dies, Kafka elects a new
leader — no data loss, no pipeline pause.

**Why 4 partitions + 4 Spark workers?** 1 Kafka partition = 1 Spark
task per micro-batch. With 4 partitions and 4 workers, every worker
processes exactly 1 partition in parallel. Scaling workers beyond 4
gives no additional parallelism on `raw_txns` without increasing partitions.

**Spark task assignment:** Spark Master re-assigns tasks every micro-batch;
a worker does not own a fixed partition. A crashed worker's task is picked
up by another worker from the last committed Kafka offset (checkpoint).

**Race condition fix:** `KAFKA_NUM_PARTITIONS: 4` in docker-compose ensures
any auto-created topic gets 4 partitions. Without this, if PySpark subscribes
before `ensure_topic()` runs, Kafka auto-creates at 1 partition and it cannot
be altered without deleting the topic.

---

## Multi-process mental model (listener_mp.py, quick reference)

**Why one feeder + N workers, not N independent subscriptions?** The
mempool subscription (live) or CSV read (offline) is cheap; the expensive
part is the per-item `get_transaction` RPC round-trip (live) or
filter+decode (offline). Duplicating the subscription would make every
worker see every item and redo the expensive part N times — strictly
worse. One feeder distributes items via a `multiprocessing.Queue`; N
workers each do their own expensive part in parallel.

**Why per-worker local counters instead of a shared `Manager().dict()`
with `+= 1`?** That pattern is read-RPC + increment + write-RPC across
process boundaries — NOT atomic. Confirmed empirically: 4 processes each
incrementing a shared counter 50 times (200 total) landed at 90–122,
i.e. 39–55% of increments silently lost. The fix used here: each worker
accumulates a plain local dict (zero IPC in the hot loop) and writes it
ONCE to a shared results map after draining its `_STOP` sentinel; the
parent sums per-worker dicts after `join()`. This is also strictly faster
(no per-item Manager round-trip).

**Why a `Barrier(n_workers + 1)`?** With zero per-item delay, a feeder can
enqueue + the OS can schedule faster than `mp.Process(start_method="spawn")`
workers finish spawning and connecting. Confirmed: without the barrier, 11
items split 9/2/0/0 across 4 workers (2 workers got nothing). The barrier
makes the feeder wait until every worker has signalled readiness before
the first item is enqueued. Work *distribution* after that point still
varies run-to-run (OS scheduling on `Queue.get()`) — only the *totals*
(`detected + filtered_out == rows`, `sum(worker_loads) == rows`) are
guaranteed deterministic.

**Why does more workers = slower in the offline benchmark?** Same root
cause as "Spark 1 worker slower than single-process" above: `mp.Process`
spawn cost (tens of ms per worker with `spawn`, since each re-imports the
module and rebuilds the Web3 ABI decoders) is fixed overhead per worker.
At 60 rows with sub-ms per-row work, N workers pay N× that overhead to
parallelize <0.5s of total work. This inverts once per-item cost is large
relative to spawn cost — i.e. real RPC fetches (50–200ms each), which
`--rpc-delay` simulates in live mode.

---

## Detection architecture (actual, not spec)

The spec PDF describes graph construction + DFS cycle detection. **That
design was not implemented.** The actual detection logic in
`streaming_job.py` works as follows:

```
Raw calldata
    ↓
decode_flash_loan_udf   (parse ABI-encoded assets + amounts from input hex)
    ↓
symbol_udf + human_amount_udf   (address → symbol, uint256 → human float)
    ↓
price_udf   (Redis hist cache → CoinGecko daily average → static fallback)
    ↓
amount_usd = primary_amount_human × price_usd
    ↓
confidence_udf   (HIGH / MEDIUM / LOW by USD threshold)
    ↓
foreachPartition → MongoDB Atlas bulk upsert
```

There is no cycle path, no graph, no arbitrage profit estimation, and
no price-deviation scoring. The `cycle_path` field returned by the API
is always `[]`.

---

## Skill routing — which skill file to read for each task

When the user asks about one of these topics, read the corresponding
markdown file before responding (these are session notes, not formal
skill files):

| User says...                                              | Read file                              |
|-----------------------------------------------------------|----------------------------------------|
| Kafka, producer, broker, raw_txns, consumer group         | `walkthrough.md`                       |
| Spark, streaming, UDF, processing, micro-batch            | `spark-ui-walkthrough.md`              |
| SSE, EventSource, /stream/detections, polling             | `sse_implementation_report.md`         |
| Redis auth, price valuation, CoinGecko, fallback price    | `session-debug-price-valuation.md`     |
| Polling→SSE plan, implementation_plan                     | `implementation_plan.md`               |
| listener_mp, multi-process, --offline, fan-out, worker pool, race condition (counters) | `ingestion/LISTENER_MP_NOTES.md` |
