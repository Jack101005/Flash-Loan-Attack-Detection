# Flash-Loan Attack Detection — Claude Project Brief

Read this file at the start of every conversation. It tells Claude the
current state of the project, what every file does, and which skill to
use for each type of task.

---

## Project summary

A real-time distributed system that watches the Ethereum mempool for
flash loan attack transactions. Built by a 5-person team as a university
project. Uses Web3.py → Kafka → Spark → Redis/MongoDB → Telegram/Dashboard.

**Tech stack:** Python, Web3.py, Apache Kafka, PySpark, Redis, MongoDB,
Telegram Bot API, React/Next.js, Docker Compose.

---

## Team responsibilities

| Person | Role | Stage |
|---|---|---|
| Person 1 | System Architect & DevOps | Docker, Kafka, infrastructure |
| Person 2 (Ngan) | Blockchain Data Engineer | Stage 1 — Ingestion, Stage 2 — Kafka wiring |
| Person 3 | Backend Logic & Algorithm | Stages 3, 4, 5 — Processing |
| Person 4 | Database & State Management | Redis, MongoDB |
| Person 5 | Frontend, Alerting, Integration | Telegram, Dashboard, E2E tests |

---

## Current project state (update this section as work progresses)

### Stage 1 — Ingestion ✅ COMPLETE
- `ingestion/listener.py` — WebSocket listener with two-pass filter,
  ABI decoding, auto-reconnect with exponential backoff, dedup set,
  Kafka producer integration (--no-kafka flag for offline mode)
- `ingestion/mock_server.py` — JSON-RPC 2.0 compliant mock Ethereum node
- `ingestion/config.py` — WATCHLIST (3 contracts) + SELECTORS (4 selectors)
- `ingestion/prepare_test_data.py` — fetches real tx calldata from Etherscan CSVs
- `abis/aave_v3_pool.json` — Aave V3 Pool ABI (flashLoan + flashLoanSimple)
- `abis/balancer_v2_vault.json` — Balancer V2 Vault ABI (flashLoan)
- `abis/uniswap_v3_pool.json` — Uniswap V3 Pool ABI (flash)
- `data/test_data.csv` — 60 real transactions (30 flash loans + 30 noise)
- `data/detected_flash_loans.csv` — export for Person 3 (35 detections)
- `data/verification_expected.csv` — answer key for Person 3
- `export_detections.py` — runs detection pipeline and exports CSV
- `tests/test_ingestion.py` — integration tests (all 3 pass)

### Stage 2 — Kafka ✅ COMPLETE
- Person 1 delivered: `infra/Docker-compose.yml` with Kafka on `localhost:9094`
  (host) / `kafka:9092` (Docker internal), topic `raw_txns` auto-created with
  4 partitions and 1-hour retention. Also includes Redis, MongoDB, Spark cluster.
- Person 2 (Ngan) delivered all broker modules and wired listener.py:
  - `broker/kafka_producer.py` — create_producer(), produce_message(),
    flush_producer(), ensure_topic(); graceful fallback if Kafka unreachable
  - `broker/kafka_consumer.py` — create_consumer(), consume_messages()
    iterator, start_consumer_loop() for Person 5's alerting
  - `broker/consumer_groups.py` — group registry (flash_loan_detectors,
    alerting_group, debug_consumer), describe_group_lag(), reset helper
  - `ingestion/listener.py` — TODO placeholder replaced with full Kafka
    produce call; kafka_failures.log fallback; --no-kafka CLI flag
- Verified live: 35 flash loans detected and published to raw_txns topic.
  Kafka connected confirmed in listener output. Decode failures on first
  12 txs (attacker address 0xC6E1aF0...) are expected — see ingestion.md.
- Note: `ingestion`, `price-feed`, `processing-job`, `dashboard` Docker
  services need Dockerfiles before they can build. Start only infra for now:
  `docker compose up -d zookeeper kafka redis mongodb spark-master spark-worker-1 spark-worker-2`

### Stage 3 — Processing ❌ NOT STARTED
- `processing/stream_processor.py` — empty (Person 3's PySpark job)
- `processing/graph_builder.py` — empty (Person 3's NetworkX graph builder)
- `processing/cycle_detector.py` — empty (Person 3's DFS cycle detection)
- `processing/price_monitor.py` — empty (Person 3's price deviation check)
- Person 3 should consume from `raw_txns` topic using `broker/kafka_consumer.py`
  or PySpark's Kafka connector directly (see skills/kafka_integration.md)

### Stage 4/5 — Storage & State ❌ NOT STARTED
- `storage/redis_cache.py` — empty (Person 4's Redis client)
- `storage/mongo_store.py` — empty (Person 4's MongoDB writer)
- Redis running on `localhost:6379`, MongoDB on `localhost:27017` (via Docker)

### Stage 6 — Alerting ❌ NOT STARTED
- `alerting/telegram_bot.py` — empty (Person 5)
- `alerting/react_dashboard/` — empty (Person 5)
- `alerting/grafana_dashboard/` — empty (Person 5)
- Person 5 should use `start_consumer_loop()` from `broker/kafka_consumer.py`
  with `group_id="alerting_group"` to receive detections

---

## File map (what every file does)

```
Flash-Loan-Attack-Detection/
│
├── CLAUDE.md                        ← THIS FILE (read at session start)
├── README.md                        ← Project overview for team
├── requirements.txt                 ← Python dependencies
├── docker-compose.yml               ← Main orchestration (root-level, currently empty)
├── config.yaml                      ← Shared config (currently empty)
├── export_detections.py             ← Runs pipeline, exports detected_flash_loans.csv
│
├── infra/                           ← Person 1 — COMPLETE
│   └── Docker-compose.yml           ← Full stack: Kafka, Zookeeper, Redis, MongoDB,
│                                       Spark master+workers, ingestion, price-feed,
│                                       processing-job, dashboard containers
│
├── ingestion/                       ← Stage 1+2 (Person 2 — COMPLETE)
│   ├── config.py                    ← WATCHLIST addresses + SELECTORS dict
│   ├── listener.py                  ← Main WebSocket listener + Kafka producer wiring
│   ├── mock_server.py               ← Local test server (JSON-RPC 2.0)
│   └── prepare_test_data.py         ← Fetch calldata from public RPCs
│
├── abis/                            ← Contract ABIs for decoding
│   ├── aave_v3_pool.json            ← flashLoan (0xab9c4b5d) + flashLoanSimple (0x42b0b77c)
│   ├── balancer_v2_vault.json       ← flashLoan (0x5c38449e)
│   └── uniswap_v3_pool.json         ← flash (0x490e6cbc)
│
├── broker/                          ← Stage 2 (Person 2 — COMPLETE)
│   ├── kafka_producer.py            ← create_producer(), produce_message(), flush_producer()
│   ├── kafka_consumer.py            ← create_consumer(), consume_messages(), start_consumer_loop()
│   └── consumer_groups.py           ← group registry + describe_group_lag() + reset helper
│
├── processing/                      ← Stage 3 (Person 3 — NOT STARTED)
│   ├── stream_processor.py          ← PySpark Structured Streaming job
│   ├── graph_builder.py             ← NetworkX DiGraph construction
│   ├── cycle_detector.py            ← DFS cycle detection + confidence scoring
│   └── price_monitor.py             ← Redis price deviation check
│
├── storage/                         ← Person 4 — NOT STARTED
│   ├── redis_cache.py               ← get_price() / set_price() / dedup
│   └── mongo_store.py               ← write_detection() to MongoDB
│
├── alerting/                        ← Person 5 — NOT STARTED
│   ├── telegram_bot.py              ← send_alert() via Telegram Bot API
│   ├── react_dashboard/             ← Live detection dashboard
│   └── grafana_dashboard/           ← Metrics panels
│
├── data/                            ← Test data + runtime logs
│   ├── test_data.csv                ← 60 real txs (30 flash loans + 30 noise)
│   ├── detected_flash_loans.csv     ← 35 detections from export_detections.py
│   ├── verification_expected.csv   ← 23 decoded rows for Person 3 to verify
│   └── kafka_failures.log           ← Auto-created: messages that failed to produce
│
├── etherscan_exports/               ← Raw Etherscan CSV downloads
│
├── tests/                           ← Integration tests
│   └── test_ingestion.py            ← 3 tests: detection, dedup, reconnect (all pass)
│
├── utils/                           ← Shared utilities (currently empty)
│
└── skills/                          ← Claude workflow guides (read before responding)
    ├── ingestion.md                 ← Stage 1 workflows + common tasks
    ├── kafka_integration.md         ← Stage 2 complete — how broker modules work
    ├── processing.md                ← Stage 3/4/5 implementation guides
    ├── storage.md                   ← Redis + MongoDB interfaces
    ├── alerting.md                  ← Telegram + dashboard requirements
    ├── testing.md                   ← How to run/fix/add tests
    ├── devops.md                    ← Docker, Kafka setup, Person 1 tasks
    └── debugging.md                 ← Common errors and how to fix them
```

---

## Key constants (refer to these, don't guess)

**Watched contract addresses (WATCHLIST):**
- `0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2` → Aave V3 Pool
- `0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8` → Uniswap V3 USDC/WETH
- `0xba12222222228d8ba445958a75a0704d566bf2c8` → Balancer V2 Vault

**Flash loan function selectors (SELECTORS):**
- `0xab9c4b5d` → Aave V3 flashLoan
- `0x42b0b77c` → Aave V3 flashLoanSimple
- `0x5c38449e` → Balancer V2 flashLoan
- `0x490e6cbc` → Uniswap V3 flash

**Kafka topics:**
- `raw_txns` — 4 partitions, key = tx_hash, retention = 1 hour
- `suspicious_txns` — detections from Stage 3 (not yet implemented)
- `decode_failures` — failed decode rows (not yet implemented)

**Kafka bootstrap servers:**
- Host machine: `localhost:9094`
- Inside Docker network: `kafka:9092`

**MongoDB:** `localhost:27017`, collection = `detections`
**Redis:** `localhost:6379`, key pattern = `price:{token_address}`, TTL = 30s

---

## How to run locally

```powershell
# Start infrastructure (Docker Desktop must be open)
cd infra
docker compose up -d zookeeper kafka redis mongodb spark-master spark-worker-1 spark-worker-2

# Terminal 1 — start mock Ethereum node
python ingestion/mock_server.py

# Terminal 2 — start listener (with Kafka)
python ingestion/listener.py

# Terminal 2 — start listener (without Kafka / Docker)
python ingestion/listener.py --no-kafka

# Run integration tests (self-contained, no Docker needed)
python tests/test_ingestion.py

# Export detections to CSV for Person 3
python export_detections.py

# Smoke test Kafka producer
python broker/kafka_producer.py

# Smoke test Kafka consumer
python broker/kafka_consumer.py
```

---

## Skill routing — which skill file to read for each task

When the user asks about one of these topics, read the corresponding
skill file before responding:

| User says...                                                   | Read skill file              |
|----------------------------------------------------------------|------------------------------|
| listener, WebSocket, filter, selector, reconnect, mock server  | `skills/ingestion.md`        |
| Kafka, producer, broker, raw_txns, consumer group             | `skills/kafka_integration.md`|
| Spark, graph, cycle detection, PySpark, processing            | `skills/processing.md`       |
| Redis, MongoDB, price feed, storage, database, Person 4       | `skills/storage.md`          |
| Telegram, dashboard, alerting, React, Person 5                | `skills/alerting.md`         |
| run tests, test failing, fix test, pytest                     | `skills/testing.md`          |
| Docker, docker-compose, infrastructure, Person 1              | `skills/devops.md`           |
| bug, error, not working, crash, exception                     | `skills/debugging.md`        |
