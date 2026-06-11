# ⚡ Flash Loan Attack Detection System

A real-time blockchain monitoring system that detects flash loan attacks using **Kafka**, **PySpark Structured Streaming**, **Redis**, and **MongoDB Atlas**. Transactions are ingested from Ethereum (or a mock node), streamed through a distributed pipeline, enriched with token price data, scored for maliciousness, and surfaced on a React dashboard.

---

## 🏗️ Architecture Overview

```
Mock Ethereum Node (WebSocket)
        │
        ▼
  listener.py  ──── publishes raw txs ──►  Kafka (3 brokers, 4 partitions)
                                                    │
                                                    ▼
                                        PySpark Structured Streaming
                                        (1 master + 4 workers)
                                                    │
                                       ┌────────────┴────────────┐
                                       ▼                         ▼
                                 Redis (price cache)     MongoDB Atlas (alerts)
                                                                  │
                                                                  ▼
                                                         FastAPI backend
                                                                  │
                                                                  ▼
                                                         React frontend (Vite)
```

---

## 📋 Prerequisites

Make sure the following are installed:

| Tool | Version | Notes |
|------|---------|-------|
| **Docker Desktop** | ≥ 4.x | Must be running |
| **Python** | ≥ 3.11 | For ingestion scripts & backend |
| **Node.js** | ≥ 18.x | For React frontend |
| **uv** (optional) | latest | Fast Python package manager for backend |

> **Windows users:** All commands below are for **PowerShell**. Run as Administrator if Docker commands fail.

---

## 🚀 Quick Start (from scratch)

### Step 1 — Clone & configure environment

```powershell
git clone <your-repo-url>
cd Flash-Loan-Attack-Detection
```

Create a `.env` file in the **root** of the project:

```env
# Alchemy HTTP URL (used by backend price lookups)
ALCHEMY_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/<YOUR_API_KEY>

# WebSocket RPC — primary provider (Alchemy) + passive failover
ETH_WSS_PRIMARY=wss://eth-mainnet.g.alchemy.com/v2/<YOUR_API_KEY>
ETH_WSS_FALLBACK=wss://ethereum-rpc.publicnode.com

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASS=redis_dev_pass

# MongoDB Atlas
MONGODB_URI=mongodb+srv://<user>:<pass>@cluster0.xxxx.mongodb.net/?appName=Cluster0
MONGODB_FLASHLOAN_NAME=flash_loan_detection
MONGODB_TRANSACTIONS_NAME=defi_transactions
```

> ⚠️ The `.env` is already present if you cloned with the file. Update values as needed.

---

### Step 2 — Start all infrastructure (Docker)

This single command starts **Zookeeper**, **3 Kafka brokers**, **Kafka UI**, **Redis**, **RedisInsight**, **Spark Master**, **4 Spark Workers**, and the **PySpark streaming job**:

```powershell
docker compose up -d --build
```

Wait ~30 seconds for all services to be healthy, then verify:

```powershell
docker compose ps
```

All containers should show **`running`** or **`Up`** status.

To stop and remove all containers + volumes:

```powershell
docker compose down -v
```

---

### Step 3 — Start the ingestion pipeline

Open **two separate terminals**:

**Terminal 1 — Mock Ethereum Node** (replays test transactions via WebSocket):

```powershell
python ingestion/mock_server.py
```

Options:
```
--data data/test_data_enriched.csv             # default dataset
--delay 2.0                                    # seconds between each tx push
--loop                                         # replay dataset forever
```

**Terminal 2 — Listener** (subscribes to the mock node and publishes to Kafka):

```powershell
python ingestion/listener.py                          # uses ETH_WSS_PRIMARY + ETH_WSS_FALLBACK from .env
python ingestion/listener.py --url ws://localhost:8765  # mock server (development)
python ingestion/listener.py --no-kafka               # print-only, no Docker needed
```

Options:
```
--url <wss://...>          Primary WebSocket RPC URL (overrides ETH_WSS_PRIMARY)
--fallback-url <wss://...> Failover URL (overrides ETH_WSS_FALLBACK)
--max-retries N            Max reconnection attempts before exit (default: 5)
--no-kafka                 Disable Kafka — print detections only
```

You should see logs like:
```
[listener] RPC providers: 2 (primary=wss://eth-mainnet..., fallback=wss://ethereum-rpc...)
[listener] Connected to wss://eth-mainnet.g.alchemy.com/v2/...
[listener] Kafka: connected
[listener] Listening for flash loans...
```

---

### Step 4 — Start the backend API

```powershell
uvicorn backend.Main:app --host 0.0.0.0 --port 8000 --reload
```

API will be available at: **http://localhost:8000**  
Swagger docs: **http://localhost:8000/docs**

> **First run:** Install dependencies with `uv sync` (inside `backend/`) or `pip install -r requirements.txt` from the project root.

---

### Step 5 — Start the frontend dashboard

```powershell
cd frontend
npm install        # only needed first time
npm run dev
```

Dashboard available at: **http://localhost:5173**

---

## 🖥️ UI Dashboards

Once the stack is running, access each monitoring UI:

| Service | URL | Description |
|---------|-----|-------------|
| **React Dashboard** | http://localhost:5173 | Flash loan alerts & real-time monitoring |
| **FastAPI Swagger** | http://localhost:8000/docs | REST API explorer |
| **Kafka UI** | http://localhost:8082 | Browse topics, messages, consumer groups |
| **Spark Master UI** | http://localhost:8080 | Cluster overview, worker health |
| **Spark App UI** | http://localhost:4040 | Running job stages, tasks, executors |
| **RedisInsight** | http://localhost:5540 | Visual Redis key browser & monitor |

---

## 🔍 Accessing Each UI

### Kafka UI — `http://localhost:8082`

Provectus Kafka UI lets you inspect your Kafka cluster at a glance.

**What to look for:**
1. Open **Topics** → `raw_transactions` — see messages being produced by `listener.py`
2. Click a message to see the raw JSON transaction payload
3. **Consumer Groups** → check lag on the `spark-streaming` group
4. **Brokers** — verify all 3 brokers (`kafka-1`, `kafka-2`, `kafka-3`) are online

```
Cluster: local
Brokers:  kafka-1:9092, kafka-2:9092, kafka-3:9092
Partitions per topic: 4
Replication factor:   3
```

---

### Spark Master UI — `http://localhost:8080`

Apache Spark's built-in cluster UI.

**What to look for:**
- **Workers** section: should show **4 alive workers**, each with 1 core / 1 GB memory
- **Running Applications**: the `FlashLoanDetection` streaming job should appear here
- Click the application name → opens the **Spark App UI (port 4040)**

---

### Spark App UI — `http://localhost:4040`

The Spark Structured Streaming job detail view.

**What to look for:**
- **Streaming** tab → micro-batch duration, input rate, processing rate
- **Stages** tab → see task distribution across the 4 workers
- **Executors** tab → per-worker memory & CPU usage
- **SQL/DataFrame** tab → query plan for the streaming pipeline

> 💡 Port 4040 is exposed from the `processing-job` container. If another Spark app is also running, it may use 4041, 4042, etc.

---

### RedisInsight — `http://localhost:5540`

A visual GUI for Redis.

**First-time setup:**

1. Open http://localhost:5540
2. Click **"Add Redis Database"**
3. Fill in:
   - **Host:** `redis` *(inside Docker network)* or `localhost` *(from host)*
   - **Port:** `6379`
   - **Password:** `redis_dev_pass` *(from `.env` REDIS_PASS)*
4. Click **"Add Redis Database"**

**What to look for:**
- Keys with prefix `price:` — cached token prices from CoinGecko (e.g. `price:ETH`)
- Browse key TTL to see cache expiry
- Use the **CLI** tab to run: `KEYS price:*` or `GET price:ETH`

---

## 🗂️ Project Structure

```
Flash-Loan-Attack-Detection/
├── docker-compose.yml          # Full infrastructure definition
├── .env                        # Environment variables (secrets)
├── requirements.txt            # Python dependencies (root)
│
├── ingestion/                  # Data ingestion layer
│   ├── mock_server.py          # Mock Ethereum WebSocket node
│   ├── listener.py             # Subscribes to Ethereum & publishes to Kafka
│   ├── config.py               # Ingestion config (Kafka, WebSocket URL)
│   └── data/
│       └── test_data_enriched.csv   # Test dataset (35 flash loan txs)
│
├── processing/                 # PySpark streaming job
│   ├── streaming_job.py        # Main PySpark Structured Streaming pipeline
│   ├── Dockerfile              # Image for the driver (processing-job)
│   └── Dockerfile.worker       # Image for Spark workers
│
├── backend/                    # FastAPI REST API
│   ├── Main.py                 # API endpoints
│   └── .env                    # Backend-specific env vars
│
├── frontend/                   # React + Vite dashboard
│   ├── src/
│   └── package.json
│
├── abis/                       # Ethereum contract ABIs
├── data/                       # Shared data directory
└── storage/                    # Persistent storage configs
```

---

## 🔄 Data Flow

```
1. mock_server.py     → Replays 35 test transactions via WebSocket (ws://localhost:8765)
2. listener.py        → Receives tx hashes, fetches full tx via eth_getTransactionByHash,
                         filters for flash loan selectors, publishes to Kafka
3. Kafka              → Buffers raw transactions in topic `raw_transactions` (4 partitions)
4. streaming_job.py   → PySpark reads from Kafka, decodes flash loan UDF,
                         fetches token price from Redis/CoinGecko,
                         computes risk score, writes alerts to MongoDB
5. FastAPI            → Reads alerts from MongoDB, exposes REST API
6. React              → Polls API, displays real-time alert dashboard
```

---

## 🛠️ Useful Commands

### Docker

```powershell
# View logs for a specific service
docker compose logs -f processing-job
docker compose logs -f kafka-1
docker compose logs -f spark-master

# Restart a single service
docker compose restart processing-job

# Scale spark workers
docker compose up -d --scale spark-worker=4

# Full reset (removes all data)
docker compose down -v
docker compose up -d --build
```

### Kafka (via Docker exec)

```powershell
# List topics
docker exec flash-loan-attack-detection-kafka-1-1 kafka-topics --bootstrap-server localhost:9092 --list

# Describe a topic
docker exec flash-loan-attack-detection-kafka-1-1 kafka-topics --bootstrap-server localhost:9092 --describe --topic raw_transactions

# Consume messages from beginning
docker exec flash-loan-attack-detection-kafka-1-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic raw_transactions --from-beginning --max-messages 5
```

### Redis (via Docker exec)

```powershell
# Connect to Redis CLI
docker exec -it flash-loan-attack-detection-redis-1 redis-cli -a redis_dev_pass

# Inside redis-cli:
KEYS price:*
GET price:ETH
INFO memory
```

### MongoDB

```powershell
# Clear all alerts (use with caution)
python clear_mongo.py
```

---

## ⚠️ Troubleshooting

| Problem | Solution |
|---------|----------|
| `processing-job` keeps restarting | Kafka brokers not ready yet — wait 30s, check `docker compose logs kafka-1` |
| Kafka UI shows no messages | Make sure `listener.py` is running and `mock_server.py` is active |
| RedisInsight can't connect | Use `redis` as host (Docker internal hostname), password `redis_dev_pass` |
| Spark UI (4040) not accessible | Check `docker compose logs processing-job` — Spark app may not have started yet |
| `REDIS_PASS` auth error | Ensure `.env` has correct `REDIS_PASS` and `docker compose down -v` was run before `up` |
| Frontend can't reach API | Ensure backend is running on port 8000, check CORS settings in `backend/Main.py` |

---

## 📊 Port Reference

| Port | Service |
|------|---------|
| `5173` | React Frontend (Vite dev server) |
| `8000` | FastAPI Backend |
| `8080` | Spark Master Web UI |
| `8082` | Kafka UI (Provectus) |
| `4040` | Spark Application UI (Streaming job) |
| `5540` | RedisInsight |
| `6379` | Redis (direct access) |
| `8765` | Mock Ethereum WebSocket node |
| `9094` | Kafka broker 1 (external) |
| `9095` | Kafka broker 2 (external) |
| `9096` | Kafka broker 3 (external) |
