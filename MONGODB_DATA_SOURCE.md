# Quick Start: Using MongoDB as Data Source

## TL;DR

You've already got MongoDB set up in `infra/docker-compose.yml`. Now you have **two new tools** to replace CSV/WebSocket ingestion:

1. **`ingestion/mongodb_listener.py`** — Polls MongoDB continuously, publishes to Kafka (like the WebSocket listener)
2. **`processing/mongodb_reader.py`** — Reads from MongoDB for batch processing (replaces Kafka consumer)

---

## Quick Setup

### 1. Ensure MongoDB Has Data

```bash
# Check your backend/.env or root .env
cat .env | grep MONGODB

# Should show:
# MONGODB_URI=mongodb+srv://...
# MONGODB_FLASHLOAN_NAME=flash_loan_db
```

### 2. Test MongoDB Connection

```bash
python3 -c "
from storage.mongo_store import get_db, ping
if ping():
    print('✓ MongoDB connected')
    db = get_db()
    print(f'✓ Collections: {db.list_collection_names()}')
else:
    print('✗ MongoDB not reachable')
"
```

---

## Usage: Option A — MongoDB Listener (Replace WebSocket)

Use this if you want to **continuously pull data from MongoDB** into Kafka (same as live WebSocket listener, but from MongoDB instead).

### Start the Listener

```bash
# Terminal 1: Poll MongoDB every 5 seconds, look back 10 minutes for new txns
python3 ingestion/mongodb_listener.py

# Or with custom settings
python3 ingestion/mongodb_listener.py --poll-interval 2 --lookback 30

# Print-only mode (no Kafka)
python3 ingestion/mongodb_listener.py --no-kafka
```

### Example Output

```
[mongodb_listener] Starting MongoDB listener...
[mongodb_listener] Poll interval: 5s
[mongodb_listener] Lookback: 10 minutes
[mongodb_listener] Kafka: enabled

[mongodb_listener] Poll #1: Found 3 recent detections

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  NEW DETECTION  #1
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  Tx Hash:   0xabcd1234...
  Risk:      HIGH
  Summary:   Aave V3 flash loan with complex path
  Detected:  2024-05-25 14:50:10

  [kafka] → raw_txns  key=0xabcd1234...
```

---

## Usage: Option B — MongoDB Reader (For Processing)

Use this in **`processing/stream_processor.py`** to batch-read from MongoDB instead of Kafka consumer.

### In Python Code

```python
from processing.mongodb_reader import read_transactions_from_mongo, read_recent_detections

# Stream all detections
for tx in read_transactions_from_mongo():
    print(f"Processing: {tx['tx_hash']}")
    # Your cycle detection, graph building, etc.

# Or get recent N detections
recent = read_recent_detections(limit=100, risk_level="CRITICAL")
for tx in recent:
    print(f"{tx['tx_hash']}: {tx['summary']}")
```

### CLI Query

```bash
# Show stats
python3 processing/mongodb_reader.py --command stats

# Show recent 10 detections
python3 processing/mongodb_reader.py --command recent --limit 10

# Show only CRITICAL risk
python3 processing/mongodb_reader.py --command recent --risk-level CRITICAL

# Count all detections
python3 processing/mongodb_reader.py --command count
```

---

## Migration Steps by Role

### For Person 2 (Kafka Broker)
Instead of consuming from Kafka topic `raw_txns`, you now poll MongoDB:

```python
# OLD: Consume from Kafka
# from broker.kafka_consumer import start_consumer_loop
# for msg in start_consumer_loop("flash_loan_detectors"):
#     process(msg)

# NEW: Read from MongoDB
from processing.mongodb_reader import read_transactions_from_mongo

for tx in read_transactions_from_mongo(filters={"is_flash_loan": True}):
    process(tx)
```

### For Person 3 (Processing/Detection)

```python
# OLD: Consume from Kafka
# from broker.kafka_consumer import create_consumer

# NEW: Read from MongoDB
from processing.mongodb_reader import read_recent_detections
from processing.graph_builder import build_transaction_graph
from processing.cycle_detector import detect_arbitrage_cycles

for tx in read_recent_detections(limit=1000):
    graph = build_transaction_graph(tx)
    cycles = detect_arbitrage_cycles(graph)
    
    if cycles:
        print(f"Found {len(cycles)} arbitrage cycles in {tx['tx_hash']}")
        # Save results to MongoDB
```

### For Person 4 (Storage/MongoDB)

You already handle writes to MongoDB. Now just ensure your indices are set up:

```python
from storage.mongo_store import init_indexes

# Run once at startup to create indexes
init_indexes()
```

### For Person 5 (Alerting)

```python
# OLD: Consume from Kafka alerting_group
# from broker.kafka_consumer import start_consumer_loop

# NEW: Poll MongoDB directly
from processing.mongodb_reader import read_recent_detections

while True:
    recent = read_recent_detections(limit=10)
    for tx in recent:
        if tx.get("risk_level") == "CRITICAL":
            send_telegram_alert(tx)
    time.sleep(10)
```

---

## Architecture Diagram

### Before (CSV + WebSocket + Kafka)
```
Ethereum Mempool
    ↓
WebSocket Listener (listener.py)
    ↓
Kafka Topic [raw_txns]
    ↓
Spark Processor
    ↓
MongoDB
```

### After (MongoDB-First)
```
MongoDB [transactions] Collection
    ↓
MongoDB Listener (mongodb_listener.py) — OPTIONAL, for Kafka compatibility
    ↓
Kafka Topic [raw_txns] — OPTIONAL
    ↓
MongoDB Reader (mongodb_reader.py) — Can read directly
    ↓
Processing / Alerting
```

Or skip Kafka entirely:

```
MongoDB [transactions] Collection
    ↓
MongoDB Reader (mongodb_reader.py)
    ↓
Processing / Alerting (Batch or Polling)
```

---

## Data Schema

MongoDB transactions collection structure:

```javascript
{
  "_id": ObjectId(...),
  "tx_hash": "0xabcd...",
  "block_number": 19500000,
  "is_flash_loan": true,
  "risk_level": "HIGH",              // "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
  "summary": "Aave V3 flash loan with cyclic arbitrage pattern",
  "raw_data": {
    "from": "0x1234...",
    "to": "0x5678...",
    "input": "0xab9c4b5d...",
    "value": "0",
    "gas": "500000",
    "gas_price": "50000000000",
    "timestamp": 1716649810,
    "source": "ethereum_mainnet",
    "protocol": "Aave V3 flashLoan"
  },
  "detected_at": ISODate("2024-05-25T14:50:10Z")
}
```

---

## Common Queries

### Python

```python
from processing.mongodb_reader import *

# Get all critical detections
critical = read_recent_detections(limit=999, risk_level="CRITICAL")

# Get stats
stats = get_stats()
print(stats)
# Output: {
#   'total_detections': 150,
#   'flash_loan_detections': 42,
#   'by_risk_level': {'HIGH': 15, 'CRITICAL': 27},
#   'collection_stats': 150
# }

# Get specific tx
tx = get_transaction_by_hash("0xabcd...")

# Count by protocol
aave = get_transactions_by_protocol("Aave V3", limit=50)
```

### MongoDB CLI (if running in Docker)

```bash
# Connect to MongoDB
docker exec -it $(docker ps -q -f name=mongodb) mongosh

# Query
use flash_loan_db

db.transactions.find({"is_flash_loan": true, "risk_level": "CRITICAL"}).limit(5)

db.transactions.aggregate([
  {"$match": {"is_flash_loan": true}},
  {"$group": {"_id": "$risk_level", "count": {"$sum": 1}}}
])

# Count
db.transactions.countDocuments({"is_flash_loan": true})
```

---

## Testing

### Test MongoDB Reader

```bash
python3 -c "
from processing.mongodb_reader import get_stats, read_recent_detections
stats = get_stats()
print('Stats:', stats)
recent = read_recent_detections(limit=3)
print(f'Recent detections: {len(recent)}')
for tx in recent:
    print(f'  - {tx[\"tx_hash\"][:16]}...')
"
```

### Test MongoDB Listener

```bash
# Start listener in print-only mode
python3 ingestion/mongodb_listener.py --no-kafka --poll-interval 2 --lookback 30

# In another terminal, insert a test document
python3 -c "
from storage.mongo_store import transactions_collection, make_transaction_doc
col = transactions_collection()
test_doc = make_transaction_doc(
    tx_hash='0xtest123',
    block_number=19500000,
    is_flash_loan=True,
    risk_level='HIGH',
    summary='Test detection'
)
col.insert_one(test_doc)
print('Inserted test document')
"

# Listener should detect it in next poll
```

---

## Troubleshooting

### MongoDB Not Connecting

```bash
# Check .env
grep MONGODB .env

# If not set, create it:
echo "MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority" >> .env
echo "MONGODB_FLASHLOAN_NAME=flash_loan_db" >> .env

# Test connection
python3 -c "from storage.mongo_store import ping; print('Connected!' if ping() else 'Failed')"
```

### Kafka Publisher Issues

```bash
# If Kafka is unavailable, listeners automatically fall back to print-only:
python3 ingestion/mongodb_listener.py
# Shows: "[mongodb_listener] Kafka not reachable — running in print-only mode"

# Or explicitly skip Kafka:
python3 ingestion/mongodb_listener.py --no-kafka
```

### No Data in MongoDB

```bash
# Ensure data was written:
python3 -c "
from storage.mongo_store import transactions_collection
col = transactions_collection()
count = col.count_documents({})
print(f'Total documents in collection: {count}')

# If 0, you need to:
# 1. Run the listener to collect data from Ethereum
# 2. Or populate MongoDB with existing data
# 3. Or use the export_detections.py to load from CSV
"
```

---

## Next Steps

1. **Choose your architecture:**
   - Option A: Keep Kafka, use `mongodb_listener.py` to feed it MongoDB data
   - Option B: Skip Kafka, use `mongodb_reader.py` directly in processing

2. **Update your main pipeline:**
   - If Option A: Replace WebSocket listener startup with `mongodb_listener.py`
   - If Option B: Replace Kafka consumer in processing with `mongodb_reader.py`

3. **Test with sample data:**
   ```bash
   python3 ingestion/mongodb_listener.py --no-kafka
   # Or
   python3 processing/mongodb_reader.py --command stats
   ```

4. **Scale to production:**
   - Use `--poll-interval` to control query frequency
   - Monitor MongoDB query performance
   - Consider batch window sizes (lookback_minutes)
