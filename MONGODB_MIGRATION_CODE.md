# Code Migration Reference: CSV/WebSocket → MongoDB

Quick copy-paste code snippets for each team member to integrate MongoDB as the data source.

---

## Person 2: Kafka Broker Modifications

### Current Code (Kafka Consumer)
```python
# OLD: broker/kafka_consumer.py
from broker.kafka_consumer import create_consumer

consumer = create_consumer()
for msg in consumer:
    tx_data = json.loads(msg.value())
    print(f"Received: {tx_data['tx_hash']}")
```

### New Code (MongoDB Reader)
```python
# NEW: Use mongodb_reader.py instead
from processing.mongodb_reader import read_transactions_from_mongo

for tx_data in read_transactions_from_mongo():
    print(f"Received: {tx_data['tx_hash']}")
```

---

## Person 3: Processing Job Modifications

### Current Code (Kafka Stream)
```python
# OLD: processing/stream_processor.py
from broker.kafka_consumer import start_consumer_loop
from processing.graph_builder import build_transaction_graph
from processing.cycle_detector import detect_arbitrage_cycles

def main():
    for tx in start_consumer_loop("flash_loan_detectors"):
        graph = build_transaction_graph(tx)
        cycles = detect_arbitrage_cycles(graph)
```

### New Code (MongoDB Batch)
```python
# NEW: processing/stream_processor.py
from processing.mongodb_reader import read_recent_detections
from processing.graph_builder import build_transaction_graph
from processing.cycle_detector import detect_arbitrage_cycles

def main():
    # Option A: Process recent detections (batch)
    recent = read_recent_detections(limit=1000)
    for tx in recent:
        graph = build_transaction_graph(tx)
        cycles = detect_arbitrage_cycles(graph)
    
    # Option B: Stream continuously from MongoDB
    # from processing.mongodb_reader import read_transactions_from_mongo
    # for tx in read_transactions_from_mongo():
    #     graph = build_transaction_graph(tx)
    #     cycles = detect_arbitrage_cycles(graph)

if __name__ == "__main__":
    main()
```

---

## Person 4: Storage Layer (No Changes Needed!)

Your `storage/mongo_store.py` already provides the interface. Just ensure indexes are created:

```python
# storage/mongo_store.py — already done, but call once at startup
from storage.mongo_store import init_indexes

init_indexes()  # Creates indexes on transactions and alerts collections
```

---

## Person 5: Alerting & Dashboard

### Current Code (Kafka Consumer)
```python
# OLD: alerting/telegram_bot.py
from broker.kafka_consumer import start_consumer_loop

def monitor_alerts():
    for msg in start_consumer_loop("alerting_group"):
        alert = json.loads(msg.value())
        if alert["risk_level"] == "CRITICAL":
            send_telegram_alert(alert)
```

### New Code (MongoDB Polling)
```python
# NEW: alerting/telegram_bot.py
from processing.mongodb_reader import read_recent_detections
import time

def monitor_alerts():
    last_check = time.time()
    seen_hashes = set()
    
    while True:
        # Poll MongoDB every 10 seconds
        recent = read_recent_detections(limit=100)
        
        for alert in recent:
            tx_hash = alert.get("tx_hash")
            
            # Skip already-alerted txs
            if tx_hash in seen_hashes:
                continue
            
            seen_hashes.add(tx_hash)
            
            # Send alert for CRITICAL detections
            if alert.get("risk_level") == "CRITICAL":
                send_telegram_alert(alert)
        
        time.sleep(10)  # Poll every 10 seconds
```

---

## Person 1: Ingestion Stage Changes

### Option A: Replace WebSocket Listener

**Current:** `ingestion/listener.py` (WebSocket subscriber)

**New:** `ingestion/mongodb_listener.py` (MongoDB poller)

```bash
# OLD: Start WebSocket listener
python3 ingestion/listener.py

# NEW: Start MongoDB listener
python3 ingestion/mongodb_listener.py

# Or with custom settings
python3 ingestion/mongodb_listener.py --poll-interval 5 --lookback 10
```

### Update Docker Compose

```yaml
# OLD: infra/docker-compose.yml
services:
  ingestion:
    build: ../ingestion/
    command: python listener.py
    depends_on:
      - kafka

# NEW: Same, but can use mongodb_listener.py instead
services:
  ingestion:
    build: ../ingestion/
    command: python mongodb_listener.py --poll-interval 5
    depends_on:
      - mongodb
      - kafka  # Optional — mongodb_listener can work without Kafka
```

---

## Integration Checklist

### Step 1: Create New Files (Already Done!)
- ✅ `processing/mongodb_reader.py` — provides MongoDB queries
- ✅ `ingestion/mongodb_listener.py` — polls MongoDB continuously

### Step 2: Update Each Component

**Ingestion (Person 1/2):**
```bash
# Option A: Use mongodb_listener instead of listener
python3 ingestion/mongodb_listener.py

# Option B: Keep listener, but also read from MongoDB
# Add to your pipeline as alternate data source
```

**Processing (Person 3):**
```python
# In processing/stream_processor.py, replace Kafka consumer
from processing.mongodb_reader import read_recent_detections

for tx in read_recent_detections(limit=1000):
    # Your processing logic
```

**Storage (Person 4):**
```python
# Already working! Just ensure:
from storage.mongo_store import init_indexes
init_indexes()  # Run once at startup
```

**Alerting (Person 5):**
```python
# In alerting/telegram_bot.py, replace Kafka consumer
from processing.mongodb_reader import read_recent_detections
import time

while True:
    recent = read_recent_detections(limit=50)
    for alert in recent:
        if alert["risk_level"] == "CRITICAL":
            send_alert(alert)
    time.sleep(10)
```

### Step 3: Test Each Component

```bash
# Test MongoDB connection
python3 -c "from storage.mongo_store import ping; print('OK' if ping() else 'FAIL')"

# Test reader
python3 processing/mongodb_reader.py --command stats

# Test listener
python3 ingestion/mongodb_listener.py --no-kafka --poll-interval 2 &

# Insert test data in background
sleep 2
python3 -c "
from storage.mongo_store import transactions_collection, make_transaction_doc
col = transactions_collection()
doc = make_transaction_doc('0xtest', 19500000, True, 'HIGH', 'Test')
col.insert_one(doc)
"

# Listener should detect it
```

### Step 4: Update Configuration

Ensure `.env` has MongoDB credentials:

```bash
# backend/.env or .env
MONGODB_URI=mongodb+srv://user:password@cluster.mongodb.net/?retryWrites=true&w=majority
MONGODB_FLASHLOAN_NAME=flash_loan_db
```

Test it:
```bash
python3 -c "
from storage.mongo_store import get_db
db = get_db()
print('Connected to:', db.name)
print('Collections:', db.list_collection_names())
"
```

---

## Performance Tuning

### MongoDB Query Optimization

```python
# Create compound index for common queries
from storage.mongo_store import transactions_collection

col = transactions_collection()

# Index for filtering by risk level and date
col.create_index([
    ("is_flash_loan", 1),
    ("risk_level", 1),
    ("detected_at", -1)
])

# Index for hash lookups
col.create_index([("tx_hash", 1)], unique=True)
```

### Polling Strategy

```python
# Tune poll_interval and lookback for your workload

# High frequency (real-time): small window, short interval
# python mongodb_listener.py --poll-interval 1 --lookback 5

# Medium: balanced
# python mongodb_listener.py --poll-interval 5 --lookback 10  # DEFAULT

# Low frequency (batch): long window, long interval
# python mongodb_listener.py --poll-interval 30 --lookback 60
```

---

## Rollback Plan

If you need to revert to Kafka/WebSocket:

```bash
# The old files still exist and work unchanged:
# - ingestion/listener.py
# - broker/kafka_consumer.py
# - export_detections.py

# Just restart using the old entry points:
python3 ingestion/listener.py              # Old: WebSocket listener
python3 ingestion/listener.py --no-kafka   # Old: Print-only mode

# Processing can use either:
python3 processing/stream_processor.py     # Can accept Kafka OR MongoDB
```

---

## FAQ

### Q: Do I need to keep Kafka?
**A:** No! MongoDB can replace Kafka entirely. But you can run both in parallel during transition.

### Q: What about historical data?
**A:** All data stays in MongoDB. Use `mongodb_listener.py --lookback 1440` (24 hours) to reprocess old data.

### Q: How do I switch individual components?
**A:** Each component (`processing/`, `alerting/`, etc.) is independent:
- Person 3 can use MongoDB reader while Person 2 still feeds Kafka
- They can coexist — add MongoDB reading as an optional parallel path

### Q: Performance with large datasets?
**A:** MongoDB handles millions of documents well. Use pagination:
```python
from processing.mongodb_reader import read_transactions_from_mongo

for batch in read_transactions_from_mongo(batch_size=1000):
    # Process 1000 at a time
```

### Q: What if MongoDB goes down?
**A:** Both listeners have fallbacks:
```python
# mongodb_listener.py catches exceptions:
try:
    cursor = collection.find({...})
except Exception as e:
    print(f"[mongodb_listener] Query error: {e}")
    # Retries next poll

# mongodb_reader.py returns empty results:
def read_transactions_from_mongo(...):
    try:
        collection = transactions_collection()
    except Exception as e:
        print(f"Failed to connect: {e}")
        return  # Generator yields nothing
```

---

## Summary

| Component | Current | New | Effort |
|-----------|---------|-----|--------|
| **Ingestion** | `listener.py` (WebSocket) | `mongodb_listener.py` (Poll) | 1-2 lines |
| **Broker** | `kafka_consumer.py` | `mongodb_reader.py` | 2-3 lines |
| **Processing** | Kafka consumer loop | MongoDB batch reader | 3-5 lines |
| **Storage** | ✓ Already ready | ✓ No changes | 0 lines |
| **Alerting** | Kafka consumer loop | MongoDB poller loop | 5-10 lines |

**Total effort:** ~30 lines of code changes across the entire pipeline.
