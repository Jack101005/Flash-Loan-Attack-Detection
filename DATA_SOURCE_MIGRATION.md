# Data Source Migration Guide: CSV/Live Stream → MongoDB

This guide shows how to change the data source from CSV files or live WebSocket streams to MongoDB JSON documents.

---

## Current Architecture

### Stage 1: Ingestion (Current)
- **Source**: Live WebSocket mempool stream OR CSV files
- **Output**: Kafka topic `raw_txns` or CSV files
- **Files**: `ingestion/listener.py`, `export_detections.py`

### Stage 3: Processing (To Be Updated)
- **Consumer**: Reads from Kafka topic `raw_txns`
- **Files**: `processing/stream_processor.py` (empty - Person 3)

---

## Migration Path

### Option A: Read from MongoDB (Recommended for Existing Data)

Use this when you have existing flash loan transactions already stored in MongoDB.

#### Step 1: Create a MongoDB Query Reader

Create `processing/mongodb_reader.py`:

```python
from storage.mongo_store import transactions_collection, get_db
from typing import Generator, dict

def read_transactions_from_mongo(
    filters: dict | None = None,
    batch_size: int = 100
) -> Generator[dict, None, None]:
    """
    Yields transactions from MongoDB in batches.
    
    Args:
        filters: MongoDB query filter (e.g., {"risk_level": "HIGH"})
        batch_size: Number of documents per batch
    
    Yields:
        Transaction document dicts
    """
    collection = transactions_collection()
    cursor = collection.find(filters or {}).batch_size(batch_size)
    
    for doc in cursor:
        # Remove MongoDB _id for compatibility
        doc.pop("_id", None)
        yield doc


def read_recent_detections(limit: int = 1000) -> list[dict]:
    """Get most recent N detections from MongoDB."""
    from pymongo import DESCENDING
    collection = transactions_collection()
    cursor = (
        collection
        .find({"is_flash_loan": True})
        .sort("detected_at", DESCENDING)
        .limit(limit)
    )
    return [doc for doc in cursor]
```

#### Step 2: Update Processing Job to Use MongoDB

Modify `processing/stream_processor.py` to read from MongoDB instead of Kafka:

```python
from processing.mongodb_reader import read_transactions_from_mongo
from processing.graph_builder import build_transaction_graph
from processing.cycle_detector import detect_arbitrage_cycles

def process_mongodb_transactions(limit: int = 1000):
    """
    Batch process transactions from MongoDB instead of live Kafka stream.
    
    Usage:
        python -c "from processing.stream_processor import process_mongodb_transactions; process_mongodb_transactions(100)"
    """
    print("[processor] Reading from MongoDB...")
    
    for i, tx in enumerate(read_transactions_from_mongo(batch_size=50)):
        if i >= limit:
            break
        
        # Build transaction graph
        graph = build_transaction_graph(tx)
        
        # Detect cycles (arbitrage paths)
        cycles = detect_arbitrage_cycles(graph)
        
        # Store results
        if cycles:
            print(f"[processor] Found {len(cycles)} cycles in tx {tx['tx_hash']}")
            # Save to MongoDB (Person 4)
```

---

### Option B: Replace Listener with MongoDB Feeder

Use this when you want to continuously ingest new transactions from MongoDB instead of WebSocket.

#### Create `ingestion/mongodb_listener.py`:

```python
"""
mongodb_listener.py — Read flash loan transactions from MongoDB

Instead of listening to live mempool via WebSocket, continuously
polls MongoDB for new transactions and publishes to Kafka or local queue.

Usage:
    python ingestion/mongodb_listener.py --poll-interval 5
"""

import time
import json
import argparse
from datetime import datetime, timedelta, timezone

from storage.mongo_store import transactions_collection
from broker.kafka_producer import create_producer, produce_message, flush_producer


def listen_to_mongodb(
    poll_interval: int = 5,
    lookback_minutes: int = 10,
    use_kafka: bool = True
):
    """
    Poll MongoDB for new flash loan transactions and publish to Kafka.
    
    Args:
        poll_interval: Seconds between polls
        lookback_minutes: How far back to search for "new" transactions
        use_kafka: Whether to publish to Kafka or print only
    """
    collection = transactions_collection()
    kafka_producer = None
    
    if use_kafka:
        kafka_producer = create_producer()
        if kafka_producer is None:
            print("[mongodb_listener] Kafka not reachable — print-only mode")
    
    print(f"[mongodb_listener] Starting MongoDB listener...")
    print(f"[mongodb_listener] Poll interval: {poll_interval}s")
    print(f"[mongodb_listener] Lookback: {lookback_minutes}m")
    
    last_check = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    seen_hashes = set()
    
    try:
        while True:
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
            
            # Query MongoDB for recent transactions
            cursor = collection.find(
                {
                    "detected_at": {"$gte": cutoff},
                    "is_flash_loan": True
                },
                {"_id": 0}
            )
            
            for tx in cursor:
                tx_hash = tx.get("tx_hash")
                
                if tx_hash in seen_hashes:
                    continue
                
                seen_hashes.add(tx_hash)
                
                print(f"\n[mongodb_listener] Found: {tx_hash[:16]}...")
                print(f"  Risk: {tx.get('risk_level')}")
                print(f"  Detected: {tx.get('detected_at')}")
                
                # Publish to Kafka
                if kafka_producer:
                    try:
                        produce_message(
                            kafka_producer,
                            "raw_txns",
                            tx_hash,
                            tx
                        )
                    except Exception as e:
                        print(f"  [kafka] Failed: {e}")
            
            time.sleep(poll_interval)
    
    except KeyboardInterrupt:
        print("\n[mongodb_listener] Stopped by user")
    finally:
        if kafka_producer:
            flush_producer(kafka_producer)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MongoDB Flash Loan Listener"
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds between MongoDB polls (default: 5)"
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=10,
        help="Minutes to look back in history (default: 10)"
    )
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Print only (no Kafka publish)"
    )
    args = parser.parse_args()
    
    listen_to_mongodb(
        poll_interval=args.poll_interval,
        lookback_minutes=args.lookback,
        use_kafka=not args.no_kafka
    )
```

---

### Option C: Update `export_detections.py` to Read from MongoDB

Modify to read test data from MongoDB instead of CSV:

```python
"""export_detections.py — Modified to read from MongoDB"""

from storage.mongo_store import get_db
import csv
import os

def process_transactions_from_mongo(output_path: str = "data/detected_flash_loans.csv"):
    """Read flash loans from MongoDB and export to CSV."""
    
    collection = get_db()["transactions"]
    
    # Query for all detected flash loans
    cursor = collection.find(
        {"is_flash_loan": True},
        {"_id": 0}
    )
    
    detected = list(cursor)
    
    # Write to CSV (same schema as before)
    output_fields = [
        "tx_hash", "from", "to", "input", "value",
        "gas", "gas_price", "timestamp", "source",
        "protocol", "risk_level", "summary"
    ]
    
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(detected)
    
    print(f"[export] Exported {len(detected)} detections from MongoDB to {output_path}")


if __name__ == "__main__":
    process_transactions_from_mongo()
```

---

## Configuration

### 1. Ensure MongoDB Connection

Check `.env` or `backend/.env`:

```bash
MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net/?retryWrites=true&w=majority
MONGODB_FLASHLOAN_NAME=flash_loan_db
```

### 2. Verify MongoDB Collections Exist

```python
from storage.mongo_store import get_db, init_indexes

db = get_db()
print(db.list_collection_names())  # Should include "transactions"

# Initialize indexes (run once at startup)
init_indexes()
```

---

## Usage Examples

### Read from MongoDB in Python

```python
from storage.mongo_store import transactions_collection
from pymongo import DESCENDING

# Get recent flash loans
collection = transactions_collection()
recent = collection.find(
    {"is_flash_loan": True, "risk_level": "CRITICAL"},
    {"_id": 0}
).sort("detected_at", DESCENDING).limit(10)

for tx in recent:
    print(tx["tx_hash"], tx["summary"])
```

### Query MongoDB from CLI

```bash
# Start MongoDB shell (if running in Docker)
docker exec -it flash-loan-detection-mongodb-1 mongosh

# List databases
show dbs

# Use the flash loan database
use flash_loan_db

# Query transactions
db.transactions.find({"is_flash_loan": true}).limit(5)

# Count detections by risk level
db.transactions.aggregate([
    {"$match": {"is_flash_loan": true}},
    {"$group": {"_id": "$risk_level", "count": {"$sum": 1}}}
])
```

---

## Migration Checklist

- [ ] MongoDB instance running and connected (verify with `storage/mongo_store.py`)
- [ ] Data already exists in MongoDB collection `transactions`
- [ ] Choose migration option (A, B, or C) based on use case
- [ ] Update `ingestion/listener.py` OR create new `ingestion/mongodb_listener.py`
- [ ] Update `processing/stream_processor.py` to consume from MongoDB
- [ ] Test with sample queries before production
- [ ] Update Docker Compose to start MongoDB if needed
- [ ] Update README for new team members

---

## Rollback Plan

If you need to revert to CSV/WebSocket:

1. Keep original `ingestion/listener.py` (it's still working)
2. Keep original `export_detections.py` (reads from CSV)
3. Simply don't run the new MongoDB readers — use `--no-kafka` flag to skip Kafka

---

## Next Steps

1. **For Person 2 (Kafka)**: Update `broker/` modules to optionally read from MongoDB instead of Kafka
2. **For Person 3 (Processing)**: Replace Kafka consumer with `mongodb_reader.py`
3. **For Person 4 (Storage)**: Extend `mongo_store.py` with write operations for detections
4. **For Person 5 (Alerting)**: Query MongoDB directly for recent detections instead of consuming Kafka
