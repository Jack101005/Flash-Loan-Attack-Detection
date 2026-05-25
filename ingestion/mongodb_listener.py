"""
mongodb_listener.py — Poll MongoDB for new flash loan transactions

Alternative to WebSocket listener. Instead of listening to live mempool,
continuously polls MongoDB for new detections and publishes to Kafka
or local queue. Useful for batch processing and replay scenarios.

Usage:
    python ingestion/mongodb_listener.py                    # Default settings
    python ingestion/mongodb_listener.py --poll-interval 5 --lookback 30
    python ingestion/mongodb_listener.py --no-kafka          # Print-only mode

Requires: storage.mongo_store, broker.kafka_producer
"""

import asyncio
import json
import time
import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

# Allow importing from sibling packages
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from broker.kafka_producer import (
        create_producer,
        produce_message,
        flush_producer,
    )
    _KAFKA_AVAILABLE = True
except ImportError:
    _KAFKA_AVAILABLE = False

from storage.mongo_store import transactions_collection


# ─────────────────────────────────────────────────────────────────
# Stats tracking
# ─────────────────────────────────────────────────────────────────
class Stats:
    def __init__(self):
        self.total_found = 0
        self.new_detections = 0
        self.duplicates = 0
        self.kafka_sent = 0
        self.kafka_failures = 0
        self.start_time = time.time()
    
    def summary(self):
        elapsed = time.time() - self.start_time
        print(f"\n{'='*60}")
        print(f"  Session Summary ({elapsed:.1f}s)")
        print(f"{'='*60}")
        print(f"  MongoDB documents found:    {self.total_found}")
        print(f"  New detections processed:   {self.new_detections}")
        print(f"  Duplicates skipped:         {self.duplicates}")
        print(f"  Kafka messages sent:        {self.kafka_sent}")
        print(f"  Kafka send failures:        {self.kafka_failures}")
        print(f"{'='*60}\n")


# ─────────────────────────────────────────────────────────────────
# Core polling logic
# ─────────────────────────────────────────────────────────────────
def listen_to_mongodb(
    poll_interval: int = 5,
    lookback_minutes: int = 10,
    use_kafka: bool = True,
):
    """
    Poll MongoDB for new flash loan transactions.
    
    Args:
        poll_interval: Seconds between MongoDB polls
        lookback_minutes: How many minutes back to search for "new" transactions
        use_kafka: Whether to publish to Kafka or print only
    """
    stats = Stats()
    seen_hashes = set()
    
    # Kafka setup
    kafka_producer = None
    if use_kafka and _KAFKA_AVAILABLE:
        print("[mongodb_listener] Connecting to Kafka...")
        kafka_producer = create_producer()
        if kafka_producer is None:
            print("[mongodb_listener] Kafka not reachable — running in print-only mode")
    elif not _KAFKA_AVAILABLE:
        print("[mongodb_listener] broker/kafka_producer.py not available — print-only mode")
    
    try:
        collection = transactions_collection()
    except Exception as e:
        print(f"[mongodb_listener] Failed to connect to MongoDB: {e}")
        print("[mongodb_listener] Ensure MONGODB_URI is set in .env")
        return
    
    print(f"\n[mongodb_listener] Starting MongoDB listener...")
    print(f"[mongodb_listener] Poll interval: {poll_interval}s")
    print(f"[mongodb_listener] Lookback: {lookback_minutes} minutes")
    print(f"[mongodb_listener] Kafka: {'enabled' if kafka_producer else 'disabled (print-only)'}\n")
    
    try:
        poll_count = 0
        
        while True:
            poll_count += 1
            
            # Calculate cutoff time for "new" documents
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
            
            # Query MongoDB for recent flash loans
            try:
                cursor = collection.find(
                    {
                        "detected_at": {"$gte": cutoff},
                        "is_flash_loan": True
                    },
                    {"_id": 0}
                ).sort("detected_at", -1)
                
                poll_results = list(cursor)
                stats.total_found += len(poll_results)
                
                print(f"[mongodb_listener] Poll #{poll_count}: Found {len(poll_results)} recent detections")
                
                for tx in poll_results:
                    tx_hash = tx.get("tx_hash", "unknown")
                    
                    # Deduplication
                    if tx_hash in seen_hashes:
                        stats.duplicates += 1
                        continue
                    
                    seen_hashes.add(tx_hash)
                    stats.new_detections += 1
                    
                    # Print detection
                    print(f"\n{'!'*60}")
                    print(f"  NEW DETECTION  #{stats.new_detections}")
                    print(f"{'!'*60}")
                    print(f"  Tx Hash:   {tx_hash[:16]}...")
                    print(f"  Risk:      {tx.get('risk_level', 'N/A')}")
                    print(f"  Summary:   {tx.get('summary', 'N/A')[:60]}")
                    print(f"  Detected:  {tx.get('detected_at', 'N/A')}")
                    
                    # Publish to Kafka
                    if kafka_producer:
                        try:
                            produce_message(
                                kafka_producer,
                                "raw_txns",
                                tx_hash,
                                tx
                            )
                            stats.kafka_sent += 1
                            print(f"  [kafka] → raw_txns  key={tx_hash[:16]}...")
                        except BufferError:
                            stats.kafka_failures += 1
                            print(f"  [kafka] Queue full")
                        except Exception as e:
                            stats.kafka_failures += 1
                            print(f"  [kafka] Failed: {e}")
                    
                    print()
            
            except Exception as e:
                print(f"[mongodb_listener] Query error: {e}")
            
            # Wait for next poll
            try:
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                print("\n[mongodb_listener] Stopped by user during poll wait")
                break
    
    except KeyboardInterrupt:
        print("\n[mongodb_listener] Stopped by user")
    
    finally:
        # Flush Kafka before exit
        if kafka_producer:
            flush_producer(kafka_producer)
        
        stats.summary()


# ─────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="MongoDB Flash Loan Listener"
    )
    
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=5,
        help="Seconds between MongoDB polls (default: 5)",
    )
    
    parser.add_argument(
        "--lookback",
        type=int,
        default=10,
        help="Minutes to look back for 'new' transactions (default: 10)",
    )
    
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Disable Kafka — print detections only",
    )
    
    args = parser.parse_args()
    
    try:
        listen_to_mongodb(
            poll_interval=args.poll_interval,
            lookback_minutes=args.lookback,
            use_kafka=not args.no_kafka,
        )
    except KeyboardInterrupt:
        print("\n[mongodb_listener] Stopped by user")
