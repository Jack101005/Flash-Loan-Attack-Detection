"""
mongodb_reader.py — Read flash loan transactions from MongoDB

Provides streaming and batch query interfaces to MongoDB transactions collection.
Use this instead of Kafka consumer for batch processing or replay scenarios.

Example:
    from processing.mongodb_reader import read_transactions_from_mongo
    
    for tx in read_transactions_from_mongo(filters={"risk_level": "HIGH"}):
        print(tx["tx_hash"], tx["summary"])
"""

import sys
import os
from typing import Generator, Optional

# Allow importing from storage/ sibling package
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from storage.mongo_store import transactions_collection, get_db
from pymongo import DESCENDING, ASCENDING


def read_transactions_from_mongo(
    filters: Optional[dict] = None,
    batch_size: int = 100,
    sort_field: str = "detected_at",
    sort_direction: int = DESCENDING,
) -> Generator[dict, None, None]:
    """
    Yields transactions from MongoDB as a generator.
    
    Args:
        filters: MongoDB query filter dict
                 (e.g., {"risk_level": "HIGH", "is_flash_loan": true})
        batch_size: Number of docs to fetch per batch (for performance)
        sort_field: Field to sort by (default: "detected_at")
        sort_direction: ASCENDING or DESCENDING (default: DESCENDING)
    
    Yields:
        dict: Transaction document (with MongoDB _id removed)
    
    Example:
        >>> for tx in read_transactions_from_mongo(filters={"is_flash_loan": True}):
        ...     print(tx["tx_hash"])
    """
    try:
        collection = transactions_collection()
    except Exception as e:
        print(f"[mongodb_reader] Failed to connect to MongoDB: {e}")
        return
    
    try:
        cursor = (
            collection
            .find(filters or {})
            .sort(sort_field, sort_direction)
            .batch_size(batch_size)
        )
        
        for doc in cursor:
            # Remove MongoDB internal _id
            doc.pop("_id", None)
            yield doc
    
    except Exception as e:
        print(f"[mongodb_reader] Query failed: {e}")


def read_recent_detections(
    limit: int = 1000,
    risk_level: Optional[str] = None
) -> list[dict]:
    """
    Return most recent N flash loan detections from MongoDB.
    
    Args:
        limit: Maximum number of documents to return
        risk_level: Filter by risk level ("LOW", "MEDIUM", "HIGH", "CRITICAL")
    
    Returns:
        List of transaction dicts (newest first)
    
    Example:
        >>> critical = read_recent_detections(limit=50, risk_level="CRITICAL")
        >>> print(f"Found {len(critical)} critical detections")
    """
    try:
        collection = transactions_collection()
    except Exception as e:
        print(f"[mongodb_reader] Failed to connect to MongoDB: {e}")
        return []
    
    filters = {"is_flash_loan": True}
    if risk_level:
        filters["risk_level"] = risk_level
    
    try:
        cursor = (
            collection
            .find(filters)
            .sort("detected_at", DESCENDING)
            .limit(limit)
        )
        
        results = []
        for doc in cursor:
            doc.pop("_id", None)
            results.append(doc)
        
        return results
    
    except Exception as e:
        print(f"[mongodb_reader] Query failed: {e}")
        return []


def count_detections(filters: Optional[dict] = None) -> int:
    """
    Count total detections matching filters.
    
    Args:
        filters: MongoDB query filter
    
    Returns:
        Document count
    
    Example:
        >>> total = count_detections({"is_flash_loan": True})
        >>> critical = count_detections({"is_flash_loan": True, "risk_level": "CRITICAL"})
        >>> print(f"Total: {total}, Critical: {critical}")
    """
    try:
        collection = transactions_collection()
        return collection.count_documents(filters or {})
    except Exception as e:
        print(f"[mongodb_reader] Count failed: {e}")
        return 0


def get_transaction_by_hash(tx_hash: str) -> Optional[dict]:
    """
    Fetch a single transaction by hash.
    
    Args:
        tx_hash: Transaction hash string
    
    Returns:
        Transaction dict or None if not found
    
    Example:
        >>> tx = get_transaction_by_hash("0xabcd...")
        >>> if tx:
        ...     print(tx["summary"])
    """
    try:
        collection = transactions_collection()
        doc = collection.find_one(
            {"tx_hash": tx_hash},
            {"_id": 0}
        )
        return doc
    except Exception as e:
        print(f"[mongodb_reader] Lookup failed: {e}")
        return None


def get_transactions_by_protocol(protocol: str, limit: int = 100) -> list[dict]:
    """
    Get all detections from a specific protocol.
    
    Args:
        protocol: Protocol name ("Aave V3", "Balancer V2", "Uniswap V3")
        limit: Maximum results
    
    Returns:
        List of transaction dicts
    
    Example:
        >>> aave_txs = get_transactions_by_protocol("Aave V3", limit=50)
        >>> print(f"Found {len(aave_txs)} Aave V3 transactions")
    """
    try:
        collection = transactions_collection()
        cursor = (
            collection
            .find({"raw_data.protocol": protocol})
            .sort("detected_at", DESCENDING)
            .limit(limit)
        )
        
        results = []
        for doc in cursor:
            doc.pop("_id", None)
            results.append(doc)
        
        return results
    
    except Exception as e:
        print(f"[mongodb_reader] Protocol query failed: {e}")
        return []


def get_stats() -> dict:
    """
    Return MongoDB collection statistics.
    
    Returns:
        dict with counts by risk_level and is_flash_loan status
    
    Example:
        >>> stats = get_stats()
        >>> print(f"Total detections: {stats['total_detections']}")
        >>> print(f"By risk level: {stats['by_risk_level']}")
    """
    try:
        collection = transactions_collection()
        
        # Aggregation pipeline for stats
        pipeline = [
            {"$group": {
                "_id": {
                    "is_flash_loan": "$is_flash_loan",
                    "risk_level": "$risk_level"
                },
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        stats_result = list(collection.aggregate(pipeline))
        
        total = sum(s["count"] for s in stats_result)
        by_risk = {}
        
        for item in stats_result:
            if item["_id"]["is_flash_loan"]:
                risk = item["_id"]["risk_level"]
                by_risk[risk] = item["count"]
        
        return {
            "total_detections": total,
            "flash_loan_detections": sum(by_risk.values()),
            "by_risk_level": by_risk,
            "collection_stats": collection.estimated_document_count()
        }
    
    except Exception as e:
        print(f"[mongodb_reader] Stats query failed: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────────
# CLI usage
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="MongoDB Flash Loan Reader CLI"
    )
    
    parser.add_argument(
        "--command",
        choices=["recent", "stats", "count", "all"],
        default="stats",
        help="Query type (default: stats)"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Result limit (default: 10)"
    )
    
    parser.add_argument(
        "--risk-level",
        choices=["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        help="Filter by risk level"
    )
    
    args = parser.parse_args()
    
    print("[mongodb_reader] Connecting to MongoDB...\n")
    
    if args.command == "stats":
        stats = get_stats()
        print("Collection Statistics:")
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    elif args.command == "recent":
        recent = read_recent_detections(
            limit=args.limit,
            risk_level=args.risk_level
        )
        print(f"Recent {len(recent)} detections:")
        for tx in recent:
            print(f"  {tx['tx_hash'][:16]}...  "
                  f"Risk: {tx.get('risk_level', 'N/A')}  "
                  f"Summary: {tx.get('summary', 'N/A')[:60]}")
    
    elif args.command == "count":
        filters = {}
        if args.risk_level:
            filters["risk_level"] = args.risk_level
        count = count_detections(filters)
        print(f"Total detections matching filters: {count}")
    
    elif args.command == "all":
        print(f"Streaming all detections (limit: {args.limit})...")
        for i, tx in enumerate(read_transactions_from_mongo(limit=args.limit)):
            if i >= args.limit:
                break
            print(f"  {i+1}. {tx['tx_hash'][:16]}...")
