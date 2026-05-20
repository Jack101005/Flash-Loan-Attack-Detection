"""
Import DeFi CSVs into MongoDB using PyMongo.

Install deps:
    pip install pymongo pandas

Run:
    python import_pymongo.py
    python import_pymongo.py --uri "mongodb+srv://user:pass@cluster.mongodb.net"
"""

import argparse
import json
import os
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

COLLECTIONS = [
    "balancer_v2",
    "aave_v3",
    "flash_loan",
    "flash_loan_simple",
    "uniswap_v3",
]


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        c.strip().lower()
         .replace(" ", "_")
         .replace("(", "")
         .replace(")", "")
         .replace("/", "_")
        for c in df.columns
    ]
    return df


def load_json(name: str) -> list[dict]:
    path = Path(__file__).parent / f"{name}.json"
    with open(path) as f:
        return json.load(f)


def import_collection(db, name: str, drop: bool = True) -> int:
    docs = load_json(name)
    col = db[name]
    if drop:
        col.drop()
    if not docs:
        return 0
    result = col.insert_many(docs, ordered=False)
    col.create_index([("transaction_hash", ASCENDING)])
    col.create_index([("source", ASCENDING)])
    if "datetime_utc" in docs[0]:
        col.create_index([("datetime_utc", ASCENDING)])
    if "block" in docs[0]:
        col.create_index([("block", ASCENDING)])
    return len(result.inserted_ids)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default="mongodb://localhost:27017",
                        help="MongoDB connection URI")
    parser.add_argument("--db", default="defi_transactions",
                        help="Database name")
    parser.add_argument("--no-drop", action="store_true",
                        help="Don't drop existing collections before import")
    args = parser.parse_args()

    client = MongoClient(args.uri, serverSelectionTimeoutMS=5000)
    db = client[args.db]

    print(f"Connected to: {args.uri}")
    print(f"Target database: {args.db}\n")

    total = 0
    for name in COLLECTIONS:
        count = import_collection(db, name, drop=not args.no_drop)
        print(f"  {name:<22} {count:>4} documents inserted")
        total += count

    print(f"\nTotal: {total} documents across {len(COLLECTIONS)} collections.")
    client.close()


if __name__ == "__main__":
    main()
