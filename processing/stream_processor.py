"""
stream_processor.py — Graph Builder & Cycle Detector (Jack's P3 - Step 2)

Reads decoded_output.csv (from decoder.py),
builds a transaction graph, and detects circular money flows.

Usage:
    python3 stream_processor.py
    python3 stream_processor.py --input decoded_output.csv
"""

import csv
import argparse
from collections import defaultdict

try:
    import networkx as nx
except ImportError:
    print("[error] networkx not installed. Run: pip3 install networkx")
    exit(1)


def load_transactions(csv_path: str) -> list:
    transactions = []
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            transactions.append(row)
    print(f"[loader] Loaded {len(transactions)} transactions from {csv_path}")
    return transactions


def build_graph(transactions: list) -> nx.DiGraph:
    G = nx.DiGraph()
    for tx in transactions:
        sender   = tx["from_address"].lower()
        pool     = tx["to_address"].lower()
        symbol   = tx.get("primary_symbol", "UNKNOWN")
        amount   = tx.get("primary_amount_human", "0")
        protocol = tx.get("protocol", "")
        tx_hash  = tx.get("tx_hash", "")

        G.add_edge(sender, pool,
            tx_hash=tx_hash, symbol=symbol,
            amount=amount, protocol=protocol, action="borrow")
        G.add_edge(pool, sender,
            tx_hash=tx_hash, symbol=symbol,
            amount=amount, protocol=protocol, action="repay")

    print(f"[graph] Nodes: {G.number_of_nodes()} | Edges: {G.number_of_edges()}")
    return G


def detect_cycles(G: nx.DiGraph) -> list:
    print(f"[detector] Running cycle detection (Johnson's algorithm)...")
    cycles = list(nx.simple_cycles(G))
    print(f"[detector] Found {len(cycles)} cycle(s)")
    return cycles


def print_results(cycles: list, transactions: list):
    tx_by_sender = defaultdict(list)
    for tx in transactions:
        tx_by_sender[tx["from_address"].lower()].append(tx)

    suspicious = [tx for tx in transactions if tx.get("data_quality") == "SUSPICIOUS_AMOUNT"]

    print(f"\n{'='*60}")
    print(f"  CYCLE DETECTION RESULTS")
    print(f"{'='*60}")

    if not cycles:
        print("\n  No circular flows detected.")
    else:
        for i, cycle in enumerate(cycles, 1):
            print(f"\n  Cycle #{i}:")
            print(f"  Path   : {' → '.join(cycle[:3])}{'...' if len(cycle) > 3 else ''} → {cycle[0]}")
            print(f"  Length : {len(cycle)} nodes")
            involved = []
            for node in cycle:
                involved.extend(tx_by_sender.get(node, []))
            for tx in involved:
                print(f"    TX      : {tx['tx_hash'][:20]}...")
                print(f"    Protocol: {tx['protocol']}")
                print(f"    Token   : {tx['primary_symbol']}")
                print(f"    Amount  : {tx['primary_amount_human']}")

    print(f"\n{'='*60}")
    print(f"  SUMMARY")
    print(f"{'='*60}")
    print(f"  Transactions analyzed : {len(transactions)}")
    print(f"  Cycles detected       : {len(cycles)}")
    if suspicious:
        print(f"  Suspicious flagged    : {len(suspicious)}")
        for tx in suspicious:
            print(f"    → {tx['tx_hash'][:20]}... ({tx['primary_symbol']} {tx['primary_amount_human']})")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="decoded_output.csv")
    args = parser.parse_args()

    print(f"\n{'='*60}")
    print(f"  Flash Loan Stream Processor — P3 Step 2")
    print(f"{'='*60}\n")

    transactions = load_transactions(args.input)
    if not transactions:
        print("[error] No transactions. Run decoder.py first!")
        return

    G = build_graph(transactions)
    cycles = detect_cycles(G)
    print_results(cycles, transactions)


if __name__ == "__main__":
    main()