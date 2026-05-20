"""
stream_processor.py - Flash Loan Stream Processor (Jack's P3)

Reads decoded flash loan transactions from Kafka topic 'raw_txns'
(produced by Ngan's listener.py), builds a transaction graph,
detects cycles, and outputs suspicious transactions.

Pipeline:
    Kafka raw_txns -> decode -> graph -> cycle detection -> print

Usage:
    python3 processing/stream_processor.py
    python3 processing/stream_processor.py --csv-fallback file.csv
"""

import sys
import os
import json
import time
import argparse
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import networkx as nx
except ImportError:
    print("[error] networkx not installed. Run: pip3 install networkx")
    sys.exit(1)

try:
    from broker.kafka_consumer import create_consumer, consume_messages
    KAFKA_AVAILABLE = True
except ImportError as e:
    print(f"[warning] Kafka consumer not available: {e}")
    KAFKA_AVAILABLE = False


TOKEN_SYMBOLS = {
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "USDC",
    "0xdac17f958d2ee523a2206206994597c13d831ec7": "USDT",
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "WETH",
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": "WBTC",
    "0x4c9edd5852cd905f086c759e8383e09bff1e68b3": "USDe",
    "0x6b175474e89094c44da98b954eedeac495271d0f": "DAI",
}

TOKEN_DECIMALS = {
    "USDC": 6, "USDT": 6, "WETH": 18, "WBTC": 8, "USDe": 18, "DAI": 18,
}


def manual_decode_flash_loan(input_hex):
    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
        if len(chunks) < 6:
            return None
        assets_ptr = int(chunks[1], 16) // 32
        assets_len = int(chunks[assets_ptr], 16)
        assets = ["0x" + chunks[assets_ptr+1+i][24:]
                  for i in range(assets_len)
                  if assets_ptr+1+i < len(chunks)]
        amounts_ptr = int(chunks[2], 16) // 32
        amounts_len = int(chunks[amounts_ptr], 16)
        amounts = [int(chunks[amounts_ptr+1+i], 16)
                   for i in range(amounts_len)
                   if amounts_ptr+1+i < len(chunks)]
        return {"assets": assets, "amounts": amounts, "function": "flashLoan"}
    except Exception:
        return None


def manual_decode_flash_loan_simple(input_hex):
    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
        asset = "0x" + chunks[1][24:]
        amount = int(chunks[2], 16)
        return {
            "assets": [asset], "amounts": [amount],
            "function": "flashLoanSimple",
        }
    except Exception:
        return None


def manual_decode_uniswap_flash(input_hex):
    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
        amount0 = int(chunks[1], 16)
        amount1 = int(chunks[2], 16)
        return {
            "assets": ["token0", "token1"],
            "amounts": [amount0, amount1],
            "function": "flash",
        }
    except Exception:
        return None


def decode_transaction(tx):
    selector = tx.get("input", "0x")[:10]
    if selector == "0xab9c4b5d":
        decoded = manual_decode_flash_loan(tx["input"])
        protocol = "Aave V3 flashLoan"
    elif selector == "0x42b0b77c":
        decoded = manual_decode_flash_loan_simple(tx["input"])
        protocol = "Aave V3 flashLoanSimple"
    elif selector == "0x490e6cbc":
        decoded = manual_decode_uniswap_flash(tx["input"])
        protocol = "Uniswap V3 flash"
    else:
        return None

    if not decoded:
        return None

    return {
        "tx_hash": tx["tx_hash"],
        "from": tx["from"].lower(),
        "to": tx["to"].lower(),
        "protocol": protocol,
        "selector": selector,
        "function": decoded["function"],
        "assets": decoded["assets"],
        "amounts": decoded["amounts"],
    }


def get_symbol(asset):
    return TOKEN_SYMBOLS.get(asset.lower(), "UNKNOWN")


def to_human(amount, symbol):
    decimals = TOKEN_DECIMALS.get(symbol, 18)
    return amount / (10 ** decimals)


def build_graph(transactions):
    G = nx.DiGraph()
    for tx in transactions:
        sender = tx["from"]
        pool = tx["to"]
        G.add_edge(sender, pool, tx_hash=tx["tx_hash"], action="borrow")
        G.add_edge(pool, sender, tx_hash=tx["tx_hash"], action="repay")
    return G


def detect_cycles(G):
    start = time.perf_counter()
    cycles = list(nx.simple_cycles(G))
    elapsed_ms = (time.perf_counter() - start) * 1000
    if elapsed_ms > 50:
        print(f"[warning] Cycle detection took {elapsed_ms:.1f}ms")
    return cycles


def score_confidence(has_cycle, amount_usd, threshold=1_000_000):
    if has_cycle and amount_usd > threshold:
        return "HIGH"
    elif has_cycle:
        return "MEDIUM"
    else:
        return "LOW"


def print_detection(decoded_tx, has_cycle, confidence):
    primary_symbol = get_symbol(decoded_tx["assets"][0]) if decoded_tx["assets"] else "UNKNOWN"
    primary_amount = to_human(decoded_tx["amounts"][0], primary_symbol) if decoded_tx["amounts"] else 0

    color = {"HIGH": "\033[91m", "MEDIUM": "\033[93m", "LOW": "\033[92m"}.get(confidence, "")
    reset = "\033[0m"

    print(f"\n{'='*60}")
    print(f"  {color}[{confidence}]{reset} Flash Loan Detection")
    print(f"{'='*60}")
    print(f"  TX Hash    : {decoded_tx['tx_hash'][:30]}...")
    print(f"  Protocol   : {decoded_tx['protocol']}")
    print(f"  From       : {decoded_tx['from']}")
    print(f"  Token      : {primary_symbol}")
    print(f"  Amount     : {primary_amount:,.4f}")
    print(f"  Has cycle  : {'YES' if has_cycle else 'no'}")
    print(f"{'='*60}")


def run_kafka_mode():
    print(f"\n{'='*60}")
    print(f"  Flash Loan Stream Processor - P3 (Kafka mode)")
    print(f"{'='*60}\n")

    if not KAFKA_AVAILABLE:
        print("[error] Kafka consumer not available.")
        return

    print("[processor] Connecting to Kafka topic 'raw_txns'...")
    consumer = create_consumer(group_id="flash_loan_detectors")
    print("[processor] Connected. Waiting for messages...\n")

    transactions = []
    detection_count = 0

    try:
        for tx in consume_messages(consumer):
            decoded = decode_transaction(tx)
            if not decoded:
                continue

            transactions.append(decoded)
            detection_count += 1

            G = build_graph(transactions)
            cycles = detect_cycles(G)
            has_cycle = len(cycles) > 0

            primary_symbol = get_symbol(decoded["assets"][0]) if decoded["assets"] else "UNKNOWN"
            primary_amount = to_human(decoded["amounts"][0], primary_symbol) if decoded["amounts"] else 0
            confidence = score_confidence(has_cycle, primary_amount)

            print_detection(decoded, has_cycle, confidence)
            print(f"\n[stats] Total: {detection_count} | Cycles: {len(cycles)}")

    except KeyboardInterrupt:
        print(f"\n[processor] Stopped by user.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-fallback", help="Use CSV instead of Kafka")
    args = parser.parse_args()
    run_kafka_mode()


if __name__ == "__main__":
    main()