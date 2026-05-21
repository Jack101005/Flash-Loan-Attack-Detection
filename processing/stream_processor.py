"""
stream_processor.py - Flash Loan Stream Processor

Reads decoded flash loan transactions from Kafka topic 'raw_txns',
builds a per-sender borrow graph, scores confidence, looks up
historical token prices via Redis/CoinGecko, and pushes detections
to Redis DETECTION_QUEUE for the backend API.

Pipeline:
    Kafka raw_txns -> decode -> borrow graph -> score -> Redis push

Usage:
    python3 processing/stream_processor.py
"""

import sys
import os
import json
import time
import argparse
from collections import defaultdict
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import redis as redis_lib
    _redis_client = redis_lib.Redis(
        host=os.getenv("REDIS_HOST", "localhost"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        password=os.getenv("REDIS_PASS") or None,
        decode_responses=True,
        socket_connect_timeout=3,
    )
    _redis_client.ping()
    REDIS_AVAILABLE = True
    print("[processor] Redis connected.")
except Exception as _e:
    _redis_client = None
    REDIS_AVAILABLE = False
    print(f"[processor] Redis not available ({_e}). Detections will be printed only.")

try:
    from broker.kafka_consumer import create_consumer, consume_messages
    KAFKA_AVAILABLE = True
except ImportError as e:
    print(f"[warning] Kafka consumer not available: {e}")
    KAFKA_AVAILABLE = False

try:
    from storage.mongo_store import save_detection as _mongo_save
    MONGO_AVAILABLE = True
except Exception as e:
    print(f"[processor] MongoDB not available ({e}). Detections will be printed only.")
    MONGO_AVAILABLE = False
    _mongo_save = None


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

COINGECKO_IDS = {
    "WETH": "ethereum",
    "WBTC": "bitcoin",
}

STABLECOINS = {"USDC", "USDT", "DAI", "USDe"}

FALLBACK_PRICES = {
    "WETH": 2000.0,
    "WBTC": 50000.0,
}

PROTOCOL_LABELS = {
    "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": "Aave V3 Pool",
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "Balancer V2 Vault",
    "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8": "Uniswap V3 USDC/ETH",
}


def _decode_array_pair(input_hex: str, function_name: str) -> dict | None:
    """Generic ABI decoder for (address recipient/receiver, address[] tokens, uint256[] amounts, ...).
    Works for Aave V3 flashLoan AND Balancer V2 flashLoan since both share the same
    dynamic-array layout at chunks[1] (tokens ptr) and chunks[2] (amounts ptr).
    """
    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]
        if len(chunks) < 4:
            return None
        assets_ptr = int(chunks[1], 16) // 32
        assets_len = int(chunks[assets_ptr], 16)
        assets = ["0x" + chunks[assets_ptr + 1 + i][24:]
                  for i in range(assets_len)
                  if assets_ptr + 1 + i < len(chunks)]
        amounts_ptr = int(chunks[2], 16) // 32
        amounts_len = int(chunks[amounts_ptr], 16)
        amounts = [int(chunks[amounts_ptr + 1 + i], 16)
                   for i in range(amounts_len)
                   if amounts_ptr + 1 + i < len(chunks)]
        if not assets or not amounts:
            return None
        return {"assets": assets, "amounts": amounts, "function": function_name}
    except Exception:
        return None


def manual_decode_flash_loan(input_hex):
    return _decode_array_pair(input_hex, "flashLoan")


def manual_decode_balancer_flash(input_hex):
    return _decode_array_pair(input_hex, "flashLoan")


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
    elif selector == "0x5c38449e":
        decoded = manual_decode_balancer_flash(tx["input"])
        protocol = "Balancer V2 flashLoan"
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
        "timestamp": tx.get("timestamp", time.time()),
    }


def get_symbol(asset):
    return TOKEN_SYMBOLS.get(asset.lower(), "UNKNOWN")


def to_human(amount, symbol):
    decimals = TOKEN_DECIMALS.get(symbol, 18)
    return amount / (10 ** decimals)


def get_historical_price_usd(symbol: str, unix_timestamp: float) -> float:
    """Return USD price of `symbol` at `unix_timestamp`.
    Stablecoins return 1.0 immediately.
    Non-stablecoins: check Redis cache first, then CoinGecko /history.
    """
    if symbol in STABLECOINS:
        return 1.0

    coin_id = COINGECKO_IDS.get(symbol)
    if not coin_id:
        return 0.0

    date_str = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc).strftime("%d-%m-%Y")
    redis_key = f"hist_price:{symbol}:{date_str}"

    if _redis_client:
        try:
            cached = _redis_client.get(redis_key)
            if cached:
                return float(cached)
        except Exception:
            pass

    try:
        url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
            f"?date={date_str}&localization=false"
        )
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        price = resp.json()["market_data"]["current_price"]["usd"]
        if _redis_client:
            try:
                _redis_client.set(redis_key, price)
            except Exception:
                pass
        return float(price)
    except Exception as e:
        print(f"[price] CoinGecko failed for {symbol} on {date_str}: {e}")
        return FALLBACK_PRICES.get(symbol, 0.0)


def analyze_sender(sender: str, pool: str, sender_history: dict) -> dict:
    """Return borrow analysis for a sender.
    sender_history: {sender: [(pool, protocol, amount_usd)]}
    Returns {pools_count, total_usd, pools}
    """
    history = sender_history.get(sender, [])
    unique_pools = {entry["pool"] for entry in history}
    total_usd = sum(entry["amount_usd"] for entry in history)
    return {
        "pools_count": len(unique_pools),
        "total_usd": total_usd,
        "pools": list(unique_pools),
    }


def score_confidence(amount_usd: float) -> str:
    if amount_usd >= 1_000_000:
        return "HIGH"
    elif amount_usd >= 100_000:
        return "MEDIUM"
    else:
        return "LOW"


def build_cycle_path(sender: str, pool: str) -> list[str]:
    pool_label = PROTOCOL_LABELS.get(pool, pool[:10] + "...")
    return [sender[:10] + "...", pool_label, sender[:10] + "..."]


def push_detection(detection: dict) -> None:
    """Save detection to MongoDB for the backend API."""
    if not MONGO_AVAILABLE or _mongo_save is None:
        return
    try:
        _mongo_save(detection)
    except Exception as e:
        print(f"[processor] MongoDB save failed: {e}")


def print_detection(decoded_tx: dict, analysis: dict, confidence: str, amount_usd: float) -> None:
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
    print(f"  Amount     : {primary_amount:,.4f} ({amount_usd:,.0f} USD)")
    print(f"  Pools (sender): {analysis['pools_count']}")
    print(f"{'='*60}")


def run_kafka_mode():
    print(f"\n{'='*60}")
    print(f"  Flash Loan Stream Processor (Kafka mode)")
    print(f"{'='*60}\n")

    if not KAFKA_AVAILABLE:
        print("[error] Kafka consumer not available.")
        return

    print("[processor] Connecting to Kafka topic 'raw_txns'...")
    consumer = create_consumer(group_id="flash_loan_detectors")
    print("[processor] Connected. Waiting for messages...\n")

    sender_history: dict = defaultdict(list)
    detection_count = 0

    try:
        for tx in consume_messages(consumer):
            decoded = decode_transaction(tx)
            if not decoded:
                continue

            detection_count += 1
            sender   = decoded["from"]
            pool     = decoded["to"]
            ts       = decoded["timestamp"]

            primary_symbol = get_symbol(decoded["assets"][0]) if decoded["assets"] else "UNKNOWN"
            primary_amount = to_human(decoded["amounts"][0], primary_symbol) if decoded["amounts"] else 0
            price_usd      = get_historical_price_usd(primary_symbol, ts)
            amount_usd     = primary_amount * price_usd

            sender_history[sender].append({
                "pool":       pool,
                "protocol":   decoded["protocol"],
                "amount_usd": amount_usd,
                "tx_hash":    decoded["tx_hash"],
            })

            analysis   = analyze_sender(sender, pool, sender_history)
            confidence = score_confidence(amount_usd)

            print_detection(decoded, analysis, confidence, amount_usd)
            print(f"[stats] Total: {detection_count} | Sender pools: {analysis['pools_count']} | USD: ${analysis['total_usd']:,.0f}")

            detection_doc = {
                "tx_hash":      decoded["tx_hash"],
                "protocol":     decoded["protocol"],
                "from":         sender,
                "pool":         pool,
                "token":        primary_symbol,
                "amount_usd":   round(amount_usd, 2),
                "total_usd":    round(analysis["total_usd"], 2),
                "pools_count":  analysis["pools_count"],
                "confidence":   confidence,
                "cycle_path":   build_cycle_path(sender, pool),
                "timestamp":    int(time.time()),
            }
            push_detection(detection_doc)

    except KeyboardInterrupt:
        print(f"\n[processor] Stopped by user.")


def main():
    parser = argparse.ArgumentParser()
    parser.parse_args()
    run_kafka_mode()


if __name__ == "__main__":
    main()