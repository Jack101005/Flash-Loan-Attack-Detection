"""
single_process_job.py — Single-process Flash Loan Detection Pipeline

Baseline implementation using identical decode/enrich/score/write logic
as processing/streaming_job.py, but:
  - One Python process, one thread (no Spark)
  - Reads directly from CSV (no Kafka)
  - No distributed state — crash loses all pending (unwritten) records
  - No automatic recovery — restart re-processes from row 0

This file exists to make the distributed vs non-distributed comparison
concrete and measurable. Run it alongside the Spark job to compare
throughput and observe fault tolerance differences.

Usage:
    python benchmarks/single_process_job.py
    python benchmarks/single_process_job.py --crash-after 10 --delay 0.3
    python benchmarks/single_process_job.py --data ingestion/data/test_data_enriched.csv
"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from dotenv import load_dotenv
    # Load root .env, then backend/.env (where MONGODB_URI may live)
    _root = os.path.join(os.path.dirname(__file__), "..")
    load_dotenv(os.path.join(_root, ".env"))
    load_dotenv(os.path.join(_root, "backend", ".env"))
except ImportError:
    print("[single] ⚠  python-dotenv not installed — .env will NOT be loaded.")
    print("[single]    Install with:  pip install python-dotenv")

import certifi
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

# ── Same constants as processing/streaming_job.py ─────────────────────────────

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

STABLECOINS = {"USDC", "USDT", "DAI", "USDe"}

FALLBACK_PRICES = {"WETH": 2000.0, "WBTC": 50000.0}

PROTOCOL_BY_SELECTOR = {
    "0xab9c4b5d": "Aave V3 flashLoan",
    "0x42b0b77c": "Aave V3 flashLoanSimple",
    "0x490e6cbc": "Uniswap V3 flash",
    "0x5c38449e": "Balancer V2 flashLoan",
}

MONGO_URI  = os.getenv("MONGODB_URI")
MONGO_DB   = os.getenv("MONGODB_FLASHLOAN_NAME", "flash_loan_detection")
MONGO_COLL = "transactions_benchmark"  # separate from main collection


# ── Pipeline functions — identical logic to streaming_job.py UDFs ─────────────

def decode_flash_loan(input_hex: str, selector: str) -> dict | None:
    if not input_hex or not selector:
        return None
    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]

        if selector in ("0xab9c4b5d", "0x5c38449e"):
            if len(chunks) < 4:
                return None
            assets_ptr  = int(chunks[1], 16) // 32
            assets_len  = int(chunks[assets_ptr], 16)
            assets = [
                "0x" + chunks[assets_ptr + 1 + i][24:]
                for i in range(assets_len)
                if assets_ptr + 1 + i < len(chunks)
            ]
            amounts_ptr = int(chunks[2], 16) // 32
            amounts_len = int(chunks[amounts_ptr], 16)
            amounts = [
                str(int(chunks[amounts_ptr + 1 + i], 16))
                for i in range(amounts_len)
                if amounts_ptr + 1 + i < len(chunks)
            ]
            if not assets or not amounts:
                return None
            return {"assets": assets, "amounts": amounts}

        elif selector == "0x42b0b77c":
            asset  = "0x" + chunks[1][24:]
            amount = str(int(chunks[2], 16))
            return {"assets": [asset], "amounts": [amount]}

        elif selector == "0x490e6cbc":
            amount0 = str(int(chunks[1], 16))
            amount1 = str(int(chunks[2], 16))
            return {"assets": ["token0", "token1"], "amounts": [amount0, amount1]}

    except Exception:
        return None
    return None


def get_symbol(asset: str) -> str:
    if not asset:
        return "UNKNOWN"
    return TOKEN_SYMBOLS.get(asset.lower(), "UNKNOWN")


def to_human(amount_str: str, symbol: str) -> float:
    try:
        decimals = TOKEN_DECIMALS.get(symbol, 18)
        return float(int(amount_str)) / (10 ** decimals)
    except Exception:
        return 0.0


# Module-level flag: when True, skip Redis + CoinGecko and use static prices.
# Set via run(offline_prices=...) / --offline-prices. Keeps the benchmark fast
# and reproducible by removing external network latency, so the measured
# throughput reflects the parallelizable compute (decode/enrich/score) rather
# than CoinGecko's per-IP rate limit.
OFFLINE_PRICES = False


def get_price_usd(symbol: str, timestamp_unix: float) -> float:
    """Price lookup: Redis cache → CoinGecko → static fallback.
    Same order as streaming_job.py price_udf.

    When OFFLINE_PRICES is set, short-circuits to stablecoin=1.0 / static
    fallback with no network calls.
    """
    if symbol in STABLECOINS:
        return 1.0
    if symbol not in ("WETH", "WBTC"):
        return 0.0

    if OFFLINE_PRICES:
        return FALLBACK_PRICES.get(symbol, 0.0)

    try:
        date_str = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc).strftime("%d-%m-%Y")
    except Exception:
        return FALLBACK_PRICES.get(symbol, 0.0)

    # Try Redis
    try:
        import redis as redis_lib
        r = redis_lib.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            password=os.getenv("REDIS_PASS") or None,
            decode_responses=True,
            socket_connect_timeout=2,
        )
        cache_key = f"hist_price:{symbol}:{date_str}"
        cached = r.get(cache_key)
        if cached:
            return float(cached)
    except Exception:
        pass

    # Try CoinGecko
    try:
        import requests
        coin_id = {"WETH": "ethereum", "WBTC": "bitcoin"}[symbol]
        url = (f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
               f"?date={date_str}&localization=false")
        resp = requests.get(url, timeout=5)
        if resp.ok:
            data = resp.json()
            market_data = data.get("market_data")
            if market_data:
                return float(market_data["current_price"]["usd"])
    except Exception:
        pass

    return FALLBACK_PRICES.get(symbol, 0.0)


def score_confidence(amount_usd: float) -> str:
    if amount_usd >= 1_000_000:
        return "HIGH"
    elif amount_usd >= 100_000:
        return "MEDIUM"
    return "LOW"


def process_transaction(tx: dict) -> dict | None:
    """Process one transaction through the full pipeline.
    Returns a MongoDB-ready doc, or None if tx is not a flash loan.
    """
    input_hex = tx.get("input", "")
    if len(input_hex) < 10:
        return None

    selector = input_hex[:10].lower()
    if selector not in PROTOCOL_BY_SELECTOR:
        return None

    decoded = decode_flash_loan(input_hex, selector)
    if not decoded or not decoded.get("assets"):
        return None

    primary_asset  = decoded["assets"][0]
    primary_amount = decoded["amounts"][0]
    symbol         = get_symbol(primary_asset)
    human_amount   = to_human(primary_amount, symbol)

    try:
        # CSV stores block_timestamp; Kafka messages use timestamp
        timestamp = float(tx.get("block_timestamp") or tx.get("timestamp") or 0)
    except (ValueError, TypeError):
        timestamp = 0.0

    price      = get_price_usd(symbol, timestamp)
    amount_usd = human_amount * price
    confidence = score_confidence(amount_usd)

    return {
        "tx_hash":      tx.get("tx_hash", ""),
        "protocol":     PROTOCOL_BY_SELECTOR[selector],
        "from":         tx.get("from", ""),
        "pool":         tx.get("to", ""),
        "token":        symbol,
        "amount_human": human_amount,
        "amount_usd":   round(amount_usd, 2),
        "confidence":   confidence,
        "timestamp":    timestamp,
        "processed_at": int(time.time()),
        "mode":         "single_process",
    }


def resolve_data_path(data_arg: str) -> str:
    """Resolve the dataset path, tolerating either repo layout.

    The CSV has historically lived at both data/ and ingestion/data/.
    Try the path as given, then the same filename under both known dirs.
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    filename = os.path.basename(data_arg)
    candidates = [
        os.path.join(project_root, data_arg),
        os.path.join(project_root, "data", filename),
        os.path.join(project_root, "ingestion", "data", filename),
    ]
    for c in candidates:
        if os.path.isfile(c):
            return c
    raise FileNotFoundError(
        f"Could not find dataset '{filename}'. Looked in:\n  "
        + "\n  ".join(candidates)
    )


def load_csv(csv_path: str) -> list[dict]:
    rows = []
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rows.append(dict(row))
    return rows


def write_batch_to_mongo(docs: list[dict]) -> int:
    """Bulk upsert to MongoDB. Single connection, sequential — no parallelism."""
    if not docs:
        return 0
    if not MONGO_URI:
        print("[single] ⚠  MONGODB_URI not set — skipping MongoDB write. "
              "Check that .env is loaded and contains MONGODB_URI.")
        return 0
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        tls=True,
        tlsCAFile=certifi.where(),
    )
    try:
        coll = client[MONGO_DB][MONGO_COLL]
        ops  = [UpdateOne({"tx_hash": d["tx_hash"]}, {"$set": d}, upsert=True) for d in docs]
        result = coll.bulk_write(ops, ordered=False)
        return result.upserted_count + result.modified_count
    except PyMongoError as e:
        print(f"[single] MongoDB write error: {e}")
        return 0
    finally:
        client.close()


def run(
    csv_path: str,
    crash_after: int = None,
    delay: float = 0.0,
    batch_size: int = 10,
    quiet: bool = False,
    offline_prices: bool = False,
) -> dict:
    """
    Run the single-process pipeline end-to-end.

    Args:
        csv_path:    Input CSV file.
        crash_after: Simulate a crash after N successfully processed txs.
                     Pending (un-flushed) docs in the current batch are LOST.
        delay:       Artificial per-tx sleep (seconds) to slow the demo.
        batch_size:  Number of docs to accumulate before writing to MongoDB.
        quiet:       Suppress per-tx console output.
        offline_prices: Skip Redis + CoinGecko, use static fallback prices.
                     Makes the run fast + reproducible for throughput benchmarks.

    Returns a stats dict suitable for the benchmark comparison table.
    """
    global OFFLINE_PRICES
    OFFLINE_PRICES = offline_prices

    print(f"\n{'='*60}")
    print(f"  SINGLE-PROCESS PIPELINE")
    print(f"{'='*60}")
    if crash_after:
        print(f"  Will simulate crash after {crash_after} tx  (fault tolerance demo)")
    print(f"  Source:     {csv_path}")
    print(f"  Batch size: {batch_size}")
    print(f"  Prices:     {'OFFLINE (static fallback)' if offline_prices else 'LIVE (Redis → CoinGecko)'}")
    print(f"{'='*60}\n")

    rows = load_csv(csv_path)
    print(f"[single] Loaded {len(rows)} rows from CSV")

    stats = {
        "total_input":  len(rows),
        "processed":    0,
        "written":      0,
        "errors":       0,
        "crashed":      False,
        "start_time":   time.time(),
        "end_time":     None,
        "elapsed_sec":  None,
        "throughput":   None,
    }

    pending: list[dict] = []

    for i, row in enumerate(rows):
        # ── Simulate crash ────────────────────────────────────────────────────
        if crash_after is not None and stats["processed"] >= crash_after:
            print(f"\n[single] ⚡ SIMULATED CRASH after {stats['processed']} transactions")
            print(f"[single]    {len(pending)} docs were pending (in-memory, NOT written)")
            print(f"[single]    These records are LOST — no checkpoint exists")
            print(f"[single]    Restart will re-process from row 0")
            stats["crashed"]    = True
            stats["end_time"]   = time.time()
            stats["elapsed_sec"] = stats["end_time"] - stats["start_time"]
            return stats

        if delay > 0:
            time.sleep(delay)

        doc = process_transaction(row)
        if doc:
            pending.append(doc)
            stats["processed"] += 1
            if not quiet:
                print(f"[single] {i+1:3d}/{len(rows)}  "
                      f"{doc['tx_hash'][:20]}...  "
                      f"{doc['confidence']:6s}  "
                      f"${doc['amount_usd']:>12,.0f}")
        else:
            stats["errors"] += 1

        # ── Flush batch to MongoDB ────────────────────────────────────────────
        if len(pending) >= batch_size:
            written = write_batch_to_mongo(pending)
            stats["written"] += written
            pending.clear()

    # Flush remainder
    if pending:
        written = write_batch_to_mongo(pending)
        stats["written"] += written

    stats["end_time"]   = time.time()
    stats["elapsed_sec"] = stats["end_time"] - stats["start_time"]
    if stats["elapsed_sec"] > 0:
        stats["throughput"] = round(stats["processed"] / stats["elapsed_sec"], 2)

    print(f"\n{'='*60}")
    print(f"  SINGLE-PROCESS COMPLETE")
    print(f"  Input rows:  {stats['total_input']}")
    print(f"  Processed:   {stats['processed']}")
    print(f"  Written:     {stats['written']}")
    print(f"  Errors:      {stats['errors']}")
    print(f"  Time:        {stats['elapsed_sec']:.2f}s")
    if stats["throughput"]:
        print(f"  Throughput:  {stats['throughput']} tx/sec")
    print(f"{'='*60}\n")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Single-process flash loan detection baseline")
    parser.add_argument("--data", default="data/test_data_enriched.csv",
                        help="Path to input CSV (relative to project root)")
    parser.add_argument("--crash-after", type=int, default=None,
                        help="Simulate crash after N processed transactions")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="Artificial per-tx delay in seconds (slows demo)")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--offline-prices", action="store_true",
                        help="Skip Redis + CoinGecko; use static fallback prices "
                             "(fast + reproducible — recommended for benchmarking)")
    args = parser.parse_args()

    csv_path = resolve_data_path(args.data)

    run(
        csv_path,
        crash_after=args.crash_after,
        delay=args.delay,
        batch_size=args.batch_size,
        quiet=args.quiet,
        offline_prices=args.offline_prices,
    )
