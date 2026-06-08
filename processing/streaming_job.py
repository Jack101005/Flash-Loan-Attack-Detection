"""
streaming_job.py - PySpark Structured Streaming Job for Flash Loan Detection

Reads raw transactions from Kafka topic 'raw_txns', decodes flash loan calldata,
computes USD values using historical prices (Redis cache + CoinGecko fallback),
scores confidence (HIGH/MEDIUM/LOW), and writes detections to MongoDB.

Submitted via spark-submit to spark://spark-master:7077.
Designed to run inside the processing-job Docker container.

Pipeline (distributed):
    Kafka raw_txns
        ↓ readStream
    Spark partitions (parallel)
        ↓ decode + price lookup (UDF)
        ↓ score confidence
        ↓ foreachBatch → foreachPartition (distributed write)
    MongoDB detections collection

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        streaming_job.py
"""

import os
import json
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType,
    ArrayType, IntegerType, MapType
)

# ─── Configuration ────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC     = "raw_txns"
MONGO_URI       = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGO_DB        = os.getenv("MONGODB_FLASHLOAN_NAME", "flash_loan_detection")
MONGO_COLL      = "transactions"
REDIS_HOST      = os.getenv("REDIS_HOST", "redis")
REDIS_PORT      = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASS      = os.getenv("REDIS_PASS", "")
CHECKPOINT_DIR  = "/tmp/spark-checkpoints/flash-loan-detection"


# ─── Constants (broadcast to workers) ─────────────────────────────────────────
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

# Static fallback prices if Redis + CoinGecko both fail
FALLBACK_PRICES = {
    "WETH": 2000.0,
    "WBTC": 50000.0,
}

PROTOCOL_BY_SELECTOR = {
    "0xab9c4b5d": "Aave V3 flashLoan",
    "0x42b0b77c": "Aave V3 flashLoanSimple",
    "0x490e6cbc": "Uniswap V3 flash",
    "0x5c38449e": "Balancer V2 flashLoan",
}


# ─── UDF: Decode flash loan calldata ──────────────────────────────────────────
def decode_flash_loan_udf(input_hex: str, selector: str) -> dict:
    """Decode raw calldata into structured flash loan info.
    Runs on Spark workers in parallel.
    """
    if not input_hex or not selector:
        return None

    try:
        raw = input_hex[10:]
        chunks = [raw[i:i+64] for i in range(0, len(raw), 64)]

        if selector in ("0xab9c4b5d", "0x5c38449e"):  # Aave V3 / Balancer V2 flashLoan
            if len(chunks) < 4:
                return None
            assets_ptr = int(chunks[1], 16) // 32
            assets_len = int(chunks[assets_ptr], 16)
            assets = ["0x" + chunks[assets_ptr + 1 + i][24:]
                      for i in range(assets_len)
                      if assets_ptr + 1 + i < len(chunks)]
            amounts_ptr = int(chunks[2], 16) // 32
            amounts_len = int(chunks[amounts_ptr], 16)
            amounts = [str(int(chunks[amounts_ptr + 1 + i], 16))
                       for i in range(amounts_len)
                       if amounts_ptr + 1 + i < len(chunks)]
            if not assets or not amounts:
                return None
            return {"assets": assets, "amounts": amounts}

        elif selector == "0x42b0b77c":  # Aave V3 flashLoanSimple
            asset = "0x" + chunks[1][24:]
            amount = str(int(chunks[2], 16))
            return {"assets": [asset], "amounts": [amount]}

        elif selector == "0x490e6cbc":  # Uniswap V3 flash
            amount0 = str(int(chunks[1], 16))
            amount1 = str(int(chunks[2], 16))
            return {"assets": ["token0", "token1"], "amounts": [amount0, amount1]}

    except Exception:
        return None

    return None


# Schema for the decoded UDF output
decoded_schema = StructType([
    StructField("assets",  ArrayType(StringType()), nullable=True),
    StructField("amounts", ArrayType(StringType()), nullable=True),
])

decode_udf = udf(decode_flash_loan_udf, decoded_schema)


# ─── UDF: Get token symbol from address ───────────────────────────────────────
def get_symbol_udf(asset: str) -> str:
    if asset is None:
        return "UNKNOWN"
    return TOKEN_SYMBOLS.get(asset.lower(), "UNKNOWN")

symbol_udf = udf(get_symbol_udf, StringType())


# ─── UDF: Convert raw amount to human-readable using decimals ─────────────────
def to_human_udf(amount_str: str, symbol: str) -> float:
    if not amount_str or not symbol:
        return 0.0
    try:
        decimals = TOKEN_DECIMALS.get(symbol, 18)
        return float(int(amount_str)) / (10 ** decimals)
    except Exception:
        return 0.0

human_amount_udf = udf(to_human_udf, DoubleType())


# ─── UDF: Get historical USD price (Redis cache + CoinGecko + fallback) ───────
def get_price_usd_udf(symbol: str, timestamp_unix: float) -> float:
    """Fetch USD price at given timestamp.

    Runs per-row on workers. Each worker initializes its own Redis client
    on first call (lazy singleton via module-level cache).
    """
    if symbol in STABLECOINS:
        return 1.0

    if symbol not in ("WETH", "WBTC"):
        return 0.0

    date_str = "UNKNOWN"
    try:
        date_str = datetime.fromtimestamp(timestamp_unix, tz=timezone.utc).strftime("%d-%m-%Y")
    except Exception as e:
        print(f"[price_udf] ❌ Failed to parse timestamp {timestamp_unix}: {e}")
        print(f"[price_udf] ⚠️  FALLBACK used for {symbol} (bad timestamp)")
        return FALLBACK_PRICES.get(symbol, 0.0)

    # ── Step 1: Try Redis cache ────────────────────────────────────────────────
    redis_client = None
    try:
        import redis as redis_lib
        redis_client = redis_lib.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            password=REDIS_PASS or None,
            decode_responses=True, socket_connect_timeout=2
        )
        cache_key = f"hist_price:{symbol}:{date_str}"
        cached = redis_client.get(cache_key)
        if cached:
            print(f"[price_udf] ✅ Redis HIT  {symbol} @ {date_str} = {cached}")
            return float(cached)
        print(f"[price_udf] 🔍 Redis MISS {symbol} @ {date_str} → trying CoinGecko")
    except Exception as e:
        print(f"[price_udf] ⚠️  Redis ERROR for {symbol} @ {date_str}: {type(e).__name__}: {e}")

    # ── Step 2: Try CoinGecko ──────────────────────────────────────────────────
    try:
        import requests
        coin_id = {"WETH": "ethereum", "WBTC": "bitcoin"}[symbol]
        url = (
            f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
            f"?date={date_str}&localization=false"
        )
        resp = requests.get(url, timeout=5)

        if resp.ok:
            data = resp.json()
            market_data = data.get("market_data")
            if not market_data:
                print(f"[price_udf] ⚠️  CoinGecko OK but no market_data for {symbol} @ {date_str}")
                print(f"[price_udf] 📄 Response keys: {list(data.keys())}")
            else:
                price = float(market_data["current_price"]["usd"])
                print(f"[price_udf] ✅ CoinGecko  {symbol} @ {date_str} = {price}")
                # Write-back to Redis if available
                if redis_client:
                    try:
                        redis_client.set(cache_key, price, ex=86400)
                    except Exception as e:
                        print(f"[price_udf] ⚠️  Redis write-back failed: {e}")
                return price
        else:
            print(
                f"[price_udf] ❌ CoinGecko HTTP {resp.status_code} for {symbol} @ {date_str}"
                f" | body: {resp.text[:200]}"
            )
    except Exception as e:
        print(f"[price_udf] ❌ CoinGecko EXCEPTION for {symbol} @ {date_str}: {type(e).__name__}: {e}")

    # ── Step 3: Fallback ───────────────────────────────────────────────────────
    fallback = FALLBACK_PRICES.get(symbol, 0.0)
    print(f"[price_udf] ⚠️  FALLBACK used for {symbol} @ {date_str} = {fallback}")
    return fallback

price_udf = udf(get_price_usd_udf, DoubleType())


# ─── UDF: Score confidence based on USD value ─────────────────────────────────
def score_confidence_udf(amount_usd: float) -> str:
    if amount_usd is None:
        return "LOW"
    if amount_usd >= 1_000_000:
        return "HIGH"
    elif amount_usd >= 100_000:
        return "MEDIUM"
    return "LOW"

confidence_udf = udf(score_confidence_udf, StringType())


# ─── UDF: Get protocol name from selector ─────────────────────────────────────
def get_protocol_udf(selector: str) -> str:
    return PROTOCOL_BY_SELECTOR.get(selector, "Unknown")

protocol_udf = udf(get_protocol_udf, StringType())


# ─── foreachBatch sink: Write detections to MongoDB (DISTRIBUTED) ─────────────
def _write_partition_to_mongo(rows_iter, batch_id):
    """Executor-side writer. Each Spark partition opens its own MongoClient
    and bulk-writes its rows directly to Atlas. No collect() to driver.

    This is what makes the DB write step distributed: with N partitions and
    N executors, N writers run in parallel — instead of the driver being a
    serial bottleneck.
    """
    import certifi
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
    import time as _time

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
        tls=True,
        tlsCAFile=certifi.where(),
    )
    try:
        coll = client[MONGO_DB][MONGO_COLL]
        ops = []
        for r in rows_iter:
            doc = {
                "tx_hash":      r["tx_hash"],
                "protocol":     r["protocol"],
                "from":         r["sender"],
                "pool":         r["pool"],
                "token":        r["primary_symbol"],
                "amount_human": r["primary_amount_human"],
                "amount_usd":   round(r["amount_usd"] or 0.0, 2),
                "confidence":   r["confidence"],
                "timestamp":    r["tx_timestamp"],
                "batch_id":     batch_id,
                "processed_at": int(_time.time()),
            }
            ops.append(UpdateOne({"tx_hash": doc["tx_hash"]}, {"$set": doc}, upsert=True))

        if ops:
            try:
                # bulk_write = 1 round-trip per partition, not N round-trips
                coll.bulk_write(ops, ordered=False)
                # NOTE: this print appears in the EXECUTOR (worker) log, not the driver log.
                # That is the proof the write is distributed.
                print(f"[batch {batch_id}] partition wrote {len(ops)} detections to MongoDB")
            except PyMongoError as e:
                print(f"[batch {batch_id}] partition Mongo bulk_write failed: {e}")
    finally:
        client.close()


def write_to_mongo(batch_df, batch_id):
    """foreachBatch sink. Dispatches the actual writes to executors via
    foreachPartition instead of pulling the batch to the driver.
    """
    if batch_df.rdd.isEmpty():
        return
    batch_df.foreachPartition(lambda it: _write_partition_to_mongo(it, batch_id))


# ─── Main entry point ─────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  PySpark Flash Loan Streaming Job (P3)")
    print("=" * 60)
    print(f"  Kafka:       {KAFKA_BOOTSTRAP}")
    print(f"  Topic:       {KAFKA_TOPIC}")
    print(f"  MongoDB:     {MONGO_URI}")
    print(f"  Redis:       {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Checkpoint:  {CHECKPOINT_DIR}")
    print("=" * 60)

    # Build Spark session connected to the cluster
    spark = (SparkSession.builder
             .appName("FlashLoanDetector")
             .master(os.getenv("SPARK_MASTER", "spark://spark-master:7077"))
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.streaming.stopGracefullyOnShutdown", "true")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # Schema of incoming Kafka messages (matches listener.py output)
    tx_schema = StructType([
        StructField("tx_hash",   StringType()),
        StructField("from",      StringType()),
        StructField("to",        StringType()),
        StructField("input",     StringType()),
        StructField("value",     StringType()),
        StructField("gas",       StringType()),
        StructField("gas_price", StringType()),
        StructField("timestamp", DoubleType()),
        StructField("source",    StringType()),
    ])

    # Read stream from Kafka
    raw_stream = (spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                  .option("subscribe", KAFKA_TOPIC)
                  .option("startingOffsets", "earliest")
                  .option("failOnDataLoss", "false")
                  .load())

    # Parse JSON value
    parsed = (raw_stream
              .select(from_json(col("value").cast("string"), tx_schema).alias("tx"))
              .select("tx.*"))

    # Extract selector (first 10 chars of input)
    with_selector = parsed.withColumn("selector", col("input").substr(1, 10))

    # Filter only flash loan selectors
    flash_loans = with_selector.filter(
        col("selector").isin(list(PROTOCOL_BY_SELECTOR.keys()))
    )

    # Decode calldata via UDF
    decoded = (flash_loans
               .withColumn("decoded", decode_udf(col("input"), col("selector")))
               .filter(col("decoded").isNotNull())
               .filter(col("decoded.assets").isNotNull()))

    # Extract primary asset/amount, get symbol + human amount + USD price
    enriched = (decoded
                .withColumn("primary_asset",  col("decoded.assets")[0])
                .withColumn("primary_amount_raw", col("decoded.amounts")[0])
                .withColumn("primary_symbol", symbol_udf(col("primary_asset")))
                .withColumn("primary_amount_human",
                            human_amount_udf(col("primary_amount_raw"), col("primary_symbol")))
                .withColumn("price_usd", price_udf(col("primary_symbol"), col("timestamp")))
                .withColumn("amount_usd", col("primary_amount_human") * col("price_usd"))
                .withColumn("confidence", confidence_udf(col("amount_usd")))
                .withColumn("protocol", protocol_udf(col("selector"))))

    # Final shape for MongoDB
    detections = enriched.select(
        col("tx_hash"),
        col("from").alias("sender"),
        col("to").alias("pool"),
        col("protocol"),
        col("primary_symbol"),
        col("primary_amount_human"),
        col("amount_usd"),
        col("confidence"),
        col("timestamp").alias("tx_timestamp"),
    )

    # Print to console for debugging
    console_query = (detections.writeStream
                     .outputMode("append")
                     .format("console")
                     .option("truncate", "false")
                     .trigger(processingTime="5 seconds")
                     .start())

    # Write to MongoDB via foreachBatch → foreachPartition (distributed)
    mongo_query = (detections.writeStream
                   .outputMode("append")
                   .foreachBatch(write_to_mongo)
                   .option("checkpointLocation", CHECKPOINT_DIR)
                   .trigger(processingTime="10 seconds")
                   .start())

    print("[streaming] Job started. Awaiting messages...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()