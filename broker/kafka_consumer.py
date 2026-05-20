"""
kafka_consumer.py — Kafka Consumer for Flash Loan Detection (Stage 2)

Provides create_consumer() and consume_messages() for Person 3's
PySpark Structured Streaming job to read from the 'raw_txns' topic.

Also exposes a standalone poll loop (start_consumer_loop) for
lightweight non-Spark consumers (e.g. quick debug / alerting).

Consumer group: flash_loan_detectors
Topic:          raw_txns
"""

import json
import logging
from typing import Callable, Iterator
from confluent_kafka import Consumer, KafkaError, KafkaException, Message

logger = logging.getLogger(__name__)

# ── Bootstrap addresses ────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS_HOST   = "localhost:9094"
BOOTSTRAP_SERVERS_DOCKER = "kafka:9092"

TOPIC_NAME     = "raw_txns"
CONSUMER_GROUP = "flash_loan_detectors"


# ── Consumer factory ───────────────────────────────────────────────────────────
def create_consumer(
    group_id: str = CONSUMER_GROUP,
    bootstrap: str = BOOTSTRAP_SERVERS_HOST,
    auto_offset_reset: str = "earliest",
) -> Consumer:
    """
    Build and return a confluent_kafka.Consumer subscribed to raw_txns.

    Args:
        group_id:          Kafka consumer group. All workers in the same
                           group share the topic's 4 partitions automatically.
        bootstrap:         Broker address (host or Docker).
        auto_offset_reset: 'earliest' → replay from start (good for dev/testing).
                           'latest'   → only new messages (production default).

    Returns:
        Subscribed Consumer instance ready to poll().
    """
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": auto_offset_reset,
        "enable.auto.commit": False,   # manual commit for exactly-once processing
        "max.poll.interval.ms": 300_000,
        "session.timeout.ms": 30_000,
        "heartbeat.interval.ms": 10_000,
    })
    consumer.subscribe([TOPIC_NAME])
    logger.info(
        "[consumer] Subscribed to topic '%s' (group='%s')", TOPIC_NAME, group_id
    )
    return consumer


# ── Message iterator ───────────────────────────────────────────────────────────
def consume_messages(
    consumer: Consumer,
    poll_timeout: float = 1.0,
) -> Iterator[dict]:
    """
    Yield decoded message dicts from raw_txns indefinitely.

    Commits offsets only after each message is yielded (manual commit).
    Skips and logs messages that fail JSON decoding.

    Usage (Person 3 — PySpark is preferred, but this works for testing):

        consumer = create_consumer()
        for tx in consume_messages(consumer):
            print(tx["tx_hash"], tx["timestamp"])

    Yields:
        dict with keys: tx_hash, from, to, input, value, gas,
                        gas_price, timestamp, source
    """
    try:
        while True:
            msg: Message = consumer.poll(poll_timeout)

            if msg is None:
                continue  # timeout — no messages yet

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Normal end of partition — keep polling
                    logger.debug(
                        "[consumer] Reached end of partition %d @ offset %d",
                        msg.partition(), msg.offset(),
                    )
                else:
                    raise KafkaException(msg.error())
                continue

            # Decode JSON payload
            try:
                tx_data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(
                    "[consumer] Malformed message at offset %d: %s",
                    msg.offset(), e,
                )
                consumer.commit(message=msg, asynchronous=False)
                continue

            # Manual commit AFTER successful decode
            consumer.commit(message=msg, asynchronous=False)
            yield tx_data

    except KeyboardInterrupt:
        logger.info("[consumer] Interrupted by user.")
    finally:
        consumer.close()
        logger.info("[consumer] Consumer closed.")


# ── Standalone poll loop (for debugging / lightweight consumers) ───────────────
def start_consumer_loop(
    callback: Callable[[dict], None],
    group_id: str = CONSUMER_GROUP,
    bootstrap: str = BOOTSTRAP_SERVERS_HOST,
    auto_offset_reset: str = "latest",
) -> None:
    """
    Run a blocking consumer loop, calling callback(tx_dict) for each message.

    Useful for:
      - Person 5's Telegram alerting service
      - Quick debug scripts
      - Unit tests that don't need PySpark

    Args:
        callback: Function that receives a single decoded tx dict.
        group_id: Consumer group — use a DIFFERENT group than flash_loan_detectors
                  if you don't want this consumer to affect Person 3's offsets.
        bootstrap: Broker address.
        auto_offset_reset: 'latest' for live monitoring, 'earliest' for replay.

    Example:
        def my_handler(tx):
            print(f"New flash loan: {tx['tx_hash']}")

        start_consumer_loop(my_handler, group_id="alerting_group")
    """
    consumer = create_consumer(
        group_id=group_id,
        bootstrap=bootstrap,
        auto_offset_reset=auto_offset_reset,
    )
    logger.info("[consumer] Starting loop (Ctrl+C to stop)...")
    for tx in consume_messages(consumer):
        try:
            callback(tx)
        except Exception as e:
            logger.error("[consumer] Callback error for tx %s: %s",
                         tx.get("tx_hash", "?"), e)


# ── Quick smoke-test ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("=== kafka_consumer.py smoke test ===")
    print(f"Consuming from topic '{TOPIC_NAME}' (group='{CONSUMER_GROUP}')")
    print("Waiting for messages... (Ctrl+C to stop)\n")

    def print_tx(tx: dict) -> None:
        print(f"  tx_hash  : {tx.get('tx_hash')}")
        print(f"  from     : {tx.get('from')}")
        print(f"  to       : {tx.get('to')}")
        print(f"  selector : {tx.get('input', '')[:10]}")
        print(f"  timestamp: {tx.get('timestamp')}")
        print()

    start_consumer_loop(
        callback=print_tx,
        group_id="debug_consumer",
        auto_offset_reset="earliest",
    )
