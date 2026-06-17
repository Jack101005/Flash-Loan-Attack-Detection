"""
kafka_producer.py — Kafka Producer for Flash Loan Detection (Stage 2)

Provides create_producer() and produce_message() used by ingestion/listener.py
to publish detected flash loan transactions to the 'raw_txns' topic.

Topic:     raw_txns
Partitions: 4  (key = tx_hash ensures same tx always → same partition)
Broker:    kafka:9092  (inside Docker network)
           localhost:9094 (from host machine)
"""

import json
import logging
from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

# Bootstrap addresses
# Use 'kafka:9092'     when running inside Docker (container → container)
# Use 'localhost:9094' when running listener.py directly on your host machine
BOOTSTRAP_SERVERS_DOCKER = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
BOOTSTRAP_SERVERS_HOST = "Thiens-MacBook-Pro.local:9094,Thiens-MacBook-Pro.local:9095,Thiens-MacBook-Pro.local:9096"

TOPIC_NAME   = "raw_txns"
NUM_PARTITIONS = 4


# Topic creation (idempotent)
def ensure_topic(bootstrap: str = BOOTSTRAP_SERVERS_HOST) -> None:
    """
    Create the raw_txns topic if it does not already exist.
    Safe to call multiple times — skips silently if topic is present.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topic = NewTopic(
        TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=3,
        config={"retention.ms": str(60 * 60 * 1000)},  # 1 hour
    )
    futures = admin.create_topics([topic])
    for t, future in futures.items():
        try:
            future.result()
            logger.info("[producer] Topic '%s' created (%d partitions).", t, NUM_PARTITIONS)
        except Exception as e:
            # TOPIC_ALREADY_EXISTS is expected on restart — not an error
            if "TOPIC_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                logger.debug("[producer] Topic '%s' already exists — OK.", t)
            else:
                logger.warning("[producer] Could not create topic '%s': %s", t, e)


# Producer factory
def create_producer(bootstrap: str = BOOTSTRAP_SERVERS_HOST) -> Producer:
    """
    Build and return a confluent_kafka.Producer.

    Returns None (with a warning) when Kafka is unreachable so that
    listener.py can continue running without Kafka (degrades gracefully).

    Args:
        bootstrap: Kafka bootstrap address.
                   Use BOOTSTRAP_SERVERS_HOST  for host-side listener.py
                   Use BOOTSTRAP_SERVERS_DOCKER for containerised ingestion service.
    """
    try:
        producer = Producer({
            "bootstrap.servers": bootstrap,
            # Exactly-once-ish guarantees against in-flight retries.
            # Broker dedups by (PID, partition, seq) so a retried produce
            # cannot land twice in the topic.
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2147483647,
            "max.in.flight.requests.per.connection": 5,
            "retry.backoff.ms": 500,
            "linger.ms": 5,
            "compression.type": "lz4",
        })
        # Probe the broker — raises KafkaException if unreachable
        producer.list_topics(timeout=5)
        logger.info("[producer] Connected to Kafka at %s", bootstrap)
        return producer
    except KafkaException as e:
        logger.warning(
            "[producer] Kafka unavailable at %s (%s). "
            "listener.py will run WITHOUT Kafka — detections printed only.",
            bootstrap, e,
        )
        return None
    except Exception as e:
        logger.warning("[producer] Unexpected error connecting to Kafka: %s", e)
        return None


# Delivery callback
def _on_delivery(err, msg):
    if err:
        logger.error(
            "[producer] Delivery FAILED  topic=%s  key=%s  err=%s",
            msg.topic(), msg.key().decode(), err,
        )
    else:
        logger.debug(
            "[producer] Delivered  topic=%s  partition=%d  offset=%d  key=%s",
            msg.topic(), msg.partition(), msg.offset(), msg.key().decode(),
        )


# Main publish function
def produce_message(
    producer: Producer,
    topic: str,
    key: str,
    value: dict,
) -> None:
    """
    Serialise value as JSON and produce to topic with the given key.

    Key = tx_hash → deterministic partition routing (same tx always goes to
    the same partition — important for exactly-once dedup downstream).

    Args:
        producer: confluent_kafka.Producer instance from create_producer().
        topic:    Kafka topic name (use TOPIC_NAME = 'raw_txns').
        key:      Partition key — use tx_hash string.
        value:    Python dict; must match the raw_txns message schema.

    Raises:
        BufferError: if the internal producer queue is full (rare — means
                     Kafka is very far behind; caller should back off).
    """
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        on_delivery=_on_delivery,
    )
    # poll(0) triggers delivery callbacks without blocking
    producer.poll(0)


# Graceful shutdown
def flush_producer(producer: Producer, timeout: float = 10.0) -> None:
    """
    Wait for all in-flight messages to be delivered.
    Call this before process exit or container shutdown.
    """
    if producer is None:
        return
    remaining = producer.flush(timeout)
    if remaining > 0:
        logger.warning(
            "[producer] %d message(s) NOT delivered after %.1fs flush.",
            remaining, timeout,
        )
    else:
        logger.info("[producer] All messages flushed successfully.")


# Quick smoke-test
if __name__ == "__main__":
    import time
    logging.basicConfig(level=logging.INFO)

    print("=== kafka_producer.py smoke test ===")
    print(f"Connecting to {BOOTSTRAP_SERVERS_HOST} ...")

    ensure_topic(BOOTSTRAP_SERVERS_HOST)
    p = create_producer(BOOTSTRAP_SERVERS_HOST)

    if p is None:
        print("Kafka not available. Start Docker first:")
        print("  cd infra && docker compose up -d")
    else:
        test_msg = {
            "tx_hash":   "0xTEST_HASH_000",
            "from":      "0xSenderAddress",
            "to":        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
            "input":     "0xab9c4b5d000000",
            "value":     "0",
            "gas":       "500000",
            "gas_price": "30000000000",
            "timestamp": time.time(),
            "source":    "smoke_test",
        }
        produce_message(p, TOPIC_NAME, test_msg["tx_hash"], test_msg)
        flush_producer(p)
        print(f"Sent 1 test message to topic '{TOPIC_NAME}'. Check with:")
        print(f"  docker exec kafka kafka-console-consumer "
              f"--bootstrap-server localhost:9092 "
              f"--topic {TOPIC_NAME} --from-beginning --max-messages 1")
