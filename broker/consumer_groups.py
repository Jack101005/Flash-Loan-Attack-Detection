"""
consumer_groups.py — Consumer Group Configuration (Stage 2)

Defines all consumer groups used in the Flash Loan Detection system
and provides helpers to inspect group lag and partition assignments.

Groups:
  flash_loan_detectors  — Person 3's PySpark Structured Streaming job
  alerting_group        — Person 5's Telegram bot / dashboard
  debug_consumer        — local development / smoke tests (not in prod)
"""

import logging
from dataclasses import dataclass, field
from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicPartition

logger = logging.getLogger(__name__)

# ── Bootstrap ──────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS_HOST   = "localhost:9094"
BOOTSTRAP_SERVERS_DOCKER = "kafka:9092"

# ── Group registry ─────────────────────────────────────────────────────────────
@dataclass
class GroupConfig:
    """Configuration for a single Kafka consumer group."""
    group_id: str
    description: str
    auto_offset_reset: str = "latest"
    # Additional Consumer kwargs merged at create_consumer() time
    extra_config: dict = field(default_factory=dict)


CONSUMER_GROUPS: dict[str, GroupConfig] = {
    # ── Stage 3 — PySpark Structured Streaming ─────────────────────────────────
    "flash_loan_detectors": GroupConfig(
        group_id="flash_loan_detectors",
        description="Person 3 — PySpark job that processes raw_txns",
        auto_offset_reset="earliest",   # replay from beginning on first start
        extra_config={
            "max.poll.interval.ms": 600_000,  # long interval for Spark batches
        },
    ),

    # ── Stage 6 — Alerting (Telegram / Dashboard) ──────────────────────────────
    "alerting_group": GroupConfig(
        group_id="alerting_group",
        description="Person 5 — Telegram bot and React dashboard consumer",
        auto_offset_reset="latest",     # only forward-looking; no replay needed
    ),

    # ── Local development / smoke tests ────────────────────────────────────────
    "debug_consumer": GroupConfig(
        group_id="debug_consumer",
        description="Development / debugging — safe to reset or delete",
        auto_offset_reset="earliest",
    ),
}

TOPIC_NAME = "raw_txns"


# ── Lag inspector ──────────────────────────────────────────────────────────────
def describe_group_lag(
    group_id: str,
    bootstrap: str = BOOTSTRAP_SERVERS_HOST,
) -> None:
    """
    Print the current consumer lag for every partition of raw_txns.

    Lag = (latest offset in partition) − (committed offset for this group).
    A lag of 0 means the consumer is fully caught up.

    Run this from the command line to check if Person 3's Spark job
    is keeping up with incoming flash loan transactions.

    Args:
        group_id: e.g. 'flash_loan_detectors'
        bootstrap: broker address
    """
    from confluent_kafka import Consumer

    # 1. Fetch committed offsets for the group
    admin = AdminClient({"bootstrap.servers": bootstrap})

    # We need a temporary consumer to call committed() and get_watermark_offsets()
    tmp_consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
    })

    # Build TopicPartition list for all 4 partitions
    partitions = [TopicPartition(TOPIC_NAME, p) for p in range(4)]

    try:
        committed = tmp_consumer.committed(partitions, timeout=10)
    except Exception as e:
        logger.error("[groups] Failed to fetch committed offsets: %s", e)
        tmp_consumer.close()
        return

    print(f"\n{'='*56}")
    print(f"  Consumer Lag — group: {group_id}")
    print(f"  Topic: {TOPIC_NAME}")
    print(f"{'='*56}")
    print(f"  {'Partition':<12} {'Committed':<14} {'Latest':<14} {'Lag'}")
    print(f"  {'-'*48}")

    total_lag = 0
    for tp in committed:
        try:
            lo, hi = tmp_consumer.get_watermark_offsets(tp, timeout=5)
        except Exception:
            hi = "?"
            lo = "?"

        committed_offset = tp.offset if tp.offset >= 0 else 0
        lag = (hi - committed_offset) if isinstance(hi, int) else "?"
        total_lag += lag if isinstance(lag, int) else 0

        print(f"  Partition {tp.partition:<3}   "
              f"{committed_offset:<14} {hi:<14} {lag}")

    print(f"  {'-'*48}")
    print(f"  {'Total lag':<27} {total_lag}")
    print(f"{'='*56}\n")

    tmp_consumer.close()


# ── Reset offsets (dev only) ───────────────────────────────────────────────────
def reset_group_to_earliest(
    group_id: str,
    bootstrap: str = BOOTSTRAP_SERVERS_HOST,
) -> None:
    """
    Reset a consumer group to the earliest available offset on all partitions.

    USE WITH CAUTION — this causes the group to reprocess all retained messages.
    Intended for local development / test runs only.

    Steps:
      1. Stop all consumers in the group first.
      2. Call this function.
      3. Restart consumers.
    """
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
    })
    consumer.assign([TopicPartition(TOPIC_NAME, p) for p in range(4)])

    partitions = consumer.assignment()
    consumer.seek_to_beginning(*partitions)   # type: ignore[arg-type]

    # Commit the earliest offset for each partition
    for tp in partitions:
        lo, _ = consumer.get_watermark_offsets(tp, timeout=5)
        tp.offset = lo

    consumer.commit(offsets=partitions, asynchronous=False)
    consumer.close()
    logger.info(
        "[groups] Reset group '%s' to earliest offset on all partitions.", group_id
    )
    print(f"[groups] Group '{group_id}' reset to earliest. "
          f"Restart your consumers to replay from the beginning.")


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)

    print("=== consumer_groups.py ===")
    print("Registered groups:")
    for name, cfg in CONSUMER_GROUPS.items():
        print(f"  [{name}]  {cfg.description}")

    print()
    # Show lag for each group
    for name in CONSUMER_GROUPS:
        try:
            describe_group_lag(name, BOOTSTRAP_SERVERS_HOST)
        except Exception as e:
            print(f"  [{name}] Could not fetch lag: {e}")
