# Skill: Kafka Integration (Stage 2)

## What this stage does
Kafka sits between Stage 1 (Ingestion) and Stage 3 (Processing) as a
durable message queue. listener.py produces to it; stream_processor.py
consumes from it.

## Status: ✅ COMPLETE (Person 2 — Ngan)
- Person 1 set up Kafka + full Docker stack in `infra/Docker-compose.yml`
- Person 2 implemented all broker modules and wired listener.py to Kafka
- Verified live: 35 flash loans detected and published to `raw_txns`
- Kafka running on `localhost:9094` (host) / `kafka:9092` (Docker internal)

## Key files
- `broker/kafka_producer.py` — ✅ DONE: create_producer(), produce_message(), flush_producer()
- `broker/kafka_consumer.py` — ✅ DONE: create_consumer(), consume_messages(), start_consumer_loop()
- `broker/consumer_groups.py` — ✅ DONE: group registry + describe_group_lag() + reset helper
- `ingestion/listener.py` — ✅ DONE: Kafka wired in, --no-kafka flag added

## Infrastructure (Person 1 — infra/Docker-compose.yml)
All services confirmed working. Start with:
```powershell
cd infra
docker compose up -d zookeeper kafka redis mongodb spark-master spark-worker-1 spark-worker-2
```
Note: `ingestion`, `price-feed`, `processing-job`, `dashboard` services need
their Dockerfiles before they can build. Skip them for now — run those
components directly from host instead.

## How to run the full Stage 1+2 pipeline
```powershell
# Step 1 — start infrastructure (Docker Desktop must be open first)
cd infra
docker compose up -d zookeeper kafka redis mongodb spark-master spark-worker-1 spark-worker-2

# Step 2 — start mock Ethereum node (Terminal 1)
cd ..
python ingestion/mock_server.py

# Step 3 — start listener with Kafka (Terminal 2)
python ingestion/listener.py

# Step 4 — run without Kafka (Docker not needed)
python ingestion/listener.py --no-kafka
```

## Bootstrap addresses
| Context | Address |
|---|---|
| Running listener.py on host machine | `localhost:9094` |
| Running inside a Docker container | `kafka:9092` |

## Consumer groups
| Group ID | Used by | Offset reset |
|---|---|---|
| `flash_loan_detectors` | Person 3 — PySpark job | earliest |
| `alerting_group` | Person 5 — Telegram / dashboard | latest |
| `debug_consumer` | Local dev / smoke tests | earliest |

## Message schema (raw_txns topic)
```json
{
  "tx_hash":    "0xabc...123",
  "from":       "0xSenderAddress",
  "to":         "0xContractAddress",
  "input":      "0xRawCalldata",
  "value":      "0",
  "gas":        "500000",
  "gas_price":  "30000000000",
  "timestamp":  1718000000.123,
  "source":     "ethereum_mainnet"
}
```
Key = `tx_hash` string — ensures same transaction always routes to same partition.
Topic: `raw_txns`, 4 partitions, retention = 1 hour.

## How Person 3 consumes from raw_txns
Option A — use broker/kafka_consumer.py directly (lightweight, no Spark):
```python
from broker.kafka_consumer import create_consumer, consume_messages
consumer = create_consumer(group_id="flash_loan_detectors")
for tx in consume_messages(consumer):
    print(tx["tx_hash"], tx["timestamp"])
```

Option B — PySpark Structured Streaming (production):
```python
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "raw_txns") \
    .option("startingOffsets", "earliest") \
    .load()
```

## Verify Kafka is working
```powershell
# Check messages arriving in raw_txns
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic raw_txns --from-beginning

# Check consumer lag for Person 3's group
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group flash_loan_detectors

# List all topics
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# Smoke test producer directly
python broker/kafka_producer.py

# Smoke test consumer directly
python broker/kafka_consumer.py
```

## Graceful degradation
If Kafka is unreachable, `listener.py` continues running in print-only mode.
Failed messages are written to `data/kafka_failures.log` for manual replay.
Use `--no-kafka` flag to skip Kafka entirely (useful when Docker is not running).

## Known behaviours (not bugs)
- Detections #1–#12 from `0xC6E1aF0...` show `Decode: FAILED` — these are
  real reverted attacker transactions with malformed ABI encoding. The selector
  still matches correctly so they ARE published to Kafka; decoding failure is
  expected and handled gracefully.
- `consumer_groups.py` lag inspector requires the consumer group to have
  committed at least one offset before meaningful lag data appears.
