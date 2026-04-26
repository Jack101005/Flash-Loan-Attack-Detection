# Skill: DevOps & Infrastructure (Person 1)

## Key files
- `docker-compose.yml` — main orchestration (project root, currently empty)
- `storage/docker-compose.yml` — Redis + MongoDB containers (currently empty)
- `broker/kafka_producer.py` — Kafka producer module (currently empty)
- `broker/kafka_consumer.py` — Kafka consumer module (currently empty)
- `broker/consumer_groups.py` — Consumer group configuration (currently empty)

## Status: NOT STARTED

## Required services (docker-compose.yml must define all of these)
1. Zookeeper — required by Kafka
2. Kafka broker — message queue
3. Redis — in-memory price cache + dedup
4. MongoDB — persistent detection storage
5. Spark master — stream processing coordinator
6. Spark worker (×2) — parallel processing nodes
7. Ingestion container — runs ingestion/listener.py
8. Processing container — runs processing/stream_processor.py
9. Dashboard container — runs alerting/react_dashboard

## Kafka setup requirements (from PDF spec)
- Topic: `raw_txns`, 4 partitions, replication factor 1 (dev) / 3 (prod)
- Topic: `suspicious_txns` — detections from Stage 3
- Topic: `decode_failures` — failed decode rows
- Retention: 1 hour (`log.retention.hours=1`)
- Consumer group: `flash_loan_detectors`
- Message key: `tx_hash` (for consistent partition routing)

## docker-compose.yml template (Person 1 should implement this)
```yaml
version: "3.8"
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:latest
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:latest
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

  mongodb:
    image: mongo:6
    ports: ["27017:27017"]
    volumes: ["mongo_data:/data/db"]

  spark-master:
    image: bitnami/spark:3
    environment:
      SPARK_MODE: master

  spark-worker:
    image: bitnami/spark:3
    depends_on: [spark-master]
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077

volumes:
  mongo_data:
```

## How to start the full stack
```powershell
# Start everything
docker-compose up -d

# Create Kafka topics
docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw_txns \
  --partitions 4 \
  --replication-factor 1

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic suspicious_txns --partitions 4 --replication-factor 1

docker exec -it kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic decode_failures --partitions 1 --replication-factor 1

# Verify topics exist
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Stop everything
docker-compose down
```

## Fault tolerance demo (Definition of Done for Person 1)
```powershell
# Kill one Spark worker while transactions flow
docker-compose stop spark-worker

# System should recover within 10 seconds
# Dashboard should continue updating

# Add a third worker
docker-compose up --scale spark-worker=3 -d
```

## Check consumer lag
```powershell
docker exec -it kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group flash_loan_detectors
# LAG column should be near 0 under load
```

## Environment variables (.env file — never commit real values)
```
ALCHEMY_API_KEY=your_key_here
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
MONGODB_URI=mongodb://localhost:27017
REDIS_HOST=localhost
REDIS_PORT=6379
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```
