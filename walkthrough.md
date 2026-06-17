# Kafka & PySpark Infrastructure — Walkthrough

## Mục tiêu cuộc hội thoại

Review toàn bộ Kafka setup trong project Flash Loan Attack Detection, hiểu cách hoạt động, phát hiện vấn đề, và scale lên 3 Kafka brokers + 4 Spark workers.

---

## 1. Review Codebase Kafka

### Kafka Setup ban đầu (docker-compose.yml)

| Thuộc tính | Giá trị |
|---|---|
| Broker | 1 duy nhất (`confluentinc/cp-kafka:7.6.1`) |
| Zookeeper | 1 (`confluentinc/cp-zookeeper:7.6.1`) |
| Internal listener | `PLAINTEXT://kafka:9092` (container ↔ container) |
| External listener | `PLAINTEXT_HOST://localhost:9094` (host → container) |
| Replication factor | 1 |
| Log retention | 1 giờ / 512MB |
| Kafka UI | `provectuslabs/kafka-ui` tại `http://localhost:8082` |

### Ý nghĩa folder `broker/`

| File | Vai trò |
|---|---|
| `kafka_producer.py` | Module gửi message — `listener.py` import để đẩy flash loan vào topic `raw_txns`. Gồm: `ensure_topic()`, `create_producer()`, `produce_message()`, `flush_producer()` |
| `kafka_consumer.py` | Module nhận message cho debug/lightweight consumers. PySpark **không dùng** file này (Spark có reader riêng). Gồm: `create_consumer()`, `consume_messages()`, `start_consumer_loop()` |
| `consumer_groups.py` | Registry 3 consumer groups + tools check lag và reset offset. Groups: `flash_loan_detectors`, `alerting_group`, `debug_consumer` |

### Data Flow: listener.py → Kafka

```
Mock Server (ws://localhost:8765)
    → listener.py subscribe newPendingTransactions
    → Two-pass filter: contract address + function selector
    → Flash loan detected → build JSON dict
    → produce_message(producer, "raw_txns", tx_hash, out_data)
    → Kafka topic raw_txns (key = tx_hash)
    → PySpark Structured Streaming đọc và xử lý
    → Ghi vào MongoDB
```

**Message schema gửi vào `raw_txns`:**
```json
{
  "tx_hash": "0xabc123...",
  "from": "0xSenderAddress",
  "to": "0x87870bca...",
  "input": "0xab9c4b5d...",
  "value": "0",
  "gas": "500000",
  "gas_price": "30000000000",
  "timestamp": 1717849200.0,
  "source": "ethereum_mainnet"
}
```

### Trạng thái live lúc kiểm tra

- 1 topic (`raw_txns`), **1 partition** (mismatch với code khai báo 4), 70 messages, 0 consumer groups active

---

## 2. Vấn đề phát hiện: Topic chỉ có 1 partition

### Nguyên nhân gốc: Race condition

```
1. docker compose up → Kafka broker khởi động (auto.create.topics.enable = true)
2. processing-job (PySpark) subscribe "raw_txns"
   → Kafka auto-create topic với DEFAULT 1 partition ← VẤN ĐỀ
3. listener.py chạy SAU CÙNG → ensure_topic(NUM_PARTITIONS=4)
   → TOPIC_ALREADY_EXISTS → skip → vẫn 1 partition
```

Kafka mặc định `num.partitions = 1` khi auto-create. Code `ensure_topic()` chỉ tạo mới, không alter topic đã tồn tại.

### Fix vĩnh viễn đã áp dụng

Thêm `KAFKA_NUM_PARTITIONS: 4` vào docker-compose → mọi topic auto-create sẽ có 4 partitions.

---

## 3. Topic có tự mất không?

| Cái gì | Tự mất? | Điều kiện |
|---|---|---|
| **Topic** (`raw_txns`) | ❌ Không | Tồn tại cho đến khi xóa thủ công hoặc `docker compose down -v` |
| **Messages** trong topic | ✅ Có | Sau 1 giờ hoặc khi partition đạt 512MB |
| **Consumer offsets** | ✅ Có | Sau 7 ngày không active |
| **Toàn bộ data** | ✅ Mất | Khi `docker compose down` (Kafka không mount volume) |

---

## 4. Scale lên 3 Brokers + 4 Workers

### Thay đổi docker-compose.yml

**Trước → Sau:**

| Component | Trước | Sau |
|---|---|---|
| Kafka brokers | 1 (`kafka`) | 3 (`kafka-1`, `kafka-2`, `kafka-3`) |
| Host ports | 9094 | 9094, 9095, 9096 |
| Replication factor | 1 | 3 |
| Spark workers | 1 | 4 (`deploy.replicas: 4`) |
| Mỗi worker | 1 core, 1GB RAM | 1 core, 1GB RAM (tổng 4 cores, 4GB) |

### Thay đổi broker/*.py (3 files)

```python
# Trước
BOOTSTRAP_SERVERS_DOCKER = "kafka:9092"
BOOTSTRAP_SERVERS_HOST   = "localhost:9094"

# Sau
BOOTSTRAP_SERVERS_DOCKER = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
BOOTSTRAP_SERVERS_HOST   = "localhost:9094,localhost:9095,localhost:9096"
```

`replication_factor` trong `ensure_topic()`: 1 → 3

### streaming_job.py — Không cần sửa

Đọc từ env var `KAFKA_BOOTSTRAP` trong docker-compose, đã update thành `kafka-1:9092,kafka-2:9092,kafka-3:9092`.

---

## 5. Kiến thức Kafka & Spark đã thảo luận

### Partition + Broker: Leader/Follower phân bổ

Với 4 partitions + 3 brokers + replication factor 3:

```
         Broker 1         Broker 2         Broker 3
P0       Leader ★         Follower         Follower
P1       Follower         Leader ★         Follower
P2       Follower         Follower         Leader ★
P3       Leader ★         Follower         Follower
```

- Producer (listener.py) chỉ ghi vào **Leader**
- Follower tự replicate từ Leader
- Nếu 1 broker chết → Kafka elect follower lên làm Leader
- PySpark chỉ đọc từ **Leader** replicas

### Spark Worker ≠ Spark Task

```
4 Workers (containers) ← tài nguyên vật lý
4 Tasks (per batch)    ← đơn vị xử lý (1 task = 1 partition)
```

- Worker **không** xử lý cố định 1 partition
- Mỗi micro-batch (~10s), Spark Master phân lại task cho worker
- Trong cùng batch: 1 partition = 1 task = 1 worker (không bị 2 worker xử lý cùng lúc)
- Thứ tự trong partition luôn đảm bảo
- Worker rảnh **không** giúp worker bận (1 task không chia nhỏ hơn)

### Docker compose up khi chưa có message

- PySpark subscribe topic → Kafka auto-create với 4 partitions (nhờ `KAFKA_NUM_PARTITIONS=4`)
- Spark ngồi chờ, poll liên tục, **không crash** (`failOnDataLoss: false`)
- Khi listener.py produce message → Spark nhận ngay trong micro-batch tiếp theo

### Thứ tự khởi động đúng

```
1. docker compose up -d --build    ← Kafka + Spark + Redis
2. python ingestion/mock_server.py ← WebSocket mock
3. uvicorn backend                 ← API server
4. npm run dev                     ← Frontend
5. python ingestion/listener.py    ← Producer (chạy sau cùng)
```

---

## Files đã thay đổi

| File | Thay đổi |
|---|---|
| `docker-compose.yml` | 3 Kafka brokers, 4 Spark workers, replication 3, `KAFKA_NUM_PARTITIONS: 4` |
| `broker/kafka_producer.py` | Bootstrap servers 3 brokers, `replication_factor=3` |
| `broker/kafka_consumer.py` | Bootstrap servers 3 brokers |
| `broker/consumer_groups.py` | Bootstrap servers 3 brokers |

---

## Cập nhật: Idempotent Producer + RPC Failover

### Idempotent Kafka Producer (`broker/kafka_producer.py`)

Bật `enable.idempotence=True` trong `create_producer()`:

```python
"enable.idempotence": True,
"acks": "all",
"retries": 2147483647,
"max.in.flight.requests.per.connection": 5,
```

**Tại sao:** Khi listener retry produce do mất ACK (network blip, broker leader election), broker sẽ nhận ra duplicate bằng (PID, partition, seq) và tự discard — không bao giờ có 2 record cho cùng 1 tx trong `raw_txns`.

**Giới hạn:** Chỉ hoạt động trong cùng 1 producer lifetime. Nếu listener khởi động lại (PID mới), cần downstream dedup bằng `tx_hash` ở Spark — đã có sẵn.

### RPC Failover (`ingestion/listener.py` + `.env`)

Thêm 2 env vars:
```
ETH_WSS_PRIMARY=wss://eth-mainnet.g.alchemy.com/v2/<KEY>
ETH_WSS_FALLBACK=wss://ethereum-rpc.publicnode.com
```

`log_mempool()` nhận danh sách URL thay vì 1 string. Logic rotate URL:
- Session nhận được ≥1 tx → giữ nguyên URL hiện tại khi retry
- Session thất bại trước khi nhận tx nào → chuyển sang URL tiếp theo
- Chỉ rotate khi provider thực sự unhealthy, không phải khi có flash loan thưa
