# Spark UI & Job/Task/Partition — Walkthrough

## Mục tiêu cuộc hội thoại

Tìm hiểu cách xem PySpark UI để monitor workers, tasks, partitions. Hiểu cấu trúc Spark Job và cách thử nghiệm fault tolerance (kill worker).

---

## 1. Spark có 2 UI riêng biệt

### Spark Master UI (port 8080)

**Góc nhìn**: Quản trị viên cluster — "Tôi có bao nhiêu tài nguyên?"

| Thông tin | Mô tả |
|---|---|
| Danh sách Workers | 4 workers, trạng thái ALIVE/DEAD |
| CPU/RAM mỗi worker | 1 core, 1GB RAM mỗi worker |
| Running Applications | Các Spark app đang chạy |
| Completed Applications | Lịch sử các app trước |

→ **Không** cho biết chi tiết bên trong app đang làm gì.

### Spark Application UI (port 4040)

**Góc nhìn**: Developer — "App của tôi đang xử lý gì, ở đâu, nhanh hay chậm?"

| Thông tin | Chi tiết |
|---|---|
| **Jobs** | Mỗi micro-batch tạo ra nhiều jobs |
| **Stages** | Các bước trong job (read Kafka → transform → write MongoDB) |
| **Tasks** | ⭐ Mỗi task = 1 partition, thấy rõ task chạy trên executor/worker nào |
| **Executors** | Worker nào đang active, bao nhiêu task đã xử lý |
| **Streaming** | Tab riêng — input rate, processing time, batch duration |
| **SQL/DataFrame** | Query plan, physical plan |

### So sánh trực quan

```
Spark Master UI (8080)          Spark Application UI (4040)
─────────────────────           ──────────────────────────
"Sân bay"                       "Buồng lái máy bay"

• Có bao nhiêu máy bay?         • Máy bay này đang bay đâu?
• Đường băng nào trống?          • Tốc độ bao nhiêu?
• Máy bay nào đang cất cánh?    • Nhiên liệu còn bao nhiêu?
                                 • Hành khách ngồi ghế nào?
                                   (task → partition → worker)
```

---

## 2. Expose port 4040 cho Spark Application UI

### Thay đổi docker-compose.yml

```yaml
processing-job:
  build:
    context: ./processing
    dockerfile: Dockerfile
  container_name: processing-job
  restart: on-failure
  ports:
    - "4040:4040"    # ← THÊM DÒNG NÀY
  environment:
    - KAFKA_BOOTSTRAP=kafka-1:9092,kafka-2:9092,kafka-3:9092
    # ...
```

### Sau khi thêm port

- Spark Master UI: `http://localhost:8080` — tổng quan cluster/workers
- Spark Application UI: `http://localhost:4040` — chi tiết tasks, partitions, executors

### Lưu ý khi rebuild

```bash
# Cần remove orphan containers trước nếu có
docker compose down -v --remove-orphans
docker compose up -d --build
```

---

## 3. Kết quả Spark Master UI

Sau khi start lại toàn bộ hệ thống, Spark Master UI tại `localhost:8080` hiển thị:

- **4 Workers** — tất cả `ALIVE`
- Mỗi worker: 1 core, 1024 MiB RAM
- Tổng cluster: 4 cores, 4.0 GiB RAM
- Status: ALIVE

---

## 4. Giải thích Spark Job — Phân cấp khái niệm

### Cấu trúc phân cấp

```
Application (processing-job)         ← 1 app duy nhất, chạy liên tục
  └── Micro-batch (~10s mỗi lần)     ← Spark poll Kafka, có data mới → xử lý
        └── Jobs (nhiều job/batch)    ← Mỗi "action" tạo 1 job
              └── Stages              ← Các bước trong job
                    └── Tasks         ← 1 task = 1 partition
```

### "Job" là gì?

Mỗi micro-batch khi xử lý data tạo ra **nhiều jobs**, không phải 1. Mỗi job tương ứng với 1 **action** (hành động thực thi) trong pipeline:

| Action | Sinh ra Job |
|---|---|
| Đọc data từ Kafka | Job (read) |
| `foreachBatch` → xử lý DataFrame | Job (transform + UDF) |
| Ghi vào MongoDB | Job (write) |
| Commit offset checkpoint | Job (commit) |

→ 32 completed jobs cho 35 transactions vì mỗi micro-batch tạo nhiều jobs.

### 1 Task xử lý bao nhiêu TX?

Phụ thuộc vào **số message trong partition tại thời điểm micro-batch chạy**.

```
35 transactions → Kafka topic raw_txns (4 partitions)
    ├── Partition 0: ~9 tx  ← Hash(tx_hash) % 4 = 0
    ├── Partition 1: ~9 tx  ← Hash(tx_hash) % 4 = 1
    ├── Partition 2: ~9 tx  ← Hash(tx_hash) % 4 = 2
    └── Partition 3: ~8 tx  ← Hash(tx_hash) % 4 = 3
```

**Trong 1 micro-batch:**
- Spark tạo **4 tasks** (1 task/partition)
- Mỗi task đọc **tất cả messages mới** trong partition đó kể từ offset cuối
- Nếu tất cả 35 tx đến trong 1 micro-batch → mỗi task xử lý ~8-9 tx

**Thực tế** listener.py gửi tx có delay, nên:
- Micro-batch 1: có thể chỉ nhận 5 tx → 4 tasks, mỗi task xử lý 0-2 tx
- Micro-batch 2: nhận thêm 10 tx → 4 tasks, mỗi task 2-3 tx
- ...tiếp tục cho đến hết

### Giải thích kết quả Spark Jobs UI (32 completed)

| Job | Tasks | Giải thích |
|---|---|---|
| Job 31 | **3/3** | 3 partitions có data, 1 partition trống → chỉ 3 tasks |
| Job 30 | **1/1** | Commit offset — chỉ cần 1 task |
| Job 29 | **2/2** | Transform — 2 partitions có data mới |
| Job 28 | **2/2** | Tương tự |
| Job 27 | **1/1** | Commit hoặc metadata operation |

### Cách xem chi tiết task nào xử lý partition nào

Click vào **Job ID** → **Stages** → click vào Stage → bảng **Tasks**:
- **Locality**: worker nào chạy task
- **Input Size**: bao nhiêu data (tương ứng số tx)

---

## 5. Kill Worker — Fault Tolerance (chưa thử)

Spark có khả năng tự recovery khi 1 worker chết:

- Spark Master detect worker DEAD (timeout ~15s, cấu hình `SPARK_MASTER_OPTS: "-Dspark.worker.timeout=15"`)
- 3 workers còn lại chia nhau xử lý 4 partitions
- Không mất data vì Kafka giữ offset checkpoint

### Cách thử

```bash
# Xem danh sách worker containers
docker ps | findstr spark-worker

# Kill 1 worker cụ thể
docker stop flash-loan-attack-detection-spark-worker-1

# Quan sát Spark Master UI: worker chuyển từ ALIVE → DEAD
# 3 workers còn lại tiếp tục xử lý bình thường
```

---

## Files đã thay đổi

| File | Thay đổi |
|---|---|
| `docker-compose.yml` | Thêm `ports: - "4040:4040"` cho `processing-job` |
