# MongoDB Data Source Migration — Complete Guide

## 📦 What's Included

This package enables migrating the Flash Loan Detection system from **CSV/WebSocket sources** to **MongoDB JSON sources**.

### Files Created

#### 📄 Documentation (Read in Order)

| File | Purpose | Audience |
|------|---------|----------|
| **`GETTING_STARTED_MONGODB.txt`** | Visual quick-start guide with ASCII art | Everyone (start here!) |
| **`MONGODB_MIGRATION_SUMMARY.md`** | High-level overview & success criteria | Project leads |
| **`MONGODB_DATA_SOURCE.md`** | Practical examples & CLI usage | Developers |
| **`MONGODB_MIGRATION_CODE.md`** | Role-specific code changes (Person 1-5) | Each team member |
| **`DATA_SOURCE_MIGRATION.md`** | Architecture options & deep dive | Architects |

#### 🐍 Python Modules (Ready to Use)

| File | Purpose | Usage |
|------|---------|-------|
| **`processing/mongodb_reader.py`** | Query MongoDB with Python generators | In processing pipeline |
| **`ingestion/mongodb_listener.py`** | Poll MongoDB continuously | Replace WebSocket listener |

---

## 🚀 Quick Start (5 Minutes)

```bash
# 1. Verify MongoDB works
python3 -c "from storage.mongo_store import ping; print('✓ OK' if ping() else '✗ Check .env')"

# 2. Check data in MongoDB
python3 processing/mongodb_reader.py --command stats

# 3. View recent detections
python3 processing/mongodb_reader.py --command recent --limit 5

# 4. Start polling MongoDB (optional)
python3 ingestion/mongodb_listener.py --no-kafka
```

---

## 📖 Reading Guide

### If You're...

**🎯 The Project Lead**
1. Read: `GETTING_STARTED_MONGODB.txt` (2 min)
2. Read: `MONGODB_MIGRATION_SUMMARY.md` (5 min)
3. Share links with team

**👨‍💻 A Developer (Person 1-5)**
1. Read: `GETTING_STARTED_MONGODB.txt` (2 min)
2. Read: `MONGODB_DATA_SOURCE.md` (10 min)
3. Find your section in: `MONGODB_MIGRATION_CODE.md` (5 min)
4. Copy-paste code snippets into your files

**🏛️ An Architect**
1. Read: `MONGODB_MIGRATION_SUMMARY.md` (5 min)
2. Read: `DATA_SOURCE_MIGRATION.md` (15 min)
3. Decide on Option A, B, or C
4. Share decision with team

---

## 🎯 By Role

### Person 1: DevOps/Ingestion
- **New Tool**: `ingestion/mongodb_listener.py`
- **Action**: Replace WebSocket listener with MongoDB listener
- **Read**: `MONGODB_MIGRATION_CODE.md` → "Person 1 section"
- **Command**: `python3 ingestion/mongodb_listener.py`

### Person 2: Kafka/Broker
- **New Tool**: `processing/mongodb_reader.py`
- **Action**: Add MongoDB as data source to broker
- **Read**: `MONGODB_MIGRATION_CODE.md` → "Person 2 section"
- **Import**: `from processing.mongodb_reader import read_transactions_from_mongo`

### Person 3: Processing/Detection
- **New Tool**: `processing/mongodb_reader.py`
- **Action**: Replace Kafka consumer with MongoDB reader
- **Read**: `MONGODB_MIGRATION_CODE.md` → "Person 3 section"
- **Function**: `read_recent_detections(limit=1000)`

### Person 4: Storage/Database
- **New Tool**: Already exists (`storage/mongo_store.py`)
- **Action**: Ensure indexes are created
- **Read**: `MONGODB_MIGRATION_CODE.md` → "Person 4 section"
- **Call**: `from storage.mongo_store import init_indexes; init_indexes()`

### Person 5: Alerting/Dashboard
- **New Tool**: `processing/mongodb_reader.py`
- **Action**: Replace Kafka consumer with MongoDB poller
- **Read**: `MONGODB_MIGRATION_CODE.md` → "Person 5 section"
- **Loop**: `while True: recent = read_recent_detections(limit=50)`

---

## 🔄 Three Migration Paths

### Option A: Keep Kafka (Recommended for Smooth Transition)
```
MongoDB → mongodb_listener → Kafka → Processing
```
- Minimal changes to existing pipeline
- Can run WebSocket listener alongside for redundancy
- Familiar to team (already using Kafka)

### Option B: Skip Kafka (Recommended for New Systems)
```
MongoDB → mongodb_reader → Python Processing directly
```
- Simpler architecture
- No Kafka needed
- Better for batch processing
- Faster development

### Option C: Keep Everything as Is (No Migration)
```
Ethereum → listener.py → Kafka → Processing
```
- Original setup, fully working
- Can integrate MongoDB reader in parallel
- Low risk, proven architecture

**→ Choose your option in `DATA_SOURCE_MIGRATION.md`**

---

## ✅ Testing

### Test MongoDB Connection
```bash
python3 -c "from storage.mongo_store import ping; print('Connected!' if ping() else 'Not connected')"
```

### Test Reader Module
```bash
# Show statistics
python3 processing/mongodb_reader.py --command stats

# Show recent detections
python3 processing/mongodb_reader.py --command recent --limit 10

# Filter by risk level
python3 processing/mongodb_reader.py --command recent --risk-level CRITICAL

# Count detections
python3 processing/mongodb_reader.py --command count
```

### Test Listener Module
```bash
# Poll MongoDB (print-only mode)
python3 ingestion/mongodb_listener.py --no-kafka

# Poll with custom settings
python3 ingestion/mongodb_listener.py --poll-interval 2 --lookback 30
```

### Syntax Check
```bash
python3 -m py_compile processing/mongodb_reader.py ingestion/mongodb_listener.py
echo "✓ All modules compile successfully"
```

---

## 📊 Architecture Comparison

| Aspect | CSV | WebSocket | MongoDB |
|--------|-----|-----------|---------|
| **Source** | Static files | Live stream | Persistent DB |
| **Real-time** | No | Yes | Polling |
| **Historical** | Single snapshot | None | Full history |
| **Replay** | Difficult | Impossible | Easy |
| **Scalability** | Limited | Single node | Distributed |
| **Reliability** | File-based | Network-dependent | Database guarantees |

---

## 🔧 Implementation Checklist

- [ ] Read `GETTING_STARTED_MONGODB.txt`
- [ ] Run MongoDB connection test
- [ ] Run `processing/mongodb_reader.py --command stats`
- [ ] Identify your role (Person 1-5)
- [ ] Read your role section in `MONGODB_MIGRATION_CODE.md`
- [ ] Copy-paste code snippets into your files
- [ ] Test with provided CLI commands
- [ ] Update Docker Compose (if using containers)
- [ ] Document any team changes

---

## 🐛 Troubleshooting

### MongoDB Not Reachable
```bash
# Check environment variables
grep MONGODB .env

# Expected output:
# MONGODB_URI=mongodb+srv://...
# MONGODB_FLASHLOAN_NAME=flash_loan_db

# If not set, add them:
echo "MONGODB_URI=<your-uri>" >> .env
echo "MONGODB_FLASHLOAN_NAME=flash_loan_db" >> .env
```

### No Data in MongoDB
```bash
python3 processing/mongodb_reader.py --command count

# If count is 0:
# Option 1: Run listener to collect new data
# Option 2: Import existing test data
# Option 3: Check if collection name matches MONGODB_FLASHLOAN_NAME
```

### Import Errors
```bash
# Ensure project root is in Python path
cd /path/to/Flash-Loan-Attack-Detection
python3 processing/mongodb_reader.py --command stats

# Verify sys.path handling in both modules (already included!)
```

---

## 📚 File Organization

```
Flash-Loan-Attack-Detection/
├── 📄 GETTING_STARTED_MONGODB.txt        ← START HERE
├── 📄 README_MONGODB_MIGRATION.md        ← THIS FILE
├── 📄 MONGODB_MIGRATION_SUMMARY.md       ← Overview
├── 📄 MONGODB_DATA_SOURCE.md             ← Practical guide
├── 📄 MONGODB_MIGRATION_CODE.md          ← Code changes
├── 📄 DATA_SOURCE_MIGRATION.md           ← Architecture
│
├── ingestion/
│   ├── listener.py                       (WebSocket - keep as backup)
│   ├── mongodb_listener.py               ✨ NEW (MongoDB poller)
│   └── ...
│
├── processing/
│   ├── stream_processor.py               (update to use mongodb_reader)
│   ├── mongodb_reader.py                 ✨ NEW (MongoDB queries)
│   └── ...
│
├── storage/
│   └── mongo_store.py                    (existing - no changes needed)
│
└── ...
```

---

## 🎓 Learning Resources

### Quick References
- CLI help: `python3 ingestion/mongodb_listener.py --help`
- CLI help: `python3 processing/mongodb_reader.py --help`
- Module docstrings: Detailed comments in each file

### Documentation
- See reading guide above (by role)
- Each `.md` file is self-contained with examples

### Testing
- Use provided CLI commands to test each component
- Each module includes example usage

---

## ⚡ Performance Tips

### Query Optimization
- Use filters to reduce data: `read_transactions_from_mongo(filters={"risk_level": "HIGH"})`
- Batch processing: `batch_size=1000`
- Create indexes on frequently queried fields

### Polling Strategy
- **Real-time**: `--poll-interval 1 --lookback 5` (high CPU)
- **Balanced**: `--poll-interval 5 --lookback 10` (default)
- **Batch**: `--poll-interval 30 --lookback 60` (low CPU)

### Kafka Integration
- If using Option A, Kafka can be optional or mandatory
- Test with `--no-kafka` first, then enable Kafka connection

---

## 🔄 Rollback Plan

If you need to revert:

1. **Keep original files** — They still work unchanged
   - `ingestion/listener.py` (WebSocket)
   - `broker/kafka_consumer.py` (Kafka consumer)
   - `export_detections.py` (CSV exporter)

2. **Revert component**
   - Stop MongoDB listener: just restart `ingestion/listener.py`
   - Remove mongodb_reader imports from processing
   - Use Kafka consumer again

3. **No data loss** — MongoDB data remains available

---

## 📞 Support & Q&A

### Common Questions

**Q: Do I need to rewrite my entire pipeline?**
A: No! Minimal changes. See your role section in `MONGODB_MIGRATION_CODE.md` (3-5 lines typically).

**Q: Can I run old and new in parallel?**
A: Yes! Both WebSocket listener and MongoDB listener can coexist.

**Q: Do I need Kafka?**
A: No, it's optional. Use Option B to skip Kafka entirely.

**Q: How do I handle historical data?**
A: MongoDB stores everything. Use `read_recent_detections()` or set large `--lookback`.

**Q: What if MongoDB goes down?**
A: Built-in error handling. Falls back gracefully, logs errors, can retry.

---

## ✨ What You Get

✅ **Two production-ready Python modules**
✅ **5 comprehensive documentation files**
✅ **Copy-paste code snippets for each role**
✅ **CLI tools for testing & debugging**
✅ **Zero breaking changes to existing code**
✅ **Backward compatible with WebSocket/Kafka setup**
✅ **Ready for Docker Compose integration**

---

## 🎯 Success Criteria

You'll know it's working when:

1. ✓ `processing/mongodb_reader.py --command stats` shows your data
2. ✓ `ingestion/mongodb_listener.py --no-kafka` polls MongoDB
3. ✓ Each team member integrates their role's code changes
4. ✓ Pipeline runs with MongoDB as primary data source

---

## 🚀 Next Steps

1. **Read** `GETTING_STARTED_MONGODB.txt` (2 min)
2. **Test** MongoDB connection (1 min)
3. **Find** your role in `MONGODB_MIGRATION_CODE.md` (2 min)
4. **Copy** code snippets into your files (10 min)
5. **Test** with provided examples (5 min)
6. **Integrate** into your pipeline (varies by role)
7. **Deploy** using Docker Compose or standalone (varies)

---

**Total estimated time: 30 minutes for most roles**

Start with: `cat GETTING_STARTED_MONGODB.txt`

Happy migrating! 🎉
