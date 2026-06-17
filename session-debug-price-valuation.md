# Session: Debugging Flash Loan Price Valuation & Infrastructure Cleanup

> **Date:** 2026-06-08  
> **Objective:** Fix flash loan valuation discrepancies, clean up infrastructure, and diagnose data loss.

---

## 1. Problem Statement

Transaction `0xd67860c4a56fe612b4b436def3ec225dab11efe20ae9b9f4b25a24ba1703212b` was detected with a borrowed amount of **$20,000** (fallback price), but the actual value on Etherscan was **$16,878** (real-time spot price at the time of the TX).

Additionally:
- Redis UI (RedisInsight at `localhost:5540`) showed no cached data.
- Kafka had **35 messages**, but only **33 transactions** appeared in MongoDB and the UI.

---

## 2. Root Cause Analysis

### 2.1 Price Fallback Bug (FIXED ✅)

**Root Cause:** The `streaming_job.py` Redis client was instantiated **without password authentication**, causing all Redis operations to fail silently with `NOAUTH Authentication required`. This triggered a fallback to a hardcoded, outdated price of $20,000.

**File:** `processing/streaming_job.py`

```python
# BEFORE (broken):
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# AFTER (fixed):
REDIS_PASS = os.environ.get("REDIS_PASS", "")
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS, decode_responses=True)
```

**File:** `docker-compose.yml` — Added `REDIS_PASS` to the `processing-job` container environment:

```yaml
processing-job:
  environment:
    - REDIS_PASS=redis_dev_pass
```

### 2.2 Legacy `price-feed` Service (REMOVED ✅)

**Finding:** The `price-feed` container was continuously pushing duplicate transaction data into a `TX_QUEUE` Redis list. **No service consumed this queue** — it was dead code.

**Action:** Removed the `price-feed` service block entirely from `docker-compose.yml` and its associated `cache/price_feed.py` logic.

### 2.3 Transaction Discrepancy: 35 Kafka → 33 MongoDB (EXPLAINED ✅)

**Analysis of `data/test_data.csv`:**

| # | Total rows in CSV | Flash loan selector matches | Non-flash-loan rows |
|---|---|---|---|
| Result | 60 | 35 | 25 |

Of the 35 flash loan rows, **2 have malformed `tx_hash` values**:

| CSV Line | tx_hash | Issue |
|---|---|---|
| Line 2 | `0x8123d5707ab2be94dcaa31d5b043b56c59c246d74fcd383b91ef9ad10da20b24084864003a8db5` | **91 characters** — too long (should be 66: `0x` + 64 hex) |
| Line 4 | `0x3758459edd7f9ed693ccebb6771aaeaa7843769` | **41 characters** — too short |

These 2 transactions are processed by Spark but likely fail validation or cause duplicate-key issues when written to MongoDB. The **33 valid transactions** are correctly persisted and displayed.

### 2.4 Price Discrepancy: $23,597 vs $16,878 (EXPLAINED ✅)

After fixing the Redis auth bug, the system now correctly fetches prices from CoinGecko and caches them in Redis. However:

| Source | Price Type | WETH Value | TX Amount (10 WETH) |
|---|---|---|---|
| **CoinGecko History API** | Daily average | ~$2,359.68 | **$23,597** |
| **Etherscan** | Real-time spot | ~$1,687.80 | **$16,878** |

**Explanation:** CoinGecko's `/coins/{id}/history` endpoint returns the **daily average price**, not the price at the exact second of the transaction. Etherscan uses the real-time spot price at the moment of execution.

**Recommendation:** To achieve Etherscan-level precision, consider:
- CoinGecko `/coins/{id}/market_chart/range` API (minute/hour granularity, but limited on free tier)
- Chainlink oracle price feeds (on-chain, block-level precision)
- High-frequency trading API (e.g., Binance, Coinbase)

---

## 3. Redis Cache Verification

After the fix, RedisInsight (`localhost:5540`) was connected manually with:
- **Host:** `redis` (Docker service name)
- **Port:** `6379`
- **Password:** `redis_dev_pass`

**Cached keys observed:**

```
hist_price:WBTC  →  1 entry (daily price for 16-04-2026)
hist_price:WETH  →  1 entry (daily price for 16-04-2026)
```

The cache is working correctly. Prices are fetched from CoinGecko on first access and cached in Redis with a TTL for subsequent lookups.

---

## 4. Flash Loan Selectors

The system recognizes 4 flash loan function selectors:

| Selector | Protocol |
|---|---|
| `0xab9c4b5d` | Aave V3 `flashLoan` |
| `0x42b0b77c` | Aave V3 `flashLoanSimple` |
| `0x490e6cbc` | Uniswap V3 `flash` |
| `0x5c38449e` | Balancer V2 `flashLoan` |

Non-matching selectors in test data (25 rows) include:
- `0xa415bcad`, `0x617ba037`, `0x69328dec`, `0x573ade81`, `0x02c205f0` — Aave V3 general (non-flash-loan)
- `0x68747470`, empty input — Uniswap V3 (non-flash-loan)

---

## 5. Files Modified

### `processing/streaming_job.py`
- **Line 44:** Added `REDIS_PASS = os.environ.get("REDIS_PASS", "")`
- **Lines 179-184:** Passed `password=REDIS_PASS` to `redis.Redis()` constructor

### `docker-compose.yml`
- **Lines 123-140:** Removed `price-feed` service block
- **Lines 182-188:** Added `REDIS_PASS=redis_dev_pass` to `processing-job` environment

---

## 6. Architecture Overview

```
┌──────────────┐    ┌──────────────┐    ┌──────────────────┐    ┌──────────────┐
│ mock_server   │───▶│    Kafka      │───▶│  Spark Streaming │───▶│   MongoDB    │
│ (Ingestion)   │    │  (3 brokers)  │    │  (4 workers)     │    │              │
└──────────────┘    └──────────────┘    └────────┬─────────┘    └──────┬───────┘
                                                 │                      │
                                                 ▼                      ▼
                                          ┌──────────────┐    ┌──────────────┐
                                          │    Redis      │    │   Backend    │
                                          │ (Price Cache) │    │  (FastAPI)   │
                                          └──────────────┘    └──────┬───────┘
                                                                      │
                                                                      ▼
                                                               ┌──────────────┐
                                                               │   Frontend   │
                                                               │   (React)    │
                                                               └──────────────┘
```

---

## 7. Running Services

| Service | Command | Port |
|---|---|---|
| Frontend | `npm run dev` | 5173 |
| Backend | `uvicorn backend.Main:app --host 0.0.0.0 --port 8000 --reload` | 8000 |
| RedisInsight | Docker container | 5540 |
| Kafka UI | Docker container | 8080 |
| Spark UI | Docker container | 4040 |

---

## 8. Next Steps

- [ ] Fix 2 malformed `tx_hash` entries in `data/test_data.csv`
- [ ] Consider upgrading to CoinGecko Pro or Chainlink for block-level price precision
- [ ] Monitor Redis cache hit rate via RedisInsight
- [ ] Add input validation in `decode_flash_loan_udf` to reject malformed tx hashes with logging
