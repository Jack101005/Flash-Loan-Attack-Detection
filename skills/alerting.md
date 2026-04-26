# Skill: Alerting & Dashboard (Person 5)

## Key files (currently empty)
- `alerting/telegram_bot.py` — Telegram Bot API integration
- `alerting/react_dashboard/` — Live detection dashboard
- `alerting/grafana_dashboard/` — Metrics panels

## Status: NOT STARTED

## Telegram bot requirements
- send_alert(detection_result) → formatted message within 500ms
- Rate limit: max 25 messages/second (token bucket)
- Retry: 5 times with exponential backoff on 4xx/5xx
- Message format: confidence badge, tx hash (Etherscan link),
  protocol, cycle path as arrow chain, estimated profit in USD

## telegram_bot.py interface
```python
def send_alert(detection_result: dict) -> bool:
    """
    Sends formatted Telegram message.
    detection_result must have: tx_hash, confidence, protocol,
    cycle_path, profit_estimate.
    Returns True on success.
    """
```

## Telegram message template
```
🚨 [HIGH] Flash Loan Detected

Protocol:  Aave V3
Tx:        0xabc...123 (https://etherscan.io/tx/0xabc...123)
Cycle:     USDT → WETH → USDT
Profit:    ~$4,100
Deviation: 50.3%
```

## React dashboard requirements
- Poll MongoDB every 5 seconds
- Show: live detections table, count by confidence, timeline chart
- Reads from MongoDB `detections` collection
- Depends on Person 4's mongo_store.py being set up first

## End-to-end test (Person 5's responsibility)
File: `tests/test_e2e.py`
1. Inject known flash loan fixture into Kafka `raw_txns` topic manually
2. Wait 3 seconds
3. Assert document appears in MongoDB with is_suspicious = true

## Dependencies
- Depends on Person 3: detection result object schema
- Depends on Person 4: MongoDB schema + mongo_store.py
- Must agree MongoDB schema with Person 4 before writing any read/write code
