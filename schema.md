# Project Schemas

## 1. Kafka: raw_txns topic
- **Key**: 'tx_hash' (String)
- **Value**: JSON Object
```json
{
    "tx_hash": "0x....",
    "from": "0x.....",
    "to": "0x......",
    "input": "0x...",
    "value": "0",
    "gas": "500000",
    "gas_price": "30000000000",
    "timestamp": 1718000000.123,
    "source": "ethereum_mainnet"
}
```
## 2. MongoDB: detections collection
```json
{
  "tx_hash": "0xabc...123",
  "is_suspicious": true,
  "confidence": "HIGH",
  "protocol": "aave_v3",
  "cycle_path": ["0xUSDT", "0xWETH", "0xUSDT"],
  "profit_estimate": 4100.0,
  "price_deviation": 0.503,
  "graph_snapshot": {},
  "timestamp": 1718000000.456
}
```