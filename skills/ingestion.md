# Skill: Ingestion (Stage 1)

## What this stage does
Watches the Ethereum mempool via WebSocket, filters transactions by
contract address and function selector, decodes flash loan parameters,
and forwards structured JSON to Kafka (when ready).

## Key files
- `ingestion/listener.py` — main production listener
- `ingestion/mock_server.py` — local test server
- `ingestion/config.py` — WATCHLIST + SELECTORS
- `abis/*.json` — ABI files for decoding

## Status: COMPLETE
All Person 2 deliverables done. Tests pass. Kafka placeholder ready.

## How the two-pass filter works
1. Check `tx.to.lower()` against WATCHLIST → reject if not a known protocol
2. Check `tx.input[:10]` against SELECTORS → reject if not a flash loan selector
3. Only after both pass: ABI decode → build out_data → send to Kafka

## How to run
```powershell
python ingestion/mock_server.py          # Terminal 1
python ingestion/listener.py             # Terminal 2
python ingestion/mock_server.py --delay 0.5 --loop   # fast loop mode
python ingestion/listener.py --url wss://your-key    # real Alchemy
```

## Add a new protocol
1. Add address to WATCHLIST in `ingestion/config.py`
2. Add selector to SELECTORS in `ingestion/config.py`
3. Add ABI JSON to `abis/`
4. Add decoder entry in `listener.py` DECODERS dict

## Compute a selector
```python
from web3 import Web3
sig = "flashLoan(address,address[],uint256[],uint256[],address,bytes,uint16)"
print("0x" + Web3.keccak(text=sig)[:4].hex())
```

## Known decode failures
12 rows in test_data.csv fail to decode with "Invalid pointer in tuple at
location 32". These are real reverted transactions from attacker address
0xC6E1aF01... that used malformed ABI encoding. This is expected — the
listener correctly detects them (selector matched) but cannot decode params.
The try/except handles this gracefully. Not a bug to fix.

## Output schema (matches raw_txns Kafka spec)
```json
{
  "tx_hash": "0xabc...123",
  "from": "0xSender",
  "to": "0xContract",
  "input": "0xCalldata",
  "value": "0",
  "gas": "500000",
  "gas_price": "30000000000",
  "timestamp": 1718000000.123,
  "source": "ethereum_mainnet"
}
```
