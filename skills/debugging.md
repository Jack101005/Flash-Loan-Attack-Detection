# Skill: Debugging

## Before debugging anything — read the actual file first
Always read the relevant file using the filesystem tool before guessing.
Do not rely on memory of what a file contains.

## Common errors and what they mean

### BadResponseFormat: "jsonrpc" field must be present
**Where:** listener.py on startup
**Cause:** mock_server.py is sending messages that don't match JSON-RPC 2.0
format. The mock server must respond to `eth_subscribe` with
`{"jsonrpc":"2.0","id":N,"result":"0xSUBSCRIPTION_ID"}` before pushing
subscription notifications.
**Fix:** Use the current `mock_server.py` which handles the full handshake.
Old versions just pushed `{"result": "0x1111..."}` which is wrong.

### "Invalid pointer in tuple at location 32"
**Where:** listener.py ABI decoding
**Cause:** The transaction's calldata is malformed. The ABI encoding uses
pointer offsets to locate dynamic arrays, and one pointer points outside
the valid data range.
**This is NOT a bug.** 12 of the 60 test transactions have this — they
are real Ethereum transactions that were REVERTED on-chain by attacker
`0xC6E1aF0136776FB02dc7520AFe0CFf3484c6e0bB`. The try/except in
listener.py handles this correctly. Do not try to fix it.

### ProviderConnectionError: Could not connect to ws://localhost:8765
**Where:** listener.py on startup or reconnect
**Cause:** mock_server.py is not running, or it crashed.
**Fix:** Start mock_server.py first in a separate terminal.

### "Warning: abis/xxx.json not found, using inline ABI"
**Where:** listener.py startup
**Cause:** ABI files not found. listener.py checks two paths:
- `ingestion/abis/` (next to listener.py)
- `../abis/` (project root — correct location)
**Fix:** Make sure `abis/` directory is at project root with these files:
- `aave_v3_pool.json`
- `balancer_v2_vault.json`
- `uniswap_v3_pool.json`

### ModuleNotFoundError: No module named 'config'
**Where:** listener.py or test_ingestion.py
**Cause:** Python can't find `ingestion/config.py`
**Fix:** Run from project root, not from inside ingestion/:
```powershell
# Correct
cd D:\ThirdYear\second_semester\Flash-Loan-Attack-Detection
python ingestion/listener.py

# Wrong
cd ingestion
python listener.py
```

### Detection count is 0 when it should detect flash loans
**Checklist:**
1. Is mock_server.py running and showing "Pushed..." lines?
2. Does test_data.csv have rows with selector 0xab9c4b5d or 0x42b0b77c?
3. Are WATCHLIST addresses lowercase in config.py?
4. Is the `to` field in test_data.csv lowercase?
Run: `python export_detections.py` — if that detects correctly, the
problem is in the WebSocket path, not the filter logic.

### Reconnect not triggering after Ctrl+C on mock server
**Cause:** Ctrl+C sends a clean WebSocket close frame. The listener
receives it as a normal close and calls the reconnect loop.
**Expected behavior:** You should see:
`[listener] Server closed connection at ... Reconnecting in 1s (attempt 1/5)...`
If you don't see this, check that `log_mempool()` has the outer
`while True` loop with `try/except Exception`.

### Test 1 fails with "Took 10.0s, must be < 5s"
**Cause:** The MockServer._push() method in test_ingestion.py doesn't
call `await websocket.close()` after sending all transactions. Without
closing, the listener's `process_subscriptions()` loop blocks waiting
for more messages until the 10s timeout.
**Fix:** Add `await websocket.close()` at end of `_push()` after the
`await asyncio.sleep(1.0)` call.

## Debugging workflow

1. Read the file that's causing the error
2. Find the exact line number from the traceback
3. Check what the variable contains at that line
4. Look up the error in this file
5. Apply the fix

## Useful one-liners for debugging

```python
# Check what selectors are in test_data.csv
import csv
with open("data/test_data.csv") as f:
    for row in csv.DictReader(f):
        print(row["input"][:10], row["to"][:20])

# Verify an ABI file loads correctly
import json
from web3 import Web3
with open("abis/aave_v3_pool.json") as f:
    abi = json.load(f)
c = Web3().eth.contract(abi=abi)
print([fn["name"] for fn in abi if fn.get("type") == "function"])

# Test a single selector computation
from web3 import Web3
sig = "flashLoan(address,address[],uint256[],uint256[],address,bytes,uint16)"
print("0x" + Web3.keccak(text=sig)[:4].hex())
# Should print: 0xab9c4b5d
```
