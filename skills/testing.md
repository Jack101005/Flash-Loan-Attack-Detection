# Skill: Testing

## Test files
- `tests/test_ingestion.py` — Stage 1 integration tests (3 tests, all pass)
- `tests/test_e2e.py` — End-to-end test (Person 5's responsibility, not yet written)

## How to run all tests
```powershell
cd D:\ThirdYear\second_semester\Flash-Loan-Attack-Detection
python tests/test_ingestion.py
```
Expected output:
```
[OK]  Flash loan detection: PASSED
[OK]  Deduplication: PASSED
[OK]  Reconnection: PASSED
All tests passed!
```

## What each test verifies

### Test 1 — Flash loan detection
- Creates 10 synthetic flash loan transactions + 5 noise transactions
- Runs the listener's detection logic against an embedded mock server
- Asserts: all 10 flash loans detected, 0 false positives, completes < 5s
- Failure means: two-pass filter is broken, ABI mismatch, or timing issue

### Test 2 — Deduplication
- Sends same tx_hash twice to the listener
- Asserts: only 1 detection (dedup set working)
- Failure means: seen_hashes set is not working

### Test 3 — Reconnection
- Phase 1: detect a flash loan from server 1
- Stops server 1
- Phase 2: start server 2 on same port, detect a different flash loan
- Asserts: at least 1 detection in each phase
- Failure means: reconnect loop is broken

## Common test failures and fixes

### "Took 10.0s, must be < 5s"
The mock server didn't close the WebSocket after sending all transactions.
Fix: check that `_push()` in `MockServer` calls `await websocket.close()`
after the loop.

### "Missed X flash loans"
Possible causes:
1. WATCHLIST addresses use wrong case — must be lowercase in config.py
2. Mock server push delay too fast — increase `delay` in MockServer
3. Listener not subscribing before first push — check the 0.2s sleep in
   MockServer.handle() after eth_subscribe response

### "Expected 1, got 2" (dedup test)
The `seen` set in `run_listener_capture()` is missing or not being checked.
Check that `h_norm = normalize_hash(h)` and `if h_norm in seen: continue`
are both present in the capture function.

### "Phase 2 should detect >= 1" (reconnection test)
Port 18765 may be in use from a previous test run.
Fix: wait a moment and retry, or change PORT in the test.

## Adding new tests
When adding a new test function:
1. Name it `async def test_something()`
2. Print a header: `print("\\n=== TEST N: description ===")`
3. Return `True` on pass, raise `AssertionError` on fail
4. Add to the tests list in `run_all_tests()`

## Running with pytest (optional)
```powershell
pip install pytest pytest-asyncio
pytest tests/ -v
```
Note: test_ingestion.py uses `asyncio.run()` directly, so it works
without pytest. Pytest requires `@pytest.mark.asyncio` decorator if used.
