# Mac Migration Notes (for AI assistants / next session)

Ngan switched from Windows to macOS (Apple Silicon, MacBook Pro). This file
tracks what's been done to get the Flash-Loan-Attack-Detection project
running on Mac, and what's still pending. Read `CLAUDE.md` first for full
project context — this file is only the OS-migration checklist.

## Environment

- **Project path:** `/Users/duongxuanngan/Documents/Flash-Loan-Attack-Detection`
  (NOT `/Users/duongxuanngan/Flash-Loan-Attack-Detection` — common typo,
  the project lives under `Documents/`)
- **Shell:** zsh
- **Python:** Homebrew python@3.11 installed at
  `/opt/homebrew/Cellar/python@3.11/3.11.15_3/bin/python3.11`
  - System Python is still 3.9.6 at `/usr/bin/python3` (Apple default, do not touch/remove)
  - `~/.zshrc` has `export PATH="/opt/homebrew/opt/python@3.11/bin:$PATH"`
  - Note: that Homebrew bin dir does NOT contain an unversioned `python3`
    symlink, only `python3.11`. Use `python3.11` explicitly when creating
    venvs from a fresh shell.
- **Venv:** root-level Python 3.11 venv lives at
  `/Users/duongxuanngan/Documents/ngan` (sibling of the project directory,
  NOT inside it — prompt shows `(ngan)` when activated). Confirmed via
  `uv`'s `VIRTUAL_ENV` warning during `backend/uv sync`. Left in place;
  no need to relocate.
  - Activate with whatever shell mechanism was used to create it (likely
    `source /Users/duongxuanngan/Documents/ngan/bin/activate`).
- **Backend venv:** separate from the above. `backend/.venv` is managed by
  `uv` (Python 3.13.14, per `backend/.python-version` and
  `backend/pyproject.toml`'s `requires-python = ">=3.13"`). Created via
  `uv sync` — do not activate the root `(ngan)` venv when running backend
  commands; use `uv run <cmd>` from `backend/`, which targets `.venv`
  automatically regardless of any active shell venv.

## Completed

- [x] Located project at correct path (see above)
- [x] Created Python 3.11 venv (`ngan`, at
      `/Users/duongxuanngan/Documents/ngan`), activated successfully
- [x] Fixed root `requirements.txt`:
  - Was UTF-16 with BOM (Windows artifact) — converted to UTF-8 via:
    `iconv -f UTF-16 -t UTF-8 requirements.txt | grep -v '^pywin32' > requirements_clean.txt && mv requirements_clean.txt requirements.txt`
  - Removed `pywin32==311` (Windows-only package, not needed on Mac)
  - `pip install -r requirements.txt` now succeeds cleanly (verified)
- [x] Clean up stray file `.!10323!requirements.txt` at project root
      — deleted (`rm '.!10323!requirements.txt'`, quoted for zsh `!`
      history expansion)
- [x] Installed `uv`, ran `cd backend && uv sync` — created `backend/.venv`
      with Python 3.13.14, 18 packages installed successfully
- [x] `cd frontend && npm install` — completed successfully, `npm run dev`
      starts Vite without errors
- [x] Recreated `.env` at project root with all required vars
      (`ALCHEMY_RPC_URL`, `ETH_WSS_PRIMARY`, `ETH_WSS_FALLBACK`,
      `REDIS_HOST`, `REDIS_PORT`, `REDIS_PASS`, `MONGODB_URI`,
      `MONGODB_FLASHLOAN_NAME`). Also contains an extra
      `MONGODB_TRANSACTIONS_NAME=defi_transactions` var not referenced in
      `CLAUDE.md` or `docker-compose.yml` — unclear if any code reads it;
      worth auditing.
  - **Note:** file was initially created inside `storage/` by mistake and
    moved to project root (`mv storage/.env .env`) — `.env` MUST be at
    project root for `docker-compose.yml`'s `env_file: .env` and
    `python-dotenv` calls (which resolve relative to launch directory) to
    pick it up.
- [x] Docker Desktop confirmed running. `docker compose up -d --build` —
      all 18 services pulled/built successfully on Apple Silicon, **no
      arch/platform issues** (including `provectuslabs/kafka-ui:latest`,
      which historically lacked arm64 — pulled fine here, no
      `platform: linux/amd64` pin needed).
- [x] `docker compose up -d --build` completed — see "Known issue" below
      for one post-startup fix that was required.

## Known issue (resolved this session)

- **`processing-job` crash-looped on first startup** with
  `UnknownTopicOrPartitionException: This server does not host this
  topic-partition.` This is expected on a cold start: the Spark job
  subscribes to `raw_txns`, but the topic doesn't exist until
  `ingestion/listener.py` (or its `ensure_topic()` call) runs at least
  once.
  - **Fix applied:** manually created the topic ahead of time:
    ```bash
    docker compose exec kafka-1 kafka-topics --create \
      --topic raw_txns --bootstrap-server kafka-1:9092 \
      --partitions 4 --replication-factor 3 --if-not-exists
    ```
  - Verified via `kafka-topics --describe`: 4 partitions, replication
    factor 3, all ISRs in sync (`1,2,3` / `2,3,1` / `3,1,2` / `1,3,2`),
    leaders distributed across brokers 1/2/3 as expected.
  - `processing-job` (has `restart: on-failure`) recovered on next
    restart once the topic existed.
  - **For future cold starts:** either run this `kafka-topics --create`
    command proactively right after `docker compose up`, or start
    `ingestion/listener.py` first (which calls `ensure_topic()` itself) —
    either order works, but `processing-job` will crash-loop harmlessly
    until one of them runs.

## Bug fixed this session (not Mac-specific)

- **`ingestion/listener.py` — `UnboundLocalError: wss_urls`**: a
  half-finished multi-URL-failover refactor left `log_mempool`'s
  parameter named `wss_url` (singular) while the function body, docstring,
  and call site (`urls`, a list) all referenced `wss_urls` (plural). This
  is a pure Python naming bug — would have crashed identically on Windows.
  It only surfaced now because this was the first run of `listener.py`
  since the refactor landed.
  - **Fix applied:** renamed the parameter to `wss_urls` to match the
    body/docstring/call-site.
  - **Remaining incomplete work (not blocking, but dead code):** the
    docstring describes failover behavior — "advances to the next URL in
    the list... switch providers when unhealthy" — but `url_idx` is
    initialized to `0` and never incremented, and `session_failed` is set
    but never read. With a single URL (mock server, no fallback
    configured) this doesn't matter; `wss_urls[0]` is always used. If/when
    a real `ETH_WSS_FALLBACK` is configured, the failover logic won't
    actually engage. Flagging for whoever owns `ingestion/listener.py`.

## End-to-end smoke test — PASSED

Full pipeline confirmed working on Mac:

```
mock_server.py → listener.py (decode/ABI/Kafka produce)
  → Kafka raw_txns (4 partitions, 3 brokers)
  → processing-job (Spark Structured Streaming)
  → MongoDB transactions collection (35 docs written)
  → backend (FastAPI/SSE) → frontend (localhost:5173)
```

- Listener decoded 29 flash loans across Aave V3 `flashLoan` /
  `flashLoanSimple` (Balancer V2 / Uniswap V3 also supported per ABI
  loading, not seen in this dataset sample).
- Verified via:
  ```bash
  docker compose exec processing-job python3 -c "
  from pymongo import MongoClient
  import os
  c = MongoClient(os.environ.get('MONGODB_URI'))
  db = c[os.environ.get('MONGODB_FLASHLOAN_NAME', 'flash_loan_detection')]
  print('count:', db.transactions.count_documents({}))
  print('sample:', db.transactions.find_one())
  "
  ```
  (Note: run this inside `processing-job`, not `spark-master` —
  `spark-master`'s image doesn't have `pymongo` installed; only
  `processing-job`'s Dockerfile installs it.)
- Backend (`uv run uvicorn Main:app --reload --port 8000`) and frontend
  (`npm run dev`, localhost:5173) both confirmed working by Ngan directly.

### Data-quality findings (Spark job logic — NOT Mac/migration issues)

Sample Mongo doc for the first transaction
(`8123d5707ab2...`, Aave V3 `flashLoan`, asset `0xdAC17F95...831ec7`
= USDT):
```json
{"amount_human": 0.1, "amount_usd": 0.0, "confidence": "LOW",
 "token": "UNKNOWN", "protocol": "Aave V3 flashLoan", "batch_id": 31}
```

Two issues for whoever owns `processing/streaming_job.py`:

1. **`amount_usd: 0.0`** — USD price lookup (Redis cache + CoinGecko
   fallback per the module docstring) returned 0 or failed silently.
2. **`token: 'UNKNOWN'`** — `0xdac17f958d2ee523a2206206994597c13d831ec7`
   IS present in `streaming_job.py`'s `TOKEN_SYMBOLS` dict (mapped to
   `"USDT"`), all-lowercase. If the address extracted from decoded
   calldata isn't lowercased before the dict lookup, mixed-case
   addresses (as returned by `decode_function_input`, e.g.
   `0xdAC17F958D2ee523a2206206994597C13D831ec7`) would miss the map
   entirely — worth checking for a missing `.lower()` on the lookup key.

These don't block the Mac migration — the wiring works end-to-end — but
are real bugs in the detection pipeline's output quality.

## Still pending

- [ ] Verify line endings: files edited on Windows may have CRLF.
      Consider `git config core.autocrlf input`
- [ ] (Optional) Audit whether `MONGODB_TRANSACTIONS_NAME` env var is read
      anywhere in `storage/mongo_store.py` or `backend/Main.py`; if not,
      either remove it from `.env` or document it properly in `CLAUDE.md`
- [ ] Fix `amount_usd: 0.0` and `token: 'UNKNOWN'` issues in
      `processing/streaming_job.py` (see findings above) — separate from
      Mac migration, but discovered during this session's smoke test

## Tooling notes

- Connected to project via Claude Desktop's filesystem MCP server,
  scoped to this project directory only.
- VS Code's integrated terminal may not pick up `~/.zshrc` PATH changes
  from before VS Code was launched — restart VS Code (Cmd+Q, reopen) if
  `python3.11` isn't found in its terminal.
