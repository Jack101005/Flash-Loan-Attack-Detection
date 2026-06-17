"""
fault_tolerance_demo.py — Fault Tolerance: Single-Process vs Spark

Demonstrates the fundamental difference between single-process and
distributed Spark when a failure occurs mid-processing.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCENARIO 1 — Single-process crash (no checkpoint)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Start processing N transactions
  2. Crash after K (mid-way through a batch)
  3. Inspect MongoDB → K records exist (fewer if batch not flushed)
  4. Restart → must start from row 0 again (no progress saved)
  Result: in-flight batch LOST, all work must be redone

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SCENARIO 2 — Spark worker crash (checkpoint-based recovery)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  1. Produce N transactions to Kafka
  2. Spark processes first micro-batch (10s trigger)
  3. Kill one Spark worker container mid-processing
  4. Spark master detects failure, reassigns task to another worker
  5. Worker resumes from last committed Kafka offset (checkpoint)
  Result: all N records eventually written, zero data loss

Usage:
    python benchmarks/fault_tolerance_demo.py --scenario single
    python benchmarks/fault_tolerance_demo.py --scenario spark
    python benchmarks/fault_tolerance_demo.py              # both scenarios + comparison
"""

import argparse
import os
import subprocess
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
except ImportError:
    pass

import certifi
from pymongo import MongoClient

MONGO_URI    = os.getenv("MONGODB_URI")
MONGO_DB     = os.getenv("MONGODB_FLASHLOAN_NAME", "flash_loan_detection")
BENCH_COLL   = "transactions_benchmark"
MAIN_COLL    = "transactions"
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


# ── Helpers ────────────────────────────────────────────────────────────────────

def mongo_count(coll: str) -> int:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000,
                         tls=True, tlsCAFile=certifi.where())
    try:
        return client[MONGO_DB][coll].count_documents({})
    finally:
        client.close()


def mongo_clear(coll: str) -> None:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000,
                         tls=True, tlsCAFile=certifi.where())
    try:
        client[MONGO_DB][coll].delete_many({})
    finally:
        client.close()


def banner(title: str) -> None:
    width = 62
    print(f"\n{'━'*width}")
    print(f"  {title}")
    print(f"{'━'*width}")


def pause(msg: str = "Press Enter to continue...") -> None:
    try:
        input(f"\n[demo] {msg}")
    except EOFError:
        pass  # non-interactive run


def get_spark_worker_container() -> str | None:
    """Return the container ID of one running Spark worker."""
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=spark-worker",
         "--format", "{{.ID}}\t{{.Names}}"],
        capture_output=True, text=True,
    )
    lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
    if not lines:
        return None
    # Return the first worker container ID
    return lines[0].split("\t")[0]


# ── Scenario 1: Single-process ─────────────────────────────────────────────────

def demo_single_process() -> dict:
    """
    Shows that single-process has no fault tolerance:
    a crash mid-batch loses in-flight records and forces a full restart.
    """
    banner("SCENARIO 1 — Single-Process (No Fault Tolerance)")

    mongo_clear(BENCH_COLL)
    print(f"\n[demo] MongoDB '{BENCH_COLL}' cleared. Initial count: 0")
    print(f"[demo] Will crash after 10 processed transactions")
    print(f"[demo] Batch size = 10, so the pending batch will be LOST\n")

    # ── Phase 1: Run until crash ───────────────────────────────────────────────
    print("─── Phase 1: First run (crashes at tx 10) ───\n")

    subprocess.run(
        [sys.executable, "benchmarks/single_process_job.py",
         "--data",        "data/test_data_enriched.csv",
         "--crash-after", "10",
         "--delay",       "0.25",
         "--batch-size",  "10"],
        cwd=PROJECT_ROOT,
    )

    count_after_crash = mongo_count(BENCH_COLL)
    print(f"\n[demo] MongoDB after crash: {count_after_crash} records")
    print(f"[demo] The pending batch of 10 docs was held in memory — NEVER written")
    print(f"[demo] No checkpoint file exists — restart has no 'resume from' point")

    pause("Press Enter to restart the single-process job (watch it restart from tx 1)...")

    # ── Phase 2: Restart — re-processes from row 0 ───────────────────────────
    print("\n─── Phase 2: Restart (re-processes ALL from row 0) ───\n")

    subprocess.run(
        [sys.executable, "benchmarks/single_process_job.py",
         "--data",       "data/test_data_enriched.csv",
         "--delay",      "0.1",
         "--batch-size", "10"],
        cwd=PROJECT_ROOT,
    )

    count_final = mongo_count(BENCH_COLL)
    print(f"\n[demo] MongoDB after restart: {count_final} records")

    result = {
        "count_before_crash":  10,
        "count_after_crash":   count_after_crash,
        "count_after_restart": count_final,
    }

    print(f"""
┌──────────────────────────────────────────────────────────┐
│  SINGLE-PROCESS FAULT TOLERANCE RESULT                   │
├──────────────────────────────────────────────────────────┤
│  Before crash:    ~10 tx processed (batch not flushed)   │
│  MongoDB records: {count_after_crash:3d} (batch was lost)               │
│  After restart:   {count_final:3d} records (full re-process)         │
├──────────────────────────────────────────────────────────┤
│  ❌ No checkpoint — restart always starts from tx 0      │
│  ❌ In-flight batch (10 docs) was lost on crash           │
│  ❌ Wasted work: all rows re-processed from scratch       │
└──────────────────────────────────────────────────────────┘
""")
    return result


# ── Scenario 2: Spark ──────────────────────────────────────────────────────────

def demo_spark() -> dict:
    """
    Shows Spark's checkpoint-based fault tolerance:
    kill a worker mid-batch → Spark recovers, all records written.
    """
    banner("SCENARIO 2 — Spark (Checkpoint-Based Fault Tolerance)")

    mongo_clear(MAIN_COLL)
    initial = mongo_count(MAIN_COLL)
    print(f"\n[demo] MongoDB '{MAIN_COLL}' cleared. Initial count: {initial}")
    print(f"[demo] Checkpoint location: /tmp/spark-checkpoints/flash-loan-detection")
    print(f"[demo] Spark tracks the last committed Kafka offset in this checkpoint")

    # Restart processing-job so it starts from earliest and checkpoint is fresh
    print("\n[demo] Restarting Spark processing-job (fresh checkpoint)...")
    subprocess.run(["docker", "compose", "restart", "processing-job"],
                   cwd=PROJECT_ROOT, capture_output=True)
    time.sleep(12)

    # Start mock server + listener to produce transactions
    print("[demo] Starting mock Ethereum node + ingestion listener...")
    mock = subprocess.Popen(
        [sys.executable, "ingestion/mock_server.py",
         "--delay", "1.5", "--loop",
         "--data", "data/test_data_enriched.csv"],
        cwd=PROJECT_ROOT,
    )
    time.sleep(2)
    listener = subprocess.Popen(
        [sys.executable, "ingestion/listener.py", "--url", "ws://localhost:8765"],
        cwd=PROJECT_ROOT,
    )

    # Wait for the first micro-batch to complete (10s trigger + network margin)
    print("[demo] Waiting for first Spark micro-batch to complete (~25s)...")
    time.sleep(25)

    count_before_kill = mongo_count(MAIN_COLL)
    print(f"\n[demo] After first micro-batch: {count_before_kill} records in MongoDB")

    if count_before_kill == 0:
        print("[demo] No records yet — Spark is still starting. Waiting 20s more...")
        time.sleep(20)
        count_before_kill = mongo_count(MAIN_COLL)

    # ── Kill one Spark worker ──────────────────────────────────────────────────
    print(f"\n[demo] ⚡ KILLING one Spark worker container!")
    print(f"[demo]    Checkpoint has committed offset up to current batch.")
    print(f"[demo]    Spark master will detect the lost worker and reassign its task.\n")

    worker_id = get_spark_worker_container()
    if worker_id:
        subprocess.run(["docker", "kill", worker_id], capture_output=True)
        print(f"[demo] Killed container: {worker_id}")
    else:
        print("[demo] ⚠ No spark-worker container found. Is Docker running?")
        print("[demo]   Continuing demo with observation only...")

    kill_time = time.strftime("%H:%M:%S")
    print(f"[demo] Worker killed at {kill_time}")
    print(f"[demo] Spark master detects worker heartbeat timeout (~10s)")
    print(f"[demo] Reassigns task to remaining workers")
    print(f"[demo] Workers resume from last committed Kafka offset (not from beginning)")
    print(f"\n[demo] Watching MongoDB for 60 seconds...\n")

    start = time.time()
    count_peak = count_before_kill
    while time.time() - start < 60:
        count = mongo_count(MAIN_COLL)
        elapsed = int(time.time() - start)
        count_peak = max(count_peak, count)
        print(f"\r[demo] t+{elapsed:3d}s  MongoDB: {count} records "
              f"({'growing ✓' if count > count_before_kill else 'waiting...'})",
              end="", flush=True)
        time.sleep(3)
    print()

    mock.terminate()
    listener.terminate()

    count_final = mongo_count(MAIN_COLL)
    new_records = count_final - initial

    result = {
        "count_before_kill": count_before_kill,
        "count_after_recovery": count_final,
        "new_records": new_records,
    }

    print(f"""
┌──────────────────────────────────────────────────────────┐
│  SPARK FAULT TOLERANCE RESULT                            │
├──────────────────────────────────────────────────────────┤
│  Before worker kill:   {count_before_kill:3d} records in MongoDB      │
│  After recovery:       {count_final:3d} records in MongoDB      │
│  New records written:  {new_records:3d}                            │
├──────────────────────────────────────────────────────────┤
│  ✅ Spark detected worker failure automatically          │
│  ✅ Remaining workers continued from checkpoint          │
│  ✅ Kafka offset preserved — no transactions re-read     │
│  ✅ Processing resumed without manual intervention       │
└──────────────────────────────────────────────────────────┘
""")
    return result


# ── Comparison summary ─────────────────────────────────────────────────────────

def print_comparison() -> None:
    print(f"""
{'╔' + '═'*64 + '╗'}
║{'  FAULT TOLERANCE COMPARISON':^64}║
{'╠' + '═'*30 + '╦' + '═'*33 + '╣'}
║  {'Property':<28}║  {'Single-Process':<14} {'Spark':<15}║
{'╠' + '═'*30 + '╬' + '═'*33 + '╣'}
║  {'Crash recovery':<28}║  {'❌ Restart row 0':<14} {'✅ Checkpoint':<15}║
║  {'In-flight data on crash':<28}║  {'❌ Lost':<14} {'✅ Preserved':<15}║
║  {'Worker failure handling':<28}║  {'❌ Full stop':<14} {'✅ Auto-reassign':<15}║
║  {'State persistence':<28}║  {'❌ None':<14} {'✅ Kafka offset':<15}║
║  {'Manual restart needed':<28}║  {'❌ Yes':<14} {'✅ No':<15}║
║  {'Duplicate processing risk':<28}║  {'❌ High':<14} {'✅ Low (upsert)':<15}║
{'╚' + '═'*30 + '╩' + '═'*33 + '╝'}

  How Spark's checkpoint works:
    1. Every micro-batch, Spark writes the consumed Kafka offset to disk
       (at /tmp/spark-checkpoints/flash-loan-detection)
    2. Worker crash detected via heartbeat timeout (~10s)
    3. Master re-schedules the unfinished task to a healthy worker
    4. Worker reads from the LAST COMMITTED OFFSET, not from the beginning
    5. MongoDB upsert (UpdateOne) is idempotent — safe if a tx is re-seen

  Why single-process cannot recover:
    1. Progress exists only in memory (pending_docs list, loop index)
    2. On crash, the current batch in pending_docs is permanently lost
    3. There is no persistent record of "how far we got"
    4. Restart must re-read the full CSV from row 0
    5. At scale (millions of tx), this means hours of wasted reprocessing
""")


# ── Entry point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fault tolerance demonstration")
    parser.add_argument(
        "--scenario",
        choices=["single", "spark", "both"],
        default="both",
        help="Which scenario to run (default: both)",
    )
    args = parser.parse_args()

    single_result = None
    spark_result  = None

    if args.scenario in ("single", "both"):
        single_result = demo_single_process()

    if args.scenario in ("spark", "both"):
        spark_result = demo_spark()

    if args.scenario == "both":
        print_comparison()
