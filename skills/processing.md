# Skill: Processing (Stages 3, 4, 5)

## What these stages do
Stage 3: Spark reads raw_txns from Kafka, decodes calldata, builds graphs
Stage 4: Graph Construction — NetworkX DiGraph per transaction
Stage 5: Cycle Detection — DFS to find arbitrage cycles, confidence scoring

## Key files (all currently empty — Person 3's work)
- `processing/stream_processor.py` — PySpark Structured Streaming job
- `processing/graph_builder.py` — NetworkX graph construction
- `processing/cycle_detector.py` — DFS cycle detection + confidence scoring
- `processing/price_monitor.py` — Redis price deviation check

## Status: NOT STARTED

## Input (from Kafka topic raw_txns)
Raw transaction JSON — see `skills/kafka_integration.md` for schema.
Person 3 also has `data/detected_flash_loans.csv` as a CSV alternative
while Kafka is not yet set up.

## Output schema (suspicious_txns Kafka topic / what Person 5 receives)
```json
{
  "tx_hash": "0xabc...123",
  "is_suspicious": true,
  "confidence": "HIGH",
  "cycle_path": ["0xUSDT", "0xWETH", "0xUSDT"],
  "profit_estimate": 4100.0,
  "price_deviation": 0.503,
  "protocol": "aave_v3",
  "timestamp": 1718000000.456,
  "graph_snapshot": {"nodes": [...], "edges": [...]}
}
```

## Confidence scoring rules
- HIGH = cycle detected AND price deviation > threshold (e.g. 5%)
- MEDIUM = cycle detected only (no Redis price data available)
- LOW = no cycle but large flash loan with no obvious arbitrage path

## stream_processor.py implementation guide
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import json

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("FlashLoanDetector") \
    .getOrCreate()

# 2. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "raw_txns") \
    .option("startingOffsets", "latest") \
    .load()

# 3. Deserialize JSON
# 4. For each row: decode input calldata using ABIs
# 5. Call build_graph() from graph_builder.py
# 6. Call detect_cycle() from cycle_detector.py
# 7. Write results to suspicious_txns Kafka topic
```

## graph_builder.py implementation guide
```python
import networkx as nx

def build_graph(decoded_tx: dict, event_logs: list) -> nx.DiGraph:
    """
    Build a directed graph from a decoded flash loan transaction.
    Nodes = token addresses
    Edges = token transfers/swaps with attributes: amount, dex_name, step_index
    """
    G = nx.DiGraph()
    # Add nodes and edges from decoded_tx and event_logs
    # event_logs come from eth_getLogs for the tx hash
    return G
```

## cycle_detector.py implementation guide
```python
import networkx as nx
import time

def detect_cycle(graph: nx.DiGraph) -> list:
    """
    Returns the first cycle found, or empty list if none.
    Uses networkx.simple_cycles() which handles self-loops safely.
    Must complete in < 50ms (spec requirement).
    """
    start = time.perf_counter()
    cycles = list(nx.simple_cycles(graph))
    elapsed_ms = (time.perf_counter() - start) * 1000
    if elapsed_ms > 50:
        print(f"[warning] DFS took {elapsed_ms:.1f}ms (spec limit: 50ms)")
    return cycles[0] if cycles else []

def score_confidence(cycle_found: bool, price_deviation: float,
                     threshold: float = 0.05) -> str:
    if cycle_found and price_deviation > threshold:
        return "HIGH"
    elif cycle_found:
        return "MEDIUM"
    else:
        return "LOW"
```

## Verification
Person 3 can verify their decoding is correct by comparing against
`data/verification_expected.csv`. See `data/verification_expected.csv`
for the exact expected values for each of the 23 successfully decoded
flash loan transactions.

Key things to verify:
- `function_name` must be exact: "flashLoan" or "flashLoanSimple"
- `amounts_raw_json` must use strings not floats (uint256 precision)
- USDC/USDT: divide by 10^6 to get human amount
- WETH/DAI/USDe/LINK: divide by 10^18
- WBTC: divide by 10^8
