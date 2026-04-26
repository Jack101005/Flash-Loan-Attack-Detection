# Skill: Storage & State Management (Person 4)

## Key files (currently empty)
- `storage/redis_cache.py` — shared Redis client
- `storage/mongo_store.py` — MongoDB writer
- `storage/docker-compose.yml` — Redis + MongoDB containers

## Status: NOT STARTED

## Redis responsibilities
- Price feed: `price:{token_address}` → USD price as string, TTL = 30s
- Deduplication: `alerted:{tx_hash}` → "1", TTL = 24h
- Config: maxmemory 256mb, policy allkeys-lru

## redis_cache.py interface (what Person 3 and Person 5 import)
```python
def get_price(token_address: str) -> float | None:
    """Returns USD price or None if not cached."""

def set_price(token_address: str, usd_price: float) -> None:
    """Stores price with 30s TTL."""

def is_duplicate(tx_hash: str) -> bool:
    """Returns True if already alerted for this tx."""

def mark_alerted(tx_hash: str) -> None:
    """Sets alerted flag with 24h TTL."""
```

## MongoDB schema (detections collection)
```json
{
  "tx_hash": "string (unique index)",
  "timestamp": "datetime (index)",
  "confidence": "HIGH | MEDIUM | LOW",
  "protocol": "string",
  "cycle_path": ["address", "address", ...],
  "profit_estimate": "float",
  "price_deviation": "float",
  "graph_snapshot": {"nodes": [], "edges": []},
  "alerted": "bool"
}
```

## mongo_store.py interface (what Person 5 imports)
```python
def write_detection(result_dict: dict) -> bool:
    """Inserts detection into MongoDB. Returns True on success."""
```

## Token decimals reference (for price calculations)
- USDC: 10^6 (address: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48)
- USDT: 10^6 (address: 0xdAC17F958D2ee523a2206206994597C13D831ec7)
- WETH: 10^18 (address: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2)
- WBTC: 10^8 (address: 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599)
- DAI:  10^18 (address: 0x6B175474E89094C44Da98b954EedeAC495271d0F)
