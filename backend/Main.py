import sys
import os
import redis
import json

# Allow importing from the project root (e.g. storage.mongo_store)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from storage.mongo_store import init_indexes, ping, transactions_collection, make_transaction_doc

app = FastAPI(title="Flash Loan Attack Detection API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

redis_client = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379"), 
    decode_responses=True
)

# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

@app.on_event("startup")
def on_startup():
    """Ensure MongoDB indexes exist on first boot."""
    init_indexes()
    print("[OK] MongoDB indexes initialized.")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class DecodeRequest(BaseModel):
    tx_hash: str


class DecodeResponse(BaseModel):
    tx_hash: str
    is_flash_loan: bool
    risk_level: str
    summary: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/")
def root():
    return {"message": "Server is running!"}


@app.get("/health/db")
def health_db():
    """Ping MongoDB Atlas and report connection status."""
    ok = ping()
    return {
        "database": "mongodb",
        "status": "connected" if ok else "disconnected",
        "ok": ok,
    }


@app.post("/decode", response_model=DecodeResponse)
def decode(body: DecodeRequest):
    result = DecodeResponse(
        tx_hash=body.tx_hash,
        is_flash_loan=True,
        risk_level="HIGH",
        summary="Flash loan attack detected! Circular trade found.",
    )

    # Persist to MongoDB
    doc = make_transaction_doc(
        tx_hash=result.tx_hash,
        block_number=0,
        is_flash_loan=result.is_flash_loan,
        risk_level=result.risk_level,
        summary=result.summary,
    )
    transactions_collection().update_one(
        {"tx_hash": doc["tx_hash"]},
        {"$setOnInsert": doc},
        upsert=True,
    )

    return result

@app.get("/live-detections")
def get_live_detections():
    """Fetch the latest transactions pushed to Redis by price_feed.py"""
    try:
        raw_txs = redis_client.lrange('TX_QUEUE', 0, 49)
        detections = []
        
        for tx_str in raw_txs:
            try:
                tx = json.loads(tx_str)
                
                # 1. Clean up the token symbol (Extracts "ETH" from "Ethereum(ETH)")
                raw_asset = tx.get("asset", "ETH")
                asset_symbol = raw_asset.split('(')[-1].replace(')', '') if '(' in raw_asset else raw_asset
                
                # 2. Clean up the target protocol/pool name (Removes extra spaces)
                target_pool = tx.get("to_nametag", "Smart Contract").strip()
                if not target_pool:
                    target_pool = "Smart Contract"

                # 3. Construct a logical visual cycle for the topology graph
                # Creates a path like: ["ETH", "Aave: Pool V3", "Arbitrage Execution", "ETH"]
                fallback_cycle = [asset_symbol, target_pool, "Arbitrage Execution", asset_symbol]
                
                # Map the raw JSON to the frontend's Detection interface
                detections.append({
                    "tx_hash": tx.get("transaction_hash", "0x000").strip(),
                    "is_suspicious": tx.get("is_suspicious", True),
                    "confidence": "HIGH" if float(tx.get("txn_fee", 0)) > 0.0001 else "MEDIUM",
                    "cycle_path": tx.get("cycle_path", fallback_cycle), 
                    "profit_estimate": float(tx.get("amount", 0)), 
                    "price_deviation": float(tx.get("txn_fee", 0)) * 1000, # Mocking deviation for UI
                    "protocol": tx.get("source", "Unknown"),
                    "timestamp": tx.get("block", 0) # Using block as a mock timestamp if age isn't parsed
                })
            except (json.JSONDecodeError, ValueError):
                continue
                
        return detections
    except Exception as e:
        print(f"Error fetching from Redis: {e}")
        return []