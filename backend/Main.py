import sys
import os

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

@app.on_event("startup")
def on_startup():
    """Ensure MongoDB indexes exist on first boot."""
    init_indexes()
    print("[OK] MongoDB indexes initialized.")


class DecodeRequest(BaseModel):
    tx_hash: str


class DecodeResponse(BaseModel):
    tx_hash: str
    is_flash_loan: bool
    risk_level: str
    summary: str

#route
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