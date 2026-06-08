import sys
import os
import asyncio
import json

# Allow importing from the project root (e.g. storage.mongo_store)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Optional
from pydantic import BaseModel

from storage.mongo_store import init_indexes, ping, transactions_collection, make_transaction_doc, get_recent_detections

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
    try:
        init_indexes()
        print("[OK] MongoDB indexes initialized.")
    except Exception as e:
        print(f"[WARN] MongoDB unavailable at startup, indexes skipped: {e}")


class DecodeRequest(BaseModel):
    tx_hash: str


class DecodeResponse(BaseModel):
    tx_hash: str
    is_flash_loan: bool
    risk_level: str
    summary: str
    protocol: Optional[str] = None
    from_address: Optional[str] = None
    token: Optional[str] = None
    amount_usd: Optional[float] = None
    total_usd: Optional[float] = None
    confidence: Optional[str] = None
    pools_count: Optional[int] = None
    cycle_path: Optional[list] = None

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
    raw = body.tx_hash.strip()
    # Normalize: build both variants for flexible matching
    lower = raw.lower()
    no_prefix = lower[2:] if lower.startswith("0x") else lower
    with_prefix = "0x" + no_prefix

    try:
        doc = transactions_collection().find_one(
            {"tx_hash": {"$in": [no_prefix, with_prefix, raw, lower]}},
            {"_id": 0},
        )
    except Exception as e:
        print(f"[decode] MongoDB query error: {e}")
        doc = None

    if not doc:
        return DecodeResponse(
            tx_hash=raw,
            is_flash_loan=False,
            risk_level="UNKNOWN",
            summary="Transaction not found in detection database. It may not have been processed yet or is not a flash loan.",
        )

    confidence = doc.get("confidence", "LOW")
    risk_map   = {"HIGH": "HIGH", "MEDIUM": "MEDIUM", "LOW": "LOW"}
    risk_level = risk_map.get(confidence, "LOW")

    protocol   = doc.get("protocol", "Unknown")
    token      = doc.get("token", "Unknown")
    amount     = doc.get("amount_usd", 0)
    total      = doc.get("total_usd", 0)
    sender     = doc.get("from", "Unknown")
    pools      = doc.get("pools_count", 1)

    summary = (
        f"Flash loan detected via {protocol}. "
        f"Sender: {sender[:20]}... | "
        f"Token: {token} | "
        f"Amount: ${amount:,.0f} USD | "
        f"Total by sender: ${total:,.0f} USD | "
        f"Pools used: {pools}"
    )

    return DecodeResponse(
        tx_hash=raw,
        is_flash_loan=True,
        risk_level=risk_level,
        summary=summary,
        protocol=protocol,
        from_address=sender,
        token=token,
        amount_usd=float(amount),
        total_usd=float(total),
        confidence=confidence,
        pools_count=int(pools),
        cycle_path=doc.get("cycle_path", []),
    )

@app.get("/stream/detections")
async def stream_detections():
    """SSE endpoint — stream flash loan detections to frontend."""

    async def event_generator():
        last_fingerprint = None
        first_poll = True
        while True:
            try:
                docs = get_recent_detections(50)
                detections = [
                    {
                        "tx_hash":       d.get("tx_hash", "0x000"),
                        "is_suspicious": True,
                        "confidence":    d.get("confidence", "LOW"),
                        "cycle_path":    d.get("cycle_path", []),
                        "amount_usd":    float(d.get("amount_usd", 0)),
                        "total_usd":     float(d.get("total_usd", 0)),
                        "protocol":      d.get("protocol", "Unknown"),
                        "timestamp":     int(d.get("timestamp", 0)),
                        "from":          d.get("from", ""),
                        "token":         d.get("token", ""),
                    }
                    for d in docs
                ]

                # Fingerprint = count + newest tx_hash (detects inserts, deletes, and updates)
                fingerprint = f"{len(detections)}:{detections[0]['tx_hash'] if detections else ''}"

                if first_poll or fingerprint != last_fingerprint:
                    yield f"event: detections\ndata: {json.dumps(detections)}\n\n"
                    last_fingerprint = fingerprint
                    first_poll = False
                else:
                    yield ": heartbeat\n\n"

            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'error': str(e)})}\n\n"

            await asyncio.sleep(1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )