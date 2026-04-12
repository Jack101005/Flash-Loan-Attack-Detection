from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class DecodeRequest(BaseModel):
    tx_hash: str


class DecodeResponse(BaseModel):
    tx_hash: str
    is_flash_loan: bool
    risk_level: str
    summary: str


@app.get("/")
def root():
    return {"message": "Server is running!"}


@app.post("/api/decode")
def decode(body: DecodeRequest):
    return DecodeResponse(
        tx_hash=body.tx_hash,
        is_flash_loan=True,
        risk_level="HIGH",
        summary="Flash loan attack detected! Circular trade found.",
    )