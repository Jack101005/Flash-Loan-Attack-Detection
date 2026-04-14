import os
import certifi
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Load .env – try backend/.env first, then root .env
_here = os.path.dirname(__file__)
for _candidate in [
    os.path.join(_here, "..", "backend", ".env"),
    os.path.join(_here, ".env"),
    os.path.join(_here, "..", ".env"),
]:
    if os.path.isfile(_candidate):
        load_dotenv(dotenv_path=_candidate)
        break

MONGODB_URI: str = os.getenv("MONGODB_URI", "")
MONGODB_DB_NAME: str = os.getenv("MONGODB_DB_NAME", "flash_loan_detection")

if not MONGODB_URI:
    raise EnvironmentError("MONGODB_URI is not set. Check your backend/.env file.")


# Singleton client
_client: MongoClient | None = None


def get_client() -> MongoClient:
    """Return (or lazily create) the shared MongoClient."""
    global _client
    if _client is None:
        # certifi provides an up-to-date CA bundle, fixing SSL handshake
        # errors with MongoDB Atlas on Python 3.13 / newer OpenSSL.
        _client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            tls=True,
            tlsCAFile=certifi.where(),
        )
    return _client


def get_db() -> Database:
    """Return the application database."""
    return get_client()[MONGODB_DB_NAME]


def ping() -> bool:
    """Return True if the cluster is reachable, False otherwise."""
    try:
        get_client().admin.command("ping")
        return True
    except (ConnectionFailure, ServerSelectionTimeoutError):
        return False


# Collection helpers
def transactions_collection() -> Collection:
    return get_db()["transactions"]


def alerts_collection() -> Collection:
    return get_db()["alerts"]


# Schema / index initialisation
def init_indexes() -> None:
    """
    Ensure indexes exist for all collections.
    Call once at application startup.
    """
    txn = transactions_collection()
    txn.create_index([("tx_hash", ASCENDING)], unique=True, name="tx_hash_unique")
    txn.create_index([("block_number", DESCENDING)], name="block_number_desc")
    txn.create_index([("detected_at", DESCENDING)], name="detected_at_desc")
    txn.create_index([("risk_level", ASCENDING)], name="risk_level_asc")

    alr = alerts_collection()
    alr.create_index([("tx_hash", ASCENDING)], name="alert_tx_hash")
    alr.create_index([("created_at", DESCENDING)], name="alert_created_at_desc")


# Document schemas (plain dicts – use as factory functions)
def make_transaction_doc(
    tx_hash: str,
    block_number: int,
    is_flash_loan: bool,
    risk_level: str,          # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    summary: str,
    raw_data: dict | None = None,
) -> dict:
    """
    Schema for the `transactions` collection.

    {
        tx_hash:       str    unique transaction hash
        block_number:  int    Ethereum block number
        is_flash_loan: bool   detection result
        risk_level:    str    severity
        summary:       str    human-readable description
        raw_data:      dict   original decoded payload (optional)
        detected_at:   datetime (UTC)
    }
    """
    return {
        "tx_hash": tx_hash,
        "block_number": block_number,
        "is_flash_loan": is_flash_loan,
        "risk_level": risk_level,
        "summary": summary,
        "raw_data": raw_data or {},
        "detected_at": datetime.now(timezone.utc),
    }


def make_alert_doc(
    tx_hash: str,
    risk_level: str,
    message: str,
    notified: bool = False,
) -> dict:
    """
    Schema for the `alerts` collection.

    {
        tx_hash:    str    linked transaction
        risk_level: str    severity
        message:    str    alert body
        notified:   bool   whether the alert was dispatched
        created_at: datetime (UTC)
    }
    """
    return {
        "tx_hash": tx_hash,
        "risk_level": risk_level,
        "message": message,
        "notified": notified,
        "created_at": datetime.now(timezone.utc),
    }
