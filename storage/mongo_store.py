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

MONGODB_URI: str = os.getenv("MONGODB_URI")
MONGODB_DB_NAME: str = os.getenv("MONGODB_FLASHLOAN_NAME")

if not MONGODB_URI:
    raise EnvironmentError("MONGODB_URI is not set. Check your backend/.env file.")


# singleton client
_client: MongoClient | None = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(
            MONGODB_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            tls=True,
            tlsCAFile=certifi.where(),
        )
    return _client


def get_db() -> Database:
    return get_client()[MONGODB_DB_NAME]


def ping() -> bool:
    try:
        get_client().admin.command("ping")
        return True
    except (ConnectionFailure, ServerSelectionTimeoutError):
        return False


# collection helpers
def transactions_collection() -> Collection:
    return get_db()["transactions"]


def alerts_collection() -> Collection:
    return get_db()["alerts"]


# index initialisation
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


# document schemas
def make_transaction_doc(
    tx_hash: str,
    block_number: int,
    is_flash_loan: bool,
    risk_level: str,          # "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"
    summary: str,
    raw_data: dict | None = None,
) -> dict:

    return {
        "tx_hash": tx_hash,
        "block_number": block_number,
        "is_flash_loan": is_flash_loan,
        "risk_level": risk_level,
        "summary": summary,
        "raw_data": raw_data or {},
        "detected_at": datetime.now(timezone.utc),
    }


def save_detection(doc: dict) -> None:
    """Upsert a stream_processor detection into the transactions collection."""
    transactions_collection().update_one(
        {"tx_hash": doc["tx_hash"]},
        {"$set": {**doc, "detected_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


def get_recent_detections(limit: int = 50) -> list[dict]:
    """Return the most recent flash loan detections, newest first."""
    cursor = (
        transactions_collection()
        .find({}, {"_id": 0})
        .sort("detected_at", DESCENDING)
        .limit(limit)
    )
    return list(cursor)


def make_alert_doc(
    tx_hash: str,
    risk_level: str,
    message: str,
    notified: bool = False,
) -> dict:

    return {
        "tx_hash": tx_hash,
        "risk_level": risk_level,
        "message": message,
        "notified": notified,
        "created_at": datetime.now(timezone.utc),
    }