import redis
import json
import time
import os
import certifi
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# 1. Connect to Redis
try:
    r = redis.Redis(
        host=os.getenv('REDIS_HOST', 'redis'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASS'),
        decode_responses=True
    )
    print("✅ P4: Connected to Redis successfully!")
except Exception as e:
    print(f"❌ P4: Could not connect to Redis: {e}")

# 2. Connect to MongoDB
MONGODB_URI = os.getenv("MONGODB_URI")
# Use 'defi_transactions' to match the default from import_pymongo.py
MONGODB_DB_NAME = os.getenv("MONGODB_TRANSACTIONS_NAME")

try:
    mongo_client = MongoClient(
        MONGODB_URI,
        tls=True,
        tlsCAFile=certifi.where()
    )
    db = mongo_client[MONGODB_DB_NAME]
    
    # Target the specific collection imported from aave_v3.json
    collections = ["aave_v3", "balancer_v2", "flash_loan_simple", "flash_loan", "uniswap_v3"] 
    print(f"✅ P4: Connected to MongoDB database '{MONGODB_DB_NAME}' successfully!")
except Exception as e:
    print(f"❌ P4: Could not connect to MongoDB: {e}")

def fetch_mongo_data():
    try:
        # 3. Query the all transactions
        for collection_name in collections:
            collection = db[collection_name]
            all_tx = collection.find()
        
            for tx in all_tx:
                # Convert ObjectId to string to prevent JSON serialization errors
                tx['_id'] = str(tx['_id'])
                
                # Save the full transaction payload on Redis for downstream processing
                r.lpush('TX_QUEUE', json.dumps(tx))
                
                tx_hash = tx.get('transaction_hash', 'Unknown')
                method = tx.get('method', 'Unknown')
                amount = tx.get('amount', 'Unknown')
                value_usd = tx.get('value_usd', 'Unknown')
                print(f"🔗 [Data Feed] \n Fetched {method} \n Tx: {tx_hash} \n Amount: {amount} \n Value: {value_usd} \n\n")
                
    except Exception as e:
        print(f"⚠️ [Data Feed] MongoDB Query Error: {e}")

if __name__ == "__main__":
    print("🚀 Mongo Data Feed service is starting...")
    while True:
        fetch_mongo_data()