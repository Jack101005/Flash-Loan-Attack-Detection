import redis
import requests
import json
import time


try:
    r = redis.Redis(host='redis', port=6379, decode_responses=True)
    print("✅ P4: Connected to Redis successfully!")
except Exception as e:
    print(f"❌ P4: Could not connect to Redis: {e}")

def fetch_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        price = data['ethereum']['usd']
        
        # save on redis for P3
        payload = {
            "price": price,
            "timestamp": time.time()
        }
        r.set('ETH_PRICE_STATE', json.dumps(payload))
        print(f"💰 [Price Feed] ETH Price updated: ${price}")
    except Exception as e:
        print(f"⚠️ [Price Feed] API Error: {e}")

if __name__ == "__main__":
    print("🚀 Price Feed service is starting...")
    while True:
        fetch_price()
        time.sleep(15)