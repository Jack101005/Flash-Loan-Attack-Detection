import os
from pyspark.sql import SparkSession

MONGO_URI  = os.getenv("MONGODB_URI", "mongodb://mongodb:27017")
MONGO_DB   = os.getenv("MONGODB_FLASHLOAN_NAME", "flash_loan_detection")
MONGO_COLL = "transactions_test"

def write_partition(rows):
    import socket, certifi
    from pymongo import MongoClient, UpdateOne
    host = socket.gethostname()
    client = MongoClient(MONGO_URI, tls=True, tlsCAFile=certifi.where(),
                         serverSelectionTimeoutMS=5000, connectTimeoutMS=5000)
    try:
        coll = client[MONGO_DB][MONGO_COLL]
        ops = []
        for r in rows:
            doc = {"tx_hash": r["tx_hash"], "amount_usd": r["amount_usd"], "worker_host": host}
            ops.append(UpdateOne({"tx_hash": doc["tx_hash"]}, {"$set": doc}, upsert=True))
        if ops:
            coll.bulk_write(ops, ordered=False)
            print(f"[test] partition wrote {len(ops)} docs from host {host}")
    finally:
        client.close()

spark = (SparkSession.builder.appName("TestDistributedWrite")
         .master("spark://spark-master:7077").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

rows = [(f"0xtest{i:03d}", float(i * 1000)) for i in range(16)]
df = spark.createDataFrame(rows, ["tx_hash", "amount_usd"]).repartition(4)
df.foreachPartition(write_partition)

print("[test] done - check worker logs for partition wrote")
spark.stop()
