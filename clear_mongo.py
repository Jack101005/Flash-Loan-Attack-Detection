import os
import certifi
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

uri = os.environ.get("MONGODB_URI")
flashloan_db_name = os.environ.get("MONGODB_FLASHLOAN_NAME", "flash_loan_detection")


client = MongoClient(uri, tlsCAFile=certifi.where())

# Lấy database
db = client[flashloan_db_name]

# Xóa tất cả document (row) trong collection 'transactions' (hoặc collection bạn đang dùng)
# Nếu collection có tên là 'transactions'
result = db.transactions.delete_many({})

print(f"Successfully deleted {result.deleted_count} rows in database: {flashloan_db_name}")
