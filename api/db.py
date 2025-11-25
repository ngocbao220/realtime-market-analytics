import redis
from clickhouse_driver import Client
from config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB
)

# --- 1. KẾT NỐI REDIS (Hot Data) ---
redis_client = None
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
except Exception as e:
    print(f"❌ Lỗi kết nối Redis: {e}")

# --- 2. KẾT NỐI CLICKHOUSE (Cold Data) ---
ch_client = None
try:
    ch_client = Client(
        host=CLICKHOUSE_HOST, 
        port=CLICKHOUSE_PORT, 
        user=CLICKHOUSE_USER, 
        password=CLICKHOUSE_PASSWORD, 
        database=CLICKHOUSE_DB
    )
except Exception as e:
    print(f"❌ Lỗi kết nối ClickHouse: {e}")