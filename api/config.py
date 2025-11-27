import os

# --- CẤU HÌNH CLICKHOUSE ---
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "12345")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DATABASE", "default")

# --- CẤU HÌNH REDIS ---
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

# --- CẤU HÌNH INTERVAL QUERY (Mapping cho ClickHouse) ---
INTERVAL_MAP = {
    "1m": "toStartOfMinute(Open_time)",
    "5m": "toStartOfFiveMinutes(Open_time)",
    "15m": "toStartOfFifteenMinutes(Open_time)",
    "1h": "toStartOfHour(Open_time)",
    "1d": "toStartOfDay(Open_time)"
}

# Tốc độ làm mới bảng
SPEED = 0.5
SPEED_WEBSOCKET = 0.1