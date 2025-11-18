# Kafka config
KAFKA_BROKER = "kafka:9092"
TOPIC_TRADES = "binance_trades"
TOPIC_TICKERS = "binance_tickers_1h"
TOPIC_ORDERBOOK = "binance_orderbook"

# Đường dẫn lưu Parquet
OUTPUT_PATH = "/data/processed"
CHECKPOINT_DIR = "/checkpoints"


# Clickhouse config
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "12345"
CLICKHOUSE_USER = "default"
CLICKHOUSE_DATABASE = "default"

# Processing time for real-time
PROCESSING_TIME="2 seconds"