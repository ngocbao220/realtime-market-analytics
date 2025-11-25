# Kafka config
KAFKA_BROKER = "kafka:9092"
TOPIC_TRADES = "binance_trades"
TOPIC_TICKERS = "binance_tickers_1d"
TOPIC_ORDERBOOK = "binance_orderbook"
TOPIC_KLINE = "binance_kline_1m"


# Clickhouse config
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 9000
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "12345"
CLICKHOUSE_USER = "default"
CLICKHOUSE_DATABASE = "default"

# Redis congig
REDIS_HOST = "redis" 
REDIS_PORT = 6379
CHECKPOINT_DIR = "/checkpoints"

# Processing time for real-time
PROCESSING_TIME="2 seconds"