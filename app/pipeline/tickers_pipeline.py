from consumer.kafka_reader import read_kafka_stream
from transform.trades_transform import transform_trades
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.redis_writer import write_redis_batch_logic
from table.create_trades_table import create_clickhouse_table_trade
from config.setting import *

def start_tickers_pipeline(spark):
    # 1. Read and clean df
    df_raw = read_kafka_stream(spark, KAFKA_BROKER, TOPIC_TICKERS)
    df_clean = transform_tickers(df_raw)
    
    # 2. Write console log to observation
    write_console_stream(df_clean, "tickers", ["symbol","close_price","volume","event_time","open_time","close_time"])

    # 4. Write clickhouse to process real-time
    # Create table if not exists
    try:
        create_clickhouse_table_ticker(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        print("Table tickers created/exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

    # Write to ClickHouse safely
    def safe_write(batch_df, batch_id):
        try:
            write_clickhouse_batch(
                batch_df,
                batch_id,
                table_name="tickers",
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASSWORD,
                database=CLICKHOUSE_DATABASE,
                host=CLICKHOUSE_HOST,
                port=CLICKHOUSE_PORT
            )
        except Exception as e:
            print(f"Batch {batch_id} failed: {e}")

    query_ch = df_clean.writeStream.foreachBatch(safe_write).start()
    return query_ch