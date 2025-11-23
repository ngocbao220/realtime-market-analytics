from consumer.kafka_reader import read_kafka_stream
from transform.kline_transform import transform_kline
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.redis_writer import write_redis_batch_logic
from table.create_kline_table import create_clickhouse_table_kline
from config.setting import *

def start_kline_pipeline(spark):
    # 1. Read and clean df
    df_raw = read_kafka_stream(spark, KAFKA_BROKER, TOPIC_KLINE)
    df_clean = transform_kline(df_raw)
    
    # 2. Write console log to observation
    # write_console_stream(df_clean, "kline", ["Symbol","Price","Quantity","Side","TradeValue","TradeTime"])

    
    # 4. Write clickhouse to process real-time
    # Create table if not exists
    try:
        create_clickhouse_table_kline(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        print("Table kline created/exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

    # Write to ClickHouse safely
    def safe_write(batch_df, batch_id):
        try:
            write_clickhouse_batch(
                batch_df,
                batch_id,
                table_name="klines",
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
