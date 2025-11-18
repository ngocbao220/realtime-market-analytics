from ingestion.kafka_reader import read_kafka_stream
from transform.trades_transform import transform_trades
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.parquet_writer import write_parquet_stream
from sinks.console_writer import write_console_stream
from table.create_trades_table import create_clickhouse_table_trade
from config.setting import *

def start_trades_pipeline(spark):
    # 1. Read and clean df
    df_raw = read_kafka_stream(spark, KAFKA_BROKER, TOPIC_TRADES)
    df_clean = transform_trades(df_raw)
    
    # 2. Write console log to observation
    write_console_stream(df_clean, "trades", ["Symbol","Price","Quantity","Side","TradeValue","TradeTime"])

    # 3. Write parquet to store further
    write_parquet_stream(
        df_clean,
        path=f"{OUTPUT_PATH}/trades",
        checkpoint=f"{CHECKPOINT_DIR}/trades",
        partition_cols=["Symbol","Year","Month","Day"]
    )

    # 4. Write clickhouse to process real-time
    # Create table if not exists
    try:
        create_clickhouse_table_trade(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        print("Table trades created/exists.")
    except Exception as e:
        print(f"Error creating table: {e}")

    # Write to ClickHouse safely
    def safe_write(batch_df, batch_id):
        try:
            write_clickhouse_batch(
                batch_df,
                batch_id,
                table_name="trades",
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
