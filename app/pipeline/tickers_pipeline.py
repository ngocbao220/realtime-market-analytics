from ingestion.kafka_reader import read_kafka_stream
from transform.tickers_transform import transform_tickers
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.parquet_writer import write_parquet_stream
from sinks.console_writer import write_console_stream
from table.create_tickers_table import create_clickhouse_table_ticker
from config.setting import *
import traceback

def start_tickers_pipeline(spark):
    # 1. Read and clean df
    df_raw = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TOPIC_TICKERS)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            # --- QUAN TRỌNG: Giới hạn cứng 200 dòng ---
            .option("maxOffsetsPerTrigger", 5000) 
            .load())
    df_clean = transform_tickers(df_raw)
    
    # 2. Write console log to observation
    write_console_stream(df_clean, "tickers", ["symbol","close_price","volume","event_time","open_time","close_time"])

    # 3. Write parquet to store further
    write_parquet_stream(
       df_clean,
       path=f"{OUTPUT_PATH}/tickers",
       checkpoint=f"{CHECKPOINT_DIR}/tickers",
       partition_cols=["symbol","Year","Month","Day"]
    )

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