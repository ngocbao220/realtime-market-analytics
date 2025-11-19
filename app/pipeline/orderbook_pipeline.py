from ingestion.kafka_reader import read_kafka_stream
from transform.orderbook_transform import orderbook_transform
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.console_writer import write_console_stream
from sinks.parquet_writer import write_parquet_stream
from table.create_orderbook_table import create_clickhouse_table_orderbook
from config.setting import *

def start_orderbook_pipeline(spark):
    # 1. Đọc Kafka
    # Lưu ý: Orderbook rất nặng, cần đảm bảo read_kafka_stream đã có maxOffsetsPerTrigger
   
    df_raw = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", TOPIC_TICKERS)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 1000) 
            .load())
    df_clean = orderbook_transform(df_raw)
    #write_console_stream(df_clean, "orderbook", ["symbol","event_time","bid_prices","bid_quantities","ask_prices","ask_quantities"])
    # 3. Ghi Parquet (Lưu kho)
    #write_parquet_stream(
    #    df_clean,
    #    path=f"{OUTPUT_PATH}/orderbook",
    #    checkpoint=f"{CHECKPOINT_DIR}/orderbook",
    #    partition_cols=["symbol","Year","Month","Day"]
    #)

    # 4. Tạo bảng ClickHouse (nếu chưa có)
    try:
        create_clickhouse_table_orderbook(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE
        )
        print("Table 'orderbook' checked/created.")
    except Exception as e:
        print(f"Error creating table orderbook: {e}")

    # 5. Ghi ClickHouse (Real-time)
    def safe_write(batch_df, batch_id):
        try:
            write_clickhouse_batch(
                batch_df,
                batch_id,
                table_name="orderbook",
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