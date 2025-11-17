from ingestion.kafka_reader import read_kafka_stream
from transform.trades_transform import transform_trades
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.parquet_writer import write_parquet_stream

from config.setting import *

def start_trades_pipeline(spark):
    # 1. Đọc dữ liệu từ Kafka
    df_raw = read_kafka_stream(spark=spark, kafka_broker=KAFKA_BROKER, topic=TOPIC_TRADES)
    
    # 2. Clean & Transform
    df_clean = transform_trades(df_raw)
    
    # 3. Ghi vào Parquet (Streaming)
    write_parquet_stream(
        df=df_clean,
        path=f"{OUTPUT_PATH}/trades",
        checkpoint=f"{CHECKPOINT_DIR}/trades",
        partition_cols=["Symbol", "Year", "Month", "Day"]
    )
    
    # 4. Ghi vào ClickHouse (Streaming) qua foreachBatch
    query_ch = df_clean.writeStream.foreachBatch(
        lambda batch_df, batch_id: write_clickhouse_batch(batch_df, table_name="trades", user=USER, password=PASSWORD)
    ).start()
    
    return query_ch
