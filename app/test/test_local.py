# test_local.py
from pyspark.sql import SparkSession
from config.setting import OUTPUT_PATH, CHECKPOINT_DIR, USER, PASSWORD
from transform.trades_transform import transform_trades
from sinks.clickhouse_writer import write_clickhouse_batch
from sinks.parquet_writer import write_parquet_stream

if __name__ == "__main__":
    # 1. Khởi tạo Spark
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("TestLocalPipeline")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 2. Giả lập dữ liệu raw Kafka
    raw_data = [
        ('{"s":"BTCUSDT","t":123456,"p":"30000","q":"0.5","E":1700000000000,"T":1700000001000,"m":true}',),
        ('{"s":"ETHUSDT","t":123457,"p":"2000","q":"1.0","E":1700000002000,"T":1700000003000,"m":false}',)
    ]
    df_raw = spark.createDataFrame(raw_data, ["value"])

    # 3. Transform
    df_clean = transform_trades(df_raw)
    print("===== Transformed Data =====")
    df_clean.show(truncate=False)

    # 4. Ghi vào Parquet (local folder)
    write_parquet_stream(
        df=df_clean,
        path=f"{OUTPUT_PATH}/trades_test",
        checkpoint=f"{CHECKPOINT_DIR}/trades_test",
        partition_cols=["Symbol", "Year", "Month", "Day"]
    )

    # 5. Ghi vào ClickHouse
    write_clickhouse_batch(
        df=df_clean,
        batch_id=0,
        table_name="trades",
        user=USER,
        password=PASSWORD
    )

    print("===== Local pipeline test finished =====")
