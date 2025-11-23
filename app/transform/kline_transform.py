from pyspark.sql.functions import col, from_json, year, month, dayofmonth, coalesce, lit
from pyspark.sql.types import DoubleType, LongType, IntegerType
from schema.kline_schema import kline_schema

def transform_kline(kline_raw_df):
    kline_cleaned_df = (
        kline_raw_df
        .select(from_json(col("value").cast("string"), kline_schema).alias("data"))
        .filter(col("data").isNotNull())
        .select(
            col("data.s").alias("symbol"),
            (col("data.E") / 1000).cast("timestamp").alias("event_time"),
            (col("data.k.t") / 1000).cast("timestamp").alias("open_time"),
            
            # Giữ 2 cột này (Nhớ sửa file create table để hứng nó)
            (col("data.k.T") / 1000).cast("timestamp").alias("close_time"),
            col("data.k.i").alias("interval"),
            
            col("data.k.o").cast(DoubleType()).alias("open"),
            col("data.k.h").cast(DoubleType()).alias("high"),
            col("data.k.l").cast(DoubleType()).alias("low"),
            col("data.k.c").cast(DoubleType()).alias("close"),
            col("data.k.v").cast(DoubleType()).alias("volume"),
            col("data.k.q").cast(DoubleType()).alias("quote_volume"),
            
            col("data.k.n").cast(LongType()).alias("num_trades"),
            col("data.k.V").cast(DoubleType()).alias("taker_buy_volume"),
            col("data.k.Q").cast(DoubleType()).alias("taker_buy_quote_vol"),
            
            col("data.k.x").cast("integer").alias("is_closed")
            
            # ĐÃ XÓA CỘT 'ignore' Ở ĐÂY ĐỂ TRÁNH LỖI
        )
        .filter(col("symbol").isNotNull())
        .withColumn("Year", coalesce(year(col("event_time")), lit(2025)))
        .withColumn("Month", coalesce(month(col("event_time")), lit(1)))
        .withColumn("Day", coalesce(dayofmonth(col("event_time")), lit(1)))
    )
    return kline_cleaned_df