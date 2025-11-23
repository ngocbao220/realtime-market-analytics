from pyspark.sql.functions import col, from_json, year, month, dayofmonth, coalesce, lit
from pyspark.sql.types import DoubleType, LongType, IntegerType
from schema.kline_schema import kline_schema
from pyspark.sql.functions import from_utc_timestamp

def transform_klines(kline_raw_df):
    kline_cleaned_df = (
        kline_raw_df
        .select(from_json(col("value").cast("string"), kline_schema).alias("data"))
        .filter(col("data").isNotNull())
        .select(
            col("data.s").alias("Symbol"),
            from_utc_timestamp((col("data.E") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Event_time"),
            from_utc_timestamp((col("data.k.t") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Open_time"),
            
            # Giữ 2 cột này (Nhớ sửa file create table để hứng nó)
            from_utc_timestamp((col("data.k.T") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Close_time"),
            col("data.k.i").alias("Interval"),
            
            col("data.k.o").cast(DoubleType()).alias("Open"),
            col("data.k.h").cast(DoubleType()).alias("High"),
            col("data.k.l").cast(DoubleType()).alias("Low"),
            col("data.k.c").cast(DoubleType()).alias("Close"),
            col("data.k.v").cast(DoubleType()).alias("Volume"),
            col("data.k.q").cast(DoubleType()).alias("Quote_volume"),
            
            col("data.k.n").cast(LongType()).alias("Num_trades"),
            col("data.k.V").cast(DoubleType()).alias("Taker_buy_volume"),
            col("data.k.Q").cast(DoubleType()).alias("Taker_buy_quote_vol"),
            
            col("data.k.x").cast("integer").alias("Is_closed")
            
        )
        .filter(col("Symbol").isNotNull())
        .withColumn("Year", coalesce(year(col("Event_time")), lit(2025)))
        .withColumn("Month", coalesce(month(col("Event_time")), lit(1)))
        .withColumn("Day", coalesce(dayofmonth(col("Event_time")), lit(1)))
    )
    return kline_cleaned_df