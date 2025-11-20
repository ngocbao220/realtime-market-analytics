from pyspark.sql.functions import (
    col, to_timestamp, from_json, year, month, dayofmonth, hour,
    lit, posexplode, when, sum as _sum, avg, stddev, count,
    window, lag, expr
)
from pyspark.sql.types import *
from schema.ticker_schema import ticker_schema 

def transform_tickers(tickers_raw_df):
    """
    Transform raw tickers DataFrame từ Kafka sang DataFrame đã clean.
    Mapping dựa trên Binance Ticker Schema:
    
    s -> symbol
    e -> event_type
    E -> event_time (Event timestamp)
    O -> open_time  (Statistics open time - Thời gian mở nến)
    C -> close_time (Statistics close time - Thời gian đóng nến)
    o -> open_price
    h -> high_price
    l -> low_price
    c -> close_price
    v -> volume
    q -> quote_volume
    """
    
    tickers_cleaned_df = (
        tickers_raw_df
        # 1. Parse JSON bằng ticker_schema
        .select(from_json(col("value").cast("string"), ticker_schema).alias("data"))
        .filter(col("data").isNotNull())
        .select(
            # 2. Rename và Cast đúng kiểu dữ liệu cho ClickHouse
            col("data.s").alias("symbol"),
            col("data.e").alias("event_type"),
            
            # Thời gian: Chia 1000 để đổi từ ms sang seconds cho timestamp
            (col("data.E") / 1000).cast("timestamp").alias("event_time"),
            (col("data.O") / 1000).cast("timestamp").alias("open_time"),  
            (col("data.C") / 1000).cast("timestamp").alias("close_time"), 
            
            # Giá và Volume: Cast sang Double (Float64 trong ClickHouse)
            col("data.o").cast(DoubleType()).alias("open_price"),         # Chú ý: 'o' thường là Price
            col("data.h").cast(DoubleType()).alias("high_price"),
            col("data.l").cast(DoubleType()).alias("low_price"),
            col("data.c").cast(DoubleType()).alias("close_price"),
            col("data.v").cast(DoubleType()).alias("volume"),
            col("data.q").cast(DoubleType()).alias("quote_volume")
        )
        .filter(col("symbol").isNotNull())
        .filter(col("open_price") > 0)
        .filter(col("close_price") > 0)
        .filter(col("volume") >= 0)
        .filter(col("high_price") >= col("low_price"))
        .withColumn("Year", year(col("event_time")))
        .withColumn("Month", month(col("event_time")))
        .withColumn("Day", dayofmonth(col("event_time")))

    )

    return tickers_cleaned_df