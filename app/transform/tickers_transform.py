from pyspark.sql.functions import (
    col, to_timestamp, from_json, year, month, dayofmonth, hour,
    lit, posexplode, when, sum as _sum, avg, stddev, count,
    window, lag, expr
)
from pyspark.sql.types import *
from schema.ticker_schema import ticker_schema 
from pyspark.sql.functions import from_utc_timestamp

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
            col("data.s").alias("Symbol"),
            col("data.e").alias("Event_type"),
            
            # Thời gian: Chia 1000 để đổi từ ms sang seconds cho timestamp
            from_utc_timestamp((col("data.E") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Event_time"),
            from_utc_timestamp((col("data.O") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Open_time"),  
            from_utc_timestamp((col("data.C") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Close_time"), 
            
            # Giá và Volume: Cast sang Double (Float64 trong ClickHouse)
            col("data.o").cast(DoubleType()).alias("Open_price"),         # Chú ý: 'o' thường là Price
            col("data.h").cast(DoubleType()).alias("High_price"),
            col("data.l").cast(DoubleType()).alias("Low_price"),
            col("data.c").cast(DoubleType()).alias("Close_price"),
            col("data.v").cast(DoubleType()).alias("Volume"),
            col("data.q").cast(DoubleType()).alias("Quote_volume")
        )
        .filter(col("Symbol").isNotNull())
        .filter(col("Open_price") > 0)
        .filter(col("Close_price") > 0)
        .filter(col("Volume") >= 0)
        .filter(col("High_price") >= col("Low_price"))
        .withColumn("Year", year(col("Event_time")))
        .withColumn("Month", month(col("Event_time")))
        .withColumn("Day", dayofmonth(col("Event_time")))

    )

    return tickers_cleaned_df