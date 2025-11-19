from pyspark.sql.functions import (
    col, to_timestamp, from_json, year, month, dayofmonth, hour,
    lit, posexplode, when, sum as _sum, avg, stddev, count,
    window, lag, expr
)
from pyspark.sql.types import *
from schema.orderbook_schema import orderbook_schema 

def orderbook_transform(orderbook_raw_df):
    """
    Transform Orderbook:
    - Parse JSON dựa trên orderbook_schema
    - Tách mảng lồng nhau [[price, qty],...] thành 2 mảng phẳng: [price, price...] và [qty, qty...]
    - Ép kiểu String sang Double để tính toán sau này
    """
    
    orderbook_cleaned_df = (
        orderbook_raw_df
        .select(from_json(col("value").cast("string"), orderbook_schema).alias("data"))
        .filter(col("data").isNotNull())
        .select(
            col("data.s").alias("symbol"),
            # Chia 1000 vì timestamp của Binance là ms
            (col("data.E") / 1000).cast("timestamp").alias("event_time"),
            
            # --- Xử lý BIDS (Mua) ---
            # data.b là mảng các mảng. x[0] là giá, x[1] là lượng.
            # Dùng hàm expr của Spark SQL để transform mảng
            expr("transform(data.b, x -> cast(x[0] as double))").alias("bid_prices"),
            expr("transform(data.b, x -> cast(x[1] as double))").alias("bid_quantities"),
            
            # --- Xử lý ASKS (Bán) ---
            expr("transform(data.a, x -> cast(x[0] as double))").alias("ask_prices"),
            expr("transform(data.a, x -> cast(x[1] as double))").alias("ask_quantities")
        )
        .filter(col("symbol").isNotNull())
        .withColumn("Year", year(col("event_time")))
        .withColumn("Month", month(col("event_time")))
        .withColumn("Day", dayofmonth(col("event_time")))
    )

    return orderbook_cleaned_df