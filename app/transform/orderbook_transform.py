from pyspark.sql.functions import (
    col, to_timestamp, from_json, year, month, dayofmonth, hour,
    lit, posexplode, when, sum as _sum, avg, stddev, count,
    window, lag, expr
)
from pyspark.sql.types import *
from schema.orderbook_schema import orderbook_schema 
from pyspark.sql.functions import from_utc_timestamp

def transform_orderbook(orderbook_raw_df):
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
            col("data.s").alias("Symbol"),
                from_utc_timestamp((col("data.E") / 1000).cast("timestamp"), "Asia/Ho_Chi_Minh").alias("Event_time"),
            
            # --- Xử lý BIDS (Mua) ---
            # data.b là mảng các mảng. x[0] là giá, x[1] là lượng.
            # Dùng hàm expr của Spark SQL để transform mảng
            expr("transform(data.b, x -> cast(x[0] as double))").alias("Bid_prices"),
            expr("transform(data.b, x -> cast(x[1] as double))").alias("Bid_quantities"),
            
            # --- Xử lý ASKS (Bán) ---
            expr("transform(data.a, x -> cast(x[0] as double))").alias("Ask_prices"),
            expr("transform(data.a, x -> cast(x[1] as double))").alias("Ask_quantities")
        )
        .filter(col("Symbol").isNotNull())
        # FIX: Chấp nhận orderbook có bid HOẶC ask (không yêu cầu cả 2)
        .filter(expr("size(Bid_prices) > 0 OR size(Ask_prices) > 0"))
        .withColumn("Year", year(col("Event_time")))
        .withColumn("Month", month(col("Event_time")))
        .withColumn("Day", dayofmonth(col("Event_time")))
    )

    return orderbook_cleaned_df