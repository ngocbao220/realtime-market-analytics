from pyspark.sql.functions import (
    col, to_timestamp, from_json, year, month, dayofmonth, hour,
    lit, posexplode, when, sum as _sum, avg, stddev, count,
    window, lag, expr
)
from pyspark.sql.types import *
from schema.trade_schema import trade_schema

def transform_trades(trades_raw_df):
    """
    Transform raw trades DataFrame từ Kafka sang DataFrame đã clean và chuẩn hóa.

    Các cột sau khi transform:

    Symbol       : Mã coin/market (ví dụ 'BTCUSDT')
    TradeID      : ID của trade
    Price        : Giá của trade
    Quantity     : Số lượng trade
    EventTime    : Thời gian event (timestamp Binance gửi)
    TradeTime    : Thời gian thực hiện trade
    IsBuyerMaker : True nếu bên mua là maker, False nếu buyer là taker
    Side         : SELL/BUY dựa trên IsBuyerMaker
    TradeValue   : Giá trị trade = Price * Quantity
    Year         : Năm của TradeTime
    Month        : Tháng của TradeTime
    Day          : Ngày của TradeTime
    Hour         : Giờ của TradeTime
    """
    
    trades_cleaned_df = (
        trades_raw_df
        .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
        .filter(col("data").isNotNull())
        .select(
            col("data.s").alias("Symbol"),
            col("data.t").alias("TradeID"),
            col("data.p").cast(DoubleType()).alias("Price"),
            col("data.q").cast(DoubleType()).alias("Quantity"),
            (col("data.E") / 1000).cast("timestamp").alias("EventTime"),
            (col("data.T") / 1000).cast("timestamp").alias("TradeTime"),
            #clickhouse nhận  (0/1) nên phải cast về int
            col("data.m").cast("int").alias("IsBuyerMaker")
        )
        .withColumn("Side", when(col("IsBuyerMaker") == 1, "SELL").otherwise("BUY"))
        .withColumn("TradeValue", col("Price") * col("Quantity"))
        .filter(col("Price") > 0)
        .filter(col("Quantity") > 0)
        .withColumn("Year", year(col("TradeTime")))
        .withColumn("Month", month(col("TradeTime")))
        .withColumn("Day", dayofmonth(col("TradeTime")))
        .withColumn("Hour", hour(col("TradeTime")))
    )

    return trades_cleaned_df
