from pyspark.sql.types import *

# Schema khớp với Raw JSON của Binance
# Tài liệu: https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams#klinecandlestick-streams-for-utc
kline_schema = StructType([
    StructField("e", StringType()),  # Event type
    StructField("E", LongType()),    # Event time
    StructField("s", StringType()),  # Symbol
    StructField("k", StructType([    # Kline object
        StructField("t", LongType()),    # Kline start time
        StructField("T", LongType()),    # Kline close time
        StructField("s", StringType()),  # Symbol
        StructField("i", StringType()),  # Interval
        StructField("f", LongType()),    # First trade ID
        StructField("L", LongType()),    # Last trade ID
        StructField("o", StringType()),  # Open price
        StructField("c", StringType()),  # Close price
        StructField("h", StringType()),  # High price
        StructField("l", StringType()),  # Low price
        StructField("v", StringType()),  # Base asset volume
        StructField("n", LongType()),    # Number of trades
        StructField("x", BooleanType()), # Is this kline closed?
        StructField("q", StringType()),  # Quote asset volume
        StructField("V", StringType()),  # Taker buy base asset volume
        StructField("Q", StringType()),  # Taker buy quote asset volume
        StructField("B", StringType())   # Ignore
    ]))
])