from pyspark.sql.types import *

ticker_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("O", LongType()),
    StructField("C", LongType()),
    StructField("o", StringType()),
    StructField("h", StringType()),
    StructField("l", StringType()),
    StructField("c", StringType()),
    StructField("v", StringType()),
    StructField("q", StringType())
])