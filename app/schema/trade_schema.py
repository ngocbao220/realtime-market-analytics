from pyspark.sql.types import *

trade_schema = StructType([
    StructField("e", StringType()),
    StructField("E", LongType()),
    StructField("s", StringType()),
    StructField("t", LongType()),
    StructField("p", StringType()),
    StructField("q", StringType()),
    StructField("b", LongType()),
    StructField("a", LongType()),
    StructField("T", LongType()),
    StructField("m", BooleanType()),
    StructField("M", BooleanType())
])