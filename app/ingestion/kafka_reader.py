from pyspark.sql import SparkSession

def read_kafka_stream(spark: SparkSession, kafka_broker: str, topic: str):
    """
    Đọc dữ liệu streaming từ Kafka.

    Tham số:
    ----------
    spark        : SparkSession
    kafka_broker : địa chỉ Kafka broker, ví dụ "kafka:9092"
    topic        : tên topic Kafka

    Trả về:
    -------
    DataFrame streaming với schema mặc định Kafka:
      - key   : binary
      - value : binary
      - topic : string
      - partition : int
      - offset    : long
      - timestamp : timestamp
      - timestampType : int

    Lưu ý:
    -------
    - failOnDataLoss=False: cho phép bỏ qua dữ liệu đã bị xóa/không còn trên Kafka,
      tránh streaming query crash khi offset bị reset hoặc dữ liệu cũ đã bị xóa.
    - startingOffsets="earliest": đọc từ offset đầu tiên nếu chưa có checkpoint.
    """

    df_raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")   # "earliest" hoặc "latest"
        .option("failOnDataLoss", "false")       # quan trọng: không fail nếu mất data
        .option("maxOffsetsPerTrigger", "5000")  # giới hạn số bản ghi mỗi trigger
        .load()
    )

    return df_raw
