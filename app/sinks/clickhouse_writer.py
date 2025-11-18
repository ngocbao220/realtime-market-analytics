from pyspark.sql import DataFrame
from clickhouse_driver import Client

def write_clickhouse_batch(
    batch_df: DataFrame, 
    batch_id: int,
    table_name: str, 
    host: str, 
    port: int,
    user: str, 
    password: str,
    database: str
) -> None:
    """
    Ghi từng micro-batch của streaming DataFrame vào ClickHouse (TCP, driver).
    """

    try:
        count = batch_df.count()
        if count == 0:
            print(f"[Batch {batch_id}] Empty batch, skipping...")
            return

        print(f"[Batch {batch_id}] Processing {count} rows")
        batch_df.show(3, truncate=False)

        # Chuyển Spark DataFrame sang Pandas để insert
        pdf = batch_df.toPandas()
        if pdf.empty:
            return

        # Khởi tạo client với database
        client = Client(
            host=host,
            port=port,      # TCP port, mặc định 9000
            user=user,
            password=password,
            database=database
        )

        # Chuyển DataFrame thành list of dict
        records = pdf.to_dict(orient='records')

        # Chuẩn bị danh sách column
        columns = ", ".join(pdf.columns)

        # Dùng execute để insert
        client.execute(f'INSERT INTO {table_name} ({columns}) VALUES', records)

        print(f"[Batch {batch_id}] Inserted {len(records)} rows into {database}.{table_name}")

    except Exception as e:
        print(f"[Batch {batch_id}] Error writing to ClickHouse: {e}", flush=True)
