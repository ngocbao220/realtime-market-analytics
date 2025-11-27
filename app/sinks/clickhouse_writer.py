from pyspark.sql import DataFrame

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

    if batch_df.isEmpty():
        return

    def process_partition(iterator):
        # Tạo client mới TẠI WORKER
        from clickhouse_driver import Client  # import trong worker
        client = Client(
            host=host, port=port, user=user, password=password, database=database
        )

        rows = [row.asDict() for row in iterator]

        if rows:
            keys = rows[0].keys()
            columns_str = ", ".join(keys)
            client.execute(f'INSERT INTO {table_name} ({columns_str}) VALUES', rows)

        client.disconnect()

    # Gọi foreachPartition
    batch_df.foreachPartition(process_partition)
    print(f"[Batch {batch_id}] Write to ClickHouse Success.")
