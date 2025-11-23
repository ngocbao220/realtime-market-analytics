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
    Ghi micro-batch vào ClickHouse theo cách phân tán (Worker nodes).
    """
    
    # Check nhanh xem batch có rỗng không (Metadata check)
    if batch_df.isEmpty():
        return

    # --- HÀM XỬ LÝ TRÊN TỪNG WORKER ---
    def process_partition(iterator):
        # 1. Tạo kết nối ClickHouse TẠI WORKER
        # Lưu ý: Client này mở kết nối TCP cực nhanh
        client = Client(
            host=host, port=port, 
            user=user, password=password, 
            database=database,
            settings={'use_numpy': True} # Tối ưu nếu có cài numpy
        )
        
        # 2. Chuẩn bị dữ liệu
        # Convert Iterator[Row] thành List[Dict] hoặc List[Tuple]
        # ClickHouse Driver insert nhanh nhất với List of Tuples hoặc List of Dicts
        rows = []
        for row in iterator:
            # Convert Row object sang Dict
            r_dict = row.asDict()
            rows.append(r_dict)
            
        if rows:
            try:
                # 3. Thực hiện Bulk Insert
                # Lấy tên cột từ dòng đầu tiên để map đúng field
                keys = rows[0].keys()
                columns_str = ", ".join(keys)
                
                # INSERT INTO table (col1, col2) VALUES ...
                # client.execute tự động xử lý list of dicts
                client.execute(
                    f'INSERT INTO {table_name} ({columns_str}) VALUES', 
                    rows
                )
            except Exception as e:
                # Log lỗi cụ thể tại worker (sẽ hiện trong executor logs)
                print(f"Error inserting partition to CH: {e}")
                raise e # Raise để Spark biết batch này fail
        
        # Đóng kết nối (Client tự quản lý nhưng disconnect cho chắc)
        client.disconnect()

    # --- KÍCH HOẠT GHI PHÂN TÁN ---
    try:
        # foreachPartition sẽ đẩy hàm process_partition xuống các Executor chạy song song
        batch_df.foreachPartition(process_partition)
        print(f"[Batch {batch_id}] Write to ClickHouse Success.")
    except Exception as e:
        print(f"[Batch {batch_id}] Failed to write ClickHouse: {e}")