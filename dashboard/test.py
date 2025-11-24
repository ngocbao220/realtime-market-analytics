from clickhouse_driver import Client
import os

# Cấu hình (Sửa lại nếu password/host khác)
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "12345" 

try:
    client = Client(host=CLICKHOUSE_HOST, port=9000, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD, database='default')
    
    print("===== 🕵️ BÁO CÁO ĐIỀU TRA DỮ LIỆU =====\n")
    
    # 1. Kiểm tra tổng số dòng
    total_rows = client.execute("SELECT count(*) FROM klines")[0][0]
    print(f"1️⃣  TỔNG SỐ DÒNG TRONG CLICKHOUSE: {total_rows}")
    
    if total_rows == 0:
        print("   ⚠️  CẢNH BÁO: Bảng 'klines' trống trơn! Spark chưa ghi được gì cả.")
    else:
        # 2. Kiểm tra phân bố theo Symbol
        print("\n2️⃣  PHÂN BỐ DỮ LIỆU THEO CẶP TIỀN:")
        rows = client.execute("SELECT Symbol, Interval, count(*) FROM klines GROUP BY Symbol, Interval")
        for r in rows:
            print(f"   - {r[0]} ({r[1]}): {r[2]} cây nến")
            
        # 3. Soi 5 dòng cũ nhất và mới nhất của BTCUSDT
        print("\n3️⃣  KIỂM TRA THỜI GIAN (BTCUSDT - 1m):")
        last_5 = client.execute("SELECT Open_time FROM klines WHERE Symbol='BTCUSDT' AND Interval='1m' ORDER BY Open_time DESC LIMIT 5")
        first_5 = client.execute("SELECT Open_time FROM klines WHERE Symbol='BTCUSDT' AND Interval='1m' ORDER BY Open_time ASC LIMIT 5")
        
        print("   👉 5 nến MỚI NHẤT:", [str(r[0]) for r in last_5])
        print("   👉 5 nến CŨ NHẤT: ", [str(r[0]) for r in first_5])

except Exception as e:
    print(f"❌ Lỗi kết nối ClickHouse: {e}")