import redis
import os

# Cấu hình (khớp với backend của bạn)
REDIS_HOST = "localhost" 
REDIS_PORT = 6379
REDIS_DB = 0

try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
    
    print(f"🔌 Đang kết nối tới Redis {REDIS_HOST}:{REDIS_PORT}...")
    
    # 1. Lấy tất cả các key
    keys = r.keys("*")
    print(f"🔑 Tổng số Key tìm thấy: {len(keys)}")
    
    if len(keys) == 0:
        print("⚠️  Redis đang TRỐNG RỖNG! Chưa có dữ liệu nào được đẩy vào.")
    else:
        print("📋 Danh sách các Key hiện có:")
        for k in keys:
            key_type = r.type(k)
            print(f" - Tên: {k} | Loại: {key_type}")
            
            # Nếu là List (dạng kline mong đợi), in thử 1 dòng
            if key_type == 'list':
                val = r.lrange(k, 0, 0)
                print(f"   Example data: {val}")
            # Nếu là String (có thể Spark lưu dạng json string?)
            elif key_type == 'string':
                val = r.get(k)
                print(f"   Example data: {val}")

except Exception as e:
    print(f"❌ Lỗi kết nối: {e}")