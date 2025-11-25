import redis
import json
import os

# Cấu hình kết nối (Mặc định localhost nếu chạy từ máy ngoài)
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))

def inspect_redis():
    print(f"🔌 Đang kết nối tới Redis tại {REDIS_HOST}:{REDIS_PORT} (DB={REDIS_DB})...")
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        r.ping()
        print("✅ Kết nối thành công!\n")
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        return

    # Lấy tất cả key
    keys = r.keys("*")
    if not keys:
        print("⚠️ Redis đang TRỐNG RỖNG.")
        return

    print(f"🔍 Tìm thấy {len(keys)} keys. Đang phân tích dữ liệu mẫu...\n")
    print("="*60)

    # Phân loại và in mẫu dữ liệu
    for key in sorted(keys):
        key_type = r.type(key)
        print(f"🔑 Key: {key} | Type: {key_type}")
        
        try:
            if key_type == 'string':
                val = r.get(key)
                # Nếu là JSON thì format đẹp
                try:
                    json_val = json.loads(val)
                    print(f"   📄 Value (JSON): {json.dumps(json_val, indent=2, ensure_ascii=False)}")
                except:
                    print(f"   📄 Value: {val}")
            
            elif key_type == 'hash':
                val = r.hgetall(key)
                print(f"   🗂️ Value (Hash): {json.dumps(val, indent=2, ensure_ascii=False)}")
            
            elif key_type == 'list':
                # Lấy 2 phần tử đầu tiên làm mẫu
                items = r.lrange(key, 0, 1)
                print(f"   📚 List (Size: {r.llen(key)}):")
                for item in items:
                    print(f"      - {item}")
                if r.llen(key) > 2:
                    print("      - ... (còn nữa)")
            
            elif key_type == 'set':
                members = list(r.smembers(key))[:3]
                print(f"   Set: {members} ...")
            
            elif key_type == 'zset':
                members = r.zrange(key, 0, 2, withscores=True)
                print(f"   Sorted Set: {members} ...")
                
        except Exception as e:
            print(f"   ❌ Lỗi đọc value: {e}")
        
        print("-" * 60)

if __name__ == "__main__":
    inspect_redis()