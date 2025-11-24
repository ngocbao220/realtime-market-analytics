from ..db import r
import logging

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def init_admin_account():
    """
    Khởi tạo tài khoản Admin (ID 0) nếu chưa có.
    Hàm này nên được gọi khi server khởi động.
    """
    admin_key = "user:0"
    if r.exists(admin_key):
        return

    logging.info("⚙️ Creating default Admin account...")
    admin_data = {
        "user_id": "0",
        "username": "admin",
        "role": "admin",
        "usd": 1000000000.0, # 1 Tỷ USD
        "btc": 1000.0,
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    }
    r.hset(admin_key, mapping=admin_data)

def create_new_user(username: str):
    username = username.strip()
    
    # 1. Kiểm tra username đã tồn tại chưa
    # (Dùng cách quét keys cũ để đảm bảo unique name, 
    # sau này tối ưu có thể dùng Set: usernames_taken)
    all_keys = r.keys("user:*")
    for key in all_keys:
        if key == "user_id_counter": continue
        try:
            # Chỉ lấy field username để check cho nhẹ
            stored_name = r.hget(key, "username")
            if stored_name == username:
                return r.hgetall(key)
        except: continue

    # 2. Tạo User mới
    new_id = r.incr("user_id_counter") 
    user_key = f"user:{new_id}"
    
    new_user_data = {
        "user_id": str(new_id),
        "username": username,
        "role": "user",
        "usd": 1000.0,   # Tặng 1000 USD
        "btc": 1.0,      # Tặng 1 BTC
        "reserved_usd": 0.0,
        "reserved_btc": 0.0
    }
    r.hset(user_key, mapping=new_user_data)
    return new_user_data

def get_user_balance(user_id: str):
    # Admin check
    user_key = f"user:{user_id}"
    
    # Nếu là admin nhưng chưa có trong DB thì init luôn
    if user_id == "0" and not r.exists(user_key):
        init_admin_account()

    if not r.exists(user_key):
        return None 
        
    data = r.hgetall(user_key)
    
    # Helper convert float an toàn
    def safe_float(val):
        try: return float(val)
        except: return 0.0

    data["usd"] = safe_float(data.get("usd"))
    data["btc"] = safe_float(data.get("btc"))
    data["reserved_usd"] = safe_float(data.get("reserved_usd"))
    data["reserved_btc"] = safe_float(data.get("reserved_btc"))
    
    return data

def get_all_users_logic():
    """
    Lấy danh sách user tối ưu bằng Pipeline
    """
    # 1. Lấy ID lớn nhất hiện tại
    max_id = r.get("user_id_counter")
    if not max_id:
        # Nếu chưa có user nào, thử check admin
        if r.exists("user:0"):
            max_id = 0
        else:
            return []
    
    max_id = int(max_id)
    
    # 2. Dùng Pipeline để gom lệnh (Tối ưu tốc độ)
    pipe = r.pipeline()
    # Quét từ 0 (Admin) đến max_id
    for i in range(0, max_id + 1):
        pipe.hgetall(f"user:{i}")
        
    results = pipe.execute()
    
    # 3. Xử lý kết quả
    users = []
    for data in results:
        if data and "user_id" in data:
            # Convert số liệu
            try: 
                data["usd"] = float(data.get("usd", 0))
                data["btc"] = float(data.get("btc", 0))
            except: pass
            users.append(data)
            
    return users

def delete_user_logic(user_id: str):
    # 1. Chặn xóa Admin
    if str(user_id) == "0":
        return {"success": False, "message": "Không thể xóa tài khoản Admin"}
    
    user_key = f"user:{user_id}"
    if not r.exists(user_key):
        return {"success": False, "message": "User không tồn tại"}
    
    try:
        # Xóa key user
        r.delete(user_key)
        # Nếu sau này có key balance riêng thì xóa thêm ở đây
        # r.delete(f"user:{user_id}:balance")
        return {"success": True, "message": f"Đã xóa user {user_id}"}
    except Exception as e:
        return {"success": False, "message": str(e)}