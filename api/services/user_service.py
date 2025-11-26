from db import redis_client
import logging
import time
from datetime import datetime, timezone, timedelta

INF_USD = 9999999999999999.0
INF_COIN = 999999.0

# --- CÁC KEY REDIS ---
KEY_USER_PROFILE = "user:{}:profile"   # Hash: user:{id}:profile
KEY_USER_BALANCE = "user:{}:balance"   # Hash: user:{id}:balance
KEY_USER_ID_SEQ = "sys:user_id_seq"    # String: Bộ đếm ID
KEY_USERNAME_LOOKUP = "sys:usernames"  # Hash: username -> user_id
KEY_ACTIVE_USERS = "sys:active_users"  # Set: user_id

VN_TZ = timezone(timedelta(hours=7))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def safe_float(val):
    try: return float(val)
    except: return 0.0

def init_special_account():
    """
    Khởi tạo Admin (0), Bots (1,2), System (3).
    """
    if redis_client.hexists(KEY_USERNAME_LOOKUP, "admin"):
        return

    print("⚙️ Initializing special accounts...")
    pipe = redis_client.pipeline()

    # Danh sách tài khoản đặc biệt
    special_accounts = [
        (0, "admin", "admin"),
        (1, "marker_bot", "bot"),
        (2, "taker_bot", "bot"),
        (3, "system", "system") # Dùng để đối ứng khi khớp Real Data
    ]

    for uid, name, role in special_accounts:
        # 1. Profile
        pipe.hset(KEY_USER_PROFILE.format(uid), mapping={
            "user_id": str(uid), "username": name, "role": role, 
            "created_at": int(time.time())
        })
        # 2. Balance (Vô tận)
        pipe.hset(KEY_USER_BALANCE.format(uid), mapping={
            "usd": INF_USD, "btc": INF_COIN, "reserved_usd": 0.0, "reserved_btc": 0.0
        })
        # 3. Index
        pipe.hset(KEY_USERNAME_LOOKUP, name, str(uid))
        pipe.sadd(KEY_ACTIVE_USERS, str(uid))

    pipe.set(KEY_USER_ID_SEQ, 3)
    pipe.execute()
    logging.info("✅ Special accounts created successfully.")

def create_new_user(username: str):
    """
    Tạo user mới: Tách Profile và Balance.
    """
    username = username.strip()
    existing_id = redis_client.hget(KEY_USERNAME_LOOKUP, username)
    if existing_id:
        return get_user_info(existing_id)

    new_id = redis_client.incr(KEY_USER_ID_SEQ)
    
    pipe = redis_client.pipeline()
    # 1. Profile
    pipe.hset(KEY_USER_PROFILE.format(new_id), mapping={
        "user_id": str(new_id),
        "username": username,
        "role": "user",
        "created_at": int(time.time())
    })
    # 2. Balance (Tặng 1000 USD demo)
    pipe.hset(KEY_USER_BALANCE.format(new_id), mapping={
        "usd": 1000.0, "btc": 1.0, "reserved_usd": 0.0, "reserved_btc": 0.0
    })
    # 3. Index
    pipe.hset(KEY_USERNAME_LOOKUP, username, new_id)
    pipe.sadd(KEY_ACTIVE_USERS, new_id)
    pipe.execute()
    
    logging.info(f"👤 Created new user: {username} (ID: {new_id})")
    return get_user_info(new_id)

def get_user_info(user_id: str):
    """
    Lấy thông tin tổng hợp (Profile + Balance)
    """
    pipe = redis_client.pipeline()
    pipe.hgetall(KEY_USER_PROFILE.format(user_id))
    pipe.hgetall(KEY_USER_BALANCE.format(user_id))
    profile, balance = pipe.execute()
    
    if not profile: return None
    
    # Merge data lại để trả về frontend cho tiện
    result = profile.copy()
    if balance:
        result["usd"] = safe_float(balance.get("usd"))
        result["btc"] = safe_float(balance.get("btc"))
        result["reserved_usd"] = safe_float(balance.get("reserved_usd"))
        result["reserved_btc"] = safe_float(balance.get("reserved_btc"))
    else:
        result.update({"usd": 0, "btc": 0, "reserved_usd": 0, "reserved_btc": 0})
        
    return result

def get_all_users_logic():
    """
    Lấy danh sách tất cả user.
    Logic mới: Phải lấy cả Profile (Hash) và Balance (Hash) rồi gộp lại.
    """
    # 1. Lấy danh sách ID đang hoạt động
    active_ids = redis_client.smembers(KEY_ACTIVE_USERS)
    
    if not active_ids:
        return []
    
    # Sắp xếp ID từ nhỏ đến lớn để pipeline chạy theo thứ tự
    # (Chuyển sang int để sort cho đúng: 1, 2, 10 thay vì 1, 10, 2)
    sorted_ids = sorted(list(active_ids), key=lambda x: int(x))

    # 2. Pipeline lấy data (Lấy Profile trước, Balance sau xen kẽ)
    pipe = redis_client.pipeline()
    for uid in sorted_ids:
        pipe.hgetall(KEY_USER_PROFILE.format(uid))
        pipe.hgetall(KEY_USER_BALANCE.format(uid))
    
    results = pipe.execute()
    
    # 3. Format dữ liệu
    # Results sẽ có dạng: [Profile_1, Balance_1, Profile_2, Balance_2, ...]
    users = []
    
    # Bước nhảy là 2 vì mỗi user chiếm 2 slot trong results
    for i in range(0, len(results), 2):
        profile = results[i]
        balance = results[i+1]
        
        # Chỉ xử lý nếu lấy được profile hợp lệ
        if profile and "user_id" in profile:
            # Merge data: Profile + Balance
            user_data = profile.copy()
            
            if balance:
                user_data["usd"] = safe_float(balance.get("usd"))
                user_data["btc"] = safe_float(balance.get("btc"))
                # (Optional) Có thể ẩn reserved nếu không muốn hiện ở danh sách tổng
                # user_data["reserved_usd"] = safe_float(balance.get("reserved_usd"))
                # user_data["reserved_btc"] = safe_float(balance.get("reserved_btc"))
            else:
                # Fallback nếu không có balance (tránh lỗi None)
                user_data["usd"] = 0.0
                user_data["btc"] = 0.0
            
            users.append(user_data)
            
    return users


def delete_user_logic(user_id: str):
    """
    Xóa user và dọn dẹp các index liên quan.
    Logic mới: Phải xóa cả KEY_USER_PROFILE và KEY_USER_BALANCE.
    """
    # 1. Chặn xóa các tài khoản đặc biệt (0: Admin, 1: Marker, 2: Taker, 3: System)
    if str(user_id) in ["0", "1", "2", "3"]:
        return {"success": False, "message": "Không thể xóa tài khoản Hệ thống (Admin/Bot/System)"}
    
    # Xác định các Key cần xóa
    profile_key = KEY_USER_PROFILE.format(user_id)
    balance_key = KEY_USER_BALANCE.format(user_id)
    
    # Lấy username trước để còn xóa trong bảng Lookup (Index)
    username = redis_client.hget(profile_key, "username")
    
    if not username:
        return {"success": False, "message": "User không tồn tại"}
    
    try:
        pipe = redis_client.pipeline()
        
        # A. Xóa Data
        pipe.delete(profile_key)   # Xóa Profile
        pipe.delete(balance_key)   # Xóa Balance
        
        # B. Xóa Index
        pipe.hdel(KEY_USERNAME_LOOKUP, username)  # Xóa khỏi bảng check trùng username
        pipe.srem(KEY_ACTIVE_USERS, user_id)      # Xóa khỏi danh sách active
        
        # (Optional) Xóa thêm danh sách lệnh đang mở và lịch sử trade nếu cần sạch sẽ tuyệt đối
        # pipe.delete(f"user:{user_id}:open_orders")
        # pipe.delete(f"user:{user_id}:trades")
        
        pipe.execute()
        
        logging.info(f"🗑️ Deleted user {user_id} ({username})")
        return {"success": True, "message": f"Đã xóa user {user_id}"}
        
    except Exception as e:
        logging.error(f"Error deleting user {user_id}: {e}")
        return {"success": False, "message": str(e)}