import requests
import os
import logging
from typing import Optional, Dict, Any, List, Tuple

# --- CẤU HÌNH ---
# Nên đưa vào class, nhưng để global biến này cũng ổn
API_BASE_URL = os.getenv("API_URL", "http://localhost:8000")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - API - %(levelname)s - %(message)s")

class ExchangeAPI:
    """
    Client chuyên dụng để gọi API sàn giao dịch.
    Sử dụng Session để tái sử dụng kết nối TCP -> Tăng tốc độ gấp 5 lần.
    """
    def __init__(self, base_url=API_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session() # Quan trọng: Keep-Alive connection
        
        # Cấu hình retry đơn giản (nếu mạng chập chờn)
        adapter = requests.adapters.HTTPAdapter(max_retries=3)
        self.session.mount('http://', adapter)

    def _req(self, method: str, endpoint: str, params: dict = None, json_data: dict = None) -> Optional[Any]:
        """Hàm nội bộ xử lý request an toàn"""
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(method, url, params=params, json=json_data, timeout=3)
            
            # Nếu status code >= 400, raise lỗi để vào except
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            # Lỗi từ Server trả về (VD: 400 Bad Request, 404 Not Found)
            try:
                error_detail = response.json().get("detail", str(e))
            except:
                error_detail = str(e)
            logging.warning(f"⚠️ API Fail {endpoint}: {error_detail}")
            return None
            
        except Exception as e:
            # Lỗi kết nối, timeout...
            logging.error(f"❌ Connection Error {endpoint}: {e}")
            return None

    # ==========================================
    # 1. USER & AUTH
    # ==========================================
    
    def login_or_register(self, username: str) -> Optional[Dict]:
        """
        API: POST /users
        """
        # [SỬA LỖI]: Router prefix là /users nên endpoint phải là /users, không phải /
        return self._req("POST", "/users", json_data={"username": username})

    def get_user_info(self, user_id: str) -> Optional[Dict]:
        """
        API: GET /users/{user_id}
        """
        return self._req("GET", f"/users/{user_id}")

    def get_all_users(self) -> List[Dict]:
        """
        API: GET /users
        """
        return self._req("GET", "/users") or []

    def delete_user(self, user_id: str) -> Tuple[bool, str]:
        """
        API: DELETE /users/{user_id}
        """
        res = self._req("DELETE", f"/users/{user_id}")
        if res and res.get("success"):
            return True, res.get("message", "Đã xóa")
        # Nếu _req trả về None (lỗi) hoặc success=False
        return False, "Lỗi khi xóa user"

    # ==========================================
    # 2. MARKET DATA
    # ==========================================

    def get_kline(self, symbol, interval, limit=30):
        data = self._req("GET", f"/klines/{symbol}", {"interval": interval, "limit": limit})
        return data.get("data", []) if data else []

    def get_tickers(self):
        return self._req("GET", "/tickers") or []

    def get_orderbook(self, symbol):
        return self._req("GET", f"/market/orderbook/{symbol}") or {"bids": [], "asks": []}

    def get_recent_trades(self, symbol, type="real", limit=20):
        return self._req("GET", f"/market/trades/{symbol}", {"limit": limit, "type": type}) or []

    def get_orderbook(self, symbol: str, type="real", side="both") -> Dict:
        """
        API: GET /market/orderbook/{symbol}?type=...&side=...
        """
        params = {"type": type, "side": side}
        # Trả về mặc định cấu trúc rỗng để frontend không bị crash
        return self._req("GET", f"/market/orderbook/{symbol}", params=params) or {"bids": [], "asks": []}

    def get_trades(self, symbol: str, type="real", limit=50) -> List[Dict]:
        """
        API: GET /market/trades/{symbol}
        """
        params = {"type": type, "limit": limit}
        return self._req("GET", f"/market/trades/{symbol}", params=params) or []

    # ==========================================
    # 3. TRADING (ORDERS)
    # ==========================================

    def place_order(self, user_id, symbol, side, price, amount) -> Tuple[bool, str]:
        """
        API: POST /orders
        """
        payload = {
            "user_id": str(user_id),
            "symbol": str(symbol),
            "side": str(side),
            "price": float(price),
            "amount": float(amount)
        }
        # Hàm _req trả về Dict hoặc None
        res = self._req("POST", "/orders", json_data=payload)
        
        if res and res.get("success"):
            return True, res.get("msg", "Đặt lệnh thành công")
        
        # Lấy chi tiết lỗi nếu có
        error_msg = "Lỗi kết nối"
        if res and "detail" in res: error_msg = res["detail"] # Lỗi do FastAPI catch
        if res and "msg" in res: error_msg = res["msg"]       # Lỗi do logic trả về
            
        return False, error_msg

    def cancel_order(self, order_id, user_id) -> Tuple[bool, str]:
        """
        API: DELETE /orders/{order_id}?user_id=...
        """
        # Backend dùng Query Param cho user_id
        params = {"user_id": user_id} 
        res = self._req("DELETE", f"/orders/{order_id}", params=params)
        
        if res and res.get("message"): # Backend trả về {"message": "Order cancelled..."}
            return True, res.get("message")
            
        return False, "Không thể hủy lệnh"
    
    def get_open_orders(self, user_id):
        """Lấy danh sách lệnh chờ của user"""
        data = self._req("GET", f"/orders/{user_id}")
        return data if isinstance(data, list) else []

    def cancel_order(self, order_id, user_id):
        result = self._req("DELETE", f"/orders/{order_id}", params={"user_id": user_id})
        if result and result.get("status") == "success":
            return True, result.get("message")
        return False, result.get("detail") if result else "Lỗi kết nối"


# --- KHỞI TẠO CLIENT ĐỂ DÙNG CHUNG ---
api = ExchangeAPI()