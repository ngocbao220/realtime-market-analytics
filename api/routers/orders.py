from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from services import order_service, trades_service, user_service
from schemas.models import PlaceOrderRequest
import asyncio
from config import SPEED_WEBSOCKET
router = APIRouter(prefix="/orders", tags=["Trading"])

@router.post("")
def place_order(order: PlaceOrderRequest):
    """
    Đặt lệnh mới (Mua hoặc Bán)
    Endpoint: POST /orders
    """
    try:
        # Gọi service logic (đã gộp chung buy/sell vào place_order)
        result = order_service.place_virtual_order(
            user_id=order.user_id,
            symbol=order.symbol,
            side=order.side,
            price=order.price,
            amount=order.amount
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/{order_id}")
def cancel_order(order_id: str, user_id: str):
    """
    Hủy lệnh
    Endpoint: DELETE /orders/{order_id}?user_id=...
    """
    # SỬA Ở ĐÂY: Truyền đủ user_id vào hàm service
    success = order_service.cancel_virtual_order(user_id, order_id) 
    
    # Kiểm tra kết quả trả về từ service (Service trả về dict {"success": True/False...})
    if not success or not success.get("success"):
        msg = success.get("msg") if success else "Unknown error"
        raise HTTPException(status_code=400, detail=msg)
        
    return {"message": "Order cancelled successfully"}

@router.get("/history/{user_id}")
def get_order_history_api(user_id: str):
    """
    Lấy lịch sử lệnh (Đã khớp/Hủy)
    Endpoint: GET /orders/history/{user_id}
    """
    return order_service.get_user_order_history(user_id)

@router.get("/trades/{user_id}")
def get_trade_history_api(user_id: str):
    """
    Lấy lịch sử khớp lệnh (My Trades)
    Endpoint: GET /orders/trades/{user_id}
    (Lưu ý: Bạn có thể đổi prefix router nếu muốn đúng chuẩn /users/{id}/trades)
    """
    return trades_service.get_user_trade_history(user_id)

@router.get("/{user_id}")
def get_my_orders(user_id: str):
     """
     Lấy danh sách lệnh đang chờ khớp của User
     Endpoint: GET /orders/{user_id}
     """
     return order_service.get_user_open_orders(user_id)


# --- PHẦN WEBSOCKET (Dùng để theo dõi lệnh Real-time) ---
@router.websocket("/ws/{user_id}")
async def websocket_my_orders(websocket: WebSocket, user_id: str):
    """
    WebSocket trả về danh sách lệnh mở (Open Orders) của User theo thời gian thực.
    URL kết nối: ws://localhost:8000/orders/ws/{user_id}
    """
    await websocket.accept()
    print(f"User {user_id} connected to orders WebSocket")
    
    try:
        while True:
            try:
                # 1. Lấy danh sách lệnh từ Service
                my_orders = order_service.get_user_open_orders(user_id)
                
                # 2. Xử lý nếu data là None (để tránh lỗi khi gửi JSON)
                if my_orders is None:
                    my_orders = []

                # 3. Gửi dữ liệu về Client
                await websocket.send_json(my_orders)

            except Exception as e:
                # Bắt lỗi logic bên trong vòng lặp để không bị ngắt kết nối
                print(f"Error fetching orders for user {user_id}: {e}")
                # Có thể gửi message lỗi về client nếu muốn
                # await websocket.send_json({"error": "Error fetching data"})

            # 4. Nghỉ 1 giây rồi mới cập nhật tiếp (tránh spam server)
            await asyncio.sleep(SPEED_WEBSOCKET)

    except WebSocketDisconnect:
        print(f"User {user_id} disconnected")
    except Exception as e:
        # Lỗi nghiêm trọng khác
        print(f"Critical Error: {e}")
        try:
            await websocket.close()
        except:
            pass

@router.websocket("/ws/history/{user_id}")
async def websocket_history(websocket: WebSocket, user_id: str):
    """
    WebSocket tổng hợp lịch sử:
    Trả về object: { "orders": [...], "trades": [...] }
    URL: ws://localhost:8000/orders/ws/history/{user_id}
    """
    await websocket.accept()
    # print(f"User {user_id} connected to History WS")
    
    try:
        while True:
            # 1. Lấy dữ liệu từ Redis
            # Order History: Lệnh đã kết thúc (Filled/Cancelled)
            order_hist = order_service.get_user_order_history(user_id, limit=20)
            
            # Trade History: Các lệnh khớp chi tiết
            trade_hist = trades_service.get_user_trade_history(user_id, limit=20)
            
            payload = {
                "orders": order_hist,
                "trades": trade_hist
            }

            # 2. Gửi về Client
            await websocket.send_json(payload)

            # 3. Nghỉ để tránh spam Redis (History ít thay đổi hơn Open Orders nên để delay cao hơn chút cũng được)
            await asyncio.sleep(2) 

    except WebSocketDisconnect:
        # print(f"User {user_id} disconnected History WS")
        pass
    except Exception as e:
        print(f"History WS Error: {e}")
        try:
            await websocket.close()
        except:
            pass

@router.websocket("/ws/admin/monitor")
async def websocket_admin_monitor(websocket: WebSocket):
    """
    WebSocket Monitor toàn hệ thống (Real-time).
    Tối ưu: Chỉ gửi 200 bản ghi mới nhất cho mỗi loại để tránh crash Client.
    URL: ws://localhost:8000/orders/ws/admin/monitor
    """
    await websocket.accept()
    print(f"⚡ Admin connected to Global Monitor: {websocket.client}")
    
    try:
        while True:
            # --- 1. RESET BIẾN (Quan trọng để không bị trùng lặp) ---
            global_open_orders = []
            global_history = []
            global_trades = []

            try:
                # --- 2. GOM DỮ LIỆU TỪ TẤT CẢ USER ---
                users = user_service.get_all_users_logic()
                
                for u in users:
                    uid = str(u.get("user_id"))
                    uname = u.get("username", "Unknown")

                    # --- A. Open Orders ---
                    # Lấy hết lệnh mở (vì số lượng này thường không quá lớn/user)
                    orders = order_service.get_user_open_orders(uid)
                    if orders:
                        for o in orders:
                            o["username"] = uname
                            o["user_id"] = uid
                            # Đảm bảo time là số để sort
                            o["time"] = float(o.get("time", 0))
                        global_open_orders.extend(orders)

                    # --- B. Order History ---
                    # Chỉ lấy 5-10 lệnh gần nhất mỗi user để giảm tải
                    hist = order_service.get_user_order_history(uid, limit=10)
                    if hist:
                        for h in hist:
                            h["username"] = uname
                            h["user_id"] = uid
                            h["time"] = float(h.get("time") or h.get("timestamp") or 0)
                        global_history.extend(hist)
                    
                    # --- C. Trade History ---
                    # Chỉ lấy 5-10 trade gần nhất mỗi user
                    trds = trades_service.get_user_trade_history(uid, limit=10)
                    if trds:
                        for t in trds:
                            t["username"] = uname
                            t["user_id"] = uid
                            
                            # Chuẩn hóa key (Backend làm sạch luôn cho Frontend nhàn)
                            # Ưu tiên lấy 'amount', nếu không có thì lấy 'quantity', 'qty'...
                            raw_amt = t.get("amount") or t.get("quantity") or t.get("qty") or 0
                            t["amount"] = float(raw_amt)
                            
                            raw_price = t.get("price") or t.get("Price") or 0
                            t["price"] = float(raw_price)
                            
                            raw_time = t.get("time") or t.get("timestamp") or t.get("TradeTime") or 0
                            t["time"] = float(raw_time)
                            
                        global_trades.extend(trds)

                # --- 3. SẮP XẾP & CẮT GỌN (QUAN TRỌNG) ---
                # Sắp xếp giảm dần theo thời gian (Mới nhất lên đầu)
                global_open_orders.sort(key=lambda x: x["time"], reverse=True)
                global_history.sort(key=lambda x: x["time"], reverse=True)
                global_trades.sort(key=lambda x: x["time"], reverse=True)

                # CẮT (SLICING): Chỉ giữ lại 200 bản ghi mới nhất toàn sàn
                # Nếu không cắt, Admin bật dashboard 1 lúc sẽ bị tràn RAM trình duyệt
                final_payload = {
                    "open_orders": global_open_orders[:200], 
                    "history": global_history[:200],
                    "trades": global_trades[:200]
                }
                
                # --- 4. GỬI DATA ---
                await websocket.send_json(final_payload)

            except Exception as e:
                print(f"⚠️ Global Monitor Processing Error: {str(e)}")
                # Vẫn gửi dữ liệu rỗng hoặc lỗi để frontend không bị treo loading
                # await websocket.send_json({"open_orders": [], "history": [], "trades": []})

            # --- 5. NGHỈ (Rate Limit) ---
            # 3 giây là hợp lý cho Admin Dashboard tổng quan
            await asyncio.sleep(3)

    except WebSocketDisconnect:
        print("❌ Admin Monitor disconnected")
    except Exception as e:
        print(f"❌ Critical WebSocket Error: {str(e)}")