from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from services import order_service 
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