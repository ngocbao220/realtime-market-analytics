from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from services import order_service 

router = APIRouter(prefix="/orders", tags=["Trading"])

# Schema request body chuẩn cho đặt lệnh
class PlaceOrderRequest(BaseModel):
    user_id: str
    symbol: str
    side: str = Field(..., description="buy or sell") # 'buy' hoặc 'sell'
    price: float
    amount: float

@router.post("/")
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
    # Cần logic lấy symbol/side từ order_id hoặc truyền thêm nếu service yêu cầu
    # Ở đây giả sử service tự tra cứu được từ order_id
    success = order_service.cancel_order(order_id) 
    if not success:
        raise HTTPException(status_code=404, detail="Order not found or cannot be cancelled")
    return {"message": "Order cancelled successfully"}

@router.get("/user/{user_id}")
def get_my_orders(user_id: str):
    """
    Lấy danh sách lệnh đang chờ khớp của User
    Endpoint: GET /orders/user/{user_id}
    """
    return order_service.get_user_open_orders(user_id)