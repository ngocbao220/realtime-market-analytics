from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from services.matching_engine.order_service import place_order, get_orderbook, cancel_order

router = APIRouter()

# Schema dữ liệu gửi lên
class OrderRequest(BaseModel):
    user_id: str
    price: float
    amount: float

# --- API ĐẶT LỆNH MUA ---
@router.post("/buy")
def api_buy(req: OrderRequest):
    # Có thể thêm logic trừ tiền USD của user ở đây
    return place_order(req.user_id, "buy", req.price, req.amount)

# --- API ĐẶT LỆNH BÁN ---
@router.post("/sell")
def api_sell(req: OrderRequest):
    # Có thể thêm logic trừ coin BTC của user ở đây
    return place_order(req.user_id, "sell", req.price, req.amount)

# --- API LẤY ORDERBOOK ---
@router.get("/book/{side}")
def api_get_book(side: str):
    """side là 'buy' hoặc 'sell'"""
    if side not in ["buy", "sell"]:
        raise HTTPException(status_code=400, detail="Side must be 'buy' or 'sell'")
    
    return get_orderbook(side)

# --- API HỦY LỆNH ---
@router.delete("/{order_id}")
def api_cancel(order_id: str):
    success = cancel_order(order_id)
    if not success:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"message": "Order cancelled"}