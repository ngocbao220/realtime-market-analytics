from fastapi import APIRouter, HTTPException
from schemas.models import OrderRequest
from services import orderbook_service

router = APIRouter(tags=["Order"])

@router.post("/orders/{side}")
def place_order(side: str, order: OrderRequest):
    """
    Đặt lệnh Mua (Buy) hoặc Bán (Sell)
    """
    try:
        result = orderbook_service.place_order_logic(order.user_id, side, order.price, order.amount)
        if result["status"] == "failed":
            # Trả về lỗi 400 nếu logic thất bại (vd: không đủ tiền)
            raise HTTPException(status_code=400, detail=result.get("detail", "Order failed"))
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/api/orderbook/{symbol}")
def get_orderbook(symbol: str):
    """
    Lấy orderbook snapshot mới nhất
    Dùng để hiển thị bid/ask depth
    """
    try:
        return orderbook_service.get_orderbook_data(symbol)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))