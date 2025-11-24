from fastapi import APIRouter
from ..services import kline_service

router = APIRouter(tags=["Kline"])

# [UPDATE] Thêm limit vào query param, mặc định 500
@router.get("/api/kline/{symbol}")
def get_kline(symbol: str, interval: str = "1m", limit: int = 500):
    return {"data": kline_service.get_kline_hybrid_logic(symbol, interval, limit)}