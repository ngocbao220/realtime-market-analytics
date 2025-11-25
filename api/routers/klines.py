from fastapi import APIRouter
from services import kline_service

router = APIRouter(tags=["Kline"])

# Dùng để vẽ biểu đồ lịch sử giá + biểu đồ nến
@router.get("/kline/get/{symbol}")
def get_kline(symbol: str, interval: str = "1m", limit: int = 500):
    return {"data": kline_service.get_kline_hybrid_logic(symbol, interval, limit)}