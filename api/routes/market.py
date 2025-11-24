from fastapi import APIRouter
from ..services import market_service

router = APIRouter(tags=["Market"])

@router.get("/api/market/tickers")
def get_tickers():
    return market_service.get_tickers_logic()

# [UPDATE] Thêm limit vào query param, mặc định 20
@router.get("/api/trades/{symbol}")
def get_trades(symbol: str, limit: int = 20):
    return market_service.get_recent_trades_logic(symbol, limit)