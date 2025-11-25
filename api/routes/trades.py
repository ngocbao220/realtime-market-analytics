from fastapi import APIRouter
from services import trades_service

router = APIRouter(tags=["Trades"])

# Dùng để lấy realtime trades
@router.get("/dashboard/trades_realtime/{type}/{symbol}/{limit}")
def get_trades_realtime(symbol: str, type: str, limit: int):
    return trades_service.get_trades(symbol, mode="real_time", type=type, limit=limit)

# Dùng để lấy toàn bộ lịch sử trades
@router.get("/dashboard/trades_history/{type}/{symbol}")
def get_trades_history(symbol: str, type: str):
    return trades_service.get_trades(symbol, mode="history", type=type)
