from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from services import kline_service
import asyncio
router = APIRouter(tags=["Kline"])

# Dùng để vẽ biểu đồ lịch sử giá + biểu đồ nến
#@router.get("/klines/{symbol}")
#def get_kline(symbol: str, interval: str = "1m", limit: int = 500):
#    return {"data": kline_service.get_kline_hybrid_logic(symbol, interval, limit)}
@router.websocket("/ws/klines/{symbol}")
async def websocket_klines(
    websocket: WebSocket,
    symbol: str,
    interval: str = "1m",
    limit: int = 500
):
    await websocket.accept()
    try:
        while True:
            # lấy dữ liệu kline mới nhất từ Redis
            klines = kline_service.get_kline_hybrid_logic(symbol, interval, limit)
            await websocket.send_json(klines)
            await asyncio.sleep(1)  # Gửi dữ liệu mỗi giây
    except WebSocketDisconnect:
        print(f"Client disconnected from klines {symbol}")