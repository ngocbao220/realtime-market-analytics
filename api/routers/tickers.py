from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from services import ticker_service
import asyncio

router = APIRouter(tags=["Tickers"])

# Dùng để hiển thị OCLH trong 24h
#@router.get("/tickers")
#def get_tickers():
#    return ticker_service.get_tickers()
@router.websocket("/ws/tickers")
async def websocket_tickers(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # lấy dữ liệu ticker mới nhất từ Redis
            tickers = ticker_service.get_tickers()
            await websocket.send_json(tickers)
            await asyncio.sleep(1)  # Gửi dữ liệu mỗi giây
    except WebSocketDisconnect:
        print("Client disconnected from tickers")
