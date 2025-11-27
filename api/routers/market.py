from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect
from services import orderbook_service, trades_service
import asyncio
router = APIRouter(prefix="/market", tags=["Market Data"])

#@router.get("/orderbook/{symbol}")
#def get_orderbook(
#    symbol: str, 
#    type: str = Query("real", enum=["real", "virtual"], description="Data source type"),
#    side: str = Query("both", enum=["bids", "asks", "both"], description="Side to fetch")
#):
#    """
#    Lấy snapshot orderbook.
#    Ví dụ: GET /market/orderbook/BTCUSDT?type=real&side=bids
#    """
#    try:
#        return orderbook_service.get_orderbook(symbol=symbol, type=type, side=side)
#    except Exception as e:
#        raise HTTPException(status_code=500, detail=str(e))

# --- ORDERBOOK ---
@router.websocket("/ws/orderbook/{symbol}")
async def websocket_orderbook(
   websocket: WebSocket,
   symbol: str,
   type: str = Query("real", enum=["real", "virtual"], description="Data source type"),
    side: str = Query("both", enum=["bids", "asks", "both"], description="Side to fetch")

):
    await websocket.accept()
    try:
        while True:
            # lấy dữ liệu orderbook mới nhất từ Redis
            orderbook = orderbook_service.get_orderbook(symbol, type=type, side=side)
            await websocket.send_json(orderbook)
            await asyncio.sleep(1)  # Gửi dữ liệu mỗi giây
    except WebSocketDisconnect:
        print(f"Client disconnected from orderbook {symbol}")

# --- TRADES ---
# @router.get("/trades/{symbol}")
# def get_trades(
#     symbol: str,
#     type: str = Query("real", enum=["real", "virtual"]),
#     mode: str = Query("real_time", enum=["real_time", "history"]),
#     limit: int = Query(50, ge=1, le=1000)
# ):
#     """
#     Lấy lịch sử khớp lệnh.
#     Ví dụ: GET /market/trades/real/BTCUSDT?mode=history&limit=100
#     """
#     return trades_service.get_trades(symbol, mode=mode, type=type, limit=limit)
@router.websocket("/ws/trades/{symbol}")
async def websocket_trades(
   websocket: WebSocket,
   symbol: str,
   type: str = Query("real", enum=["real", "virtual"]), 
    mode: str = Query("real_time", enum=["real_time", "history"]),
    limit: int = Query(50, ge=1, le=1000) 
):
    await websocket.accept()
    try:
        while True:
            # lấy dữ liệu trades mới nhất từ Redis
            trades = trades_service.get_trades(symbol, mode=mode, type=type, limit= limit)
            await websocket.send_json(trades)
            await asyncio.sleep(1)  # Gửi dữ liệu mỗi giây
    except WebSocketDisconnect:
        print(f"Client disconnected from trades {symbol}")