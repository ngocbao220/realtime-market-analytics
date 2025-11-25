from fastapi import APIRouter, HTTPException, Query
from services import orderbook_service, trades_service

router = APIRouter(prefix="/market", tags=["Market Data"])

# --- ORDERBOOK ---
@router.get("/orderbook/{symbol}")
def get_orderbook(
    symbol: str, 
    type: str = Query("real", enum=["real", "virtual"], description="Data source type"),
    side: str = Query("both", enum=["bids", "asks", "both"], description="Side to fetch")
):
    """
    Lấy snapshot orderbook.
    Ví dụ: GET /market/orderbook/BTCUSDT?type=real&side=bids
    """
    try:
        return orderbook_service.get_orderbook(symbol=symbol, type=type, side=side)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- TRADES ---
@router.get("/trades/{symbol}")
def get_trades(
    symbol: str,
    type: str = Query("real", enum=["real", "virtual"]),
    mode: str = Query("real_time", enum=["real_time", "history"]),
    limit: int = Query(50, ge=1, le=1000)
):
    """
    Lấy lịch sử khớp lệnh.
    Ví dụ: GET /market/trades/real/BTCUSDT?mode=history&limit=100
    """
    return trades_service.get_trades(symbol, mode=mode, type=type, limit=limit)