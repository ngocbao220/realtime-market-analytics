from fastapi import APIRouter
from services import ticker_service, trades_service

router = APIRouter(tags=["Tickers"])

# Dùng để hiển thị OCLH trong 24h
@router.get("/dashboard/get_all_tickers_data")
def get_tickers():
    return ticker_service.get_tickers()