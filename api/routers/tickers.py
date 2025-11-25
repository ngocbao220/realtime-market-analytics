from fastapi import APIRouter
from services import ticker_service, trades_service

router = APIRouter(tags=["Tickers"])

# Dùng để hiển thị OCLH trong 24h
@router.get("/ticker/get")
def get_tickers():
    return ticker_service.get_tickers()