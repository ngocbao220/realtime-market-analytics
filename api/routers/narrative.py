from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from services.llm_service import narrative_service
from db import redis_client
import json
import time
import logging

router = APIRouter(prefix="/narrative", tags=["Narrative Analysis"])
logger = logging.getLogger("NarrativeRouter")

class MarketMovementAlert(BaseModel):
    symbol: str
    change_percent: float # Thay đổi tên biến cho tổng quát (có thể âm hoặc dương)
    current_price: float

def save_alert_to_redis(alert_data: dict):
    try:
        if redis_client:
            redis_client.lpush("dashboard:alerts", json.dumps(alert_data))
            redis_client.ltrim("dashboard:alerts", 0, 19)
    except Exception as e:
        logger.error(f"❌ Failed to save alert to Redis: {e}")

@router.post("/analyze")
async def analyze_market(alert: MarketMovementAlert, background_tasks: BackgroundTasks):
    """
    Endpoint phân tích biến động giá (Tăng/Giảm/Đi ngang).
    """
    try:
        # Gọi hàm mới analyze_market_movement
        analysis_result = narrative_service.analyze_market_movement(
            alert.symbol, 
            alert.change_percent, 
            alert.current_price
        )
        
        dashboard_data = {
            "timestamp": time.strftime("%H:%M:%S"),
            "symbol": alert.symbol,
            "price": alert.current_price,
            "change": alert.change_percent, # Key mới để dashboard hiển thị màu xanh/đỏ
            "analysis": analysis_result
        }
        
        background_tasks.add_task(save_alert_to_redis, dashboard_data)
        
        return analysis_result
        
    except Exception as e:
        logger.error(f"Error in analyze endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
# [MỚI] Endpoint cho Frontend (Header) lấy danh sách cảnh báo
@router.get("/alerts")
async def get_alerts():
    """Trả về danh sách cảnh báo từ Redis cho Web App"""
    try:
        if not redis_client:
            return []
        
        # Lấy 50 tin mới nhất
        alerts_raw = redis_client.lrange("dashboard:alerts", 0, 49)
        alerts = [json.loads(a) for a in alerts_raw]
        
        # [QUAN TRỌNG] Lọc trùng lặp ngay tại API để Frontend đỡ phải xử lý
        # Chỉ giữ lại tin mới nhất của mỗi Symbol
        unique_alerts = {}
        cleaned_list = []
        
        for alert in alerts:
            symbol = alert['symbol']
            if symbol not in unique_alerts:
                unique_alerts[symbol] = True
                cleaned_list.append(alert)
                
        return cleaned_list
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return []