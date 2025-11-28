from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from services.llm_service import narrative_service
from db import redis_client
import json
import time
import logging

router = APIRouter(prefix="/narrative", tags=["Narrative Analysis"])
logger = logging.getLogger("NarrativeRouter")

# ... (Giữ nguyên class MarketMovementAlert và hàm save_alert_to_redis cũ) ...
class MarketMovementAlert(BaseModel):
    symbol: str
    change_percent: float
    current_price: float

def save_alert_to_redis(alert_data: dict):
    try:
        if redis_client:
            redis_client.lpush("dashboard:alerts", json.dumps(alert_data))
            redis_client.ltrim("dashboard:alerts", 0, 19)
    except Exception as e:
        logger.error(f"❌ Failed to save alert to Redis: {e}")

# ... (Giữ nguyên endpoint /analyze cũ) ...
@router.post("/analyze")
async def analyze_market(alert: MarketMovementAlert, background_tasks: BackgroundTasks):
    try:
        analysis_result = narrative_service.analyze_market_movement(
            alert.symbol, alert.change_percent, alert.current_price
        )
        dashboard_data = {
            "timestamp": time.strftime("%H:%M:%S"),
            "symbol": alert.symbol,
            "price": alert.current_price,
            "change": alert.change_percent,
            "analysis": analysis_result
        }
        background_tasks.add_task(save_alert_to_redis, dashboard_data)
        return analysis_result
    except Exception as e:
        logger.error(f"Error in analyze endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ... (Giữ nguyên import)

@router.get("/alerts")
async def get_alerts():
    """
    Trả về nhận định mới nhất của 5 đồng coin quan trọng (Format: BTCUSDT).
    """
    try:
        if not redis_client: return []
        
        alerts_raw = redis_client.lrange("dashboard:alerts", 0, 49)
        alerts = [json.loads(a) for a in alerts_raw]
        
        unique_alerts = {}
        cleaned_list = []
        
        # [CẬP NHẬT] Danh sách target có thêm USDT
        target_coins = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"]
        
        for alert in alerts:
            symbol = alert.get('symbol') # Lúc này symbol sẽ là "BTCUSDT"
            
            if symbol in target_coins and symbol not in unique_alerts:
                unique_alerts[symbol] = True
                cleaned_list.append(alert)
        
        # Sắp xếp
        cleaned_list.sort(key=lambda x: target_coins.index(x['symbol']) if x['symbol'] in target_coins else 99)
        
        return cleaned_list
    except Exception as e:
        logger.error(f"Error fetching alerts: {e}")
        return []
# [MỚI] Endpoint lấy tin tức cho nút "Tin tức"
@router.get("/news")
async def get_news():
    """Trả về danh sách 10 tin mới nhất"""
    return narrative_service.get_raw_news(limit=10)


# ... (Giữ nguyên import cũ)

# [MỚI] Endpoint để Worker gọi định kỳ (Trigger AI tóm tắt)
@router.post("/summarize-news")
async def trigger_news_summary(background_tasks: BackgroundTasks):
    try:
        # Hàm chạy ngầm để không block API
        def process_summary():
            summary = narrative_service.summarize_weekly_news()
            # Lưu vào Redis key 'dashboard:news_summary'
            if redis_client:
                redis_client.set("dashboard:news_summary", summary)
                # Lưu thêm timestamp để biết cập nhật lúc nào
                redis_client.set("dashboard:news_summary_time", time.strftime("%H:%M %d/%m"))
        
        background_tasks.add_task(process_summary)
        return {"status": "Processing weekly summary..."}
    except Exception as e:
        logger.error(f"Error triggering summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# [MỚI] Endpoint để Frontend hiển thị
@router.get("/news-summary")
async def get_news_summary():
    """Lấy bản tóm tắt tin tức từ Redis"""
    try:
        if not redis_client:
            return {"summary": "Lỗi kết nối Redis", "time": ""}
        
        summary = redis_client.get("dashboard:news_summary")
        updated_time = redis_client.get("dashboard:news_summary_time")
        
        return {
            "summary": summary if summary else "Đang chờ AI tổng hợp dữ liệu tuần...",
            "time": updated_time if updated_time else "Chưa cập nhật"
        }
    except Exception as e:
        logger.error(f"Error fetching summary: {e}")
        return {"summary": "Lỗi hệ thống.", "time": ""}