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

# ... (Giữ nguyên endpoint /alerts cũ) ...
@router.get("/alerts")
async def get_alerts():
    try:
        # 1. Kiểm tra kết nối Redis
        if not redis_client:
            print("❌ LỖI: Redis Client chưa kết nối!")
            return []
        
        # 2. Lấy dữ liệu thô
        alerts_raw = redis_client.lrange("dashboard:alerts", 0, 49)
        print(f"🔍 Debug: Tìm thấy {len(alerts_raw)} bản ghi trong Redis.")

        if not alerts_raw:
            return [] # Redis rỗng thật sự

        alerts = []
        for a in alerts_raw:
            try:
                alerts.append(json.loads(a))
            except json.JSONDecodeError:
                print(f"⚠️ Lỗi decode JSON: {a}")
                continue

        unique_alerts = {}
        cleaned_list = []
        
        # 3. Kiểm tra logic lọc
        target_coins = ["BTC", "ETH", "BNB", "SOL", "DOGE"]
        
        for alert in alerts:
            symbol = alert.get('symbol')
            
            # Debug: In ra xem symbol là gì
            # print(f"👉 Checking symbol: {symbol}") 

            # Chuẩn hóa symbol (đề phòng BTCUSDT vs BTC)
            # Nếu symbol là BTCUSDT thì cắt bỏ USDT để so sánh
            normalized_symbol = symbol.replace("USDT", "") if symbol else ""

            if normalized_symbol in target_coins and normalized_symbol not in unique_alerts:
                unique_alerts[normalized_symbol] = True
                cleaned_list.append(alert)
        
        print(f"✅ Debug: Trả về {len(cleaned_list)} bản ghi sau khi lọc.")
        
        # Sắp xếp
        cleaned_list.sort(key=lambda x: target_coins.index(x['symbol'].replace("USDT", "")) if x['symbol'].replace("USDT", "") in target_coins else 99)
        
        return cleaned_list

    except Exception as e:
        print(f"❌ Exception in get_alerts: {e}")
        return []
# [MỚI] Endpoint lấy tin tức cho nút "Tin tức"
@router.get("/news")
async def get_news():
    """Trả về danh sách 10 tin mới nhất"""
    return narrative_service.get_raw_news(limit=10)