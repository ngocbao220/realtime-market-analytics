import time
import json
import redis
import requests
import logging
import os

# --- CONFIG ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
API_URL = os.getenv("API_URL", "http://34.124.203.62:8000")
CHECK_INTERVAL = 30 # Worker chạy mỗi 30s

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PriceMonitor")

# Kết nối Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")

def monitor_loop():
    logger.info("🚀 Starting Price Monitor (SMART MODE: 3% Threshold / 24h)...")
    
    watchlist = ["BTC", "ETH", "BNB", "SOL"]
    
    # Lưu thời điểm phân tích cuối cùng của từng coin để tránh spam
    last_analysis_time = {}

    while True:
        for symbol in watchlist:
            try:
                # 1. Lấy giá Real-time từ Redis
                # [FIX]: Cập nhật key theo hình ảnh bạn gửi: ticker_1d:BTCUSDT
                ticker_key = f"ticker_1d:{symbol}USDT"
                ticker_data = r.get(ticker_key)
                
                if not ticker_data:
                    # logger.warning(f"No data for {ticker_key}")
                    continue

                data = json.loads(ticker_data)
                
                # [FIX]: Cập nhật tên trường dữ liệu theo hình ảnh
                # Close_price là giá hiện tại, Open_price là giá mở cửa 24h trước
                current_price = float(data.get('Close_price', 0))
                open_price = float(data.get('Open_price', 0))

                if open_price == 0: continue

                # 2. Tính toán % biến động trong 24h
                change_percent = ((current_price - open_price) / open_price) * 100
                
                # 3. LOGIC QUYẾT ĐỊNH GỌI AI
                current_time = time.time()
                last_time = last_analysis_time.get(symbol, 0)
                
                should_analyze = False
                trigger_reason = ""

                # Kịch bản 1: Biến động mạnh (>= 3%) -> Gọi AI thường xuyên (mỗi 5 phút)
                if abs(change_percent) >= 3.0:
                    if current_time - last_time > 100: # 300s = 5 phút
                        should_analyze = True
                        trend = "PUMP 🟢" if change_percent > 0 else "DUMP 🔴"
                        trigger_reason = f"🚨 ALERT {trend} (>3%)"
                
                # Kịch bản 2: Thị trường ổn định (< 3%) -> Gọi AI định kỳ (mỗi 15 phút)
                else:
                    if current_time - last_time > 100: # 900s = 15 phút
                        should_analyze = True
                        trigger_reason = "📉 SIDEWAY UPDATE (<3%)"

                # 4. THỰC HIỆN GỌI API
                if should_analyze:
                    logger.info(f"{trigger_reason}: {symbol} {change_percent:.2f}% | Sending to AI...")
                    
                    payload = {
                        "symbol": symbol,
                        "change_percent": round(change_percent, 2),
                        "current_price": current_price
                    }
                    
                    try:
                        # Timeout 60s để chờ Gemini đọc báo
                        response = requests.post(f"{API_URL}/narrative/analyze", json=payload, timeout=60)
                        
                        if response.status_code == 200:
                            analysis = response.json()
                            logger.info(f"✅ AI Response: {analysis.get('summary')}")
                            # Cập nhật thời điểm đã phân tích
                            last_analysis_time[symbol] = current_time
                        else:
                            logger.error(f"API Error ({response.status_code}): {response.text}")
                            
                    except requests.exceptions.Timeout:
                        logger.error("❌ AI API Timeout (60s)!")
                    except Exception as req_err:
                        logger.error(f"Failed to call API: {req_err}")
                
            except Exception as e:
                logger.error(f"Error checking {symbol}: {e}")
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    time.sleep(30)
    monitor_loop()