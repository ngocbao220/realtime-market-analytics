import time
import json
import redis
import requests
import logging
import os

# --- CONFIG ---
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
API_URL = os.getenv("API_URL", "http://34.124.203.62:8000")
CHECK_INTERVAL = 60  # Kiểm tra giá mỗi 60s

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("PriceMonitor")

# Kết nối Redis
try:
    r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")

def monitor_loop():
    logger.info("🚀 Starting Price Monitor (Top 5 Coins Strategy)...")
    
    # Danh sách 5 đồng Coin cố định
    watchlist = ["BTC", "ETH", "BNB", "SOL", "DOGE"]
    
    # Lưu thời điểm phân tích lần cuối { "BTC": 17000000.0, ... }
    last_analysis_time = {}

    while True:
        for symbol in watchlist:
            try:
                # 1. Lấy dữ liệu giá từ Redis
                ticker_key = f"ticker_1d:{symbol}USDT"
                ticker_data = r.get(ticker_key)
                
                if not ticker_data: 
                    # Nếu chưa có dữ liệu trong Redis thì bỏ qua
                    continue

                data = json.loads(ticker_data)
                current_price = float(data.get('Close_price', 0))
                open_price = float(data.get('Open_price', 0))

                # Tránh chia cho 0
                if open_price == 0: 
                    continue

                # 2. Tính % biến động trong 24h
                change_percent = ((current_price - open_price) / open_price) * 100
                
                # 3. Logic Trigger Phân tích AI
                current_time = time.time()
                last_time = last_analysis_time.get(symbol, 0)
                should_analyze = False
                trigger_reason = ""

                # [LOGIC QUYẾT ĐỊNH CÓ GỌI AI HAY KHÔNG] 
                
                # TH1: Chưa phân tích bao giờ (Lần chạy đầu tiên) -> Chạy ngay
                if last_time == 0:
                    should_analyze = True
                    trigger_reason = "🚀 INIT (First Run)"
                
                # TH2: Biến động mạnh (>= 3%) -> Cách nhau ít nhất 10 phút (600s)
                elif abs(change_percent) >= 3.0:
                    if current_time - last_time > 600: 
                        should_analyze = True
                        trend = "PUMP 🟢" if change_percent > 0 else "DUMP 🔴"
                        trigger_reason = f"🚨 ALERT {trend} (>3%)"
                
                # TH3: Thị trường bình thường -> Cập nhật mỗi 30 phút (1800s)
                else:
                    if current_time - last_time > 1800: 
                        should_analyze = True
                        trigger_reason = "📉 ROUTINE UPDATE (30m)"

                # 4. Gọi API AI
                if should_analyze:
                    logger.info(f"{trigger_reason}: {symbol} {change_percent:.2f}% | Asking AI...")
                    
                    payload = {
                        "symbol": symbol,
                        "change_percent": round(change_percent, 2),
                        "current_price": current_price
                    }
                    
                    try:
                        # Gọi vào API nội bộ
                        response = requests.post(f"{API_URL}/narrative/analyze", json=payload, timeout=60)
                        
                        if response.status_code == 200:
                            # Chỉ cập nhật thời gian nếu gọi thành công
                            logger.info(f"✅ AI Done: {symbol}")
                            last_analysis_time[symbol] = current_time
                            
                            # Nghỉ 5s để Gemini hồi phục quota, tránh lỗi rate limit
                            time.sleep(5) 
                        else:
                            logger.error(f"API Error ({response.status_code}): {response.text}")
                            
                    except Exception as req_err:
                        logger.error(f"Failed to call API: {req_err}")
                
            except Exception as e:
                logger.error(f"Error checking {symbol}: {e}")
        
        # Đợi 60s trước khi quét lại toàn bộ danh sách
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    logger.info("⏳ Waiting 20s for Redis/Producer...")
    time.sleep(20)
    monitor_loop()