# ... (Giữ nguyên các import cũ)
import logging
import os
import google.generativeai as genai
from db import ch_client 

class NarrativeService:
    # ... (Giữ nguyên hàm __init__ cũ) ...
    def __init__(self):
        self.logger = logging.getLogger("NarrativeService")
        self.ch_client = ch_client 
        
        api_key = os.getenv("GEMINI_API_KEY")
        
        if not api_key:
            self.logger.warning("⚠️ GEMINI_API_KEY not found! System will run in MOCK mode.")
            self.model = None
        else:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel('models/gemini-2.5-flash') 
                self.logger.info("✅ Gemini 2.5 Flash initialized successfully.")
            except Exception as e:
                self.logger.error(f"Failed to initialize Gemini: {e}")
                self.model = None

    # ... (Giữ nguyên hàm get_real_news_context cũ) ...

    # [MỚI] Hàm lấy tin tức thô cho Frontend hiển thị
    def get_raw_news(self, limit: int = 10):
        """Lấy danh sách tin tức mới nhất từ ClickHouse"""
        if not self.ch_client:
            return []
        try:
            # Lấy tin mới nhất, sắp xếp theo thời gian
            query = f"""
            SELECT title, published_at, source_id, url 
            FROM news 
            ORDER BY published_at DESC 
            LIMIT {limit}
            """
            rows = self.ch_client.execute(query)
            
            news_list = []
            for row in rows:
                news_list.append({
                    "title": row[0],
                    "time": row[1].strftime("%H:%M %d/%m"), # Format giờ đẹp
                    "source": row[2],
                    "url": row[3]
                })
            return news_list
        except Exception as e:
            self.logger.error(f"Error fetching raw news: {e}")
            return []

    # ... (Giữ nguyên hàm analyze_market_movement cũ) ...
    def get_real_news_context(self, symbol: str, lookback_hours: int = 168) -> str:
        # Code cũ của bạn...
        if not self.ch_client:
            return ""
        try:
            entities_filter = f"['{symbol}', 'SEC', 'MACRO', 'CRYPTO', 'FED']"
            query = f"""
            SELECT title, content, published_at, url 
            FROM news 
            WHERE published_at >= now() - INTERVAL {lookback_hours} HOUR
            AND hasAny(related_entities, {entities_filter})
            ORDER BY published_at DESC
            LIMIT 10
            """
            rows = self.ch_client.execute(query)
            if not rows: return ""
            context = ""
            for i, row in enumerate(rows):
                title, content, pub_at, url = row
                short_content = content[:300] + "..." if len(content) > 300 else content
                context += f"{i+1}. [{pub_at.strftime('%Y-%m-%d %H:%M')}] {title}\n   Nội dung: {short_content}\n   Nguồn: {url}\n\n"
            return context
        except Exception as e:
            self.logger.error(f"ClickHouse RAG Error: {e}")
            return ""

    def analyze_market_movement(self, symbol: str, change_percent: float, current_price: float):
        # Code cũ của bạn...
        news_context = self.get_real_news_context(symbol, lookback_hours=168)
        
        if not self.model:
            return {"summary": "Chưa cấu hình AI.", "confidence": "Low", "source": "System"}

        trend = "STABLE"
        if change_percent >= 3.0: trend = "UP"
        elif change_percent <= -3.0: trend = "DOWN"
        else: trend = "SIDEWAY"

        prompt = f"""
        Bạn là chuyên gia phân tích Crypto.
        TÌNH HUỐNG (24h qua): {symbol} biến động {change_percent}%, giá ${current_price}.
        
        TIN TỨC (7 NGÀY):
        {news_context if news_context else "Không có tin tức quan trọng."}
        
        YÊU CẦU:
        Giải thích ngắn gọn (2-3 câu) nguyên nhân biến động giá này dựa trên tin tức.
        Bắt đầu bằng "Nhận định: ..."
        """
        
        try:
            response = self.model.generate_content(prompt)
            return {
                "summary": response.text.strip(),
                "confidence": "High" if news_context else "Medium",
                "trend": trend,
                "source": "Gemini 2.5 AI"
            }
        except Exception as e:
            return {"summary": "Lỗi phân tích.", "detail": str(e), "confidence": "Low"}

narrative_service = NarrativeService()