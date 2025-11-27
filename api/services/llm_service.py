import logging
import os
import google.generativeai as genai
from db import ch_client 

class NarrativeService:
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
                # Dùng Flash cho nhanh và tiết kiệm chi phí
                self.model = genai.GenerativeModel('models/gemini-2.5-flash') 
                self.logger.info("✅ Gemini 2.5 Flash initialized successfully.")
            except Exception as e:
                self.logger.error(f"Failed to initialize Gemini: {e}")
                self.model = None

    def get_real_news_context(self, symbol: str, lookback_hours: int = 168) -> str:
        """
        Lấy tin tức trong 7 ngày qua (168h) để AI có context xu hướng.
        """
        if not self.ch_client:
            return ""
            
        try:
            # Lấy tin liên quan Symbol hoặc Vĩ mô
            entities_filter = f"['{symbol}', 'SEC', 'MACRO', 'CRYPTO', 'FED']"
            
            # [CẬP NHẬT] Tăng giới hạn lấy tin lên 10 bài để có đủ dữ liệu xâu chuỗi sự kiện
            query = f"""
            SELECT title, content, published_at, url 
            FROM news 
            WHERE published_at >= now() - INTERVAL {lookback_hours} HOUR
            AND hasAny(related_entities, {entities_filter})
            ORDER BY published_at DESC
            LIMIT 10
            """
            
            rows = self.ch_client.execute(query)
            
            if not rows:
                return ""
            
            context = ""
            for i, row in enumerate(rows):
                title, content, pub_at, url = row
                # Cắt ngắn nội dung mỗi bài để tránh quá tải token
                short_content = content[:300] + "..." if len(content) > 300 else content
                context += f"{i+1}. [{pub_at.strftime('%Y-%m-%d %H:%M')}] {title}\n   Nội dung: {short_content}\n   Nguồn: {url}\n\n"
            
            return context
        except Exception as e:
            self.logger.error(f"ClickHouse RAG Error: {e}")
            return ""

    def analyze_market_movement(self, symbol: str, change_percent: float, current_price: float):
        """
        Phân tích biến động giá dựa trên chuỗi sự kiện lịch sử (7 ngày).
        """
        # [FIX QUAN TRỌNG] Sửa tham số lookback_hours từ 24 thành 168 (7 ngày)
        news_context = self.get_real_news_context(symbol, lookback_hours=168)
        
        if not self.model:
            return {
                "summary": f"Biến động: {symbol} {change_percent}% (Chưa cấu hình AI).",
                "detail": "Vui lòng kiểm tra API Key.",
                "confidence": "Low",
                "source": "System"
            }

        # 1. Xác định xu hướng hiện tại (Snapshot 24h)
        trend = "STABLE"
        if change_percent >= 3.0:
            trend = "UP"
            action_desc = f"TĂNG MẠNH {change_percent}%"
        elif change_percent <= -3.0:
            trend = "DOWN"
            action_desc = f"GIẢM MẠNH {change_percent}%"
        else:
            trend = "SIDEWAY"
            action_desc = f"BIẾN ĐỘNG NHẸ {change_percent}%"

        # 2. Tạo Prompt nâng cao (Chain-of-Thought)
        # [FIX] Cập nhật Prompt để AI biết đây là dữ liệu 7 ngày
        prompt = f"""
        Bạn là chuyên gia phân tích thị trường Crypto (Market Intelligence).
        
        TÌNH HUỐNG HIỆN TẠI (24h qua):
        - Tài sản: {symbol}
        - Trạng thái: Giá {action_desc}, hiện tại là ${current_price}.
        
        DỮ LIỆU LỊCH SỬ TIN TỨC (7 NGÀY QUA):
        {news_context if news_context else "Không có tin tức nổi bật trong 7 ngày qua."}
        
        YÊU CẦU PHÂN TÍCH:
        Hãy đóng vai trò là người kể chuyện, xâu chuỗi các sự kiện từ quá khứ (nếu có) để giải thích cho biến động hiện tại.
        
        1. Xu hướng: Tin tức tuần qua đang ủng hộ phe Mua (Bullish) hay phe Bán (Bearish)?
        2. Nguyên nhân: Tại sao 24h qua giá lại biến động như vậy? Có phải là hệ quả của một tin tức trước đó không?
        
        OUTPUT FORMAT:
        - Bắt đầu bằng: "Nhận định: ..."
        - Nội dung: Tối đa 2-3 câu, tập trung vào quan hệ nhân quả.
        - Ngôn ngữ: Tiếng Việt.
        """
        
        self.logger.info(f"Asking Gemini ({trend}) about {symbol} with 7-day context...")

        try:
            response = self.model.generate_content(prompt)
            explanation = response.text.strip()
            
            return {
                "summary": explanation,
                "detail": f"Dữ liệu tin tức tham khảo (7 ngày):\n{news_context}" if news_context else "Phân tích dựa trên hành động giá và thiếu vắng tin tức.",
                "confidence": "High" if news_context else "Medium",
                "trend": trend,
                "source": "Gemini 2.5 AI"
            }
            
        except Exception as e:
            self.logger.error(f"Gemini API Error: {e}")
            return {
                "summary": f"Lỗi phân tích cho {symbol}.",
                "detail": str(e),
                "confidence": "Low",
                "source": "Error"
            }

narrative_service = NarrativeService()