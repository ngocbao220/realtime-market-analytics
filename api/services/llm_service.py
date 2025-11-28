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
            self.logger.warning("GEMINI_API_KEY not found! System will run in MOCK mode.")
            self.model = None
        else:
            try:
                genai.configure(api_key=api_key)
                self.model = genai.GenerativeModel('models/gemini-2.5-flash')
                self.logger.info("Gemini 2.5 Flash initialized successfully.")
            except Exception as e:
                self.logger.error(f"Failed to initialize Gemini: {e}")
                self.model = None

    # ===================================================================
    # Hàm helper gọi Gemini an toàn (fix lỗi response.text 2025)
    # ===================================================================
    def _safe_generate(self, prompt: str) -> str:
        """Gọi Gemini an toàn với mọi phiên bản SDK hiện tại"""
        if not self.model:
            return "AI chưa được khởi tạo."

        try:
            response = self.model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.3,
                    "top_p": 0.8,
                    "top_k": 40,
                    "max_output_tokens": 8192,
                },
                safety_settings=[
                    {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                    {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
                ]
            )

            # Cách lấy text an toàn nhất 2025
            if hasattr(response, "text") and response.text:
                return response.text.strip()

            if response.candidates and len(response.candidates) > 0:
                parts = response.candidates[0].content.parts
                for part in parts:
                    if hasattr(part, "text"):
                        return part.text.strip()

            # Bị block bởi safety filter
            if (hasattr(response, "prompt_feedback") and
                    response.prompt_feedback and
                    hasattr(response.prompt_feedback, "block_reason")):
                reason = response.prompt_feedback.block_reason
                return f"Nội dung bị Google chặn (safety): {reason}"

            return "AI không trả về nội dung hợp lệ."

        except Exception as e:
            self.logger.error(f"Gemini generate_content error: {e}")
            return f"Lỗi AI: {str(e)}"

    # ===================================================================
    # Lấy tin tức thô cho Frontend
    # ===================================================================
    def get_raw_news(self, limit: int = 10):
        if not self.ch_client:
            return []

        try:
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
                    "time": row[1].strftime("%H:%M %d/%m"),
                    "source": row[2],
                    "url": row[3]
                })
            return news_list
        except Exception as e:
            self.logger.error(f"Error fetching raw news: {e}")
            return []

    # ===================================================================
    # Context tin tức cho RAG
    # ===================================================================
    def get_real_news_context(self, symbol: str, lookback_hours: int = 168) -> str:
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
            if not rows:
                return ""

            context = ""
            for i, row in enumerate(rows):
                title, content, pub_at, url = row
                short_content = content[:300] + "..." if len(content) > 300 else content
                context += (f"{i+1}. [{pub_at.strftime('%Y-%m-%d %H:%M')}] {title}\n"
                            f"   Nội dung: {short_content}\n"
                            f"   Nguồn: {url}\n\n")
            return context
        except Exception as e:
            self.logger.error(f"ClickHouse RAG Error: {e}")
            return ""

    # ===================================================================
    # Phân tích biến động giá (đã fix hoàn toàn)
    # ===================================================================
    def analyze_market_movement(self, symbol: str, change_percent: float, current_price: float):
        news_context = self.get_real_news_context(symbol, lookback_hours=168)

        if not self.model:
            return {
                "summary": "Chưa cấu hình AI.",
                "confidence": "Low",
                "trend": "STABLE",
                "source": "System"
            }

        # Xác định trend
        if change_percent >= 3.0:
            trend = "UP"
        elif change_percent <= -3.0:
            trend = "DOWN"
        else:
            trend = "SIDEWAY"

        prompt = f"""
        Bạn là chuyên gia phân tích Crypto.
        TÌNH HUỐNG (24h qua): {symbol} biến động {change_percent:+.2f}%, giá ${current_price}.
        TIN TỨC (7 NGÀY):
{news_context if news_context else "Không có tin tức quan trọng."}

        YÊU CẦU:
        Giải thích ngắn gọn (2-3 câu) nguyên nhân biến động giá này dựa trên tin tức.
        Bắt đầu bằng "Nhận định: ..."
        """

        summary = self._safe_generate(prompt)

        return {
            "summary": summary,
            "confidence": "High" if news_context else "Medium",
            "trend": trend,
            "source": "Gemini 2.5 AI"
        }

    # ===================================================================
    # Tóm tắt tin tức tuần (đã fix hoàn toàn)
    # ===================================================================
    def summarize_weekly_news(self):
        if not self.ch_client or not self.model:
            return "Hệ thống chưa sẵn sàng (DB hoặc AI lỗi)."

        try:
            query = """
                SELECT title, content, published_at, source_id
                FROM news
                WHERE published_at >= now() - INTERVAL 7 DAY
                ORDER BY published_at DESC
                LIMIT 30
            """
            rows = self.ch_client.execute(query)
            if not rows:
                return "Không có đủ dữ liệu tin tức trong tuần qua để tổng hợp."

            news_text = ""
            for row in rows:
                title = row[0]
                content_snippet = row[1][:200].replace("\n", " ")
                source = row[3]
                news_text += f"- [{source}] {title}: {content_snippet}...\n"

            prompt = f"""
            Bạn là biên tập viên tin tức Crypto chuyên nghiệp.
            Dưới đây là các tin tức nổi bật trong 7 ngày qua:
{news_text}

            YÊU CẦU:
            Viết một đoạn tóm tắt thị trường (150-200 từ) bằng tiếng Việt, giọng chuyên nghiệp, khách quan.
            Tập trung vào xu hướng lớn, sự kiện vĩ mô, biến động quan trọng.
            Không liệt kê bullet, hãy kể thành một câu chuyện mạch lạc.
            Bắt đầu bằng tiêu đề in hoa: "QUAN SÁT TUẦN QUA:"
            """

            result = self._safe_generate(prompt)
            return result

        except Exception as e:
            self.logger.error(f"Summarize News Error: {e}")
            return "Đã xảy ra lỗi khi tổng hợp tin tức tuần."

# Khởi tạo instance toàn cục (nếu bạn dùng ở nơi khác)
narrative_service = NarrativeService()