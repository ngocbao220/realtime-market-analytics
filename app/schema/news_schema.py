from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class NewsItem(BaseModel):
    source_id: str
    title: str
    content: str
    published_at: datetime
    url: str
    # Graph logic: Các thực thể liên quan (VD: ['BTC', 'ETH', 'BINANCE', 'SEC'])
    related_entities: List[str] 
    sentiment_score: float # -1.0 (Tiêu cực) đến 1.0 (Tích cực)

class NewsCreate(NewsItem):
    pass

# SQL để tạo bảng trong ClickHouse (Bạn chạy cái này trong DB Client hoặc migration script)
"""
CREATE TABLE IF NOT EXISTS default.news (
    source_id String,
    title String,
    content String,
    published_at DateTime,
    url String,
    related_entities Array(String), 
    sentiment_score Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (published_at, source_id);
"""