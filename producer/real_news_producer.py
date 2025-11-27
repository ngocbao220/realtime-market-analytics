import time
import feedparser
from datetime import datetime
from clickhouse_driver import Client
import os
import logging
from bs4 import BeautifulSoup
import re
from dateutil import parser as date_parser

# --- CONFIG ---
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
RSS_URLS = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss"
]
CHECK_INTERVAL = 30  # Quét 3 phút 1 lần để tránh bị chặn

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("NewsCrawler")

# --- GRAPH LOGIC: ENTITY MAPPING ---
# Dictionary để map từ khóa trong bài báo sang Symbol (Node)
ENTITY_MAP = {
    "Bitcoin": "BTC", "BTC": "BTC",
    "Ethereum": "ETH", "Ether": "ETH", "ETH": "ETH",
    "Binance": "BNB", "BNB": "BNB", "CZ": "BNB",
    "Ripple": "XRP", "XRP": "XRP",
    "SEC": "SEC", "Regulation": "SEC",
    "Fed": "MACRO", "Inflation": "MACRO", "Powell": "MACRO",
    "Solana": "SOL",
    "Cardano": "ADA"
}

def get_client():
    try:
        return Client(host=CLICKHOUSE_HOST, port=9000, user="default", password="12345")
    except Exception as e:
        logger.error(f"Failed to connect DB: {e}")
        return None

def clean_html(html_content):
    """Làm sạch HTML trong summary của RSS"""
    soup = BeautifulSoup(html_content, "html.parser")
    return soup.get_text(separator=" ").strip()

def extract_entities(text):
    """
    Graph Edge Creation:
    Quét text để tìm các thực thể liên quan (BTC, ETH, SEC...)
    Đây là bước quan trọng để nối Tin Tức với Dữ Liệu Giá.
    """
    found_entities = set()
    text_lower = text.lower()
    
    for keyword, symbol in ENTITY_MAP.items():
        # Tìm keyword (dùng regex để tránh match nhầm từ con)
        if re.search(r'\b' + re.escape(keyword.lower()) + r'\b', text_lower):
            found_entities.add(symbol)
            
    return list(found_entities)

def fetch_and_store_rss():
    client = get_client()
    if not client: return

    for url in RSS_URLS:
        logger.info(f"Fetching RSS: {url}")
        try:
            feed = feedparser.parse(url)
            
            new_count = 0
            for entry in feed.entries:
                # 1. Parse dữ liệu
                title = entry.title
                link = entry.link
                # Coindesk dùng 'summary', CoinTelegraph có thể khác
                raw_summary = getattr(entry, 'summary', '') or getattr(entry, 'description', '')
                clean_content = clean_html(raw_summary)
                
                # Parse thời gian (quan trọng để khớp với thời gian sập giá)
                pub_date = entry.get('published', str(datetime.now()))
                try:
                    published_at = date_parser.parse(pub_date)
                except:
                    published_at = datetime.now()

                # 2. Trích xuất Entity (Graph Nodes)
                # Kết hợp title và content để tìm entity chính xác hơn
                full_text = f"{title} {clean_content}"
                related_entities = extract_entities(full_text)
                
                # Nếu không tìm thấy entity cụ thể, gắn tag 'CRYPTO' chung
                if not related_entities:
                    related_entities = ["CRYPTO"]

                # 3. Insert vào ClickHouse
                # Dùng link làm source_id để tránh trùng lặp (Deduplication)
                try:
                    # Kiểm tra xem tin này đã tồn tại chưa (Simple Dedupe)
                    exists = client.execute(
                        "SELECT count() FROM default.news WHERE source_id = %(link)s", 
                        {'link': link}
                    )[0][0]
                    
                    if exists == 0:
                        client.execute(
                            '''
                            INSERT INTO default.news 
                            (source_id, title, content, published_at, url, related_entities, sentiment_score) 
                            VALUES
                            ''',
                            [{
                                'source_id': link,
                                'title': title,
                                'content': clean_content,
                                'published_at': published_at,
                                'url': link,
                                'related_entities': related_entities,
                                'sentiment_score': 0.0 # Để trống hoặc tích hợp Sentiment Model sau
                            }]
                        )
                        new_count += 1
                        logger.info(f"Saved: [{', '.join(related_entities)}] {title[:50]}...")
                except Exception as db_err:
                    logger.error(f"DB Error for {link}: {db_err}")

            logger.info(f"Finished {url}. New articles: {new_count}")

        except Exception as e:
            logger.error(f"Error parsing RSS {url}: {e}")

def run_crawler():
    # Chờ DB khởi động
    time.sleep(15) 
    logger.info("Starting Real News Crawler...")
    
    while True:
        fetch_and_store_rss()
        logger.info(f"Sleeping for {CHECK_INTERVAL}s...")
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    run_crawler()