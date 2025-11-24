import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# --- IMPORT SERVICES ---
# Đảm bảo thư mục services nằm trong python path
try:
    from services.user_service import init_admin_account
except ImportError:
    # Hàm giả lập (fallback) nếu file chưa tồn tại để test
    def init_admin_account():
        logging.info("Giả lập: Đã khởi tạo tài khoản Admin")

# --- IMPORT ROUTERS ---
# Tôi đã gộp các kiểu import của bạn lại.
# Đảm bảo bạn có các file này trong thư mục 'routes' hoặc 'routers'.
try:
    from routes import trades, stats, orderbook, symbols, user
    from routes.matching_engine import orders, trades as matching_trades
except ImportError:
    logging.warning("Không thể import một hoặc nhiều router. Kiểm tra lại cấu trúc thư mục (routes vs routers).")
    # Định nghĩa dummy router để app không bị crash nếu thiếu file
    from fastapi import APIRouter
    trades = stats = orderbook = symbols = user = orders = matching_trades = type('obj', (object,), {'router': APIRouter()})

# --- CẤU HÌNH LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

# --- LIFESPAN (Sự kiện Khởi động/Tắt Server) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Code chạy khi Server BẮT ĐẦU
    logging.info("🚀 Đang khởi động Backend Giao dịch...")
    try:
        init_admin_account()
        logging.info("✅ Kiểm tra/Tạo tài khoản Admin hoàn tất.")
    except Exception as e:
        logging.error(f"❌ Lỗi khi khởi tạo Admin: {e}")
    
    yield # Server chạy và phục vụ request tại đây...
    
    # 2. Code chạy khi Server TẮT
    logging.info("🛑 Đang tắt Backend Giao dịch...")

# --- KHỞI TẠO APP ---
app = FastAPI(
    title="Hệ Thống Giao Dịch BinanceAPI", 
    version="1.0.0", 
    lifespan=lifespan
)

# --- CẤU HÌNH CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Đổi thành domain cụ thể khi deploy production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- ĐĂNG KÝ ROUTERS ---

# 1. Dữ liệu chung & Thống kê
app.include_router(stats.router, prefix="/api/stats", tags=["Thống kê"])
app.include_router(symbols.router, prefix="/api/symbols", tags=["Dữ liệu thị trường"])
app.include_router(orderbook.router, prefix="/api/orderbook", tags=["Dữ liệu thị trường"])

# 2. Quản lý người dùng
app.include_router(user.router, prefix="/api/user", tags=["Người dùng"])

# 3. Giao dịch & Khớp lệnh (Matching Engine)
app.include_router(trades.router, prefix="/api/trades", tags=["Giao dịch (Công khai)"])
app.include_router(orders.router, prefix="/api/orders", tags=["Đặt lệnh"])
app.include_router(matching_trades.router, prefix="/api/matching", tags=["Công cụ khớp lệnh"])

# --- ROOT ENDPOINT ---
@app.get("/")
def root():
    """Kiểm tra sức khỏe hệ thống và danh sách endpoint"""
    return {
        "trạng_thái": "ok",
        "thông_báo": "API Hệ thống Giao dịch đang chạy ổn định",
        "tài_liệu": "/docs",
        "endpoints": {
            "thống_kê": "/api/stats",
            "sổ_lệnh": "/api/orderbook",
            "cặp_giao_dịch": "/api/symbols",
            "người_dùng": "/api/user",
            "đặt_lệnh": "/api/orders"
        }
    }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)