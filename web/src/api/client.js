import axios from 'axios';

// --- CẤU HÌNH ---
// Trong Vite dùng import.meta.env, trong Create-React-App dùng process.env
const API_BASE_URL = import.meta.env?.API_URL || "http://localhost:8000";

class ExchangeAPI {
    /**
     * Client chuyên dụng để gọi API sàn giao dịch.
     * Sử dụng Axios Instance để tái sử dụng kết nối.
     */
    constructor(baseURL = API_BASE_URL) {
        this.baseURL = baseURL;
        this.client = axios.create({
            baseURL: this.baseURL,
            timeout: 3000, // 3 seconds timeout giống Python
            headers: {
                'Content-Type': 'application/json',
            },
        });

        // Interceptor để handle lỗi global (tương tự try/except bao quanh request)
        this.client.interceptors.response.use(
            (response) => response,
            (error) => {
                // Không throw lỗi để code phía dưới tự xử lý null như Python logic
                return Promise.reject(error);
            }
        );
    }

    /**
     * Hàm nội bộ xử lý request an toàn
     * @returns {Promise<any | null>}
     */
    async _req(method, endpoint, params = null, jsonData = null) {
        try {
            const config = {
                method: method,
                url: endpoint,
                params: params,
                data: jsonData,
            };

            const response = await this.client.request(config);
            return response.data;

        } catch (error) {
            let errorDetail = error.message;

            if (error.response) {
                // Lỗi từ Server trả về (400, 404, 500)
                // Axios trả về error.response.data tương đương response.json()
                errorDetail = error.response.data?.detail || error.response.statusText;
                console.warn(`⚠️ API Fail ${endpoint}: ${errorDetail}`);
            } else if (error.request) {
                // Lỗi kết nối (không nhận được phản hồi)
                console.error(`❌ Connection Error ${endpoint}: No response received`);
            } else {
                // Lỗi setup
                console.error(`❌ Error ${endpoint}: ${error.message}`);
            }
            return null;
        }
    }

    // ==========================================
    // 1. USER & AUTH
    // ==========================================

    /**
     * API: POST /users
     */
    async loginOrRegister(username) {
        return await this._req("POST", "/users", null, { username });
    }

    /**
     * API: GET /users/{userId}
     */
    async getUserInfo(userId) {
        return await this._req("GET", `/users/${userId}`);
    }

    /**
     * API: GET /users
     */
    async getAllUsers() {
        const res = await this._req("GET", "/users");
        return res || [];
    }

    /**
     * API: DELETE /users/{userId}
     * @returns {[boolean, string]} Tuple tương đương Python
     */
    async deleteUser(userId) {
        const res = await this._req("DELETE", `/users/${userId}`);
        if (res && res.success) {
            return [true, res.message || "Đã xóa"];
        }
        return [false, "Lỗi khi xóa user"];
    }

    // ==========================================
    // 2. MARKET DATA
    // ==========================================

    async getKline(symbol, interval, limit = 30) {
        const data = await this._req("GET", `/klines/${symbol}`, { interval, limit });
        return data?.data || [];
    }

    async getTickers() {
        const res = await this._req("GET", "/tickers");
        return res || [];
    }

    /**
     * API: GET /market/orderbook/{symbol}?type=...&side=...
     * Đã gộp logic của 2 hàm get_orderbook trong Python
     */
    async getOrderbook(symbol, type = "real", side = "both") {
        const params = { type, side };
        const res = await this._req("GET", `/market/orderbook/${symbol}`, params);
        return res || { bids: [], asks: [] };
    }

    /**
     * API: GET /market/trades/{symbol}
     * Đã gộp logic của 2 hàm get_trades/get_recent_trades trong Python
     */
    async getTrades(symbol, type = "real", limit = 50) {
        const params = { type, limit };
        const res = await this._req("GET", `/market/trades/${symbol}`, params);
        return res || [];
    }

    // ==========================================
    // 3. TRADING (ORDERS)
    // ==========================================

    /**
     * API: POST /orders
     * @returns {[boolean, string]} Tuple [Success, Message]
     */
    async placeOrder(userId, symbol, side, price, amount) {
        const payload = {
            user_id: String(userId),
            symbol: String(symbol),
            side: String(side),
            price: parseFloat(price),
            amount: parseFloat(amount)
        };

        const res = await this._req("POST", "/orders", null, payload);

        if (res && res.success) {
            return [true, res.msg || "Đặt lệnh thành công"];
        }

        // Lấy chi tiết lỗi giống Python
        let errorMsg = "Lỗi kết nối";
        if (res) {
            if (res.detail) errorMsg = res.detail;
            else if (res.msg) errorMsg = res.msg;
        }

        return [false, errorMsg];
    }

    /**
     * API: GET /orders/{userId}
     */
    async getOpenOrders(userId) {
        const data = await this._req("GET", `/orders/${userId}`);
        return Array.isArray(data) ? data : [];
    }

    /**
     * API: DELETE /orders/{orderId}?user_id=...
     * Đã gộp logic của 2 hàm cancel_order trong Python
     * @returns {[boolean, string]}
     */
    async cancelOrder(orderId, userId) {
        const params = { user_id: userId };
        const res = await this._req("DELETE", `/orders/${orderId}`, params);

        // Python code có logic check 2 kiểu response, giữ nguyên cả 2 logic
        if (res) {
            // Case 1: Trả về message trực tiếp
            if (res.message) return [true, res.message];
            // Case 2: Trả về status success
            if (res.status === "success") return [true, res.message];
            
            // Case Fail
            return [false, res.detail || "Không thể hủy lệnh"];
        }

        return [false, "Lỗi kết nối"];
    }
}

// --- KHỞI TẠO CLIENT ĐỂ DÙNG CHUNG ---
export const api = new ExchangeAPI();
export default ExchangeAPI;