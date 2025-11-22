import streamlit as st
import requests
import time
import logging

# CẤU HÌNH LOGGING
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

logging.info("🚀 Streamlit UI started.")

from config import API_BASE_URL

# --- QUẢN LÝ TRẠNG THÁI (SESSION STATE) ---
if 'user_info' not in st.session_state:
    st.session_state['user_info'] = None

if 'is_admin' not in st.session_state:
    st.session_state['is_admin'] = False

# --- GỌI API ĐỂ ĐĂNG NHẬP ---
def api_login(username):
    username = username.strip()
    logging.info(f"🔐 Login request received for username: {username}")

    # 1. ADMIN ĐĂNG NHẬP
    if username.lower() == "admin":
        url = f"{API_BASE_URL}/user/get/0"
        logging.info(f"📡 Fetching admin info from {url}")

        try:
            response = requests.get(url, timeout=5)
            logging.info(f"Admin API response code: {response.status_code}")

            if response.status_code == 200:
                return response.json()
            else:
                logging.error("❌ Admin not found on backend.")
                st.error("Không tìm thấy tài khoản Admin.")
                return None

        except Exception as e:
            logging.exception("⚠️ Admin login failed due to exception:")
            st.error(f"Lỗi kết nối khi lấy Admin: {e}")
            return None

    # 2. USER THƯỜNG
    url = f"{API_BASE_URL}/user/create"
    payload = {"username": username}
    logging.info(f"📡 Creating new user via POST {url} with payload: {payload}")

    try:
        response = requests.post(url, json=payload, timeout=5)
        logging.info(f"User create API response: {response.status_code}")

        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"❌ API error: {response.text}")
            st.error(f"Lỗi API ({response.status_code}): {response.text}")
            return None

    except requests.exceptions.ConnectionError:
        logging.error("❌ Could not connect to API.")
        st.error("Không thể kết nối tới API")
        return None


# --- API LẤY BALANCE ---
def api_get_balance(user_id):
    url = f"{API_BASE_URL}/user/get/{user_id}"
    logging.info(f"📡 Fetching balance for user {user_id}")

    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            return response.json()
    except Exception as e:
        logging.error(f"❌ Error while fetching balance: {e}")

    return None


# --- UI ĐĂNG NHẬP ---
def show_login():
    st.set_page_config(page_title="Crypto Login", layout="centered")
    st.title("Sàn Giao Dịch Giả Lập")
    st.markdown("---")

    logging.info("🖥️ Rendering login page...")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.info("Nhập tên và bạn sẽ nhận được 1000USDC và 1.0BTC để trải nghiệm!")

        with st.form("login_form"):
            username = st.text_input("Tên Trader:", placeholder="VD: traderPro hoặc admin")
            submitted = st.form_submit_button("🚀 Truy cập hệ thống", use_container_width=True)

            if submitted:
                if not username.strip():
                    st.warning("Vui lòng nhập tên!")
                    logging.warning("⚠️ Login attempt with empty username.")
                else:
                    logging.info(f"🔄 Login request triggered for: {username}")

                    with st.spinner("Đang kết nối tới Blockchain (Redis)..."):
                        user_data = api_login(username)

                    if user_data and "user_id" in user_data:
                        role = user_data.get("role", "user")
                        logging.info(f"✅ Login successful: {user_data}")

                        st.session_state['user_info'] = user_data
                        st.session_state['is_admin'] = (role == "admin")

                        if role == "admin":
                            st.success(f"Xin chào ADMIN! (ID: {user_data.get('user_id')})")
                        else:
                            st.success("Đăng nhập thành công!")

                        time.sleep(0.5)
                        logging.info("🔁 Reloading UI after login.")
                        st.rerun()

                    else:
                        logging.error(f"❌ Login failed. Response: {user_data}")
                        if user_data and "detail" in user_data:
                            st.error(f"Lỗi: {user_data['detail']}")
