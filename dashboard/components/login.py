import streamlit as st
import requests
import time
import logging

from services.api_client import api_login

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

# --- UI ĐĂNG NHẬP ---
def show_login():
    st.set_page_config(page_title="Crypto Login", layout="centered")
    st.title("Sàn Giao Dịch Giả Lập")
    st.markdown("---")

    logging.info("🖥️ Rendering login page...")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.info("Nhập tên của bạn!")

        with st.form("login_form"):
            username = st.text_input("Tên Trader:", placeholder="VD: traderPro")
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
