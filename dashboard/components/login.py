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
    """
    Hiển thị form đăng nhập.
    """
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("### 🔐 Đăng nhập hệ thống")
        
        with st.form("login_form"):
            username = st.text_input("Tên đăng nhập", placeholder="Nhập tên của bạn (ví dụ: admin)")
            submitted = st.form_submit_button("Truy cập Dashboard", type="primary", use_container_width=True)
            
            if submitted:
                if not username:
                    st.warning("Vui lòng nhập tên đăng nhập.")
                    return

                with st.spinner("Đang kết nối tới Backend..."):
                    # Gọi API Login thông qua Client Service
                    user_data = api_login(username)
                    
                    if user_data and "user_id" in user_data:
                        st.success(f"Đăng nhập thành công! Xin chào {user_data.get('username')}")
                        
                        # Lưu session
                        st.session_state['user_info'] = user_data
                        
                        # Reload lại trang để vào Dashboard chính
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error("Đăng nhập thất bại. Backend không phản hồi hoặc lỗi kết nối.")