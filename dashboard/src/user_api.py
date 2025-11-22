import requests
import logging
import streamlit as st

from config import API_BASE_URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    force=True
)

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

# --- GỌI API LẤY ALL USER ---
def api_get_all_users():
    try:
        response = requests.get(f"{API_BASE_URL}/user/get_all")
        if response.status_code == 200:
            return response.json()
        return []
    except:
        return []

def api_delete_user(user_id):
    """Gọi API xóa user"""
    try:
        response = requests.delete(f"{API_BASE_URL}/user/delete/{user_id}")
        if response.status_code == 200:
            return True, response.json().get("message")
        else:
            # Lấy thông báo lỗi từ API (vd: Không thể xóa Admin)
            error_msg = response.json().get("detail", "Lỗi không xác định")
            return False, error_msg
    except Exception as e:
        return False, str(e)