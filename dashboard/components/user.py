import streamlit as st
import requests
from config import API_BASE_URL

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
    
# ----- Đoạn này chỉ cho admin thấy ----
def get_all_user_tab():
        st.subheader("👥 Danh sách người dùng trong hệ thống")
        if st.button("Làm mới danh sách"):
            st.rerun()
            
        all_users = api_get_all_users()
        
        if all_users:
            if isinstance(all_users, dict):
                if "detail" in all_users:
                    st.error(f"Lỗi từ API: {all_users['detail']}")
                    st.stop() # Dừng lại không vẽ bảng nữa
                
                all_users = [all_users]

            # Chuyển thành DataFrame
            import pandas as pd
            df = pd.DataFrame(all_users)
            
            # Kiểm tra xem DataFrame có dữ liệu không trước khi gán cột
            if not df.empty:
                # Chỉ đổi tên cột nếu số lượng cột khớp (tránh lỗi lệch cột)
                if len(df.columns) == 4:
                    df.columns = ["User ID", "Tên", "Số dư USD", "Số dư BTC"]
                
                st.dataframe(df, use_container_width=True)
            else:
                st.warning("Dữ liệu trả về rỗng.")
        else:
            st.info("Chưa có người dùng nào khác.")