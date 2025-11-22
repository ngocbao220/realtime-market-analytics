import streamlit as st

from components.login import show_login, api_get_balance
from components.dashboard import show_dashboard

def main():
    # Kiểm tra an toàn: user_info phải tồn tại VÀ phải có 'user_id'
    if st.session_state.get('user_info') and 'user_id' in st.session_state['user_info']:
        
        user_id = st.session_state['user_info']['user_id']
        
        # Cập nhật số dư mới nhất
        refreshed_user = api_get_balance(user_id)
        
        # Chỉ cập nhật nếu lấy về thành công và có dữ liệu hợp lệ
        if refreshed_user and 'user_id' in refreshed_user:
             st.session_state['user_info'] = refreshed_user
        
        show_dashboard()
    else:
        # Nếu session bị lỗi hoặc rỗng -> Về trang login
        st.session_state['user_info'] = None 
        show_login()

if __name__ == "__main__":
    main()