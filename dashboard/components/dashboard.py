import streamlit as st
import requests
import pandas as pd
import time
from config import API_BASE_URL
from components.user import api_get_all_users, api_delete_user

# --- HÀM API DELETE (Thêm vào nếu chưa có) ---
def api_delete_user(user_id):
    """Gọi API xóa user"""
    try:
        response = requests.delete(f"{API_BASE_URL}/user/delete/{user_id}", timeout=5)
        if response.status_code == 200:
            return True, response.json().get("message")
        else:
            return False, response.json().get("detail", "Lỗi không xác định")
    except Exception as e:
        return False, str(e)

# --- COMPONENT: GIAO DIỆN ADMIN ---
def show_admin_panel():
    st.header("🛠️ Admin Control Panel")
    
    admin_tab1, admin_tab2 = st.tabs(["👥 Quản lý User", "💰 Thống kê"])
    
    with admin_tab1:
        col_header_1, col_header_2 = st.columns([4, 1])
        with col_header_1:
            st.subheader("Danh sách người dùng")
        with col_header_2:
            if st.button("🔄 Refresh"):
                st.rerun()
            
        all_users = api_get_all_users()
        
        # Xử lý dữ liệu đầu vào
        user_list = []
        if all_users:
            if isinstance(all_users, dict) and "detail" in all_users:
                st.error(f"Lỗi: {all_users['detail']}")
            else:
                if isinstance(all_users, dict): all_users = [all_users]
                user_list = all_users

        if user_list:
            df = pd.DataFrame(user_list)
            
            # --- BƯỚC 1: CHUẨN BỊ DATAFRAME ---
            # Thêm cột "Chọn" (checkbox) vào đầu bảng, mặc định là False (chưa tick)
            df.insert(0, "Chọn", False)

            # Đổi tên cột cho đẹp
            col_map = {
                "user_id": "ID", "username": "Tên Trader", 
                "usd": "Số dư USD", "btc": "Số dư BTC", "role": "Quyền"
            }
            df = df.rename(columns=col_map)

            # --- BƯỚC 2: HIỂN THỊ BẢNG CÓ THỂ CHỈNH SỬA ---
            # st.data_editor trả về cái bảng sau khi bạn đã tick chọn
            edited_df = st.data_editor(
                df,
                column_config={
                    "Chọn": st.column_config.CheckboxColumn(
                        "Xóa?",
                        help="Chọn để xóa",
                        default=False,
                    ),
                    "ID": st.column_config.TextColumn("User ID", disabled=True), # Khóa không cho sửa ID
                    "Tên Trader": st.column_config.TextColumn("Tên", disabled=True),
                },
                use_container_width=True,
                hide_index=True,
                key="user_editor" # Key này quan trọng để giữ trạng thái tick
            )

            # --- BƯỚC 3: XỬ LÝ NÚT XÓA ---
            # Lọc ra những dòng mà cột "Chọn" là True
            selected_rows = edited_df[edited_df["Chọn"] == True]

            if not selected_rows.empty:
                st.markdown("---")
                st.warning(f"⚠️ Bạn đang chọn xóa **{len(selected_rows)}** người dùng.")
                
                # Nút xác nhận xóa hàng loạt
                if st.button("🗑️ Xác nhận xóa các dòng đã chọn", type="primary"):
                    
                    success_count = 0
                    fail_count = 0
                    progress_bar = st.progress(0)
                    total = len(selected_rows)

                    for index, row in selected_rows.iterrows():
                        user_id = row["ID"] # Lấy ID từ dòng đang chọn
                        
                        # Cập nhật thanh tiến trình
                        progress_bar.progress((index + 1) / total)

                        # Chặn xóa Admin (Safety check phía Client)
                        if str(user_id) == "0":
                            st.toast("🚫 Không thể xóa Admin (ID 0)!", icon="🛡️")
                            fail_count += 1
                            continue

                        # Gọi API xóa
                        success, _ = api_delete_user(user_id)
                        if success:
                            success_count += 1
                        else:
                            fail_count += 1
                    
                    time.sleep(0.5)
                    progress_bar.empty()

                    # Thông báo kết quả
                    if fail_count == 0:
                        st.success(f"✅ Đã xóa thành công {success_count} user!")
                    else:
                        st.warning(f"Đã xóa {success_count} user. Thất bại {fail_count} (Có thể do Admin hoặc lỗi mạng).")
                    
                    time.sleep(1)
                    st.rerun() # Load lại trang để cập nhật bảng
            
        else:
            st.info("Danh sách trống.")

# --- MAIN DASHBOARD VIEW ---
def show_dashboard():
    user = st.session_state['user_info']
    is_admin = str(user.get('user_id')) == '0' or user.get('role') == 'admin'

    with st.sidebar:
        if is_admin:
            st.error("🛑 CHẾ ĐỘ ADMIN")
        st.header(f"👤 {user.get('username', 'User')}")
        st.caption(f"ID: {user.get('user_id')}")
        st.divider()
        st.metric("Số dư USD", f"${user.get('usd', 0):,.2f}")
        st.metric("Số dư BTC", f"{user.get('btc', 0):.6f} BTC")
        st.divider()
        if st.button("Đăng xuất", use_container_width=True):
            st.session_state['user_info'] = None
            st.session_state['is_admin'] = False
            st.rerun()

    st.title("📈 Sàn Giao Dịch")
    tab_names = ["Giao dịch", "Lịch sử"]
    if is_admin:
        tab_names.append("🔧 Admin Panel")
    
    tabs = st.tabs(tab_names)

    with tabs[0]:
        col_trade_1, col_trade_2 = st.columns(2)
        with col_trade_1:
            st.subheader("Đặt lệnh Mua/Bán")
            # Form đặt lệnh (Sẽ kết nối API trade sau)
            trade_type = st.radio("Loại lệnh", ["MUA (Buy)", "BÁN (Sell)"], horizontal=True)
            amount = st.number_input("Số lượng (USD hoặc BTC)", min_value=0.0)
            if st.button("Gửi lệnh", use_container_width=True):
                st.toast(f"Đang gửi lệnh {trade_type} - Chức năng đang phát triển...")
        
        with col_trade_2:
            st.subheader("Thị trường")
            st.info("Biểu đồ nến sẽ hiển thị ở đây")

    with tabs[1]:
        st.write("Chưa có lịch sử giao dịch.")

    if is_admin:
        with tabs[2]:
            show_admin_panel()