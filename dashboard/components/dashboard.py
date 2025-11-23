import streamlit as st
import requests
import pandas as pd
import time
from config import API_BASE_URL
from src.user_api import api_get_all_users, api_delete_user
from src.order_api import api_place_order

# --- COMPONENT: GIAO DIỆN ADMIN ---
def show_admin_panel():
    st.header("🛠️ Admin Control Panel")
    
    admin_tab1, admin_tab2 = st.tabs(["👥 Quản lý User", "Giao dịch hệ thống"])
    
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
                        progress_bar.progress((index) / total)

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
    with admin_tab2:
        col_buy, col_sell = st.columns(2)
        with col_buy:
            st.subheader("🟢 Bid (Mua)")
            buy_orders = requests.get(f"{API_BASE_URL}/orders/book/buy").json()
            st.dataframe(buy_orders) # Hoặc vẽ bảng tùy chỉnh

        with col_sell:
            st.subheader("🔴 Ask (Bán)")
            sell_orders = requests.get(f"{API_BASE_URL}/orders/book/sell").json()
            st.dataframe(sell_orders)
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
        col_trade_1, col_trade_2 = st.columns([1, 1.5]) # Chia cột: Đặt lệnh nhỏ hơn thị trường chút
        
        # --- CỘT TRÁI: FORM ĐẶT LỆNH ---
        with col_trade_1:
            st.subheader("📝 Đặt lệnh")
            
            # 1. Chọn Mua hoặc Bán
            trade_side = st.radio(
                "Bạn muốn làm gì?", 
                ["MUA (Buy)", "BÁN (Sell)"], 
                horizontal=True
            )
            
            # Xác định màu sắc và biến side
            if "MUA" in trade_side:
                side_api = "buy"
                btn_color = "primary" # Xanh/Đậm
                balance_text = f"Số dư khả dụng: {user.get('usd', 0):,.2f} USD"
            else:
                side_api = "sell"
                btn_color = "secondary" # Xám/Nhạt (hoặc đỏ nếu config theme)
                balance_text = f"Số dư khả dụng: {user.get('btc', 0):.6f} BTC"

            st.caption(balance_text)

            # 2. Form nhập liệu
            with st.form("order_form"):
                # Giá (USD)
                price_input = st.number_input(
                    "Giá đặt (USD)", 
                    min_value=0.0, 
                    value=50000.0, 
                    step=100.0,
                    format="%.2f"
                )
                
                # Số lượng (BTC)
                amount_input = st.number_input(
                    "Số lượng (BTC)", 
                    min_value=0.0, 
                    value=0.1, 
                    step=0.01,
                    format="%.6f"
                )
                
                # Tính tổng tiền dự kiến
                total_est = price_input * amount_input
                st.markdown(f"**Tổng tiền:** `{total_est:,.2f} USD`")
                
                # Nút gửi lệnh
                submitted = st.form_submit_button(
                    f"🚀 {trade_side.split(' ')[0]} BTC", 
                    type=btn_color,
                    use_container_width=True
                )
                
                if submitted:
                    if amount_input <= 0 or price_input <= 0:
                        st.warning("Giá và số lượng phải lớn hơn 0.")
                    else:
                        # Gọi API thật
                        with st.spinner("Đang gửi lệnh lên sàn..."):
                            success, result = api_place_order(
                                user_id=user.get('user_id'),
                                side=side_api,
                                price=price_input,
                                amount=amount_input
                            )
                        
                        if success:
                            st.success(f"✅ Đặt lệnh thành công! Order ID: {result.get('order_id')}")
                            time.sleep(1)
                            st.rerun() # Refresh để cập nhật số dư (nếu backend có trừ tiền)
                        else:
                            st.error(f"❌ Thất bại: {result}")

        # --- CỘT PHẢI: THỊ TRƯỜNG (ORDER BOOK) ---
        with col_trade_2:
            st.subheader("📊 Sổ lệnh (Order Book)")
            
            # Tạo 2 tab con cho Sổ Mua và Sổ Bán
            ob_tab1, ob_tab2 = st.tabs(["Người bán (Sell)", "Người mua (Buy)"])
            
            # Hàm hiển thị bảng phụ trợ
            def show_book(side_endpoint, color_highlight):
                try:
                    res = requests.get(f"{API_BASE_URL}/orders/book/{side_endpoint}")
                    orders = res.json()
                    if orders:
                        df_ob = pd.DataFrame(orders)
                        # Lọc cột cần thiết
                        df_show = df_ob[["price", "amount", "user_id"]]
                        df_show.columns = ["Giá (USD)", "SL (BTC)", "User ID"]
                        
                        # Style bảng (Highlight giá)
                        st.dataframe(
                            df_show.style.format({"Giá (USD)": "{:,.2f}", "SL (BTC)": "{:.6f}"}),
                            use_container_width=True,
                            height=300
                        )
                    else:
                        st.info("Chưa có lệnh nào đang chờ.")
                except:
                    st.warning("Không kết nối được OrderBook API")

            with ob_tab1:
                st.caption("Danh sách người đang bán giá rẻ nhất:")
                show_book("sell", "red") # API: /orders/book/sell

            with ob_tab2:
                st.caption("Danh sách người đang mua giá cao nhất:")
                show_book("buy", "green") # API: /orders/book/buy
    with tabs[1]:
        st.subheader("🕰️ Khớp lệnh gần đây")
        
        # Nút làm mới
        if st.button("Refresh History"):
            st.rerun()

        try:
            # Gọi API lấy lịch sử
            res = requests.get(f"{API_BASE_URL}/trades/history")
            trades = res.json()
            
            if trades:
                df_history = pd.DataFrame(trades)
                
                # Format thời gian
                df_history["Time"] = pd.to_datetime(df_history["timestamp"], unit='s')
                
                # Chọn cột hiển thị
                df_show = df_history[["Time", "price", "amount", "buyer_id", "seller_id"]]
                df_show.columns = ["Thời gian", "Giá Khớp", "SL BTC", "Người Mua", "Người Bán"]
                
                st.dataframe(
                    df_show.style.format({"Giá Khớp": "{:,.2f}", "SL BTC": "{:.6f}"}),
                    use_container_width=True
                )
            else:
                st.info("Chưa có giao dịch nào được khớp.")
        except Exception as e:
            st.error(f"Lỗi tải lịch sử: {e}")
    if is_admin:
        with tabs[2]:
            show_admin_panel()