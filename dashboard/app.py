import streamlit as st
import pandas as pd
from chart import show_chart
from components.login import show_login, api_get_balance
from components.dashboard import show_dashboard
from src.orderbook_api import api_get_orderbook

def main():
    st.set_page_config(layout="wide")
    if st.session_state.get('user_info') and 'user_id' in st.session_state['user_info']:
        user_id = st.session_state['user_info']['user_id']
        refreshed_user = api_get_balance(user_id)
        if refreshed_user and 'user_id' in refreshed_user:
            st.session_state['user_info'] = refreshed_user

        # Sidebar: Tài khoản
        with st.sidebar:
            user = st.session_state['user_info']
            st.markdown(f"## <span style='color:#b5b5b5'>👤 {user.get('username', 'User')}</span>", unsafe_allow_html=True)
            st.write(f"ID: {user.get('user_id')}")
            st.markdown("---")
            st.write("Số dư USD")
            st.markdown(f"<h2>${user.get('usd', 0):,.2f}</h2>", unsafe_allow_html=True)
            st.write("Số dư BTC")
            st.markdown(f"<h2>{user.get('btc', 0):.6f} BTC</h2>", unsafe_allow_html=True)
            st.markdown("---")
            if st.button("Đăng xuất"):
                st.session_state['user_info'] = None
                st.rerun()

        # Thông tin cặp tiền ảo trải dài phía trên
        st.markdown(
            """
            <div style='background:#181a20;padding:18px 32px 18px 32px;border-radius:16px;margin-bottom:18px;display:flex;align-items:center;justify-content:center;gap:56px;box-shadow:0 2px 12px #0003;'>
                <div style='display:flex;flex-direction:column;align-items:center;'>
                    <span style='font-size:32px;font-weight:700;color:#f0b90b;letter-spacing:1px;'>BTC/USDT</span>
                    <span style='font-size:14px;color:#aaa;'>Giá Bitcoin</span>
                </div>
                <span style='font-size:38px;font-weight:700;color:#fff;background:#222;padding:6px 24px;border-radius:12px;'>$87,609.67</span>
                <span style='color:#00c076;font-size:22px;font-weight:700;'>+1.97%</span>
                <span style='color:#fff;font-size:18px;'>Cao nhất 24h: <span style='color:#00c076;font-weight:600;'>$88,127.64</span></span>
                <span style='color:#fff;font-size:18px;'>Thấp nhất 24h: <span style='color:#f6465d;font-weight:600;'>$85,420</span></span>
                <span style='color:#fff;font-size:18px;'>Volume 24h: <span style='color:#00c076;font-weight:600;'>20,022 BTC</span></span>
            </div>
            """,
            unsafe_allow_html=True
        )

        # Layout chính chia thành 3 cột, không có phân vùng, chế độ xem, bảng phụ
        col_left, col_center, col_right = st.columns([2.2, 5, 2.8], gap="large")

  # ==================================================
        # CỘT TRÁI: ORDER BOOK (ĐÃ SỬA THÀNH DỮ LIỆU THẬT)
        # ==================================================
        with col_left:
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;margin-top:12px;'>Sổ lệnh (Order Book)</div>", unsafe_allow_html=True)
            
            # 1. Lấy Symbol hiện tại (đồng bộ với biểu đồ)
            # Mặc định là BTCUSDT nếu chưa chọn gì
            current_symbol = st.session_state.get("chart_symbol", "BTCUSDT")
            
            # 2. Gọi API lấy dữ liệu thật
            ob_data = api_get_orderbook(current_symbol)
            
            # 3. Xử lý hiển thị ASKS (Người bán - Màu đỏ)
            # Sắp xếp giá từ cao xuống thấp, lấy 5 lệnh gần nhất
            asks = ob_data.get("asks", [])
            if asks:
                df_asks = pd.DataFrame(asks, columns=["Giá", "Lượng"])
                df_asks = df_asks.sort_values(by="Giá", ascending=False).tail(8) # Lấy 8 giá thấp nhất (gần giá khớp)
            else:
                df_asks = pd.DataFrame(columns=["Giá", "Lượng"])

            st.markdown(f"<div style='text-align:center; color:#F6465D; font-weight:bold;'>Bán (Asks) - {current_symbol}</div>", unsafe_allow_html=True)
            st.dataframe(
                df_asks, 
                height=200, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Giá": st.column_config.TextColumn(help="Giá bán", width="small"),
                    "Lượng": st.column_config.TextColumn(help="Số lượng", width="medium"),
                }
            )

            # 4. Hiển thị giá khớp lệnh (Ở giữa) - Có thể lấy từ nến gần nhất
            # Tạm thời để divider
            st.markdown("---")

            # 5. Xử lý hiển thị BIDS (Người mua - Màu xanh)
            bids = ob_data.get("bids", [])
            if bids:
                df_bids = pd.DataFrame(bids, columns=["Giá", "Lượng"])
                df_bids = df_bids.sort_values(by="Giá", ascending=False).head(8) # Lấy 8 giá cao nhất
            else:
                df_bids = pd.DataFrame(columns=["Giá", "Lượng"])

            st.markdown(f"<div style='text-align:center; color:#0ECB81; font-weight:bold;'>Mua (Bids) - {current_symbol}</div>", unsafe_allow_html=True)
            st.dataframe(
                df_bids, 
                height=200, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Giá": st.column_config.TextColumn(help="Giá mua", width="small"),
                    "Lượng": st.column_config.TextColumn(help="Số lượng", width="medium"),
                }
            )
        # Trung tâm: Biểu đồ, đặt lệnh mua/bán
        with col_center:
            show_chart()
            st.markdown("</div>", unsafe_allow_html=True)
            st.markdown("<div style='display:flex;gap:24px;'>", unsafe_allow_html=True)
            col_buy, col_sell = st.columns(2)
            with col_buy:
                st.markdown("<div style='background:#23272f;padding:18px 24px;border-radius:14px;margin-bottom:12px;'>", unsafe_allow_html=True)
                st.markdown("<div style='font-size:18px;font-weight:700;color:#00c076;margin-bottom:4px;'>Đặt lệnh Mua (Buy)</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='color:#aaa;font-size:15px;'>Số dư khả dụng: <span style='color:#fff'>{user.get('usd', 0):,.2f} USD</span></div>", unsafe_allow_html=True)
                st.number_input("Giá đặt mua (USD)", value=50000.00, step=0.01, key="buy_price")
                st.number_input("Số lượng mua (BTC)", value=0.01, step=0.01, key="buy_amount")
                st.button("Giao dịch", key="buy_btn")
                st.markdown("</div>", unsafe_allow_html=True)
            with col_sell:
                st.markdown("<div style='background:#23272f;padding:18px 24px;border-radius:14px;margin-bottom:12px;'>", unsafe_allow_html=True)
                st.markdown("<div style='font-size:18px;font-weight:700;color:#f6465d;margin-bottom:4px;'>Đặt lệnh Bán (Sell)</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='color:#aaa;font-size:15px;'>Số dư khả dụng: <span style='color:#fff'>{user.get('btc', 0):.6f} BTC</span></div>", unsafe_allow_html=True)
                st.number_input("Giá đặt bán (USD)", value=50000.00, step=0.01, key="sell_price")
                st.number_input("Số lượng bán (BTC)", value=0.01, step=0.01, key="sell_amount")
                st.button("Lịch sử giao dịch", key="sell_btn")
                st.markdown("</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # Bên phải: bảng giá các đồng và lịch sử trades
        with col_right:
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;'>Giá các đồng (Markets)</div>", unsafe_allow_html=True)
            markets_data = [
                ["OG/USDT", "$1.23900", "-14.26%"],
                ["1000CAT/USDT", "$0.00328", "+2.17%"],
                ["1000CHEEMS/USDT", "$0.00115", "+1.95%"],
            ]
            markets_df = pd.DataFrame(markets_data, columns=["Cặp", "Giá", "Biến động"])
            st.dataframe(markets_df, height=140, use_container_width=True, hide_index=True)
            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;'>Lịch sử giao dịch (Trades)</div>", unsafe_allow_html=True)
            trades_data = [
                ["87,609.67", "0.00112", "10:14:07"],
                ["87,609.67", "0.00035", "10:14:06"],
                ["87,609.50", "0.00450", "10:14:05"],
            ]
            trades_df = pd.DataFrame(trades_data, columns=["Giá", "Số lượng (BTC)", "Thời gian"])
            st.dataframe(trades_df, height=140, use_container_width=True, hide_index=True)
    else:
        st.session_state['user_info'] = None
        show_login()

if __name__ == "__main__":
    main()