import streamlit as st
import pandas as pd
from chart import show_chart
from components.login import show_login, api_get_balance
from src.market_api import api_get_tickers, api_get_recent_trades 
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

        # --- HEADER: HIỂN THỊ GIÁ THẬT ---
        # 1. Xác định Symbol đang xem (mặc định BTCUSDT)
        current_symbol = st.session_state.get("chart_symbol", "BTCUSDT")
        
        # 2. Lấy dữ liệu Ticker
        all_tickers = api_get_tickers()
        # Tìm data của symbol hiện tại
        ticker = next((item for item in all_tickers if item["symbol"] == current_symbol), None)

        if ticker:
            price = ticker['price']
            change = ticker['change']
            high = ticker['high']
            low = ticker['low']
            vol = ticker['volume']
            
            # Màu sắc biến động
            color_change = "#00c076" if change >= 0 else "#f6465d"
            sign = "+" if change >= 0 else ""
            
            # HTML Header được thiết kế lại (Binance Style)
            st.markdown(
                f"""
                <div style='
                    background: #181a20;
                    padding: 20px 40px;
                    border-radius: 16px;
                    margin-bottom: 20px;
                    display: flex;
                    align-items: center;
                    justify-content: space-between;
                    box-shadow: 0 4px 12px rgba(0,0,0,0.3);
                    flex-wrap: wrap;
                    gap: 20px;
                '>
                    <!-- PHẦN 1: CẶP TIỀN -->
                    <div style='display:flex; flex-direction:column;'>
                        <span style='font-size:32px; font-weight:800; color:#f0b90b; letter-spacing:1px; line-height:1.1;'>{current_symbol}</span>
                        <span style='font-size:14px; color:#848e9c; font-weight:500; margin-top:4px;'>Giá thị trường</span>
                    </div>

                    <!-- PHẦN 2: GIÁ & % -->
                    <div style='display:flex; align-items:baseline; gap:12px;'>
                        <span style='font-size:40px; font-weight:700; color:#fff;'>${price:,.2f}</span>
                        <span style='color:{color_change}; font-size:20px; font-weight:600;'>{sign}{change}%</span>
                    </div>
                    
                    <!-- PHẦN 3: THÔNG SỐ 24H (Dạng cột) -->
                    <div style='display:flex; gap:32px; align-items:center;'>
                        <div style='display:flex; flex-direction:column; align-items:flex-end;'>
                            <span style='color:#848e9c; font-size:12px; font-weight:500; margin-bottom:2px;'>Cao 24h</span>
                            <span style='color:#00c076; font-size:16px; font-weight:600;'>${high:,.2f}</span>
                        </div>
                        <div style='display:flex; flex-direction:column; align-items:flex-end;'>
                            <span style='color:#848e9c; font-size:12px; font-weight:500; margin-bottom:2px;'>Thấp 24h</span>
                            <span style='color:#f6465d; font-size:16px; font-weight:600;'>${low:,.2f}</span>
                        </div>
                        <div style='display:flex; flex-direction:column; align-items:flex-end;'>
                            <span style='color:#848e9c; font-size:12px; font-weight:500; margin-bottom:2px;'>Vol 24h</span>
                            <span style='color:#fff; font-size:16px; font-weight:600;'>{vol:,.2f}</span>
                        </div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
        else:
             # Fallback nếu chưa tải được dữ liệu
             st.info(f"Đang tải dữ liệu cho {current_symbol}...")

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

        # ==================================================
        # CỘT PHẢI: MARKET INFO & TRADES (REALTIME DATA)
        # ==================================================
        with col_right:
            # 1. BẢNG GIÁ CÁC ĐỒNG (MARKETS)
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;margin-top:12px;'>Giá các đồng (Markets)</div>", unsafe_allow_html=True)
            
            # Gọi API lấy dữ liệu thật
            tickers = api_get_tickers()
            
            if tickers:
                df_markets = pd.DataFrame(tickers)
                # Format màu sắc cho % biến động
                # Chúng ta sẽ dùng style của pandas sau, ở đây chuẩn bị dữ liệu
                
                # Hiển thị bảng
                st.dataframe(
                    df_markets,
                    column_config={
                        "symbol": "Cặp",
                        "price": st.column_config.NumberColumn("Giá", format="$%.4f"),
                        "change": st.column_config.NumberColumn("24h %", format="%.2f%%")
                    },
                    height=200,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Đang tải giá thị trường...")

            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

            # 2. LỊCH SỬ GIAO DỊCH (TRADES HISTORY)
            st.markdown(f"<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;'>Giao dịch khớp lệnh ({current_symbol})</div>", unsafe_allow_html=True)
            
            # Gọi API lấy trade thật của cặp đang chọn
            recent_trades = api_get_recent_trades(current_symbol)
            
            if recent_trades:
                df_trades = pd.DataFrame(recent_trades)
                
                # Tạo màu cho giá dựa trên là Mua hay Bán (IsBuyerMaker=True -> Taker bán -> Màu đỏ)
                def color_price(row):
                    color = "#F6465D" if row['is_buyer_maker'] else "#0ECB81"
                    return f'color: {color}'

                # Hiển thị
                st.dataframe(
                    df_trades[["price", "amount", "time"]], # Chọn cột
                    column_config={
                        "price": st.column_config.NumberColumn("Giá", format="%.2f"),
                        "amount": st.column_config.NumberColumn("Số lượng", format="%.5f"),
                        "time": "Thời gian"
                    },
                    height=300,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Chưa có giao dịch khớp lệnh.")

    else:
        st.session_state['user_info'] = None
        show_login()

if __name__ == "__main__":
    main()