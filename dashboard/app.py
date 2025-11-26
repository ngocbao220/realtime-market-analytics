import streamlit as st
import time
import pandas as pd
from chart import show_chart
from components.login import show_login

# [QUAN TRỌNG] Import mới từ services/api_client.py
# Thay thế hoàn toàn cho src.market_api, src.orderbook_api, v.v.
from services.api_client import (
    api_get_balance,
    api_place_order,
    get_tickers,
    get_recent_trades,
    get_orderbook
)

def main():
    st.set_page_config(layout="wide", page_title="Crypto Dashboard")
    
    # Logic Auth
    if st.session_state.get('user_info') and 'user_id' in st.session_state['user_info']:
        user_id = st.session_state['user_info']['user_id']
        # Gọi API lấy số dư mới nhất
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
        current_symbol = st.session_state.get("chart_symbol", "BTCUSDT")
        
        # Gọi API lấy Ticker qua Service
        all_tickers = get_tickers()
        ticker = next((item for item in all_tickers if item["symbol"] == current_symbol), None)

        if ticker:
            price = ticker.get('price', 0)
            change = ticker.get('change', 0)
            high = ticker.get('high', 0)
            low = ticker.get('low', 0)
            vol = ticker.get('volume', 0)
            
            color_change = "#00c076" if change >= 0 else "#f6465d"
            sign = "+" if change >= 0 else ""
            
            # HTML Header Binance Style
            st.markdown(
                f"""<div style='background: #181a20; padding: 20px 40px; border-radius: 16px; margin-bottom: 20px; display: flex; align-items: center; justify-content: space-between; box-shadow: 0 4px 12px rgba(0,0,0,0.3); flex-wrap: wrap; gap: 20px;'>
                <div style='display:flex; flex-direction:column;'>
                <span style='font-size:32px; font-weight:800; color:#f0b90b; letter-spacing:1px; line-height:1.1;'>{current_symbol}</span>
                <span style='font-size:14px; color:#848e9c; font-weight:500; margin-top:4px;'>Giá thị trường</span>
                </div>
                <div style='display:flex; align-items:baseline; gap:12px;'>
                <span style='font-size:40px; font-weight:700; color:#fff;'>${price:,.2f}</span>
                <span style='color:{color_change}; font-size:20px; font-weight:600;'>{sign}{change}%</span>
                </div>
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
                </div>""",
                unsafe_allow_html=True
            )
        else:
             st.info(f"Đang tải dữ liệu cho {current_symbol}...")

        col_left, col_center, col_right = st.columns([2.2, 5, 2.8], gap="large")

        # --- CỘT TRÁI: ORDER BOOK ---
        with col_left:
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;margin-top:12px;'>Sổ lệnh (Order Book)</div>", unsafe_allow_html=True)
            
            # Gọi API Orderbook qua Service
            ob_data = get_orderbook(current_symbol)
            
            asks = ob_data.get("asks", [])
            if asks:
                df_asks = pd.DataFrame(asks, columns=["Giá", "Lượng"])
                df_asks = df_asks.sort_values(by="Giá", ascending=False).tail(8)
            else:
                df_asks = pd.DataFrame(columns=["Giá", "Lượng"])

            st.markdown(f"<div style='text-align:center; color:#F6465D; font-weight:bold;'>Bán (Asks) - {current_symbol}</div>", unsafe_allow_html=True)
            st.dataframe(df_asks, height=200, use_container_width=True, hide_index=True, column_config={"Giá": st.column_config.TextColumn(width="small"), "Lượng": st.column_config.TextColumn(width="medium")})

            st.markdown("---")

            bids = ob_data.get("bids", [])
            if bids:
                df_bids = pd.DataFrame(bids, columns=["Giá", "Lượng"])
                df_bids = df_bids.sort_values(by="Giá", ascending=False).head(8)
            else:
                df_bids = pd.DataFrame(columns=["Giá", "Lượng"])

            st.markdown(f"<div style='text-align:center; color:#0ECB81; font-weight:bold;'>Mua (Bids) - {current_symbol}</div>", unsafe_allow_html=True)
            st.dataframe(df_bids, height=200, use_container_width=True, hide_index=True, column_config={"Giá": st.column_config.TextColumn(width="small"), "Lượng": st.column_config.TextColumn(width="medium")})

        # --- CỘT GIỮA: BIỂU ĐỒ & FORM ĐẶT LỆNH ---
        with col_center:
            show_chart()
            st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
            

            # === [ĐÃ CẬP NHẬT] Form Đặt Lệnh ===
            st.markdown("<div style='display:flex;gap:24px;'>", unsafe_allow_html=True)
            col_buy, col_sell = st.columns(2)
            
            # FORM MUA
            with col_buy:
                st.markdown("<div style='background:#23272f;padding:18px 24px;border-radius:14px;margin-bottom:12px;'>", unsafe_allow_html=True)
                st.markdown("<div style='font-size:18px;font-weight:700;color:#00c076;margin-bottom:4px;'>Đặt lệnh Mua (Buy)</div>", unsafe_allow_html=True)
                st.markdown(f"<div style='color:#aaa;font-size:15px;'>Số dư khả dụng: <span style='color:#fff'>{user.get('usd', 0):,.2f} USD</span></div>", unsafe_allow_html=True)
                
                # Input giá và số lượng
                buy_price = st.number_input("Giá đặt mua (USD)", value=50000.00, step=10.0, key="buy_price")
                buy_amount = st.number_input("Số lượng mua (Coin)", value=0.01, step=0.001, format="%.4f", key="buy_amount")
                
                if st.button("Giao dịch Mua", key="buy_btn", type="primary", use_container_width=True):
                    with st.spinner("Đang gửi lệnh..."):
                        # Gọi API với thêm tham số current_symbol
                        success, msg = api_place_order(user.get('user_id'), current_symbol, "buy", buy_price, buy_amount)
                        
                        if success:
                            txt = msg.get('message', 'Thành công') if isinstance(msg, dict) else str(msg)
                            st.success(f"✅ {txt}")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ Lỗi: {msg}")
                st.markdown("</div>", unsafe_allow_html=True)

            # FORM BÁN
            with col_sell:
                st.markdown("<div style='background:#23272f;padding:18px 24px;border-radius:14px;margin-bottom:12px;'>", unsafe_allow_html=True)
                st.markdown("<div style='font-size:18px;font-weight:700;color:#f6465d;margin-bottom:4px;'>Đặt lệnh Bán (Sell)</div>", unsafe_allow_html=True)
                # Lưu ý: Hiển thị số dư Coin (BTC hoặc coin khác tương ứng với symbol)
                # Ở đây tạm để BTC, nếu muốn dynamic thì phải parse current_symbol (ví dụ lấy 'ETH' từ 'ETHUSDT')
                coin_name = "BTC" if "BTC" in current_symbol else "Coin" 
                balance_coin = user.get('btc', 0) # Cần logic lấy balance theo coin nếu mở rộng nhiều coin
                
                st.markdown(f"<div style='color:#aaa;font-size:15px;'>Số dư khả dụng: <span style='color:#fff'>{balance_coin:.6f} {coin_name}</span></div>", unsafe_allow_html=True)
                
                sell_price = st.number_input("Giá đặt bán (USD)", value=50000.00, step=10.0, key="sell_price")
                sell_amount = st.number_input("Số lượng bán (Coin)", value=0.01, step=0.001, format="%.4f", key="sell_amount")
                
                if st.button("Giao dịch Bán", key="sell_btn", type="primary", use_container_width=True):
                    with st.spinner("Đang gửi lệnh..."):
                        # Gọi API với thêm tham số current_symbol
                        success, msg = api_place_order(user.get('user_id'), current_symbol, "sell", sell_price, sell_amount)
                        
                        if success:
                            txt = msg.get('message', 'Thành công') if isinstance(msg, dict) else str(msg)
                            st.success(f"✅ {txt}")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"❌ Lỗi: {msg}")
                st.markdown("</div>", unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)

        # --- CỘT PHẢI: THÔNG TIN THỊ TRƯỜNG ---
        with col_right:
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;margin-top:12px;'>Giá các đồng (Markets)</div>", unsafe_allow_html=True)
            
            tickers = get_tickers() # Gọi Service
            if tickers:
                df_markets = pd.DataFrame(tickers)
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

            st.markdown(f"<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;'>Giao dịch khớp lệnh ({current_symbol})</div>", unsafe_allow_html=True)
            
            # Gọi Service lấy trade gần nhất
            recent_trades = get_recent_trades(current_symbol)
            
            if recent_trades:
                df_trades = pd.DataFrame(recent_trades)
                st.dataframe(
                    df_trades[["price", "amount", "time"]], 
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