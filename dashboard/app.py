import streamlit as st
import pandas as pd
import time # Nhớ import time để dùng sleep
from chart import show_chart
from components.login import show_login

# [QUAN TRỌNG] Import thêm các hàm quản lý lệnh từ api_client
from services.api_client import (
    api_get_balance,
    api_place_order,
    get_tickers,
    get_recent_trades,
    get_orderbook,
    get_my_open_orders, # Hàm mới lấy danh sách lệnh
    api_cancel_order    # Hàm mới hủy lệnh
)

def main():
    st.set_page_config(layout="wide", page_title="Crypto Dashboard")
    
    if st.session_state.get('user_info') and 'user_id' in st.session_state['user_info']:
        user_id = st.session_state['user_info']['user_id']
        refreshed_user = api_get_balance(user_id)
        # Cập nhật lại session nếu lấy được số dư mới
        if refreshed_user and "usd" in refreshed_user:
            st.session_state['user_info'] = refreshed_user
        
        user = st.session_state['user_info']

        # --- SIDEBAR ---
        with st.sidebar:
            st.markdown(f"## <span style='color:#b5b5b5'>👤 {user.get('username', 'User')}</span>", unsafe_allow_html=True)
            st.write(f"ID: {user.get('user_id')}")
            st.markdown("---")
            st.write("Số dư USD")
            st.markdown(f"<h2>${user.get('usd', 0):,.2f}</h2>", unsafe_allow_html=True)
            st.write("Số dư BTC") # Bạn có thể sửa thành hiển thị coin khác nếu muốn
            st.markdown(f"<h2>{user.get('btc', 0):.6f} BTC</h2>", unsafe_allow_html=True)
            st.markdown("---")
            if st.button("Đăng xuất"):
                st.session_state['user_info'] = None
                st.rerun()

        # --- LẤY SYMBOL HIỆN TẠI ---
        current_symbol = st.session_state.get("chart_symbol", "BTCUSDT")

        # --- HEADER (Giữ nguyên code hiển thị giá của bạn) ---
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
            ob_data = get_orderbook(current_symbol)
            
            # Hiển thị Asks (Bán)
            asks = ob_data.get("asks", [])
            if asks:
                df_asks = pd.DataFrame(asks, columns=["Giá", "Lượng"])
                df_asks = df_asks.sort_values(by="Giá", ascending=False).tail(8)
            else:
                df_asks = pd.DataFrame(columns=["Giá", "Lượng"])
            st.markdown(f"<div style='text-align:center; color:#F6465D; font-weight:bold;'>Bán (Asks) - {current_symbol}</div>", unsafe_allow_html=True)
            st.dataframe(df_asks, height=200, use_container_width=True, hide_index=True, column_config={"Giá": st.column_config.TextColumn(width="small"), "Lượng": st.column_config.TextColumn(width="medium")})

            st.markdown("---")

            # Hiển thị Bids (Mua)
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
            
            # --- Form Đặt Lệnh ---
            st.markdown("<div style='display:flex;gap:24px;'>", unsafe_allow_html=True)
            col_buy, col_sell = st.columns(2)
            
            # [LOGIC MỚI] Tự động xác định tên Coin từ Symbol (VD: ETHUSDT -> ETH)
            base_currency = current_symbol.replace("USDT", "") # VD: "ETH", "BTC", "SOL"
            base_key = base_currency.lower() # VD: "eth", "btc" (để khớp với key trong Redis)
            
            # === CỘT MUA (BUY) ===
            with col_buy:
                st.markdown("<div style='background:#23272f;padding:18px 24px;border-radius:14px;margin-bottom:12px;'>", unsafe_allow_html=True)
                st.markdown(f"<div style='font-size:18px;font-weight:700;color:#00c076;margin-bottom:4px;'>Mua {base_currency}</div>", unsafe_allow_html=True)
                
                # Hiển thị số dư USD
                st.markdown(f"<div style='color:#aaa;font-size:15px;'>Số dư khả dụng: <span style='color:#fff'>{user.get('usd', 0):,.2f} USD</span></div>", unsafe_allow_html=True)
                
                buy_price = st.number_input("Giá mua (USD)", value=price, step=1.0, format="%.2f", key="buy_price")
                buy_amount = st.number_input(f"Số lượng ({base_currency})", value=0.0, step=0.001, format="%.4f", key="buy_amount")
                
                # Tính tổng tiền
                total_buy = buy_price * buy_amount
                st.caption(f"Tổng chi: ${total_buy:,.2f}")

                if st.button(f"Mua {base_currency}", key="buy_btn", type="primary", use_container_width=True):
                    if buy_price <= 0 or buy_amount <= 0:
                        st.warning("Số lượng và giá phải lớn hơn 0")
                    else:
                        with st.spinner("Đang gửi lệnh..."):
                            success, msg = api_place_order(user.get('user_id'), current_symbol, "buy", buy_price, buy_amount)
                            if success:
                                txt = msg.get('message', 'Thành công') if isinstance(msg, dict) else str(msg)
                                st.success(f"✅ {txt}")
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(f"❌ Lỗi: {msg}")
                st.markdown("</div>", unsafe_allow_html=True)

            # === CỘT BÁN (SELL) ===
            with col_sell:
                st.markdown("<div style='background:#23272f;padding:18px 24px;border-radius:14px;margin-bottom:12px;'>", unsafe_allow_html=True)
                st.markdown(f"<div style='font-size:18px;font-weight:700;color:#f6465d;margin-bottom:4px;'>Bán {base_currency}</div>", unsafe_allow_html=True)
                
                # [LOGIC MỚI] Lấy số dư động theo coin đang chọn
                # Nếu chọn ETHUSDT -> Lấy user.get('eth', 0)
                balance_coin = user.get(base_key, 0.0) 
                
                st.markdown(f"<div style='color:#aaa;font-size:15px;'>Số dư khả dụng: <span style='color:#fff'>{balance_coin:.6f} {base_currency}</span></div>", unsafe_allow_html=True)
                
                sell_price = st.number_input("Giá bán (USD)", value=price, step=1.0, format="%.2f", key="sell_price")
                sell_amount = st.number_input(f"Số lượng ({base_currency})", value=0.0, step=0.001, format="%.4f", key="sell_amount")
                
                total_sell = sell_price * sell_amount
                st.caption(f"Tổng thu: ${total_sell:,.2f}")

                if st.button(f"Bán {base_currency}", key="sell_btn", type="primary", use_container_width=True):
                    if sell_price <= 0 or sell_amount <= 0:
                        st.warning("Số lượng và giá phải lớn hơn 0")
                    else:
                        with st.spinner("Đang gửi lệnh..."):
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

        # --- CỘT PHẢI: MARKET INFO & TRADES ---
        with col_right:
            st.markdown("<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;margin-top:12px;'>Giá các đồng (Markets)</div>", unsafe_allow_html=True)
            
            tickers = get_tickers()
            if tickers:
                df_markets = pd.DataFrame(tickers)
                st.dataframe(
                    df_markets,
                    column_config={
                        "symbol": "Cặp",
                        "price": st.column_config.NumberColumn("Giá", format="$%.4f"),
                        "change": st.column_config.NumberColumn("24h %", format="%.2f%%")
                    },
                    height=200, use_container_width=True, hide_index=True
                )
            else:
                st.info("Đang tải giá thị trường...")

            st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

            st.markdown(f"<div style='font-size:22px;font-weight:700;color:#fff;margin-bottom:8px;text-align:center;'>Giao dịch khớp lệnh ({current_symbol})</div>", unsafe_allow_html=True)
            
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
                    height=300, use_container_width=True, hide_index=True
                )
            else:
                st.info("Chưa có giao dịch khớp lệnh.")

        # ==================================================
        # BẢNG QUẢN LÝ LỆNH CỦA TÔI (ORDER MANAGEMENT)
        # ==================================================
        st.divider()
        st.markdown("### 📋 Lệnh chờ khớp của tôi (Open Orders)")
        
        # Gọi API lấy danh sách lệnh đang treo
        my_orders = get_my_open_orders(user.get('user_id'))
        
        if my_orders:
            # Tạo DataFrame hiển thị
            df_orders = pd.DataFrame(my_orders)
            
            # Chọn các cột cần thiết
            # Giả sử API trả về: order_id, symbol, side, price, amount, status, time...
            
            # Hiển thị từng lệnh (Dùng columns để tạo layout giống bảng có nút Hủy)
            # Header của bảng tự chế
            h1, h2, h3, h4, h5, h6 = st.columns([1.5, 1, 1.5, 1.5, 1.5, 1])
            h1.markdown("**Cặp coin**")
            h2.markdown("**Loại**")
            h3.markdown("**Giá đặt**")
            h4.markdown("**Số lượng**")
            h5.markdown("**Trạng thái**")
            h6.markdown("**Thao tác**")
            st.markdown("---")

            for index, row in df_orders.iterrows():
                c1, c2, c3, c4, c5, c6 = st.columns([1.5, 1, 1.5, 1.5, 1.5, 1])
                
                # Màu sắc cho Mua/Bán
                side_color = "#00c076" if row.get('side') in ['buy', 'bids'] else "#f6465d"
                side_text = "MUA" if row.get('side') in ['buy', 'bids'] else "BÁN"
                
                with c1: st.write(row.get('symbol', ''))
                with c2: st.markdown(f"<span style='color:{side_color};font-weight:bold'>{side_text}</span>", unsafe_allow_html=True)
                with c3: st.write(f"${float(row.get('price', 0)):,.2f}")
                with c4: st.write(f"{float(row.get('amount', 0)):.4f}")
                with c5: st.write(row.get('status', 'NEW'))
                with c6:
                    # Nút Hủy Lệnh
                    if st.button("Hủy", key=f"cancel_{row.get('order_id')}"):
                        with st.spinner("Đang hủy..."):
                            success, msg = api_cancel_order(row.get('order_id'), user.get('user_id'))
                            if success:
                                st.success("Đã hủy lệnh!")
                                time.sleep(0.5)
                                st.rerun()
                            else:
                                st.error(f"Lỗi: {msg}")
                st.markdown("<hr style='margin: 5px 0; opacity: 0.2;'>", unsafe_allow_html=True)
        else:
            st.info("Bạn hiện không có lệnh nào đang chờ khớp.")

    else:
        st.session_state['user_info'] = None
        show_login()

if __name__ == "__main__":
    main()