import streamlit as st
from src.kline_api import api_get_kline
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

def show_chart():
    # 1. Auto Refresh mỗi 2 giây
    st_autorefresh(interval=3000, key="chart_refresh")

    
    
    # Menu chọn
    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        chart_type = st.selectbox("Loại biểu đồ:", ["Nến (Kline)", "Line", "Volume"])
    with c2:
        symbol = st.selectbox("Cặp giao dịch", ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"], key="chart_symbol")
    with c3:
        interval = st.selectbox("Interval", ["1m", "5m", "15m", "1h"], key="chart_interval")

    # 2. Gọi API (Backend đã lo việc gộp Lịch sử + Realtime)
    kline_data = api_get_kline(symbol, interval)

    if kline_data and len(kline_data) > 0:
        df = pd.DataFrame(kline_data)
        
        # Convert thời gian
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # --- QUAN TRỌNG: XỬ LÝ TRÙNG LẶP ---
        # Nếu Backend trả về trùng nến (do gộp ClickHouse & Redis), ta giữ cái mới nhất
        df = df.drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)

        # Hiển thị số lượng nến đang có để debug
        st.caption(f"Đang hiển thị: {len(df)} cây nến từ Backend")

        # Vẽ biểu đồ
        if chart_type == "Nến (Kline)":
            fig = go.Figure(data=[go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                increasing_line_color='#0ECB81',
                decreasing_line_color='#F6465D'
            )])
            # Tắt thanh trượt range slider để tránh bị tự động zoom vào vùng nhỏ
            fig.update_layout(xaxis_rangeslider_visible=False, height=450)
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == "Line":
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], mode='lines', line=dict(color='#F0B90B')))
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == "Volume":
            fig = go.Figure()
            colors = ['#0ECB81' if c >= o else '#F6465D' for c, o in zip(df['close'], df['open'])]
            fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], marker_color=colors))
            st.plotly_chart(fig, use_container_width=True)
            
    else:
        st.info("Chưa có dữ liệu. Vui lòng đợi Spark xử lý...")