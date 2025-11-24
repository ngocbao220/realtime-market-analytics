import streamlit as st
from src.kline_api import api_get_kline
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh # <-- Import thư viện này

def show_chart():
    # 1. CẤU HÌNH AUTO REFRESH
    # interval=2000 nghĩa là cứ 2000ms (2 giây) sẽ chạy lại hàm này 1 lần
    st_autorefresh(interval=2000, key="chart_autofresh")

    st.markdown("### Biểu đồ giá (Kline / Volume)")
    
    # Layout điều khiển nằm ngang cho gọn
    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        chart_type = st.selectbox("Loại biểu đồ:", ["Nến (Kline)", "Line", "Volume"])
    with c2:
        symbol = st.selectbox("Cặp giao dịch", ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"], key="chart_symbol")
    with c3:
        interval = st.selectbox("Interval", ["1m", "5m", "15m", "1h"], key="chart_interval")

    # Gọi API (Backend đã gộp ClickHouse + Redis)
    kline_data = api_get_kline(symbol, interval)

    if kline_data:
        df = pd.DataFrame(kline_data)
        
        # CHUYỂN ĐỔI TIMESTAMP SANG DATETIME ĐỂ PLOTLY HIỂU
        df['timestamp'] = pd.to_datetime(df['timestamp'])

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
            # Tắt thanh trượt range slider cho gọn
            fig.update_layout(xaxis_rangeslider_visible=False, height=450, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == "Line":
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], mode='lines', name='Close', line=dict(color='#F0B90B')))
            fig.update_layout(height=450, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)

        elif chart_type == "Volume":
            fig = go.Figure()
            # Tô màu volume xanh/đỏ tùy theo nến tăng hay giảm
            colors = ['#0ECB81' if c >= o else '#F6465D' for c, o in zip(df['close'], df['open'])]
            fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], name='Volume', marker_color=colors))
            fig.update_layout(height=450, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
            
    else:
        st.info("⏳ Đang chờ dữ liệu...")