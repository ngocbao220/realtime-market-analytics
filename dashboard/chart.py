import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

# [ĐÃ SỬA] Import từ service chung thay vì src cũ
from services.api_client import get_kline

def show_chart():
    # 1. Auto Refresh mỗi 3 giây
    st_autorefresh(interval=3000, key="chart_refresh")

    st.markdown(
        """
        <div style='background:#181a20; padding: 10px 24px; border-radius:14px; margin-bottom:12px;'>
            <h3 style='color: white; margin: 0; font-size: 22px;'>Biểu đồ giá (Kline / Volume)</h3>
        </div>
        """,
        unsafe_allow_html=True
    )
    
    # Menu chọn
    c1, c2, c3 = st.columns([2, 2, 2])
    with c1:
        chart_type = st.selectbox("Loại biểu đồ:", ["Nến (Kline)", "Line", "Volume"])
    with c2:
        symbol = st.selectbox("Cặp giao dịch", ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "DOGEUSDT"], key="chart_symbol")
    with c3:
        interval = st.selectbox("Interval", ["1m", "5m", "15m", "1h"], key="chart_interval")

    # 2. Gọi API qua Service mới
    # Hàm get_kline này nằm trong services/api_client.py
    kline_data = get_kline(symbol, interval)

    if kline_data and len(kline_data) > 0:
        df = pd.DataFrame(kline_data)
        
        # Convert thời gian
        df['timestamp'] = pd.to_datetime(df['timestamp'])

        # --- XỬ LÝ TRÙNG LẶP ---
        df = df.drop_duplicates(subset=['timestamp'], keep='last').reset_index(drop=True)

        # Hiển thị số lượng nến để debug (Tùy chọn)
        # st.caption(f"Đang hiển thị: {len(df)} cây nến từ Backend")

        fig = None

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

        elif chart_type == "Line":
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], mode='lines', line=dict(color='#F0B90B', width=2)))

        elif chart_type == "Volume":
            fig = go.Figure()
            colors = ['#0ECB81' if c >= o else '#F6465D' for c, o in zip(df['close'], df['open'])]
            fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], marker_color=colors))
            
        # Cấu hình giao diện Zoom/Pan
        if fig:
            fig.update_layout(
                height=450,
                xaxis_rangeslider_visible=False,
                dragmode='pan',
                margin=dict(l=10, r=10, t=30, b=20),
                plot_bgcolor='#1e2126',
                paper_bgcolor='#1e2126',
                font=dict(color='#b2b5be'),
                xaxis=dict(gridcolor='#2b2f36', showspikes=True, spikemode='across'),
                yaxis=dict(gridcolor='#2b2f36', side='right')
            )
            st.plotly_chart(fig, use_container_width=True, config={'scrollZoom': True, 'displayModeBar': False})
            
    else:
        st.info("⏳ Đang chờ dữ liệu từ Backend...")