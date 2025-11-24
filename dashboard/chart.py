import streamlit as st
from src.kline_api import api_get_kline
import pandas as pd
import plotly.graph_objects as go
def show_chart():
    st.markdown("### Biểu đồ giá (Kline / Volume)")
    chart_type = st.selectbox("Chọn loại biểu đồ:", ["Nến (Kline)", "Line", "Volume"])

    # Chọn symbol và interval
    symbol = st.selectbox("Chọn cặp giao dịch", ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"], key="chart_symbol")
    interval = st.selectbox("Interval", ["1m", "5m", "15m", "1h"], key="chart_interval")
    kline_data = api_get_kline(symbol, interval)

    if chart_type == "Nến (Kline)":
        if kline_data:
            df = pd.DataFrame(kline_data)
            fig = go.Figure(data=[go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                increasing_line_color='#0ECB81',
                decreasing_line_color='#F6465D'
            )])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Không có dữ liệu nến.")
    elif chart_type == "Line":
        if kline_data:
            df = pd.DataFrame(kline_data)
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=df['timestamp'], y=df['close'], mode='lines', name='Close'))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Không có dữ liệu line.")
    elif chart_type == "Volume":
        if kline_data:
            df = pd.DataFrame(kline_data)
            fig = go.Figure()
            fig.add_trace(go.Bar(x=df['timestamp'], y=df['volume'], name='Volume'))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Không có dữ liệu volume.")
