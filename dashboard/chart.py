import streamlit as st

def show_chart():
    st.markdown("### Biểu đồ giá (Kline / Volume)")
    chart_type = st.selectbox("Chọn loại biểu đồ:", ["Nến (Kline)", "Line", "Volume"])
    st.empty()  # Placeholder cho chart
    st.info(f"Đây là vùng hiển thị biểu đồ: {chart_type}")
