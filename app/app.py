"""
Binance-style Professional Trading Dashboard
Real-time Cryptocurrency Trading Interface
"""

import streamlit as st
import requests
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import time

# Config
API_URL = "http://127.0.0.1:8000"

st.set_page_config(
    page_title="Binance Trading Dashboard",
    page_icon="₿",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS - Exact Binance Theme
st.markdown("""
<style>
    /* Binance exact colors */
    :root {
        --binance-bg: #0B0E11;
        --binance-surface: #181A20;
        --binance-border: #2B3139;
        --binance-text: #EAECEF;
        --binance-gray: #848E9C;
        --binance-green: #0ECB81;
        --binance-red: #F6465D;
        --binance-yellow: #F0B90B;
    }
    
    /* Main background */
    .stApp {
        background-color: #0B0E11;
    }
    
    /* Remove all padding */
    .block-container {
        padding: 1rem 1rem 0rem 1rem !important;
        max-width: 100% !important;
    }
    
    /* Headers */
    h1, h2, h3 {
        color: #EAECEF !important;
        font-family: 'BinancePlex', Arial, sans-serif !important;
        font-weight: 500 !important;
        padding: 0 !important;
        margin: 0 !important;
    }
    
    h1 { font-size: 24px !important; }
    h2 { font-size: 18px !important; }
    h3 { font-size: 16px !important; color: #EAECEF !important; }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        font-size: 22px !important;
        font-weight: 500 !important;
        color: #EAECEF !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #848E9C !important;
        font-size: 13px !important;
        text-transform: none !important;
        font-weight: 400 !important;
    }
    
    /* Remove metric borders */
    [data-testid="metric-container"] {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
    }
    
    /* Selectbox and inputs */
    .stSelectbox label, .stSlider label {
        color: #848E9C !important;
        font-size: 13px !important;
    }
    
    .stSelectbox > div > div {
        background-color: #181A20 !important;
        border: 1px solid #2B3139 !important;
        color: #EAECEF !important;
        font-size: 14px !important;
    }
    
    /* Tables - compact like Binance */
    .dataframe {
        font-size: 12px !important;
        font-family: 'Consolas', monospace !important;
    }
    
    /* Remove borders and backgrounds */
    div[data-testid="stVerticalBlock"] > div {
        background: transparent !important;
        border: none !important;
        padding: 0 !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0px;
        background-color: #181A20;
    }
    
    .stTabs [data-baseweb="tab"] {
        background-color: transparent;
        color: #848E9C;
        font-size: 12px;
        padding: 8px 16px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: transparent;
        color: #F0B90B;
        border-bottom: 2px solid #F0B90B;
    }
    
    /* Scrollbar */
    ::-webkit-scrollbar {
        width: 6px;
        height: 6px;
    }
    
    ::-webkit-scrollbar-track {
        background: #181A20;
    }
    
    ::-webkit-scrollbar-thumb {
        background: #2B3139;
        border-radius: 3px;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ============================================
# TOP BAR - Trading Pair Selection
# ============================================

# Get available symbols
try:
    symbols_response = requests.get(f"{API_URL}/api/symbols")
    symbols = symbols_response.json()["symbols"]
except:
    symbols = ["BTCUSDT", "ETHUSDT", "BNBBTC"]

# Top navigation bar
col1, col2, col3, col4, col5, col6 = st.columns([3, 2, 2, 2, 2, 1])

with col1:
    selected_symbol = st.selectbox("Trading Pair", symbols, label_visibility="collapsed", key="symbol_select")

with col2:
    interval = st.selectbox("Interval", ["1m", "3m", "5m", "15m", "30m", "1h", "4h", "1d"], index=0, label_visibility="collapsed", key="interval_select")

with col3:
    chart_type = st.selectbox("Chart Type", ["Candlestick", "Line", "Area"], label_visibility="collapsed")

with col4:
    indicator = st.selectbox("Indicators", ["MA", "EMA", "BOLL", "RSI", "MACD"], label_visibility="collapsed")

with col5:
    view_mode = st.selectbox("View", ["Standard", "Advanced Analytics"], label_visibility="collapsed")

with col6:
    auto_refresh = st.checkbox("Auto", value=False, key="auto_refresh")

st.markdown("<div style='height: 1px; background: #2B3139; margin: 10px 0;'></div>", unsafe_allow_html=True)

# ============================================
# PRICE TICKER - Binance Exact Layout
# ============================================
hours = 24
try:
    stats_response = requests.get(f"{API_URL}/api/stats/{selected_symbol}?hours={hours}")
    stats = stats_response.json()
    
    # Price ticker bar
    ticker_col1, ticker_col2, ticker_col3, ticker_col4, ticker_col5, ticker_col6 = st.columns([3, 2, 2, 2, 2, 2])
    
    with ticker_col1:
        # Main price display
        current_price = stats['avg_price']
        price_change = 1250.45  # Mock
        price_change_pct = (price_change / current_price) * 100
        
        st.markdown(f"""
        <div style='padding: 5px 0;'>
            <div style='font-size: 24px; font-weight: 500; color: #EAECEF; line-height: 1.2;'>
                ${current_price:,.2f}
            </div>
            <div style='font-size: 14px; color: {"#0ECB81" if price_change > 0 else "#F6465D"}; margin-top: 2px;'>
                {'+' if price_change > 0 else ''}{price_change:,.2f} {'+' if price_change_pct > 0 else ''}{price_change_pct:.2f}%
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with ticker_col2:
        st.markdown(f"""
        <div style='padding: 8px 0;'>
            <div style='font-size: 11px; color: #848E9C;'>24h High</div>
            <div style='font-size: 14px; color: #EAECEF; font-weight: 500; margin-top: 2px;'>${stats['max_price']:,.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with ticker_col3:
        st.markdown(f"""
        <div style='padding: 8px 0;'>
            <div style='font-size: 11px; color: #848E9C;'>24h Low</div>
            <div style='font-size: 14px; color: #EAECEF; font-weight: 500; margin-top: 2px;'>${stats['min_price']:,.2f}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with ticker_col4:
        volume_m = stats['total_volume'] / 1_000_000
        st.markdown(f"""
        <div style='padding: 8px 0;'>
            <div style='font-size: 11px; color: #848E9C;'>24h Volume (USDT)</div>
            <div style='font-size: 14px; color: #EAECEF; font-weight: 500; margin-top: 2px;'>{volume_m:,.2f}M</div>
        </div>
        """, unsafe_allow_html=True)
    
    with ticker_col5:
        st.markdown(f"""
        <div style='padding: 8px 0;'>
            <div style='font-size: 11px; color: #848E9C;'>24h Trades</div>
            <div style='font-size: 14px; color: #EAECEF; font-weight: 500; margin-top: 2px;'>{stats['total_trades']:,}</div>
        </div>
        """, unsafe_allow_html=True)
    
    with ticker_col6:
        spread = stats['max_price'] - stats['min_price']
        st.markdown(f"""
        <div style='padding: 8px 0;'>
            <div style='font-size: 11px; color: #848E9C;'>24h Range</div>
            <div style='font-size: 14px; color: #EAECEF; font-weight: 500; margin-top: 2px;'>${spread:,.2f}</div>
        </div>
        """, unsafe_allow_html=True)

except Exception as e:
    st.error(f"Error loading stats: {e}")

st.markdown("<div style='height: 1px; background: #2B3139; margin: 10px 0;'></div>", unsafe_allow_html=True)

# ============================================
# MAIN LAYOUT - Chart + Orderbook + Trades
# ============================================

chart_col, side_col = st.columns([3, 1])

with chart_col:
    # ============================================
    # CANDLESTICK CHART - TradingView Style
    # ============================================
    try:
        price_response = requests.get(
            f"{API_URL}/api/price-history/{selected_symbol}?interval={interval}&limit=200"
        )
        price_data = price_response.json()["data"]
        
        df = pd.DataFrame(price_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Tính Moving Averages (đường dự đoán xu hướng)
        df['MA7'] = df['close'].rolling(window=7, min_periods=1).mean()
        df['MA25'] = df['close'].rolling(window=25, min_periods=1).mean()
        df['MA99'] = df['close'].rolling(window=99, min_periods=1).mean()
        
        # Create figure with secondary y-axis for volume
        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
            subplot_titles=('', '')
        )
        
        # Candlestick với custom hover
        fig.add_trace(
            go.Candlestick(
                x=df['timestamp'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'],
                name='',
                increasing_line_color='#0ECB81',
                increasing_fillcolor='#0ECB81',
                decreasing_line_color='#F6465D',
                decreasing_fillcolor='#F6465D',
                whiskerwidth=1,
                line=dict(width=1),
                hoverinfo='skip'  # Tắt hover mặc định
            ),
            row=1, col=1
        )
        
        # Thêm invisible scatter để custom hover (giống Binance)
        hover_text = []
        for idx, row in df.iterrows():
            text = f"""<b>Time:</b> {row['timestamp'].strftime('%Y-%m-%d %H:%M')}<br><b>Open:</b> {row['open']:.2f}<br><b>High:</b> {row['high']:.2f}<br><b>Low:</b> {row['low']:.2f}<br><b>Close:</b> {row['close']:.2f}<br><b>Volume:</b> {row['volume']:,.2f}"""
            hover_text.append(text)
        
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['close'],
                mode='markers',
                marker=dict(size=0.1, opacity=0),
                showlegend=False,
                hovertemplate='%{text}<extra></extra>',
                text=hover_text,
                name=''
            ),
            row=1, col=1
        )
        
        # MA7 - Đường trung bình động 7 nến (ngắn hạn - vàng)
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['MA7'],
                mode='lines',
                name='MA(7)',
                line=dict(color='#F0B90B', width=1.5),
                hovertemplate='<b>MA(7):</b> %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # MA25 - Đường trung bình động 25 nến (trung hạn - tím)
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['MA25'],
                mode='lines',
                name='MA(25)',
                line=dict(color='#B37FEB', width=1.5),
                hovertemplate='<b>MA(25):</b> %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # MA99 - Đường trung bình động 99 nến (dài hạn - xanh dương)
        fig.add_trace(
            go.Scatter(
                x=df['timestamp'],
                y=df['MA99'],
                mode='lines',
                name='MA(99)',
                line=dict(color='#2E8B57', width=1.5),
                hovertemplate='<b>MA(99):</b> %{y:.2f}<extra></extra>'
            ),
            row=1, col=1
        )
        
        # Volume bars
        colors = ['#0ECB81' if close >= open else '#F6465D' 
                  for close, open in zip(df['close'], df['open'])]
        
        fig.add_trace(
            go.Bar(
                x=df['timestamp'],
                y=df['volume'],
                name='Volume',
                marker=dict(color=colors, opacity=0.5),
                showlegend=False
            ),
            row=2, col=1
        )
        
        # TradingView-style layout
        fig.update_layout(
            template='plotly_dark',
            paper_bgcolor='#0B0E11',
            plot_bgcolor='#0B0E11',
            font=dict(color='#EAECEF', family='Arial', size=13),  # Tăng font size
            
            uirevision=f'{selected_symbol}_{interval}',  # Giữ trạng thái riêng cho mỗi symbol+interval
            
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor='rgba(24, 26, 32, 0.8)',
                bordercolor='#2B3139',
                borderwidth=1,
                font=dict(size=12, color='#EAECEF')
            ),
            hovermode='x',
            dragmode='pan',  # Mặc định là pan (kéo thả) thay vì zoom
            
            height=600,
            margin=dict(l=0, r=0, t=30, b=0),
            
            xaxis=dict(
                rangeslider=dict(visible=False),
                showgrid=True,
                gridcolor='#2B3139',
                gridwidth=0.5,
                zeroline=False,
                showline=False,
                # Bật range để có thể scroll/zoom
                range=[df['timestamp'].iloc[-50], df['timestamp'].iloc[-1]] if len(df) > 50 else None
            ),
            
            yaxis=dict(
                side='right',
                showgrid=True,
                gridcolor='#2B3139',
                gridwidth=0.5,
                zeroline=False,
                showline=False,
                tickformat=',.2f',
                tickfont=dict(size=13, color='#EAECEF')  # Tăng font size
            ),
            
            xaxis2=dict(
                showgrid=False,
                zeroline=False
            ),
            
            yaxis2=dict(
                side='right',
                showgrid=False,
                zeroline=False
            ),
            
            hoverlabel=dict(
                bgcolor='#181A20',
                font_size=13,  # Tăng font size tooltip
                font_color='#EAECEF',
                bordercolor='#2B3139',
                align='left'
            )
        )
        
        # Update x-axis format
        fig.update_xaxes(
            type='date',
            tickformat='%H:%M',
            dtick=3600000 * 2,  # 2 hours
            tickfont=dict(size=12, color='#848E9C')
        )
        
        # Update y-axis cho volume
        fig.update_yaxes(
            tickfont=dict(size=12, color='#848E9C'),
            row=2, col=1
        )
        
        st.plotly_chart(fig, use_container_width=True, config={
            'displayModeBar': True,
            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
            'displaylogo': False,
            'scrollZoom': True  # Bật scroll để zoom
        })

    except Exception as e:
        st.error(f"Error loading chart: {e}")

with side_col:
    # ============================================
    # ORDER BOOK - Binance Layout
    # ============================================
    st.markdown("### Order Book")
    
    try:
        orderbook_response = requests.get(f"{API_URL}/api/orderbook/{selected_symbol}")
        orderbook = orderbook_response.json()
        
        if "bids" in orderbook and "asks" in orderbook:
            # Spread
            spread = orderbook['spread']
            spread_pct = (spread / orderbook['asks'][0]['price'] * 100) if len(orderbook['asks']) > 0 else 0
            
            st.markdown(f"""
            <div style='text-align: center; padding: 8px; background: #181A20; margin-bottom: 8px;'>
                <span style='color: #848E9C; font-size: 12px;'>Spread: </span>
                <span style='color: #F0B90B; font-size: 14px; font-weight: 500;'>
                    ${spread:.2f} ({spread_pct:.3f}%)
                </span>
            </div>
            """, unsafe_allow_html=True)
            
            # Header
            col_h1, col_h2, col_h3 = st.columns([1, 1, 1])
            with col_h1:
                st.markdown("<p style='color: #848E9C; font-size: 11px; margin: 0;'>Price</p>", unsafe_allow_html=True)
            with col_h2:
                st.markdown("<p style='color: #848E9C; font-size: 11px; margin: 0; text-align: right;'>Amount</p>", unsafe_allow_html=True)
            with col_h3:
                st.markdown("<p style='color: #848E9C; font-size: 11px; margin: 0; text-align: right;'>Total</p>", unsafe_allow_html=True)
            
            # Asks (Sell - Red)
            asks_df = pd.DataFrame(orderbook['asks'][:15])
            for _, row in asks_df.iterrows():
                c1, c2, c3 = st.columns([1, 1, 1])
                total = row['price'] * row['quantity']
                with c1:
                    st.markdown(f"<p style='color: #F6465D; margin: 1px 0; font-size: 12px;'>{row['price']:.2f}</p>", unsafe_allow_html=True)
                with c2:
                    st.markdown(f"<p style='color: #EAECEF; margin: 1px 0; font-size: 12px; text-align: right;'>{row['quantity']:.5f}</p>", unsafe_allow_html=True)
                with c3:
                    st.markdown(f"<p style='color: #848E9C; margin: 1px 0; font-size: 12px; text-align: right;'>{total:.2f}</p>", unsafe_allow_html=True)
            
            st.markdown("<div style='height: 2px; background: #2B3139; margin: 8px 0;'></div>", unsafe_allow_html=True)
            
            # Bids (Buy - Green)
            bids_df = pd.DataFrame(orderbook['bids'][:15])
            for _, row in bids_df.iterrows():
                c1, c2, c3 = st.columns([1, 1, 1])
                total = row['price'] * row['quantity']
                with c1:
                    st.markdown(f"<p style='color: #0ECB81; margin: 1px 0; font-size: 12px;'>{row['price']:.2f}</p>", unsafe_allow_html=True)
                with c2:
                    st.markdown(f"<p style='color: #EAECEF; margin: 1px 0; font-size: 12px; text-align: right;'>{row['quantity']:.5f}</p>", unsafe_allow_html=True)
                with c3:
                    st.markdown(f"<p style='color: #848E9C; margin: 1px 0; font-size: 12px; text-align: right;'>{total:.2f}</p>", unsafe_allow_html=True)
        else:
            st.info("No orderbook data")
    
    except Exception as e:
        st.error(f"{e}")
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # ============================================
    # RECENT TRADES
    # ============================================
    st.markdown("### Market Trades")
    
    try:
        trades_response = requests.get(f"{API_URL}/api/realtime/{selected_symbol}?limit=30")
        trades = trades_response.json()["trades"]
        
        trades_df = pd.DataFrame(trades)
        
        # Header
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            st.markdown("<p style='color: #848E9C; font-size: 11px; margin: 0;'>Price</p>", unsafe_allow_html=True)
        with c2:
            st.markdown("<p style='color: #848E9C; font-size: 11px; margin: 0; text-align: right;'>Amount</p>", unsafe_allow_html=True)
        with c3:
            st.markdown("<p style='color: #848E9C; font-size: 11px; margin: 0; text-align: right;'>Time</p>", unsafe_allow_html=True)
        
        # Trades
        for _, row in trades_df.head(25).iterrows():
            col1, col2, col3 = st.columns([2, 1, 1])
            
            trade_time = pd.to_datetime(row['time']).strftime('%H:%M:%S')
            color = '#0ECB81' if row['side'] == 'BUY' else '#F6465D'
            
            with col1:
                st.markdown(f"<p style='color: {color}; margin: 1px 0; font-size: 12px; font-weight: 500;'>{row['price']:.2f}</p>", unsafe_allow_html=True)
            with col2:
                st.markdown(f"<p style='color: #EAECEF; margin: 1px 0; font-size: 12px; text-align: right;'>{row['quantity']:.5f}</p>", unsafe_allow_html=True)
            with col3:
                st.markdown(f"<p style='color: #848E9C; margin: 1px 0; font-size: 11px; text-align: right;'>{trade_time}</p>", unsafe_allow_html=True)
    
    except Exception as e:
        st.error(f"{e}")

# ============================================
# ADVANCED ANALYTICS (nếu chọn)
# ============================================
if view_mode == "Advanced Analytics":
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div style='height: 2px; background: #2B3139; margin: 20px 0;'></div>", unsafe_allow_html=True)
    st.markdown("## 📊 Advanced Analytics")
    
    # Tabs cho các loại phân tích
    tab1, tab2, tab3, tab4 = st.tabs(["Volatility & Risk", "Market Depth", "Price Distribution", "Technical Indicators"])
    
    with tab1:
        # ============================================
        # 1. VOLATILITY CHART (Biểu đồ biến động)
        # ============================================
        col_vol1, col_vol2 = st.columns([2, 1])
        
        with col_vol1:
            st.markdown("### Price Volatility (ATR)")
            try:
                price_response = requests.get(f"{API_URL}/api/price-history/{selected_symbol}?interval={interval}&limit=100")
                price_data = price_response.json()["data"]
                df_vol = pd.DataFrame(price_data)
                df_vol['timestamp'] = pd.to_datetime(df_vol['timestamp'])
                
                # Tính ATR (Average True Range)
                df_vol['tr1'] = df_vol['high'] - df_vol['low']
                df_vol['tr2'] = abs(df_vol['high'] - df_vol['close'].shift(1))
                df_vol['tr3'] = abs(df_vol['low'] - df_vol['close'].shift(1))
                df_vol['tr'] = df_vol[['tr1', 'tr2', 'tr3']].max(axis=1)
                df_vol['atr'] = df_vol['tr'].rolling(window=14).mean()
                
                # Volatility chart
                fig_vol = go.Figure()
                fig_vol.add_trace(go.Scatter(
                    x=df_vol['timestamp'],
                    y=df_vol['atr'],
                    mode='lines',
                    name='ATR',
                    line=dict(color='#F0B90B', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(240, 185, 11, 0.1)'
                ))
                
                fig_vol.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='#0B0E11',
                    plot_bgcolor='#0B0E11',
                    height=300,
                    margin=dict(l=0, r=0, t=20, b=0),
                    xaxis=dict(showgrid=True, gridcolor='#2B3139'),
                    yaxis=dict(showgrid=True, gridcolor='#2B3139', side='right'),
                    font=dict(color='#EAECEF', size=11)
                )
                
                st.plotly_chart(fig_vol, use_container_width=True, config={'displayModeBar': False})
            except Exception as e:
                st.error(f"Error: {e}")
        
        with col_vol2:
            st.markdown("### Risk Metrics")
            try:
                # Calculate risk metrics
                returns = df_vol['close'].pct_change().dropna()
                volatility = returns.std() * np.sqrt(len(returns))
                sharpe = (returns.mean() / returns.std() * np.sqrt(252)) if returns.std() > 0 else 0
                max_drawdown = ((df_vol['close'].cummax() - df_vol['close']) / df_vol['close'].cummax()).max() * 100
                
                st.markdown(f"""
                <div style='background: #181A20; padding: 15px; border-radius: 4px; margin-bottom: 10px;'>
                    <div style='color: #848E9C; font-size: 11px;'>Volatility (σ)</div>
                    <div style='color: #F0B90B; font-size: 20px; font-weight: 600;'>{volatility:.2f}%</div>
                </div>
                <div style='background: #181A20; padding: 15px; border-radius: 4px; margin-bottom: 10px;'>
                    <div style='color: #848E9C; font-size: 11px;'>Sharpe Ratio</div>
                    <div style='color: {"#0ECB81" if sharpe > 1 else "#F6465D"}; font-size: 20px; font-weight: 600;'>{sharpe:.2f}</div>
                </div>
                <div style='background: #181A20; padding: 15px; border-radius: 4px;'>
                    <div style='color: #848E9C; font-size: 11px;'>Max Drawdown</div>
                    <div style='color: #F6465D; font-size: 20px; font-weight: 600;'>{max_drawdown:.2f}%</div>
                </div>
                """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error: {e}")
    
    with tab2:
        # ============================================
        # 2. ORDER BOOK DEPTH CHART
        # ============================================
        st.markdown("### Market Depth (Order Book)")
        try:
            orderbook_response = requests.get(f"{API_URL}/api/orderbook/{selected_symbol}")
            orderbook = orderbook_response.json()
            
            if "bids" in orderbook and "asks" in orderbook:
                bids_df = pd.DataFrame(orderbook['bids'][:30])
                asks_df = pd.DataFrame(orderbook['asks'][:30])
                
                # Tính cumulative volume
                bids_df['cum_qty'] = bids_df['quantity'].cumsum()
                asks_df['cum_qty'] = asks_df['quantity'].cumsum()
                
                # Depth chart
                fig_depth = go.Figure()
                
                fig_depth.add_trace(go.Scatter(
                    x=bids_df['price'],
                    y=bids_df['cum_qty'],
                    mode='lines',
                    name='Bids',
                    line=dict(color='#0ECB81', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(14, 203, 129, 0.2)'
                ))
                
                fig_depth.add_trace(go.Scatter(
                    x=asks_df['price'],
                    y=asks_df['cum_qty'],
                    mode='lines',
                    name='Asks',
                    line=dict(color='#F6465D', width=2),
                    fill='tozeroy',
                    fillcolor='rgba(246, 70, 93, 0.2)'
                ))
                
                fig_depth.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='#0B0E11',
                    plot_bgcolor='#0B0E11',
                    height=400,
                    margin=dict(l=0, r=0, t=20, b=0),
                    xaxis=dict(title='Price', showgrid=True, gridcolor='#2B3139'),
                    yaxis=dict(title='Cumulative Volume', showgrid=True, gridcolor='#2B3139'),
                    font=dict(color='#EAECEF', size=12),
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig_depth, use_container_width=True, config={'displayModeBar': False})
            else:
                st.info("No orderbook data")
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab3:
        # ============================================
        # 3. PRICE DISTRIBUTION HISTOGRAM
        # ============================================
        st.markdown("### Price Distribution (Support/Resistance)")
        try:
            price_response = requests.get(f"{API_URL}/api/price-history/{selected_symbol}?interval={interval}&limit=200")
            price_data = price_response.json()["data"]
            df_dist = pd.DataFrame(price_data)
            
            # Histogram của giá
            fig_hist = go.Figure()
            
            fig_hist.add_trace(go.Histogram(
                y=df_dist['close'],
                nbinsy=30,
                orientation='h',
                marker=dict(
                    color='#F0B90B',
                    opacity=0.7,
                    line=dict(color='#2B3139', width=1)
                ),
                name='Price Frequency'
            ))
            
            fig_hist.update_layout(
                template='plotly_dark',
                paper_bgcolor='#0B0E11',
                plot_bgcolor='#0B0E11',
                height=400,
                margin=dict(l=0, r=0, t=20, b=0),
                xaxis=dict(title='Frequency', showgrid=True, gridcolor='#2B3139'),
                yaxis=dict(title='Price', showgrid=True, gridcolor='#2B3139'),
                font=dict(color='#EAECEF', size=12)
            )
            
            st.plotly_chart(fig_hist, use_container_width=True, config={'displayModeBar': False})
            
            st.markdown("""
            <div style='color: #848E9C; font-size: 12px; padding: 10px;'>
                💡 <b>Tip:</b> Vùng có nhiều giao dịch (cột cao) thường là vùng hỗ trợ/kháng cự mạnh
            </div>
            """, unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"Error: {e}")
    
    with tab4:
        # ============================================
        # 4. TECHNICAL INDICATORS (RSI, MACD)
        # ============================================
        col_rsi, col_macd = st.columns(2)
        
        with col_rsi:
            st.markdown("### RSI (Relative Strength Index)")
            try:
                price_response = requests.get(f"{API_URL}/api/price-history/{selected_symbol}?interval={interval}&limit=100")
                price_data = price_response.json()["data"]
                df_rsi = pd.DataFrame(price_data)
                df_rsi['timestamp'] = pd.to_datetime(df_rsi['timestamp'])
                
                # Calculate RSI
                delta = df_rsi['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                df_rsi['rsi'] = 100 - (100 / (1 + rs))
                
                # RSI Chart
                fig_rsi = go.Figure()
                
                fig_rsi.add_trace(go.Scatter(
                    x=df_rsi['timestamp'],
                    y=df_rsi['rsi'],
                    mode='lines',
                    name='RSI',
                    line=dict(color='#B37FEB', width=2)
                ))
                
                # Overbought/Oversold zones
                fig_rsi.add_hline(y=70, line_dash="dash", line_color="#F6465D", annotation_text="Overbought")
                fig_rsi.add_hline(y=30, line_dash="dash", line_color="#0ECB81", annotation_text="Oversold")
                
                fig_rsi.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='#0B0E11',
                    plot_bgcolor='#0B0E11',
                    height=300,
                    margin=dict(l=0, r=0, t=20, b=0),
                    xaxis=dict(showgrid=True, gridcolor='#2B3139'),
                    yaxis=dict(showgrid=True, gridcolor='#2B3139', range=[0, 100]),
                    font=dict(color='#EAECEF', size=11)
                )
                
                st.plotly_chart(fig_rsi, use_container_width=True, config={'displayModeBar': False})
                
            except Exception as e:
                st.error(f"Error: {e}")
        
        with col_macd:
            st.markdown("### MACD (Moving Average Convergence Divergence)")
            try:
                # Calculate MACD
                ema12 = df_rsi['close'].ewm(span=12, adjust=False).mean()
                ema26 = df_rsi['close'].ewm(span=26, adjust=False).mean()
                df_rsi['macd'] = ema12 - ema26
                df_rsi['signal'] = df_rsi['macd'].ewm(span=9, adjust=False).mean()
                df_rsi['histogram'] = df_rsi['macd'] - df_rsi['signal']
                
                # MACD Chart
                fig_macd = go.Figure()
                
                fig_macd.add_trace(go.Bar(
                    x=df_rsi['timestamp'],
                    y=df_rsi['histogram'],
                    name='Histogram',
                    marker_color=['#0ECB81' if x > 0 else '#F6465D' for x in df_rsi['histogram']]
                ))
                
                fig_macd.add_trace(go.Scatter(
                    x=df_rsi['timestamp'],
                    y=df_rsi['macd'],
                    mode='lines',
                    name='MACD',
                    line=dict(color='#F0B90B', width=2)
                ))
                
                fig_macd.add_trace(go.Scatter(
                    x=df_rsi['timestamp'],
                    y=df_rsi['signal'],
                    mode='lines',
                    name='Signal',
                    line=dict(color='#B37FEB', width=2)
                ))
                
                fig_macd.update_layout(
                    template='plotly_dark',
                    paper_bgcolor='#0B0E11',
                    plot_bgcolor='#0B0E11',
                    height=300,
                    margin=dict(l=0, r=0, t=20, b=0),
                    xaxis=dict(showgrid=True, gridcolor='#2B3139'),
                    yaxis=dict(showgrid=True, gridcolor='#2B3139'),
                    font=dict(color='#EAECEF', size=11),
                    legend=dict(orientation="h", yanchor="top", y=1.1)
                )
                
                st.plotly_chart(fig_macd, use_container_width=True, config={'displayModeBar': False})
                
            except Exception as e:
                st.error(f"Error: {e}")

# ============================================
# FOOTER & AUTO REFRESH
# ============================================
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(f"""
<div style='text-align: center; padding: 10px 0; border-top: 1px solid #2B3139;'>
    <span style='color: #848E9C; font-size: 10px;'>
        Last Update: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC | 
        Data Source: Binance WebSocket | 
        Powered by ClickHouse & Spark
    </span>
</div>
""", unsafe_allow_html=True)

# Auto refresh
if auto_refresh:
    time.sleep(10)
    st.rerun()
