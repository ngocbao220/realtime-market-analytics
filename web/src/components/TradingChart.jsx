import React, { useEffect, useRef, useState } from 'react';
import { 
    createChart, 
    ColorType, 
    CrosshairMode, 
    CandlestickSeries, 
    AreaSeries, 
    HistogramSeries,
    LineSeries 
} from 'lightweight-charts';
import '../styles/TradingChart.css';

const TradingChart = ({ symbol = "BTCUSDT" }) => {
  // Ref cho 2 container riêng biệt
  const priceChartContainerRef = useRef(null);
  const volumeChartContainerRef = useRef(null);
  
  // Refs giữ instance chart
  const priceChartRef = useRef(null);
  const volumeChartRef = useRef(null);

  // Refs Series
  const candleSeriesRef = useRef(null);
  const lineSeriesRef = useRef(null); 
  const volumeSeriesRef = useRef(null);
  const ma7SeriesRef = useRef(null);
  const ma25SeriesRef = useRef(null);
  
  const wsRef = useRef(null);

  // State
  const [interval, setIntervalState] = useState('1m'); 
  const [chartType, setChartType] = useState('candle'); 
  const [isLoading, setIsLoading] = useState(true);

  // Hàm tính MA
  const calculateSMA = (data, count) => {
    const avg = data.map((d, i) => {
        if (i < count - 1) return { time: d.time }; 
        const slice = data.slice(i - count + 1, i + 1);
        const sum = slice.reduce((a, b) => a + b.close, 0);
        return { time: d.time, value: sum / count };
    });
    return avg.filter(d => d.value !== undefined);
  };

  // --- INIT CHARTS ---
  useEffect(() => {
    if (!priceChartContainerRef.current || !volumeChartContainerRef.current) return;

    // --- CẤU HÌNH CHUNG ---
    const chartOptions = {
        layout: {
            background: { type: ColorType.Solid, color: '#161a1e' },
            textColor: '#848E9C',
            fontFamily: "'Roboto', sans-serif",
            fontSize: 11,
        },
        grid: {
            vertLines: { color: '#2B3139', style: 1, visible: false },
            horzLines: { color: '#2B3139', style: 1, visible: true },
        },
        
        // --- FIX LỖI THẲNG HÀNG TẠI ĐÂY ---
        rightPriceScale: {
            borderColor: '#2B3139',
            visible: true,
            // Ép độ rộng trục phải cố định là 70px cho cả 2 biểu đồ
            minimumWidth: 70, 
        },
        // ------------------------------------

        timeScale: {
            timeVisible: true,
            secondsVisible: false,
            borderColor: '#2B3139',
            rightOffset: 5,
            barSpacing: 12,
            tickMarkFormatter: (time, tickMarkType, locale) => {
                const date = new Date(time * 1000);
                const options = { timeZone: 'Asia/Ho_Chi_Minh', hour12: false };
                if (interval === '1d' || interval === '1w') {
                    return new Intl.DateTimeFormat('en-GB', { ...options, day: '2-digit', month: '2-digit' }).format(date);
                }
                return new Intl.DateTimeFormat('en-GB', { ...options, hour: '2-digit', minute: '2-digit' }).format(date);
            },
        },
        // ... (Giữ nguyên localization và crosshair)
        localization: {
            locale: 'en-GB',
            timeFormatter: (timestamp) => {
                const date = new Date(timestamp * 1000);
                const options = { 
                    timeZone: 'Asia/Ho_Chi_Minh',
                    hour12: false,
                    hour: '2-digit', minute: '2-digit',
                    day: '2-digit', month: '2-digit', year: 'numeric'
                };
                if (interval === '1d' || interval === '1w') {
                     return new Intl.DateTimeFormat('en-GB', { ...options, hour: undefined, minute: undefined }).format(date);
                }
                return new Intl.DateTimeFormat('en-GB', options).format(date);
            }
        },
        crosshair: {
            mode: CrosshairMode.Normal,
            vertLine: { labelBackgroundColor: '#404040' },
            horzLine: { labelBackgroundColor: '#404040' },
        },
    };

    // 1. TẠO PRICE CHART (KHUNG TRÊN)
    const priceChart = createChart(priceChartContainerRef.current, {
        ...chartOptions,
        width: priceChartContainerRef.current.clientWidth,
        height: priceChartContainerRef.current.clientHeight,
        timeScale: {
            ...chartOptions.timeScale,
            visible: false, // Ẩn trục thời gian của chart trên để đỡ bị lặp
        }
    });

    // 2. TẠO VOLUME CHART (KHUNG DƯỚI)
    const volumeChart = createChart(volumeChartContainerRef.current, {
        ...chartOptions,
        width: volumeChartContainerRef.current.clientWidth,
        height: volumeChartContainerRef.current.clientHeight,
        grid: { ...chartOptions.grid, horzLines: { visible: false } } // Ẩn lưới ngang volume cho thoáng
    });

    // --- SETUP SERIES ---

    // A. Series cho Price Chart
    const candleSeries = priceChart.addSeries(CandlestickSeries, {
        upColor: '#0ECB81', downColor: '#F6465D',
        borderVisible: false, wickUpColor: '#0ECB81', wickDownColor: '#F6465D',
    });
    const lineSeries = priceChart.addSeries(AreaSeries, {
        lineColor: '#F0B90B', topColor: 'rgba(240, 185, 11, 0.4)',
        bottomColor: 'rgba(240, 185, 11, 0.0)', lineWidth: 2, visible: false,
    });
    const ma7 = priceChart.addSeries(LineSeries, { color: '#F0B90B', lineWidth: 2, crosshairMarkerVisible: false });
    const ma25 = priceChart.addSeries(LineSeries, { color: '#E056FD', lineWidth: 2, crosshairMarkerVisible: false });

    // B. Series cho Volume Chart
    const volumeSeries = volumeChart.addSeries(HistogramSeries, {
        color: '#26a69a',
        priceFormat: { type: 'volume' },
    });

    // Lưu Refs
    priceChartRef.current = priceChart;
    volumeChartRef.current = volumeChart;
    candleSeriesRef.current = candleSeries;
    lineSeriesRef.current = lineSeries;
    ma7SeriesRef.current = ma7;
    ma25SeriesRef.current = ma25;
    volumeSeriesRef.current = volumeSeries;

    // --- ĐỒNG BỘ HÓA (SYNC) 2 BIỂU ĐỒ ---
    // Khi cuộn chart này, chart kia cuộn theo
    const syncCharts = (source, target) => {
        source.timeScale().subscribeVisibleLogicalRangeChange((range) => {
            if (range) {
                target.timeScale().setVisibleLogicalRange(range);
            }
        });
    };
    
    syncCharts(priceChart, volumeChart);
    syncCharts(volumeChart, priceChart);

    // --- RESIZE HANDLER ---
    const handleResize = () => {
        if (priceChartContainerRef.current && volumeChartContainerRef.current) {
            priceChart.applyOptions({ 
                width: priceChartContainerRef.current.clientWidth,
                height: priceChartContainerRef.current.clientHeight 
            });
            volumeChart.applyOptions({ 
                width: volumeChartContainerRef.current.clientWidth,
                height: volumeChartContainerRef.current.clientHeight 
            });
        }
    };
    window.addEventListener('resize', handleResize);

    return () => {
        window.removeEventListener('resize', handleResize);
        priceChart.remove();
        volumeChart.remove();
    };
  }, [interval]);

  // --- SWITCH TYPE ---
  useEffect(() => {
    if (!candleSeriesRef.current || !lineSeriesRef.current) return;
    const isCandle = chartType === 'candle'; 
    candleSeriesRef.current.applyOptions({ visible: isCandle });
    lineSeriesRef.current.applyOptions({ visible: !isCandle });
    if(ma7SeriesRef.current) ma7SeriesRef.current.applyOptions({ visible: isCandle });
    if(ma25SeriesRef.current) ma25SeriesRef.current.applyOptions({ visible: isCandle });
  }, [chartType]);

  // --- WEBSOCKET DATA ---
  useEffect(() => {
    setIsLoading(true);
    const WS_URL = `ws://localhost:8000/ws/klines/${symbol}?interval=${interval}&limit=500`;
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => console.log(`Connected to Klines WS: ${interval}`);

    ws.onmessage = (event) => {
        try {
            const rawData = JSON.parse(event.data);
            if (Array.isArray(rawData) && rawData.length > 0) {
                const candleData = rawData.map(d => {
                    let rawTime = d.timestamp || d[0]; 
                    const time = typeof rawTime === 'string' 
                        ? new Date(rawTime).getTime() / 1000 
                        : (rawTime > 10000000000 ? rawTime / 1000 : rawTime);

                    const open = parseFloat(d.open || d[1]);
                    const high = parseFloat(d.high || d[2]);
                    const low = parseFloat(d.low || d[3]);
                    const close = parseFloat(d.close || d[4]);
                    const volume = parseFloat(d.volume || d[5]);
                    
                    return {
                        time, open, high, low, close,
                        value: close, 
                        volume,       
                        color: close >= open ? 'rgba(14, 203, 129, 0.5)' : 'rgba(246, 70, 93, 0.5)'
                    };
                }).sort((a, b) => a.time - b.time); 
                
                // Update Price Chart
                if (candleSeriesRef.current) candleSeriesRef.current.setData(candleData);
                if (lineSeriesRef.current) lineSeriesRef.current.setData(candleData.map(d => ({ time: d.time, value: d.close })));
                if (chartType === 'candle') {
                    if (ma7SeriesRef.current) ma7SeriesRef.current.setData(calculateSMA(candleData, 7));
                    if (ma25SeriesRef.current) ma25SeriesRef.current.setData(calculateSMA(candleData, 25));
                }

                // Update Volume Chart (Riêng biệt)
                if (volumeSeriesRef.current) {
                    volumeSeriesRef.current.setData(candleData.map(d => ({
                        time: d.time,
                        value: d.volume,
                        color: d.color
                    })));
                }

                setIsLoading(false);
            }
        } catch (err) {
            console.error("WS Error:", err);
        }
    };

    return () => { if (wsRef.current) wsRef.current.close(); };
  }, [symbol, interval, chartType]); 

  return (
    <div className="chart-wrapper">
      <div className="chart-toolbar">
        <div className="toolbar-group">
            <span className="text-xs text-gray-500 mr-2 self-center font-bold">Time</span>
            {['1m', '15m', '1h', '4h', '1d'].map(t => (
                <button 
                    key={t}
                    className={`time-btn ${interval === t ? 'active' : ''}`}
                    onClick={() => setIntervalState(t)}
                >
                    {t}
                </button>
            ))}
        </div>
        <div className="toolbar-group">
             <button className={`type-btn ${chartType === 'candle' ? 'active' : ''}`} onClick={() => setChartType('candle')}>Candles</button>
             <button className={`type-btn ${chartType === 'line' ? 'active' : ''}`} onClick={() => setChartType('line')}>Line</button>
        </div>
      </div>

      {/* CONTAINER CHIA ĐÔI */}
      <div className="charts-split-container">
          {isLoading && <div className="chart-loading"><span className="animate-pulse">Loading Chart...</span></div>}
          
          {/* Biểu đồ trên: GIÁ */}
          <div className="price-chart-container" ref={priceChartContainerRef}>
             {!isLoading && chartType === 'candle' && (
                 <div className="absolute top-2 left-2 text-[10px] z-10 font-mono pointer-events-none">
                    <span className="text-[#F0B90B] mr-2">MA(7)</span>
                    <span className="text-[#E056FD]">MA(25)</span>
                 </div>
             )}
          </div>

          {/* Biểu đồ dưới: VOLUME */}
          <div className="volume-chart-container" ref={volumeChartContainerRef}></div>
      </div>
    </div>
  );
};

export default TradingChart;