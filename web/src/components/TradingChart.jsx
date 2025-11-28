import React, { useEffect, useRef, useState } from 'react';
import { api } from '../api/client';

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
  const chartContainerRef = useRef(null);
  const chartInstance = useRef(null);
  
  // Refs
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

  // --- 1. KHỞI TẠO CHART ---
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const chart = createChart(chartContainerRef.current, {
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
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      
      // Localization
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
      
      // --- CẤU HÌNH TRỤC PHẢI (RIGHT) - Dành riêng cho NẾN ---
      rightPriceScale: {
        visible: true,
        borderColor: '#2B3139',
        scaleMargins: {
            top: 0.1,    
            bottom: 0.25, 
        },
      },

      // --- CẤU HÌNH TRỤC TRÁI (LEFT) - Dành riêng cho VOLUME ---
      leftPriceScale: {
        visible: true,
        borderColor: '#2B3139',
        scaleMargins: {
            top: 0.8,
            bottom: 0,
        },
      },

      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { labelBackgroundColor: '#404040' },
        horzLine: { labelBackgroundColor: '#404040' },
      },
    });

    // --- 1. VOLUME ---
    const volumeSeries = chart.addSeries(HistogramSeries, {
        color: '#26a69a',
        priceFormat: { type: 'volume' },
        priceScaleId: 'left', 
    });
    volumeSeriesRef.current = volumeSeries;

    // --- 2. MA LINES (GẮN VÀO TRỤC PHẢI) ---
    const ma7 = chart.addSeries(LineSeries, { 
        color: '#0ba4f0ff', lineWidth: 2, crosshairMarkerVisible: false, 
        priceScaleId: 'right' 
    });
    ma7SeriesRef.current = ma7;
    
    const ma25 = chart.addSeries(LineSeries, { 
        color: '#ff00f2ff', lineWidth: 2, crosshairMarkerVisible: false, 
        priceScaleId: 'right' 
    });
    ma25SeriesRef.current = ma25;

    // --- 3. NẾN ---
    const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: '#0ECB81',
        downColor: '#F6465D',
        borderVisible: false,
        wickUpColor: '#0ECB81',
        wickDownColor: '#F6465D',
        priceScaleId: 'right', 
    });
    candleSeriesRef.current = candleSeries;

    // --- 4. LINE CHART ---
    const lineSeries = chart.addSeries(AreaSeries, {
        lineColor: '#F0B90B',
        topColor: 'rgba(240, 185, 11, 0.4)',
        bottomColor: 'rgba(240, 185, 11, 0.0)',
        lineWidth: 2,
        visible: false,
        priceScaleId: 'right',
    });
    lineSeriesRef.current = lineSeries;

    chartInstance.current = chart;

    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ 
            width: chartContainerRef.current.clientWidth,
            height: chartContainerRef.current.clientHeight 
        });
      }
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, [interval]); 

  // --- SWITCH TYPE (SỬA ĐỔI: Luôn hiện MA) ---
  useEffect(() => {
    if (!candleSeriesRef.current || !lineSeriesRef.current) return;
    const isCandle = chartType === 'candle'; 

    // Chỉ ẩn hiện Nến hoặc Đường
    candleSeriesRef.current.applyOptions({ visible: isCandle });
    lineSeriesRef.current.applyOptions({ visible: !isCandle });
    
    // MA7 và MA25 luôn luôn hiện (visible: true) bất kể chartType là gì
    if(ma7SeriesRef.current) ma7SeriesRef.current.applyOptions({ visible: true });
    if(ma25SeriesRef.current) ma25SeriesRef.current.applyOptions({ visible: true });

  }, [chartType]);

  // --- WEBSOCKET DATA ---
  useEffect(() => {
    setIsLoading(true);

    const endpoint = `/ws/klines/${symbol}?interval=${interval}&limit=500`;
    const socketUrl = api.getWebSocketUrl(endpoint);
        
    const ws = new WebSocket(socketUrl);

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
                
                // Update Price & Line
                if (candleSeriesRef.current) candleSeriesRef.current.setData(candleData);
                if (lineSeriesRef.current) lineSeriesRef.current.setData(candleData.map(d => ({ time: d.time, value: d.close })));

                // Update Volume
                if (volumeSeriesRef.current) {
                    volumeSeriesRef.current.setData(candleData.map(d => ({
                        time: d.time,
                        value: d.volume,
                        color: d.color
                    })));
                }

                // SỬA ĐỔI: Luôn tính toán và vẽ MA (bỏ điều kiện if chartType === 'candle')
                if (ma7SeriesRef.current) ma7SeriesRef.current.setData(calculateSMA(candleData, 7));
                if (ma25SeriesRef.current) ma25SeriesRef.current.setData(calculateSMA(candleData, 25));

                setIsLoading(false);
            }
        } catch (err) {
            console.error("WS Error:", err);
        }
    };

    return () => { if (wsRef.current) wsRef.current.close(); };
  }, [symbol, interval, chartType]); // Thêm chartType để re-render khi đổi loại biểu đồ

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
             <button 
                className={`type-btn ${chartType === 'candle' ? 'active' : ''}`}
                onClick={() => setChartType('candle')}
             >
                Candles
             </button>
             <button 
                className={`type-btn ${chartType === 'line' ? 'active' : ''}`}
                onClick={() => setChartType('line')}
             >
                Line
             </button>
        </div>
      </div>

      <div className="chart-container" ref={chartContainerRef}>
          {isLoading && (
              <div className="chart-loading"><span className="animate-pulse">Loading Chart...</span></div>
          )}
          {/* SỬA ĐỔI: Luôn hiện Legend MA bất kể chartType là gì */}
          {!isLoading && (
             <div className="absolute top-2 left-2 text-[10px] z-10 font-mono pointer-events-none">
                <span className="text-[#F0B90B] mr-2"></span>
                <span className="text-[#E056FD]"></span>
             </div>
          )}
      </div>
    </div>
  );
};

export default TradingChart;