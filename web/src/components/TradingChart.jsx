import React, { useEffect, useRef, useState, useCallback } from 'react';
import { 
    createChart, 
    ColorType, 
    CrosshairMode, 
    CandlestickSeries, 
    AreaSeries, 
    HistogramSeries 
} from 'lightweight-charts';
import { api } from '../api/client';
import '../styles/TradingChart.css';

const TradingChart = ({ symbol = "BTCUSDT" }) => {
  const chartContainerRef = useRef(null);
  const chartInstance = useRef(null);
  
  // Refs cho các Series (Dữ liệu)
  const candleSeriesRef = useRef(null);
  const lineSeriesRef = useRef(null);
  const volumeSeriesRef = useRef(null);

  // State quản lý UI
  const [interval, setIntervalState] = useState('15m'); 
  const [chartType, setChartType] = useState('candle'); // 'candle' | 'line'
  const [isLoading, setIsLoading] = useState(true);

  // Ref lưu nến cuối cùng để update realtime từ Redis
  const lastCandleRef = useRef(null); 
  const intervalIdRef = useRef(null);

  // --- 1. KHỞI TẠO BIỂU ĐỒ (CHỈ CHẠY 1 LẦN) ---
  useEffect(() => {
    if (!chartContainerRef.current) return;

    // Tạo Chart Container
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#161a1e' },
        textColor: '#848E9C',
      },
      grid: {
        vertLines: { color: '#2B3139', style: 1, visible: true },
        horzLines: { color: '#2B3139', style: 1, visible: true },
      },
      width: chartContainerRef.current.clientWidth,
      height: chartContainerRef.current.clientHeight,
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        borderColor: '#2B3139',
      },
      rightPriceScale: {
        borderColor: '#2B3139',
        scaleMargins: {
            top: 0.1, // Chừa lề trên cho nến
            bottom: 0.2, // Chừa lề dưới cho Volume
        },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
      },
    });

    // --- SETUP CÁC SERIES ---
    
    // 1. Volume Series (Histogram - Nằm dưới cùng)
    const volumeSeries = chart.addSeries(HistogramSeries, {
        color: '#26a69a',
        priceFormat: { type: 'volume' },
        priceScaleId: '', // Overlay lên cùng scale chính nhưng chỉnh margin
        scaleMargins: {
            top: 0.8, // Đẩy volume xuống dưới đáy (chiếm 20% dưới)
            bottom: 0,
        },
    });
    volumeSeriesRef.current = volumeSeries;

    // 2. Candlestick Series (Nến)
    const candleSeries = chart.addSeries(CandlestickSeries, {
        upColor: '#0ECB81',
        downColor: '#F6465D',
        borderVisible: false,
        wickUpColor: '#0ECB81',
        wickDownColor: '#F6465D',
    });
    candleSeriesRef.current = candleSeries;

    // 3. Line Series (Đường - Ẩn mặc định)
    const lineSeries = chart.addSeries(AreaSeries, {
        lineColor: '#F0B90B',
        topColor: 'rgba(240, 185, 11, 0.4)',
        bottomColor: 'rgba(240, 185, 11, 0.0)',
        lineWidth: 2,
        visible: false, // Mặc định ẩn
    });
    lineSeriesRef.current = lineSeries;

    chartInstance.current = chart;

    // Resize Handler
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
  }, []);

  // --- 2. XỬ LÝ CHUYỂN ĐỔI LOẠI BIỂU ĐỒ ---
  useEffect(() => {
    if (!candleSeriesRef.current || !lineSeriesRef.current) return;

    if (chartType === 'candle') {
        candleSeriesRef.current.applyOptions({ visible: true });
        lineSeriesRef.current.applyOptions({ visible: false });
    } else {
        candleSeriesRef.current.applyOptions({ visible: false });
        lineSeriesRef.current.applyOptions({ visible: true });
    }
  }, [chartType]);

  // --- 3. FETCH DATA TỪ BACKEND (CLICKHOUSE) ---
  const fetchHistoryData = useCallback(async () => {
    setIsLoading(true);
    try {
        // Gọi API klines (Backend sẽ lấy từ ClickHouse)
        const klines = await api.getKline(symbol, interval, 500); 
        
        if (Array.isArray(klines) && klines.length > 0) {
            // Xử lý dữ liệu thô
            const processedData = klines.map(k => {
                if (!k || k[0] === undefined) return null;

                // Chuẩn hóa thời gian (Giây)
                const time = k[0] > 10000000000 ? k[0] / 1000 : k[0];
                const open = parseFloat(k[1]);
                const high = parseFloat(k[2]);
                const low = parseFloat(k[3]);
                const close = parseFloat(k[4]);
                const volume = parseFloat(k[5]);

                // Màu volume theo nến tăng/giảm
                const color = close >= open ? 'rgba(14, 203, 129, 0.5)' : 'rgba(246, 70, 93, 0.5)';

                return {
                    time: time,
                    open, high, low, close,
                    value: close, // Cho Line Chart
                    volume: volume, // Cho Volume Chart
                    color: color    // Màu Volume
                };
            }).filter(Boolean); // Lọc null

            // Sort & Dedup
            processedData.sort((a, b) => a.time - b.time);
            const uniqueData = [];
            const seen = new Set();
            for (const item of processedData) {
                if (!seen.has(item.time)) {
                    uniqueData.push(item);
                    seen.add(item.time);
                }
            }

            // Set Data cho các Series
            candleSeriesRef.current.setData(uniqueData);
            lineSeriesRef.current.setData(uniqueData.map(d => ({ time: d.time, value: d.close })));
            
            // Set Data Volume (Histogram)
            volumeSeriesRef.current.setData(uniqueData.map(d => ({
                time: d.time,
                value: d.volume,
                color: d.color
            })));

            // Lưu nến cuối cùng để update realtime
            if (uniqueData.length > 0) {
                lastCandleRef.current = uniqueData[uniqueData.length - 1];
            }
        }
    } catch (error) {
        console.error("Fetch history error:", error);
    }
    setIsLoading(false);
  }, [symbol, interval]);

  // Gọi fetch khi đổi Symbol/Interval
  useEffect(() => {
    fetchHistoryData();
  }, [fetchHistoryData]);

  // --- 4. REALTIME UPDATE (REDIS SIMULATION) ---
  useEffect(() => {
    intervalIdRef.current = setInterval(async () => {
        try {
            // Lấy giá mới nhất (Thường từ Redis cache ở Backend)
            const tickers = await api.getTickers();
            const currentTicker = tickers.find(t => t.symbol === symbol);
            
            if (currentTicker && lastCandleRef.current) {
                const price = parseFloat(currentTicker.price);
                const timestamp = Math.floor(Date.now() / 1000); 

                let currentCandle = { ...lastCandleRef.current };
                const intervalSeconds = getIntervalInSeconds(interval);
                
                // Tính thời gian bắt đầu của nến hiện tại
                const candleTime = Math.floor(timestamp / intervalSeconds) * intervalSeconds;

                if (currentCandle.time === candleTime) {
                    // --- CẬP NHẬT NẾN ĐANG CHẠY ---
                    currentCandle.close = price;
                    currentCandle.high = Math.max(currentCandle.high, price);
                    currentCandle.low = Math.min(currentCandle.low, price);
                    // Giả lập volume tăng nhẹ
                    currentCandle.volume += Math.random() * 0.1; 

                    // Update UI
                    candleSeriesRef.current.update(currentCandle);
                    lineSeriesRef.current.update({ time: candleTime, value: price });
                    volumeSeriesRef.current.update({ 
                        time: candleTime, 
                        value: currentCandle.volume,
                        color: currentCandle.close >= currentCandle.open ? 'rgba(14, 203, 129, 0.5)' : 'rgba(246, 70, 93, 0.5)'
                    });
                    
                    lastCandleRef.current = currentCandle;

                } else if (candleTime > currentCandle.time) {
                    // --- TẠO NẾN MỚI ---
                    const newCandle = {
                        time: candleTime,
                        open: price, high: price, low: price, close: price,
                        value: price,
                        volume: 0,
                        color: 'rgba(14, 203, 129, 0.5)'
                    };
                    
                    candleSeriesRef.current.update(newCandle);
                    lineSeriesRef.current.update({ time: candleTime, value: price });
                    volumeSeriesRef.current.update({ time: candleTime, value: 0, color: newCandle.color });
                    
                    lastCandleRef.current = newCandle;
                }
            }
        } catch (e) {
            console.error("Live update error", e);
        }
    }, 2000); // Poll mỗi 2s

    return () => clearInterval(intervalIdRef.current);
  }, [interval, symbol]);

  // Helper
  const getIntervalInSeconds = (intv) => {
      const map = { '1m': 60, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400 };
      return map[intv] || 900;
  };

  return (
    <div className="chart-wrapper">
      {/* Toolbar */}
      <div className="chart-toolbar">
        <div className="toolbar-group">
            <span className="text-xs text-gray-500 mr-2 self-center">Time</span>
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
          {isLoading && <div className="chart-loading">Loading from ClickHouse...</div>}
      </div>
    </div>
  );
};

export default TradingChart;