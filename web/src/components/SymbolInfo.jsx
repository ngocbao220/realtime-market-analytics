import React, { useEffect, useState, useRef } from 'react';
import '../styles/SymbolInfo.css';
import { Star } from 'lucide-react';

const SymbolInfo = ({ activeSymbol = "BTCUSDT" }) => {
  const [ticker, setTicker] = useState(null);
  
  // 1. Thêm State riêng để lưu màu của giá (Tick Color)
  const [priceColor, setPriceColor] = useState('text-green');
  
  // 2. Dùng Ref để lưu giá trị của lần cập nhật TRƯỚC ĐÓ
  const prevPriceRef = useRef(0); 
  const wsRef = useRef(null);

  useEffect(() => {
    const WS_URL = "ws://localhost:8000/ws/tickers";
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => console.log("Connected to Tickers WS");

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (Array.isArray(data)) {
                const foundCoin = data.find(item => item.symbol === activeSymbol);
                
                if (foundCoin) {
                    const newPrice = parseFloat(foundCoin.price);
                    const oldPrice = prevPriceRef.current;

                    // --- LOGIC TICK-BY-TICK (Cái bạn cần) ---
                    // Nếu giá mới cao hơn giá cũ -> Xanh
                    // Nếu giá mới thấp hơn giá cũ -> Đỏ
                    // Nếu bằng nhau -> Giữ nguyên màu cũ (không đổi)
                    if (oldPrice > 0) { // Bỏ qua lần đầu tiên (oldPrice = 0)
                        if (newPrice > oldPrice) {
                            setPriceColor('text-green');
                        } else if (newPrice < oldPrice) {
                            setPriceColor('text-red');
                        }
                    }

                    // Cập nhật giá cũ thành giá hiện tại cho lần sau so sánh
                    prevPriceRef.current = newPrice;
                    
                    // Cập nhật data vào state để render
                    setTicker(foundCoin);
                }
            }
        } catch (error) {
            console.error("Error parsing WS data:", error);
        }
    };

    return () => {
        if (wsRef.current) wsRef.current.close();
        // Reset giá cũ khi đổi coin khác để tránh so sánh khập khiễng
        prevPriceRef.current = 0; 
    };
  }, [activeSymbol]);

  if (!ticker) {
    return (
        <div className="symbol-info-container text-gray-500 flex items-center justify-center h-full">
            Loading {activeSymbol}...
        </div>
    );
  }

  // --- XỬ LÝ DỮ LIỆU ---
  const currentPrice = parseFloat(ticker.price);
  const openPrice = parseFloat(ticker.open);
  const highPrice = parseFloat(ticker.high);
  const lowPrice = parseFloat(ticker.low);
  const percentChange = parseFloat(ticker.change);
  
  // 24h Change (Vẫn giữ logic 24h cho phần thống kê bên phải)
  const priceChangeAmount = currentPrice - openPrice;
  // Màu của phần trăm thay đổi 24h thì vẫn nên theo trend ngày (Positive/Negative)
  const statsColorClass = percentChange >= 0 ? 'text-green' : 'text-red';
  const sign = percentChange >= 0 ? '+' : '';

  // Formatters
  const formatPrice = (num) => new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(num);
  const formatVol = (num) => {
      if (num >= 1000000000) return (num / 1000000000).toFixed(2) + 'B';
      if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
      if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
      return num.toFixed(2);
  };

  const baseAsset = activeSymbol.replace("USDT", "");
  const iconUrl = `https://cryptologos.cc/logos/${baseAsset.toLowerCase()}-${baseAsset.toLowerCase()}-logo.png?v=029`;

  // Tính Volume USDT
  const volBase = parseFloat(ticker.volume);
  const volQuote = volBase * currentPrice;

  return (
    <div className="symbol-info-container">
      {/* 1. Identity */}
      <div className="info-group identity">
        <div className="star-icon"><Star size={16} /></div>
        <img 
            src={iconUrl} 
            alt={baseAsset} 
            className="coin-icon" 
            onError={(e) => {e.target.src="https://cryptologos.cc/logos/bitcoin-btc-logo.png"}}
        />
        <div className="name-col">
            <div className="symbol-name">{baseAsset}/USDT</div>
            <div className="symbol-desc">Giá {baseAsset} ↗</div>
        </div>
      </div>

      {/* 2. Giá Lớn - Dùng priceColor (Realtime Tick) */}
      <div className="info-group price-group">
        <span className={`current-price ${priceColor}`}>
            {formatPrice(currentPrice)}
        </span>
        <span className="fiat-price">${formatPrice(currentPrice)}</span>
      </div>

      {/* 3. Stats Grid - Dùng statsColorClass (Trend 24h) */}
      <div className="info-group stats-grid">
        <div className="stat-item">
          <span className="stat-label">Biến động 24h</span>
          <span className={`stat-value ${statsColorClass}`}>
            {sign}{formatPrice(priceChangeAmount)} &nbsp; {sign}{percentChange}%
          </span>
        </div>

        <div className="stat-item">
          <span className="stat-label">Cao nhất 24h</span>
          <span className="stat-value">{formatPrice(highPrice)}</span>
        </div>

        <div className="stat-item">
          <span className="stat-label">Thấp nhất 24h</span>
          <span className="stat-value">{formatPrice(lowPrice)}</span>
        </div>

        <div className="stat-item">
          <span className="stat-label">KL 24h({baseAsset})</span>
          <span className="stat-value">{formatVol(volBase)}</span>
        </div>

        <div className="stat-item">
          <span className="stat-label">KL 24h(USDT)</span>
          <span className="stat-value">{formatVol(volQuote)}</span>
        </div>
      </div>
    </div>
  );
};

export default SymbolInfo;