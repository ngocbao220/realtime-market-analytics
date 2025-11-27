import React, { useEffect, useState, useRef } from 'react';
import '../styles/SymbolInfo.css';
import { Star, ChevronDown } from 'lucide-react'; // Thêm ChevronDown cho đẹp
import { api } from '../api/client';

// Danh sách các cặp coin bạn muốn hỗ trợ trong dropdown
const SUPPORTED_PAIRS = [
    "BTCUSDT", "BNBUSDT", "BNBUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT"
];

const SymbolInfo = ({ symbol = "BTCUSDT", onSymbolChange }) => {
  const [ticker, setTicker] = useState(null);
  const [priceColor, setPriceColor] = useState('text-green');
  
  const prevPriceRef = useRef(0); 
  const wsRef = useRef(null);

  useEffect(() => {
    // Reset ticker khi đổi symbol để tạo hiệu ứng loading nhẹ (tránh hiện dữ liệu cũ)
    setTicker(null); 
    prevPriceRef.current = 0;

    const socketUrl = api.getWebSocketUrl('/ws/tickers');
    const ws = new WebSocket(socketUrl);
    wsRef.current = ws;

    ws.onopen = () => console.log(`✅ Connected to Tickers WS for ${symbol}`);

    ws.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            if (Array.isArray(data)) {
                // Lọc lấy đúng coin đang chọn
                const foundCoin = data.find(item => item.symbol === symbol);
                
                if (foundCoin) {
                    const newPrice = parseFloat(foundCoin.price);
                    const oldPrice = prevPriceRef.current;

                    if (oldPrice > 0) { 
                        if (newPrice > oldPrice) setPriceColor('text-green');
                        else if (newPrice < oldPrice) setPriceColor('text-red');
                    }

                    prevPriceRef.current = newPrice;
                    setTicker(foundCoin);
                }
            }
        } catch (error) {
            console.error("Error parsing WS data:", error);
        }
    };

    return () => {
        if (wsRef.current) wsRef.current.close();
    };
  }, [symbol]); // Chạy lại useEffect khi prop 'symbol' thay đổi

  // Logic hiển thị Loading hoặc data
  const isLoading = !ticker;
  
  // Dữ liệu tạm hoặc dữ liệu thật
  const currentPrice = isLoading ? 0 : parseFloat(ticker.price);
  const openPrice = isLoading ? 0 : parseFloat(ticker.open);
  const highPrice = isLoading ? 0 : parseFloat(ticker.high);
  const lowPrice = isLoading ? 0 : parseFloat(ticker.low);
  const percentChange = isLoading ? 0 : parseFloat(ticker.change);
  const volBase = isLoading ? 0 : parseFloat(ticker.volume);
  const volQuote = volBase * currentPrice;

  const priceChangeAmount = currentPrice - openPrice;
  const statsColorClass = percentChange >= 0 ? 'text-green' : 'text-red';
  const sign = percentChange >= 0 ? '+' : '';

  const formatPrice = (num) => new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(num);
  const formatVol = (num) => {
      if (num >= 1000000000) return (num / 1000000000).toFixed(2) + 'B';
      if (num >= 1000000) return (num / 1000000).toFixed(2) + 'M';
      if (num >= 1000) return (num / 1000).toFixed(2) + 'K';
      return num.toFixed(2);
  };

  const baseAsset = symbol.replace("USDT", "");
  const iconUrl = `https://cryptologos.cc/logos/${baseAsset.toLowerCase()}-${baseAsset.toLowerCase()}-logo.png?v=029`;

  return (
    <div className="symbol-info-container">
      <div className="info-group identity">
        <div className="star-icon"><Star size={16} /></div>
        
        <img 
            src={iconUrl} 
            alt={baseAsset} 
            className="coin-icon" 
            onError={(e) => {e.target.src="https://cryptologos.cc/logos/bitcoin-btc-logo.png"}}
        />
        
        <div className="name-col">
            {/* --- DROPDOWN SELECT --- */}
            <div className="symbol-select-wrapper">
                <select 
                    className="symbol-dropdown"
                    value={symbol}
                    onChange={(e) => onSymbolChange && onSymbolChange(e.target.value)}
                >
                    {SUPPORTED_PAIRS.map(pair => (
                        <option key={pair} value={pair}>
                            {pair.replace("USDT", "/USDT")}
                        </option>
                    ))}
                </select>
                <ChevronDown size={14} className="dropdown-arrow" />
            </div>
            
            <div className="symbol-desc">Giá {baseAsset} ↗</div>
        </div>
      </div>

      {isLoading ? (
          <div className="loading-state">Loading data...</div>
      ) : (
          <>
            <div className="info-group price-group">
                <span className={`current-price ${priceColor}`}>
                    {formatPrice(currentPrice)}
                </span>
                <span className="fiat-price">${formatPrice(currentPrice)}</span>
            </div>

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
          </>
      )}
    </div>
  );
};

export default SymbolInfo;