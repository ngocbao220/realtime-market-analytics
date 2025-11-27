import React, { useEffect, useState, useRef } from 'react';
import '../styles/SymbolInfo.css';
import { Star, ChevronDown } from 'lucide-react'; 
import { useSymbolTicker } from '../contexts/TickerContext'; // Import Hook từ Context

// Danh sách các cặp coin hỗ trợ trong dropdown
const SUPPORTED_PAIRS = [
    "BTCUSDT", "BNBUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT"
];

const SymbolInfo = ({ symbol = "BTCUSDT", onSymbolChange }) => {
  // 1. Lấy dữ liệu từ Context (Thay vì tự tạo WebSocket)
  const ticker = useSymbolTicker(symbol);

  const [priceColor, setPriceColor] = useState('text-green');
  const prevPriceRef = useRef(0); 

  // 2. Logic so sánh giá để đổi màu (xanh/đỏ) khi giá thay đổi
  useEffect(() => {
    if (ticker) {
        const newPrice = parseFloat(ticker.price);
        const oldPrice = prevPriceRef.current;

        if (oldPrice > 0 && newPrice !== oldPrice) { 
            setPriceColor(newPrice > oldPrice ? 'text-green' : 'text-red');
        }

        prevPriceRef.current = newPrice;
    }
  }, [ticker]); 

  // Logic hiển thị Loading hoặc data
  const isLoading = !ticker;
  
  // Parse dữ liệu từ ticker context
  const currentPrice = isLoading ? 0 : parseFloat(ticker.price);
  const openPrice    = isLoading ? 0 : parseFloat(ticker.open);
  const closePrice    = isLoading ? 0 : parseFloat(ticker.close);
  const highPrice    = isLoading ? 0 : parseFloat(ticker.high);
  const lowPrice     = isLoading ? 0 : parseFloat(ticker.low);
  const percentChange = isLoading ? 0 : parseFloat(ticker.change);
  const volBase      = isLoading ? 0 : parseFloat(ticker.volume);
  const volQuote     = volBase * currentPrice;

  const priceChangeAmount = currentPrice - openPrice;
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
                <span className="stat-label">Giá mở</span>
                <span className="stat-value">{formatPrice(openPrice)}</span>
                </div>

                <div className="stat-item">
                <span className="stat-label">Giá đóng</span>
                <span className="stat-value">{formatPrice(closePrice)}</span>
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