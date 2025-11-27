import React, { useEffect, useState } from 'react';
import '../styles/SymbolInfo.css';
import { Star } from 'lucide-react';
import { api } from '../api/client'; // Import API client

const SymbolInfo = () => {
  // State lưu trữ dữ liệu ticker của coin hiện tại
  const [ticker, setTicker] = useState(null);
  
  // Mặc định hiển thị BTCUSDT
  const currentSymbol = "BTCUSDT"; 

  useEffect(() => {
    const fetchData = async () => {
      try {
        const data = await api.getTickers();
        if (Array.isArray(data)) {
          // Tìm đúng coin đang cần hiển thị
          const foundCoin = data.find(item => item.symbol === currentSymbol);
          if (foundCoin) {
            setTicker(foundCoin);
          }
        }
      } catch (error) {
        console.error("Lỗi tải ticker:", error);
      }
    };

    fetchData();
  }, []);

  // Nếu chưa có dữ liệu thì hiện Loading
  if (!ticker) {
    return <div className="symbol-info-container text-gray-500">Loading data...</div>;
  }

  // --- XỬ LÝ DỮ LIỆU TỪ API ---
  
  // 1. Tách tên (VD: BTCUSDT -> BTC và USDT)
  const baseAsset = ticker.symbol.replace("USDT", ""); // BTC
  const quoteAsset = "USDT";
  const displayName = `${baseAsset}/${quoteAsset}`;

  // 2. Tính giá trị thay đổi
  const priceChangeAmount = ticker.price - ticker.open;
  const percentChange = ticker.change; 

  // 3. Tính toán Volume (ĐÂY LÀ PHẦN BẠN BỊ THIẾU)
  const volBase = ticker.volume; // Volume của đồng coin (BTC)
  const volQuote = ticker.volume * ticker.price; // Volume quy đổi ra USDT (ước lượng)

  // 4. Xác định xu hướng để chọn màu và dấu
  const isPositive = priceChangeAmount >= 0;
  const colorClass = isPositive ? 'text-green' : 'text-red';
  
  // 5. Logic dấu cộng
  const sign = isPositive ? '+' : '';

  // --- HÀM FORMAT SỐ ---
  const formatCurrency = (num) => {
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(num);
  };

  const formatVolume = (num) => {
    return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
    }).format(num);
  }

  const iconUrl = "https://cryptologos.cc/logos/bitcoin-btc-logo.png"; 

  return (
    <div className="symbol-info-container">
      {/* 1. Logo & Tên */}
      <div className="symbol-identity">
        <Star size={20} className="text-[#848E9C] cursor-pointer hover:text-[#F0B90B]" />
        <img src={iconUrl} alt={baseAsset} className="coin-icon" />
        <div>
            <div className="symbol-name">{displayName}</div>
            <div className="symbol-link">Bitcoin Price ↗</div>
        </div>
      </div>

      {/* 2. Giá hiện tại */}
      <div className="current-price-block">
        <span className={`price-large ${colorClass}`}>
            {formatCurrency(ticker.price)}
        </span>
        <span className="price-fiat">${formatCurrency(ticker.price)}</span>
      </div>

      {/* 3. Các chỉ số 24h */}
      <div className="market-stats">
        
        {/* 24h Change */}
        <div className="stat-item">
          <span className="stat-label">24h Change</span>
          <span className={`stat-value ${colorClass}`}>
            {sign}{formatCurrency(priceChangeAmount)} &nbsp; {sign}{percentChange}%
          </span>
        </div>

        {/* 24h High */}
        <div className="stat-item">
          <span className="stat-label">24h High</span>
          <span className="stat-value">{formatCurrency(ticker.high)}</span>
        </div>

        {/* 24h Low */}
        <div className="stat-item">
          <span className="stat-label">24h Low</span>
          <span className="stat-value">{formatCurrency(ticker.low)}</span>
        </div>

        {/* 24h Vol (Base Asset - BTC) */}
        <div className="stat-item">
          <span className="stat-label">24h Vol({baseAsset})</span>
          {/* Đã sửa: Biến volBase giờ đã được định nghĩa */}
          <span className="stat-value">{formatVolume(volBase)}</span>
        </div>

        {/* 24h Vol (Quote Asset - USDT) */}
        <div className="stat-item">
           <span className="stat-label">24h Vol({quoteAsset})</span>
           {/* Đã sửa: Biến volQuote giờ đã được định nghĩa */}
           <span className="stat-value">{formatVolume(volQuote)}</span>
        </div>
      </div>
    </div>
  );
};

export default SymbolInfo;