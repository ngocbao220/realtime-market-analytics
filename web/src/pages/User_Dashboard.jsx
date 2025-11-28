import React, { useState } from 'react';
import Header from '../components/Header';
import SymbolInfo from '../components/SymbolInfo';
import OrderBook from '../components/Orderbook';
import Trades from '../components/Trades';
import TradingChart from '../components/TradingChart';
import TradeForm from '../components/TradeForm';
import UserHistory from '../components/UserHistory';
import '../styles/User_Dashboard.css';

function User_Dashboard() {
  const [currentSymbol, setCurrentSymbol] = useState("BTCUSDT");

  const handleSymbolChange = (newSymbol) => {
    setCurrentSymbol(newSymbol);
  };

  return (
    <div className="app-layout">
      
      {/* 1. Khu vực Header Cố định (Không cuộn cùng nội dung dưới) */}
      <div className="fixed-header-group">
        <Header />
        <SymbolInfo 
            symbol={currentSymbol} 
            onSymbolChange={handleSymbolChange} 
        />
      </div>

      {/* 2. Khu vực Nội dung (Có thể cuộn) */}
      <div className="scrollable-content">
        
        {/* Phần Trading: Chiếm trọn 1 màn hình còn lại */}
        <main className="main-content-grid">
          
          {/* Cột Trái */}
          <aside className="col-left">
              <OrderBook symbol={currentSymbol} />
          </aside>

          {/* Cột Giữa */}
          <section className="col-center">
              <div className="chart-container">
                  <TradingChart symbol={currentSymbol} />
              </div>
              <div className="trade-form-container">
                  <TradeForm />
              </div>
          </section>
          
          {/* Cột Phải */}
          <aside className="col-right">
              <Trades symbol={currentSymbol} />
          </aside>

        </main>

        {/* Phần History: Nằm bên dưới, phải cuộn mới thấy */}
        <div className="user-history-section">
          <UserHistory />
        </div>

      </div>
    </div>
  );
}

export default User_Dashboard;