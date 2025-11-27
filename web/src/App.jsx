import React from 'react';
import Header from './components/Header';
import SymbolInfo from './components/SymbolInfo';
import OrderBook from './components/Orderbook';
import Trades from './components/Trades';
import TradingChart from './components/TradingChart';
import './index.css';

function App() {
  return (
    // .app-layout: Flex column, cao 100vh (đã định nghĩa trong index.css)
    <div className="app-layout">
      
      {/* 1. Phần Đầu (Header + Info) - Chiều cao tự động */}
      <header>
        <Header />
        <SymbolInfo />
      </header>

      {/* 2. Phần Thân (3 Cột) - Flex 1 để chiếm hết chiều cao còn lại */}
      <main className="main-content-grid">
        
        {/* Cột Trái: OrderBook */}
        <aside className="layout-col-fixed border-right">
           <OrderBook symbol="BTCUSDT" />
        </aside>

        {/* Cột Giữa: Chart TradingView */}
        <section className="layout-col-fluid">
            {/* 2. Nhúng TradingChart vào đây */}
            <TradingChart symbol="BTCUSDT" />
        </section>
        
        {/* Cột Phải: Market Trades */}
        <aside className="layout-col-fixed border-left">
           <Trades symbol="BTCUSDT" />
        </aside>

      </main>
    </div>
  );
}

export default App;