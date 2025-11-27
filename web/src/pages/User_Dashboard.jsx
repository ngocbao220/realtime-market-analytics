import React from 'react';

import Header from '../components/Header';
import SymbolInfo from '../components/SymbolInfo';

function User_Dashboard() {
  return (
    <div>
      <Header />       {/* Header cũ: Menu + Ticker chạy ngang */}
      <SymbolInfo />   {/* Header mới: Thông tin chi tiết BTC/USDT */}
      
      {/* Nội dung chính của trang (Chart, Orderbook...) nằm ở dưới */}
      <main className="p-4 text-white">
          Nội dung biểu đồ nến...
      </main>
    </div>
  );
}

export default User_Dashboard;