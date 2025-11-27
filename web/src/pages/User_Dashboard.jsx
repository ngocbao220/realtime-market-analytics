import React from 'react';
import Header from '../components/Header';

const User_Dashboard = () => {
  return (
    // Wrapper màu đen bao phủ toàn màn hình
    <div className="min-h-screen bg-[#161A1E] text-white">
      
      {/* 1. Hiển thị Header */}
      <Header />

      {/* 2. Khu vực nội dung bên dưới (Hiện tại để trống hoặc text tạm) */}
      <div className="p-10 text-center text-gray-500">
        <h2 className="text-xl">Khu vực này sẽ hiển thị Biểu đồ và Lệnh sau...</h2>
      </div>

    </div>
  );
};

export default User_Dashboard;