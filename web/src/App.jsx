import React from 'react';
import { BrowserRouter, Routes, Route, Navigate, Outlet } from 'react-router-dom';

// --- 1. Import các trang (Đã sửa đường dẫn chuẩn vào folder pages) ---
import Login from './pages/Login'; 
import User_Dashboard from './pages/User_Dashboard'; 
import Admin_Dashboard from './pages/Admin_Dashboard'; 
import Manage_user from './components/Manage_user';     // Sửa từ components -> pages
import HistoryTrades from './components/HistoryTrades'; // Sửa từ components -> pages

// --- 2. Import Context ---
import { TickerProvider } from './contexts/TickerContext'; 

// --- 3. CỔNG BẢO VỆ CHUNG (Layout cho tất cả trang cần đăng nhập) ---
const ProtectedLayout = () => {
  const user = JSON.parse(localStorage.getItem("user"));
  
  // Nếu chưa đăng nhập -> Đá ngay về Login
  if (!user) {
    return <Navigate to="/login" replace />;
  }

  // Nếu đã đăng nhập -> Cho phép hiển thị các Route con bên trong (Outlet)
  return <Outlet />;
};

// --- 4. CỔNG BẢO VỆ RIÊNG CHO ADMIN ---
const AdminLayout = () => {
  const user = JSON.parse(localStorage.getItem("user"));
  
  // Logic kiểm tra quyền Admin
  if (user && user.username === "admin") {
      return <Outlet />; // Cho phép đi tiếp
  }
  
  // Nếu không phải admin -> Đá về Dashboard thường
  return <Navigate to="/dashboard" replace />;
};

function App() {
  const handleLogout = () => {
    localStorage.removeItem("user");
    window.location.href = "/login"; 
  };

  return (
    <BrowserRouter>
      {/* Bao bọc TickerProvider ở ngoài cùng để mọi trang đều dùng được data */}
      <TickerProvider>
        <Routes>
          
          {/* =========================================
              KHU VỰC CÔNG KHAI (Public Routes)
             ========================================= */}
          <Route path="/login" element={<Login />} />

          {/* =========================================
              KHU VỰC CẦN ĐĂNG NHẬP (Protected Routes)
              Tất cả route nằm trong này đều bắt buộc phải Login
             ========================================= */}
          <Route element={<ProtectedLayout />}>
              
              {/* Mặc định vào trang gốc (/) sẽ nhảy vào dashboard */}
              <Route path="/" element={<Navigate to="/dashboard" replace />} />

              {/* Trang Dashboard cho User thường */}
              <Route path="/dashboard" element={<User_Dashboard onLogout={handleLogout} />} />

              {/* --- KHU VỰC CỦA ADMIN --- */}
              <Route element={<AdminLayout />}>
                  <Route path="/admin" element={<Admin_Dashboard />} />
                  <Route path="/manage-users" element={<Manage_user />} />
                  <Route path="/history-trades" element={<HistoryTrades />} />
              </Route>

          </Route>

          {/* =========================================
              XỬ LÝ ĐƯỜNG DẪN RÁC (404)
              Gõ bậy bạ -> Đá về login hết
             ========================================= */}
          <Route path="*" element={<Navigate to="/login" replace />} />

        </Routes>
      </TickerProvider>
    </BrowserRouter>
  );
}

export default App;