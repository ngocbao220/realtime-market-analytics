import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';

// Import các trang (Lưu ý: Sửa lại đường dẫn Manage_user cho đúng thư mục pages)
import Login from './pages/Login'; 
import User_Dashboard from './pages/User_Dashboard'; 
import Admin_Dashboard from './pages/Admin_Dashboard'; 
import Manage_user from './components/Manage_user'; // <--- Đã sửa từ components thành pages
import HistoryTrades from './components/HistoryTrades';
import { TickerProvider } from './contexts/TickerContext'; // Import mới

// --- Component bảo vệ Route (Bắt buộc đăng nhập) ---
const PrivateRoute = ({ children }) => {
  const user = JSON.parse(localStorage.getItem("user"));
  return user ? children : <Navigate to="/login" />;
};

// --- Component bảo vệ Route Admin (Chỉ admin mới vào được) ---
const AdminRoute = ({ children }) => {
  const user = JSON.parse(localStorage.getItem("user"));
  // Nếu chưa đăng nhập -> Login
  if (!user) return <Navigate to="/login" />;
  // Nếu đăng nhập nhưng không phải admin -> Về Dashboard thường
  if (user.username !== "admin") return <Navigate to="/dashboard" />;
  
  return children;
};

function App() {
  const handleLogout = () => {
    localStorage.removeItem("user");
    window.location.href = "/login"; 
  };

  return (
    <BrowserRouter>
      <Routes>
        {/* 1. MẶC ĐỊNH VÀO LOGIN (Khi truy cập trang chủ /) */}
        <Route path="/" element={<Navigate to="/login" replace />} />
        
        {/* Trang Login */}
        <Route path="/login" element={<Login />} />
      {/* Đặt Provider ở đây để bao bọc toàn bộ App hoặc chỉ bao bọc Dashboard */}
      <TickerProvider> 
        <Routes>
          <Route path="/" element={<Navigate to="/login" />} />
          <Route path="/login" element={<Login />} />

          <Route 
            path="/dashboard" 
            element={
              <PrivateRoute>
                <User_Dashboard onLogout={handleLogout} />
              </PrivateRoute>
            } 
          />

        {/* Route cho Admin (Dashboard chính) */}
        <Route 
          path="/admin" 
          element={
            <AdminRoute>
              <Admin_Dashboard />
            </AdminRoute>
          } 
        />

        {/* Route Quản lý User (Chỉ Admin) */}
        <Route 
          path="/manage-users" 
          element={
            <AdminRoute>
               <Manage_user />
            </AdminRoute>
          } 
        />
        {/* Route Lịch sử Giao dịch (Admin) */}
        <Route 
          path="/history-trades" 
          element={
            <AdminRoute>
               <HistoryTrades />
            </AdminRoute>
          } 
        />
        
        {/* Đường dẫn sai bất kỳ (404) -> Quay về Login */}
        <Route path="*" element={<Navigate to="/login" replace />} />
      </Routes>
          <Route 
            path="/admin" 
            element={
              <AdminRoute>
                <Admin_Dashboard />
              </AdminRoute>
            } 
          />
          
          <Route path="*" element={<Navigate to="/login" />} />
        </Routes>
      </TickerProvider>
    </BrowserRouter>
  );
}

export default App;