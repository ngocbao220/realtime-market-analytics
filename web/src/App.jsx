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
      <TickerProvider>
        <Routes>
          {/* MẶC ĐỊNH VÀO LOGIN */}
          <Route path="/" element={<Navigate to="/login" replace />} />

          {/* Trang Login */}
          <Route path="/login" element={<Login />} />

          {/* Dashboard User */}
          <Route 
            path="/dashboard"
            element={
              <PrivateRoute>
                <User_Dashboard onLogout={handleLogout} />
              </PrivateRoute>
            }
          />

          {/* Admin Dashboard */}
          <Route
            path="/admin"
            element={
              <AdminRoute>
                <Admin_Dashboard />
              </AdminRoute>
            }
          />

          {/* Manage Users */}
          <Route
            path="/manage-users"
            element={
              <AdminRoute>
                <Manage_user />
              </AdminRoute>
            }
          />

          {/* History Trades */}
          <Route
            path="/history-trades"
            element={
              <AdminRoute>
                <HistoryTrades />
              </AdminRoute>
            }
          />

          {/* 404 */}
          <Route path="*" element={<Navigate to="/login" replace />} />
        </Routes>
      </TickerProvider>
    </BrowserRouter>
  );
}

export default App;