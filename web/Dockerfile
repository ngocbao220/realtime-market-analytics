# --- GIAI ĐOẠN 1: Build (Dùng Node.js) ---
FROM node:18-alpine AS builder

# Tạo thư mục làm việc
WORKDIR /app

# 1. Copy package.json trước để tận dụng Docker Cache
# (Nếu code thay đổi nhưng thư viện không đổi, bước npm ci sẽ được bỏ qua -> build cực nhanh)
COPY package*.json ./

# 2. Cài đặt thư viện (npm ci nhanh và sạch hơn npm install cho CI/CD)
RUN npm ci

# 3. Copy toàn bộ code nguồn
COPY . .

# 4. Build ra file tĩnh (Kết quả nằm trong thư mục /app/dist)
RUN npm run build

# --- GIAI ĐOẠN 2: Production (Dùng Nginx Alpine) ---
FROM nginx:alpine

# Copy file cấu hình Nginx (để xử lý React Router không bị lỗi 404)
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy kết quả build từ giai đoạn 1 sang thư mục phục vụ của Nginx
COPY --from=builder /app/dist /usr/share/nginx/html

# Expose cổng 80 (cổng web mặc định)
EXPOSE 80

# Chạy Nginx
CMD ["nginx", "-g", "daemon off;"]