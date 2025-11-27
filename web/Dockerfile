FROM node:18-alpine AS builder
WORKDIR /app
COPY package*.json ./
	# 2. Cài đặt thư viện (dùng npm install nếu không có package-lock.json)
	RUN npm install
COPY . .
RUN npm run dev

FROM nginx:alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]