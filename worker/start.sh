#!/bin/bash

# 1. Chạy Price Monitor (AI Detect) dưới nền (&)
echo "🚀 Starting Price Monitor..."
python price_monitor.py &

# 2. Chạy Worker chính (Matching Engine/Consumers) của bạn
echo "🚀 Starting Main Worker..."
# Dùng exec để process này nhận tín hiệu dừng từ Docker
exec python -u main.py