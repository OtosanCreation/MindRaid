#!/bin/bash
# taker_bot を .env 読み込んで実行するラッパー
cd /Users/lotusfamily/MindRaid

# .env を export
set -a
source .env
set +a

# ログディレクトリ
mkdir -p logs
LOG="logs/taker_$(date +%Y%m%d).log"

echo "===== $(date '+%Y-%m-%d %H:%M:%S') START =====" >> "$LOG"
/usr/bin/env python3 taker_bot.py >> "$LOG" 2>&1
echo "===== $(date '+%Y-%m-%d %H:%M:%S') END =====" >> "$LOG"
