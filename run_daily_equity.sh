#!/bin/bash
# daily_equity_report を .env 読み込んで実行するラッパー
cd /Users/lotusfamily/MindRaid

set -a
source .env
set +a

mkdir -p logs
LOG="logs/daily_equity.log"
echo "===== $(date '+%Y-%m-%d %H:%M:%S') START =====" >> "$LOG"
/usr/bin/env python3 daily_equity_report.py >> "$LOG" 2>&1
echo "===== $(date '+%Y-%m-%d %H:%M:%S') END =====" >> "$LOG"
