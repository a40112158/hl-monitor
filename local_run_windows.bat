@echo off
chcp 65001
REM 先在当前 CMD 窗口设置环境变量：
REM set TG_BOT_TOKEN=你的token
REM set TG_CHAT_ID=你的chat_id
python hl_monitor_final.py --rpm 200 --concurrency 5 --note "local windows"
pause
