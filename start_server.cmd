@echo off
set PYTHONPATH=D:\a-check\src
set AUTH_ENABLED=false
set PORT=8765
python -m uvicorn agent_runner.main:app --host 127.0.0.1 --port 8765 --log-level warning
pause
