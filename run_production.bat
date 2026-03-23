@echo off
setlocal
cd /d %~dp0
if not exist .venv (
  py -3 -m venv .venv 2>nul
  if errorlevel 1 python -m venv .venv
)
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt
set APPDATA=%USERPROFILE%\AppData\Roaming
cd app
python -m uvicorn main:app --host 0.0.0.0 --port 8000
endlocal
