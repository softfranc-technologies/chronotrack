@echo off
echo.
echo ==========================================
echo   ChronoTrack - Timesheet Management App
echo ==========================================
echo.

where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python not found. Install from https://python.org
    pause
    exit /b 1
)

echo [1/3] Installing Python packages...
cd /d "%~dp0backend"
pip install -r requirements.txt -q
if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] pip install failed
    pause
    exit /b 1
)

echo [2/3] Starting backend on http://localhost:5000...
set MONGO_URI=mongodb://localhost:27017
start "ChronoTrack Backend" cmd /k "uvicorn main:app --reload --port 5000"

timeout /t 3 /nobreak >nul

echo [3/3] Starting frontend on http://localhost:8080...
cd /d "%~dp0frontend"
start "ChronoTrack Frontend" cmd /k "python -m http.server 8080"

timeout /t 2 /nobreak >nul

echo.
echo ==========================================
echo   ChronoTrack is running!
echo   Frontend: http://localhost:8080
echo   Backend:  http://localhost:5000
echo   API Docs: http://localhost:5000/docs
echo ==========================================
echo.
echo   FIRST TIME SETUP:
echo   1. Open http://localhost:8080
echo   2. Login: admin@admin.company.com / password123
echo   3. Go to Admin - Seed Data - Seed All Demo Data
echo.

start http://localhost:8080
pause
