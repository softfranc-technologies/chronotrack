#!/bin/bash
echo ""
echo "=========================================="
echo "  ChronoTrack - Timesheet Management App"
echo "=========================================="
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] Python3 not found. Install from https://python.org"
    exit 1
fi

# Check MongoDB
if command -v mongod &> /dev/null; then
    echo "[INFO] MongoDB found"
else
    echo "[WARN] mongod not found. Install MongoDB first:"
    echo "  Mac:   brew install mongodb-community && brew services start mongodb-community"
    echo "  Linux: sudo apt install mongodb && sudo systemctl start mongodb"
    echo ""
    echo "  Or use Docker instead: docker compose up --build"
    echo ""
fi

echo "[1/3] Installing Python packages..."
cd "$(dirname "$0")/backend"
pip3 install -r requirements.txt -q
if [ $? -ne 0 ]; then
    echo "[ERROR] pip install failed"
    exit 1
fi

echo "[2/3] Starting backend on http://localhost:5000 ..."
MONGO_URI="mongodb://localhost:27017" uvicorn main:app --reload --port 5000 &
BACKEND_PID=$!

sleep 2

echo "[3/3] Starting frontend on http://localhost:8080 ..."
cd ../frontend
python3 -m http.server 8080 &
FRONTEND_PID=$!

sleep 1

echo ""
echo "=========================================="
echo "  ChronoTrack is running!"
echo "  Frontend: http://localhost:8080"
echo "  Backend:  http://localhost:5000"
echo "  API Docs: http://localhost:5000/docs"
echo "=========================================="
echo ""
echo "  FIRST TIME SETUP:"
echo "  1. Open http://localhost:8080"
echo "  2. The app will auto-detect the backend at localhost:5000"
echo "  3. Login as: admin@admin.company.com / password123"
echo "  4. Go to Admin > Seed Data > Seed All Demo Data"
echo ""
echo "  Press Ctrl+C to stop all servers"
echo ""

# Open browser
if command -v open &> /dev/null; then
    sleep 1 && open http://localhost:8080
elif command -v xdg-open &> /dev/null; then
    sleep 1 && xdg-open http://localhost:8080
fi

# Wait and cleanup on Ctrl+C
trap "echo 'Stopping servers...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0" INT
wait
