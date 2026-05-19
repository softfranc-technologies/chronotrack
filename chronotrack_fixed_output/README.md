# ChronoTrack - Timesheet Management System

A full-stack timesheet management web app with role-based access (Admin / Manager / Employee), time tracking, approvals, and reporting.

---

## 🏗 Architecture

```
Frontend (nginx :8080) → proxies /api/* → Backend (FastAPI :5000) → MongoDB (:27017)
```

| Component | Tech |
|-----------|------|
| Frontend  | Vanilla JS + HTML/CSS (single file) |
| Backend   | Python FastAPI + Uvicorn |
| Database  | MongoDB 7 |
| Proxy     | Nginx (Docker) or direct (local) |

---

## 🚀 Option A: Docker Compose (Recommended)

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Steps

```bash
# 1. Clone / unzip the project
cd chronotrack

# 2. Build and start all services
docker compose up --build

# 3. Open the app
open http://localhost:8080
```

On first run:
1. Login with `admin@admin.company.com` / `password123`
2. Go to **Admin → Seed Data → Seed All Demo Data**
3. You now have 17 employees, 8 projects, and 12 weeks of data

### Stop
```bash
docker compose down        # stop
docker compose down -v     # stop + delete database
```

---

## 🖥 Option B: Local (No Docker)

### Prerequisites
- Python 3.10+
- MongoDB running locally (`mongod`)
- Node.js not required

### Mac / Linux

```bash
# Install MongoDB (Mac)
brew install mongodb-community && brew services start mongodb-community

# Or Linux
sudo apt install -y mongodb && sudo systemctl start mongodb

# Start the app
chmod +x START_MAC_LINUX.sh
./START_MAC_LINUX.sh
```

### Windows

```bat
START_WINDOWS.bat
```

The script:
1. Installs Python dependencies
2. Starts backend on port 5000
3. Starts frontend server on port 8080
4. Opens http://localhost:8080 in your browser

---

## 📦 Option C: Manual Setup

### Backend

```bash
cd backend
pip install -r requirements.txt

# Set env (edit .env or export)
export MONGO_URI=mongodb://localhost:27017
export DB_NAME=chronotrack
export JWT_SECRET=your-secret-here

uvicorn main:app --reload --port 5000
```

### Frontend

```bash
cd frontend
python3 -m http.server 8080
# OR: npx serve . -p 8080
```

Open http://localhost:8080 — the app will auto-detect the backend.

---

## 🔑 Demo Accounts

All accounts use password: **`password123`**

| Role     | Email                          |
|----------|-------------------------------|
| Admin    | admin@admin.company.com        |
| Manager  | mgr.jane@mgr.company.com       |
| Manager  | mgr.tom@mgr.company.com        |
| Employee | aryan@company.com              |
| Employee | priya@company.com              |

> **Note:** Users registered with `@admin.company.com` get Admin role, `@mgr.company.com` get Manager role, all others get Employee role.

---

## 🌐 Cloud Deployment

### Render (Free tier)

#### Backend (Web Service)
1. Push project to GitHub
2. New → Web Service → connect repo
3. Root directory: `backend`
4. Runtime: Python 3
5. Build command: `pip install -r requirements.txt`
6. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
7. Environment variables:
   - `MONGO_URI` → your MongoDB Atlas URI
   - `DB_NAME` → `chronotrack`
   - `JWT_SECRET` → any long random string
8. Deploy → copy the service URL (e.g. `https://chronotrack-api.onrender.com`)

#### Frontend (Static Site)
1. New → Static Site → connect same repo
2. Root directory: `frontend`
3. Build command: *(leave blank)*
4. Publish directory: `.`
5. Open `index.html` → in Settings page set Backend URL to your Render backend URL

#### MongoDB
- Use [MongoDB Atlas](https://www.mongodb.com/cloud/atlas) free tier
- Whitelist `0.0.0.0/0` or Render's IP range
- Copy connection string to backend env var

---

### Railway

```bash
# Install Railway CLI
npm i -g @railway/cli
railway login
railway init

# Deploy backend
cd backend
railway up

# Set env vars in Railway dashboard:
# MONGO_URI, DB_NAME, JWT_SECRET
```

For frontend: deploy as a Static service or use Netlify/Vercel.

---

### VPS / Ubuntu Server

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER

# Clone project
git clone <your-repo> chronotrack
cd chronotrack

# Start
docker compose up -d --build

# App is on port 8080
# Point your domain's A record to server IP
# Then add Nginx reverse proxy on port 80/443 for your domain
```

---

## 🔧 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URI` | `mongodb://mongodb:27017` | MongoDB connection string |
| `DB_NAME` | `chronotrack` | Database name |
| `JWT_SECRET` | *(change this!)* | JWT signing secret |

---

## 📡 API Reference

Interactive docs available at: `http://localhost:5000/docs`

Key endpoints:
- `POST /api/auth/login` — login
- `POST /api/auth/register` — register
- `GET /api/entries` — list time entries
- `POST /api/entries` — create entry
- `POST /api/entries/submit-week` — submit week for approval
- `POST /api/dev/seed` — seed demo data (dev only)

---

## 🐛 Troubleshooting

**Login fails / "Cannot reach backend"**
- Docker: ensure `docker compose up --build` completed successfully
- Local: ensure backend is running on port 5000 and MongoDB is started
- Go to Settings → set Backend URL to `http://localhost:5000` if using local dev

**MongoDB not connecting (Docker)**
- The backend will retry 10 times waiting for MongoDB to be healthy
- Check: `docker compose logs mongodb`

**Port already in use**
```bash
# Free port 8080 or 5000
lsof -ti:8080 | xargs kill
lsof -ti:5000 | xargs kill
```
