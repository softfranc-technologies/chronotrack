"""
ChronoTrack - FastAPI Backend
Run: uvicorn main:app --reload --port 5000
Docs: http://localhost:5000/docs
"""

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime, timedelta
from bson import ObjectId
import jwt
import bcrypt
import os
import csv
import io
import random
import email_service
import scheduler
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="ChronoTrack API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
DB_NAME   = os.getenv("DB_NAME",   "chronotrack")
JWT_SECRET= os.getenv("JWT_SECRET", "chronotrack-secret-key-change-in-production")
JWT_ALGO  = "HS256"

mongo_client = AsyncIOMotorClient(MONGO_URI)
db = mongo_client[DB_NAME]

col_users    = db["users"]
col_entries  = db["time_entries"]
col_projects = db["projects"]
col_tasks    = db["tasks"]
col_clients  = db["clients"]
col_notifs   = db["notifications"]
col_holidays = db["holidays"]

bearer_scheme = HTTPBearer(auto_error=False)


def _oid(s):
    try:
        return ObjectId(s)
    except Exception:
        raise HTTPException(400, "Invalid ID: " + str(s))


def _serialize(doc):
    if not doc:
        return None
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    doc.pop("password_hash", None)
    return doc


def _t2m(t: str) -> int:
    h, m = map(int, t.split(":"))
    return h * 60 + m


def _make_token(uid: str, role: str, email: str) -> str:
    payload = {
        "sub": uid, "role": role, "email": email,
        "exp": datetime.now() + timedelta(hours=24),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def _decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired - please log in again")
    except jwt.PyJWTError:
        raise HTTPException(401, "Invalid token")


async def get_current_user(creds: HTTPAuthorizationCredentials = Depends(bearer_scheme)):
    if not creds:
        raise HTTPException(401, "Authentication required")
    payload = _decode_token(creds.credentials)
    user = await col_users.find_one({"_id": _oid(payload["sub"])})
    if not user:
        raise HTTPException(401, "User not found")
    return _serialize(user)


def require(*roles):
    async def _check(u=Depends(get_current_user)):
        if u["role"] not in roles:
            raise HTTPException(403, "Access denied")
        return u
    return _check


async def _overlap(user_id, date, start, end, exclude_id=None):
    q = {"user_id": user_id, "date": date, "status": {"$ne": "running"}}
    if exclude_id:
        q["_id"] = {"$ne": _oid(exclude_id)}
    async for e in col_entries.find(q):
        es, ee = _t2m(e["start_time"]), _t2m(e["end_time"])
        ns, ne = _t2m(start), _t2m(end)
        if ns < ee and ne > es:
            return True
    return False


# ── PYDANTIC MODELS ──────────────────────────────────────────────────────────

class LoginBody(BaseModel):
    email: str
    password: str

class RegisterBody(BaseModel):
    name: str
    email: str
    password: str
    role: Optional[str] = None
    department: Optional[str] = ""
    employment_type: Optional[str] = "payroll"
    shift: Optional[int] = 1
    manager_id: Optional[str] = None

class EntryCreate(BaseModel):
    project_id: str
    task_id: Optional[str] = None
    date: str
    start_time: str
    end_time: str
    description: Optional[str] = ""
    is_billable: bool = True

class EntryUpdate(BaseModel):
    project_id: Optional[str] = None
    task_id: Optional[str] = None
    date: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    description: Optional[str] = None
    is_billable: Optional[bool] = None

class ActionComment(BaseModel):
    comment: Optional[str] = ""

class ProjectCreate(BaseModel):
    name: str
    client_id: str
    billing_rate: float = 0.0
    currency: str = "USD"
    color: str = "#4F46E5"
    assigned_employees: List[str] = []

class ClientCreate(BaseModel):
    name: str
    industry: Optional[str] = ""

class TaskCreate(BaseModel):
    project_id: str
    name: str
class TimerStart(BaseModel):
    project_id: str
    task_name: str              # free-text task name
    description: Optional[str] = ""
    tz_offset_mins: int = 0     # client's UTC offset in minutes (e.g. +330 for IST)

class DailySummaryRequest(BaseModel):
    date: Optional[str] = None
    recipient_emails: List[str] = []
    cc_emails: List[str] = []

class TimerStop(BaseModel):
    entry_id: str
    tz_offset_mins: int = 0     # client's UTC offset in minutes


# ── STARTUP ──────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    import asyncio
    await col_users.create_index("email", unique=True)
    await col_entries.create_index([("user_id", 1), ("date", 1)])
    await col_entries.create_index("status")
    await col_entries.create_index([("user_id", 1), ("status", 1)])
    for attempt in range(10):
        try:
            await mongo_client.admin.command("ping")
            print("✅ MongoDB connected")
            break
        except Exception as e:
            if attempt == 9:
                print(f"❌ MongoDB not reachable after 10 attempts: {e}")
            else:
                print(f"⏳ Waiting for MongoDB... ({attempt+1}/10)")
                await asyncio.sleep(2)
    print("✅ ChronoTrack API ready on http://localhost:5000")
    print("📖 API Docs: http://localhost:5000/docs")
    scheduler.start_scheduler(db, email_service)
@app.on_event("shutdown")
async def shutdown():
    scheduler.stop_scheduler()

# ── HEALTH ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "ChronoTrack API", "version": "2.0.0"}


# ── AUTH ─────────────────────────────────────────────────────────────────────

@app.post("/api/auth/login")
async def login(body: LoginBody):
    user = await col_users.find_one({"email": body.email.lower().strip()})
    if not user:
        raise HTTPException(401, "Invalid email or password")
    if not bcrypt.checkpw(body.password.encode(), user["password_hash"].encode()):
        raise HTTPException(401, "Invalid email or password")
    token = _make_token(str(user["_id"]), user["role"], user["email"])
    return {"access_token": token, "token_type": "bearer", "user": _serialize(user)}


@app.post("/api/auth/register", status_code=201)
async def register(body: RegisterBody):
    if await col_users.find_one({"email": body.email.lower().strip()}):
        raise HTTPException(400, "Email already registered")
    allowed_roles = {"admin", "manager", "employee"}
    if body.role and body.role in allowed_roles:
        role = body.role
    else:
        role = "admin" if "@admin.company.com" in body.email else "manager" if "@mgr.company.com" in body.email else "employee"
    hashed = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    doc = {
        "name": body.name, "email": body.email.lower().strip(),
        "password_hash": hashed, "role": role,
        "department": body.department, "employment_type": body.employment_type,
        "shift": body.shift, "manager_id": body.manager_id,
        "created_at": datetime.utcnow().isoformat(),
    }
    r = await col_users.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    doc.pop("password_hash")
    token = _make_token(doc["id"], role, body.email)
    return {"access_token": token, "token_type": "bearer", "user": doc}


@app.get("/api/me")
async def me(u=Depends(get_current_user)):
    return u


# ── USERS ────────────────────────────────────────────────────────────────────

@app.get("/api/users")
async def list_users(u=Depends(require("admin", "manager"))):
    docs = await col_users.find({}, {"password_hash": 0}).to_list(500)
    return [_serialize(d) for d in docs]


@app.post("/api/users", status_code=201)
async def create_user(body: RegisterBody, u=Depends(require("admin"))):
    if await col_users.find_one({"email": body.email.lower()}):
        raise HTTPException(400, "Email already registered")
    allowed_roles = {"admin", "manager", "employee"}
    role = body.role if body.role and body.role in allowed_roles else "employee"
    hashed = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    doc = {
        "name": body.name, "email": body.email.lower(),
        "password_hash": hashed, "role": role,
        "department": body.department, "employment_type": body.employment_type,
        "shift": body.shift, "manager_id": body.manager_id,
        "created_at": datetime.utcnow().isoformat(),
    }
    r = await col_users.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    doc.pop("_id", None)          # ← FIX
    doc.pop("password_hash", None)
    return doc

# ── CLIENTS ──────────────────────────────────────────────────────────────────

@app.get("/api/clients")
async def list_clients(u=Depends(get_current_user)):
    docs = await col_clients.find().to_list(200)
    return [_serialize(d) for d in docs]


@app.post("/api/clients", status_code=201)
async def create_client(body: ClientCreate, u=Depends(require("admin"))):
    doc = {**body.dict(), "created_at": datetime.utcnow().isoformat()}
    r = await col_clients.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    return doc


# ── PROJECTS ─────────────────────────────────────────────────────────────────

@app.get("/api/projects")
async def list_projects(u=Depends(get_current_user)):
    q = {} if u["role"] == "admin" else {"assigned_employees": u["id"]}
    docs = await col_projects.find(q).to_list(200)
    return [_serialize(d) for d in docs]


@app.post("/api/projects", status_code=201)
async def create_project(body: ProjectCreate, u=Depends(require("admin", "manager"))):
    doc = {**body.dict(), "status": "active", "created_by": u["id"], "created_at": datetime.utcnow().isoformat()}
    # FIND: create_project function, the last 3 lines:
    r = await col_projects.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    doc.pop("_id", None)
    return doc

# REPLACE with (no change needed — it already has pop, just verify it's there)

@app.delete("/api/projects/{pid}", status_code=204)
async def delete_project(pid: str, u=Depends(require("admin"))):
    result = await col_projects.delete_one({"_id": _oid(pid)})
    if result.deleted_count == 0:
        raise HTTPException(404, "Project not found")

# ── TASKS ────────────────────────────────────────────────────────────────────

@app.get("/api/tasks")
async def list_tasks(project_id: Optional[str] = None, u=Depends(get_current_user)):
    q = {"project_id": project_id} if project_id else {}
    docs = await col_tasks.find(q).to_list(500)
    return [_serialize(d) for d in docs]


# FIND: create_task function
# REPLACE with:

@app.post("/api/tasks", status_code=201)
async def create_task(body: TaskCreate, u=Depends(require("admin", "manager", "employee"))):
    # Any authenticated user can create custom tasks
    if not body.name.strip():
        raise HTTPException(400, "Task name cannot be empty")
    doc = {
        **body.dict(),
        "name": body.name.strip(),
        "created_by": u["id"],
        "created_at": datetime.utcnow().isoformat(),
    }
    r = await col_tasks.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    doc.pop("_id", None)          # ← CRITICAL FIX
    return doc

# ── TIMER ────────────────────────────────────────────────────────────────────

@app.get("/api/timer/active")
async def get_active_timer(u=Depends(get_current_user)):
    """Return the currently running timer entry for this user, or null."""
    entry = await col_entries.find_one({"user_id": u["id"], "status": "running"})
    return _serialize(entry) if entry else None


@app.post("/api/timer/start", status_code=201)
async def timer_start(body: TimerStart, u=Depends(get_current_user)):
    """
    Create a new 'running' time entry immediately on Start click.
    Prevents duplicate active timers — only one running entry per user allowed.
    """
    # Prevent duplicate active timers
    existing = await col_entries.find_one({"user_id": u["id"], "status": "running"})
    if existing:
        raise HTTPException(
            400,
            "You already have an active timer running. Stop it before starting a new one."
        )

    if not body.project_id:
        raise HTTPException(400, "Project is required to start a timer.")

    task_name = body.task_name.strip() if body.task_name else ""

    now = datetime.utcnow()
    now_iso = now.isoformat()
    # Shift to client's local time using the browser-supplied UTC offset
    local_now  = now + timedelta(minutes=body.tz_offset_mins)
    date_str   = local_now.strftime("%Y-%m-%d")
    start_time = local_now.strftime("%H:%M")

    holiday = await col_holidays.find_one({"date": date_str})

    doc = {
        "user_id":          u["id"],
        "project_id":       body.project_id,
        "task_id":          None,           # no pre-existing task — task_name stored in description prefix
        "task_name":        task_name,       # custom field for display
        "date":             date_str,
        "start_time":       start_time,
        "end_time":         None,            # not set until Stop
        "duration_mins":    0,
        "description":      body.description or "",
        "is_billable":      True,
        "is_holiday":       holiday is not None,
        "is_overtime":      False,
        "status":           "running",       # new lifecycle status
        "timer_started_at": now_iso,         # precise epoch for front-end sync
        "submitted_at":     None,
        "approved_by":      None,
        "approval_comment": None,
        "created_at":       now_iso,
        "updated_at":       now_iso,
    }

    r = await col_entries.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    doc.pop("_id", None)
    return doc


@app.post("/api/timer/stop")
async def timer_stop(body: TimerStop, u=Depends(get_current_user)):
    """
    Finalise a running timer entry: compute end_time + duration, set status='draft'.
    """
    entry = await col_entries.find_one({"_id": _oid(body.entry_id)})
    if not entry:
        raise HTTPException(404, "Timer entry not found.")
    if entry["user_id"] != u["id"]:
        raise HTTPException(403, "Not your timer.")
    if entry["status"] != "running":
        raise HTTPException(400, "This entry is not running.")

    now      = datetime.utcnow()
    # Shift to client's local time using the browser-supplied UTC offset
    local_now = now + timedelta(minutes=body.tz_offset_mins)
    end_time  = local_now.strftime("%H:%M")
    date_str  = entry["date"]

    # Handle cross-midnight: if local today != entry date, clamp end_time to 23:59
    if local_now.strftime("%Y-%m-%d") != date_str:
        end_time = "23:59"

    start_mins = _t2m(entry["start_time"])
    end_mins   = _t2m(end_time)
    duration   = max(end_mins - start_mins, 0)

    upd = {
        "end_time":      end_time,
        "duration_mins": duration,
        "is_overtime":   duration > 480,
        "status":        "draft",
        "updated_at":    now.isoformat(),
    }

    await col_entries.update_one({"_id": _oid(body.entry_id)}, {"$set": upd})
    doc = await col_entries.find_one({"_id": _oid(body.entry_id)})
    return _serialize(doc)

# ── TIME ENTRIES ─────────────────────────────────────────────────────────────

@app.get("/api/entries")
async def list_entries(
    week: Optional[str] = None,
    status: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    user_id: Optional[str] = None,
    u=Depends(get_current_user),
):
    q = {}
    if u["role"] == "employee":
        q["user_id"] = u["id"]
    elif u["role"] == "manager":
        team = await col_users.find({"manager_id": u["id"]}, {"_id": 1}).to_list(200)
        tids = [str(t["_id"]) for t in team] + [u["id"]]
        q["user_id"] = {"$in": tids}
    else:
        if user_id:
            q["user_id"] = user_id

    if status:
        q["status"] = status

    if week:
        import re
        m = re.match(r"(\d{4})-W(\d{2})", week)
        if m:
            yr, wk = int(m.group(1)), int(m.group(2))
            jan1 = datetime(yr, 1, 1)
            mon  = jan1 + timedelta(weeks=wk - 1, days=-(jan1.weekday()))
            sun  = mon + timedelta(days=6)
            q["date"] = {"$gte": mon.strftime("%Y-%m-%d"), "$lte": sun.strftime("%Y-%m-%d")}

    if from_date or to_date:
        q.setdefault("date", {})
        if from_date: q["date"]["$gte"] = from_date
        if to_date:   q["date"]["$lte"] = to_date

    docs = await col_entries.find(q).sort("date", -1).to_list(2000)
    return [_serialize(d) for d in docs]


# FIND: create_entry function (~line 232)
# REPLACE the entire function with:

@app.post("/api/entries", status_code=201)
async def create_entry(body: EntryCreate, u=Depends(get_current_user)):
    mins = _t2m(body.end_time) - _t2m(body.start_time)
    if mins <= 0:
        raise HTTPException(400, "End time must be after start time")
    if mins > 1440:
        raise HTTPException(400, "Entry cannot exceed 24 hours")
    if not body.date:
        raise HTTPException(400, "Date is required")
    if not body.project_id:
        raise HTTPException(400, "Project is required")
    if await _overlap(u["id"], body.date, body.start_time, body.end_time):
        raise HTTPException(
            400,
            f"Time {body.start_time}–{body.end_time} overlaps with an existing entry on {body.date}"
        )
    holiday = await col_holidays.find_one({"date": body.date})
    now_iso = datetime.utcnow().isoformat()
    doc = {
        "user_id":        u["id"],
        "project_id":     body.project_id,
        "task_id":        body.task_id,
        "date":           body.date,
        "start_time":     body.start_time,
        "end_time":       body.end_time,
        "duration_mins":  mins,
        "description":    body.description or "",
        "is_billable":    body.is_billable,
        "is_holiday":     holiday is not None,
        "is_overtime":    mins > 480,
        "status":         "draft",
        "submitted_at":   None,
        "approved_by":    None,
        "approval_comment": None,
        "created_at":     now_iso,
        "updated_at":     now_iso,
    }
    r = await col_entries.insert_one(doc)
    doc["id"] = str(r.inserted_id)
    doc.pop("_id", None)          # ← CRITICAL FIX: remove non-serializable ObjectId
    return doc


@app.put("/api/entries/{eid}")
async def update_entry(eid: str, body: EntryUpdate, u=Depends(get_current_user)):
    entry = await col_entries.find_one({"_id": _oid(eid)})
    if not entry:
        raise HTTPException(404, "Entry not found")
    if entry["user_id"] != u["id"] and u["role"] not in ("manager", "admin"):
        raise HTTPException(403, "Not authorised")
    if entry["status"] not in ("draft", "rejected"):
        raise HTTPException(400, "Only draft or rejected entries can be edited")
    upd = {k: v for k, v in body.dict().items() if v is not None}
    start = upd.get("start_time", entry["start_time"])
    end   = upd.get("end_time",   entry["end_time"])
    mins  = _t2m(end) - _t2m(start)
    if mins <= 0:
        raise HTTPException(400, "End time must be after start time")
    upd["duration_mins"] = mins
    upd["is_overtime"] = mins > 480
    upd["updated_at"] = datetime.utcnow().isoformat()
    await col_entries.update_one({"_id": _oid(eid)}, {"$set": upd})
    doc = await col_entries.find_one({"_id": _oid(eid)})
    return _serialize(doc)


@app.delete("/api/entries/{eid}", status_code=204)
async def delete_entry(eid: str, u=Depends(get_current_user)):
    entry = await col_entries.find_one({"_id": _oid(eid)})
    if not entry:
        raise HTTPException(404, "Entry not found")
    if entry["user_id"] != u["id"] and u["role"] != "admin":
        raise HTTPException(403, "Not authorised")
    if entry["status"] == "approved" and u["role"] != "admin":
        raise HTTPException(400, "Approved entries are locked")
    await col_entries.delete_one({"_id": _oid(eid)})


@app.post("/api/entries/submit-week")
async def submit_week(u=Depends(get_current_user)):
    res = await col_entries.update_many(
        {"user_id": u["id"], "status": "draft"},   # "running" entries excluded intentionally
        {"$set": {"status": "submitted", "submitted_at": datetime.utcnow().isoformat()}}
    )
    return {"submitted": res.modified_count}


@app.post("/api/entries/bulk-approve")
async def bulk_approve(user_id: str, body: ActionComment, u=Depends(require("manager", "admin"))):
    res = await col_entries.update_many(
        {"user_id": user_id, "status": "submitted"},
        {"$set": {"status": "approved", "approved_by": u["id"], "approval_comment": body.comment}}
    )
    await col_notifs.insert_one({
        "user_id": user_id, "type": "success",
        "message": f"Your timesheet was approved by {u['name']}.",
        "is_read": False, "created_at": datetime.utcnow().isoformat(),
    })
    return {"approved": res.modified_count}


@app.post("/api/entries/bulk-reject")
async def bulk_reject(user_id: str, body: ActionComment, u=Depends(require("manager", "admin"))):
    res = await col_entries.update_many(
        {"user_id": user_id, "status": "submitted"},
        {"$set": {"status": "rejected", "approved_by": u["id"], "approval_comment": body.comment}}
    )
    await col_notifs.insert_one({
        "user_id": user_id, "type": "error",
        "message": f"Your timesheet was rejected by {u['name']}. Reason: {body.comment or 'None'}",
        "is_read": False, "created_at": datetime.utcnow().isoformat(),
    })
    return {"rejected": res.modified_count}


@app.post("/api/entries/{eid}/approve")
async def approve_entry(eid: str, body: ActionComment, u=Depends(require("manager", "admin"))):
    entry = await col_entries.find_one({"_id": _oid(eid)})
    if not entry or entry["status"] != "submitted":
        raise HTTPException(400, "Entry must be in submitted state")
    await col_entries.update_one({"_id": _oid(eid)}, {"$set": {"status": "approved", "approved_by": u["id"], "approval_comment": body.comment}})
    return {"status": "approved"}


@app.post("/api/entries/{eid}/reject")
async def reject_entry(eid: str, body: ActionComment, u=Depends(require("manager", "admin"))):
    entry = await col_entries.find_one({"_id": _oid(eid)})
    if not entry or entry["status"] != "submitted":
        raise HTTPException(400, "Entry must be in submitted state")
    await col_entries.update_one({"_id": _oid(eid)}, {"$set": {"status": "rejected", "approved_by": u["id"], "approval_comment": body.comment}})
    return {"status": "rejected"}


# ── REPORTS ──────────────────────────────────────────────────────────────────

@app.get("/api/reports/summary")
async def report_summary(
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    user_id: Optional[str] = None,
    u=Depends(get_current_user),
):
    q = {}
    if u["role"] == "employee":
        q["user_id"] = u["id"]
    elif user_id and u["role"] in ("admin", "manager"):
        q["user_id"] = user_id
    if from_date: q.setdefault("date", {})["$gte"] = from_date
    if to_date:   q.setdefault("date", {})["$lte"] = to_date

    entries = await col_entries.find(q).to_list(10000)
    total  = sum(e["duration_mins"] for e in entries)
    bill   = sum(e["duration_mins"] for e in entries if e.get("is_billable"))
    apprd  = sum(e["duration_mins"] for e in entries if e["status"] == "approved")
    dm = {}
    for e in entries:
        dm[e["date"]] = dm.get(e["date"], 0) + e["duration_mins"]
    ot = sum(1 for m in dm.values() if m > 480)

    by_proj = {}
    for e in entries:
        pid = e["project_id"]
        by_proj.setdefault(pid, {"total_mins": 0, "billable_mins": 0})
        by_proj[pid]["total_mins"] += e["duration_mins"]
        if e.get("is_billable"):
            by_proj[pid]["billable_mins"] += e["duration_mins"]

    return {
        "total_hours":    round(total / 60, 2),
        "billable_hours": round(bill / 60, 2),
        "approved_hours": round(apprd / 60, 2),
        "billable_pct":   round(bill / total * 100 if total else 0, 1),
        "overtime_days":  ot,
        "entry_count":    len(entries),
        "by_project":     by_proj,
        "daily":          {d: {"total_mins": m} for d, m in dm.items()},
    }
@app.post("/api/email/send-daily-summary")
async def send_daily_summary_email(
    body: DailySummaryRequest,
    u=Depends(get_current_user),
):
    """
    Fetches today's (or a specific date's) completed time entries for the
    requesting user, builds a project-grouped summary, and sends a
    professional HTML email to the specified recipients.

    Employees always receive a copy. Manager is auto-CC'd if assigned.
    """
    from bson import ObjectId as BsonOid

    # ── Resolve date ──────────────────────────────────────────────────────
    target_date = body.date or datetime.utcnow().strftime("%Y-%m-%d")
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD.")

    # ── Fetch entries ─────────────────────────────────────────────────────
    entries = await col_entries.find({
        "user_id": u["id"],
        "date":    target_date,
        "status":  {"$ne": "running"},
    }).to_list(500)

    if not entries:
        raise HTTPException(
            404,
            f"No completed time entries found for {target_date}. "
            "Start and stop a timer first, or add entries manually."
        )

    # ── Resolve projects ──────────────────────────────────────────────────
    proj_ids_set = {e["project_id"] for e in entries}
    proj_docs = await col_projects.find(
        {"_id": {"$in": [_oid(pid) for pid in proj_ids_set]}}
    ).to_list(100)
    proj_map = {str(p["_id"]): p for p in proj_docs}

    # ── Group entries by project ──────────────────────────────────────────
    groups: dict = {}
    for e in entries:
        pid = e["project_id"]
        if pid not in groups:
            proj = proj_map.get(pid, {})
            groups[pid] = {
                "project_name":      proj.get("name", "Unknown Project"),
                "color":             proj.get("color", "#4F46E5"),
                "entries":           [],
                "project_total_mins": 0,
            }
        groups[pid]["entries"].append({
            "task_name":     e.get("task_name") or "",
            "description":   e.get("description") or "",
            "start_time":    e.get("start_time") or "",
            "end_time":      e.get("end_time") or "",
            "duration_mins": e.get("duration_mins") or 0,
        })
        groups[pid]["project_total_mins"] += e.get("duration_mins") or 0

    project_groups  = list(groups.values())
    total_mins      = sum(e.get("duration_mins") or 0 for e in entries)

    # ── Build recipient list ──────────────────────────────────────────────
    # Build recipient list — only the addresses the user explicitly provided.
    # Do NOT auto-add the sender's own email; let them add it themselves if desired.
    to_addresses = list(dict.fromkeys(body.recipient_emails))   # deduplicate, preserve order

    cc_addresses = list(set(body.cc_emails))
    if u.get("manager_id"):
        mgr = await col_users.find_one({"_id": _oid(u["manager_id"])})
        if mgr and mgr.get("email"):
            cc_addresses = list({mgr["email"], *cc_addresses})

    # ── Format date for display ───────────────────────────────────────────
    try:
        d_obj = datetime.strptime(target_date, "%Y-%m-%d")
        date_label = d_obj.strftime("%A, %d %B %Y")
    except Exception:
        date_label = target_date

    # ── Send email ────────────────────────────────────────────────────────
    try:
        result = email_service.send_daily_summary(
            to_addresses    = to_addresses,
            employee_name   = u["name"],
            date_label      = date_label,
            project_groups  = project_groups,
            total_mins      = total_mins,
            total_entries   = len(entries),
            cc_addresses    = cc_addresses or None,
        )
    except email_service.EmailConfigError as exc:
        raise HTTPException(503, f"Email not configured: {exc}")
    except Exception as exc:
        raise HTTPException(500, f"Failed to send email: {exc}")

    # ── Log a notification ────────────────────────────────────────────────
    await col_notifs.insert_one({
        "user_id":    u["id"],
        "type":       "success",
        "message":    f"Daily summary email sent for {date_label} to {', '.join(result['recipients'])}.",
        "is_read":    False,
        "created_at": datetime.utcnow().isoformat(),
    })

    return {
        "sent":        True,
        "date":        target_date,
        "recipients":  result["recipients"],
        "entry_count": len(entries),
        "total_mins":  total_mins,
        "projects":    len(project_groups),
    }


@app.get("/api/email/preview-daily-summary")
async def preview_daily_summary(
    date: Optional[str] = None,
    u=Depends(get_current_user),
):
    """
    Returns the JSON data that would be emailed — useful for frontend
    preview and debugging without actually sending.
    """
    target_date = date or datetime.utcnow().strftime("%Y-%m-%d")

    entries = await col_entries.find({
        "user_id": u["id"],
        "date":    target_date,
        "status":  {"$ne": "running"},
    }).to_list(500)

    if not entries:
        return {
            "date":         target_date,
            "entry_count":  0,
            "total_mins":   0,
            "project_groups": [],
        }

    proj_ids_set = {e["project_id"] for e in entries}
    proj_docs = await col_projects.find(
        {"_id": {"$in": [_oid(pid) for pid in proj_ids_set]}}
    ).to_list(100)
    proj_map = {str(p["_id"]): p for p in proj_docs}

    groups: dict = {}
    for e in entries:
        pid = e["project_id"]
        if pid not in groups:
            proj = proj_map.get(pid, {})
            groups[pid] = {
                "project_name":       proj.get("name", "Unknown"),
                "color":              proj.get("color", "#4F46E5"),
                "entries":            [],
                "project_total_mins": 0,
            }
        groups[pid]["entries"].append({
            "task_name":     e.get("task_name") or "",
            "description":   e.get("description") or "",
            "start_time":    e.get("start_time") or "",
            "end_time":      e.get("end_time") or "",
            "duration_mins": e.get("duration_mins") or 0,
        })
        groups[pid]["project_total_mins"] += e.get("duration_mins") or 0

    return {
        "date":           target_date,
        "employee":       u["name"],
        "entry_count":    len(entries),
        "total_mins":     sum(e.get("duration_mins") or 0 for e in entries),
        "project_groups": list(groups.values()),
    }



@app.get("/api/reports/export")
async def export_csv(from_date: Optional[str] = None, to_date: Optional[str] = None, u=Depends(get_current_user)):
    q = {}
    if u["role"] == "employee":
        q["user_id"] = u["id"]
    if from_date: q.setdefault("date", {})["$gte"] = from_date
    if to_date:   q.setdefault("date", {})["$lte"] = to_date
    entries = await col_entries.find(q).sort("date", 1).to_list(20000)

    proj_map = {}
    async for p in col_projects.find({}, {"name": 1}):
        proj_map[str(p["_id"])] = p["name"]
    task_map = {}
    async for t in col_tasks.find({}, {"name": 1}):
        task_map[str(t["_id"])] = t["name"]
    user_map = {}
    async for usr in col_users.find({}, {"name": 1}):
        user_map[str(usr["_id"])] = usr["name"]

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Date","Employee","Project","Task","Start","End","Duration(h)","Description","Billable","Status"])
    for e in entries:
        w.writerow([
            e.get("date",""), user_map.get(e.get("user_id",""),""),
            proj_map.get(e.get("project_id",""),""), task_map.get(e.get("task_id",""),""),
            e.get("start_time",""), e.get("end_time",""),
            round(e.get("duration_mins",0)/60,2), e.get("description",""),
            "Yes" if e.get("is_billable") else "No", e.get("status",""),
        ])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=timesheet_export.csv"})


# ── NOTIFICATIONS ─────────────────────────────────────────────────────────────

@app.get("/api/notifications")
async def list_notifs(u=Depends(get_current_user)):
    docs = await col_notifs.find({"user_id": u["id"]}).sort("created_at", -1).to_list(50)
    return [_serialize(d) for d in docs]


@app.put("/api/notifications/read-all")
async def read_all(u=Depends(get_current_user)):
    await col_notifs.update_many({"user_id": u["id"]}, {"$set": {"is_read": True}})
    return {"ok": True}


# ── HOLIDAYS ─────────────────────────────────────────────────────────────────

@app.get("/api/holidays")
async def list_holidays(u=Depends(get_current_user)):
    docs = await col_holidays.find().to_list(100)
    return [_serialize(d) for d in docs]


# ── SEED ─────────────────────────────────────────────────────────────────────

@app.post("/api/dev/seed")
async def seed():
    """Wipe all collections and seed realistic demo data."""

    for c in [col_users, col_entries, col_projects, col_tasks, col_clients, col_holidays, col_notifs]:
        await c.delete_many({})

    pw = bcrypt.hashpw(b"password123", bcrypt.gensalt()).decode()

    # Holidays
    await col_holidays.insert_many([
        {"date": "2025-01-26", "name": "Republic Day"},
        {"date": "2025-03-31", "name": "Holi"},
        {"date": "2025-08-15", "name": "Independence Day"},
        {"date": "2025-10-02", "name": "Gandhi Jayanti"},
        {"date": "2025-11-12", "name": "Diwali"},
        {"date": "2025-12-25", "name": "Christmas"},
    ])

    # Clients
    raw_clients = [
        ("Nexus Corp", "Finance"), ("Orbital Labs", "SaaS"),
        ("Meridian Health", "Healthcare"), ("Vanta Retail", "E-commerce"),
        ("ZeroPoint AI", "AI/ML"),
    ]
    client_ids = []
    for name, ind in raw_clients:
        r = await col_clients.insert_one({"name": name, "industry": ind, "created_at": datetime.utcnow().isoformat()})
        client_ids.append(str(r.inserted_id))

    # Admin
    r = await col_users.insert_one({
        "name": "Admin HR", "email": "admin@admin.company.com",
        "password_hash": pw, "role": "admin", "department": "HR",
        "employment_type": "payroll", "shift": 1, "manager_id": None,
        "created_at": datetime.utcnow().isoformat(),
    })

    # Managers
    mgr_specs = [
        ("Jane Wilson",  "mgr.jane@mgr.company.com",  "Engineering", 1),
        ("Tom Parker",   "mgr.tom@mgr.company.com",   "Product",     1),
        ("Sara Lee",     "mgr.sara@mgr.company.com",  "QA & AI",     2),
    ]
    mgr_ids = []
    for name, email, dept, shift in mgr_specs:
        r = await col_users.insert_one({
            "name": name, "email": email, "password_hash": pw,
            "role": "manager", "department": dept,
            "employment_type": "payroll", "shift": shift, "manager_id": None,
            "created_at": datetime.utcnow().isoformat(),
        })
        mgr_ids.append(str(r.inserted_id))

    # Employees
    emp_specs = [
        ("Aryan Mehta",    "aryan@company.com",     "Engineering", 1, "payroll",  0),
        ("Priya Sharma",   "priya@company.com",     "Engineering", 1, "payroll",  0),
        ("Rohan Gupta",    "rohan@company.com",     "Engineering", 2, "payroll",  0),
        ("Sneha Patel",    "sneha@company.com",     "Engineering", 2, "payroll",  1),
        ("Karan Singh",    "karan@company.com",     "QA",          1, "payroll",  1),
        ("Divya Nair",     "divya@company.com",     "Design",      3, "payroll",  1),
        ("Akash Verma",    "akash@company.com",     "Backend",     2, "payroll",  2),
        ("Nisha Reddy",    "nisha@company.com",     "QA",          3, "payroll",  2),
        ("Vikram Joshi",   "vikram@company.com",    "AI/ML",       1, "payroll",  2),
        ("Pooja Iyer",     "pooja@company.com",     "Frontend",    2, "payroll",  0),
        ("Siddharth Rao",  "siddharth@company.com", "Frontend",    3, "payroll",  1),
        ("Meera Jain",     "meera@company.com",     "DevOps",      1, "payroll",  2),
        ("Chris Bauer",    "chris@contract.dev",    "Backend",     2, "contract", 0),
        ("Emily Stone",    "emily@contract.dev",    "AI/ML",       3, "contract", 2),
        ("Leo Tanaka",     "leo@contract.dev",      "Frontend",    1, "contract", 1),
        ("Sofia Gonzalez", "sofia@contract.dev",    "Design",      2, "contract", 0),
        ("Raj Kumar",      "raj@contract.dev",      "QA",          3, "contract", 2),
    ]
    emp_ids = []
    for name, email, dept, shift, etype, midx in emp_specs:
        r = await col_users.insert_one({
            "name": name, "email": email, "password_hash": pw,
            "role": "employee", "department": dept,
            "employment_type": etype, "shift": shift,
            "manager_id": mgr_ids[midx],
            "created_at": datetime.utcnow().isoformat(),
        })
        emp_ids.append(str(r.inserted_id))

    # Projects  (name, client_idx, billing_rate, color, [emp_indices])
    proj_specs = [
        ("Banking Portal Revamp",    0, 180, "#4F46E5", [0,1,2,3,4]),
        ("SaaS Dashboard v3",        1, 150, "#06B6D4", [1,3,5,6]),
        ("Patient Portal",           2, 200, "#10B981", [2,4,7,8]),
        ("Mobile Commerce App",      3, 160, "#F59E0B", [0,5,9,10]),
        ("LLM Pipeline",             4, 220, "#EF4444", [3,6,8,11]),
        ("Internal DevOps",          0,   0, "#8B5CF6", [0,1,11]),
        ("QA Automation Suite",      1, 130, "#EC4899", [4,7,10]),
        ("Data Analytics Dashboard", 4, 175, "#14B8A6", [2,5,11]),
    ]
    proj_ids   = []
    proj_rates = []
    proj_names = []
    for name, ci, rate, color, eidxs in proj_specs:
        assigned = [emp_ids[i] for i in eidxs if i < len(emp_ids)]
        r = await col_projects.insert_one({
            "name": name, "client_id": client_ids[ci],
            "billing_rate": rate, "currency": "USD", "color": color,
            "assigned_employees": assigned, "status": "active",
            "created_at": datetime.utcnow().isoformat(),
        })
        proj_ids.append(str(r.inserted_id))
        proj_rates.append(rate)
        proj_names.append(name)

    # Tasks
    task_groups = [
        ["UI Components","API Integration","Testing"],
        ["Dashboard Design","Backend API","Analytics"],
        ["Patient Records","Appointments","Notifications"],
        ["Product Listing","Checkout Flow","Push Notifications"],
        ["Model Training","Inference API","Evaluation"],
        ["CI/CD Pipelines","Docker Setup"],
        ["Test Cases","Automation Scripts"],
        ["ETL Pipeline","Visualization"],
    ]
    task_ids_by_proj = {}
    for pi, pid in enumerate(proj_ids):
        task_ids_by_proj[pid] = []
        for tn in task_groups[pi]:
            r = await col_tasks.insert_one({"project_id": pid, "name": tn, "created_at": datetime.utcnow().isoformat()})
            task_ids_by_proj[pid].append(str(r.inserted_id))

    # Build assigned map
    assigned_map = {}
    for pi, (_, _, _, _, eidxs) in enumerate(proj_specs):
        for ei in eidxs:
            if ei < len(emp_ids):
                uid = emp_ids[ei]
                assigned_map.setdefault(uid, [])
                assigned_map[uid].append(proj_ids[pi])

    # Time entries - 12 weeks per employee
    entries_bulk = []
    now = datetime.utcnow()
    holiday_dates = {"2025-01-26","2025-03-31","2025-08-15","2025-10-02","2025-11-12","2025-12-25"}
    verbs = ["Working on","Implementing","Reviewing","Testing","Designing","Debugging"]

    for eidx, uid in enumerate(emp_ids):
        shift = emp_specs[eidx][3]
        my_projs = assigned_map.get(uid, [proj_ids[0]])
        sh = {1: 9, 2: 14, 3: 21}[shift]

        for w in range(12):
            for d in range(5):
                entry_date = now - timedelta(weeks=w, days=d)
                ds = entry_date.strftime("%Y-%m-%d")
                if ds in holiday_dates:
                    continue

                pid = my_projs[d % len(my_projs)]
                pi  = proj_ids.index(pid)
                tids = task_ids_by_proj.get(pid, [])
                tid  = tids[d % len(tids)] if tids else None

                dur = random.randint(360, 540)
                end_total = sh * 60 + dur
                end_h = (end_total // 60) % 24
                end_m = end_total % 60

                st = "draft" if w == 0 else "submitted" if w == 1 else "approved"

                entries_bulk.append({
                    "user_id": uid, "project_id": pid, "task_id": tid,
                    "date": ds,
                    "start_time": f"{sh:02d}:00",
                    "end_time":   f"{end_h:02d}:{end_m:02d}",
                    "duration_mins": dur,
                    "description": f"{random.choice(verbs)} {proj_names[pi]}",
                    "is_billable": proj_rates[pi] > 0,
                    "is_holiday": False,
                    "is_overtime": dur > 480,
                    "status": st,
                    "submitted_at": datetime.utcnow().isoformat() if st != "draft" else None,
                    "approved_by": None,
                    "approval_comment": None,
                    "created_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat(),
                })

    if entries_bulk:
        await col_entries.insert_many(entries_bulk)

    # Welcome notifications
    admin_doc = await col_users.find_one({"email": "admin@admin.company.com"})
    if admin_doc:
        await col_notifs.insert_many([
            {"user_id": str(admin_doc["_id"]), "type": "success",
             "message": "Welcome to ChronoTrack! Demo data has been seeded successfully.",
             "is_read": False, "created_at": datetime.utcnow().isoformat()},
            {"user_id": str(admin_doc["_id"]), "type": "info",
             "message": f"{len(emp_ids)} employees and {len(entries_bulk)} time entries loaded.",
             "is_read": False, "created_at": datetime.utcnow().isoformat()},
        ])

    return {
        "message": "Seed complete!",
        "users": len(emp_ids) + len(mgr_ids) + 1,
        "clients": len(client_ids),
        "projects": len(proj_ids),
        "entries": len(entries_bulk),
    }
