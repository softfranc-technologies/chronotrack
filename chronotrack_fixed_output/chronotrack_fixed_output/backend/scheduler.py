"""
ChronoTrack - Daily Email Scheduler
Uses APScheduler to fire end-of-day email reports automatically.
Optional — only activated if ENABLE_EMAIL_SCHEDULER=true in .env
"""

# asyncio is available via FastAPI's event loop
import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

ENABLE_EMAIL_SCHEDULER = os.getenv("ENABLE_EMAIL_SCHEDULER", "false").lower() == "true"
# 24-hour HH:MM — default 17:30 (5:30 PM)
EMAIL_SCHEDULE_TIME = os.getenv("EMAIL_SCHEDULE_TIME", "17:30")
# Comma-separated default manager/admin recipients for scheduled sends
EMAIL_DEFAULT_RECIPIENTS = [
    addr.strip()
    for addr in os.getenv("EMAIL_DEFAULT_RECIPIENTS", "").split(",")
    if addr.strip()
]

_scheduler = None


async def _send_all_employee_summaries(db, email_service_module):
    """
    Pull every active employee's today-entries and fire summary emails.
    Called by the APScheduler job.
    """
    from bson import ObjectId

    today = datetime.now().strftime("%Y-%m-%d")
    print(f"[Scheduler] Running end-of-day email job for {today}")

    col_users   = db["users"]
    col_entries = db["time_entries"]
    col_projects = db["projects"]

    employees = await col_users.find({"role": "employee"}).to_list(500)

    for emp in employees:
        uid = str(emp["_id"])
        entries = await col_entries.find({
            "user_id": uid,
            "date": today,
            "status": {"$ne": "running"},
        }).to_list(200)

        if not entries:
            continue  # nothing to report today

        # Build project map
        proj_ids = list({e["project_id"] for e in entries})
        proj_docs = await col_projects.find(
            {"_id": {"$in": [ObjectId(pid) for pid in proj_ids]}}
        ).to_list(100)
        proj_map = {str(p["_id"]): p for p in proj_docs}

        # Group by project
        groups: dict = {}
        for e in entries:
            pid = e["project_id"]
            if pid not in groups:
                proj = proj_map.get(pid, {})
                groups[pid] = {
                    "project_name": proj.get("name", "Unknown Project"),
                    "color": proj.get("color", "#4F46E5"),
                    "entries": [],
                    "project_total_mins": 0,
                }
            groups[pid]["entries"].append({
                "task_name":    e.get("task_name") or e.get("description", "")[:40],
                "description":  e.get("description", ""),
                "start_time":   e.get("start_time", ""),
                "end_time":     e.get("end_time", ""),
                "duration_mins": e.get("duration_mins", 0),
            })
            groups[pid]["project_total_mins"] += e.get("duration_mins", 0)

        total_mins = sum(e.get("duration_mins", 0) for e in entries)
        project_groups = list(groups.values())

        # Determine recipients
        to_addrs = [emp["email"]]
        if emp.get("manager_id"):
            mgr = await col_users.find_one({"_id": ObjectId(emp["manager_id"])})
            if mgr:
                to_addrs.append(mgr["email"])
        to_addrs += EMAIL_DEFAULT_RECIPIENTS

        date_label = datetime.now().strftime("%A, %d %B %Y")

        try:
            result = email_service_module.send_daily_summary(
                to_addresses=list(set(to_addrs)),
                employee_name=emp["name"],
                date_label=date_label,
                project_groups=project_groups,
                total_mins=total_mins,
                total_entries=len(entries),
            )
            print(f"[Scheduler] ✅ Email sent for {emp['name']} → {result['recipients']}")
        except Exception as ex:
            print(f"[Scheduler] ❌ Failed for {emp['name']}: {ex}")


def start_scheduler(db, email_service_module):
    """
    Call once at FastAPI startup if ENABLE_EMAIL_SCHEDULER=true.
    Schedules _send_all_employee_summaries at EMAIL_SCHEDULE_TIME daily.
    """
    if not ENABLE_EMAIL_SCHEDULER:
        print("[Scheduler] Disabled (ENABLE_EMAIL_SCHEDULER != true)")
        return

    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print("[Scheduler] APScheduler not installed — run: pip install apscheduler")
        return

    h, m = EMAIL_SCHEDULE_TIME.split(":")

    global _scheduler
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(
        _send_all_employee_summaries,
        trigger=CronTrigger(hour=int(h), minute=int(m)),
        args=[db, email_service_module],
        id="daily_email",
        replace_existing=True,
        max_instances=1,
    )
    _scheduler.start()
    print(f"[Scheduler] ✅ Daily email job scheduled at {EMAIL_SCHEDULE_TIME} UTC")


def stop_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        print("[Scheduler] Stopped.")
