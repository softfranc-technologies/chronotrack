"""
ChronoTrack - Email Service
Handles HTML email generation and delivery via SMTP (Gmail/Outlook/custom SMTP).
"""

import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import List, Optional
import os
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────

SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")          # sender email
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")      # app password / SMTP password
SMTP_FROM     = os.getenv("SMTP_FROM", SMTP_USER)   # "From" display address
SMTP_TLS      = os.getenv("SMTP_TLS", "true").lower() == "true"


# ── EMAIL TEMPLATE ────────────────────────────────────────────────────────────

def _build_html(
    employee_name: str,
    date_label: str,
    project_groups: List[dict],
    total_mins: int,
    total_entries: int,
    sender_email: str,
) -> str:
    """
    Build a professional, responsive HTML email body.

    project_groups = [
      {
        "project_name": str,
        "color": str,           # hex color
        "entries": [
          {
            "task_name": str,
            "description": str,
            "start_time": str,
            "end_time": str,
            "duration_mins": int,
          }
        ],
        "project_total_mins": int,
      }
    ]
    """

    def fmt_mins(m: int) -> str:
        if m <= 0:
            return "0m"
        h, mn = divmod(m, 60)
        return f"{h}h {mn}m" if h else f"{mn}m"

    def fmt_time(t: str) -> str:
        """Convert 24h HH:MM to 12h with AM/PM."""
        try:
            h, m = map(int, t.split(":"))
            period = "AM" if h < 12 else "PM"
            h12 = h % 12 or 12
            return f"{h12}:{m:02d} {period}"
        except Exception:
            return t or "—"

    # Build project rows
    project_rows_html = ""
    for pg in project_groups:
        color = pg.get("color", "#4F46E5")
        proj_name = pg["project_name"]
        proj_total = fmt_mins(pg["project_total_mins"])

        entry_rows = ""
        for e in pg["entries"]:
            task = e.get("task_name") or "—"
            desc = e.get("description") or "—"
            start = fmt_time(e.get("start_time", ""))
            end = fmt_time(e.get("end_time", ""))
            dur = fmt_mins(e.get("duration_mins", 0))
            entry_rows += f"""
              <tr>
                <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;font-weight:500;color:#1e1b4b">{task}</td>
                <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;color:#6b7280;font-size:13px;max-width:260px">{desc}</td>
                <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;color:#374151;white-space:nowrap">{start} → {end}</td>
                <td style="padding:10px 14px;border-bottom:1px solid #f0f0f5;font-weight:600;color:#4F46E5;white-space:nowrap;text-align:right">{dur}</td>
              </tr>"""

        project_rows_html += f"""
        <div style="margin-bottom:28px">
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:12px;padding:10px 14px;
                      background:linear-gradient(135deg,{color}18,{color}08);
                      border-left:4px solid {color};border-radius:0 8px 8px 0">
            <span style="font-size:16px;font-weight:700;color:{color}">{proj_name}</span>
            <span style="margin-left:auto;font-size:13px;font-weight:600;color:#6b7280;
                         background:white;padding:3px 10px;border-radius:20px;
                         border:1px solid {color}44">Total: {proj_total}</span>
          </div>
          <table style="width:100%;border-collapse:collapse;background:white;
                        border-radius:8px;overflow:hidden;
                        box-shadow:0 1px 4px rgba(0,0,0,.06)">
            <thead>
              <tr style="background:#f8f9fc">
                <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;
                            color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Task</th>
                <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;
                            color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Description</th>
                <th style="padding:9px 14px;text-align:left;font-size:11px;font-weight:700;
                            color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Time</th>
                <th style="padding:9px 14px;text-align:right;font-size:11px;font-weight:700;
                            color:#9ca3af;text-transform:uppercase;letter-spacing:.06em">Duration</th>
              </tr>
            </thead>
            <tbody>{entry_rows}
            </tbody>
          </table>
        </div>"""

    total_hours = fmt_mins(total_mins)
    pct_of_8h = min(round((total_mins / 480) * 100), 100)
    pct_color = "#10b981" if total_mins >= 480 else "#f59e0b" if total_mins >= 300 else "#ef4444"
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Daily Work Summary – {date_label}</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;-webkit-font-smoothing:antialiased">

  <!-- Email wrapper -->
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f8;padding:32px 16px">
    <tr>
      <td align="center">
        <table width="100%" style="max-width:680px">

          <!-- Header -->
          <tr>
            <td style="background:linear-gradient(135deg,#4F46E5,#7C3AED);
                        border-radius:12px 12px 0 0;padding:32px 36px">
              <table width="100%">
                <tr>
                  <td>
                    <div style="font-size:22px;font-weight:800;color:white;letter-spacing:-.3px">
                      ⏱ ChronoTrack
                    </div>
                    <div style="font-size:13px;color:rgba(255,255,255,.75);margin-top:3px">
                      Daily Work Summary
                    </div>
                  </td>
                  <td align="right">
                    <div style="background:rgba(255,255,255,.15);border-radius:8px;
                                padding:8px 16px;display:inline-block">
                      <div style="font-size:11px;color:rgba(255,255,255,.75);text-transform:uppercase;
                                  letter-spacing:.06em;margin-bottom:2px">Date</div>
                      <div style="font-size:15px;font-weight:700;color:white">{date_label}</div>
                    </div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

          <!-- Employee greeting -->
          <tr>
            <td style="background:white;padding:28px 36px 20px;border-left:1px solid #e5e7eb;
                        border-right:1px solid #e5e7eb">
              <div style="font-size:19px;font-weight:700;color:#111827">
                Hi {employee_name}! 👋
              </div>
              <div style="font-size:14px;color:#6b7280;margin-top:6px">
                Here's your complete work summary for <strong style="color:#374151">{date_label}</strong>.
                You logged <strong style="color:#4F46E5">{total_entries} time {"entry" if total_entries == 1 else "entries"}</strong> today.
              </div>
            </td>
          </tr>

          <!-- Stats strip -->
          <tr>
            <td style="background:white;padding:0 36px 24px;
                        border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
              <table width="100%" style="background:#f8f9fc;border-radius:10px;overflow:hidden">
                <tr>
                  <td style="padding:18px 20px;text-align:center;border-right:1px solid #e5e7eb">
                    <div style="font-size:26px;font-weight:800;color:#4F46E5">{total_hours}</div>
                    <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;
                                letter-spacing:.06em;margin-top:2px">Total Logged</div>
                  </td>
                  <td style="padding:18px 20px;text-align:center;border-right:1px solid #e5e7eb">
                    <div style="font-size:26px;font-weight:800;color:#10b981">{len(project_groups)}</div>
                    <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;
                                letter-spacing:.06em;margin-top:2px">{"Project" if len(project_groups) == 1 else "Projects"}</div>
                  </td>
                  <td style="padding:18px 20px;text-align:center">
                    <div style="font-size:26px;font-weight:800;color:{pct_color}">{pct_of_8h}%</div>
                    <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;
                                letter-spacing:.06em;margin-top:2px">of 8h Target</div>
                  </td>
                </tr>
              </table>
              <!-- Progress bar -->
              <div style="margin-top:12px;background:#e5e7eb;border-radius:100px;height:8px;overflow:hidden">
                <div style="width:{pct_of_8h}%;background:linear-gradient(90deg,{pct_color},{pct_color}cc);
                             height:100%;border-radius:100px;transition:width .3s"></div>
              </div>
              <div style="font-size:11px;color:#9ca3af;margin-top:5px;text-align:right">
                {pct_of_8h}% of daily 8h target completed
              </div>
            </td>
          </tr>

          <!-- Divider -->
          <tr>
            <td style="background:white;padding:0 36px 8px;
                        border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
              <div style="border-top:1px solid #e5e7eb"></div>
              <div style="font-size:13px;font-weight:700;color:#374151;
                          text-transform:uppercase;letter-spacing:.07em;
                          margin-top:20px;margin-bottom:4px">
                📋 Project Breakdown
              </div>
            </td>
          </tr>

          <!-- Project details -->
          <tr>
            <td style="background:white;padding:8px 36px 28px;
                        border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
              {project_rows_html}
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td style="background:#1e1b4b;border-radius:0 0 12px 12px;padding:20px 36px">
              <table width="100%">
                <tr>
                  <td>
                    <div style="font-size:12px;color:rgba(255,255,255,.6);line-height:1.6">
                      This summary was sent automatically by <strong style="color:rgba(255,255,255,.9)">ChronoTrack</strong>.<br>
                      Sent: {now_iso}
                    </div>
                  </td>
                  <td align="right">
                    <div style="font-size:11px;color:rgba(255,255,255,.4)">
                      ⏱ ChronoTrack v2.0
                    </div>
                  </td>
                </tr>
              </table>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>

</body>
</html>"""
    return html


# ── SEND EMAIL ────────────────────────────────────────────────────────────────

class EmailConfigError(Exception):
    """Raised when SMTP credentials are not configured."""
    pass


def send_daily_summary(
    to_addresses: List[str],
    employee_name: str,
    date_label: str,
    project_groups: List[dict],
    total_mins: int,
    total_entries: int,
    cc_addresses: Optional[List[str]] = None,
) -> dict:
    """
    Send the daily summary email.

    Returns {"sent": True, "recipients": [...]} on success.
    Raises EmailConfigError or smtplib.SMTPException on failure.
    """
    if not SMTP_USER or not SMTP_PASSWORD:
        raise EmailConfigError(
            "SMTP credentials not configured. "
            "Set SMTP_USER and SMTP_PASSWORD in your .env file."
        )

    if not to_addresses:
        raise ValueError("At least one recipient address is required.")

    html_body = _build_html(
        employee_name=employee_name,
        date_label=date_label,
        project_groups=project_groups,
        total_mins=total_mins,
        total_entries=total_entries,
        sender_email=SMTP_FROM,
    )

    plain_body = (
        f"Daily Work Summary – {employee_name} – {date_label}\n\n"
        f"Total time logged: {total_mins // 60}h {total_mins % 60}m\n"
        f"Projects: {len(project_groups)}\n\n"
        + "\n".join(
            f"• {pg['project_name']}: "
            + ", ".join(e.get("task_name") or "Task" for e in pg["entries"])
            for pg in project_groups
        )
        + "\n\nSent by ChronoTrack"
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📋 Daily Work Summary – {employee_name} – {date_label}"
    msg["From"]    = f"ChronoTrack <{SMTP_FROM}>"
    msg["To"]      = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)

    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    all_recipients = to_addresses + (cc_addresses or [])

    context = ssl.create_default_context()

    if SMTP_TLS:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=context)
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, all_recipients, msg.as_string())
    else:
        # SSL on port 465
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, all_recipients, msg.as_string())

    return {"sent": True, "recipients": all_recipients}
