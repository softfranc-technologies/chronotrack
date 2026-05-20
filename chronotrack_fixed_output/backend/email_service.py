"""
ChronoTrack – Email Service
Sends via Resend API (HTTPS) when RESEND_API_KEY is set,
falls back to SMTP otherwise.

WHY: Render free-tier blocks outbound SMTP (port 587/465).
     Resend uses port 443 (HTTPS) which is always allowed.
"""

import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────────────────────────


BREVO_API_KEY = os.getenv("BREVO_API_KEY", "").strip()

SMTP_HOST     = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").replace(" ", "")
SMTP_FROM     = os.getenv("SMTP_FROM", SMTP_USER).strip()
SMTP_TIMEOUT  = int(os.getenv("SMTP_TIMEOUT", "15"))
SMTP_TLS      = os.getenv("SMTP_TLS", "true").lower() == "true"
COMPANY_NAME  = os.getenv("COMPANY_NAME", "ChronoTrack")


# ── EXCEPTIONS ────────────────────────────────────────────────────────────────

class EmailConfigError(Exception):
    """Raised when no sending method is configured."""

class EmailSendError(Exception):
    """Raised when delivery fails."""


# ── HTML BUILDER ──────────────────────────────────────────────────────────────

def _build_html(
    employee_name: str,
    date_label: str,
    project_groups: List[dict],
    total_mins: int,
    total_entries: int,
    sender_email: str,
) -> str:
    def fmt_mins(m: int) -> str:
        if m <= 0:
            return "0m"
        h, mn = divmod(m, 60)
        return f"{h}h {mn}m" if h else f"{mn}m"

    def fmt_time(t: str) -> str:
        try:
            h, m = map(int, t.split(":"))
            period = "AM" if h < 12 else "PM"
            h12 = h % 12 or 12
            return f"{h12}:{m:02d} {period}"
        except Exception:
            return t or "—"

    project_rows_html = ""
    for pg in project_groups:
        color = pg.get("color", "#4F46E5")
        proj_name = pg["project_name"]
        proj_total = fmt_mins(pg["project_total_mins"])

        entry_rows = ""
        for e in pg["entries"]:
            task  = e.get("task_name") or "—"
            desc  = e.get("description") or "—"
            start = fmt_time(e.get("start_time", ""))
            end   = fmt_time(e.get("end_time", ""))
            dur   = fmt_mins(e.get("duration_mins", 0))
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
                        border-radius:8px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.06)">
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
    pct_of_8h   = min(round((total_mins / 480) * 100), 100)
    pct_color   = "#10b981" if total_mins >= 480 else "#f59e0b" if total_mins >= 300 else "#ef4444"
    now_iso     = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Daily Work Summary – {date_label}</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f3f4f8;padding:32px 16px">
    <tr><td align="center">
      <table width="100%" style="max-width:680px">
        <tr><td style="background:linear-gradient(135deg,#4F46E5,#7C3AED);border-radius:12px 12px 0 0;padding:32px 36px">
          <table width="100%"><tr>
            <td><div style="font-size:22px;font-weight:800;color:white">⏱ ChronoTrack</div>
                <div style="font-size:13px;color:rgba(255,255,255,.75);margin-top:3px">Daily Work Summary</div></td>
            <td align="right"><div style="background:rgba(255,255,255,.15);border-radius:8px;padding:8px 16px">
              <div style="font-size:11px;color:rgba(255,255,255,.75);text-transform:uppercase;letter-spacing:.06em;margin-bottom:2px">Date</div>
              <div style="font-size:15px;font-weight:700;color:white">{date_label}</div>
            </div></td>
          </tr></table>
        </td></tr>
        <tr><td style="background:white;padding:28px 36px 20px;border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
          <div style="font-size:19px;font-weight:700;color:#111827">Hi {employee_name}! 👋</div>
          <div style="font-size:14px;color:#6b7280;margin-top:6px">
            Here's your complete work summary for <strong style="color:#374151">{date_label}</strong>.
            You logged <strong style="color:#4F46E5">{total_entries} time {"entry" if total_entries == 1 else "entries"}</strong> today.
          </div>
        </td></tr>
        <tr><td style="background:white;padding:0 36px 24px;border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
          <table width="100%" style="background:#f8f9fc;border-radius:10px;overflow:hidden"><tr>
            <td style="padding:18px 20px;text-align:center;border-right:1px solid #e5e7eb">
              <div style="font-size:26px;font-weight:800;color:#4F46E5">{total_hours}</div>
              <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-top:2px">Total Logged</div>
            </td>
            <td style="padding:18px 20px;text-align:center;border-right:1px solid #e5e7eb">
              <div style="font-size:26px;font-weight:800;color:#10b981">{len(project_groups)}</div>
              <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-top:2px">{"Project" if len(project_groups) == 1 else "Projects"}</div>
            </td>
            <td style="padding:18px 20px;text-align:center">
              <div style="font-size:26px;font-weight:800;color:{pct_color}">{pct_of_8h}%</div>
              <div style="font-size:11px;color:#9ca3af;text-transform:uppercase;letter-spacing:.06em;margin-top:2px">of 8h Target</div>
            </td>
          </tr></table>
          <div style="margin-top:12px;background:#e5e7eb;border-radius:100px;height:8px;overflow:hidden">
            <div style="width:{pct_of_8h}%;background:linear-gradient(90deg,{pct_color},{pct_color}cc);height:100%;border-radius:100px"></div>
          </div>
          <div style="font-size:11px;color:#9ca3af;margin-top:5px;text-align:right">{pct_of_8h}% of daily 8h target completed</div>
        </td></tr>
        <tr><td style="background:white;padding:0 36px 8px;border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
          <div style="border-top:1px solid #e5e7eb"></div>
          <div style="font-size:13px;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:.07em;margin-top:20px;margin-bottom:4px">📋 Project Breakdown</div>
        </td></tr>
        <tr><td style="background:white;padding:8px 36px 28px;border-left:1px solid #e5e7eb;border-right:1px solid #e5e7eb">
          {project_rows_html}
        </td></tr>
        <tr><td style="background:#1e1b4b;border-radius:0 0 12px 12px;padding:20px 36px">
          <table width="100%"><tr>
            <td><div style="font-size:12px;color:rgba(255,255,255,.6);line-height:1.6">
              This summary was sent automatically by <strong style="color:rgba(255,255,255,.9)">ChronoTrack</strong>.<br>Sent: {now_iso}
            </div></td>
            <td align="right"><div style="font-size:11px;color:rgba(255,255,255,.4)">⏱ ChronoTrack v2.0</div></td>
          </tr></table>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ── SEND VIA RESEND (HTTPS) ───────────────────────────────────────────────────

def _send_via_brevo(
    from_addr: str,
    to_addresses: List[str],
    cc_addresses: List[str],
    subject: str,
    html_body: str,
    plain_body: str,
) -> None:
    import sib_api_v3_sdk
    from sib_api_v3_sdk.rest import ApiException

    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = BREVO_API_KEY

    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
        sib_api_v3_sdk.ApiClient(configuration)
    )

    to_list = [{"email": e} for e in to_addresses]
    cc_list = [{"email": e} for e in cc_addresses] if cc_addresses else []

    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
        to=to_list,
        cc=cc_list or None,
        sender={"email": SMTP_FROM, "name": "ChronoTrack"},
        subject=subject,
        html_content=html_body,
        text_content=plain_body,
    )

    try:
        api_instance.send_transac_email(send_smtp_email)
    except ApiException as exc:
        raise EmailSendError(f"Brevo API error: {exc}") from exc

# ── SEND VIA SMTP ─────────────────────────────────────────────────────────────

def _send_via_smtp(
    from_addr: str,
    to_addresses: List[str],
    cc_addresses: List[str],
    subject: str,
    html_body: str,
    plain_body: str,
) -> None:
    """Send via SMTP (works locally; may be blocked on Render free tier)."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = ", ".join(to_addresses)
    if cc_addresses:
        msg["Cc"] = ", ".join(cc_addresses)

    msg.attach(MIMEText(plain_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body,  "html",  "utf-8"))

    all_recipients = to_addresses + cc_addresses
    context = ssl.create_default_context()

    try:
        if SMTP_TLS:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT) as s:
                s.ehlo(); s.starttls(context=context); s.ehlo()
                s.login(SMTP_USER, SMTP_PASSWORD)
                s.sendmail(SMTP_FROM, all_recipients, msg.as_string())
        else:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=SMTP_TIMEOUT) as s:
                s.login(SMTP_USER, SMTP_PASSWORD)
                s.sendmail(SMTP_FROM, all_recipients, msg.as_string())
    except smtplib.SMTPAuthenticationError as exc:
        raise EmailSendError(
            "Gmail authentication failed. Use an App Password (not your account password). "
            f"Detail: {exc}"
        ) from exc
    except (smtplib.SMTPConnectError, OSError) as exc:
        raise EmailSendError(
            f"Cannot connect to {SMTP_HOST}:{SMTP_PORT}. "
            "On Render free tier, outbound SMTP is blocked — set RESEND_API_KEY instead. "
            f"Detail: {exc}"
        ) from exc
    except TimeoutError as exc:
        raise EmailSendError(f"SMTP timed out after {SMTP_TIMEOUT}s.") from exc
    except smtplib.SMTPException as exc:
        raise EmailSendError(f"SMTP error: {exc}") from exc


# ── PUBLIC API ────────────────────────────────────────────────────────────────

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
    Prefers Resend API (HTTPS) when RESEND_API_KEY is set.
    Falls back to SMTP otherwise.
    """
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

    subject    = f"📋 Daily Work Summary – {employee_name} – {date_label}"
    from_addr  = f"ChronoTrack <{SMTP_FROM}>"
    cc_list    = cc_addresses or []

    if BREVO_API_KEY:
        # ✅ Use Resend HTTPS API — works on Render free tier
        _send_via_brevo(from_addr, to_addresses, cc_list, subject, html_body, plain_body)
    elif SMTP_USER and SMTP_PASSWORD:
        # Fallback to SMTP (for local dev)
        _send_via_smtp(from_addr, to_addresses, cc_list, subject, html_body, plain_body)
    else:
        raise EmailConfigError(
            "No email provider configured. "
            "Set RESEND_API_KEY (recommended for Render) or SMTP_USER + SMTP_PASSWORD in your .env file."
        )

    all_recipients = to_addresses + cc_list
    return {"sent": True, "recipients": all_recipients}


def send_login_notification(
    user_name: str,
    user_email: str,
    login_date: str,
    login_time: str,
    browser: str = "Unknown",
    ip_address: str = "Unknown",
    location: str = "—",
) -> dict:
    """Send login activity notification. Never raises — failures are logged silently."""
    import logging
    log = logging.getLogger("chronotrack.email")

    has_resend = bool(RESEND_API_KEY)
    has_smtp   = bool(SMTP_USER and SMTP_PASSWORD)

    if not has_resend and not has_smtp:
        log.warning("Login notification skipped — no email provider configured.")
        return {"sent": False, "error": "Email not configured"}

    rows_html = "".join(f"""
        <tr>
          <td style="padding:10px 14px;font-size:13px;font-weight:600;color:#374151;
                     background:#f8f9fc;border-bottom:1px solid #e5e7eb;width:38%">{label}</td>
          <td style="padding:10px 14px;font-size:13px;color:#1f2937;
                     border-bottom:1px solid #e5e7eb">{value or "—"}</td>
        </tr>"""
        for label, value in [
            ("👤 User Name", user_name),
            ("📧 Email", user_email),
            ("📅 Login Date", login_date),
            ("🕐 Login Time", login_time),
            ("🖥 Device / Browser", browser[:100]),
            ("🌐 IP Address", ip_address),
            ("📍 Location", location),
        ]
    )

    html_body = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f3f4f8;font-family:sans-serif">
<table width="100%" cellpadding="0" cellspacing="0" style="padding:32px 16px">
  <tr><td align="center">
    <table width="100%" style="max-width:560px">
      <tr><td style="background:linear-gradient(135deg,#0f172a,#1e293b);border-radius:12px 12px 0 0;padding:24px 32px">
        <div style="font-size:20px;font-weight:800;color:#fff">🔐 {COMPANY_NAME}</div>
        <div style="font-size:12px;color:rgba(255,255,255,.6);margin-top:2px">Login Activity Notification</div>
      </td></tr>
      <tr><td style="background:#fff;padding:24px 32px;border:1px solid #e5e7eb;border-top:none">
        <div style="font-size:16px;font-weight:700;color:#111827;margin-bottom:8px">New sign-in detected</div>
        <div style="font-size:13px;color:#6b7280;margin-bottom:20px">
          Hi <strong>{user_name}</strong>, a new login was detected on your {COMPANY_NAME} account.
          If this wasn't you, change your password immediately.
        </div>
        <table width="100%" style="border-collapse:collapse;border:1px solid #e5e7eb;border-radius:8px;overflow:hidden">
          {rows_html}
        </table>
      </td></tr>
      <tr><td style="background:#fffbeb;padding:12px 32px;border:1px solid #fcd34d">
        <div style="font-size:12px;color:#92400e">
          ⚠️ <strong>Security:</strong> {COMPANY_NAME} will never ask for your password via email.
        </div>
      </td></tr>
      <tr><td style="background:#1e1b4b;border-radius:0 0 12px 12px;padding:14px 32px">
        <div style="font-size:11px;color:rgba(255,255,255,.5)">
          Sent automatically by {COMPANY_NAME} · {datetime.utcnow().strftime("%d %b %Y %H:%M UTC")}
        </div>
      </td></tr>
    </table>
  </td></tr>
</table>
</body></html>"""

    plain_body = (
        f"New login to {COMPANY_NAME}\n\n"
        f"User: {user_name} ({user_email})\nDate: {login_date}\nTime: {login_time}\n"
        f"Browser: {browser}\nIP: {ip_address}\n\n"
        "If this wasn't you, change your password immediately."
    )

    subject   = f"🔐 New Login Detected – {COMPANY_NAME}"
    from_addr = f"{COMPANY_NAME} Security <{SMTP_FROM}>"

    try:
        if has_resend:
            _send_via_resend(from_addr, [user_email], [], subject, html_body, plain_body)
        else:
            _send_via_smtp(from_addr, [user_email], [], subject, html_body, plain_body)
        log.info("Login notification sent to %s", user_email)
        return {"sent": True, "recipient": user_email}
    except Exception as exc:
        log.error("Login notification failed for %s: %s", user_email, exc)
        return {"sent": False, "error": str(exc)}
