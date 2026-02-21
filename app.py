#!/usr/bin/env python3
import csv
import base64
import hashlib
import json
import mimetypes
import os
import secrets
import smtplib
import sqlite3
import time
from datetime import datetime, timedelta
from email.message import EmailMessage
from http import cookies
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urlencode, urlparse
from urllib.request import Request, urlopen

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "crm.db")
STATIC_DIR = os.path.join(BASE_DIR, "static")
SESSION_AGE_SECONDS = 60 * 60 * 24 * 7
APP_SALT = os.environ.get("CRM_APP_SALT", "crm-salt-change-me")
SMS_PROVIDER_DEFAULT = "textbelt"
TWILIO_ACCOUNT_SID_DEFAULT = ""
TWILIO_AUTH_TOKEN_DEFAULT = ""
TWILIO_FROM_NUMBER_DEFAULT = ""
TEXTBELT_KEY_DEFAULT = "textbelt"
TEXTBELT_ENDPOINT = "https://textbelt.com/text"
DEFAULT_CONFIRM_EXPIRY_MINUTES = 60
MAX_CONFIRM_EXPIRY_MINUTES = 60 * 24 * 7
CHANGE_ACTION_LABELS = {
    "remove_vehicle": "Remove vehicle",
    "add_vehicle": "Add vehicle",
    "remove_driver": "Remove driver",
    "add_driver": "Add driver",
    "remove_coverage": "Remove coverage",
    "add_coverage": "Add coverage",
    "other": "Other change",
}


def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def hash_passcode(passcode: str) -> str:
    value = f"{APP_SALT}:{passcode}".encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def setting(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def upsert_setting(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        """
        INSERT INTO settings(key, value) VALUES(?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, value),
    )


def now_utc_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def normalize_phone(value: str) -> str:
    raw = str(value or "").strip()
    digits = "".join(ch for ch in raw if ch.isdigit())
    if not digits:
        return ""
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    if raw.startswith("+"):
        return f"+{digits}"
    return f"+{digits}"


def phone_matches(left: str, right: str) -> bool:
    a = normalize_phone(left)
    b = normalize_phone(right)
    if not a or not b:
        return False
    if a == b:
        return True
    return a[-10:] == b[-10:]


def send_sms_via_twilio(to_phone: str, message: str):
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", TWILIO_ACCOUNT_SID_DEFAULT).strip()
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", TWILIO_AUTH_TOKEN_DEFAULT).strip()
    from_number = os.environ.get("TWILIO_FROM_NUMBER", TWILIO_FROM_NUMBER_DEFAULT).strip()
    if not account_sid or not auth_token or not from_number:
        return False, "Twilio SMS is not configured"

    endpoint = f"https://api.twilio.com/2010-04-01/Accounts/{quote(account_sid)}/Messages.json"
    payload = urlencode({"To": to_phone, "From": from_number, "Body": message}).encode("utf-8")
    req = Request(endpoint, data=payload, method="POST")
    auth = base64.b64encode(f"{account_sid}:{auth_token}".encode("utf-8")).decode("ascii")
    req.add_header("Authorization", f"Basic {auth}")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urlopen(req, timeout=12) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            sid = ""
            try:
                payload = json.loads(raw) if raw else {}
                sid = str(payload.get("sid", "")).strip()
            except json.JSONDecodeError:
                sid = ""
            return {"sent": True, "status": "sent", "error": "", "provider": "twilio", "message_id": sid}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        detail = f"Twilio HTTP {exc.code}"
        if body:
            detail = f"{detail}: {body}"
        return {"sent": False, "status": "failed", "error": detail, "provider": "twilio", "message_id": ""}
    except Exception as exc:
        return {"sent": False, "status": "failed", "error": str(exc), "provider": "twilio", "message_id": ""}


def send_sms_via_textbelt(
    to_phone: str,
    message: str,
    api_key: str,
    reply_webhook_url: str = "",
    webhook_data: str = "",
):
    if not api_key:
        return {"sent": False, "status": "failed", "error": "Textbelt key is not configured", "provider": "textbelt", "message_id": ""}

    payload = {
        "phone": to_phone,
        "message": message,
        "key": api_key,
    }
    # Free key supports outbound only; reply webhooks work with paid keys.
    if reply_webhook_url and api_key.lower() != "textbelt":
        payload["replyWebhookUrl"] = reply_webhook_url
    if webhook_data:
        payload["webhookData"] = webhook_data[:100]

    req = Request(TEXTBELT_ENDPOINT, data=urlencode(payload).encode("utf-8"), method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with urlopen(req, timeout=12) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(raw) if raw else {}
            success = bool(data.get("success"))
            msg_id = str(data.get("textId", "")).strip()
            if success:
                return {"sent": True, "status": "sent", "error": "", "provider": "textbelt", "message_id": msg_id}
            error = str(data.get("error", "Textbelt send failed")).strip()
            return {"sent": False, "status": "failed", "error": error, "provider": "textbelt", "message_id": msg_id}
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="ignore")
        detail = f"Textbelt HTTP {exc.code}"
        if body:
            detail = f"{detail}: {body}"
        return {"sent": False, "status": "failed", "error": detail, "provider": "textbelt", "message_id": ""}
    except Exception as exc:
        return {"sent": False, "status": "failed", "error": str(exc), "provider": "textbelt", "message_id": ""}


def send_email_via_smtp(to_email: str, subject: str, body: str):
    smtp_host = os.environ.get("SMTP_HOST", "").strip()
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_username = os.environ.get("SMTP_USERNAME", "").strip()
    smtp_password = os.environ.get("SMTP_PASSWORD", "").strip()
    smtp_from = os.environ.get("SMTP_FROM", "").strip()
    smtp_use_tls = os.environ.get("SMTP_USE_TLS", "1").strip() != "0"
    if not smtp_host or not smtp_from:
        return False, "SMTP email is not configured"

    msg = EmailMessage()
    msg["From"] = smtp_from
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=12) as smtp:
            if smtp_use_tls:
                smtp.starttls()
            if smtp_username:
                smtp.login(smtp_username, smtp_password)
            smtp.send_message(msg)
            return True, ""
    except Exception as exc:
        return False, str(exc)


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = db_conn()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            role TEXT NOT NULL CHECK(role IN ('owner', 'agent')),
            passcode_hash TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions(
            token TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS sales(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            salesperson_id INTEGER NOT NULL,
            customer_name TEXT NOT NULL,
            phone TEXT,
            address TEXT,
            date_sold TEXT NOT NULL,
            policy_type TEXT,
            carrier TEXT,
            premium_amount REAL NOT NULL,
            agent_commission_rate REAL NOT NULL,
            agency_commission_rate REAL NOT NULL,
            notes TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(salesperson_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS change_confirmations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT UNIQUE NOT NULL,
            customer_name TEXT NOT NULL,
            customer_phone TEXT,
            customer_email TEXT,
            policy_label TEXT,
            actions_json TEXT NOT NULL,
            summary_text TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('pending', 'confirmed', 'declined', 'expired')),
            channel TEXT NOT NULL CHECK(channel IN ('sms', 'email', 'manual')),
            message_text TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            last_sent_at TEXT,
            confirmed_at TEXT,
            declined_at TEXT,
            decision_note TEXT,
            signature_name TEXT,
            ip_address TEXT,
            user_agent TEXT,
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE INDEX IF NOT EXISTS idx_sales_month ON sales(date_sold);
        CREATE INDEX IF NOT EXISTS idx_sales_owner ON sales(salesperson_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_expiry ON sessions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_change_confirmations_status
            ON change_confirmations(status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_change_confirmations_token
            ON change_confirmations(token);
        """
    )

    now = datetime.utcnow().isoformat(timespec="seconds")
    defaults = [
        ("owner", "Owner", "owner", "owner123!"),
        ("sales1", "Salesman 1", "agent", "agent123!"),
        ("sales2", "Salesman 2", "agent", "agent123!"),
        ("sales3", "Salesman 3", "agent", "agent123!"),
        ("sales4", "Salesman 4", "agent", "agent123!"),
    ]

    for username, display_name, role, passcode in defaults:
        conn.execute(
            """
            INSERT INTO users(username, display_name, role, passcode_hash, created_at)
            VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(username) DO NOTHING
            """,
            (username, display_name, role, hash_passcode(passcode), now),
        )

    upsert_setting(conn, "competition_mode", "1")
    upsert_setting(conn, "default_agent_commission_rate", "10")
    upsert_setting(conn, "default_agency_commission_rate", "18")
    conn.commit()
    conn.close()


class CRMHandler(BaseHTTPRequestHandler):
    server_version = "CRMTool/1.0"

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/confirm" or path == "/confirm/" or path.startswith("/confirm/"):
            return self.serve_static("/confirm.html")

        if path.startswith("/api/"):
            if path == "/api/health":
                return self.send_json(200, {"ok": True})
            if path == "/api/me":
                user = self.require_user()
                if not user:
                    return
                return self.send_json(200, {"user": user})
            if path == "/api/sales":
                user = self.require_user()
                if not user:
                    return
                return self.get_sales(user, query)
            if path == "/api/leaderboard":
                user = self.require_user()
                if not user:
                    return
                return self.get_leaderboard(user, query)
            if path == "/api/metrics":
                user = self.require_user(owner_only=True)
                if not user:
                    return
                return self.get_metrics(query)
            if path == "/api/settings":
                user = self.require_user()
                if not user:
                    return
                return self.get_settings(user)
            if path == "/api/export":
                user = self.require_user(owner_only=True)
                if not user:
                    return
                return self.export_sales(query)
            if path == "/api/change-confirmations":
                user = self.require_user()
                if not user:
                    return
                return self.get_change_confirmations(user, query)
            if path.startswith("/api/confirm/"):
                token = path.rsplit("/", 1)[-1].strip()
                return self.get_public_confirmation(token)
            return self.send_json(404, {"error": "Not found"})

        return self.serve_static(path)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/api/login":
            return self.post_login()
        if path == "/api/logout":
            return self.post_logout()
        if path == "/api/sms/inbound":
            return self.post_sms_inbound(query)
        if path == "/api/sales":
            user = self.require_user()
            if not user:
                return
            return self.post_sales(user)
        if path == "/api/upload":
            user = self.require_user(owner_only=True)
            if not user:
                return
            return self.post_upload()
        if path == "/api/settings":
            user = self.require_user(owner_only=True)
            if not user:
                return
            return self.post_settings()
        if path == "/api/change-confirmations":
            user = self.require_user()
            if not user:
                return
            return self.post_change_confirmation(user)
        if path.startswith("/api/change-confirmations/") and path.endswith("/resend"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 4:
                return self.send_json(404, {"error": "Not found"})
            try:
                confirmation_id = int(parts[2])
            except ValueError:
                return self.send_json(400, {"error": "Invalid confirmation id"})
            return self.post_resend_change_confirmation(user, confirmation_id)
        if path.startswith("/api/confirm/"):
            token = path.rsplit("/", 1)[-1].strip()
            return self.post_public_confirmation(token)

        return self.send_json(404, {"error": "Not found"})

    def log_message(self, fmt, *args):
        return

    def read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def read_form(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            parsed = parse_qs(raw.decode("utf-8"), keep_blank_values=True)
        except UnicodeDecodeError:
            return None
        out = {}
        for key, values in parsed.items():
            out[key] = values[0] if values else ""
        return out

    def send_json(self, code, payload, extra_headers=None):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, code, text, content_type="text/plain; charset=utf-8", headers=None):
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        if headers:
            for key, value in headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def user_from_session(self):
        raw_cookie = self.headers.get("Cookie")
        if not raw_cookie:
            return None
        jar = cookies.SimpleCookie()
        jar.load(raw_cookie)
        if "session" not in jar:
            return None
        token = jar["session"].value
        now = int(time.time())

        conn = db_conn()
        row = conn.execute(
            """
            SELECT u.id, u.username, u.display_name, u.role
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ? AND s.expires_at > ?
            """,
            (token, now),
        ).fetchone()
        conn.close()

        if not row:
            return None
        return dict(row)

    def require_user(self, owner_only=False):
        user = self.user_from_session()
        if not user:
            self.send_json(401, {"error": "Unauthorized"})
            return None
        if owner_only and user["role"] != "owner":
            self.send_json(403, {"error": "Owner only"})
            return None
        return user

    def parse_month(self, query):
        month = query.get("month", [""])[0]
        if not month:
            return datetime.utcnow().strftime("%Y-%m")
        try:
            datetime.strptime(month, "%Y-%m")
            return month
        except ValueError:
            return datetime.utcnow().strftime("%Y-%m")

    def build_base_url(self):
        configured = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
        if configured:
            return configured
        proto = self.headers.get("X-Forwarded-Proto", "").split(",")[0].strip()
        if not proto:
            proto = "http"
        host = self.headers.get("X-Forwarded-Host", "").split(",")[0].strip()
        if not host:
            host = self.headers.get("Host", "").split(",")[0].strip()
        if not host:
            host = "localhost:8080"
        return f"{proto}://{host}"

    def parse_change_actions(self, raw_actions):
        if not isinstance(raw_actions, list) or not raw_actions:
            raise ValueError("At least one change action is required")

        actions = []
        for item in raw_actions:
            if not isinstance(item, dict):
                raise ValueError("Each action must be an object")

            action_type = str(item.get("type", "")).strip().lower()
            target = str(item.get("target", "")).strip()
            detail = str(item.get("detail", "")).strip()
            free_text = str(item.get("text", "")).strip()

            if free_text and not target:
                target = free_text

            if not target and not detail:
                raise ValueError("Each action needs a target or detail")

            if action_type not in CHANGE_ACTION_LABELS:
                action_type = "other"

            actions.append(
                {
                    "type": action_type,
                    "label": CHANGE_ACTION_LABELS[action_type],
                    "target": target,
                    "detail": detail,
                }
            )

        return actions

    def build_change_summary(self, actions):
        parts = []
        for action in actions:
            main = action.get("target") or action.get("detail") or ""
            if main:
                parts.append(f'{action.get("label", "Change")}: {main}')
        return "; ".join(parts)

    def build_confirmation_message(
        self,
        customer_name,
        policy_label,
        summary_text,
        confirm_url,
        expires_at,
        channel,
    ):
        policy_text = f"Policy {policy_label}" if policy_label else "your policy"
        expires_text = expires_at.replace("T", " ")
        if channel == "sms":
            return (
                f"{customer_name}, confirm your insurance changes for {policy_text}: {summary_text}. "
                "Reply YES to confirm or NO to decline. "
                f"Request expires {expires_text} UTC."
            )
        return (
            f"Please confirm your requested insurance change for {customer_name} ({policy_text}). "
            f"{summary_text}. Confirm or decline here: {confirm_url}. "
            f"Link expires {expires_text} UTC."
        )

    def expire_pending_confirmations(self, conn):
        now = now_utc_iso()
        result = conn.execute(
            """
            UPDATE change_confirmations
            SET status = 'expired'
            WHERE status = 'pending' AND expires_at < ?
            """,
            (now,),
        )
        return result.rowcount

    def confirmation_delivery(self, channel, phone, email, message_text, webhook_data=""):
        if channel == "manual":
            return {"sent": False, "status": "manual", "error": ""}

        if channel == "sms":
            if not phone:
                return {"sent": False, "status": "failed", "error": "Customer phone is required for SMS"}
            provider = os.environ.get("SMS_PROVIDER", SMS_PROVIDER_DEFAULT).strip().lower()
            if not provider:
                provider = SMS_PROVIDER_DEFAULT

            if provider == "textbelt":
                key = os.environ.get("TEXTBELT_KEY", TEXTBELT_KEY_DEFAULT).strip()
                webhook_url = f"{self.build_base_url()}/api/sms/inbound?provider=textbelt"
                secret = os.environ.get("SMS_INBOUND_SECRET", "").strip()
                if secret:
                    webhook_url = f"{webhook_url}&secret={quote(secret)}"
                return send_sms_via_textbelt(
                    phone,
                    message_text,
                    key,
                    reply_webhook_url=webhook_url,
                    webhook_data=str(webhook_data or ""),
                )

            if provider == "twilio":
                return send_sms_via_twilio(phone, message_text)

            return {
                "sent": False,
                "status": "failed",
                "error": f"Unsupported SMS_PROVIDER: {provider}",
                "provider": provider,
                "message_id": "",
            }

        if channel == "email":
            if not email:
                return {"sent": False, "status": "failed", "error": "Customer email is required for email"}
            sent, error = send_email_via_smtp(
                email,
                "Please confirm your policy change request",
                message_text,
            )
            return {"sent": sent, "status": "sent" if sent else "failed", "error": error}

        return {"sent": False, "status": "failed", "error": "Unsupported channel"}

    def serialize_confirmation(self, row):
        try:
            actions = json.loads(row["actions_json"])
            if not isinstance(actions, list):
                actions = []
        except json.JSONDecodeError:
            actions = []

        return {
            "id": row["id"],
            "token": row["token"],
            "customer_name": row["customer_name"],
            "customer_phone": row["customer_phone"] or "",
            "customer_email": row["customer_email"] or "",
            "policy_label": row["policy_label"] or "",
            "actions": actions,
            "summary_text": row["summary_text"],
            "status": row["status"],
            "channel": row["channel"],
            "message_text": row["message_text"],
            "created_by": row["created_by"],
            "created_by_name": row["created_by_name"] if "created_by_name" in row.keys() else "",
            "created_at": row["created_at"],
            "expires_at": row["expires_at"],
            "last_sent_at": row["last_sent_at"],
            "confirmed_at": row["confirmed_at"],
            "declined_at": row["declined_at"],
            "decision_note": row["decision_note"] or "",
            "signature_name": row["signature_name"] or "",
        }

    def post_login(self):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        username = str(data.get("username", "")).strip()
        passcode = str(data.get("passcode", "")).strip()
        if not username or not passcode:
            return self.send_json(400, {"error": "Username and passcode required"})

        conn = db_conn()
        user = conn.execute(
            "SELECT id, username, display_name, role, passcode_hash FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if not user or user["passcode_hash"] != hash_passcode(passcode):
            conn.close()
            return self.send_json(401, {"error": "Invalid credentials"})

        token = secrets.token_urlsafe(32)
        now = int(time.time())
        expires = now + SESSION_AGE_SECONDS
        conn.execute(
            "INSERT INTO sessions(token, user_id, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (token, user["id"], expires, now),
        )
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (now,))
        conn.commit()
        conn.close()

        cookie_header = (
            f"session={token}; Path=/; Max-Age={SESSION_AGE_SECONDS}; "
            "HttpOnly; SameSite=Lax"
        )
        return self.send_json(
            200,
            {
                "ok": True,
                "user": {
                    "id": user["id"],
                    "username": user["username"],
                    "display_name": user["display_name"],
                    "role": user["role"],
                },
            },
            extra_headers={"Set-Cookie": cookie_header},
        )

    def post_logout(self):
        raw_cookie = self.headers.get("Cookie")
        token = None
        if raw_cookie:
            jar = cookies.SimpleCookie()
            jar.load(raw_cookie)
            if "session" in jar:
                token = jar["session"].value

        if token:
            conn = db_conn()
            conn.execute("DELETE FROM sessions WHERE token = ?", (token,))
            conn.commit()
            conn.close()

        return self.send_json(
            200,
            {"ok": True},
            extra_headers={"Set-Cookie": "session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax"},
        )

    def post_sales(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        customer_name = str(data.get("customer_name", "")).strip()
        phone = str(data.get("phone", "")).strip()
        address = str(data.get("address", "")).strip()
        date_sold = str(data.get("date_sold", "")).strip()
        policy_type = str(data.get("policy_type", "")).strip()
        carrier = str(data.get("carrier", "")).strip()
        notes = str(data.get("notes", "")).strip()

        if not customer_name:
            return self.send_json(400, {"error": "Customer name is required"})

        try:
            datetime.strptime(date_sold, "%Y-%m-%d")
        except ValueError:
            return self.send_json(400, {"error": "date_sold must be YYYY-MM-DD"})

        try:
            premium = float(data.get("premium_amount", 0))
            if premium <= 0:
                raise ValueError
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "premium_amount must be greater than 0"})

        conn = db_conn()
        default_agent_rate = float(setting(conn, "default_agent_commission_rate", "10"))
        default_agency_rate = float(setting(conn, "default_agency_commission_rate", "18"))

        try:
            agent_rate = float(data.get("agent_commission_rate", default_agent_rate))
            agency_rate = float(data.get("agency_commission_rate", default_agency_rate))
        except (TypeError, ValueError):
            conn.close()
            return self.send_json(400, {"error": "Commission rates must be numeric"})

        if agent_rate < 0 or agency_rate < 0:
            conn.close()
            return self.send_json(400, {"error": "Commission rates cannot be negative"})

        salesperson_id = user["id"]
        if user["role"] == "owner":
            requested_id = data.get("salesperson_id")
            if requested_id is not None:
                check = conn.execute(
                    "SELECT id FROM users WHERE id = ? AND role = 'agent'", (requested_id,)
                ).fetchone()
                if check:
                    salesperson_id = check["id"]

        now = datetime.utcnow().isoformat(timespec="seconds")
        conn.execute(
            """
            INSERT INTO sales(
                salesperson_id, customer_name, phone, address, date_sold,
                policy_type, carrier, premium_amount,
                agent_commission_rate, agency_commission_rate, notes, created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                salesperson_id,
                customer_name,
                phone,
                address,
                date_sold,
                policy_type,
                carrier,
                premium,
                agent_rate,
                agency_rate,
                notes,
                now,
            ),
        )
        conn.commit()
        conn.close()

        return self.send_json(201, {"ok": True})

    def get_sales(self, user, query):
        month = self.parse_month(query)
        conn = db_conn()
        competition_mode = setting(conn, "competition_mode", "1") == "1"

        where = ["substr(s.date_sold, 1, 7) = ?"]
        params = [month]
        if user["role"] != "owner" and not competition_mode:
            where.append("s.salesperson_id = ?")
            params.append(user["id"])

        rows = conn.execute(
            f"""
            SELECT
                s.id,
                s.date_sold,
                s.customer_name,
                s.phone,
                s.address,
                s.policy_type,
                s.carrier,
                s.premium_amount,
                s.agent_commission_rate,
                s.agency_commission_rate,
                s.notes,
                u.display_name AS salesperson
            FROM sales s
            JOIN users u ON u.id = s.salesperson_id
            WHERE {' AND '.join(where)}
            ORDER BY s.date_sold DESC, s.id DESC
            """,
            params,
        ).fetchall()
        conn.close()

        out = []
        for row in rows:
            out.append(
                {
                    "id": row["id"],
                    "date_sold": row["date_sold"],
                    "customer_name": row["customer_name"],
                    "phone": row["phone"],
                    "address": row["address"],
                    "policy_type": row["policy_type"],
                    "carrier": row["carrier"],
                    "premium_amount": row["premium_amount"],
                    "agent_commission_rate": row["agent_commission_rate"],
                    "agency_commission_rate": row["agency_commission_rate"],
                    "agent_commission_amount": round(
                        row["premium_amount"] * row["agent_commission_rate"] / 100.0, 2
                    ),
                    "agency_commission_amount": round(
                        row["premium_amount"] * row["agency_commission_rate"] / 100.0, 2
                    ),
                    "notes": row["notes"],
                    "salesperson": row["salesperson"],
                }
            )

        return self.send_json(200, {"month": month, "sales": out})

    def get_leaderboard(self, user, query):
        month = self.parse_month(query)
        conn = db_conn()
        competition_mode = setting(conn, "competition_mode", "1") == "1"

        where = ["substr(s.date_sold, 1, 7) = ?"]
        params = [month]
        if user["role"] != "owner" and not competition_mode:
            where.append("u.id = ?")
            params.append(user["id"])

        rows = conn.execute(
            f"""
            SELECT
                u.id,
                u.display_name,
                COUNT(s.id) AS deals,
                COALESCE(SUM(s.premium_amount), 0) AS premium_total,
                COALESCE(SUM(s.premium_amount * s.agent_commission_rate / 100.0), 0) AS agent_commission_total,
                COALESCE(SUM(s.premium_amount * s.agency_commission_rate / 100.0), 0) AS agency_commission_total
            FROM users u
            LEFT JOIN sales s
                ON s.salesperson_id = u.id
                AND substr(s.date_sold, 1, 7) = ?
            WHERE u.role = 'agent'
            """
            + (" AND u.id = ?" if (user["role"] != "owner" and not competition_mode) else "")
            + " GROUP BY u.id, u.display_name ORDER BY premium_total DESC, deals DESC, u.display_name ASC",
            params,
        ).fetchall()
        conn.close()

        data = [
            {
                "id": row["id"],
                "display_name": row["display_name"],
                "deals": int(row["deals"]),
                "premium_total": round(float(row["premium_total"]), 2),
                "agent_commission_total": round(float(row["agent_commission_total"]), 2),
                "agency_commission_total": round(float(row["agency_commission_total"]), 2),
            }
            for row in rows
        ]

        return self.send_json(
            200,
            {
                "month": month,
                "competition_mode": competition_mode,
                "leaderboard": data,
            },
        )

    def get_metrics(self, query):
        month = self.parse_month(query)
        conn = db_conn()

        top = conn.execute(
            """
            SELECT
                COUNT(*) AS deals,
                COALESCE(SUM(premium_amount), 0) AS premium_total,
                COALESCE(SUM(premium_amount * agent_commission_rate / 100.0), 0) AS agent_commission_total,
                COALESCE(SUM(premium_amount * agency_commission_rate / 100.0), 0) AS agency_commission_total
            FROM sales
            WHERE substr(date_sold, 1, 7) = ?
            """,
            (month,),
        ).fetchone()

        by_agent = conn.execute(
            """
            SELECT
                u.display_name,
                COUNT(s.id) AS deals,
                COALESCE(SUM(s.premium_amount), 0) AS premium_total,
                COALESCE(SUM(s.premium_amount * s.agent_commission_rate / 100.0), 0) AS agent_commission_total,
                COALESCE(SUM(s.premium_amount * s.agency_commission_rate / 100.0), 0) AS agency_commission_total
            FROM users u
            LEFT JOIN sales s
                ON s.salesperson_id = u.id
                AND substr(s.date_sold, 1, 7) = ?
            WHERE u.role = 'agent'
            GROUP BY u.id, u.display_name
            ORDER BY premium_total DESC, deals DESC
            """,
            (month,),
        ).fetchall()
        conn.close()

        payload = {
            "month": month,
            "summary": {
                "deals": int(top["deals"]),
                "premium_total": round(float(top["premium_total"]), 2),
                "agent_commission_total": round(float(top["agent_commission_total"]), 2),
                "agency_commission_total": round(float(top["agency_commission_total"]), 2),
            },
            "by_agent": [
                {
                    "display_name": row["display_name"],
                    "deals": int(row["deals"]),
                    "premium_total": round(float(row["premium_total"]), 2),
                    "agent_commission_total": round(float(row["agent_commission_total"]), 2),
                    "agency_commission_total": round(float(row["agency_commission_total"]), 2),
                }
                for row in by_agent
            ],
        }
        return self.send_json(200, payload)

    def get_settings(self, user):
        conn = db_conn()
        competition_mode = setting(conn, "competition_mode", "1")
        data = {
            "competition_mode": competition_mode == "1",
            "change_action_labels": CHANGE_ACTION_LABELS,
        }

        if user["role"] == "owner":
            data["default_agent_commission_rate"] = float(
                setting(conn, "default_agent_commission_rate", "10")
            )
            data["default_agency_commission_rate"] = float(
                setting(conn, "default_agency_commission_rate", "18")
            )
            users = conn.execute(
                "SELECT id, username, display_name FROM users WHERE role = 'agent' ORDER BY id"
            ).fetchall()
            data["agents"] = [dict(row) for row in users]
        conn.close()

        return self.send_json(200, data)

    def post_settings(self):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        conn = db_conn()
        if "competition_mode" in data:
            value = "1" if bool(data.get("competition_mode")) else "0"
            upsert_setting(conn, "competition_mode", value)

        if "default_agent_commission_rate" in data:
            try:
                rate = float(data["default_agent_commission_rate"])
                if rate < 0:
                    raise ValueError
            except (TypeError, ValueError):
                conn.close()
                return self.send_json(400, {"error": "default_agent_commission_rate must be >= 0"})
            upsert_setting(conn, "default_agent_commission_rate", f"{rate}")

        if "default_agency_commission_rate" in data:
            try:
                rate = float(data["default_agency_commission_rate"])
                if rate < 0:
                    raise ValueError
            except (TypeError, ValueError):
                conn.close()
                return self.send_json(400, {"error": "default_agency_commission_rate must be >= 0"})
            upsert_setting(conn, "default_agency_commission_rate", f"{rate}")

        if "agents" in data:
            agents = data.get("agents")
            if not isinstance(agents, list):
                conn.close()
                return self.send_json(400, {"error": "agents must be a list"})

            for item in agents:
                if not isinstance(item, dict):
                    conn.close()
                    return self.send_json(400, {"error": "Each agent payload must be an object"})
                try:
                    agent_id = int(item.get("id"))
                except (TypeError, ValueError):
                    conn.close()
                    return self.send_json(400, {"error": "Agent id must be numeric"})

                display_name = str(item.get("display_name", "")).strip()
                if not display_name:
                    conn.close()
                    return self.send_json(400, {"error": "Agent display_name is required"})

                exists = conn.execute(
                    "SELECT id FROM users WHERE id = ? AND role = 'agent'", (agent_id,)
                ).fetchone()
                if not exists:
                    conn.close()
                    return self.send_json(400, {"error": f"Invalid agent id: {agent_id}"})

                conn.execute(
                    "UPDATE users SET display_name = ? WHERE id = ?",
                    (display_name, agent_id),
                )

        conn.commit()
        conn.close()
        return self.send_json(200, {"ok": True})

    def get_change_confirmations(self, user, query):
        status = query.get("status", ["all"])[0].strip().lower()
        allowed_statuses = {"all", "pending", "confirmed", "declined", "expired"}
        if status not in allowed_statuses:
            return self.send_json(400, {"error": "Invalid status filter"})

        base_url = self.build_base_url()
        conn = db_conn()
        if self.expire_pending_confirmations(conn) > 0:
            conn.commit()

        where = []
        params = []
        if user["role"] != "owner":
            where.append("c.created_by = ?")
            params.append(user["id"])
        if status != "all":
            where.append("c.status = ?")
            params.append(status)

        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                c.*,
                u.display_name AS created_by_name
            FROM change_confirmations c
            JOIN users u ON u.id = c.created_by
            {where_sql}
            ORDER BY c.created_at DESC, c.id DESC
            LIMIT 100
            """,
            params,
        ).fetchall()
        conn.close()

        payload_rows = []
        for row in rows:
            serialized = self.serialize_confirmation(row)
            serialized["confirm_url"] = f"{base_url}/confirm/{row['token']}"
            payload_rows.append(serialized)

        return self.send_json(200, {"confirmations": payload_rows})

    def post_change_confirmation(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        customer_name = str(data.get("customer_name", "")).strip()
        customer_phone = normalize_phone(str(data.get("customer_phone", "")).strip())
        customer_email = str(data.get("customer_email", "")).strip()
        policy_label = str(data.get("policy_label", "")).strip()
        summary_text = str(data.get("summary_text", "")).strip()
        channel = str(data.get("channel", "manual")).strip().lower()

        if not customer_name:
            return self.send_json(400, {"error": "customer_name is required"})
        if channel not in {"sms", "email", "manual"}:
            return self.send_json(400, {"error": "channel must be sms, email, or manual"})
        if channel == "sms" and not customer_phone:
            return self.send_json(400, {"error": "Valid customer_phone is required for SMS"})
        if channel == "email" and not customer_email:
            return self.send_json(400, {"error": "customer_email is required for email"})

        try:
            actions = self.parse_change_actions(data.get("actions", []))
        except ValueError as exc:
            return self.send_json(400, {"error": str(exc)})

        if not summary_text:
            summary_text = self.build_change_summary(actions)
        if not summary_text:
            return self.send_json(400, {"error": "summary_text is required"})

        try:
            expires_minutes = int(data.get("expires_minutes", DEFAULT_CONFIRM_EXPIRY_MINUTES))
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "expires_minutes must be numeric"})
        if expires_minutes < 5 or expires_minutes > MAX_CONFIRM_EXPIRY_MINUTES:
            return self.send_json(
                400,
                {"error": f"expires_minutes must be between 5 and {MAX_CONFIRM_EXPIRY_MINUTES}"},
            )

        created_at = now_utc_iso()
        expires_at = (datetime.utcnow() + timedelta(minutes=expires_minutes)).isoformat(timespec="seconds")
        token = secrets.token_urlsafe(24)
        confirm_url = f"{self.build_base_url()}/confirm/{token}"
        message_text = self.build_confirmation_message(
            customer_name,
            policy_label,
            summary_text,
            confirm_url,
            expires_at,
            channel,
        )

        conn = db_conn()
        if channel == "sms" and customer_phone:
            conn.execute(
                """
                UPDATE change_confirmations
                SET status = 'expired'
                WHERE status = 'pending' AND customer_phone = ?
                """,
                (customer_phone,),
            )
        conn.execute(
            """
            INSERT INTO change_confirmations(
                token,
                customer_name,
                customer_phone,
                customer_email,
                policy_label,
                actions_json,
                summary_text,
                status,
                channel,
                message_text,
                created_by,
                created_at,
                expires_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (
                token,
                customer_name,
                customer_phone,
                customer_email,
                policy_label,
                json.dumps(actions),
                summary_text,
                channel,
                message_text,
                user["id"],
                created_at,
                expires_at,
            ),
        )
        confirmation_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        delivery = self.confirmation_delivery(
            channel,
            customer_phone,
            customer_email,
            message_text,
            webhook_data=str(confirmation_id),
        )
        if delivery["sent"]:
            conn.execute(
                "UPDATE change_confirmations SET last_sent_at = ? WHERE id = ?",
                (now_utc_iso(), confirmation_id),
            )

        conn.commit()
        row = conn.execute(
            """
            SELECT c.*, u.display_name AS created_by_name
            FROM change_confirmations c
            JOIN users u ON u.id = c.created_by
            WHERE c.id = ?
            """,
            (confirmation_id,),
        ).fetchone()
        conn.close()

        payload = self.serialize_confirmation(row)
        payload["confirm_url"] = confirm_url
        payload["delivery"] = delivery
        return self.send_json(201, payload)

    def post_resend_change_confirmation(self, user, confirmation_id):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        requested_channel = str(data.get("channel", "")).strip().lower()
        if requested_channel and requested_channel not in {"sms", "email", "manual"}:
            return self.send_json(400, {"error": "channel must be sms, email, or manual"})

        conn = db_conn()
        if self.expire_pending_confirmations(conn) > 0:
            conn.commit()

        row = conn.execute(
            """
            SELECT c.*, u.display_name AS created_by_name
            FROM change_confirmations c
            JOIN users u ON u.id = c.created_by
            WHERE c.id = ?
            """,
            (confirmation_id,),
        ).fetchone()

        if not row:
            conn.close()
            return self.send_json(404, {"error": "Change confirmation not found"})
        if user["role"] != "owner" and row["created_by"] != user["id"]:
            conn.close()
            return self.send_json(403, {"error": "Forbidden"})
        if row["status"] != "pending":
            conn.close()
            return self.send_json(409, {"error": f"Cannot resend a {row['status']} request"})

        channel = requested_channel or row["channel"]
        confirm_url = f"{self.build_base_url()}/confirm/{row['token']}"
        message_text = self.build_confirmation_message(
            row["customer_name"],
            row["policy_label"] or "",
            row["summary_text"],
            confirm_url,
            row["expires_at"],
            channel,
        )

        delivery = self.confirmation_delivery(
            channel,
            row["customer_phone"] or "",
            row["customer_email"] or "",
            message_text,
            webhook_data=str(confirmation_id),
        )
        if delivery["sent"]:
            conn.execute(
                """
                UPDATE change_confirmations
                SET channel = ?, message_text = ?, last_sent_at = ?
                WHERE id = ?
                """,
                (channel, message_text, now_utc_iso(), confirmation_id),
            )
            conn.commit()

        refreshed = conn.execute(
            """
            SELECT c.*, u.display_name AS created_by_name
            FROM change_confirmations c
            JOIN users u ON u.id = c.created_by
            WHERE c.id = ?
            """,
            (confirmation_id,),
        ).fetchone()
        conn.close()

        payload = self.serialize_confirmation(refreshed)
        payload["confirm_url"] = confirm_url
        payload["delivery"] = delivery
        return self.send_json(200, payload)

    def get_public_confirmation(self, token):
        token = token.strip()
        if not token:
            return self.send_json(400, {"error": "Invalid token"})

        conn = db_conn()
        if self.expire_pending_confirmations(conn) > 0:
            conn.commit()

        row = conn.execute(
            """
            SELECT
                id,
                customer_name,
                policy_label,
                actions_json,
                summary_text,
                status,
                created_at,
                expires_at,
                confirmed_at,
                declined_at,
                decision_note,
                signature_name
            FROM change_confirmations
            WHERE token = ?
            """,
            (token,),
        ).fetchone()
        conn.close()

        if not row:
            return self.send_json(404, {"error": "Confirmation request not found"})

        try:
            actions = json.loads(row["actions_json"])
            if not isinstance(actions, list):
                actions = []
        except json.JSONDecodeError:
            actions = []

        return self.send_json(
            200,
            {
                "id": row["id"],
                "customer_name": row["customer_name"],
                "policy_label": row["policy_label"] or "",
                "actions": actions,
                "summary_text": row["summary_text"],
                "status": row["status"],
                "created_at": row["created_at"],
                "expires_at": row["expires_at"],
                "confirmed_at": row["confirmed_at"],
                "declined_at": row["declined_at"],
                "decision_note": row["decision_note"] or "",
                "signature_name": row["signature_name"] or "",
            },
        )

    def post_public_confirmation(self, token):
        token = token.strip()
        if not token:
            return self.send_json(400, {"error": "Invalid token"})

        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        decision = str(data.get("decision", "")).strip().lower()
        if decision in {"approve", "confirm", "confirmed", "yes"}:
            decision = "confirmed"
        elif decision in {"decline", "declined", "no"}:
            decision = "declined"
        else:
            return self.send_json(400, {"error": "decision must be confirm or decline"})

        signature_name = str(data.get("signature_name", "")).strip()
        if not signature_name:
            return self.send_json(400, {"error": "signature_name is required"})
        if len(signature_name) > 120:
            return self.send_json(400, {"error": "signature_name is too long"})

        decision_note = str(data.get("decision_note", "")).strip()
        if len(decision_note) > 800:
            return self.send_json(400, {"error": "decision_note is too long"})

        forwarded_for = self.headers.get("X-Forwarded-For", "")
        ip_address = forwarded_for.split(",")[0].strip() if forwarded_for else ""
        if not ip_address and self.client_address:
            ip_address = self.client_address[0]
        user_agent = str(self.headers.get("User-Agent", "")).strip()

        conn = db_conn()
        if self.expire_pending_confirmations(conn) > 0:
            conn.commit()

        row = conn.execute(
            "SELECT id, status FROM change_confirmations WHERE token = ?",
            (token,),
        ).fetchone()
        if not row:
            conn.close()
            return self.send_json(404, {"error": "Confirmation request not found"})
        if row["status"] != "pending":
            conn.close()
            return self.send_json(409, {"error": f"Request already {row['status']}"})

        now = now_utc_iso()
        conn.execute(
            """
            UPDATE change_confirmations
            SET
                status = ?,
                confirmed_at = ?,
                declined_at = ?,
                signature_name = ?,
                decision_note = ?,
                ip_address = ?,
                user_agent = ?
            WHERE id = ?
            """,
            (
                decision,
                now if decision == "confirmed" else None,
                now if decision == "declined" else None,
                signature_name,
                decision_note,
                ip_address,
                user_agent,
                row["id"],
            ),
        )
        conn.commit()
        conn.close()

        return self.send_json(200, {"ok": True, "status": decision, "decision_at": now})

    def post_sms_inbound(self, query):
        shared_secret = os.environ.get("SMS_INBOUND_SECRET", "").strip()
        if shared_secret:
            provided = query.get("secret", [""])[0].strip()
            if not provided:
                provided = str(self.headers.get("X-SMS-Secret", "")).strip()
            if provided != shared_secret:
                return self.send_text(403, "Forbidden")

        provider = query.get("provider", [""])[0].strip().lower()
        if not provider:
            content_type = str(self.headers.get("Content-Type", "")).lower()
            provider = "textbelt" if "application/json" in content_type else "twilio"

        def twiml(message: str):
            safe = (
                str(message or "")
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;")
            )
            xml = f'<?xml version="1.0" encoding="UTF-8"?><Response><Message>{safe}</Message></Response>'
            return self.send_text(200, xml, content_type="text/xml; charset=utf-8")

        if provider == "textbelt":
            payload = self.read_json()
            if payload is None or not isinstance(payload, dict):
                return self.send_json(400, {"error": "Invalid JSON payload"})
            from_phone = normalize_phone(payload.get("fromNumber", ""))
            body_raw = str(payload.get("text", "")).strip()
            inbound_message_id = str(payload.get("textId", "")).strip()
            webhook_data = str(payload.get("data", payload.get("webhookData", ""))).strip()
            send_ack = lambda message: self.send_json(200, {"ok": True, "message": message})
        elif provider == "twilio":
            form = self.read_form()
            if form is None:
                return self.send_text(400, "Invalid form payload")
            from_phone = normalize_phone(form.get("From", ""))
            body_raw = str(form.get("Body", "")).strip()
            inbound_message_id = str(form.get("MessageSid", "")).strip()
            webhook_data = ""
            send_ack = twiml
        else:
            return self.send_json(400, {"error": "Unsupported SMS inbound provider"})

        if not from_phone and not webhook_data:
            return send_ack("Could not read your phone number. Please call the office.")

        body = body_raw.lower().strip()
        first_word = body.split()[0] if body else ""
        first_word = "".join(ch for ch in first_word if ch.isalnum())

        if first_word in {"yes", "y", "confirm", "confirmed"}:
            decision = "confirmed"
        elif first_word in {"no", "n", "decline", "declined"}:
            decision = "declined"
        else:
            return send_ack("Reply YES to confirm your policy changes or NO to decline.")

        forwarded_for = self.headers.get("X-Forwarded-For", "")
        ip_address = forwarded_for.split(",")[0].strip() if forwarded_for else ""
        if not ip_address and self.client_address:
            ip_address = self.client_address[0]

        conn = db_conn()
        if self.expire_pending_confirmations(conn) > 0:
            conn.commit()

        target_id = None
        if webhook_data.isdigit():
            row = conn.execute(
                "SELECT id FROM change_confirmations WHERE id = ? AND status = 'pending'",
                (int(webhook_data),),
            ).fetchone()
            if row:
                target_id = row["id"]

        if target_id is None:
            candidates = conn.execute(
                """
                SELECT id, customer_phone
                FROM change_confirmations
                WHERE status = 'pending'
                ORDER BY created_at DESC, id DESC
                LIMIT 200
                """
            ).fetchall()
            for row in candidates:
                if phone_matches(row["customer_phone"] or "", from_phone):
                    target_id = row["id"]
                    break

        if not target_id:
            conn.close()
            return send_ack("No pending change request found for this number. Please call the office.")

        now = now_utc_iso()
        note = f"SMS ({provider}) reply: {body_raw}"
        if inbound_message_id:
            note = f"{note} (id: {inbound_message_id})"
        conn.execute(
            """
            UPDATE change_confirmations
            SET
                status = ?,
                confirmed_at = ?,
                declined_at = ?,
                signature_name = ?,
                decision_note = ?,
                ip_address = ?,
                user_agent = ?
            WHERE id = ? AND status = 'pending'
            """,
            (
                decision,
                now if decision == "confirmed" else None,
                now if decision == "declined" else None,
                f"SMS:{from_phone}",
                note,
                ip_address,
                f"sms-webhook:{provider}",
                target_id,
            ),
        )
        conn.commit()
        conn.close()

        if decision == "confirmed":
            return send_ack("Confirmed. Your requested policy changes were approved.")
        return send_ack("Declined. We will not process the requested policy changes.")

    def normalize_col(self, name: str) -> str:
        return (
            name.lower()
            .replace("#", "")
            .replace("(", "")
            .replace(")", "")
            .replace("-", " ")
            .replace("_", " ")
            .strip()
        )

    def parse_csv_value(self, row, aliases):
        for alias in aliases:
            value = row.get(alias)
            if value is not None and str(value).strip() != "":
                return str(value).strip()
        return ""

    def post_upload(self):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        csv_text = data.get("csvText")
        if not isinstance(csv_text, str) or not csv_text.strip():
            return self.send_json(400, {"error": "csvText is required"})

        conn = db_conn()
        default_agent_rate = float(setting(conn, "default_agent_commission_rate", "10"))
        default_agency_rate = float(setting(conn, "default_agency_commission_rate", "18"))

        agents = {
            row["display_name"].lower(): row["id"]
            for row in conn.execute("SELECT id, display_name FROM users WHERE role = 'agent'").fetchall()
        }

        created = 0
        reader = csv.DictReader(StringIO(csv_text))
        if not reader.fieldnames:
            conn.close()
            return self.send_json(400, {"error": "CSV has no headers"})

        normalized = {field: self.normalize_col(field) for field in reader.fieldnames}

        for raw_row in reader:
            row = {normalized[k]: (v or "") for k, v in raw_row.items()}

            date_raw = self.parse_csv_value(row, ["date", "date sold", "sold date"])
            customer = self.parse_csv_value(row, ["customer name", "name", "customer"])
            phone = self.parse_csv_value(row, ["contact", "phone", "number"])
            address = self.parse_csv_value(row, ["address"])
            policy_type = self.parse_csv_value(row, ["policy", "policy type", "line of business"])
            carrier = self.parse_csv_value(row, ["purchase company", "carrier", "company"])
            notes = self.parse_csv_value(row, ["notes", "updates"])
            premium_raw = self.parse_csv_value(
                row,
                ["premium", "premium amount", "total premium", "listed premium", "amount"],
            )
            salesperson_name = self.parse_csv_value(
                row,
                ["salesperson", "agent", "salesman", "employee"],
            ).lower()

            if not customer or not date_raw or not premium_raw:
                continue

            date_sold = ""
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
                try:
                    date_sold = datetime.strptime(date_raw.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
            if not date_sold:
                continue

            cleaned = (
                premium_raw.replace("$", "")
                .replace(",", "")
                .replace("/month", "")
                .replace("per month", "")
                .replace("monthly", "")
                .strip()
            )
            try:
                premium = float(cleaned)
            except ValueError:
                continue
            if premium <= 0:
                continue

            salesperson_id = agents.get(salesperson_name)
            if not salesperson_id:
                salesperson_id = next(iter(agents.values()), None)
            if not salesperson_id:
                continue

            now = datetime.utcnow().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO sales(
                    salesperson_id, customer_name, phone, address, date_sold,
                    policy_type, carrier, premium_amount,
                    agent_commission_rate, agency_commission_rate, notes, created_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    salesperson_id,
                    customer,
                    phone,
                    address,
                    date_sold,
                    policy_type,
                    carrier,
                    premium,
                    default_agent_rate,
                    default_agency_rate,
                    notes,
                    now,
                ),
            )
            created += 1

        conn.commit()
        conn.close()
        return self.send_json(200, {"ok": True, "created": created})

    def export_sales(self, query):
        month = self.parse_month(query)
        conn = db_conn()
        rows = conn.execute(
            """
            SELECT
                s.date_sold,
                u.display_name AS salesperson,
                s.customer_name,
                s.phone,
                s.address,
                s.policy_type,
                s.carrier,
                s.premium_amount,
                s.agent_commission_rate,
                s.agency_commission_rate,
                ROUND(s.premium_amount * s.agent_commission_rate / 100.0, 2) AS agent_commission_amount,
                ROUND(s.premium_amount * s.agency_commission_rate / 100.0, 2) AS agency_commission_amount,
                s.notes
            FROM sales s
            JOIN users u ON u.id = s.salesperson_id
            WHERE substr(s.date_sold, 1, 7) = ?
            ORDER BY s.date_sold DESC, s.id DESC
            """,
            (month,),
        ).fetchall()
        conn.close()

        headers = [
            "date_sold",
            "salesperson",
            "customer_name",
            "phone",
            "address",
            "policy_type",
            "carrier",
            "premium_amount",
            "agent_commission_rate",
            "agency_commission_rate",
            "agent_commission_amount",
            "agency_commission_amount",
            "notes",
        ]

        stream = StringIO()
        writer = csv.DictWriter(stream, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))

        filename = f"crm_sales_{month}.csv"
        return self.send_text(
            200,
            stream.getvalue(),
            content_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    def serve_static(self, path):
        if path == "/":
            path = "/index.html"

        requested = os.path.normpath(path.lstrip("/"))
        full = os.path.abspath(os.path.join(STATIC_DIR, requested))
        static_root = os.path.abspath(STATIC_DIR)

        if not full.startswith(static_root):
            return self.send_text(403, "Forbidden")

        if not os.path.exists(full) or not os.path.isfile(full):
            # Use SPA fallback only for path-like routes, never for static assets.
            if "." in os.path.basename(requested):
                return self.send_text(404, "Not found")
            full = os.path.join(STATIC_DIR, "index.html")

        ext = os.path.splitext(full)[1].lower()
        type_overrides = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".html": "text/html; charset=utf-8",
            ".csv": "text/csv; charset=utf-8",
        }
        ctype, _ = mimetypes.guess_type(full)
        ctype = type_overrides.get(ext, ctype)
        if not ctype:
            ctype = "application/octet-stream"

        with open(full, "rb") as f:
            data = f.read()

        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        if full.endswith(".js") or full.endswith(".css"):
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)


def run_server():
    init_db()
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), CRMHandler)
    print(f"CRM server running on http://localhost:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
