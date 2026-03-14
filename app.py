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

from auto_quote_engine import estimate_quote, get_auto_quote_model_summary
from fraud_engine import get_fraud_model_summary, score_transaction

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
EO_ACCOUNT_STATUSES = {"new", "in_review", "bound", "declined", "closed"}
EO_INTERACTION_CHANNELS = {"call", "email", "sms", "meeting", "note"}
EO_INTERACTION_DIRECTIONS = {"outbound", "inbound", "internal"}
EO_CHECKLIST_TEMPLATE = [
    ("needs_assessment", "Needs assessment recorded"),
    ("quote_options", "Quote options and coverage options documented"),
    ("carrier_declinations", "Carrier declinations documented"),
    ("proposal_sent", "Proposal with limits sent to client"),
    ("client_selection", "Client selection and instructions captured"),
    ("signed_forms", "Signed forms and acknowledgements uploaded"),
]


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

        CREATE TABLE IF NOT EXISTS ema_clients(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT,
            preferred_channel TEXT NOT NULL CHECK(preferred_channel IN ('sms', 'email')),
            consent_status TEXT NOT NULL CHECK(consent_status IN ('opted_in', 'opted_out', 'unknown')),
            consent_source TEXT,
            consent_recorded_at TEXT,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS ema_policies(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            policy_number TEXT NOT NULL,
            policy_type TEXT,
            carrier TEXT,
            effective_date TEXT,
            renewal_date TEXT,
            status TEXT NOT NULL CHECK(status IN ('active', 'cancelled', 'pending')),
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(client_id) REFERENCES ema_clients(id),
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS ema_endorsements(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            policy_id INTEGER NOT NULL,
            change_summary TEXT NOT NULL,
            change_actions_json TEXT NOT NULL,
            priority TEXT NOT NULL CHECK(priority IN ('low', 'normal', 'high', 'urgent')),
            due_at TEXT,
            status TEXT NOT NULL CHECK(status IN ('draft', 'awaiting_confirmation', 'confirmed', 'declined', 'closed')),
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(client_id) REFERENCES ema_clients(id),
            FOREIGN KEY(policy_id) REFERENCES ema_policies(id),
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS ema_endorsement_confirmations(
            endorsement_id INTEGER NOT NULL,
            confirmation_id INTEGER NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            PRIMARY KEY(endorsement_id, confirmation_id),
            FOREIGN KEY(endorsement_id) REFERENCES ema_endorsements(id),
            FOREIGN KEY(confirmation_id) REFERENCES change_confirmations(id)
        );

        CREATE TABLE IF NOT EXISTS ema_communications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id INTEGER NOT NULL,
            policy_id INTEGER,
            endorsement_id INTEGER,
            confirmation_id INTEGER,
            direction TEXT NOT NULL CHECK(direction IN ('outbound', 'inbound', 'system')),
            channel TEXT NOT NULL CHECK(channel IN ('sms', 'email', 'manual')),
            message_text TEXT NOT NULL,
            delivery_status TEXT NOT NULL,
            provider TEXT,
            provider_message_id TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY(client_id) REFERENCES ema_clients(id),
            FOREIGN KEY(policy_id) REFERENCES ema_policies(id),
            FOREIGN KEY(endorsement_id) REFERENCES ema_endorsements(id),
            FOREIGN KEY(confirmation_id) REFERENCES change_confirmations(id),
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS ema_audit_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_type TEXT NOT NULL,
            entity_id INTEGER,
            action TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            actor_user_id INTEGER,
            created_at TEXT NOT NULL,
            prev_hash TEXT NOT NULL,
            hash TEXT NOT NULL,
            FOREIGN KEY(actor_user_id) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS eo_accounts(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_name TEXT NOT NULL,
            line_of_business TEXT NOT NULL,
            state TEXT,
            requested_effective_date TEXT,
            coverage_requested TEXT NOT NULL,
            coverage_bound INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL CHECK(status IN ('new', 'in_review', 'bound', 'declined', 'closed')),
            required_docs_json TEXT NOT NULL,
            notes TEXT,
            created_by INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS eo_interactions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            channel TEXT NOT NULL CHECK(channel IN ('call', 'email', 'sms', 'meeting', 'note')),
            direction TEXT NOT NULL CHECK(direction IN ('outbound', 'inbound', 'internal')),
            summary TEXT NOT NULL,
            advice_given TEXT,
            client_response TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL,
            FOREIGN KEY(account_id) REFERENCES eo_accounts(id) ON DELETE CASCADE,
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE TABLE IF NOT EXISTS eo_declinations(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            coverage_item TEXT NOT NULL,
            reason TEXT,
            signature_name TEXT NOT NULL,
            signature_ip TEXT,
            signed_at TEXT NOT NULL,
            created_by INTEGER,
            FOREIGN KEY(account_id) REFERENCES eo_accounts(id) ON DELETE CASCADE,
            FOREIGN KEY(created_by) REFERENCES users(id)
        );

        CREATE INDEX IF NOT EXISTS idx_sales_month ON sales(date_sold);
        CREATE INDEX IF NOT EXISTS idx_sales_owner ON sales(salesperson_id);
        CREATE INDEX IF NOT EXISTS idx_sessions_expiry ON sessions(expires_at);
        CREATE INDEX IF NOT EXISTS idx_change_confirmations_status
            ON change_confirmations(status, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_change_confirmations_token
            ON change_confirmations(token);
        CREATE INDEX IF NOT EXISTS idx_ema_clients_phone ON ema_clients(phone);
        CREATE INDEX IF NOT EXISTS idx_ema_policies_client ON ema_policies(client_id);
        CREATE INDEX IF NOT EXISTS idx_ema_endorsements_status_due
            ON ema_endorsements(status, due_at);
        CREATE INDEX IF NOT EXISTS idx_ema_endorsements_policy
            ON ema_endorsements(policy_id);
        CREATE INDEX IF NOT EXISTS idx_ema_confirm_links_endorsement
            ON ema_endorsement_confirmations(endorsement_id, confirmation_id DESC);
        CREATE INDEX IF NOT EXISTS idx_ema_communications_created
            ON ema_communications(created_at DESC, client_id);
        CREATE INDEX IF NOT EXISTS idx_ema_audit_created
            ON ema_audit_log(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_eo_accounts_updated
            ON eo_accounts(updated_at DESC, status);
        CREATE INDEX IF NOT EXISTS idx_eo_interactions_account_created
            ON eo_interactions(account_id, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_eo_declinations_account_signed
            ON eo_declinations(account_id, signed_at DESC);
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

        if path == "/ema" or path == "/ema/":
            return self.serve_static("/ema.html")

        if path == "/vibe" or path == "/vibe/":
            return self.serve_static("/vibe.html")

        if path == "/medicare" or path == "/medicare/":
            return self.serve_static("/medicare.html")

        if path == "/eo-shield" or path == "/eo-shield/":
            return self.serve_static("/eo_shield.html")

        if path == "/auto-quote" or path == "/auto-quote/":
            return self.serve_static("/auto_quote.html")

        if path == "/fraud" or path == "/fraud/" or path == "/fraud-lab" or path == "/fraud-lab/":
            return self.serve_static("/fraud_lab.html")

        if path == "/confirm" or path == "/confirm/" or path.startswith("/confirm/"):
            return self.serve_static("/confirm.html")

        if path.startswith("/api/"):
            if path == "/api/health":
                return self.send_json(200, {"ok": True})
            if path == "/api/auto-quote/model":
                return self.get_auto_quote_model()
            if path == "/api/fraud/model":
                return self.get_fraud_model()
            if path == "/api/me":
                user = self.require_user()
                if not user:
                    return
                return self.send_json(200, {"user": user})
            if path == "/api/ema/data":
                user = self.require_user()
                if not user:
                    return
                return self.get_ema_data(user)
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
            if path == "/api/eo/data":
                user = self.require_user()
                if not user:
                    return
                return self.get_eo_data(user)
            if path.startswith("/api/eo/accounts/") and path.endswith("/packet"):
                user = self.require_user()
                if not user:
                    return
                parts = [part for part in path.split("/") if part]
                if len(parts) != 5:
                    return self.send_json(404, {"error": "Not found"})
                try:
                    account_id = int(parts[3])
                except ValueError:
                    return self.send_json(400, {"error": "Invalid account id"})
                return self.get_eo_packet(user, account_id)
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
        if path == "/api/auto-quote/estimate":
            return self.post_auto_quote_estimate()
        if path == "/api/fraud/score":
            return self.post_fraud_score()
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
        if path == "/api/eo/accounts":
            user = self.require_user()
            if not user:
                return
            return self.post_eo_account(user)
        if path == "/api/ema/clients":
            user = self.require_user()
            if not user:
                return
            return self.post_ema_client(user)
        if path == "/api/ema/policies":
            user = self.require_user()
            if not user:
                return
            return self.post_ema_policy(user)
        if path == "/api/ema/endorsements":
            user = self.require_user()
            if not user:
                return
            return self.post_ema_endorsement(user)
        if path == "/api/ema/communications":
            user = self.require_user()
            if not user:
                return
            return self.post_ema_communication(user)
        if path.startswith("/api/ema/clients/") and path.endswith("/consent"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                client_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid client id"})
            return self.post_ema_client_consent(user, client_id)
        if path.startswith("/api/ema/endorsements/") and path.endswith("/send-confirmation"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                endorsement_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid endorsement id"})
            return self.post_ema_send_confirmation(user, endorsement_id)
        if path.startswith("/api/ema/endorsements/") and path.endswith("/status"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                endorsement_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid endorsement id"})
            return self.post_ema_endorsement_status(user, endorsement_id)
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
        if path.startswith("/api/eo/accounts/") and path.endswith("/interactions"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                account_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid account id"})
            return self.post_eo_interaction(user, account_id)
        if path.startswith("/api/eo/accounts/") and path.endswith("/declinations"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                account_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid account id"})
            return self.post_eo_declination(user, account_id)
        if path.startswith("/api/eo/accounts/") and path.endswith("/checklist"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                account_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid account id"})
            return self.post_eo_checklist(user, account_id)
        if path.startswith("/api/eo/accounts/") and path.endswith("/status"):
            user = self.require_user()
            if not user:
                return
            parts = [part for part in path.split("/") if part]
            if len(parts) != 5:
                return self.send_json(404, {"error": "Not found"})
            try:
                account_id = int(parts[3])
            except ValueError:
                return self.send_json(400, {"error": "Invalid account id"})
            return self.post_eo_status(user, account_id)
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

    def get_auto_quote_model(self):
        try:
            payload = get_auto_quote_model_summary()
            return self.send_json(200, payload)
        except FileNotFoundError:
            return self.send_json(
                503,
                {"error": "Auto quote model missing. Run scripts/train_auto_quote_model.py first."},
            )
        except Exception as exc:
            return self.send_json(500, {"error": f"Failed to load auto quote model: {exc}"})

    def post_auto_quote_estimate(self):
        payload = self.read_json()
        if payload is None:
            return self.send_json(400, {"error": "Invalid JSON payload"})
        try:
            result = estimate_quote(payload if isinstance(payload, dict) else {})
            return self.send_json(200, result)
        except FileNotFoundError:
            return self.send_json(
                503,
                {"error": "Auto quote model missing. Run scripts/train_auto_quote_model.py first."},
            )
        except Exception as exc:
            return self.send_json(500, {"error": f"Failed to estimate quote: {exc}"})

    def get_fraud_model(self):
        try:
            payload = get_fraud_model_summary()
            return self.send_json(200, payload)
        except FileNotFoundError:
            return self.send_json(503, {"error": "Fraud model JSON is missing."})
        except Exception as exc:
            return self.send_json(500, {"error": f"Failed to load fraud model: {exc}"})

    def post_fraud_score(self):
        payload = self.read_json()
        if payload is None:
            return self.send_json(400, {"error": "Invalid JSON payload"})
        try:
            result = score_transaction(payload if isinstance(payload, dict) else {})
            return self.send_json(200, result)
        except FileNotFoundError:
            return self.send_json(503, {"error": "Fraud model JSON is missing."})
        except Exception as exc:
            return self.send_json(500, {"error": f"Failed to score transaction: {exc}"})

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

    def append_ema_audit(self, conn, actor_user_id, entity_type, entity_id, action, payload):
        created_at = now_utc_iso()
        safe_payload = payload if isinstance(payload, dict) else {"value": payload}
        payload_json = json.dumps(safe_payload, sort_keys=True, separators=(",", ":"))
        prev = conn.execute("SELECT hash FROM ema_audit_log ORDER BY id DESC LIMIT 1").fetchone()
        prev_hash = prev["hash"] if prev else ""
        digest_input = (
            f"{APP_SALT}|{prev_hash}|{created_at}|{entity_type}|{entity_id}|{action}|{payload_json}"
        )
        digest = hashlib.sha256(digest_input.encode("utf-8")).hexdigest()
        conn.execute(
            """
            INSERT INTO ema_audit_log(
                entity_type,
                entity_id,
                action,
                payload_json,
                actor_user_id,
                created_at,
                prev_hash,
                hash
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(entity_type or "unknown"),
                entity_id if entity_id is not None else None,
                str(action or "event"),
                payload_json,
                actor_user_id,
                created_at,
                prev_hash,
                digest,
            ),
        )
        return digest

    def normalize_due_at(self, value):
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
            return parsed.strftime("%Y-%m-%dT00:00:00")
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(text.replace("Z", ""))
            return parsed.isoformat(timespec="seconds")
        except ValueError:
            raise ValueError("due_at must be YYYY-MM-DD or ISO date/time")

    def normalize_ymd(self, value, field_name):
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"{field_name} must be YYYY-MM-DD")

    def sync_ema_endorsement_statuses(self, conn, actor_user_id):
        rows = conn.execute(
            """
            SELECT
                e.id,
                e.status AS endorsement_status,
                c.status AS confirmation_status
            FROM ema_endorsements e
            JOIN ema_endorsement_confirmations l ON l.endorsement_id = e.id
            JOIN change_confirmations c ON c.id = l.confirmation_id
            WHERE l.confirmation_id = (
                SELECT MAX(l2.confirmation_id)
                FROM ema_endorsement_confirmations l2
                WHERE l2.endorsement_id = e.id
            )
            AND e.status = 'awaiting_confirmation'
            AND c.status IN ('confirmed', 'declined')
            """
        ).fetchall()

        if not rows:
            return 0

        changed = 0
        for row in rows:
            new_status = row["confirmation_status"]
            conn.execute(
                "UPDATE ema_endorsements SET status = ?, updated_at = ? WHERE id = ?",
                (new_status, now_utc_iso(), row["id"]),
            )
            self.append_ema_audit(
                conn,
                actor_user_id,
                "endorsement",
                row["id"],
                "auto_sync_confirmation_status",
                {"status": new_status},
            )
            changed += 1
        return changed

    def get_ema_data(self, user):
        conn = db_conn()
        dirty = False
        if self.expire_pending_confirmations(conn) > 0:
            dirty = True
        if self.sync_ema_endorsement_statuses(conn, user["id"]) > 0:
            dirty = True
        if dirty:
            conn.commit()

        clients = conn.execute(
            """
            SELECT
                c.*,
                COUNT(DISTINCT p.id) AS policies_count,
                SUM(
                    CASE
                        WHEN e.status IN ('draft', 'awaiting_confirmation') THEN 1
                        ELSE 0
                    END
                ) AS open_endorsements
            FROM ema_clients c
            LEFT JOIN ema_policies p ON p.client_id = c.id
            LEFT JOIN ema_endorsements e ON e.client_id = c.id
            GROUP BY c.id
            ORDER BY c.updated_at DESC, c.id DESC
            """
        ).fetchall()

        policies = conn.execute(
            """
            SELECT
                p.*,
                c.full_name AS client_name,
                c.phone AS client_phone
            FROM ema_policies p
            JOIN ema_clients c ON c.id = p.client_id
            ORDER BY p.updated_at DESC, p.id DESC
            """
        ).fetchall()

        endorsements = conn.execute(
            """
            SELECT
                e.*,
                c.full_name AS client_name,
                c.phone AS client_phone,
                c.email AS client_email,
                c.consent_status AS client_consent_status,
                p.policy_number,
                p.policy_type,
                p.carrier,
                (
                    SELECT c2.id
                    FROM ema_endorsement_confirmations l2
                    JOIN change_confirmations c2 ON c2.id = l2.confirmation_id
                    WHERE l2.endorsement_id = e.id
                    ORDER BY l2.confirmation_id DESC
                    LIMIT 1
                ) AS confirmation_id,
                (
                    SELECT c2.status
                    FROM ema_endorsement_confirmations l2
                    JOIN change_confirmations c2 ON c2.id = l2.confirmation_id
                    WHERE l2.endorsement_id = e.id
                    ORDER BY l2.confirmation_id DESC
                    LIMIT 1
                ) AS confirmation_status,
                (
                    SELECT c2.channel
                    FROM ema_endorsement_confirmations l2
                    JOIN change_confirmations c2 ON c2.id = l2.confirmation_id
                    WHERE l2.endorsement_id = e.id
                    ORDER BY l2.confirmation_id DESC
                    LIMIT 1
                ) AS confirmation_channel,
                (
                    SELECT c2.last_sent_at
                    FROM ema_endorsement_confirmations l2
                    JOIN change_confirmations c2 ON c2.id = l2.confirmation_id
                    WHERE l2.endorsement_id = e.id
                    ORDER BY l2.confirmation_id DESC
                    LIMIT 1
                ) AS confirmation_last_sent_at
            FROM ema_endorsements e
            JOIN ema_clients c ON c.id = e.client_id
            JOIN ema_policies p ON p.id = e.policy_id
            ORDER BY e.created_at DESC, e.id DESC
            LIMIT 250
            """
        ).fetchall()

        communications = conn.execute(
            """
            SELECT
                m.*,
                c.full_name AS client_name,
                p.policy_number,
                u.display_name AS created_by_name
            FROM ema_communications m
            JOIN ema_clients c ON c.id = m.client_id
            LEFT JOIN ema_policies p ON p.id = m.policy_id
            LEFT JOIN users u ON u.id = m.created_by
            ORDER BY m.id DESC
            LIMIT 200
            """
        ).fetchall()

        audit = conn.execute(
            """
            SELECT
                a.*,
                u.display_name AS actor_name
            FROM ema_audit_log a
            LEFT JOIN users u ON u.id = a.actor_user_id
            ORDER BY a.id DESC
            LIMIT 200
            """
        ).fetchall()
        conn.close()

        out_clients = []
        for row in clients:
            out_clients.append(
                {
                    "id": row["id"],
                    "full_name": row["full_name"],
                    "phone": row["phone"],
                    "email": row["email"] or "",
                    "preferred_channel": row["preferred_channel"],
                    "consent_status": row["consent_status"],
                    "consent_source": row["consent_source"] or "",
                    "consent_recorded_at": row["consent_recorded_at"] or "",
                    "policies_count": int(row["policies_count"] or 0),
                    "open_endorsements": int(row["open_endorsements"] or 0),
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )

        out_policies = []
        for row in policies:
            out_policies.append(
                {
                    "id": row["id"],
                    "client_id": row["client_id"],
                    "client_name": row["client_name"],
                    "client_phone": row["client_phone"] or "",
                    "policy_number": row["policy_number"],
                    "policy_type": row["policy_type"] or "",
                    "carrier": row["carrier"] or "",
                    "effective_date": row["effective_date"] or "",
                    "renewal_date": row["renewal_date"] or "",
                    "status": row["status"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )

        out_endorsements = []
        for row in endorsements:
            try:
                actions = json.loads(row["change_actions_json"])
                if not isinstance(actions, list):
                    actions = []
            except json.JSONDecodeError:
                actions = []
            out_endorsements.append(
                {
                    "id": row["id"],
                    "client_id": row["client_id"],
                    "policy_id": row["policy_id"],
                    "client_name": row["client_name"],
                    "client_phone": row["client_phone"] or "",
                    "client_email": row["client_email"] or "",
                    "client_consent_status": row["client_consent_status"] or "unknown",
                    "policy_number": row["policy_number"],
                    "policy_type": row["policy_type"] or "",
                    "carrier": row["carrier"] or "",
                    "change_summary": row["change_summary"],
                    "actions": actions,
                    "priority": row["priority"],
                    "due_at": row["due_at"] or "",
                    "status": row["status"],
                    "confirmation_id": row["confirmation_id"],
                    "confirmation_status": row["confirmation_status"] or "",
                    "confirmation_channel": row["confirmation_channel"] or "",
                    "confirmation_last_sent_at": row["confirmation_last_sent_at"] or "",
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )

        out_comms = []
        for row in communications:
            out_comms.append(
                {
                    "id": row["id"],
                    "client_id": row["client_id"],
                    "policy_id": row["policy_id"],
                    "endorsement_id": row["endorsement_id"],
                    "confirmation_id": row["confirmation_id"],
                    "direction": row["direction"],
                    "channel": row["channel"],
                    "message_text": row["message_text"],
                    "delivery_status": row["delivery_status"],
                    "provider": row["provider"] or "",
                    "provider_message_id": row["provider_message_id"] or "",
                    "created_by": row["created_by"],
                    "created_by_name": row["created_by_name"] or "",
                    "client_name": row["client_name"],
                    "policy_number": row["policy_number"] or "",
                    "created_at": row["created_at"],
                }
            )

        out_audit = []
        for row in audit:
            out_audit.append(
                {
                    "id": row["id"],
                    "entity_type": row["entity_type"],
                    "entity_id": row["entity_id"],
                    "action": row["action"],
                    "payload_json": row["payload_json"],
                    "actor_user_id": row["actor_user_id"],
                    "actor_name": row["actor_name"] or "",
                    "created_at": row["created_at"],
                    "hash": row["hash"],
                    "prev_hash": row["prev_hash"],
                }
            )

        now = now_utc_iso()
        summary = {
            "clients": len(out_clients),
            "policies": len(out_policies),
            "endorsements_total": len(out_endorsements),
            "awaiting_confirmation": len(
                [e for e in out_endorsements if e["status"] == "awaiting_confirmation"]
            ),
            "declined": len([e for e in out_endorsements if e["status"] == "declined"]),
            "confirmed": len([e for e in out_endorsements if e["status"] == "confirmed"]),
            "overdue_open": len(
                [
                    e
                    for e in out_endorsements
                    if e["due_at"]
                    and e["due_at"] < now
                    and e["status"] in {"draft", "awaiting_confirmation"}
                ]
            ),
        }

        return self.send_json(
            200,
            {
                "summary": summary,
                "clients": out_clients,
                "policies": out_policies,
                "endorsements": out_endorsements,
                "communications": out_comms,
                "audit_log": out_audit,
            },
        )

    def post_ema_client(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        full_name = str(data.get("full_name", "")).strip()
        phone = normalize_phone(data.get("phone", ""))
        email = str(data.get("email", "")).strip()
        preferred_channel = str(data.get("preferred_channel", "sms")).strip().lower()
        consent_status = str(data.get("consent_status", "unknown")).strip().lower()
        consent_source = str(data.get("consent_source", "")).strip()

        if not full_name:
            return self.send_json(400, {"error": "full_name is required"})
        if not phone:
            return self.send_json(400, {"error": "Valid phone is required"})
        if preferred_channel not in {"sms", "email"}:
            return self.send_json(400, {"error": "preferred_channel must be sms or email"})
        if consent_status not in {"opted_in", "opted_out", "unknown"}:
            return self.send_json(400, {"error": "consent_status must be opted_in, opted_out, or unknown"})
        if preferred_channel == "email" and not email:
            return self.send_json(400, {"error": "email is required when preferred_channel is email"})

        conn = db_conn()
        existing = conn.execute("SELECT id FROM ema_clients WHERE phone = ?", (phone,)).fetchone()
        if existing:
            conn.close()
            return self.send_json(409, {"error": "A client with this phone already exists"})

        now = now_utc_iso()
        consent_recorded_at = now if consent_status != "unknown" else None
        conn.execute(
            """
            INSERT INTO ema_clients(
                full_name,
                phone,
                email,
                preferred_channel,
                consent_status,
                consent_source,
                consent_recorded_at,
                created_by,
                created_at,
                updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                full_name,
                phone,
                email,
                preferred_channel,
                consent_status,
                consent_source,
                consent_recorded_at,
                user["id"],
                now,
                now,
            ),
        )
        client_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        self.append_ema_audit(
            conn,
            user["id"],
            "client",
            client_id,
            "client_created",
            {
                "full_name": full_name,
                "phone": phone,
                "preferred_channel": preferred_channel,
                "consent_status": consent_status,
            },
        )
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "client_id": client_id})

    def post_ema_policy(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        try:
            client_id = int(data.get("client_id"))
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "client_id is required"})

        policy_number = str(data.get("policy_number", "")).strip()
        policy_type = str(data.get("policy_type", "")).strip()
        carrier = str(data.get("carrier", "")).strip()
        status = str(data.get("status", "active")).strip().lower()
        try:
            effective_date = self.normalize_ymd(data.get("effective_date", ""), "effective_date")
            renewal_date = self.normalize_ymd(data.get("renewal_date", ""), "renewal_date")
        except ValueError as exc:
            return self.send_json(400, {"error": str(exc)})

        if not policy_number:
            return self.send_json(400, {"error": "policy_number is required"})
        if status not in {"active", "cancelled", "pending"}:
            return self.send_json(400, {"error": "status must be active, cancelled, or pending"})

        conn = db_conn()
        client = conn.execute("SELECT id FROM ema_clients WHERE id = ?", (client_id,)).fetchone()
        if not client:
            conn.close()
            return self.send_json(404, {"error": "Client not found"})

        now = now_utc_iso()
        conn.execute(
            """
            INSERT INTO ema_policies(
                client_id,
                policy_number,
                policy_type,
                carrier,
                effective_date,
                renewal_date,
                status,
                created_by,
                created_at,
                updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                policy_number,
                policy_type,
                carrier,
                effective_date,
                renewal_date,
                status,
                user["id"],
                now,
                now,
            ),
        )
        policy_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        self.append_ema_audit(
            conn,
            user["id"],
            "policy",
            policy_id,
            "policy_created",
            {
                "client_id": client_id,
                "policy_number": policy_number,
                "carrier": carrier,
                "status": status,
            },
        )
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "policy_id": policy_id})

    def post_ema_endorsement(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        try:
            client_id = int(data.get("client_id"))
            policy_id = int(data.get("policy_id"))
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "client_id and policy_id are required"})

        change_summary = str(data.get("change_summary", "")).strip()
        priority = str(data.get("priority", "normal")).strip().lower()
        status = str(data.get("status", "draft")).strip().lower()
        raw_actions = data.get("actions", [])

        if priority not in {"low", "normal", "high", "urgent"}:
            return self.send_json(400, {"error": "priority must be low, normal, high, or urgent"})
        if status not in {"draft", "awaiting_confirmation", "confirmed", "declined", "closed"}:
            return self.send_json(400, {"error": "Invalid endorsement status"})
        if not raw_actions and change_summary:
            raw_actions = [{"type": "other", "target": change_summary}]
        try:
            actions = self.parse_change_actions(raw_actions)
        except ValueError as exc:
            return self.send_json(400, {"error": str(exc)})

        if not change_summary:
            change_summary = self.build_change_summary(actions)
        if not change_summary:
            return self.send_json(400, {"error": "change_summary is required"})

        try:
            due_at = self.normalize_due_at(data.get("due_at", ""))
        except ValueError as exc:
            return self.send_json(400, {"error": str(exc)})

        conn = db_conn()
        policy = conn.execute(
            "SELECT id, client_id FROM ema_policies WHERE id = ?",
            (policy_id,),
        ).fetchone()
        if not policy:
            conn.close()
            return self.send_json(404, {"error": "Policy not found"})
        if int(policy["client_id"]) != client_id:
            conn.close()
            return self.send_json(400, {"error": "policy_id does not belong to client_id"})

        now = now_utc_iso()
        conn.execute(
            """
            INSERT INTO ema_endorsements(
                client_id,
                policy_id,
                change_summary,
                change_actions_json,
                priority,
                due_at,
                status,
                created_by,
                created_at,
                updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                policy_id,
                change_summary,
                json.dumps(actions),
                priority,
                due_at,
                status,
                user["id"],
                now,
                now,
            ),
        )
        endorsement_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        self.append_ema_audit(
            conn,
            user["id"],
            "endorsement",
            endorsement_id,
            "endorsement_created",
            {
                "client_id": client_id,
                "policy_id": policy_id,
                "priority": priority,
                "status": status,
                "change_summary": change_summary,
            },
        )
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "endorsement_id": endorsement_id})

    def post_ema_client_consent(self, user, client_id):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        consent_status = str(data.get("consent_status", "")).strip().lower()
        consent_source = str(data.get("consent_source", "")).strip()
        if consent_status not in {"opted_in", "opted_out", "unknown"}:
            return self.send_json(400, {"error": "consent_status must be opted_in, opted_out, or unknown"})

        conn = db_conn()
        client = conn.execute(
            "SELECT id, preferred_channel FROM ema_clients WHERE id = ?",
            (client_id,),
        ).fetchone()
        if not client:
            conn.close()
            return self.send_json(404, {"error": "Client not found"})

        now = now_utc_iso()
        conn.execute(
            """
            UPDATE ema_clients
            SET consent_status = ?, consent_source = ?, consent_recorded_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (
                consent_status,
                consent_source,
                now if consent_status != "unknown" else None,
                now,
                client_id,
            ),
        )
        conn.execute(
            """
            INSERT INTO ema_communications(
                client_id,
                direction,
                channel,
                message_text,
                delivery_status,
                created_by,
                created_at
            )
            VALUES(?, 'system', 'manual', ?, 'logged', ?, ?)
            """,
            (
                client_id,
                f"Consent status updated to {consent_status}" + (f" ({consent_source})" if consent_source else ""),
                user["id"],
                now,
            ),
        )
        self.append_ema_audit(
            conn,
            user["id"],
            "client",
            client_id,
            "consent_updated",
            {"consent_status": consent_status, "consent_source": consent_source},
        )
        conn.commit()
        conn.close()
        return self.send_json(200, {"ok": True})

    def post_ema_communication(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        try:
            client_id = int(data.get("client_id"))
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "client_id is required"})

        policy_id = data.get("policy_id")
        endorsement_id = data.get("endorsement_id")
        try:
            policy_id = int(policy_id) if policy_id not in (None, "") else None
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "policy_id must be numeric"})
        try:
            endorsement_id = int(endorsement_id) if endorsement_id not in (None, "") else None
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "endorsement_id must be numeric"})

        direction = str(data.get("direction", "system")).strip().lower()
        channel = str(data.get("channel", "manual")).strip().lower()
        message_text = str(data.get("message_text", "")).strip()
        delivery_status = str(data.get("delivery_status", "logged")).strip().lower()

        if direction not in {"outbound", "inbound", "system"}:
            return self.send_json(400, {"error": "direction must be outbound, inbound, or system"})
        if channel not in {"sms", "email", "manual"}:
            return self.send_json(400, {"error": "channel must be sms, email, or manual"})
        if not message_text:
            return self.send_json(400, {"error": "message_text is required"})
        if len(message_text) > 1200:
            return self.send_json(400, {"error": "message_text is too long"})

        conn = db_conn()
        client = conn.execute("SELECT id FROM ema_clients WHERE id = ?", (client_id,)).fetchone()
        if not client:
            conn.close()
            return self.send_json(404, {"error": "Client not found"})
        if policy_id is not None:
            policy = conn.execute(
                "SELECT id, client_id FROM ema_policies WHERE id = ?",
                (policy_id,),
            ).fetchone()
            if not policy:
                conn.close()
                return self.send_json(404, {"error": "Policy not found"})
            if int(policy["client_id"]) != client_id:
                conn.close()
                return self.send_json(400, {"error": "policy_id does not belong to client_id"})
        if endorsement_id is not None:
            endorsement = conn.execute(
                "SELECT id, client_id FROM ema_endorsements WHERE id = ?",
                (endorsement_id,),
            ).fetchone()
            if not endorsement:
                conn.close()
                return self.send_json(404, {"error": "Endorsement not found"})
            if int(endorsement["client_id"]) != client_id:
                conn.close()
                return self.send_json(400, {"error": "endorsement_id does not belong to client_id"})

        now = now_utc_iso()
        conn.execute(
            """
            INSERT INTO ema_communications(
                client_id,
                policy_id,
                endorsement_id,
                direction,
                channel,
                message_text,
                delivery_status,
                created_by,
                created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                policy_id,
                endorsement_id,
                direction,
                channel,
                message_text,
                delivery_status,
                user["id"],
                now,
            ),
        )
        communication_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        self.append_ema_audit(
            conn,
            user["id"],
            "communication",
            communication_id,
            "communication_logged",
            {
                "client_id": client_id,
                "policy_id": policy_id,
                "endorsement_id": endorsement_id,
                "direction": direction,
                "channel": channel,
            },
        )
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "communication_id": communication_id})

    def post_ema_send_confirmation(self, user, endorsement_id):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        requested_channel = str(data.get("channel", "")).strip().lower()
        if requested_channel and requested_channel not in {"sms", "email", "manual"}:
            return self.send_json(400, {"error": "channel must be sms, email, or manual"})
        try:
            expires_minutes = int(data.get("expires_minutes", 1440))
        except (TypeError, ValueError):
            return self.send_json(400, {"error": "expires_minutes must be numeric"})
        if expires_minutes < 5 or expires_minutes > MAX_CONFIRM_EXPIRY_MINUTES:
            return self.send_json(
                400,
                {"error": f"expires_minutes must be between 5 and {MAX_CONFIRM_EXPIRY_MINUTES}"},
            )

        conn = db_conn()
        if self.expire_pending_confirmations(conn) > 0:
            conn.commit()
        row = conn.execute(
            """
            SELECT
                e.*,
                c.full_name AS client_name,
                c.phone AS client_phone,
                c.email AS client_email,
                c.preferred_channel AS preferred_channel,
                c.consent_status AS consent_status,
                p.policy_number
            FROM ema_endorsements e
            JOIN ema_clients c ON c.id = e.client_id
            JOIN ema_policies p ON p.id = e.policy_id
            WHERE e.id = ?
            """,
            (endorsement_id,),
        ).fetchone()
        if not row:
            conn.close()
            return self.send_json(404, {"error": "Endorsement not found"})
        if row["status"] == "closed":
            conn.close()
            return self.send_json(409, {"error": "Closed endorsements cannot send confirmations"})

        channel = requested_channel or row["preferred_channel"] or "sms"
        customer_phone = row["client_phone"] or ""
        customer_email = row["client_email"] or ""
        if channel == "sms":
            if row["consent_status"] != "opted_in":
                conn.close()
                return self.send_json(
                    409,
                    {"error": "Cannot send SMS until client is opted_in (TCPA consent missing)"},
                )
            if not customer_phone:
                conn.close()
                return self.send_json(400, {"error": "Client phone is required for SMS"})
        if channel == "email" and not customer_email:
            conn.close()
            return self.send_json(400, {"error": "Client email is required for email"})

        try:
            actions = json.loads(row["change_actions_json"])
            if not isinstance(actions, list) or not actions:
                actions = [{"type": "other", "target": row["change_summary"]}]
        except json.JSONDecodeError:
            actions = [{"type": "other", "target": row["change_summary"]}]

        token = secrets.token_urlsafe(24)
        created_at = now_utc_iso()
        expires_at = (datetime.utcnow() + timedelta(minutes=expires_minutes)).isoformat(timespec="seconds")
        confirm_url = f"{self.build_base_url()}/confirm/{token}"
        message_text = self.build_confirmation_message(
            row["client_name"],
            row["policy_number"] or "",
            row["change_summary"],
            confirm_url,
            expires_at,
            channel,
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
                row["client_name"],
                customer_phone,
                customer_email,
                row["policy_number"] or "",
                json.dumps(actions),
                row["change_summary"],
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

        conn.execute(
            """
            INSERT INTO ema_endorsement_confirmations(endorsement_id, confirmation_id, created_at)
            VALUES(?, ?, ?)
            """,
            (endorsement_id, confirmation_id, now_utc_iso()),
        )
        conn.execute(
            """
            INSERT INTO ema_communications(
                client_id,
                policy_id,
                endorsement_id,
                confirmation_id,
                direction,
                channel,
                message_text,
                delivery_status,
                provider,
                provider_message_id,
                created_by,
                created_at
            )
            VALUES(?, ?, ?, ?, 'outbound', ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["client_id"],
                row["policy_id"],
                endorsement_id,
                confirmation_id,
                channel,
                message_text,
                delivery.get("status", "failed"),
                delivery.get("provider", ""),
                delivery.get("message_id", ""),
                user["id"],
                now_utc_iso(),
            ),
        )
        conn.execute(
            "UPDATE ema_endorsements SET status = 'awaiting_confirmation', updated_at = ? WHERE id = ?",
            (now_utc_iso(), endorsement_id),
        )
        self.append_ema_audit(
            conn,
            user["id"],
            "endorsement",
            endorsement_id,
            "confirmation_sent",
            {
                "confirmation_id": confirmation_id,
                "channel": channel,
                "delivery_status": delivery.get("status", ""),
            },
        )
        conn.commit()
        conn.close()

        return self.send_json(
            200,
            {
                "ok": True,
                "confirmation_id": confirmation_id,
                "confirm_url": confirm_url,
                "delivery": delivery,
            },
        )

    def post_ema_endorsement_status(self, user, endorsement_id):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        status = str(data.get("status", "")).strip().lower()
        if status not in {"draft", "awaiting_confirmation", "confirmed", "declined", "closed"}:
            return self.send_json(400, {"error": "Invalid status"})

        conn = db_conn()
        endorsement = conn.execute(
            "SELECT id, status FROM ema_endorsements WHERE id = ?",
            (endorsement_id,),
        ).fetchone()
        if not endorsement:
            conn.close()
            return self.send_json(404, {"error": "Endorsement not found"})

        conn.execute(
            "UPDATE ema_endorsements SET status = ?, updated_at = ? WHERE id = ?",
            (status, now_utc_iso(), endorsement_id),
        )
        self.append_ema_audit(
            conn,
            user["id"],
            "endorsement",
            endorsement_id,
            "status_updated",
            {"from": endorsement["status"], "to": status},
        )
        conn.commit()
        conn.close()
        return self.send_json(200, {"ok": True})

    def eo_default_checklist(self):
        return [
            {"key": key, "label": label, "received": False, "note": "", "updated_at": ""}
            for key, label in EO_CHECKLIST_TEMPLATE
        ]

    def eo_bool(self, value):
        if isinstance(value, bool):
            return value
        return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}

    def eo_parse_checklist(self, raw_json):
        template = {key: label for key, label in EO_CHECKLIST_TEMPLATE}
        parsed = []
        try:
            loaded = json.loads(raw_json or "[]")
            if isinstance(loaded, list):
                parsed = loaded
        except json.JSONDecodeError:
            parsed = []

        by_key = {}
        for item in parsed:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key", "")).strip()
            if key not in template:
                continue
            by_key[key] = {
                "key": key,
                "label": template[key],
                "received": bool(item.get("received")),
                "note": str(item.get("note", "")).strip(),
                "updated_at": str(item.get("updated_at", "")).strip(),
            }

        out = []
        for key, label in EO_CHECKLIST_TEMPLATE:
            if key in by_key:
                out.append(by_key[key])
            else:
                out.append({"key": key, "label": label, "received": False, "note": "", "updated_at": ""})
        return out

    def eo_metrics(self, checklist, interactions_count, declinations_count, status):
        docs_total = len(checklist)
        docs_received = len([item for item in checklist if item.get("received")])
        docs_ratio = (docs_received / docs_total) if docs_total else 0.0
        interaction_ratio = min(max(int(interactions_count or 0), 0), 8) / 8.0
        declination_ratio = min(max(int(declinations_count or 0), 0), 3) / 3.0
        status_bonus = 5 if str(status or "").strip().lower() in {"bound", "declined", "closed"} else 0
        score = int(
            round(
                docs_ratio * 60
                + interaction_ratio * 20
                + declination_ratio * 15
                + status_bonus
            )
        )
        score = max(0, min(100, score))
        if score >= 80:
            risk = "low"
        elif score >= 60:
            risk = "medium"
        else:
            risk = "high"
        return {
            "score": score,
            "risk": risk,
            "docs_received": docs_received,
            "docs_total": docs_total,
            "interactions_count": int(interactions_count or 0),
            "declinations_count": int(declinations_count or 0),
        }

    def get_eo_data(self, user):
        conn = db_conn()
        accounts = conn.execute(
            """
            SELECT
                a.*,
                u.display_name AS created_by_name,
                (
                    SELECT COUNT(1)
                    FROM eo_interactions i
                    WHERE i.account_id = a.id
                ) AS interactions_count,
                (
                    SELECT COUNT(1)
                    FROM eo_declinations d
                    WHERE d.account_id = a.id
                ) AS declinations_count,
                (
                    SELECT MAX(i.created_at)
                    FROM eo_interactions i
                    WHERE i.account_id = a.id
                ) AS last_interaction_at
            FROM eo_accounts a
            JOIN users u ON u.id = a.created_by
            ORDER BY a.updated_at DESC, a.id DESC
            """
        ).fetchall()
        interactions = conn.execute(
            """
            SELECT
                i.*,
                u.display_name AS created_by_name
            FROM eo_interactions i
            LEFT JOIN users u ON u.id = i.created_by
            ORDER BY i.created_at DESC, i.id DESC
            LIMIT 500
            """
        ).fetchall()
        declinations = conn.execute(
            """
            SELECT
                d.*,
                u.display_name AS created_by_name
            FROM eo_declinations d
            LEFT JOIN users u ON u.id = d.created_by
            ORDER BY d.signed_at DESC, d.id DESC
            LIMIT 500
            """
        ).fetchall()
        conn.close()

        out_accounts = []
        for row in accounts:
            checklist = self.eo_parse_checklist(row["required_docs_json"])
            metrics = self.eo_metrics(
                checklist,
                row["interactions_count"],
                row["declinations_count"],
                row["status"],
            )
            out_accounts.append(
                {
                    "id": row["id"],
                    "client_name": row["client_name"],
                    "line_of_business": row["line_of_business"],
                    "state": row["state"] or "",
                    "requested_effective_date": row["requested_effective_date"] or "",
                    "coverage_requested": row["coverage_requested"],
                    "coverage_bound": bool(row["coverage_bound"]),
                    "status": row["status"],
                    "notes": row["notes"] or "",
                    "created_by": row["created_by"],
                    "created_by_name": row["created_by_name"] or "",
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "last_interaction_at": row["last_interaction_at"] or "",
                    "checklist": checklist,
                    "metrics": metrics,
                }
            )

        out_interactions = []
        for row in interactions:
            out_interactions.append(
                {
                    "id": row["id"],
                    "account_id": row["account_id"],
                    "channel": row["channel"],
                    "direction": row["direction"],
                    "summary": row["summary"],
                    "advice_given": row["advice_given"] or "",
                    "client_response": row["client_response"] or "",
                    "created_by": row["created_by"],
                    "created_by_name": row["created_by_name"] or "",
                    "created_at": row["created_at"],
                }
            )

        out_declinations = []
        for row in declinations:
            out_declinations.append(
                {
                    "id": row["id"],
                    "account_id": row["account_id"],
                    "coverage_item": row["coverage_item"],
                    "reason": row["reason"] or "",
                    "signature_name": row["signature_name"],
                    "signature_ip": row["signature_ip"] or "",
                    "signed_at": row["signed_at"],
                    "created_by": row["created_by"],
                    "created_by_name": row["created_by_name"] or "",
                }
            )

        summary = {
            "accounts_total": len(out_accounts),
            "status_new": len([a for a in out_accounts if a["status"] == "new"]),
            "status_in_review": len([a for a in out_accounts if a["status"] == "in_review"]),
            "status_bound": len([a for a in out_accounts if a["status"] == "bound"]),
            "status_declined": len([a for a in out_accounts if a["status"] == "declined"]),
            "high_risk_accounts": len([a for a in out_accounts if a["metrics"]["risk"] == "high"]),
            "avg_score": int(round(sum(a["metrics"]["score"] for a in out_accounts) / len(out_accounts)))
            if out_accounts
            else 0,
        }

        return self.send_json(
            200,
            {
                "summary": summary,
                "accounts": out_accounts,
                "interactions": out_interactions,
                "declinations": out_declinations,
                "viewer": {"id": user["id"], "display_name": user["display_name"], "role": user["role"]},
            },
        )

    def get_eo_packet(self, user, account_id):
        del user
        conn = db_conn()
        account = conn.execute(
            """
            SELECT
                a.*,
                u.display_name AS created_by_name
            FROM eo_accounts a
            JOIN users u ON u.id = a.created_by
            WHERE a.id = ?
            """,
            (account_id,),
        ).fetchone()
        if not account:
            conn.close()
            return self.send_json(404, {"error": "Account not found"})

        interactions = conn.execute(
            """
            SELECT
                i.*,
                u.display_name AS created_by_name
            FROM eo_interactions i
            LEFT JOIN users u ON u.id = i.created_by
            WHERE i.account_id = ?
            ORDER BY i.created_at ASC, i.id ASC
            """,
            (account_id,),
        ).fetchall()
        declinations = conn.execute(
            """
            SELECT
                d.*,
                u.display_name AS created_by_name
            FROM eo_declinations d
            LEFT JOIN users u ON u.id = d.created_by
            WHERE d.account_id = ?
            ORDER BY d.signed_at ASC, d.id ASC
            """,
            (account_id,),
        ).fetchall()
        conn.close()

        checklist = self.eo_parse_checklist(account["required_docs_json"])
        metrics = self.eo_metrics(
            checklist,
            len(interactions),
            len(declinations),
            account["status"],
        )

        lines = [
            "# E&O Defensibility Packet",
            "",
            f"Generated: {now_utc_iso()} UTC",
            f"Account ID: {account['id']}",
            f"Client: {account['client_name']}",
            f"Line of Business: {account['line_of_business']}",
            f"State: {account['state'] or '-'}",
            f"Requested Effective Date: {account['requested_effective_date'] or '-'}",
            f"Current Status: {account['status']}",
            f"Created By: {account['created_by_name']}",
            "",
            "## Coverage Requested",
            account["coverage_requested"],
            "",
            "## Documentation Checklist",
        ]

        for item in checklist:
            mark = "[x]" if item.get("received") else "[ ]"
            note = f" - {item.get('note')}" if item.get("note") else ""
            lines.append(f"{mark} {item.get('label')}{note}")

        lines.append("")
        lines.append("## Interaction Timeline")
        if interactions:
            for row in interactions:
                lines.append(
                    f"- {row['created_at']} | {row['channel']} {row['direction']} | "
                    f"{row['summary']} (by {row['created_by_name'] or 'system'})"
                )
                if row["advice_given"]:
                    lines.append(f"  Advice: {row['advice_given']}")
                if row["client_response"]:
                    lines.append(f"  Response: {row['client_response']}")
        else:
            lines.append("- No interactions logged")

        lines.append("")
        lines.append("## Declinations & Signatures")
        if declinations:
            for row in declinations:
                reason_text = row["reason"] or "No reason provided"
                lines.append(
                    f"- {row['signed_at']} | {row['coverage_item']} | "
                    f"Signed by {row['signature_name']} | {reason_text}"
                )
        else:
            lines.append("- No signed declinations on file")

        lines.append("")
        lines.append("## Defensibility Score")
        lines.append(f"Score: {metrics['score']}/100")
        lines.append(f"Risk Level: {metrics['risk']}")
        lines.append(f"Docs Completed: {metrics['docs_received']}/{metrics['docs_total']}")
        lines.append(f"Interactions: {metrics['interactions_count']}")
        lines.append(f"Declinations: {metrics['declinations_count']}")

        return self.send_json(
            200,
            {
                "packet_text": "\n".join(lines),
                "metrics": metrics,
                "account_id": account["id"],
            },
        )

    def post_eo_account(self, user):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        client_name = str(data.get("client_name", "")).strip()
        line_of_business = str(data.get("line_of_business", "")).strip()
        state = str(data.get("state", "")).strip().upper()
        coverage_requested = str(data.get("coverage_requested", "")).strip()
        notes = str(data.get("notes", "")).strip()
        status = str(data.get("status", "new")).strip().lower()
        coverage_bound = 1 if self.eo_bool(data.get("coverage_bound", False)) else 0

        if not client_name:
            return self.send_json(400, {"error": "client_name is required"})
        if not line_of_business:
            return self.send_json(400, {"error": "line_of_business is required"})
        if not coverage_requested:
            return self.send_json(400, {"error": "coverage_requested is required"})
        if status not in EO_ACCOUNT_STATUSES:
            return self.send_json(400, {"error": "Invalid status"})
        if state and len(state) > 2:
            return self.send_json(400, {"error": "state must be 2-letter code"})
        try:
            requested_effective_date = self.normalize_ymd(
                data.get("requested_effective_date", ""),
                "requested_effective_date",
            )
        except ValueError as exc:
            return self.send_json(400, {"error": str(exc)})

        checklist = self.eo_default_checklist()
        now = now_utc_iso()
        conn = db_conn()
        conn.execute(
            """
            INSERT INTO eo_accounts(
                client_name,
                line_of_business,
                state,
                requested_effective_date,
                coverage_requested,
                coverage_bound,
                status,
                required_docs_json,
                notes,
                created_by,
                created_at,
                updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_name,
                line_of_business,
                state,
                requested_effective_date,
                coverage_requested,
                coverage_bound,
                status,
                json.dumps(checklist),
                notes,
                user["id"],
                now,
                now,
            ),
        )
        account_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "account_id": account_id})

    def post_eo_interaction(self, user, account_id):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        channel = str(data.get("channel", "call")).strip().lower()
        direction = str(data.get("direction", "outbound")).strip().lower()
        summary = str(data.get("summary", "")).strip()
        advice_given = str(data.get("advice_given", "")).strip()
        client_response = str(data.get("client_response", "")).strip()

        if channel not in EO_INTERACTION_CHANNELS:
            return self.send_json(400, {"error": "Invalid interaction channel"})
        if direction not in EO_INTERACTION_DIRECTIONS:
            return self.send_json(400, {"error": "Invalid interaction direction"})
        if not summary:
            return self.send_json(400, {"error": "summary is required"})
        if len(summary) > 1200:
            return self.send_json(400, {"error": "summary is too long"})

        conn = db_conn()
        account = conn.execute(
            "SELECT id, status FROM eo_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        if not account:
            conn.close()
            return self.send_json(404, {"error": "Account not found"})

        now = now_utc_iso()
        conn.execute(
            """
            INSERT INTO eo_interactions(
                account_id,
                channel,
                direction,
                summary,
                advice_given,
                client_response,
                created_by,
                created_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                channel,
                direction,
                summary,
                advice_given,
                client_response,
                user["id"],
                now,
            ),
        )
        interaction_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        updated_status = account["status"]
        if updated_status == "new":
            updated_status = "in_review"
        conn.execute(
            "UPDATE eo_accounts SET status = ?, updated_at = ? WHERE id = ?",
            (updated_status, now, account_id),
        )
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "interaction_id": interaction_id})

    def post_eo_declination(self, user, account_id):
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        coverage_item = str(data.get("coverage_item", "")).strip()
        reason = str(data.get("reason", "")).strip()
        signature_name = str(data.get("signature_name", "")).strip()
        mark_declined = self.eo_bool(data.get("mark_account_declined", False))

        if not coverage_item:
            return self.send_json(400, {"error": "coverage_item is required"})
        if not signature_name:
            return self.send_json(400, {"error": "signature_name is required"})

        conn = db_conn()
        account = conn.execute(
            "SELECT id FROM eo_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        if not account:
            conn.close()
            return self.send_json(404, {"error": "Account not found"})

        ip = self.headers.get("X-Forwarded-For", "").split(",")[0].strip()
        if not ip and self.client_address:
            ip = self.client_address[0]

        now = now_utc_iso()
        conn.execute(
            """
            INSERT INTO eo_declinations(
                account_id,
                coverage_item,
                reason,
                signature_name,
                signature_ip,
                signed_at,
                created_by
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account_id,
                coverage_item,
                reason,
                signature_name,
                ip,
                now,
                user["id"],
            ),
        )
        declination_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

        if mark_declined:
            conn.execute(
                "UPDATE eo_accounts SET status = 'declined', updated_at = ? WHERE id = ?",
                (now, account_id),
            )
        else:
            conn.execute(
                "UPDATE eo_accounts SET updated_at = ? WHERE id = ?",
                (now, account_id),
            )
        conn.commit()
        conn.close()
        return self.send_json(201, {"ok": True, "declination_id": declination_id})

    def post_eo_checklist(self, user, account_id):
        del user
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        item_key = str(data.get("item_key", "")).strip()
        received = self.eo_bool(data.get("received", False))
        note = str(data.get("note", "")).strip()

        if not item_key:
            return self.send_json(400, {"error": "item_key is required"})
        if len(note) > 240:
            return self.send_json(400, {"error": "note is too long"})

        conn = db_conn()
        row = conn.execute(
            "SELECT required_docs_json FROM eo_accounts WHERE id = ?",
            (account_id,),
        ).fetchone()
        if not row:
            conn.close()
            return self.send_json(404, {"error": "Account not found"})

        checklist = self.eo_parse_checklist(row["required_docs_json"])
        target = None
        for item in checklist:
            if item["key"] == item_key:
                target = item
                break
        if target is None:
            conn.close()
            return self.send_json(400, {"error": "Unknown checklist item"})

        now = now_utc_iso()
        target["received"] = received
        target["note"] = note
        target["updated_at"] = now
        conn.execute(
            "UPDATE eo_accounts SET required_docs_json = ?, updated_at = ? WHERE id = ?",
            (json.dumps(checklist), now, account_id),
        )
        conn.commit()
        conn.close()
        return self.send_json(200, {"ok": True, "checklist": checklist})

    def post_eo_status(self, user, account_id):
        del user
        data = self.read_json()
        if data is None:
            return self.send_json(400, {"error": "Invalid JSON"})

        status = str(data.get("status", "")).strip().lower()
        if status not in EO_ACCOUNT_STATUSES:
            return self.send_json(400, {"error": "Invalid status"})

        coverage_bound = 1 if status == "bound" else 0
        conn = db_conn()
        exists = conn.execute("SELECT id FROM eo_accounts WHERE id = ?", (account_id,)).fetchone()
        if not exists:
            conn.close()
            return self.send_json(404, {"error": "Account not found"})
        conn.execute(
            "UPDATE eo_accounts SET status = ?, coverage_bound = ?, updated_at = ? WHERE id = ?",
            (status, coverage_bound, now_utc_iso(), account_id),
        )
        conn.commit()
        conn.close()
        return self.send_json(200, {"ok": True})

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
