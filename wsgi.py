"""WSGI entry point — Flask app with auth, MySQL master-DB, and Phase-2 additions.

Phase 2 deltas vs. the original wsgi.py:
  * Flask-Login gate in front of every UI/API route (public: /health, webhook).
  * MySQL schema bootstrap + per-run run_history row + master_leads INSERT.
  * Per-run LeadPool injected into the pipeline so the SecondaryAgent and
    future co-runners share the same dedup set + master-DB awareness.
  * min_cpc default lowered from 1.0 → 0.05.
  * /api/master-stats   (TTL-cached master_leads count for the dashboard)
  * /admin/users + /admin/runs (admin-only)
  * /login GET+POST, /logout
"""
from __future__ import annotations

import csv as _csv
import hashlib
import hmac
import json
import logging
import os
import secrets
import threading
import time
import uuid as _uuid
from datetime import datetime
from typing import Any, Dict, Optional

from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    url_for,
)
from flask_login import current_user

import auth
import db
from auth import (
    admin_required,
    login_manager,
    login_required_json,
    manager_or_admin_required,
    role_required,
    do_login,
    do_logout,
    verify_credentials,
)
from lead_pool import LeadPool
from utils import normalize_master_key, root_domain


def _current_uid() -> Optional[int]:
    """The logged-in user's id, or None when auth is disabled (dev)."""
    try:
        if getattr(current_user, "is_authenticated", False):
            return int(current_user.id)
    except Exception:
        pass
    return None


def _current_role() -> str:
    """Effective role: real role when logged in, 'admin' when auth disabled."""
    if getattr(auth, "LOGIN_DISABLED", False):
        return "admin"
    try:
        return getattr(current_user, "role", "") or ""
    except Exception:
        return ""


def _current_username() -> str:
    try:
        if getattr(current_user, "is_authenticated", False):
            return getattr(current_user, "username", "") or ""
    except Exception:
        pass
    return "system"

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("wsgi")

# ── Flask app factory ────────────────────────────────────────────────────────
_DIR = os.path.dirname(os.path.abspath(__file__))
_TEMPLATES_DIR = os.path.join(_DIR, "templates")
app = Flask(__name__, static_folder=_DIR, template_folder=_TEMPLATES_DIR)

# Session hardening. Same-origin by default; session cookie signed with SECRET_KEY.
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE") == "1",
    PERMANENT_SESSION_LIFETIME=60 * 60 * 8,   # 8h
)

# ── Phase-2 startup: DB pool + schema + seed users ──────────────────────────
_ALLOW_WITHOUT_DB = os.environ.get("ALLOW_RUN_WITHOUT_DB") == "1"
_db_ready = False
_db_startup_error: Optional[str] = None

try:
    db.init_pool()
    db.init_schema()
    auth.configure_app(app, _DIR)
    _db_ready = True
    log.info("Phase-2 startup complete: DB + auth configured.")
    if str(os.environ.get("CLEAR_DB_ON_STARTUP", "0")).strip() == "1":
        try:
            wiped = db.clear_lead_data()
            log.warning("CLEAR_DB_ON_STARTUP=1 → wiped %s", wiped)
        except Exception as wipe_err:
            log.exception("CLEAR_DB_ON_STARTUP wipe failed: %s", wipe_err)
except Exception as e:
    _db_startup_error = f"{type(e).__name__}: {e}"
    log.error("Phase-2 startup failed: %s", _db_startup_error)
    # Flask-Login still needs a secret key to sign flashes + session messages,
    # even if we can't serve authenticated content. Wire a stub.
    app.secret_key = os.environ.get("SECRET_KEY") or secrets.token_hex(32)
    login_manager.init_app(app)

# ── CORS (scoped) ───────────────────────────────────────────────────────────
# Original wildcard CORS (`*`) is incompatible with credentialed cookies.
# Frontend is served same-origin, so we only send CORS headers on /health.
@app.after_request
def _scoped_cors(response):
    if request.path == "/health":
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET"
    return response


# ── Error handlers ──────────────────────────────────────────────────────────
@app.errorhandler(404)
def _not_found(error):
    if _wants_json():
        return jsonify({"error": "Not found"}), 404
    return "Not found", 404


@app.errorhandler(500)
def _internal(error):
    log.exception("500 Internal Server Error")
    return jsonify({"error": "Internal server error"}), 500


@app.errorhandler(Exception)
def _any_exception(error):
    log.exception("Unhandled exception on %s", request.path)
    return jsonify({"error": str(error)}), 500


def _wants_json() -> bool:
    try:
        return (
            request.is_json
            or "application/json" in (request.headers.get("Accept") or "")
            or request.path.startswith("/api/")
            or request.path.startswith("/generate")
            or request.path.startswith("/status")
        )
    except Exception:
        return False


# ── Job management (unchanged surface, enriched state) ──────────────────────
_jobs: Dict[str, "JobState"] = {}


class JobState:
    def __init__(self):
        self.progress: int = 0
        self.status_text: str = "Starting..."
        self.state: str = "running"
        self.logs: list[str] = []
        self.log_cursor: int = 0
        self.leads: list[dict] = []
        self.top_csv: str = ""
        self.all_csv: str = ""
        self.error: str = ""
        self.pipeline = None
        self.api_usage: Dict[str, int] = {}
        self.summary: Dict[str, Any] = {}
        self.start_time: float = time.time()
        # Phase 2 additions
        self.run_id: Optional[int] = None           # row in run_history
        self.user_id: Optional[int] = None
        self.master_leads_new: int = 0
        self.master_leads_deduped_out: int = 0
        # Phase 2 (2026-05-05): partial CSV path produced on cancel/error.
        # NEVER inserted into master_leads — see _finalize_run state guard.
        self.partial_csv_path: str = ""
        self.partial_csv_text: str = ""
        self.valid_leads_in_progress: int = 0  # live-count from [VALID-LEADS] log lines


# ── Login / logout ──────────────────────────────────────────────────────────
# 2026-06-15: LOGIN RE-ENABLED for the CRM layer. The SPA posts JSON
# {username,password} to /login and reads /whoami; direct browser navigation
# gets the server-rendered form fallback.
@app.route("/login", methods=["GET", "POST"])
def login():
    nxt = request.args.get("next") or (request.form.get("next") if request.form else None) or "/"
    if not nxt.startswith("/") or nxt.startswith("//"):
        nxt = "/"

    if request.method == "GET":
        if getattr(current_user, "is_authenticated", False):
            if _wants_json():
                return jsonify({"status": "ok", "user": current_user.to_dict()})
            return redirect(nxt)
        if _wants_json():
            return jsonify({"authenticated": False, "login_url": "/login"}), 401
        return _render_login()

    # POST — authenticate.
    if request.is_json:
        data = request.get_json(silent=True) or {}
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""
        remember = bool(data.get("remember", True))
    else:
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        remember = bool(request.form.get("remember", True))

    user = None
    try:
        user = verify_credentials(username, password)
    except Exception as e:
        log.warning("login verify error: %s", e)

    if not user:
        if _wants_json():
            return jsonify({"error": "invalid_credentials"}), 401
        return _render_login(error="Invalid username or password", status=401)

    do_login(user, remember=remember)
    try:
        db.AuditRepo.log(user.id, user.username, "login", "")
    except Exception:
        pass
    if _wants_json():
        return jsonify({"status": "ok", "user": user.to_dict()})
    return redirect(nxt)


def _render_login(error: str = "", status: int = 200):
    try:
        html = render_template("login.html", error=error)
    except Exception:
        # Fallback minimal HTML if template missing
        err_html = f"<p style='color:#ff6b6b'>{error}</p>" if error else ""
        html = (
            "<!doctype html><html><head><title>LeadForge Login</title>"
            "<style>body{background:#0a0a0a;color:#fff;font-family:sans-serif;display:flex;"
            "align-items:center;justify-content:center;height:100vh;margin:0}"
            "form{background:#1a1a1a;padding:40px;border-radius:8px;min-width:320px}"
            "input{width:100%;padding:12px;margin:6px 0;background:#222;color:#fff;border:1px solid #333;border-radius:4px}"
            "button{background:#4f9cff;color:#000;padding:12px 24px;border:0;border-radius:4px;"
            "cursor:pointer;width:100%;margin-top:10px;font-weight:600}</style></head>"
            "<body><form method='POST' action='/login'>"
            "<h2>LeadForge</h2>" + err_html +
            "<input name='username' placeholder='Username' autofocus required>"
            "<input name='password' type='password' placeholder='Password' required>"
            "<button type='submit'>Sign in</button></form></body></html>"
        )
    return html, status


@app.route("/logout", methods=["GET", "POST"])
def logout():
    try:
        do_logout()
    except Exception:
        pass
    if _wants_json():
        return jsonify({"status": "logged_out"})
    return redirect("/login")


@app.route("/whoami")
def whoami():
    # When auth is disabled (dev), return a synthetic admin so the SPA opens.
    if getattr(auth, "LOGIN_DISABLED", False):
        return jsonify({
            "id": 0, "username": "guest", "role": "admin", "is_admin": True,
            "is_manager_or_admin": True, "auth": "disabled",
        })
    if getattr(current_user, "is_authenticated", False):
        d = current_user.to_dict()
        d["is_admin"] = current_user.is_admin
        d["is_manager_or_admin"] = current_user.is_manager_or_admin
        return jsonify(d)
    return jsonify({"authenticated": False, "login_url": "/login"}), 401


# ── Public routes ───────────────────────────────────────────────────────────
@app.route("/")
def serve_index():
    # 2026-05-18: LOGIN DISABLED — every visitor sees the SPA directly.
    try:
        return send_from_directory(_DIR, "index.html")
    except Exception as e:
        return f"Error: {e}", 500


@app.route("/health")
def health():
    return jsonify({
        "status": "ok" if _db_ready else "degraded",
        "version": "V5.12-phase2",
        "db_ready": _db_ready,
        "db_error": _db_startup_error,
    })


@app.route("/<path:filename>")
def serve_static(filename):
    safe_ext = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".css", ".js", ".webp"}
    if os.path.splitext(filename)[1].lower() in safe_ext:
        try:
            return send_from_directory(_DIR, filename)
        except Exception:
            return "Not found", 404
    return "Not found", 404


# ── Industries / AU cities ──────────────────────────────────────────────────
@app.route("/industries")
@manager_or_admin_required
def get_industries():
    try:
        from V5 import INDUSTRY_KEYWORDS
        return jsonify({"industries": list(INDUSTRY_KEYWORDS.keys())})
    except Exception:
        return jsonify({"industries": ["Electrician", "Plumber", "Photographer"]})


@app.route("/api/au-cities")
@manager_or_admin_required
def get_au_cities():
    try:
        from cities_au import list_states_payload
        return jsonify(list_states_payload())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Credits dashboard ───────────────────────────────────────────────────────
# Phase 2 (2026-05-05): TTL dropped to 60s. The 5-minute cache made the panel
# feel "frozen" between refreshes — users would refresh and see stale data
# and report "credits not working".
_credits_cache = {"data": None, "timestamp": 0.0}
_CREDITS_CACHE_TTL = 60


def _build_credits_payload(force: bool = False):
    import requests
    from V5 import API_KEYS, LUSHA_PLAN_CREDITS, SEMRUSH_PLAN_TOTAL, _lusha_calls_total

    now = time.time()
    if not force and _credits_cache["data"] and (now - _credits_cache["timestamp"]) < _CREDITS_CACHE_TTL:
        payload = dict(_credits_cache["data"])
        payload["cached"] = True
        return payload

    def _apollo():
        # Phase 2 (2026-05-05): Apollo's /auth/health response shape varies by
        # plan tier. Some plans return plan.credits + usage.credits_used,
        # others nest pools under plan.email_credits etc., and some plans
        # don't expose pools at all (only total). To make the dashboard show
        # something useful in every case, we extract pools defensively from
        # multiple known shapes and ALWAYS surface 4 categories (total /
        # export / phone / email) as flat fields the frontend can render.
        def _empty(err: str = ""):
            return {
                "service": "Apollo", "status": "error" if err else "ok",
                "error": err,
                "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0,
                "searches_remaining": 0,
                "pools": {},
                "export_remaining": 0, "phone_remaining": 0, "email_remaining": 0,
                "export_total": 0, "phone_total": 0, "email_total": 0,
            }
        try:
            r = requests.get(
                "https://api.apollo.io/api/v1/auth/health",
                headers={"X-Api-Key": API_KEYS.get("apollo", "")},
                timeout=10,
            )
            if r.status_code != 200:
                _err = f"HTTP {r.status_code}"
                try:
                    body = r.json()
                    if isinstance(body, dict) and body.get("error"):
                        _err = f"HTTP {r.status_code}: {body['error']}"
                except Exception:
                    pass
                return _empty(err=_err)
            j = r.json() or {}
            plan = j.get("plan", {}) or {}
            usage = j.get("usage", {}) or {}

            # Top-level totals (best-effort fallbacks)
            total = int(plan.get("credits") or plan.get("total_credits") or 10000)
            used = int(usage.get("credits_used") or usage.get("used") or 0)
            remaining = max(0, total - used)

            def _pool(name_variants: tuple, used_variants: tuple):
                """Try each (plan_key, usage_key) shape until one returns data."""
                pt = pu = None
                for k in name_variants:
                    if plan.get(k) is not None:
                        pt = int(plan.get(k) or 0)
                        break
                for k in used_variants:
                    if usage.get(k) is not None:
                        pu = int(usage.get(k) or 0)
                        break
                if pt is None and pu is None:
                    return None
                pt = int(pt or 0)
                pu = int(pu or 0)
                return {"total": pt, "used": pu, "remaining": max(0, pt - pu)}

            pools: Dict[str, Any] = {}
            email_pool = _pool(
                ("email_credits", "emailCredits", "email"),
                ("email_credits_used", "emailCreditsUsed", "email_used"),
            )
            phone_pool = _pool(
                ("mobile_credits", "mobileCredits", "phone_credits", "mobile"),
                ("mobile_credits_used", "mobileCreditsUsed", "phone_credits_used", "mobile_used"),
            )
            export_pool = _pool(
                ("export_credits", "exportCredits", "exports"),
                ("export_credits_used", "exportCreditsUsed", "exports_used"),
            )
            if email_pool: pools["email_credits"] = email_pool
            if phone_pool: pools["mobile_credits"] = phone_pool
            if export_pool: pools["export_credits"] = export_pool

            return {
                "service": "Apollo", "status": "ok",
                "total": total, "used": used, "remaining": remaining,
                "pct_remaining": round(remaining / max(total, 1) * 100, 1),
                "searches_remaining": remaining // 2,
                "pools": pools,
                # Flat fields for compact rendering
                "email_remaining": (email_pool or {}).get("remaining", 0),
                "email_total":     (email_pool or {}).get("total", 0),
                "phone_remaining": (phone_pool or {}).get("remaining", 0),
                "phone_total":     (phone_pool or {}).get("total", 0),
                "export_remaining": (export_pool or {}).get("remaining", 0),
                "export_total":     (export_pool or {}).get("total", 0),
            }
        except Exception as e:
            return _empty(err=str(e))

    def _lusha():
        try:
            r = requests.get(
                "https://api.lusha.com/v2/company",
                headers={"api_key": API_KEYS.get("lusha", "")},
                params={"domain": "example.com"}, timeout=10,
            )
            if r.status_code in (401, 403):
                return {"service": "Lusha", "status": "error", "error": "API key invalid or expired",
                        "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
            total = LUSHA_PLAN_CREDITS
            used = _lusha_calls_total
            remaining = max(0, total - used)
            return {"service": "Lusha", "status": "ok", "total": total, "used": used,
                    "remaining": remaining,
                    "pct_remaining": round(remaining / max(total, 1) * 100, 1),
                    "searches_remaining": remaining // 2,
                    "note": "Locally tracked (resets on server restart)"}
        except Exception as e:
            return {"service": "Lusha", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0,
                    "pct_remaining": 0, "searches_remaining": 0}

    def _semrush():
        try:
            r = requests.get(
                "https://www.semrush.com/users/countapiunits.html",
                params={"key": API_KEYS.get("semrush", "")},
                timeout=10,
            )
            if r.status_code == 200:
                text = r.text.strip()
                try:
                    remaining = int(float(text))
                    total = max(remaining, SEMRUSH_PLAN_TOTAL)
                    used = total - remaining
                    return {"service": "SEMrush", "status": "ok", "total": total, "used": used,
                            "remaining": remaining,
                            "pct_remaining": round(remaining / max(total, 1) * 100, 1),
                            "searches_remaining": remaining // 3}
                except ValueError:
                    return {"service": "SEMrush", "status": "error", "error": text[:100],
                            "total": 0, "used": 0, "remaining": 0,
                            "pct_remaining": 0, "searches_remaining": 0}
            return {"service": "SEMrush", "status": "error", "error": f"HTTP {r.status_code}",
                    "total": 0, "used": 0, "remaining": 0,
                    "pct_remaining": 0, "searches_remaining": 0}
        except Exception as e:
            return {"service": "SEMrush", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0,
                    "pct_remaining": 0, "searches_remaining": 0}

    services = {"apollo": _apollo(), "lusha": _lusha(), "semrush": _semrush()}
    total_searches = sum(r.get("searches_remaining", 0) for r in services.values())
    alerts = []
    for k, r in services.items():
        pct = r.get("pct_remaining", 0)
        if r.get("status") == "error":
            alerts.append({"level": "error", "service": r.get("service", k),
                           "message": f"{r.get('service', k)}: {r.get('error', 'Unknown error')}"})
        elif pct <= 10:
            alerts.append({"level": "critical", "service": r["service"],
                           "message": f"{r['service']} credits critically low ({pct}%)"})
        elif pct <= 25:
            alerts.append({"level": "warning", "service": r["service"],
                           "message": f"{r['service']} credits running low ({pct}%)"})
    payload = {"services": services, "total_searches_remaining": total_searches,
               "timestamp": time.time(), "cached": False, "alerts": alerts}
    _credits_cache["data"] = payload
    _credits_cache["timestamp"] = now
    return payload


@app.route("/api/credits")
@manager_or_admin_required
def get_credits():
    try:
        return jsonify(_build_credits_payload(force=False))
    except Exception as e:
        return jsonify({"error": str(e), "services": {}, "alerts": []}), 500


@app.route("/api/credits/refresh", methods=["POST"])
@manager_or_admin_required
def refresh_credits():
    try:
        return jsonify(_build_credits_payload(force=True))
    except Exception as e:
        return jsonify({"error": str(e), "services": {}, "alerts": []}), 500


# ── Master-DB stats (dashboard panel) ───────────────────────────────────────
_master_stats_cache = {"data": None, "timestamp": 0.0}
_MASTER_STATS_TTL = 60


@app.route("/api/master-stats")
@manager_or_admin_required
def master_stats():
    now = time.time()
    cached = _master_stats_cache["data"]
    if cached and (now - _master_stats_cache["timestamp"]) < _MASTER_STATS_TTL:
        return jsonify(dict(cached, cached=True))
    try:
        total = db.MasterLeadRepo.total_count()
        payload = {"total_leads": total, "timestamp": now, "cached": False}
        _master_stats_cache["data"] = payload
        _master_stats_cache["timestamp"] = now
        return jsonify(payload)
    except db.DBUnavailable as e:
        return jsonify({"total_leads": 0, "error": "db_unavailable", "detail": str(e)}), 503
    except Exception as e:
        return jsonify({"total_leads": 0, "error": str(e)}), 500


# ── Admin routes ────────────────────────────────────────────────────────────
@app.route("/admin")
def admin_home():
    # 2026-05-18: LOGIN DISABLED — admin page is openly accessible.
    try:
        return render_template("admin.html")
    except Exception as e:
        return f"Error loading admin template: {e}", 500


@app.route("/admin/users")
@admin_required
def admin_users():
    try:
        users = db.UserRepo.list_all()
        for u in users:
            if u.get("created_at"):
                u["created_at"] = u["created_at"].isoformat() if hasattr(u["created_at"], "isoformat") else str(u["created_at"])
            if u.get("last_login_at"):
                u["last_login_at"] = u["last_login_at"].isoformat() if hasattr(u["last_login_at"], "isoformat") else str(u["last_login_at"])
        return jsonify({"users": users})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/admin/runs")
@admin_required
def admin_runs():
    limit = int(request.args.get("limit", 50) or 50)
    try:
        runs = db.RunHistoryRepo.recent(limit=min(limit, 200))
        for r in runs:
            for k in ("started_at", "finished_at"):
                if r.get(k) and hasattr(r[k], "isoformat"):
                    r[k] = r[k].isoformat()
            if isinstance(r.get("api_usage_json"), (bytes, str)):
                try:
                    r["api_usage_json"] = json.loads(r["api_usage_json"])
                except Exception:
                    pass
        return jsonify({"runs": runs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── CRM layer: users, allocation, contacted, mobile (2026-06-15) ─────────────

def _fmt_user(u: Dict[str, Any]) -> Dict[str, Any]:
    for k in ("created_at", "last_login_at"):
        if u.get(k) and hasattr(u[k], "isoformat"):
            u[k] = u[k].isoformat()
    u.pop("password_hash", None)
    return u


@app.route("/api/users", methods=["GET"])
@manager_or_admin_required
def api_users_list():
    try:
        return jsonify({"users": [_fmt_user(u) for u in db.UserRepo.list_all()]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _is_admin_user(user_id: int) -> bool:
    try:
        u = db.UserRepo.get_by_id(int(user_id))
        return bool(u and u.get("role") == "admin")
    except Exception:
        return False


@app.route("/api/users", methods=["POST"])
@manager_or_admin_required
def api_users_create():
    # Managers may manage BDEs/managers but NOT create admins (no privilege escalation).
    from werkzeug.security import generate_password_hash
    d = request.get_json(silent=True) or {}
    username = (d.get("username") or "").strip()
    role = (d.get("role") or "bde").strip()
    password = d.get("password") or ""
    email = (d.get("email") or f"{username}@leadforge.local").strip()
    full_name = (d.get("full_name") or "").strip()
    if not username or not password:
        return jsonify({"error": "username and password required"}), 400
    if role not in db.UserRepo.VALID_ROLES:
        return jsonify({"error": f"invalid role: {role}"}), 400
    if role == "admin" and _current_role() != "admin":
        return jsonify({"error": "only an admin can create admin accounts"}), 403
    try:
        pw_hash = generate_password_hash(password, method="pbkdf2:sha256:260000")
        uid, created = db.UserRepo.create_if_absent(username, email, pw_hash, role)
        if not created:
            return jsonify({"error": "username already exists"}), 409
        db.UserRepo.set_must_change_pw(uid, True)  # force change on first login
        if full_name:
            db.UserRepo.update_profile(uid, full_name=full_name)
        db.AuditRepo.log(_current_uid(), _current_username(), "user_create", f"{username} ({role})")
        return jsonify({"ok": True, "id": uid})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users/<int:user_id>", methods=["PATCH"])
@manager_or_admin_required
def api_users_update(user_id):
    d = request.get_json(silent=True) or {}
    # A manager cannot edit an admin account, nor promote anyone to admin.
    if _current_role() != "admin":
        if _is_admin_user(user_id) or d.get("role") == "admin":
            return jsonify({"error": "only an admin can manage admin accounts"}), 403
    try:
        if "role" in d:
            db.UserRepo.set_role(user_id, d["role"])
        if "is_active" in d:
            db.UserRepo.set_active(user_id, bool(d["is_active"]))
        if "full_name" in d or "email" in d:
            db.UserRepo.update_profile(user_id, full_name=d.get("full_name"), email=d.get("email"))
        db.AuditRepo.log(_current_uid(), _current_username(), "user_update", f"uid={user_id} {d}")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/users/<int:user_id>/password", methods=["POST"])
@manager_or_admin_required
def api_users_password(user_id):
    from werkzeug.security import generate_password_hash
    if _current_role() != "admin" and _is_admin_user(user_id):
        return jsonify({"error": "only an admin can reset an admin's password"}), 403
    d = request.get_json(silent=True) or {}
    pw = d.get("password") or ""
    if len(pw) < 6:
        return jsonify({"error": "password too short"}), 400
    try:
        db.UserRepo.set_password(user_id, generate_password_hash(pw, method="pbkdf2:sha256:260000"))
        db.UserRepo.set_must_change_pw(user_id, True)  # admin-set pw must be changed by the user
        db.AuditRepo.log(_current_uid(), _current_username(), "user_password_reset", f"uid={user_id}")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/allocation/bdes", methods=["GET"])
@manager_or_admin_required
def api_allocation_bdes():
    try:
        bdes = db.UserRepo.list_by_role("bde", active_only=False)
        stats = db.LeadAllocationRepo.bde_stats()
        out = []
        for b in bdes:
            st = stats.get(int(b["id"]), {})
            out.append({
                "id": b["id"], "username": b["username"],
                "full_name": b.get("full_name") or "", "is_active": bool(b.get("is_active")),
                "mobile_e164": b.get("mobile_e164") or "",
                "assigned_count": int(st.get("total", 0)),
                "contacted_count": int(st.get("contacted", 0)),
                "secured_count": int(st.get("secured", 0)),
                "call_count": int(st.get("calls", 0)),
                "last_call_at": st.get("last_call_at"),
            })
        return jsonify({"bdes": out})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/allocation/bde/<int:bde_id>/leads", methods=["GET"])
@manager_or_admin_required
def api_allocation_bde_leads(bde_id):
    """All leads currently allocated to one BDE — drives the expandable card +
    its drag-and-drop block. Capped at 500 (scrollable in the UI)."""
    try:
        rows, total = db.LeadAllocationRepo.list_for_bde(bde_id, page=1, page_size=500)
        return jsonify({"leads": rows, "total": total})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<int:lead_id>/secured", methods=["POST"])
@login_required_json
def api_lead_secured(lead_id):
    """Mark a lead 'secured' (deal won). Assigned BDE or any manager/admin."""
    role = _current_role()
    if role == "bde":
        if _lead_assigned_bde(lead_id) != _current_uid():
            return jsonify({"error": "forbidden"}), 403
    elif role not in ("admin", "manager"):
        return jsonify({"error": "forbidden"}), 403
    try:
        db.LeadAllocationRepo.mark_secured(lead_id, _current_uid())
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<int:lead_id>/alloc-status", methods=["POST"])
@login_required_json
def api_lead_alloc_status(lead_id):
    """Set the funnel stage explicitly — drives the toggle/revert UI so a BDE
    can un-contact / un-secure a lead clicked by mistake. Body: {status}."""
    role = _current_role()
    if role == "bde":
        if _lead_assigned_bde(lead_id) != _current_uid():
            return jsonify({"error": "forbidden"}), 403
    elif role not in ("admin", "manager"):
        return jsonify({"error": "forbidden"}), 403
    status = (request.get_json(silent=True) or {}).get("status", "")
    if status not in ("assigned", "contacted", "secured"):
        return jsonify({"error": "invalid status"}), 400
    try:
        db.LeadAllocationRepo.set_alloc_status(lead_id, status, _current_uid())
        db.AuditRepo.log(_current_uid(), _current_username(), "lead_status_"+status, "", lead_id)
        return jsonify({"ok": True, "status": status})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _lead_access_ok(lead_id: int) -> bool:
    role = _current_role()
    if role in ("admin", "manager"):
        return True
    return role == "bde" and _lead_assigned_bde(lead_id) == _current_uid()


@app.route("/api/lead/<int:lead_id>", methods=["PATCH"])
@manager_or_admin_required
def api_lead_edit(lead_id):
    """Manual edit of a lead's columns (admin/manager). Every lead is editable;
    future lead-searches also fill blanks automatically via the upsert."""
    d = request.get_json(silent=True) or {}
    try:
        n = db.MasterLeadRepo.update_fields(lead_id, d)
        db.AuditRepo.log(_current_uid(), _current_username(), "lead_edit",
                         ",".join(sorted(d.keys()))[:200], lead_id)
        return jsonify({"ok": True, "updated": n})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lead/<int:lead_id>/notes", methods=["POST"])
@login_required_json
def api_lead_notes(lead_id):
    if not _lead_access_ok(lead_id):
        return jsonify({"error": "forbidden"}), 403
    notes = (request.get_json(silent=True) or {}).get("notes", "")
    try:
        db.MasterLeadRepo.set_followup(lead_id, None, None, notes)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lead/<int:lead_id>/followup", methods=["POST"])
@login_required_json
def api_lead_followup(lead_id):
    if not _lead_access_ok(lead_id):
        return jsonify({"error": "forbidden"}), 403
    d = request.get_json(silent=True) or {}
    outcome = d.get("outcome")
    followup_at = (d.get("followup_at") or "").replace("T", " ").strip() or None
    notes = d.get("notes")
    try:
        db.MasterLeadRepo.set_followup(lead_id, outcome, followup_at, notes)
        db.AuditRepo.log(_current_uid(), _current_username(), "lead_followup",
                         f"outcome={outcome} at={followup_at}", lead_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lead/<int:lead_id>/touch", methods=["POST"])
@login_required_json
def api_lead_touch(lead_id):
    """Log a click-to-contact touch (call/email/whatsapp) for audit + activity."""
    if not _lead_access_ok(lead_id):
        return jsonify({"error": "forbidden"}), 403
    channel = (request.get_json(silent=True) or {}).get("channel", "call")
    try:
        db.AuditRepo.log(_current_uid(), _current_username(), "touch_" + channel, "", lead_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Self-service (any authenticated user) ────────────────────────────────────
@app.route("/api/me/password", methods=["POST"])
@login_required_json
def api_me_password():
    from werkzeug.security import generate_password_hash
    uid = _current_uid()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401
    pw = (request.get_json(silent=True) or {}).get("password", "")
    if len(pw) < 6:
        return jsonify({"error": "password must be at least 6 characters"}), 400
    try:
        db.UserRepo.set_password(uid, generate_password_hash(pw, method="pbkdf2:sha256:260000"))
        db.AuditRepo.log(uid, _current_username(), "self_password_change", "")
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/me/profile", methods=["POST"])
@login_required_json
def api_me_profile():
    uid = _current_uid()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401
    d = request.get_json(silent=True) or {}
    try:
        db.UserRepo.update_profile(uid, full_name=d.get("full_name"),
                                   mobile_e164=d.get("mobile_e164"),
                                   threecx_ext=d.get("threecx_ext"))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/3cx/call", methods=["POST"])
@login_required_json
def api_3cx_call():
    """Click-to-call: ring the current user's 3CX extension, then dial the lead.
    Inert (clear reason) until 3CX creds are set on Railway."""
    lead_id = (request.get_json(silent=True) or {}).get("lead_id")
    try:
        lead_id = int(lead_id)
    except (TypeError, ValueError):
        return jsonify({"error": "lead_id required"}), 400
    if not _lead_access_ok(lead_id):
        return jsonify({"error": "forbidden"}), 403
    try:
        phone = ""
        with db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT phone_e164 FROM master_leads WHERE id=%s", (lead_id,))
                phone = ((cur.fetchone() or {}).get("phone_e164")) or ""
        ext = (db.UserRepo.get_by_id(_current_uid()) or {}).get("threecx_ext") or ""
        import threecx
        res = threecx.make_call(ext, phone)
        try:
            db.AuditRepo.log(_current_uid(), _current_username(),
                             "3cx_call" if res.get("ok") else "3cx_call_failed",
                             (res.get("detail") or res.get("reason") or "")[:160], lead_id)
        except Exception:
            pass
        if res.get("ok"):
            return jsonify(res)
        return jsonify({**res, "error": res.get("reason")}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/me/followups", methods=["GET"])
@login_required_json
def api_me_followups():
    """Due/overdue follow-ups: BDE sees their own; manager/admin see all."""
    try:
        bde = _current_uid() if _current_role() == "bde" else None
        return jsonify({"followups": db.MasterLeadRepo.due_followups(bde_id=bde)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/audit", methods=["GET"])
@manager_or_admin_required
def api_audit():
    try:
        return jsonify({"events": db.AuditRepo.recent(limit=int(request.args.get("limit", 120) or 120))})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/stats/kpis", methods=["GET"])
@manager_or_admin_required
def api_stats_kpis():
    try:
        return jsonify(db.MasterLeadRepo.kpi_overview())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Crawler / enrichment controls (admin/manager) ────────────────────────────
@app.route("/api/crawler/pause", methods=["POST"])
@manager_or_admin_required
def api_crawler_pause():
    import enrichment_worker; enrichment_worker.pause()
    return jsonify({"ok": True, "paused": True})


@app.route("/api/crawler/resume", methods=["POST"])
@manager_or_admin_required
def api_crawler_resume():
    import enrichment_worker; enrichment_worker.resume()
    return jsonify({"ok": True, "paused": False})


@app.route("/api/crawler/retry-errored", methods=["POST"])
@manager_or_admin_required
def api_crawler_retry():
    try:
        n = db.LeadEnrichmentRepo.retry_errored()
        return jsonify({"ok": True, "requeued": n})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/crawler/openai-test", methods=["POST", "GET"])
@manager_or_admin_required
def api_crawler_openai_test():
    """Live OpenAI key probe FROM INSIDE this deployment — reports whether the
    env var is present (+ masked tail), any stray quotes/whitespace, and the exact
    API result. The decisive Railway diagnostic for 'AI summaries not generating'."""
    try:
        import enrichment_worker
        return jsonify({"ok": True, "probe": enrichment_worker.openai_health_probe()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/crawler/reanalyze", methods=["POST"])
@manager_or_admin_required
def api_crawler_reanalyze():
    """Re-run the AI cheat-sheet for leads whose AI failed earlier (e.g. a bad
    OpenAI key), reusing already-crawled data (no re-crawl). Use after fixing
    OPENAI_API_KEY to backfill the whole database cheaply."""
    try:
        import enrichment_worker
        ids = db.LeadEnrichmentRepo.failed_ai_ids(limit=5000)
        enrichment_worker.reanalyze(ids)
        return jsonify({"ok": True, "count": len(ids),
                        "message": f"Re-analysing {len(ids)} lead(s) in the background — refresh shortly."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/crawler/leads", methods=["GET"])
@manager_or_admin_required
def api_crawler_leads():
    """Drill-down for the Enrichment panel: the domains in a given crawl bucket
    (pending/crawling/done/error/total) with their details."""
    status = (request.args.get("status") or "total").strip().lower()
    try:
        limit = min(max(int(request.args.get("limit", 250)), 1), 1000)
    except (TypeError, ValueError):
        limit = 250
    try:
        rows = db.LeadEnrichmentRepo.list_by_status(status, limit=limit)
        return jsonify({"status": status, "count": len(rows), "leads": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/allocation/auto", methods=["POST"])
@manager_or_admin_required
def api_allocation_auto():
    d = request.get_json(silent=True) or {}
    bde_ids = [int(x) for x in (d.get("bde_ids") or [])]
    industry = (d.get("industry") or "").strip() or None
    if not bde_ids:
        return jsonify({"error": "no active BDEs selected"}), 400
    # per_bde: allocate exactly N to EACH active BDE. None/0 = allocate the whole
    # remaining pool evenly.
    try:
        per_bde = int(d.get("per_bde") or 0)
    except (TypeError, ValueError):
        per_bde = 0
    try:
        if per_bde > 0:
            needed = per_bde * len(bde_ids)
            available = db.LeadAllocationRepo.unassigned_count(industry)
            if available < needed:
                return jsonify({
                    "error": "insufficient",
                    "available": available, "needed": needed, "per_bde": per_bde,
                    "message": (f"Only {available} unassigned lead(s) available, but "
                                f"{per_bde} × {len(bde_ids)} BDE(s) = {needed} needed. "
                                f"Reduce the per-BDE number (max "
                                f"{available // len(bde_ids)} each) or add more leads."),
                }), 400
            assigned = db.LeadAllocationRepo.auto_distribute(
                bde_ids, _current_uid(), industry=industry, limit=needed)
        else:
            assigned = db.LeadAllocationRepo.auto_distribute(
                bde_ids, _current_uid(), industry=industry)
        return jsonify({"ok": True, "assigned": assigned, "total": sum(assigned.values())})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<int:lead_id>/unassign", methods=["POST"])
@manager_or_admin_required
def api_lead_unassign(lead_id):
    """Deallocate a lead from its BDE — back into the database pool."""
    try:
        db.LeadAllocationRepo.unassign(lead_id, _current_uid())
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/<int:lead_id>/reassign", methods=["POST"])
@manager_or_admin_required
def api_lead_reassign(lead_id):
    d = request.get_json(silent=True) or {}
    bde_id = d.get("bde_id")
    if not bde_id:
        return jsonify({"error": "bde_id required"}), 400
    try:
        db.LeadAllocationRepo.assign(lead_id, int(bde_id), _current_uid())
        db.AuditRepo.log(_current_uid(), _current_username(), "lead_reassign", f"→bde {bde_id}", lead_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _lead_assigned_bde(lead_id: int) -> Optional[int]:
    with db.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT assigned_bde_id FROM master_leads WHERE id=%s", (lead_id,))
            row = cur.fetchone()
            return int(row["assigned_bde_id"]) if row and row.get("assigned_bde_id") else None


@app.route("/api/leads/<int:lead_id>/contacted", methods=["POST"])
@login_required_json
def api_lead_contacted(lead_id):
    # The assigned BDE, or any manager/admin, may mark a lead contacted.
    role = _current_role()
    if role == "bde":
        if _lead_assigned_bde(lead_id) != _current_uid():
            return jsonify({"error": "forbidden"}), 403
    elif role not in ("admin", "manager"):
        return jsonify({"error": "forbidden"}), 403
    try:
        db.LeadAllocationRepo.mark_contacted(lead_id, _current_uid())
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/allocation/contacted-feed", methods=["GET"])
@manager_or_admin_required
def api_contacted_feed():
    try:
        rows = db.LeadAllocationRepo.recent_contacted(limit=int(request.args.get("limit", 50) or 50))
        for r in rows:
            if r.get("contacted_at") and hasattr(r["contacted_at"], "isoformat"):
                r["contacted_at"] = r["contacted_at"].isoformat()
        return jsonify({"contacted": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/leads/manual-add", methods=["POST"])
@manager_or_admin_required
def api_lead_manual_add():
    d = request.get_json(silent=True) or {}
    company = (d.get("company") or "").strip()
    domain = root_domain(d.get("domain") or "")
    name = (d.get("name") or "").strip()
    if not domain:
        return jsonify({"error": "domain required"}), 400
    nn = normalize_master_key(name or company or domain)
    try:
        lead_id = db.LeadAllocationRepo.manual_add(
            normalized_name=nn, root_domain=domain,
            display_name=name or company, company_name=company or domain,
            phone=(d.get("phone") or "").strip(), email=(d.get("email") or "").strip(),
            industry=(d.get("industry") or "Manual").strip(),
            country=(d.get("country") or "AU").strip(),
            actor_id=_current_uid(),
            bde_id=int(d["bde_id"]) if d.get("bde_id") else None,
        )
        return jsonify({"ok": True, "id": lead_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lead/<int:lead_id>/full", methods=["GET"])
@login_required_json
def api_lead_full(lead_id):
    """Everything for the individual lead page: master row + website-crawl +
    Apollo free data + AI cheat-sheet (+ call history in Phase 3). BDE accounts
    can only open their OWN allocated leads."""
    try:
        row = db.LeadEnrichmentRepo.get_full(lead_id)
        if not row:
            return jsonify({"error": "not_found"}), 404
        if _current_role() == "bde":
            if row.get("assigned_bde_id") != _current_uid():
                return jsonify({"error": "forbidden"}), 403
        row["lead_code"] = "LF-" + str(lead_id).zfill(6)
        return jsonify({"lead": row})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/lead/<int:lead_id>/recrawl", methods=["POST"])
@manager_or_admin_required
def api_lead_recrawl(lead_id):
    try:
        db.LeadEnrichmentRepo.recrawl(lead_id)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/crawler/status", methods=["GET"])
@manager_or_admin_required
def api_crawler_status():
    try:
        import enrichment_worker
        return jsonify({"queue": db.LeadEnrichmentRepo.status_counts(),
                        "worker": enrichment_worker.get_stats()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/3cx/webhook", methods=["POST"])
def threecx_webhook():
    """Public server-to-server receiver for 3CX call events. Secured by the
    shared THREECX_WEBHOOK_SECRET (header X-3CX-Secret or ?secret=) when set."""
    secret_env = os.environ.get("THREECX_WEBHOOK_SECRET", "")
    if secret_env:
        provided = request.headers.get("X-3CX-Secret") or request.args.get("secret") or ""
        if not hmac.compare_digest(provided.encode("utf-8"), secret_env.encode("utf-8")):
            return jsonify({"error": "forbidden"}), 403
    else:
        log.warning("3CX webhook hit with no THREECX_WEBHOOK_SECRET configured — accepting.")
    payload = request.get_json(silent=True)
    if payload is None and request.form:
        payload = request.form.to_dict()
    if not payload:
        return jsonify({"error": "empty payload"}), 400
    try:
        import threecx
        call_id, call = threecx.ingest(payload)
        return jsonify({"ok": True, "call_id": call_id, "lead_id": call.get("lead_id"),
                        "bde_user_id": call.get("bde_user_id")})
    except Exception as e:
        log.error("3cx webhook error: %s", e)
        return jsonify({"error": str(e)}), 500


@app.route("/api/lead/<int:lead_id>/calls", methods=["GET"])
@login_required_json
def api_lead_calls(lead_id):
    if _current_role() == "bde" and _lead_assigned_bde(lead_id) != _current_uid():
        return jsonify({"error": "forbidden"}), 403
    try:
        calls = db.LeadCallsRepo.list_for_lead(lead_id)
        return jsonify({"calls": calls, "count": len(calls)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/3cx/events", methods=["GET"])
@login_required_json
def api_3cx_events():
    """Cursor poll for live call events (screen-pop / toast). Returns calls with
    id > since + the new cursor. BDEs are scoped server-side to their own calls;
    admin/manager see all. The frontend primes the cursor on first poll so the
    page-load doesn't replay history as a toast storm."""
    try:
        since = int(request.args.get("since", 0) or 0)
    except (TypeError, ValueError):
        since = 0
    bde = _current_uid() if _current_role() == "bde" else None
    try:
        # since<=0 = prime: jump straight to the newest id (no history replay).
        if since <= 0:
            return jsonify({"events": [], "cursor": db.LeadCallsRepo.latest_call_id(bde)})
        rows = db.LeadCallsRepo.events_since(since, bde_id=bde, limit=50)
        cursor = rows[-1]["id"] if rows else since
        return jsonify({"events": rows, "cursor": cursor})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/3cx/test-call", methods=["POST"])
@admin_required
def api_3cx_test_call():
    """Manually inject a call event (for testing the pipeline + UI without a
    live PBX). Same normalization/matching path as the webhook."""
    payload = request.get_json(silent=True) or {}
    try:
        import threecx
        call_id, call = threecx.ingest(payload, transcribe_audio=bool(payload.get("recording_url")))
        return jsonify({"ok": True, "call_id": call_id,
                        "matched_lead": call.get("lead_id"),
                        "matched_bde": call.get("bde_user_id")})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/3cx/sync", methods=["POST"])
@admin_required
def api_3cx_sync():
    try:
        import threecx
        limit = int((request.get_json(silent=True) or {}).get("limit", 100))
        return jsonify(threecx.pull_recent_calls(limit=limit))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/3cx/status", methods=["GET", "POST"])
@manager_or_admin_required
def api_3cx_status():
    """Live 3CX connectivity check from inside the deployment: PBX reachable,
    OAuth token obtainable, and which scopes (XAPI / Call Control) are enabled."""
    try:
        import threecx
        return jsonify(threecx.test_connection())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/me/mobile", methods=["POST"])
@login_required_json
def api_me_mobile():
    d = request.get_json(silent=True) or {}
    mobile = (d.get("mobile_e164") or d.get("mobile") or "").strip()
    uid = _current_uid()
    if uid is None:
        return jsonify({"error": "not logged in"}), 401
    try:
        db.UserRepo.set_mobile(uid, mobile)
        return jsonify({"ok": True, "mobile_e164": mobile})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── DB viewer endpoints ──────────────────────────────────────────────────────
@app.route("/api/db/debug")
@manager_or_admin_required
def db_debug():
    """Diagnostic: DB connection state + row counts. Not cached."""
    info: Dict[str, Any] = {
        "db_ready": _db_ready,
        "db_startup_error": _db_startup_error,
        "allow_without_db": _ALLOW_WITHOUT_DB,
    }
    if _db_ready:
        try:
            info["master_leads_count"] = db.MasterLeadRepo.total_count()
            info["user_count"] = db.UserRepo.count()
            # Quick run_history count
            with db.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) AS c FROM run_history")
                    row = cur.fetchone()
                    info["run_history_count"] = int(row["c"]) if row else 0
        except Exception as e:
            info["query_error"] = str(e)
    return jsonify(info)


@app.route("/api/db/clear-all", methods=["POST"])
@admin_required
def db_clear_all():
    """Wipes master_leads + run_history (users preserved). Admin-friendly reset."""
    try:
        deleted = db.clear_lead_data()
        return jsonify({"ok": True, "deleted": deleted})
    except db.DBUnavailable as e:
        return jsonify({"error": "db_unavailable", "detail": str(e)}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/db/master-leads")
@login_required_json
def db_master_leads():
    """Paginated master_leads viewer. Query params: page (1-based), page_size (max 200).
    2026-06-15: BDE accounts are scoped server-side to their OWN allocated leads
    (non-bypassable); ADMIN/MANAGER see everything."""
    page = int(request.args.get("page", 1) or 1)
    page_size = int(request.args.get("page_size", 50) or 50)
    if not _db_ready:
        return jsonify({"error": "db_unavailable",
                        "detail": _db_startup_error or "MySQL not configured on this host"}), 503
    try:
        if _current_role() == "bde":
            uid = _current_uid()
            rows, total = db.LeadAllocationRepo.list_for_bde(uid, page=page, page_size=page_size)
            for r in rows:
                for k in ("first_seen_at", "assigned_at", "contacted_at"):
                    if r.get(k) and hasattr(r[k], "isoformat"):
                        r[k] = r[k].isoformat()
            return jsonify({
                "rows": rows, "total": total, "page": page, "page_size": page_size,
                "pages": max(1, (total + page_size - 1) // page_size), "scope": "bde",
            })
        # ADMIN/MANAGER: search + filter (q / industry / status / bde_id).
        q = (request.args.get("q") or "").strip() or None
        industry = (request.args.get("industry") or "").strip() or None
        status = (request.args.get("status") or "").strip() or None
        bde_id = request.args.get("bde_id") or None
        rows, total = db.MasterLeadRepo.search_page(
            q=q, industry=industry, status=status, bde_id=bde_id,
            page=page, page_size=page_size)
        return jsonify({
            "rows": rows, "total": total, "page": page, "page_size": page_size,
            "pages": max(1, (total + page_size - 1) // page_size),
        })
    except db.DBUnavailable as e:
        return jsonify({"error": "db_unavailable", "detail": str(e)}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/db/industries")
@manager_or_admin_required
def db_industries():
    try:
        return jsonify({"industries": db.MasterLeadRepo.distinct_industries()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/db/master-leads.csv")
@manager_or_admin_required
def db_master_leads_csv():
    """CSV export of the (optionally filtered) lead set."""
    import io as _io
    q = (request.args.get("q") or "").strip() or None
    industry = (request.args.get("industry") or "").strip() or None
    status = (request.args.get("status") or "").strip() or None
    bde_id = request.args.get("bde_id") or None
    try:
        rows = db.MasterLeadRepo.export_rows(q=q, industry=industry, status=status, bde_id=bde_id)
        cols = ["id", "display_name", "company_name", "root_domain", "role", "phone_e164",
                "primary_email", "industry", "country", "revenue", "alloc_status",
                "assigned_bde_username", "outcome", "followup_at", "first_seen_at"]
        buf = _io.StringIO()
        w = _csv.writer(buf)
        w.writerow(["Lead ID"] + cols[1:])
        for r in rows:
            w.writerow([("LF-" + str(r.get("id")).zfill(6))] + [r.get(c, "") for c in cols[1:]])
        from flask import Response
        return Response(buf.getvalue(), mimetype="text/csv",
                        headers={"Content-Disposition": "attachment; filename=leads_export.csv"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/db/run-history")
@manager_or_admin_required
def db_run_history():
    """Paginated run_history viewer. Query params: page (1-based), page_size (max 200)."""
    page = int(request.args.get("page", 1) or 1)
    page_size = int(request.args.get("page_size", 50) or 50)
    if not _db_ready:
        return jsonify({"error": "db_unavailable",
                        "detail": _db_startup_error or "MySQL not configured on this host"}), 503
    try:
        rows, total = db.RunHistoryRepo.list_page(page=page, page_size=page_size)
        for r in rows:
            for k in ("started_at", "finished_at"):
                if r.get(k) and hasattr(r[k], "isoformat"):
                    r[k] = r[k].isoformat()
            # DECIMAL columns → float (JSON-safe).
            for k in ("cost_usd", "cost_per_lead_usd", "min_cpc"):
                if r.get(k) is not None:
                    try:
                        r[k] = float(r[k])
                    except (TypeError, ValueError):
                        r[k] = None
            if isinstance(r.get("api_usage_json"), (bytes, str)):
                try:
                    r["api_usage_json"] = json.loads(r["api_usage_json"])
                except Exception:
                    pass
        return jsonify({
            "rows": rows,
            "total": total,
            "page": page,
            "page_size": page_size,
            "pages": max(1, (total + page_size - 1) // page_size),
        })
    except db.DBUnavailable as e:
        return jsonify({"error": "db_unavailable", "detail": str(e)}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── 2026-06-08: API pricing config (Cost / Pricing tab) ─────────────────────
# NOTE: this is the DEPLOYED app (gunicorn wsgi:app). The matching routes also
# exist in V5.py:main_web for local `python V5.py` runs — keep both in sync.
# No login decorator: login is disabled app-wide and pricing is non-sensitive.
@app.route("/api/pricing", methods=["GET"])
@manager_or_admin_required
def api_get_pricing():
    try:
        import pricing as _pr
        cfg = _pr.load_pricing()
        return jsonify({
            "pricing": cfg,
            "line_items": list(_pr.LINE_ITEMS),
            "unit_prices": _pr.unit_prices(cfg),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/pricing", methods=["POST"])
@admin_required
def api_save_pricing():
    try:
        import pricing as _pr
        body = request.get_json(silent=True) or {}
        cfg = body.get("pricing") if isinstance(body.get("pricing"), dict) else body
        saved = _pr.save_pricing(cfg)
        return jsonify({"ok": True, "pricing": saved, "unit_prices": _pr.unit_prices(saved)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ── Shared run-execution helper ─────────────────────────────────────────────
def _company_from_domain(domain: str) -> str:
    """Human company name derived from a domain — so leads never land in the DB
    with a blank company (the old cause of 'empty' rows for paid-only stubs)."""
    s = (domain or "").split(".")[0].replace("-", " ").replace("_", " ").strip()
    return s.title() if s else ""


def _pipeline_leads_to_master_rows(
    pipeline, industry: str, country: str, run_id: int
) -> list[dict]:
    rows: list[dict] = []
    for ld in getattr(pipeline, "leads", []) or []:
        nn = normalize_master_key(ld.get("name") or "")
        rd = root_domain(ld.get("domain") or "")
        if not (nn and rd):
            continue
        _company = ld.get("company") or _company_from_domain(rd)
        ts_raw = (ld.get("_domain_source") or "").strip().lower()
        if ts_raw in ("paid", "organic", "competitor", "secondary"):
            ts = ts_raw
        elif ts_raw:
            ts = "paid"
        else:
            ts = "paid"
        rows.append({
            "normalized_name": nn,
            "root_domain": rd,
            "display_name": ld.get("name") or _company or None,
            "company_name": _company or None,
            "role": ld.get("role") or None,
            "phone_e164": ld.get("phone") or None,
            "primary_email": ld.get("email") or None,
            "email_type": (ld.get("email_type") or ld.get("_email_type") or "") or None,
            "linkedin_url": ld.get("_linkedin_url") or None,
            "traffic_source": ts,
            "organic_traffic":  ld.get("_organic_traffic", 0),
            "paid_traffic":     ld.get("_paid_traffic", 0),
            "organic_keywords": ld.get("_organic_keywords", 0),
            "paid_keywords":    ld.get("_paid_keywords", 0),
            "revenue":          ld.get("_revenue", 0),
            "payload_json": {
                "name": ld.get("name"),
                "company": ld.get("company"),
                "domain": ld.get("domain"),
                "role": ld.get("role"),
                "phone": ld.get("phone"),
                "email": ld.get("email"),
                "industry": industry,
                "country": country,
                "lead_score": ld.get("lead_score"),
                "founder_verified": bool(ld.get("_founder_verified")),
                "organic_traffic":  ld.get("_organic_traffic", 0),
                "paid_traffic":     ld.get("_paid_traffic", 0),
                "organic_keywords": ld.get("_organic_keywords", 0),
                "paid_keywords":    ld.get("_paid_keywords", 0),
                "revenue":          ld.get("_revenue", 0),
            },
        })
    return rows


def _finalize_run(
    job: JobState,
    pipeline,
    industry: str,
    country: str,
    state: str,
    error_text: Optional[str] = None,
) -> None:
    """Update run_history + (on 'done') INSERT IGNORE new master_leads rows."""
    if job.run_id is None:
        log.warning(
            "_finalize_run: job.run_id is None for state=%s (db_ready=%s, allow_without=%s) — "
            "run_history and master_leads will NOT be updated for this run.",
            state, _db_ready, _ALLOW_WITHOUT_DB,
        )
        return
    log.info("_finalize_run: run_id=%s state=%s industry=%s", job.run_id, state, industry)
    try:
        api_usage = dict(getattr(pipeline, "_api_counter", {}) or {})
        duration = max(0, int(time.time() - job.start_time))
        # 2026-06-08: freeze this run's dollar cost using the saved pricing.
        # Reuses V5's shared helper so the number matches the local app exactly.
        try:
            from V5 import _v5_run_cost as _v5_run_cost_fn
            _cost_usage, _run_cost_usd, _run_cpl_usd, _cost_per_item = _v5_run_cost_fn(pipeline)
        except Exception:
            _cost_usage, _run_cost_usd, _run_cpl_usd, _cost_per_item = {}, 0.0, 0.0, {}
        api_usage["_cost_usage"]        = _cost_usage
        api_usage["_cost_per_item_usd"] = _cost_per_item
        api_usage["_cost_total_usd"]    = _run_cost_usd
        api_usage["_cost_per_lead_usd"] = _run_cpl_usd
        new_inserted = 0
        if state == "done":
            try:
                rows = _pipeline_leads_to_master_rows(pipeline, industry, country, job.run_id)
                for _r in rows:           # frozen per-lead cost (blank stays blank for old rows)
                    _r["cost_per_lead_usd"] = _run_cpl_usd
                log.info("_finalize_run: inserting %d leads into master_leads (run_id=%s)", len(rows), job.run_id)
                new_inserted = db.MasterLeadRepo.bulk_insert_new(rows, job.run_id, industry, country)
                log.info("_finalize_run: master_leads inserted=%d (run_id=%s)", new_inserted, job.run_id)
                job.master_leads_new = new_inserted
                job.master_leads_deduped_out = int(
                    getattr(pipeline, "_master_leads_deduped_out", 0)
                )
            except Exception as ie:
                log.exception("master_leads INSERT failed: %s", ie)
                # Don't flip run state to error over a write failure — data is still on disk.
                error_text = (error_text or "") + f" | master_insert_failed: {ie}"

        db.RunHistoryRepo.finish(
            run_id=job.run_id,
            state=state,
            leads_total=len(getattr(pipeline, "leads", []) or []),
            leads_new=job.master_leads_new,
            leads_deduped_out=job.master_leads_deduped_out,
            duration_seconds=duration,
            secondary_agent_used=bool(getattr(pipeline, "_secondary_agent_used", False)),
            competitor_depth_reached=int(getattr(pipeline, "_competitor_depth_reached", 0) or 0),
            api_usage=api_usage,
            error_text=error_text,
            cost_usd=_run_cost_usd,
            cost_per_lead_usd=_run_cpl_usd,
        )
        log.info("_finalize_run: run_history updated run_id=%s state=%s", job.run_id, state)
    except Exception as e:
        log.exception("_finalize_run failed for run_id=%s: %s", job.run_id, e)


def _run_cpl(job) -> float:
    """Live per-lead dollar cost for the Generate table (cached on the job so we
    compute once per run). Same maths as the value frozen into the DB."""
    v = getattr(job, "_cpl_cache", None)
    if v is None:
        try:
            from V5 import _v5_run_cost
            v = float(_v5_run_cost(job.pipeline)[2])
        except Exception:
            v = 0.0
        try:
            job._cpl_cache = v
        except Exception:
            pass
    return v


# ── /generate ───────────────────────────────────────────────────────────────
@app.route("/generate", methods=["POST"])
@manager_or_admin_required
def generate():
    try:
        from V5 import LeadGenerationPipeline

        if not _db_ready and not _ALLOW_WITHOUT_DB:
            return jsonify({"error": "db_unavailable", "detail": _db_startup_error}), 503

        data = request.get_json() or {}
        industry = (data.get("industry") or "").strip()
        country = data.get("country", "AU")
        min_volume = int(data.get("min_volume", 100))
        # Phase 2: CPC default lowered from 1.0 → 0.05 per user spec.
        min_cpc = float(data.get("min_cpc", 0.05))
        max_leads = int(data.get("max_leads", 0))
        enrichment = bool(data.get("enrichment", True))
        # 2026-06-08: "SerpAPI only" toggle — bypass SEMrush even if it has credits.
        disable_semrush = bool(data.get("disable_semrush", data.get("serp_only", False)))
        # 2026-06-09: credit-saving mode ON by default; UI "Regular mode" toggle
        # sends credit_saver=false to run the thorough (more-credits) path.
        credit_saver = bool(data.get("credit_saver", True))
        # 2026-06-11: "paid only — keep all confirmed advertisers" (Max Leads = floor).
        # 2026-06-12: DEFAULT ON — the user's standing requirement is that EVERY
        # confirmed advertiser exports even when it exceeds Max Leads.
        paid_only_all = bool(data.get("paid_only_all", True))
        if not industry:
            return jsonify({"error": "Industry is required"}), 400

        job_id = str(_uuid.uuid4())[:8]
        job = JobState()
        job.user_id = int(getattr(current_user, "id", 0) or 0)
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        # Persist the run-history row BEFORE the pipeline starts so state=running
        # is visible from /admin/runs while it's in flight.
        if _db_ready:
            try:
                # Defensive: ensure user_id is a valid row in users before FK insert.
                if job.user_id <= 0:
                    first_user = db.UserRepo.list_all()
                    job.user_id = int(first_user[0]["id"]) if first_user else 1
                    log.warning("generate: current_user.id was 0; using fallback user_id=%s", job.user_id)
                job.run_id = db.RunHistoryRepo.start(
                    user_id=job.user_id, job_uuid=job_id, industry=industry,
                    country=country, mode="industry", min_volume=min_volume,
                    min_cpc=min_cpc, max_leads=max_leads, enrichment_enabled=enrichment,
                )
                log.info("generate: run_history started run_id=%s job_id=%s user_id=%s", job.run_id, job_id, job.user_id)
            except Exception as ie:
                log.error("run_history.start failed (user_id=%s): %s", job.user_id, ie)
                job.run_id = None

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(message):
            job.logs.append(message)

        lead_pool = LeadPool(skip_master_known=True)
        pipeline = LeadGenerationPipeline(
            industry=industry, country=country, min_volume=min_volume,
            min_cpc=min_cpc, output_folder=output_folder,
            progress_callback=progress_cb, log_callback=log_cb,
            max_leads=max_leads, enrichment_enabled=enrichment,
            lead_pool=lead_pool, disable_semrush=disable_semrush,
            credit_saver=credit_saver,
            paid_only_all=paid_only_all,
        )
        job.pipeline = pipeline

        def run():
            try:
                job.progress = 1
                job.status_text = "Initializing pipeline..."
                job.logs.append("[SYSTEM] Pipeline initialized, starting Phase 1...")
                result_path = pipeline.run()
                job.api_usage = pipeline._api_counter.copy()

                if pipeline._cancelled:
                    job.state = "cancelled"
                    # Phase 2 (2026-05-05): expose partial CSV. V5 run() now
                    # returns the partial path when cancelled; pipeline also
                    # stashes _partial_csv_path. Master_leads stays untouched
                    # because state="cancelled" skips the bulk_insert_new path.
                    try:
                        _pp = (result_path
                               if (result_path and os.path.exists(result_path)
                                   and "_partial.csv" in os.path.basename(result_path))
                               else getattr(pipeline, "_partial_csv_path", ""))
                        if _pp and os.path.exists(_pp):
                            job.partial_csv_path = _pp
                            with open(_pp, "r", encoding="utf-8") as _pf:
                                job.partial_csv_text = _pf.read()
                    except Exception:
                        pass
                    _finalize_run(job, pipeline, industry, country, "cancelled")
                    return

                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as f:
                        for row in _csv.DictReader(f):
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "phone": row.get("Phone Number", ""),
                                "email": row.get("Email", ""),
                                "email_type": row.get("Email Type", ""),
                                "source": row.get("Source", "") or "Apollo",
                                "_traffic_source": (row.get("Traffic Source") or "").strip(),
                                "_google_intent": ((row.get("Traffic Source") or "").strip() == "Google Intent"),
                                "keyword": row.get("Keyword", ""),
                                "cost_per_lead": _run_cpl(job),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break

                    pipeline_leads = getattr(pipeline, "leads", [])
                    paid_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source", "paid") == "paid")
                    organic_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source") == "organic")
                    with_phone = sum(1 for lead in job.leads if lead.get("phone"))
                    with_email = sum(1 for lead in job.leads if lead.get("email"))
                    personal_emails = sum(1 for lead in job.leads if lead.get("email_type") == "Personal")
                    direct_phones = sum(1 for ld in pipeline_leads if ld.get("_direct_phone"))
                    verified_emails = sum(1 for ld in pipeline_leads if ld.get("_email_verified"))
                    founder_verified_count = sum(1 for ld in pipeline_leads if ld.get("_founder_verified"))

                    _credit_costs = {"semrush": 10, "apollo": 1, "lusha": 1, "serpapi": 1, "openai": 0.01, "hunter": 1}
                    total_credits = sum(job.api_usage.get(svc, 0) * cost for svc, cost in _credit_costs.items())
                    credits_breakdown = {svc: round(job.api_usage.get(svc, 0) * cost, 2)
                                         for svc, cost in _credit_costs.items()}

                    job.summary = {
                        "paid_leads": paid_count,
                        "organic_leads": organic_count,
                        "total_leads": len(job.leads),
                        "with_phone": with_phone,
                        "with_email": with_email,
                        "personal_emails": personal_emails,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "direct_phones": direct_phones,
                        "verified_emails": verified_emails,
                        "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                        "total_credits_used": round(total_credits, 2),
                        "avg_credits_per_lead": round(total_credits / max(len(job.leads), 1), 2),
                        "credits_breakdown": credits_breakdown,
                        "founder_verified_count": founder_verified_count,
                        "competitor_domains_added": getattr(pipeline, "_competitor_domains_added", 0),
                        # Phase 2 additions
                        "competitor_depth_reached": getattr(pipeline, "_competitor_depth_reached", 0),
                        "secondary_agent_used": bool(getattr(pipeline, "_secondary_agent_used", False)),
                        "secondary_domains_added": getattr(pipeline, "_secondary_domains_added", 0),
                        "paid_kw_expansion_added": getattr(pipeline, "_paid_kw_expansion_added", 0),
                        "master_leads_deduped_out": getattr(pipeline, "_master_leads_deduped_out", 0),
                    }
                    _finalize_run(job, pipeline, industry, country, "done")
                    # Re-read master count so the summary reflects the new insert.
                    try:
                        job.summary["master_leads_new"] = job.master_leads_new
                    except Exception:
                        pass
                    job.state = "done"
                else:
                    state = "done" if not pipeline._cancelled else "cancelled"
                    _finalize_run(job, pipeline, industry, country, state)
                    job.state = state
            except Exception as e:
                job.error = str(e)
                job.state = "error"
                # Phase 2 (2026-05-05): salvage partial CSV from pipeline
                # if it managed to write one before crashing.
                try:
                    _pp = getattr(pipeline, "_partial_csv_path", "")
                    if _pp and os.path.exists(_pp):
                        job.partial_csv_path = _pp
                        with open(_pp, "r", encoding="utf-8") as _pf:
                            job.partial_csv_text = _pf.read()
                except Exception:
                    pass
                try:
                    _finalize_run(job, pipeline, industry, country, "error", error_text=str(e))
                except Exception:
                    pass

        threading.Thread(target=run, daemon=True).start()
        return jsonify({"job_id": job_id})
    except Exception as e:
        log.exception("/generate failed")
        return jsonify({"error": str(e)}), 500


# ── /generate-city ──────────────────────────────────────────────────────────
@app.route("/generate-city", methods=["POST"])
@manager_or_admin_required
def generate_city():
    try:
        from city_pipeline import CityLeadPipeline

        if not _db_ready and not _ALLOW_WITHOUT_DB:
            return jsonify({"error": "db_unavailable", "detail": _db_startup_error}), 503

        data = request.get_json() or {}
        state_code = data.get("state_code", "AUSTRALIA")
        tier = data.get("tier", "all")
        city = data.get("city", "all")
        min_volume = int(data.get("min_volume", 100))
        max_leads = int(data.get("max_leads", 0))
        enrichment = bool(data.get("enrichment", True))
        country = data.get("country", "AU")
        # 2026-06-08: "SerpAPI only" toggle — bypass SEMrush even if it has credits.
        disable_semrush = bool(data.get("disable_semrush", data.get("serp_only", False)))
        # 2026-06-09: credit-saving mode ON by default; UI "Regular mode" toggle
        # sends credit_saver=false to run the thorough (more-credits) path.
        credit_saver = bool(data.get("credit_saver", True))
        # 2026-06-11: "paid only — keep all confirmed advertisers" (Max Leads = floor).
        # 2026-06-12: DEFAULT ON (see /generate above).
        paid_only_all = bool(data.get("paid_only_all", True))

        if max_leads <= 0:
            return jsonify({"error": "Max Leads must be > 0 in City Mode"}), 400

        job_id = str(_uuid.uuid4())[:8]
        job = JobState()
        job.user_id = int(getattr(current_user, "id", 0) or 0)
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        if _db_ready:
            try:
                if job.user_id <= 0:
                    first_user = db.UserRepo.list_all()
                    job.user_id = int(first_user[0]["id"]) if first_user else 1
                    log.warning("generate-city: current_user.id was 0; using fallback user_id=%s", job.user_id)
                job.run_id = db.RunHistoryRepo.start(
                    user_id=job.user_id, job_uuid=job_id, industry=f"city:{state_code}/{tier}/{city}",
                    country=country, mode="city", min_volume=min_volume,
                    min_cpc=None, max_leads=max_leads, enrichment_enabled=enrichment,
                )
                log.info("generate-city: run_history started run_id=%s job_id=%s", job.run_id, job_id)
            except Exception as ie:
                log.error("run_history.start(city) failed (user_id=%s): %s", job.user_id, ie)
                job.run_id = None

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(message):
            job.logs.append(message)

        pipeline = CityLeadPipeline(
            state_code=state_code, tier=tier, city=city,
            min_volume=min_volume, max_leads=max_leads,
            output_folder=output_folder,
            enrichment_enabled=enrichment, country=country,
            quota_guarantee=True,
            progress_callback=progress_cb, log_callback=log_cb,
            disable_semrush=disable_semrush,
            credit_saver=credit_saver,
            paid_only_all=paid_only_all,
        )
        job.pipeline = pipeline

        def run():
            try:
                job.progress = 1
                job.status_text = "Initializing city-mode pipeline..."
                job.logs.append("[SYSTEM] City-mode pipeline initialized…")
                result_path = pipeline.run()
                job.api_usage = dict(pipeline._api_counter)
                if pipeline._cancelled:
                    job.state = "cancelled"
                    # Phase 2 (2026-05-05): expose partial CSV. V5 run() now
                    # returns the partial path when cancelled; pipeline also
                    # stashes _partial_csv_path. Master_leads stays untouched
                    # because state="cancelled" skips the bulk_insert_new path.
                    try:
                        _pp = (result_path
                               if (result_path and os.path.exists(result_path)
                                   and "_partial.csv" in os.path.basename(result_path))
                               else getattr(pipeline, "_partial_csv_path", ""))
                        if _pp and os.path.exists(_pp):
                            job.partial_csv_path = _pp
                            with open(_pp, "r", encoding="utf-8") as _pf:
                                job.partial_csv_text = _pf.read()
                    except Exception:
                        pass
                    _finalize_run(job, pipeline, f"city:{state_code}", country, "cancelled")
                    return
                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as f:
                        for row in _csv.DictReader(f):
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "phone": row.get("Phone Number", ""),
                                "email": row.get("Email", ""),
                                "email_type": row.get("Email Type", ""),
                                "source": row.get("Source", "") or "Apollo",
                                "_traffic_source": (row.get("Traffic Source") or "").strip(),
                                "_google_intent": ((row.get("Traffic Source") or "").strip() == "Google Intent"),
                                "keyword": row.get("Keyword", ""),
                                "cost_per_lead": _run_cpl(job),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break

                    with_phone = sum(1 for lead in job.leads if lead.get("phone"))
                    with_email = sum(1 for lead in job.leads if lead.get("email"))
                    personal_emails = sum(1 for lead in job.leads if lead.get("email_type") == "Personal")
                    _credit_costs = {"semrush": 10, "apollo": 1, "lusha": 1, "serpapi": 1, "openai": 0.01, "hunter": 1}
                    total_credits = sum(job.api_usage.get(svc, 0) * cost for svc, cost in _credit_costs.items())
                    credits_breakdown = {svc: round(job.api_usage.get(svc, 0) * cost, 2)
                                         for svc, cost in _credit_costs.items()}
                    job.summary = {
                        "mode": "city",
                        "scope_label": pipeline._scope_label,
                        "state_code": state_code,
                        "tier": tier,
                        "city": city,
                        "enrichment_enabled": enrichment,
                        "paid_leads": len(job.leads),
                        "organic_leads": 0,
                        "total_leads": len(job.leads),
                        "with_phone": with_phone,
                        "with_email": with_email,
                        "personal_emails": personal_emails,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                        "total_credits_used": round(total_credits, 2),
                        "avg_credits_per_lead": round(total_credits / max(len(job.leads), 1), 2),
                        "credits_breakdown": credits_breakdown,
                        "competitor_rounds": getattr(pipeline, "_competitor_rounds", 0),
                        "competitor_domains_added": getattr(pipeline, "_competitor_domains_added", 0),
                        "industries_searched": len(getattr(pipeline, "keywords", [])) > 0,
                        "keywords_searched": len(getattr(pipeline, "keywords", [])),
                    }
                    _finalize_run(job, pipeline, f"city:{state_code}", country, "done")
                    job.state = "done"
                else:
                    state = "done" if not pipeline._cancelled else "cancelled"
                    _finalize_run(job, pipeline, f"city:{state_code}", country, state)
                    job.state = state
            except Exception as e:
                import traceback
                traceback.print_exc()
                job.error = str(e)
                job.state = "error"
                try:
                    _finalize_run(job, pipeline, f"city:{state_code}", country, "error", error_text=str(e))
                except Exception:
                    pass

        threading.Thread(target=run, daemon=True).start()
        return jsonify({"job_id": job_id})
    except Exception as e:
        log.exception("/generate-city failed")
        return jsonify({"error": str(e)}), 500


# ── /generate-multi — parallel multi-industry mode ───────────────────────────
@app.route("/generate-multi", methods=["POST"])
@manager_or_admin_required
def generate_multi():
    """Parallel multi-industry lead generation.

    Divides max_leads equally across all (or a specified subset of) industries
    and runs up to n_workers pipelines simultaneously. Results are combined into
    a single CSV at the end.
    """
    try:
        from V5 import INDUSTRY_KEYWORDS
        from parallel_pipeline import ParallelLeadOrchestrator

        if not _db_ready and not _ALLOW_WITHOUT_DB:
            return jsonify({"error": "db_unavailable", "detail": _db_startup_error}), 503

        data = request.get_json() or {}
        max_leads = int(data.get("max_leads", 50))
        enrichment = bool(data.get("enrichment", True))
        country = data.get("country", "AU")
        min_volume = int(data.get("min_volume", 100))
        min_cpc = float(data.get("min_cpc", 0.05))
        n_workers = max(2, min(8, int(data.get("n_workers", 4))))

        # Use the caller-specified industry list, or all available industries
        requested = data.get("industries", [])
        all_inds = list(INDUSTRY_KEYWORDS.keys())
        industries = [i for i in requested if i] if requested else all_inds

        if not industries:
            return jsonify({"error": "No industries available"}), 400
        if max_leads <= 0:
            return jsonify({"error": "max_leads must be > 0"}), 400

        job_id = str(_uuid.uuid4())[:8]
        job = JobState()
        job.user_id = int(getattr(current_user, "id", 0) or 0)
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        if _db_ready:
            try:
                if job.user_id <= 0:
                    first_user = db.UserRepo.list_all()
                    job.user_id = int(first_user[0]["id"]) if first_user else 1
                job.run_id = db.RunHistoryRepo.start(
                    user_id=job.user_id, job_uuid=job_id,
                    industry=f"multi:{len(industries)} industries",
                    country=country, mode="industry",
                    min_volume=min_volume, min_cpc=min_cpc,
                    max_leads=max_leads, enrichment_enabled=enrichment,
                )
                log.info("generate-multi: run_history run_id=%s job_id=%s", job.run_id, job_id)
            except Exception as ie:
                log.error("generate-multi run_history.start: %s", ie)
                job.run_id = None

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(msg):
            job.logs.append(msg)

        orchestrator = ParallelLeadOrchestrator(
            industries=industries,
            max_leads=max_leads,
            country=country,
            min_volume=min_volume,
            min_cpc=min_cpc,
            enrichment_enabled=enrichment,
            output_folder=output_folder,
            n_workers=n_workers,
            progress_callback=progress_cb,
            log_callback=log_cb,
        )
        job.pipeline = orchestrator  # duck-typed for /cancel

        def run():
            try:
                job.progress = 1
                job.status_text = f"Starting {len(industries)} parallel industry agents…"
                job.logs.append(
                    f"[SYSTEM] Parallel mode: {len(industries)} industries × "
                    f"{orchestrator.quota_per_industry} leads each"
                )
                result_path = orchestrator.run()
                job.api_usage = orchestrator._api_counter.copy()

                if orchestrator._cancelled:
                    job.state = "cancelled"
                    _finalize_run_multi(job, orchestrator, country, "cancelled")
                    return

                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as fcsv:
                        for row in _csv.DictReader(fcsv):
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "phone": row.get("Phone Number", ""),
                                "email": row.get("Email", ""),
                                "email_type": row.get("Email Type", ""),
                                "source": row.get("Source", "") or "Apollo",
                                "_traffic_source": (row.get("Traffic Source") or "").strip(),
                                "_google_intent": ((row.get("Traffic Source") or "").strip() == "Google Intent"),
                                "keyword": row.get("Keyword", ""),
                                "cost_per_lead": _run_cpl(job),
                            })

                _credit_costs = {"semrush": 10, "apollo": 1, "lusha": 1, "serpapi": 1, "openai": 0.01, "hunter": 1}
                total_credits = sum(job.api_usage.get(s, 0) * c for s, c in _credit_costs.items())
                credits_breakdown = {s: round(job.api_usage.get(s, 0) * c, 2) for s, c in _credit_costs.items()}
                job.summary = {
                    "mode": "multi",
                    "industries_count": len(industries),
                    "quota_per_industry": orchestrator.quota_per_industry,
                    "total_leads": len(job.leads),
                    "with_phone": sum(1 for ld in job.leads if ld.get("phone")),
                    "with_email": sum(1 for ld in job.leads if ld.get("email")),
                    "personal_emails": sum(1 for ld in job.leads if ld.get("email_type") == "Personal"),
                    "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                    "total_credits_used": round(total_credits, 2),
                    "credits_breakdown": credits_breakdown,
                }
                _finalize_run_multi(job, orchestrator, country, "done")
                job.state = "done"
                job.progress = 100
            except Exception as exc:
                import traceback
                traceback.print_exc()
                job.error = str(exc)
                job.state = "error"
                try:
                    _finalize_run_multi(job, orchestrator, country, "error", error_text=str(exc))
                except Exception:
                    pass

        job.state = "running"
        job.start_time = time.time()
        threading.Thread(target=run, daemon=True).start()
        return jsonify({
            "job_id": job_id,
            "industries": len(industries),
            "quota_per_industry": orchestrator.quota_per_industry,
        })
    except Exception as e:
        log.exception("/generate-multi failed")
        return jsonify({"error": str(e)}), 500


def _finalize_run_multi(job, orchestrator, country: str, state: str, error_text: str = None):
    """Persist run_history + master_leads for a parallel-multi run."""
    if job.run_id is None:
        return
    try:
        api_usage = dict(orchestrator._api_counter or {})
        duration = max(0, int(time.time() - job.start_time))
        new_inserted = 0
        if state == "done":
            try:
                # Build master-row list from the orchestrator's combined leads
                class _FakeP:
                    leads = orchestrator.all_leads

                rows = _pipeline_leads_to_master_rows(
                    _FakeP(), f"multi:{country}", country, job.run_id
                )
                log.info("_finalize_run_multi: inserting %d leads (run_id=%s)", len(rows), job.run_id)
                new_inserted = db.MasterLeadRepo.bulk_insert_new(rows, job.run_id, f"multi", country)
                job.master_leads_new = new_inserted
            except Exception as ie:
                log.exception("generate-multi master_leads insert failed: %s", ie)
        db.RunHistoryRepo.finish(
            run_id=job.run_id, state=state,
            leads_total=len(orchestrator.all_leads),
            leads_new=new_inserted,
            leads_deduped_out=0,
            duration_seconds=duration,
            secondary_agent_used=False,
            competitor_depth_reached=0,
            api_usage=api_usage,
            error_text=error_text,
        )
    except Exception as e:
        log.exception("_finalize_run_multi failed: %s", e)


# ── Status / cancel ─────────────────────────────────────────────────────────
@app.route("/status/<job_id>")
@manager_or_admin_required
def get_status(job_id):
    try:
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        new_logs = job.logs[job.log_cursor:]
        job.log_cursor = len(job.logs)
        elapsed = time.time() - job.start_time
        progress = max(job.progress, 1)
        remaining = (elapsed / progress) * (100 - progress) if progress < 100 else 0
        result = {
            "state": job.state,
            "progress": job.progress,
            "status_text": job.status_text,
            "new_logs": new_logs,
            "elapsed_seconds": round(elapsed),
            "time_remaining_seconds": round(remaining),
        }
        if job.state == "done":
            result["leads"] = job.leads
            result["top_csv"] = job.top_csv
            result["all_csv"] = job.all_csv
            result["api_usage"] = job.api_usage
            result["summary"] = job.summary
            result["master_leads_new"] = job.master_leads_new
            result["master_leads_deduped_out"] = job.master_leads_deduped_out
        if job.state == "error":
            result["error"] = job.error
            result["api_usage"] = job.api_usage
        # Phase 2 (2026-05-05): live valid-lead count (parsed from
        # [VALID-LEADS] log lines emitted by V5 phases) + partial-CSV
        # availability flag for cancel/error states. Frontend uses these
        # to render the live counter and the "Download partial CSV" button.
        try:
            for line in (new_logs or []):
                if "[VALID-LEADS]" in line:
                    import re as _re
                    _m = _re.search(r":\s*(\d+)\s+lead", line)
                    if _m:
                        job.valid_leads_in_progress = int(_m.group(1))
        except Exception:
            pass
        result["valid_leads_in_progress"] = job.valid_leads_in_progress
        if job.state in ("cancelled", "error") and job.partial_csv_path:
            result["partial_csv_available"] = True
            result["partial_csv_filename"] = os.path.basename(job.partial_csv_path)
        else:
            result["partial_csv_available"] = False
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/cancel", methods=["POST"])
@manager_or_admin_required
def cancel():
    try:
        for jid in reversed(list(_jobs.keys())):
            j = _jobs[jid]
            if j.state == "running" and j.pipeline:
                j.pipeline.cancel()
                return jsonify({"status": "cancelling"})
        return jsonify({"status": "no active job"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Partial CSV download (cancel / error recovery) ──────────────────────────
@app.route("/download-partial/<job_id>")
@manager_or_admin_required
def download_partial(job_id):
    """Phase 2 (2026-05-05): serves the partial CSV that V5._export_partial_now
    writes when a run is cancelled or crashes mid-pipeline. Partial leads are
    NEVER inserted into master_leads (the cancelled/error path skips that)."""
    job = _jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    path = getattr(job, "partial_csv_path", "")
    if not path or not os.path.exists(path):
        # Fall back to in-memory text if path was cleaned up
        text = getattr(job, "partial_csv_text", "") or ""
        if not text:
            return jsonify({"error": "No partial CSV available for this job"}), 404
        from flask import Response
        return Response(
            text, mimetype="text/csv",
            headers={"Content-Disposition": f'attachment; filename="leads_partial_{job_id}.csv"'},
        )
    try:
        from flask import send_file
        return send_file(
            path, as_attachment=True, mimetype="text/csv",
            download_name=os.path.basename(path),
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Apollo phone-reveal webhook (HMAC-guarded if secret configured) ─────────
@app.route("/apollo-phone-callback", methods=["POST"])
def apollo_phone_callback():
    """Public webhook (must be Internet-reachable for Apollo). If
    APOLLO_WEBHOOK_SECRET is set, request body must be signed with
    X-LeadForge-Sig = hex(HMAC-SHA256(secret, raw_body))."""
    secret = os.environ.get("APOLLO_WEBHOOK_SECRET") or ""
    raw = request.get_data() or b""
    if secret:
        sig = request.headers.get("X-LeadForge-Sig", "")
        expected = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            log.warning("apollo-phone-callback: HMAC mismatch (len=%d)", len(raw))
            return jsonify({"error": "invalid_signature"}), 403
    try:
        payload = json.loads(raw.decode("utf-8")) if raw else {}
    except Exception:
        payload = {}
    try:
        from V5 import _phone_reveal_store
        person_id = (payload.get("person_id") or payload.get("id") or "").strip()
        if person_id:
            _phone_reveal_store[person_id] = payload
    except Exception as e:
        log.error("phone_reveal store error: %s", e)
    return jsonify({"status": "ok"})


# ── Background enrichment worker (CRM Phase 2) ───────────────────────────────
# Single always-on daemon, fully separate from the lead-gen pipeline. It pauses
# whenever a /generate* run is active so it never competes with a live run.
def _a_run_is_active() -> bool:
    try:
        return any(getattr(j, "state", "") == "running" for j in _jobs.values())
    except Exception:
        return False


if _db_ready:
    try:
        import enrichment_worker
        if enrichment_worker.start_background_worker(is_busy=_a_run_is_active):
            log.info("Enrichment worker thread started.")
    except Exception as _ew_e:
        log.error("Enrichment worker failed to start (non-fatal): %s", _ew_e)


# ── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
