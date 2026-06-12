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
from auth import admin_required, login_manager, login_required_json, do_login, do_logout, verify_credentials
from lead_pool import LeadPool
from utils import normalize_master_key, root_domain

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
# 2026-05-18: LOGIN FEATURE DISABLED. The /login route now just redirects
# to "/", so old bookmarks and the frontend's "401 → /login" fallbacks
# don't dead-end. POST is accepted for back-compat (e.g. legacy form
# submissions) but ignores credentials and redirects unconditionally.
@app.route("/login", methods=["GET", "POST"])
def login():
    nxt = request.args.get("next") or (request.form.get("next") if request.form else None) or "/"
    if not nxt.startswith("/") or nxt.startswith("//"):
        nxt = "/"
    if _wants_json():
        return jsonify({"status": "ok", "auth": "disabled", "next": nxt})
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
    # 2026-05-18: LOGIN DISABLED — clear any stale session cookie, then
    # send the user to the home page (NOT /login, which now redirects
    # straight back).
    try:
        do_logout()
    except Exception:
        pass
    if _wants_json():
        return jsonify({"status": "logged_out", "auth": "disabled"})
    return redirect("/")


@app.route("/whoami")
def whoami():
    # 2026-05-18: LOGIN DISABLED — return a synthetic "guest admin" so any
    # frontend code reading the response (username/role badges, admin
    # gate checks) keeps working without an actual session.
    if getattr(current_user, "is_authenticated", False):
        return jsonify({
            "id": getattr(current_user, "id", 0),
            "username": getattr(current_user, "username", "guest"),
            "role": getattr(current_user, "role", "admin"),
            "is_admin": getattr(current_user, "is_admin", True),
        })
    return jsonify({
        "id": 0, "username": "guest", "role": "admin", "is_admin": True,
        "auth": "disabled",
    })


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
@login_required_json
def get_industries():
    try:
        from V5 import INDUSTRY_KEYWORDS
        return jsonify({"industries": list(INDUSTRY_KEYWORDS.keys())})
    except Exception:
        return jsonify({"industries": ["Electrician", "Plumber", "Photographer"]})


@app.route("/api/au-cities")
@login_required_json
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
@login_required_json
def get_credits():
    try:
        return jsonify(_build_credits_payload(force=False))
    except Exception as e:
        return jsonify({"error": str(e), "services": {}, "alerts": []}), 500


@app.route("/api/credits/refresh", methods=["POST"])
@login_required_json
def refresh_credits():
    try:
        return jsonify(_build_credits_payload(force=True))
    except Exception as e:
        return jsonify({"error": str(e), "services": {}, "alerts": []}), 500


# ── Master-DB stats (dashboard panel) ───────────────────────────────────────
_master_stats_cache = {"data": None, "timestamp": 0.0}
_MASTER_STATS_TTL = 60


@app.route("/api/master-stats")
@login_required_json
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


# ── DB viewer endpoints ──────────────────────────────────────────────────────
@app.route("/api/db/debug")
@login_required_json
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
@login_required_json
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
    """Paginated master_leads viewer. Query params: page (1-based), page_size (max 200)."""
    page = int(request.args.get("page", 1) or 1)
    page_size = int(request.args.get("page_size", 50) or 50)
    if not _db_ready:
        return jsonify({"error": "db_unavailable",
                        "detail": _db_startup_error or "MySQL not configured on this host"}), 503
    try:
        rows, total = db.MasterLeadRepo.list_page(page=page, page_size=page_size)
        for r in rows:
            if r.get("first_seen_at") and hasattr(r["first_seen_at"], "isoformat"):
                r["first_seen_at"] = r["first_seen_at"].isoformat()
            # DECIMAL/NULL → float for JSON; keep None so old leads stay blank.
            if r.get("cost_per_lead_usd") is not None:
                try:
                    r["cost_per_lead_usd"] = float(r["cost_per_lead_usd"])
                except (TypeError, ValueError):
                    r["cost_per_lead_usd"] = None
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


@app.route("/api/db/run-history")
@login_required_json
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
def _pipeline_leads_to_master_rows(
    pipeline, industry: str, country: str, run_id: int
) -> list[dict]:
    rows: list[dict] = []
    for ld in getattr(pipeline, "leads", []) or []:
        nn = normalize_master_key(ld.get("name") or "")
        rd = root_domain(ld.get("domain") or "")
        if not (nn and rd):
            continue
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
            "display_name": ld.get("name") or None,
            "company_name": ld.get("company") or None,
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
@login_required_json
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
@login_required_json
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
@login_required_json
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
@login_required_json
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
@login_required_json
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
@login_required_json
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


# ── Main ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
