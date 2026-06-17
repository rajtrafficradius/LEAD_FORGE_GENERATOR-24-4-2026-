"""Flask-Login auth for LEAD_FORGE Phase 2.

Gates the web UI behind a username/password. One admin + ten standard users
are seeded on first boot with random passwords; the plaintext credentials are
written to FIRST_RUN_CREDENTIALS.txt (gitignored) for one-time retrieval.

Decorators:
    login_required_json — for JSON/XHR endpoints. Returns 401 JSON.
    login_required      — Flask-Login's default (redirects to /login).
    admin_required      — must be logged in AND role='admin'.

Security notes:
    * werkzeug.security.generate_password_hash with pbkdf2:sha256:260000
      (matches current Django/Flask stdlib hardness)
    * SECRET_KEY must be set in env for session signing (Flask-Login uses
      Flask's cookie sessions under the hood)
"""
from __future__ import annotations

import functools
import json
import logging
import os
import secrets
import string
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from flask import Flask, jsonify, redirect, request, url_for
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_user as _flask_login_user,
    logout_user as _flask_logout_user,
)
from werkzeug.security import check_password_hash, generate_password_hash

from db import UserRepo

log = logging.getLogger("leadforge.auth")

# ── Flask-Login user model ───────────────────────────────────────────────────


class User(UserMixin):
    """Minimal UserMixin wrapper around a users-row dict."""

    def __init__(self, row: Dict[str, Any]) -> None:
        self.id: int = int(row["id"])
        self.username: str = row["username"]
        self.email: str = row.get("email", "")
        self.role: str = row.get("role", "user")
        self.full_name: str = row.get("full_name") or ""
        self.mobile_e164: str = row.get("mobile_e164") or ""
        self.threecx_ext: str = row.get("threecx_ext") or ""
        self.must_change_pw: bool = bool(row.get("must_change_pw", 0))
        self._is_active: bool = bool(row.get("is_active", 1))

    def get_id(self) -> str:
        return str(self.id)

    @property
    def is_active(self) -> bool:  # overrides UserMixin default
        return self._is_active

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def is_manager(self) -> bool:
        return self.role == "manager"

    @property
    def is_bde(self) -> bool:
        return self.role == "bde"

    @property
    def is_manager_or_admin(self) -> bool:
        return self.role in ("admin", "manager")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id, "username": self.username, "role": self.role,
            "email": self.email,
            "full_name": getattr(self, "full_name", "") or "",
            "mobile_e164": getattr(self, "mobile_e164", "") or "",
            "threecx_ext": getattr(self, "threecx_ext", "") or "",
            "must_change_pw": bool(getattr(self, "must_change_pw", False)),
        }


login_manager = LoginManager()
login_manager.login_view = "login"          # URL name for anon HTML redirects
login_manager.session_protection = "strong"


@login_manager.user_loader
def _load_user(user_id: str) -> Optional[User]:
    try:
        row = UserRepo.get_by_id(int(user_id))
    except Exception as e:
        log.warning("user_loader DB error: %s", e)
        return None
    return User(row) if row else None


# 2026-06-15: LOGIN RE-ENABLED for the CRM layer (ADMIN / MANAGER / BDE roles).
# Set to True to bypass all auth again (dev only). Can also be forced off via
# env LOGIN_DISABLED=1 for emergencies without a code change.
LOGIN_DISABLED = str(os.environ.get("LOGIN_DISABLED", "0")).strip() == "1"


@login_manager.unauthorized_handler
def _unauthorized():
    """No-op when LOGIN_DISABLED. Kept for Flask-Login internal API."""
    if LOGIN_DISABLED:
        # Pretend everything is fine — should not fire in practice.
        return jsonify({"status": "ok", "auth": "disabled"}), 200
    if _wants_json(request):
        return jsonify({"error": "unauthorized", "login_url": "/login"}), 401
    next_url = request.path or "/"
    return redirect(f"/login?next={next_url}")


def _wants_json(req) -> bool:
    """True if the caller appears to be a JSON client (XHR / fetch / API)."""
    if req.is_json or "application/json" in (req.headers.get("Accept") or ""):
        return True
    if req.headers.get("X-Requested-With", "").lower() == "xmlhttprequest":
        return True
    if req.path.startswith("/api/") or req.path.startswith("/generate") or req.path.startswith("/status"):
        return True
    return False


# ── Decorators ───────────────────────────────────────────────────────────────


def login_required_json(view: Callable) -> Callable:
    """2026-05-18: LOGIN DISABLED — pass-through. Originally returned 401
    JSON for anonymous callers; now every request is accepted."""
    if LOGIN_DISABLED:
        return view
    @functools.wraps(view)
    def wrapped(*args: Any, **kwargs: Any):
        if not current_user.is_authenticated:
            return jsonify({"error": "unauthorized", "login_url": "/login"}), 401
        return view(*args, **kwargs)
    return wrapped


def admin_required(view: Callable) -> Callable:
    """Require an authenticated user with role='admin'."""
    if LOGIN_DISABLED:
        return view
    @functools.wraps(view)
    def wrapped(*args: Any, **kwargs: Any):
        if not current_user.is_authenticated:
            return jsonify({"error": "unauthorized", "login_url": "/login"}), 401
        if not getattr(current_user, "is_admin", False):
            return jsonify({"error": "forbidden", "reason": "admin role required"}), 403
        return view(*args, **kwargs)
    return wrapped


def manager_or_admin_required(view: Callable) -> Callable:
    """Require role in {admin, manager}. Blocks BDE accounts. Used for the
    generation, database, allocation, cost and user-management endpoints."""
    if LOGIN_DISABLED:
        return view
    @functools.wraps(view)
    def wrapped(*args: Any, **kwargs: Any):
        if not current_user.is_authenticated:
            return jsonify({"error": "unauthorized", "login_url": "/login"}), 401
        if not getattr(current_user, "is_manager_or_admin", False):
            return jsonify({"error": "forbidden", "reason": "manager or admin role required"}), 403
        return view(*args, **kwargs)
    return wrapped


def role_required(*roles: str) -> Callable:
    """Decorator factory: require the user's role to be one of `roles`."""
    allowed = set(roles)
    def deco(view: Callable) -> Callable:
        if LOGIN_DISABLED:
            return view
        @functools.wraps(view)
        def wrapped(*args: Any, **kwargs: Any):
            if not current_user.is_authenticated:
                return jsonify({"error": "unauthorized", "login_url": "/login"}), 401
            if getattr(current_user, "role", None) not in allowed:
                return jsonify({"error": "forbidden",
                                "reason": f"requires role in {sorted(allowed)}"}), 403
            return view(*args, **kwargs)
        return wrapped
    return deco


# ── Login / logout helpers ──────────────────────────────────────────────────


def verify_credentials(username: str, password: str) -> Optional[User]:
    """Return a User on success, None on failure. Timing-safe via werkzeug."""
    row = UserRepo.get_by_username(username)
    if not row:
        # Still do a dummy hash check to keep response timing uniform.
        check_password_hash("pbkdf2:sha256:260000$x$y", password)
        return None
    if not check_password_hash(row["password_hash"], password):
        return None
    UserRepo.touch_last_login(int(row["id"]))
    return User(row)


def do_login(user: User, remember: bool = False) -> None:
    _flask_login_user(user, remember=remember)


def do_logout() -> None:
    _flask_logout_user()


# ── Seed users on first boot ────────────────────────────────────────────────

# Built-in roster: one admin + ten operators. Usernames match the plan
# approved by the user ("admin_radius" + "lead_hunter_01..10"). Emails are
# synthetic placeholders; the admin should update them via DB before rotation.
_DEFAULT_ADMIN_USERNAME = "admin_radius"
_DEFAULT_USER_PREFIX = "lead_hunter_"
_NUM_OPERATORS = 10
_EMAIL_DOMAIN = "leadforge.local"

# Password charset: alphanumeric only to avoid shell-escape headaches when
# users paste creds. 16 chars × ~62 alphabet = ~95 bits entropy.
_PASSWORD_CHARS = string.ascii_letters + string.digits
_PASSWORD_LEN = 16

_CREDENTIALS_FILE = "FIRST_RUN_CREDENTIALS.txt"
_CREDENTIALS_READ_MARKER = "FIRST_RUN_CREDENTIALS.read"


def _random_password() -> str:
    """Cryptographically strong random password — uses secrets, not random."""
    return "".join(secrets.choice(_PASSWORD_CHARS) for _ in range(_PASSWORD_LEN))


def seed_default_users(project_dir: str | Path) -> Tuple[bool, List[Dict[str, str]]]:
    """Idempotent first-run seed.

    Behavior:
      * If users table is non-empty → return (False, []) — nothing to do.
      * Else: generate 11 random passwords, insert all rows, write plaintext
        credentials to FIRST_RUN_CREDENTIALS.txt in project_dir, and return
        (True, list_of_dicts) where each dict has username, password, role.

    Accepts override via env var SEED_USERS_JSON (JSON list of
    {username, email, password, role} objects) — used in test/CI.
    """
    project_dir = Path(project_dir)
    existing_count = UserRepo.count()
    if existing_count > 0:
        return False, []

    # Optional override from env for reproducible provisioning
    override: List[Dict[str, str]] = []
    raw = os.environ.get("SEED_USERS_JSON")
    if raw:
        try:
            override = json.loads(raw) or []
            if not isinstance(override, list):
                raise ValueError("SEED_USERS_JSON must be a JSON list")
        except Exception as e:
            log.error("SEED_USERS_JSON invalid: %s", e)
            override = []

    if override:
        rows = override
    else:
        rows = [{
            "username": _DEFAULT_ADMIN_USERNAME,
            "email": f"{_DEFAULT_ADMIN_USERNAME}@{_EMAIL_DOMAIN}",
            "password": _random_password(),
            "role": "admin",
        }]
        for i in range(1, _NUM_OPERATORS + 1):
            uname = f"{_DEFAULT_USER_PREFIX}{i:02d}"
            rows.append({
                "username": uname,
                "email": f"{uname}@{_EMAIL_DOMAIN}",
                "password": _random_password(),
                "role": "user",
            })

    created: List[Dict[str, str]] = []
    for r in rows:
        uname = r.get("username") or ""
        email = r.get("email") or f"{uname}@{_EMAIL_DOMAIN}"
        pw = r.get("password") or _random_password()
        role = r.get("role") or "user"
        if not uname:
            continue
        pw_hash = generate_password_hash(pw, method="pbkdf2:sha256:260000")
        uid, created_now = UserRepo.create_if_absent(uname, email, pw_hash, role)
        if created_now:
            created.append({"username": uname, "password": pw, "role": role, "email": email, "user_id": str(uid)})

    if created:
        _write_credentials_file(project_dir, created)
        log.info("Seeded %d users. Credentials written to %s", len(created), _CREDENTIALS_FILE)
    return True, created


def _write_credentials_file(project_dir: Path, users: List[Dict[str, str]]) -> None:
    """Write plaintext first-run credentials. Overwritten on each seed (seed
    only runs when table is empty, so this is effectively once-per-database)."""
    path = project_dir / _CREDENTIALS_FILE
    marker = project_dir / _CREDENTIALS_READ_MARKER
    if marker.exists():
        # Honor the admin's prior "I've read this" marker — don't rewrite.
        return
    lines = [
        "# LEAD_FORGE first-run credentials",
        f"# Generated: {datetime.utcnow().isoformat()}Z",
        "#",
        "# SECURITY: delete this file (or rename to .read) once you've copied",
        "#           the passwords to a secure store. These are plaintext.",
        "#",
        f"{'ROLE':<8} {'USERNAME':<22} {'PASSWORD':<20} EMAIL",
    ]
    for u in users:
        lines.append(f"{u['role']:<8} {u['username']:<22} {u['password']:<20} {u['email']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        # Best-effort restrictive perms (no-op on Windows FAT/exFAT; real on NTFS via ACLs it's still 666 — warn only).
        os.chmod(path, 0o600)
    except Exception:
        log.warning("Could not set 0600 on %s — protect manually.", path)


# ── CRM roster (2026-06-15): 1 admin + 1 manager + 7 BDE ──────────────────────

# Memorable-but-strong defaults (changeable later in User Management). Override
# the whole roster with env CRM_ROSTER_JSON (list of {username,password,role,
# full_name}). Passwords are ~18 chars, mixed case + digit + symbol.
_CRM_ROSTER_FILE = "CRM_ROSTER_CREDENTIALS.txt"


def _default_crm_roster() -> List[Dict[str, str]]:
    roster = [
        {"username": "admin",   "role": "admin",   "password": "Radius-Admin-2026!",   "full_name": "Administrator"},
        {"username": "manager", "role": "manager", "password": "Radius-Manager-2026!", "full_name": "Sales Manager"},
    ]
    for n in range(1, 8):
        roster.append({
            "username": f"bde{n}", "role": "bde",
            "password": f"Radius-BDE{n}-2026!", "full_name": f"BDE {n}",
        })
    return roster


def ensure_crm_roster(project_dir: str | Path) -> List[Dict[str, str]]:
    """Idempotent: guarantee admin/manager/bde1..7 exist with the agreed roles.
    Creates any missing account (memorable password), then deactivates the
    legacy first-run accounts (admin_radius, lead_hunter_*) so the login roster
    is clean. NEVER deletes. Returns the list of accounts created this call."""
    project_dir = Path(project_dir)
    raw = os.environ.get("CRM_ROSTER_JSON")
    roster: List[Dict[str, str]] = _default_crm_roster()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed:
                roster = parsed
        except Exception as e:
            log.error("CRM_ROSTER_JSON invalid, using defaults: %s", e)

    created: List[Dict[str, str]] = []
    admin_ok = False
    for r in roster:
        uname = (r.get("username") or "").strip()
        role = r.get("role") or "user"
        if not uname:
            continue
        pw = r.get("password") or _random_password()
        email = r.get("email") or f"{uname}@{_EMAIL_DOMAIN}"
        pw_hash = generate_password_hash(pw, method="pbkdf2:sha256:260000")
        try:
            uid, created_now = UserRepo.create_if_absent(uname, email, pw_hash, role)
            if created_now:
                created.append({"username": uname, "password": pw, "role": role,
                                "email": email, "user_id": str(uid)})
                # Force a password change on first login (shared default passwords).
                try:
                    UserRepo.set_must_change_pw(uid, True)
                except Exception:
                    pass
                if r.get("full_name"):
                    try:
                        UserRepo.update_profile(uid, full_name=r.get("full_name"))
                    except Exception:
                        pass
            if role == "admin":
                admin_ok = True
        except Exception as e:
            log.error("ensure_crm_roster: failed to provision %s: %s", uname, e)

    # Only retire legacy accounts once a fresh admin is confirmed present.
    if admin_ok:
        try:
            for u in UserRepo.list_all():
                un = u.get("username", "")
                if (un == "admin_radius" or un.startswith("lead_hunter_")) and u.get("is_active"):
                    UserRepo.set_active(int(u["id"]), False)
                    log.info("ensure_crm_roster: deactivated legacy account %s", un)
        except Exception as e:
            log.warning("ensure_crm_roster: legacy retirement skipped: %s", e)

    if created:
        try:
            _write_named_credentials(project_dir, _CRM_ROSTER_FILE, created)
        except Exception as e:
            log.warning("ensure_crm_roster: could not write creds file: %s", e)
        log.info("ensure_crm_roster: created %d account(s); creds at %s",
                 len(created), _CRM_ROSTER_FILE)
    return created


def _write_named_credentials(project_dir: Path, filename: str, users: List[Dict[str, str]]) -> None:
    path = project_dir / filename
    lines = [
        "# LEAD_FORGE CRM roster credentials",
        f"# Generated: {datetime.utcnow().isoformat()}Z",
        "# SECURITY: plaintext — delete after copying; change passwords in User Management.",
        f"{'ROLE':<8} {'USERNAME':<12} {'PASSWORD':<22} EMAIL",
    ]
    for u in users:
        lines.append(f"{u['role']:<8} {u['username']:<12} {u['password']:<22} {u['email']}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    try:
        os.chmod(path, 0o600)
    except Exception:
        pass


# ── Flask wiring ────────────────────────────────────────────────────────────


def configure_app(app: Flask, project_dir: str | Path) -> None:
    """Attach LoginManager + provision the CRM roster. Call once at app factory time."""
    secret = os.environ.get("SECRET_KEY")
    if not secret:
        # Generate a stable per-process fallback so sessions work in dev;
        # production MUST set SECRET_KEY explicitly.
        secret = secrets.token_hex(32)
        log.warning("SECRET_KEY not set — generated ephemeral key. "
                    "Sessions will not survive restart. Set SECRET_KEY in env.")
    app.secret_key = secret
    login_manager.init_app(app)

    # 2026-06-15: provision the ADMIN/MANAGER/BDE roster (idempotent). Replaces
    # the old random first-run seed for the CRM layer; fails gracefully if DB down.
    try:
        ensure_crm_roster(project_dir)
    except Exception as e:
        log.error("ensure_crm_roster skipped: %s", e)
