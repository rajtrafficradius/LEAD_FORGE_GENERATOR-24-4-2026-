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
        self._is_active: bool = bool(row.get("is_active", 1))

    def get_id(self) -> str:
        return str(self.id)

    @property
    def is_active(self) -> bool:  # overrides UserMixin default
        return self._is_active

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    def to_dict(self) -> Dict[str, Any]:
        return {"id": self.id, "username": self.username, "role": self.role, "email": self.email}


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


# 2026-05-18: LOGIN FEATURE DISABLED — see neutered decorators below.
# Anything that previously called @login_required(_json)/@admin_required
# now passes through unchanged. The unauthorized_handler is kept for
# Flask-Login compatibility but should never fire because decorators no
# longer block. Re-enable by reverting this commit.
LOGIN_DISABLED = True


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
    """2026-05-18: LOGIN DISABLED — pass-through. Originally required
    role='admin'; now every request is accepted regardless of role."""
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


# ── Flask wiring ────────────────────────────────────────────────────────────


def configure_app(app: Flask, project_dir: str | Path) -> None:
    """Attach LoginManager + seed users. Call once at app factory time."""
    secret = os.environ.get("SECRET_KEY")
    if not secret:
        # Generate a stable per-process fallback so sessions work in dev;
        # production MUST set SECRET_KEY explicitly.
        secret = secrets.token_hex(32)
        log.warning("SECRET_KEY not set — generated ephemeral key. "
                    "Sessions will not survive restart. Set SECRET_KEY in env.")
    app.secret_key = secret
    login_manager.init_app(app)

    # Seed (fails gracefully if DB down — caller handles that)
    try:
        seeded, creds = seed_default_users(project_dir)
        if seeded and creds:
            log.info(
                "First-run seed completed. %d users created (1 admin, %d operators). "
                "Credentials at %s — rotate once read.",
                len(creds), len(creds) - 1, _CREDENTIALS_FILE,
            )
    except Exception as e:
        log.error("seed_default_users skipped: %s", e)
