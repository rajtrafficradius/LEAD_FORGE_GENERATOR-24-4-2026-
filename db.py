"""MySQL layer for LEAD_FORGE Phase 2.

Responsibilities:
  * Pool connections via PyMySQL + DBUtils.PooledDB
  * Create/verify schema (users, run_history, master_leads)
  * Repositories: UserRepo, RunHistoryRepo, MasterLeadRepo

Environment variables consumed (priority order):
  1. Discrete: MYSQLHOST, MYSQLPORT, MYSQLUSER, MYSQLPASSWORD, MYSQLDATABASE
  2. URL:     MYSQL_URL
  3. URL:     MYSQL_PUBLIC_URL  (Railway's proxy — used from laptop/CI)

Fail-closed policy: if MYSQL_* is set but unreachable, the first repo call
raises DBUnavailable. Callers (wsgi.py /generate) translate this to HTTP 503.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, unquote

try:
    import pymysql
    from pymysql.cursors import DictCursor
    from dbutils.pooled_db import PooledDB
    _DEPS_OK = True
    _DEPS_ERR: Optional[str] = None
except ImportError as _e:  # pragma: no cover - import-time only
    _DEPS_OK = False
    _DEPS_ERR = str(_e)

log = logging.getLogger("leadforge.db")

# ── Exceptions ───────────────────────────────────────────────────────────────


class DBUnavailable(RuntimeError):
    """Raised when MySQL is configured but not reachable."""


class DBConfigError(RuntimeError):
    """Raised when MySQL env vars are missing or malformed."""


# ── Config resolution ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @classmethod
    def from_env(cls) -> "DBConfig":
        """Build a DBConfig from environment. Discrete vars win; then MYSQL_URL;
        then MYSQL_PUBLIC_URL. Raises DBConfigError if none usable."""
        host = os.environ.get("MYSQLHOST") or ""
        user = os.environ.get("MYSQLUSER") or ""
        password = os.environ.get("MYSQLPASSWORD") or ""
        database = os.environ.get("MYSQLDATABASE") or ""
        port_str = os.environ.get("MYSQLPORT") or ""

        if host and user and database:
            try:
                port = int(port_str) if port_str else 3306
            except ValueError:
                port = 3306
            return cls(host=host, port=port, user=user, password=password, database=database)

        for url_var in ("MYSQL_URL", "MYSQL_PUBLIC_URL", "DATABASE_URL"):
            url = os.environ.get(url_var)
            if url:
                return cls._parse_url(url)

        raise DBConfigError(
            "MySQL not configured. Set MYSQLHOST/MYSQLUSER/MYSQLPASSWORD/MYSQLDATABASE "
            "or MYSQL_URL (or MYSQL_PUBLIC_URL for local/proxy access)."
        )

    @staticmethod
    def _parse_url(url: str) -> "DBConfig":
        """mysql://user:pass@host:port/dbname → DBConfig."""
        parsed = urlparse(url)
        if parsed.scheme not in ("mysql", "mysql+pymysql"):
            raise DBConfigError(f"Unsupported URL scheme: {parsed.scheme}")
        host = parsed.hostname or ""
        user = unquote(parsed.username or "")
        password = unquote(parsed.password or "")
        port = parsed.port or 3306
        database = (parsed.path or "").lstrip("/")
        if not (host and user and database):
            raise DBConfigError(f"Malformed MySQL URL: missing host/user/database")
        return DBConfig(host=host, port=port, user=user, password=password, database=database)


# ── Pool lifecycle ───────────────────────────────────────────────────────────

_pool: Optional[Any] = None          # PooledDB instance
_pool_lock = threading.Lock()
_pool_error: Optional[str] = None    # last connection error string


def init_pool(size: int = 12) -> None:
    """Create the shared PyMySQL connection pool. Idempotent."""
    global _pool, _pool_error
    if not _DEPS_OK:
        _pool_error = f"PyMySQL/DBUtils not installed: {_DEPS_ERR}"
        raise DBUnavailable(_pool_error)
    with _pool_lock:
        if _pool is not None:
            return
        cfg = DBConfig.from_env()
        try:
            _pool = PooledDB(
                creator=pymysql,
                maxconnections=size,
                mincached=2,
                maxcached=4,
                blocking=True,
                # ping=4 → re-check the connection before EVERY query. Railway's
                # MySQL closes idle connections after ~60s, and PooledDB's
                # ping=1 only re-checks on borrow which still hands out stale
                # sockets. With ping=4, PyMySQL transparently reconnects when
                # the server has dropped the link, eliminating the
                # "MySQL server has gone away" errors that surfaced as
                # "database connectivity keeps getting cut off". Reset
                # behaviour also covered: stale conns are dropped, new ones
                # are pulled from the pool.
                ping=4,
                host=cfg.host,
                port=cfg.port,
                user=cfg.user,
                password=cfg.password,
                database=cfg.database,
                charset="utf8mb4",
                autocommit=False,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30,
                cursorclass=DictCursor,
            )
            # Eager connect to surface errors early
            with _pool.connection() as _probe:
                with _probe.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            _pool_error = None
            log.info("MySQL pool initialized (%s:%s/%s, size=%d)",
                     cfg.host, cfg.port, cfg.database, size)
        except Exception as e:
            _pool = None
            # 2026-05-21: include the active host/port/user/db on failure so
            # the operator can immediately see WHICH MySQL we couldn't reach
            # (was previously just "init failed: …" with no context).
            _pool_error = (
                f"{type(e).__name__}: {e} "
                f"[host={cfg.host}:{cfg.port}, db={cfg.database}, user={cfg.user}]"
            )
            log.error("MySQL pool init failed: %s", _pool_error)
            raise DBUnavailable(_pool_error) from e


def is_available() -> bool:
    """Return True if the pool is healthy (has completed init_pool)."""
    return _pool is not None


def pool_error() -> Optional[str]:
    return _pool_error


@contextmanager
def get_conn():
    """Borrow a pooled connection. Caller manages its own transactions.

    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    if _pool is None:
        # Lazy init on first use (covers gunicorn preload edge cases)
        init_pool()
    if _pool is None:
        raise DBUnavailable(_pool_error or "pool not initialized")
    conn = _pool.connection()
    try:
        yield conn
    finally:
        try:
            conn.close()   # returns to pool
        except Exception:
            pass


# ── Schema ───────────────────────────────────────────────────────────────────

_SCHEMA_SQL: Sequence[str] = (
    """
    CREATE TABLE IF NOT EXISTS users (
        id            INT UNSIGNED NOT NULL AUTO_INCREMENT,
        username      VARCHAR(64)  NOT NULL,
        email         VARCHAR(255) NOT NULL,
        password_hash VARCHAR(255) NOT NULL,
        role          ENUM('admin','manager','bde','user') NOT NULL DEFAULT 'user',
        full_name     VARCHAR(128) NULL,
        mobile_e164   VARCHAR(24)  NULL,
        must_change_pw TINYINT(1)  NOT NULL DEFAULT 0,
        is_active     TINYINT(1)   NOT NULL DEFAULT 1,
        created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
        last_login_at DATETIME NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_username (username),
        UNIQUE KEY uk_email (email)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS run_history (
        id                       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        user_id                  INT UNSIGNED    NOT NULL,
        job_uuid                 CHAR(8)         NOT NULL,
        industry                 VARCHAR(128)    NOT NULL,
        country                  VARCHAR(8)      NOT NULL,
        mode                     ENUM('industry','city') NOT NULL DEFAULT 'industry',
        min_volume               INT,
        min_cpc                  DECIMAL(6,2),
        max_leads                INT,
        enrichment_enabled       TINYINT(1)      NOT NULL,
        secondary_agent_used     TINYINT(1)      NOT NULL DEFAULT 0,
        competitor_depth_reached TINYINT         NOT NULL DEFAULT 0,
        leads_total              INT             NOT NULL DEFAULT 0,
        leads_new                INT             NOT NULL DEFAULT 0,
        leads_deduped_out        INT             NOT NULL DEFAULT 0,
        duration_seconds         INT             NOT NULL DEFAULT 0,
        api_usage_json           JSON,
        state                    ENUM('running','done','cancelled','error') NOT NULL,
        error_text               TEXT,
        started_at               DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
        finished_at              DATETIME NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_job (job_uuid),
        KEY idx_user_started (user_id, started_at),
        CONSTRAINT fk_run_user FOREIGN KEY (user_id) REFERENCES users(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS master_leads (
        id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        normalized_name   VARCHAR(255)    NOT NULL,
        root_domain       VARCHAR(255)    NOT NULL,
        display_name      VARCHAR(255),
        company_name      VARCHAR(255),
        role              VARCHAR(255),
        phone_e164        VARCHAR(20),
        primary_email     VARCHAR(320),
        email_type        VARCHAR(16),
        linkedin_url      VARCHAR(512),
        traffic_source    ENUM('paid','organic','competitor','secondary') NOT NULL DEFAULT 'paid',
        industry          VARCHAR(128)    NOT NULL,
        country           VARCHAR(8)      NOT NULL,
        organic_traffic   INT             NOT NULL DEFAULT 0,
        paid_traffic      INT             NOT NULL DEFAULT 0,
        organic_keywords  INT             NOT NULL DEFAULT 0,
        paid_keywords     INT             NOT NULL DEFAULT 0,
        revenue           BIGINT          NOT NULL DEFAULT 0,
        first_seen_run_id BIGINT UNSIGNED NOT NULL,
        first_seen_at     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
        payload_json      JSON,
        assigned_bde_id   INT UNSIGNED NULL,
        assigned_by_id    INT UNSIGNED NULL,
        assigned_at       DATETIME NULL,
        alloc_status      ENUM('unassigned','assigned','contacted','secured') NOT NULL DEFAULT 'unassigned',
        contacted_at      DATETIME NULL,
        secured_at        DATETIME NULL,
        outcome           VARCHAR(32) NULL,
        followup_at       DATETIME NULL,
        notes             MEDIUMTEXT NULL,
        PRIMARY KEY (id),
        UNIQUE KEY uk_name_domain (normalized_name, root_domain),
        KEY idx_root_domain (root_domain),
        KEY idx_industry_country (industry, country),
        KEY idx_first_seen_at (first_seen_at),
        KEY idx_assigned_bde (assigned_bde_id),
        CONSTRAINT fk_master_run FOREIGN KEY (first_seen_run_id) REFERENCES run_history(id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS allocation_events (
        id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        lead_id   BIGINT UNSIGNED NOT NULL,
        action    ENUM('assign','reassign','contacted','manual_add','unassign','secured') NOT NULL,
        actor_id  INT UNSIGNED NULL,
        bde_id    INT UNSIGNED NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_alloc_lead (lead_id),
        KEY idx_alloc_bde (bde_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS lead_enrichment (
        lead_id      BIGINT UNSIGNED NOT NULL,
        crawl_status ENUM('pending','crawling','done','error') NOT NULL DEFAULT 'pending',
        crawl_json   JSON,
        apollo_json  JSON,
        ai_summary_json JSON,
        crawled_at   DATETIME NULL,
        error_text   TEXT,
        updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (lead_id),
        KEY idx_enrich_status (crawl_status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS lead_calls (
        id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        lead_id      BIGINT UNSIGNED NULL,
        call_uuid    VARCHAR(96) NOT NULL,
        direction    ENUM('inbound','outbound','unknown') NOT NULL DEFAULT 'unknown',
        from_number  VARCHAR(40),
        to_number    VARCHAR(40),
        bde_user_id  INT UNSIGNED NULL,
        agent_name   VARCHAR(128),
        started_at   DATETIME NULL,
        duration_sec INT NOT NULL DEFAULT 0,
        recording_url VARCHAR(768),
        transcript   MEDIUMTEXT,
        raw_json     JSON,
        created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        UNIQUE KEY uk_call_uuid (call_uuid),
        KEY idx_call_lead (lead_id),
        KEY idx_call_bde (bde_user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
        id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        user_id    INT UNSIGNED NULL,
        username   VARCHAR(64),
        action     VARCHAR(48) NOT NULL,
        detail     VARCHAR(512),
        lead_id    BIGINT UNSIGNED NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_audit_created (created_at),
        KEY idx_audit_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    """,
)


# Idempotent column adds for upgrading older DBs already created without metrics.
_SCHEMA_MIGRATIONS: Sequence[Tuple[str, str, str]] = (
    ("master_leads", "organic_traffic",  "INT NOT NULL DEFAULT 0"),
    ("master_leads", "paid_traffic",     "INT NOT NULL DEFAULT 0"),
    ("master_leads", "organic_keywords", "INT NOT NULL DEFAULT 0"),
    ("master_leads", "paid_keywords",    "INT NOT NULL DEFAULT 0"),
    ("master_leads", "revenue",          "BIGINT NOT NULL DEFAULT 0"),
    # 2026-06-08: per-lead dollar cost, frozen at run finalize. NULL on purpose
    # so leads inserted BEFORE this feature show blank (no cost) in the UI.
    ("master_leads", "cost_per_lead_usd", "DECIMAL(12,4) NULL"),
    # Per-run dollar cost + per-lead split, frozen with the prices active at run time.
    ("run_history",  "cost_usd",          "DECIMAL(12,4) NULL"),
    ("run_history",  "cost_per_lead_usd", "DECIMAL(12,4) NULL"),
    # 2026-06-15 (CRM layer): user profile + per-lead allocation. All ADD COLUMN,
    # backward-compatible — existing rows get NULL / 'unassigned' defaults.
    ("users",        "full_name",         "VARCHAR(128) NULL"),
    ("users",        "mobile_e164",       "VARCHAR(24) NULL"),
    # 2026-06-17: BDE's 3CX extension/DN — the extension click-to-call rings.
    ("users",        "threecx_ext",       "VARCHAR(16) NULL"),
    ("master_leads", "assigned_bde_id",   "INT UNSIGNED NULL"),
    ("master_leads", "assigned_by_id",    "INT UNSIGNED NULL"),
    ("master_leads", "assigned_at",       "DATETIME NULL"),
    ("master_leads", "alloc_status",      "ENUM('unassigned','assigned','contacted') NOT NULL DEFAULT 'unassigned'"),
    ("master_leads", "contacted_at",      "DATETIME NULL"),
    # 2026-06-16: 'secured' funnel stage for the allocation board.
    ("master_leads", "secured_at",        "DATETIME NULL"),
    # 2026-06-16 (CRM v2): per-lead notes / outcome / follow-up + forced pw change.
    ("master_leads", "outcome",           "VARCHAR(32) NULL"),
    ("master_leads", "followup_at",       "DATETIME NULL"),
    ("master_leads", "notes",             "MEDIUMTEXT NULL"),
    ("users",        "must_change_pw",    "TINYINT(1) NOT NULL DEFAULT 0"),
)


# 2026-06-15: raw idempotent migrations the ADD-COLUMN runner can't express —
# the role-ENUM widening, the allocation index, and the allocation_events table
# for DBs created before the CRM layer. Each runs inside its own try/except so a
# already-applied step (or a benign "duplicate key" error) never blocks boot.
_SCHEMA_MIGRATIONS_RAW: Sequence[str] = (
    # Widen the role enum (was ENUM('admin','user')). Idempotent: re-running a
    # MODIFY to the same definition is a no-op.
    "ALTER TABLE users MODIFY COLUMN role "
    "ENUM('admin','manager','bde','user') NOT NULL DEFAULT 'user'",
    # Index for the BDE 'my leads' scoped query. CREATE INDEX errors if it
    # already exists → caught and ignored by the runner.
    "CREATE INDEX idx_assigned_bde ON master_leads (assigned_bde_id)",
    # 2026-06-16: widen alloc_status to add 'secured', and the allocation_events
    # action enum likewise. Idempotent MODIFYs (no-op if already widened).
    "ALTER TABLE master_leads MODIFY COLUMN alloc_status "
    "ENUM('unassigned','assigned','contacted','secured') NOT NULL DEFAULT 'unassigned'",
    "ALTER TABLE allocation_events MODIFY COLUMN action "
    "ENUM('assign','reassign','contacted','manual_add','unassign','secured') NOT NULL",
)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) AS c FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s",
        (table, column),
    )
    row = cur.fetchone()
    return bool(row and int(row.get("c", 0)) > 0)


def init_schema() -> None:
    """Create the three tables if they don't exist. Safe to call on every boot.
    Also runs idempotent ALTER TABLE migrations for newly added columns."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            for ddl in _SCHEMA_SQL:
                cur.execute(ddl)
            for table, col, coldef in _SCHEMA_MIGRATIONS:
                if not _column_exists(cur, table, col):
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coldef}")
                    log.info("Migrated: ADD COLUMN %s.%s", table, col)
            # Raw idempotent migrations (ENUM widen, index) — each isolated so a
            # benign "already exists" never blocks the others or the boot.
            for raw_sql in _SCHEMA_MIGRATIONS_RAW:
                try:
                    cur.execute(raw_sql)
                    log.info("Migrated (raw): %s", raw_sql[:60])
                except Exception as _mig_e:
                    log.debug("Raw migration skipped (likely already applied): %s", _mig_e)
        conn.commit()
    log.info("Schema verified: users, run_history, master_leads, allocation_events")


# ── User repo ────────────────────────────────────────────────────────────────


class UserRepo:
    """CRUD for users table. All methods are class-level (thin wrappers)."""

    # 2026-06-15: valid roles for the CRM layer.
    VALID_ROLES = ("admin", "manager", "bde", "user")

    @staticmethod
    def get_by_username(username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, email, password_hash, role, full_name, "
                    "mobile_e164, threecx_ext, must_change_pw, is_active, last_login_at "
                    "FROM users WHERE username = %s AND is_active = 1",
                    (username,),
                )
                return cur.fetchone()

    @staticmethod
    def get_by_id(user_id: int) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, email, role, full_name, mobile_e164, threecx_ext, "
                    "must_change_pw, is_active, last_login_at "
                    "FROM users WHERE id = %s",
                    (user_id,),
                )
                return cur.fetchone()

    @staticmethod
    def create(username: str, email: str, password_hash: str, role: str = "user") -> int:
        if role not in UserRepo.VALID_ROLES:
            raise ValueError(f"invalid role: {role}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (username, email, password_hash, role) VALUES (%s,%s,%s,%s)",
                    (username, email, password_hash, role),
                )
                new_id = cur.lastrowid
            conn.commit()
            return new_id

    @staticmethod
    def create_if_absent(username: str, email: str, password_hash: str, role: str = "user") -> Tuple[int, bool]:
        """Returns (id, created_now). Idempotent — safe for seed scripts."""
        existing = UserRepo.get_by_username(username)
        if existing:
            return existing["id"], False
        return UserRepo.create(username, email, password_hash, role), True

    @staticmethod
    def touch_last_login(user_id: int) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET last_login_at = NOW() WHERE id = %s", (user_id,))
            conn.commit()

    @staticmethod
    def list_all() -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, username, email, role, full_name, mobile_e164, "
                    "is_active, created_at, last_login_at "
                    "FROM users ORDER BY id ASC"
                )
                return list(cur.fetchall() or [])

    @staticmethod
    def list_by_role(role: str, active_only: bool = True) -> List[Dict[str, Any]]:
        """All users of a role (e.g. every BDE) — drives allocation + user mgmt."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = ("SELECT id, username, email, role, full_name, mobile_e164, "
                       "is_active, created_at, last_login_at FROM users WHERE role = %s")
                if active_only:
                    sql += " AND is_active = 1"
                sql += " ORDER BY id ASC"
                cur.execute(sql, (role,))
                return list(cur.fetchall() or [])

    @staticmethod
    def count() -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM users")
                row = cur.fetchone()
                return int(row["c"]) if row else 0

    @staticmethod
    def set_role(user_id: int, role: str) -> None:
        if role not in UserRepo.VALID_ROLES:
            raise ValueError(f"invalid role: {role}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET role=%s WHERE id=%s", (role, user_id))
            conn.commit()

    @staticmethod
    def set_active(user_id: int, is_active: bool) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET is_active=%s WHERE id=%s",
                            (1 if is_active else 0, user_id))
            conn.commit()

    @staticmethod
    def set_password(user_id: int, password_hash: str, clear_force: bool = True) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if clear_force:
                    cur.execute("UPDATE users SET password_hash=%s, must_change_pw=0 WHERE id=%s",
                                (password_hash, user_id))
                else:
                    cur.execute("UPDATE users SET password_hash=%s WHERE id=%s",
                                (password_hash, user_id))
            conn.commit()

    @staticmethod
    def set_must_change_pw(user_id: int, must: bool) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET must_change_pw=%s WHERE id=%s",
                            (1 if must else 0, user_id))
            conn.commit()

    @staticmethod
    def update_profile(user_id: int, full_name: Optional[str] = None,
                       email: Optional[str] = None, mobile_e164: Optional[str] = None,
                       threecx_ext: Optional[str] = None) -> None:
        sets, params = [], []
        if full_name is not None:
            sets.append("full_name=%s"); params.append(full_name)
        if email is not None:
            sets.append("email=%s"); params.append(email)
        if mobile_e164 is not None:
            sets.append("mobile_e164=%s"); params.append(mobile_e164)
        if threecx_ext is not None:
            sets.append("threecx_ext=%s"); params.append(threecx_ext)
        if not sets:
            return
        params.append(user_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE users SET {', '.join(sets)} WHERE id=%s", tuple(params))
            conn.commit()

    @staticmethod
    def set_mobile(user_id: int, mobile_e164: str) -> None:
        """BDE self-service mobile number update."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET mobile_e164=%s WHERE id=%s",
                            (mobile_e164, user_id))
            conn.commit()


# ── Run-history repo ─────────────────────────────────────────────────────────


class RunHistoryRepo:
    """One row per pipeline run. Start → Finish lifecycle."""

    @staticmethod
    def start(
        user_id: int,
        job_uuid: str,
        industry: str,
        country: str,
        mode: str,
        min_volume: Optional[int],
        min_cpc: Optional[float],
        max_leads: Optional[int],
        enrichment_enabled: bool,
    ) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO run_history "
                    "(user_id, job_uuid, industry, country, mode, min_volume, min_cpc, "
                    " max_leads, enrichment_enabled, state) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'running')",
                    (user_id, job_uuid, industry, country, mode,
                     min_volume, min_cpc, max_leads, 1 if enrichment_enabled else 0),
                )
                new_id = cur.lastrowid
            conn.commit()
            return new_id

    @staticmethod
    def finish(
        run_id: int,
        state: str,
        leads_total: int = 0,
        leads_new: int = 0,
        leads_deduped_out: int = 0,
        duration_seconds: int = 0,
        secondary_agent_used: bool = False,
        competitor_depth_reached: int = 0,
        api_usage: Optional[Dict[str, int]] = None,
        error_text: Optional[str] = None,
        cost_usd: Optional[float] = None,
        cost_per_lead_usd: Optional[float] = None,
    ) -> None:
        if state not in ("done", "cancelled", "error"):
            raise ValueError(f"invalid state: {state}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE run_history SET state=%s, leads_total=%s, leads_new=%s, "
                    "leads_deduped_out=%s, duration_seconds=%s, secondary_agent_used=%s, "
                    "competitor_depth_reached=%s, api_usage_json=%s, error_text=%s, "
                    "cost_usd=%s, cost_per_lead_usd=%s, "
                    "finished_at=NOW() WHERE id=%s",
                    (
                        state, leads_total, leads_new, leads_deduped_out,
                        duration_seconds, 1 if secondary_agent_used else 0,
                        competitor_depth_reached,
                        json.dumps(api_usage or {}, default=str),
                        error_text,
                        cost_usd, cost_per_lead_usd,
                        run_id,
                    ),
                )
            conn.commit()

    @staticmethod
    def recent(limit: int = 50, user_id: Optional[int] = None) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if user_id is None:
                    cur.execute(
                        "SELECT rh.*, u.username FROM run_history rh "
                        "JOIN users u ON rh.user_id = u.id "
                        "ORDER BY rh.id DESC LIMIT %s",
                        (int(limit),),
                    )
                else:
                    cur.execute(
                        "SELECT rh.*, u.username FROM run_history rh "
                        "JOIN users u ON rh.user_id = u.id "
                        "WHERE rh.user_id = %s ORDER BY rh.id DESC LIMIT %s",
                        (user_id, int(limit)),
                    )
                return list(cur.fetchall() or [])

    @staticmethod
    def list_page(page: int = 1, page_size: int = 50) -> Tuple[List[Dict[str, Any]], int]:
        """Returns (rows, total_count) for the given page (1-indexed)."""
        page = max(1, int(page))
        page_size = max(1, min(200, int(page_size)))
        offset = (page - 1) * page_size
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM run_history")
                total = int((cur.fetchone() or {}).get("c", 0))
                cur.execute(
                    "SELECT rh.id, rh.job_uuid, rh.industry, rh.country, rh.mode, "
                    "rh.min_cpc, rh.max_leads, rh.leads_total, rh.leads_new, "
                    "rh.leads_deduped_out, rh.duration_seconds, rh.state, "
                    "rh.secondary_agent_used, rh.competitor_depth_reached, "
                    "rh.cost_usd, rh.cost_per_lead_usd, "
                    "rh.started_at, rh.finished_at, rh.error_text, "
                    "u.username "
                    "FROM run_history rh "
                    "JOIN users u ON rh.user_id = u.id "
                    "ORDER BY rh.id DESC LIMIT %s OFFSET %s",
                    (page_size, offset),
                )
                rows = list(cur.fetchall() or [])
        return rows, total


def clear_lead_data() -> Dict[str, int]:
    """Wipes master_leads then run_history (FK-safe order). Users are preserved.
    Returns {'master_leads': N, 'run_history': N} — rows deleted."""
    deleted = {"master_leads": 0, "run_history": 0}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS c FROM master_leads")
            deleted["master_leads"] = int((cur.fetchone() or {}).get("c", 0))
            cur.execute("SELECT COUNT(*) AS c FROM run_history")
            deleted["run_history"] = int((cur.fetchone() or {}).get("c", 0))
            cur.execute("DELETE FROM master_leads")
            cur.execute("DELETE FROM run_history")
            cur.execute("ALTER TABLE master_leads AUTO_INCREMENT = 1")
            cur.execute("ALTER TABLE run_history AUTO_INCREMENT = 1")
        conn.commit()
    log.info("clear_lead_data: deleted %s", deleted)
    return deleted


# ── Master-lead repo ─────────────────────────────────────────────────────────


class MasterLeadRepo:
    """Append-only store of every unique (normalized_name, root_domain) ever seen."""

    @staticmethod
    def existing_keys(keys: Iterable[Tuple[str, str]]) -> set[Tuple[str, str]]:
        """Given an iterable of (norm_name, root_domain) pairs, return the subset
        already present in master_leads. Batched in 500s to stay well under
        `max_allowed_packet` and under MySQL's IN()-list perf cliff."""
        key_list = [(n, d) for n, d in keys if n and d]
        if not key_list:
            return set()
        found: set[Tuple[str, str]] = set()
        BATCH = 500
        with get_conn() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(key_list), BATCH):
                    batch = key_list[i:i + BATCH]
                    # (n,d) IN ((..,..),(..,..),...) — prepared safely
                    placeholders = ",".join(["(%s,%s)"] * len(batch))
                    flat: List[str] = []
                    for n, d in batch:
                        flat.append(n)
                        flat.append(d)
                    sql = (
                        "SELECT normalized_name, root_domain FROM master_leads "
                        f"WHERE (normalized_name, root_domain) IN ({placeholders})"
                    )
                    cur.execute(sql, tuple(flat))
                    for row in cur.fetchall() or []:
                        found.add((row["normalized_name"], row["root_domain"]))
        return found

    @staticmethod
    def existing_by_email_domain(pairs: Iterable[Tuple[str, str]]) -> set:
        """Check (email, root_domain) pairs. Returns subset already in master_leads.
        Used as a fallback dedup for leads with obfuscated/incomplete names."""
        key_list = [(e.lower().strip(), d) for e, d in pairs if e and d]
        if not key_list:
            return set()
        found: set = set()
        BATCH = 500
        with get_conn() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(key_list), BATCH):
                    batch = key_list[i:i + BATCH]
                    placeholders = ",".join(["(%s,%s)"] * len(batch))
                    flat: List[str] = []
                    for e, d in batch:
                        flat.append(e)
                        flat.append(d)
                    sql = (
                        "SELECT primary_email, root_domain FROM master_leads "
                        "WHERE primary_email IS NOT NULL AND primary_email != '' "
                        f"AND (primary_email, root_domain) IN ({placeholders})"
                    )
                    cur.execute(sql, tuple(flat))
                    for row in cur.fetchall() or []:
                        found.add((
                            (row["primary_email"] or "").lower().strip(),
                            row["root_domain"],
                        ))
        return found

    @staticmethod
    def domain_counts(domains: Iterable[str]) -> Dict[str, int]:
        """2026-05-28: return {root_domain: existing_lead_count} for the given
        domains. Powers the HYBRID cross-run dedup: a company is allowed a
        LIFETIME maximum of N contacts across ALL runs. The caller compares
        this count against the per-domain cap and drops overflow.

        Only domains with >= 1 existing row appear in the result; absent
        domains imply count 0. Batched in 500s like existing_keys()."""
        dom_list = [d for d in {(_d or "").strip().lower() for _d in domains} if d]
        if not dom_list:
            return {}
        counts: Dict[str, int] = {}
        BATCH = 500
        with get_conn() as conn:
            with conn.cursor() as cur:
                for i in range(0, len(dom_list), BATCH):
                    batch = dom_list[i:i + BATCH]
                    placeholders = ",".join(["%s"] * len(batch))
                    sql = (
                        "SELECT root_domain, COUNT(*) AS c FROM master_leads "
                        f"WHERE root_domain IN ({placeholders}) "
                        "GROUP BY root_domain"
                    )
                    cur.execute(sql, tuple(batch))
                    for row in cur.fetchall() or []:
                        counts[row["root_domain"]] = int(row["c"])
        return counts

    @staticmethod
    def bulk_insert_new(
        rows: Sequence[Dict[str, Any]],
        run_id: int,
        industry: str,
        country: str,
    ) -> int:
        """UPSERT master rows. Returns count of NEW rows inserted (excludes updates).

        On duplicate key (same normalized_name + root_domain), fills in any field
        that was previously NULL/empty — phone, email, role, LinkedIn URL,
        SEMrush metrics, etc. Existing non-empty values are preserved so a future
        enrichment pass cannot wipe earlier good data.

        MySQL's executemany rowcount counts INSERTs as 1 and UPDATEs as 2 with
        ON DUPLICATE KEY UPDATE (when the row actually changes). To return only
        truly-new rows, we INSERT IGNORE first and capture rowcount, then run a
        second pass that updates the duplicates with COALESCE-fill semantics.

        Each row dict must have keys:
          normalized_name, root_domain, display_name, company_name, role,
          phone_e164, primary_email, email_type, linkedin_url,
          traffic_source, organic_traffic, paid_traffic, organic_keywords,
          paid_keywords, revenue, payload_json (dict or None)
        """
        if not rows:
            return 0
        valid = [r for r in rows if r.get("normalized_name") and r.get("root_domain")]
        if not valid:
            return 0

        # PHASE 2 FIX (2026-04-28) — name-upgrade pre-pass.
        # User scenario: run 1 (enrichment OFF) stores ("matt", "acme.com").
        # Run 2 (enrichment ON) resolves the same person to "matt cornell" and
        # tries to insert ("matt cornell", "acme.com") — DIFFERENT unique key,
        # so naive INSERT IGNORE leaves the old single-name record orphaned with
        # no phone/email. Before INSERT, we look up existing single-name records
        # on each incoming domain and rename them to the incoming full name when
        # the first word matches. This keeps the master DB additive: the same
        # person is one row, not two.
        upgrades: list[tuple[str, str, str]] = []  # (old_norm, new_norm, root_domain)
        domains_with_full_names: dict[str, list[str]] = {}
        for r in valid:
            nn = r.get("normalized_name") or ""
            rd = r.get("root_domain") or ""
            if " " in nn:  # only full-name leads can upgrade single-name records
                domains_with_full_names.setdefault(rd, []).append(nn)
        if domains_with_full_names:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    for rd, full_names in domains_with_full_names.items():
                        # Existing single-name rows on this domain
                        cur.execute(
                            "SELECT normalized_name FROM master_leads "
                            "WHERE root_domain=%s AND normalized_name NOT LIKE '%% %%'",
                            (rd,),
                        )
                        existing_singles = {row["normalized_name"] for row in (cur.fetchall() or [])}
                        if not existing_singles:
                            continue
                        for full in full_names:
                            first_word = full.split(" ", 1)[0]
                            if first_word in existing_singles and first_word != full:
                                # Don't collide with an already-existing full-name row.
                                cur.execute(
                                    "SELECT 1 FROM master_leads WHERE normalized_name=%s "
                                    "AND root_domain=%s LIMIT 1",
                                    (full, rd),
                                )
                                if cur.fetchone():
                                    continue  # full name already exists separately — skip
                                upgrades.append((first_word, full, rd))
                                existing_singles.discard(first_word)  # don't upgrade twice
                    if upgrades:
                        cur.executemany(
                            "UPDATE master_leads SET normalized_name=%s "
                            "WHERE normalized_name=%s AND root_domain=%s",
                            [(new, old, rd) for (old, new, rd) in upgrades],
                        )
                conn.commit()
            if upgrades:
                log.info("Name-upgrade pass: %d single-name record(s) renamed to full name", len(upgrades))

        sql = (
            "INSERT IGNORE INTO master_leads "
            "(normalized_name, root_domain, display_name, company_name, role, "
            " phone_e164, primary_email, email_type, linkedin_url, traffic_source, "
            " industry, country, organic_traffic, paid_traffic, organic_keywords, "
            " paid_keywords, revenue, cost_per_lead_usd, first_seen_run_id, payload_json) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )

        def _safe_int(v) -> int:
            try:
                if v is None or v == "":
                    return 0
                return int(float(v))
            except (TypeError, ValueError):
                return 0

        def _safe_cost(v):
            """Cost is a nullable DECIMAL — pass None through as SQL NULL (blank)."""
            if v is None or v == "":
                return None
            try:
                return round(float(v), 4)
            except (TypeError, ValueError):
                return None

        params = []
        for r in valid:
            payload = r.get("payload_json")
            if isinstance(payload, dict):
                payload = json.dumps(payload, default=str)
            ts = r.get("traffic_source", "paid")
            if ts not in ("paid", "organic", "competitor", "secondary"):
                ts = "paid"
            params.append((
                r["normalized_name"], r["root_domain"],
                r.get("display_name"), r.get("company_name"), r.get("role"),
                r.get("phone_e164"), r.get("primary_email"), r.get("email_type"),
                r.get("linkedin_url"),
                ts,
                industry, country,
                _safe_int(r.get("organic_traffic")),
                _safe_int(r.get("paid_traffic")),
                _safe_int(r.get("organic_keywords")),
                _safe_int(r.get("paid_keywords")),
                _safe_int(r.get("revenue")),
                _safe_cost(r.get("cost_per_lead_usd")),
                run_id,
                payload,
            ))
        # PHASE 2 — fill-on-update SQL. Updates the existing row only when the
        # incoming value is non-empty AND the existing column is NULL or '' (or 0
        # for numeric metrics). This makes re-runs additive: a later enrichment
        # pass can fill in phone/email that was missing on the first capture
        # without ever overwriting earlier good data.
        update_sql = (
            "UPDATE master_leads SET "
            " display_name     = COALESCE(NULLIF(display_name, ''), %s), "
            " company_name     = COALESCE(NULLIF(company_name, ''), %s), "
            " role             = COALESCE(NULLIF(role, ''), %s), "
            " phone_e164       = COALESCE(NULLIF(phone_e164, ''), %s), "
            " primary_email    = COALESCE(NULLIF(primary_email, ''), %s), "
            " email_type       = COALESCE(NULLIF(email_type, ''), %s), "
            " linkedin_url     = COALESCE(NULLIF(linkedin_url, ''), %s), "
            " organic_traffic  = IF(organic_traffic  > 0, organic_traffic,  %s), "
            " paid_traffic     = IF(paid_traffic     > 0, paid_traffic,     %s), "
            " organic_keywords = IF(organic_keywords > 0, organic_keywords, %s), "
            " paid_keywords    = IF(paid_keywords    > 0, paid_keywords,    %s), "
            " revenue          = IF(revenue          > 0, revenue,          %s) "
            "WHERE normalized_name=%s AND root_domain=%s"
        )

        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, params)
                inserted = cur.rowcount  # MySQL: rows actually inserted
                # Fill-on-update pass for every input row. INSERTs already wrote
                # the data so the COALESCE/IF guards make this a safe no-op for
                # them; UPDATEs fill in any previously-empty fields.
                update_params = []
                for r in valid:
                    update_params.append((
                        r.get("display_name"),
                        r.get("company_name"),
                        r.get("role"),
                        r.get("phone_e164"),
                        r.get("primary_email"),
                        r.get("email_type"),
                        r.get("linkedin_url"),
                        _safe_int(r.get("organic_traffic")),
                        _safe_int(r.get("paid_traffic")),
                        _safe_int(r.get("organic_keywords")),
                        _safe_int(r.get("paid_keywords")),
                        _safe_int(r.get("revenue")),
                        r["normalized_name"], r["root_domain"],
                    ))
                cur.executemany(update_sql, update_params)
            conn.commit()
            return int(inserted or 0)

    @staticmethod
    def total_count() -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM master_leads")
                row = cur.fetchone()
                return int(row["c"]) if row else 0

    @staticmethod
    def list_page(page: int = 1, page_size: int = 50) -> Tuple[List[Dict[str, Any]], int]:
        """Returns (rows, total_count) for the given page (1-indexed)."""
        page = max(1, int(page))
        page_size = max(1, min(200, int(page_size)))
        offset = (page - 1) * page_size
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM master_leads")
                total = int((cur.fetchone() or {}).get("c", 0))
                cur.execute(
                    "SELECT id, normalized_name, root_domain, display_name, company_name, "
                    "role, phone_e164, primary_email, email_type, traffic_source, "
                    "industry, country, organic_traffic, paid_traffic, organic_keywords, "
                    "paid_keywords, revenue, cost_per_lead_usd, first_seen_at "
                    "FROM master_leads ORDER BY id DESC LIMIT %s OFFSET %s",
                    (page_size, offset),
                )
                rows = list(cur.fetchall() or [])
        return rows, total

    @staticmethod
    def count_by_industry(industry: str, country: Optional[str] = None) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                if country:
                    cur.execute(
                        "SELECT COUNT(*) AS c FROM master_leads WHERE industry=%s AND country=%s",
                        (industry, country),
                    )
                else:
                    cur.execute(
                        "SELECT COUNT(*) AS c FROM master_leads WHERE industry=%s",
                        (industry,),
                    )
                row = cur.fetchone()
                return int(row["c"]) if row else 0

    # ── CRM v2 (2026-06-16): search / filter / edit / follow-ups / KPIs ──────
    _SEARCH_COLS = ("ml.id, ml.normalized_name, ml.root_domain, ml.display_name, "
                    "ml.company_name, ml.role, ml.phone_e164, ml.primary_email, "
                    "ml.email_type, ml.linkedin_url, ml.traffic_source, ml.industry, "
                    "ml.country, ml.revenue, ml.cost_per_lead_usd, ml.alloc_status, "
                    "ml.assigned_bde_id, ml.contacted_at, ml.secured_at, ml.followup_at, "
                    "ml.outcome, ml.first_seen_at")
    _EDITABLE = ("display_name", "company_name", "role", "phone_e164", "primary_email",
                 "email_type", "linkedin_url", "industry", "notes", "outcome")

    @staticmethod
    def _build_where(q, industry, status, bde_id):
        where, args = [], []
        if q:
            where.append("(ml.company_name LIKE %s OR ml.display_name LIKE %s OR "
                         "ml.root_domain LIKE %s OR ml.primary_email LIKE %s)")
            like = "%" + q + "%"; args += [like, like, like, like]
        if industry:
            where.append("ml.industry=%s"); args.append(industry)
        if status in ("unassigned", "assigned", "contacted", "secured"):
            where.append("ml.alloc_status=%s"); args.append(status)
        if bde_id:
            where.append("ml.assigned_bde_id=%s"); args.append(int(bde_id))
        return ((" WHERE " + " AND ".join(where)) if where else ""), args

    @staticmethod
    def _fmt_lead(r):
        for k in ("contacted_at", "secured_at", "followup_at", "first_seen_at"):
            if r.get(k) and hasattr(r[k], "isoformat"):
                r[k] = r[k].isoformat()
        if r.get("cost_per_lead_usd") is not None:
            try: r["cost_per_lead_usd"] = float(r["cost_per_lead_usd"])
            except (TypeError, ValueError): r["cost_per_lead_usd"] = None
        return r

    @staticmethod
    def search_page(q=None, industry=None, status=None, bde_id=None,
                    page: int = 1, page_size: int = 50):
        page = max(1, int(page)); page_size = max(1, min(200, int(page_size)))
        offset = (page - 1) * page_size
        wsql, args = MasterLeadRepo._build_where(q, industry, status, bde_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM master_leads ml" + wsql, tuple(args))
                total = int((cur.fetchone() or {}).get("c", 0))
                cur.execute(
                    "SELECT " + MasterLeadRepo._SEARCH_COLS + ", u.username AS assigned_bde_username "
                    "FROM master_leads ml LEFT JOIN users u ON ml.assigned_bde_id=u.id"
                    + wsql + " ORDER BY ml.id DESC LIMIT %s OFFSET %s",
                    tuple(args) + (page_size, offset),
                )
                rows = [MasterLeadRepo._fmt_lead(r) for r in (cur.fetchall() or [])]
        return rows, total

    @staticmethod
    def export_rows(q=None, industry=None, status=None, bde_id=None, limit: int = 10000):
        wsql, args = MasterLeadRepo._build_where(q, industry, status, bde_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT " + MasterLeadRepo._SEARCH_COLS + ", u.username AS assigned_bde_username "
                    "FROM master_leads ml LEFT JOIN users u ON ml.assigned_bde_id=u.id"
                    + wsql + " ORDER BY ml.id DESC LIMIT %s",
                    tuple(args) + (int(limit),),
                )
                return [MasterLeadRepo._fmt_lead(r) for r in (cur.fetchall() or [])]

    @staticmethod
    def distinct_industries() -> List[str]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT industry FROM master_leads "
                            "WHERE industry IS NOT NULL AND industry<>'' ORDER BY industry")
                return [r["industry"] for r in (cur.fetchall() or [])]

    @staticmethod
    def get_one(lead_id: int) -> Optional[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM master_leads WHERE id=%s", (lead_id,))
                return cur.fetchone()

    @staticmethod
    def update_fields(lead_id: int, fields: Dict[str, Any]) -> int:
        """Manual edit — overwrite the whitelisted columns with provided values."""
        sets, args = [], []
        for k, v in (fields or {}).items():
            if k in MasterLeadRepo._EDITABLE:
                if k == "revenue":
                    continue  # handled below (numeric)
                sets.append(f"{k}=%s"); args.append(v if v != "" else None)
        if "revenue" in (fields or {}):
            try:
                sets.append("revenue=%s"); args.append(int(float(fields["revenue"] or 0)))
            except (TypeError, ValueError):
                pass
        if not sets:
            return 0
        args.append(lead_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE master_leads SET {', '.join(sets)} WHERE id=%s", tuple(args))
                n = cur.rowcount
            conn.commit()
            return int(n or 0)

    @staticmethod
    def set_followup(lead_id: int, outcome: Optional[str], followup_at: Optional[str],
                     notes: Optional[str]) -> None:
        sets, args = [], []
        if outcome is not None:
            sets.append("outcome=%s"); args.append(outcome or None)
        if followup_at is not None:
            sets.append("followup_at=%s"); args.append(followup_at or None)
        if notes is not None:
            sets.append("notes=%s"); args.append(notes or None)
        if not sets:
            return
        args.append(lead_id)
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"UPDATE master_leads SET {', '.join(sets)} WHERE id=%s", tuple(args))
            conn.commit()

    @staticmethod
    def due_followups(bde_id: Optional[int] = None, limit: int = 200) -> List[Dict[str, Any]]:
        """Leads with a follow-up due now/overdue (optionally for one BDE)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                sql = ("SELECT ml.id, ml.display_name, ml.company_name, ml.root_domain, "
                       "ml.phone_e164, ml.primary_email, ml.alloc_status, ml.outcome, "
                       "ml.followup_at, ml.assigned_bde_id "
                       "FROM master_leads ml WHERE ml.followup_at IS NOT NULL "
                       "AND ml.followup_at <= NOW() + INTERVAL 1 DAY ")
                args: list = []
                if bde_id:
                    sql += "AND ml.assigned_bde_id=%s "; args.append(int(bde_id))
                sql += "ORDER BY ml.followup_at ASC LIMIT %s"; args.append(int(limit))
                cur.execute(sql, tuple(args))
                rows = list(cur.fetchall() or [])
        for r in rows:
            if r.get("followup_at") and hasattr(r["followup_at"], "isoformat"):
                r["followup_at"] = r["followup_at"].isoformat()
        return rows

    @staticmethod
    def enrich_core_from_apollo(lead_id: int, apollo: Dict[str, Any]) -> None:
        """Fill BLANK core columns (company/industry/revenue) from the Apollo org
        object so the Database view fills up as the crawler runs. Never clobbers."""
        if not apollo:
            return
        company = apollo.get("name") or ""
        industry = apollo.get("industry") or ""
        rev = 0
        for k in ("annual_revenue", "organization_revenue", "estimated_annual_revenue"):
            try:
                if apollo.get(k):
                    rev = int(float(apollo[k])); break
            except (TypeError, ValueError):
                pass
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE master_leads SET "
                    " company_name = COALESCE(NULLIF(company_name,''), %s), "
                    " industry     = COALESCE(NULLIF(industry,''), %s), "
                    " revenue      = IF(revenue>0, revenue, %s) "
                    "WHERE id=%s",
                    (company or None, industry or None, rev, lead_id),
                )
            conn.commit()

    @staticmethod
    def kpi_overview() -> Dict[str, Any]:
        """Funnel totals + per-BDE leaderboard + call stats for the manager dashboard."""
        out: Dict[str, Any] = {"funnel": {}, "leaderboard": [], "calls": {}}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS total, "
                    "SUM(alloc_status<>'unassigned') AS allocated, "
                    "SUM(alloc_status IN ('contacted','secured')) AS contacted, "
                    "SUM(alloc_status='secured') AS secured FROM master_leads")
                f = cur.fetchone() or {}
                out["funnel"] = {k: int(f.get(k) or 0) for k in ("total", "allocated", "contacted", "secured")}
                cur.execute(
                    "SELECT u.id, u.username, u.full_name, "
                    "COUNT(ml.id) AS allocated, "
                    "SUM(ml.alloc_status IN ('contacted','secured')) AS contacted, "
                    "SUM(ml.alloc_status='secured') AS secured "
                    "FROM users u LEFT JOIN master_leads ml ON ml.assigned_bde_id=u.id "
                    "WHERE u.role='bde' GROUP BY u.id ORDER BY secured DESC, contacted DESC")
                lb = []
                for r in (cur.fetchall() or []):
                    lb.append({"id": r["id"], "username": r["username"],
                               "full_name": r.get("full_name") or "",
                               "allocated": int(r.get("allocated") or 0),
                               "contacted": int(r.get("contacted") or 0),
                               "secured": int(r.get("secured") or 0)})
                out["leaderboard"] = lb
                cur.execute("SELECT COUNT(*) AS c, COALESCE(SUM(duration_sec),0) AS secs FROM lead_calls")
                c = cur.fetchone() or {}
                out["calls"] = {"total": int(c.get("c") or 0), "total_seconds": int(c.get("secs") or 0)}
        return out


# ── Audit-log repo (CRM v2, 2026-06-16) ───────────────────────────────────────


class AuditRepo:
    """Lightweight activity trail: who did what, when. Best-effort (never blocks)."""

    @staticmethod
    def log(user_id: Optional[int], username: str, action: str,
            detail: str = "", lead_id: Optional[int] = None) -> None:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO audit_log (user_id, username, action, detail, lead_id) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (user_id, (username or "")[:64], action[:48], (detail or "")[:512], lead_id),
                    )
                conn.commit()
        except Exception as e:
            log.debug("audit log skipped: %s", e)

    @staticmethod
    def recent(limit: int = 100) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, user_id, username, action, detail, lead_id, created_at "
                            "FROM audit_log ORDER BY id DESC LIMIT %s", (int(limit),))
                rows = list(cur.fetchall() or [])
        for r in rows:
            if r.get("created_at") and hasattr(r["created_at"], "isoformat"):
                r["created_at"] = r["created_at"].isoformat()
        return rows


# ── Lead-allocation repo (CRM layer, 2026-06-15) ──────────────────────────────


class LeadAllocationRepo:
    """Allocation of master_leads to BDE users. Additive — never touches the
    lead-generation flow. All methods class-level, mirroring the other repos."""

    @staticmethod
    def _event(cur, lead_id: int, action: str, actor_id: Optional[int], bde_id: Optional[int]) -> None:
        cur.execute(
            "INSERT INTO allocation_events (lead_id, action, actor_id, bde_id) "
            "VALUES (%s,%s,%s,%s)",
            (lead_id, action, actor_id, bde_id),
        )

    @staticmethod
    def assign(lead_id: int, bde_id: int, actor_id: Optional[int]) -> None:
        """Assign / reassign one lead to a BDE."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE master_leads SET assigned_bde_id=%s, assigned_by_id=%s, "
                    "assigned_at=NOW(), alloc_status='assigned', contacted_at=NULL "
                    "WHERE id=%s",
                    (bde_id, actor_id, lead_id),
                )
                LeadAllocationRepo._event(cur, lead_id, "reassign", actor_id, bde_id)
            conn.commit()

    @staticmethod
    def unassign(lead_id: int, actor_id: Optional[int]) -> None:
        """Return one lead to the unassigned database pool."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT assigned_bde_id FROM master_leads WHERE id=%s",
                    (lead_id,),
                )
                row = cur.fetchone() or {}
                previous_bde = row.get("assigned_bde_id")
                cur.execute(
                    "UPDATE master_leads SET assigned_bde_id=NULL, assigned_by_id=NULL, "
                    "assigned_at=NULL, alloc_status='unassigned', contacted_at=NULL, secured_at=NULL "
                    "WHERE id=%s",
                    (lead_id,),
                )
                LeadAllocationRepo._event(
                    cur,
                    lead_id,
                    "unassign",
                    actor_id,
                    int(previous_bde) if previous_bde else None,
                )
            conn.commit()

    @staticmethod
    def mark_contacted(lead_id: int, actor_id: Optional[int]) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE master_leads SET alloc_status='contacted', contacted_at=NOW() "
                    "WHERE id=%s",
                    (lead_id,),
                )
                LeadAllocationRepo._event(cur, lead_id, "contacted", actor_id, None)
            conn.commit()

    @staticmethod
    def mark_secured(lead_id: int, actor_id: Optional[int]) -> None:
        """Final funnel stage — deal won. Stamps secured_at (keeps contacted_at)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE master_leads SET alloc_status='secured', secured_at=NOW(), "
                    "contacted_at=COALESCE(contacted_at, NOW()) WHERE id=%s",
                    (lead_id,),
                )
                LeadAllocationRepo._event(cur, lead_id, "secured", actor_id, None)
            conn.commit()

    @staticmethod
    def set_alloc_status(lead_id: int, status: str, actor_id: Optional[int]) -> None:
        """Set the allocation funnel stage to an explicit value — powers the
        TOGGLE/revert in the UI (e.g. un-contact a lead clicked by mistake).
        status: 'assigned' (back to just allocated) | 'contacted' | 'secured'."""
        if status == "contacted":
            sql = ("UPDATE master_leads SET alloc_status='contacted', "
                   "contacted_at=NOW(), secured_at=NULL WHERE id=%s")
            ev = "contacted"
        elif status == "secured":
            sql = ("UPDATE master_leads SET alloc_status='secured', secured_at=NOW(), "
                   "contacted_at=COALESCE(contacted_at, NOW()) WHERE id=%s")
            ev = "secured"
        elif status == "assigned":
            sql = ("UPDATE master_leads SET alloc_status='assigned', "
                   "contacted_at=NULL, secured_at=NULL WHERE id=%s")
            ev = "assign"
        else:
            raise ValueError(f"invalid alloc status: {status}")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (lead_id,))
                LeadAllocationRepo._event(cur, lead_id, ev, actor_id, None)
            conn.commit()

    @staticmethod
    def unassign(lead_id: int, actor_id: Optional[int]) -> None:
        """Send a lead back to the database pool (deallocate from its BDE).
        Resets the allocation funnel to 'unassigned' (clean slate)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE master_leads SET assigned_bde_id=NULL, assigned_by_id=NULL, "
                    "assigned_at=NULL, alloc_status='unassigned', contacted_at=NULL, "
                    "secured_at=NULL WHERE id=%s",
                    (lead_id,),
                )
                LeadAllocationRepo._event(cur, lead_id, "unassign", actor_id, None)
            conn.commit()

    @staticmethod
    def unassigned_count(industry: Optional[str] = None) -> int:
        """How many leads are currently in the pool (unassigned)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                if industry:
                    cur.execute("SELECT COUNT(*) AS c FROM master_leads "
                                "WHERE alloc_status='unassigned' AND industry=%s", (industry,))
                else:
                    cur.execute("SELECT COUNT(*) AS c FROM master_leads "
                                "WHERE alloc_status='unassigned'")
                return int((cur.fetchone() or {}).get("c", 0))

    @staticmethod
    def auto_distribute(bde_ids: Sequence[int], actor_id: Optional[int],
                        industry: Optional[str] = None, limit: int = 100000) -> Dict[int, int]:
        """Round-robin every UNASSIGNED lead equally across `bde_ids` (up to
        `limit` leads total). Returns {bde_id: count_assigned}. Equal ±1.
        For 'N per BDE' the caller passes limit = N * len(bde_ids)."""
        if not bde_ids:
            return {}
        assigned: Dict[int, int] = {b: 0 for b in bde_ids}
        with get_conn() as conn:
            with conn.cursor() as cur:
                if industry:
                    cur.execute(
                        "SELECT id FROM master_leads WHERE alloc_status='unassigned' "
                        "AND industry=%s ORDER BY id ASC LIMIT %s",
                        (industry, int(limit)),
                    )
                else:
                    cur.execute(
                        "SELECT id FROM master_leads WHERE alloc_status='unassigned' "
                        "ORDER BY id ASC LIMIT %s",
                        (int(limit),),
                    )
                lead_ids = [int(r["id"]) for r in (cur.fetchall() or [])]
                for i, lid in enumerate(lead_ids):
                    bde = bde_ids[i % len(bde_ids)]
                    cur.execute(
                        "UPDATE master_leads SET assigned_bde_id=%s, assigned_by_id=%s, "
                        "assigned_at=NOW(), alloc_status='assigned' WHERE id=%s",
                        (bde, actor_id, lid),
                    )
                    LeadAllocationRepo._event(cur, lid, "assign", actor_id, bde)
                    assigned[bde] += 1
            conn.commit()
        return assigned

    @staticmethod
    def counts_by_bde() -> Dict[int, int]:
        """{bde_id: number of leads currently assigned} — drives the Allocation UI."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT assigned_bde_id AS bde, COUNT(*) AS c FROM master_leads "
                    "WHERE assigned_bde_id IS NOT NULL GROUP BY assigned_bde_id"
                )
                return {int(r["bde"]): int(r["c"]) for r in (cur.fetchall() or [])}

    @staticmethod
    def bde_stats() -> Dict[int, Dict[str, int]]:
        """Per-BDE funnel: {bde_id: {total, contacted, secured, calls}}.
        total = all leads assigned; contacted = contacted-or-secured (been
        worked); secured = deals won; calls = 3CX calls placed/taken by them."""
        stats: Dict[int, Dict[str, int]] = {}
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT assigned_bde_id AS bde, COUNT(*) AS total, "
                    "SUM(alloc_status IN ('contacted','secured')) AS contacted, "
                    "SUM(alloc_status='secured') AS secured "
                    "FROM master_leads WHERE assigned_bde_id IS NOT NULL "
                    "GROUP BY assigned_bde_id"
                )
                for r in (cur.fetchall() or []):
                    stats[int(r["bde"])] = {
                        "total": int(r["total"] or 0),
                        "contacted": int(r["contacted"] or 0),
                        "secured": int(r["secured"] or 0),
                        "calls": 0,
                    }
                cur.execute(
                    "SELECT bde_user_id AS bde, COUNT(*) AS c FROM lead_calls "
                    "WHERE bde_user_id IS NOT NULL GROUP BY bde_user_id"
                )
                for r in (cur.fetchall() or []):
                    b = int(r["bde"])
                    stats.setdefault(b, {"total": 0, "contacted": 0, "secured": 0, "calls": 0})
                    stats[b]["calls"] = int(r["c"] or 0)
        return stats

    @staticmethod
    def list_for_bde(bde_id: int, page: int = 1, page_size: int = 50) -> Tuple[List[Dict[str, Any]], int]:
        """Paginated leads assigned to one BDE (their restricted dashboard)."""
        page = max(1, int(page))
        page_size = max(1, min(200, int(page_size)))
        offset = (page - 1) * page_size
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS c FROM master_leads WHERE assigned_bde_id=%s",
                    (bde_id,),
                )
                total = int((cur.fetchone() or {}).get("c", 0))
                cur.execute(
                    "SELECT ml.id, ml.normalized_name, ml.root_domain, ml.display_name, "
                    "ml.company_name, ml.role, ml.phone_e164, ml.primary_email, ml.email_type, "
                    "ml.traffic_source, ml.industry, ml.country, ml.revenue, ml.alloc_status, "
                    "ml.assigned_at, ml.contacted_at, ml.secured_at, ml.first_seen_at, "
                    "(SELECT COUNT(*) FROM lead_calls lc WHERE lc.lead_id=ml.id) AS call_count "
                    "FROM master_leads ml WHERE ml.assigned_bde_id=%s "
                    "ORDER BY ml.id DESC LIMIT %s OFFSET %s",
                    (bde_id, page_size, offset),
                )
                rows = list(cur.fetchall() or [])
        for r in rows:
            for k in ("assigned_at", "contacted_at", "secured_at", "first_seen_at"):
                if r.get(k) and hasattr(r[k], "isoformat"):
                    r[k] = r[k].isoformat()
        return rows, total

    @staticmethod
    def _ensure_manual_run(cur, actor_id: Optional[int]) -> int:
        """Singleton run_history row that owns manually-added leads (FK target)."""
        cur.execute("SELECT id FROM run_history WHERE job_uuid=%s", ("manual00",))
        row = cur.fetchone()
        if row:
            return int(row["id"])
        uid = actor_id
        if not uid:
            cur.execute("SELECT MIN(id) AS m FROM users")
            r = cur.fetchone()
            uid = int(r["m"]) if r and r.get("m") else 1
        cur.execute(
            "INSERT INTO run_history (user_id, job_uuid, industry, country, mode, "
            "enrichment_enabled, state, leads_total) "
            "VALUES (%s,'manual00','Manual','AU','industry',0,'done',0)",
            (uid,),
        )
        return int(cur.lastrowid)

    @staticmethod
    def manual_add(normalized_name: str, root_domain: str, display_name: str,
                   company_name: str, phone: str, email: str, industry: str,
                   country: str, actor_id: Optional[int], bde_id: Optional[int]) -> int:
        """Insert (or reuse) a manually-added lead; optionally allocate to a BDE.
        Returns the master_leads.id. Uses INSERT ... ON DUPLICATE KEY so a repeat
        (name,domain) returns the existing row rather than erroring."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                run_id = LeadAllocationRepo._ensure_manual_run(cur, actor_id)
                cur.execute(
                    "INSERT INTO master_leads "
                    "(normalized_name, root_domain, display_name, company_name, "
                    " phone_e164, primary_email, traffic_source, industry, country, "
                    " first_seen_run_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,'secondary',%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), "
                    "  display_name=COALESCE(NULLIF(display_name,''), VALUES(display_name)), "
                    "  company_name=COALESCE(NULLIF(company_name,''), VALUES(company_name)), "
                    "  phone_e164=COALESCE(NULLIF(phone_e164,''), VALUES(phone_e164)), "
                    "  primary_email=COALESCE(NULLIF(primary_email,''), VALUES(primary_email))",
                    (normalized_name, root_domain, display_name, company_name,
                     phone or None, email or None, industry, country, run_id),
                )
                lead_id = int(cur.lastrowid)
                LeadAllocationRepo._event(cur, lead_id, "manual_add", actor_id, bde_id)
                if bde_id:
                    cur.execute(
                        "UPDATE master_leads SET assigned_bde_id=%s, assigned_by_id=%s, "
                        "assigned_at=NOW(), alloc_status='assigned' WHERE id=%s",
                        (bde_id, actor_id, lead_id),
                    )
                    LeadAllocationRepo._event(cur, lead_id, "assign", actor_id, bde_id)
            conn.commit()
        return lead_id

    @staticmethod
    def recent_contacted(limit: int = 50) -> List[Dict[str, Any]]:
        """Latest 'contacted' events with lead + BDE — the ADMIN/MANAGER feed."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ml.id, ml.company_name, ml.root_domain, ml.assigned_bde_id, "
                    "u.username AS bde_username, ml.contacted_at "
                    "FROM master_leads ml LEFT JOIN users u ON ml.assigned_bde_id = u.id "
                    "WHERE ml.alloc_status='contacted' "
                    "ORDER BY ml.contacted_at DESC LIMIT %s",
                    (int(limit),),
                )
                return list(cur.fetchall() or [])


# ── Lead-enrichment repo (CRM Phase 2, 2026-06-15) ────────────────────────────


class LeadEnrichmentRepo:
    """Per-lead website-crawl + Apollo-free-data + OpenAI-analysis store. Driven
    by the standalone background worker (enrichment_worker.py) — completely
    separate from the lead-generation pipeline."""

    @staticmethod
    def backfill_pending() -> int:
        """Create a 'pending' enrichment row for every master_lead that doesn't
        have one yet. Cheap INSERT...SELECT; returns rows added."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT IGNORE INTO lead_enrichment (lead_id, crawl_status) "
                    "SELECT ml.id, 'pending' FROM master_leads ml "
                    "LEFT JOIN lead_enrichment le ON le.lead_id = ml.id "
                    "WHERE le.lead_id IS NULL"
                )
                n = cur.rowcount
            conn.commit()
            return int(n or 0)

    @staticmethod
    def claim_next() -> Optional[Dict[str, Any]]:
        """Atomically claim the next lead to enrich. Priority: leads assigned to
        a BDE first, then newest. Marks it 'crawling' and returns its
        {lead_id, root_domain, company_name, display_name} — or None if idle."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT le.lead_id, ml.root_domain, ml.company_name, ml.display_name "
                    "FROM lead_enrichment le JOIN master_leads ml ON le.lead_id = ml.id "
                    "WHERE le.crawl_status='pending' "
                    "ORDER BY (ml.assigned_bde_id IS NOT NULL) DESC, ml.id DESC LIMIT 1"
                )
                row = cur.fetchone()
                if not row:
                    return None
                cur.execute(
                    "UPDATE lead_enrichment SET crawl_status='crawling' "
                    "WHERE lead_id=%s AND crawl_status='pending'",
                    (row["lead_id"],),
                )
                claimed = cur.rowcount
            conn.commit()
            return row if claimed else None

    @staticmethod
    def save_result(lead_id: int, crawl_json: Any, apollo_json: Any,
                    ai_summary_json: Any) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE lead_enrichment SET crawl_status='done', crawl_json=%s, "
                    "apollo_json=%s, ai_summary_json=%s, crawled_at=NOW(), error_text=NULL "
                    "WHERE lead_id=%s",
                    (json.dumps(crawl_json or {}, default=str),
                     json.dumps(apollo_json or {}, default=str),
                     json.dumps(ai_summary_json or {}, default=str),
                     lead_id),
                )
            conn.commit()

    @staticmethod
    def save_error(lead_id: int, error_text: str) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE lead_enrichment SET crawl_status='error', "
                    "error_text=%s, crawled_at=NOW() WHERE lead_id=%s",
                    ((error_text or "")[:2000], lead_id),
                )
            conn.commit()

    @staticmethod
    def recrawl(lead_id: int) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO lead_enrichment (lead_id, crawl_status) VALUES (%s,'pending') "
                    "ON DUPLICATE KEY UPDATE crawl_status='pending', error_text=NULL",
                    (lead_id,),
                )
            conn.commit()

    @staticmethod
    def retry_errored() -> int:
        """Re-queue every 'error' (and stuck 'crawling') row → 'pending'."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE lead_enrichment SET crawl_status='pending', error_text=NULL "
                            "WHERE crawl_status IN ('error','crawling')")
                n = cur.rowcount
            conn.commit()
            return int(n or 0)

    @staticmethod
    def top_pending_ids(limit: int = 3) -> List[int]:
        """Lead ids (lowest id = oldest leads)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM master_leads ORDER BY id ASC LIMIT %s", (int(limit),))
                return [int(r["id"]) for r in (cur.fetchall() or [])]

    @staticmethod
    def failed_ai_ids(limit: int = 5000, include_fallback: bool = True) -> List[int]:
        """Crawled leads whose AI cheat-sheet failed/empty (e.g. bad OpenAI key) —
        candidates for cheap re-analysis (no re-crawl). When include_fallback is
        False, leads that already have an Apollo-derived fallback summary are
        excluded (so a bad key doesn't make the worker reprocess them forever);
        when True (e.g. after fixing the key) they're included so fallbacks get
        upgraded to the full AI cheat-sheet."""
        sql = ("SELECT lead_id FROM lead_enrichment WHERE crawl_status='done' AND "
               "(ai_summary_json IS NULL OR JSON_EXTRACT(ai_summary_json,'$._note') IS NOT NULL) "
               # Only leads that actually HAVE something to analyse — a lead with no
               # crawl text AND no Apollo data can never be improved by re-analysis,
               # so excluding it stops the worker looping on dead/blocked sites.
               "AND (CAST(JSON_EXTRACT(crawl_json,'$.fetched') AS UNSIGNED) > 0 "
               "     OR (apollo_json IS NOT NULL AND JSON_LENGTH(apollo_json) > 0))")
        if not include_fallback:
            sql += (" AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(ai_summary_json,'$._source')),'')"
                    " <> 'apollo-fallback'")
        sql += " ORDER BY lead_id ASC LIMIT %s"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (int(limit),))
                return [int(r["lead_id"]) for r in (cur.fetchall() or [])]

    @staticmethod
    def save_ai(lead_id: int, ai_json: Any) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE lead_enrichment SET ai_summary_json=%s WHERE lead_id=%s",
                            (json.dumps(ai_json or {}, default=str), lead_id))
            conn.commit()

    @staticmethod
    def get_crawl_apollo(lead_id: int) -> Optional[Dict[str, Any]]:
        """Stored crawl + Apollo JSON + lead identity, for re-analysis."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT le.crawl_json, le.apollo_json, ml.company_name, ml.display_name, "
                    "ml.root_domain FROM lead_enrichment le "
                    "JOIN master_leads ml ON ml.id=le.lead_id WHERE le.lead_id=%s",
                    (lead_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        for k in ("crawl_json", "apollo_json"):
            v = row.get(k)
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            if isinstance(v, str):
                try:
                    row[k] = json.loads(v)
                except Exception:
                    row[k] = {}
        return row

    @staticmethod
    def status_counts() -> Dict[str, int]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT crawl_status AS s, COUNT(*) AS c FROM lead_enrichment GROUP BY crawl_status"
                )
                out = {r["s"]: int(r["c"]) for r in (cur.fetchall() or [])}
                cur.execute("SELECT COUNT(*) AS c FROM master_leads")
                out["total_leads"] = int((cur.fetchone() or {}).get("c", 0))
                return out

    @staticmethod
    def list_by_status(status: str, limit: int = 250, offset: int = 0) -> List[Dict[str, Any]]:
        """Leads in a given crawl bucket (pending/crawling/done/error) with the
        details the Enrichment panel's drill-down needs. status='total'/'' lists
        every lead regardless of crawl state.

        Done in two steps — a cheap ORDER BY over just (lead_id, crawled_at) to
        pick the page of ids, then a PK fetch of the heavy/JSON columns with NO
        sort — because filesorting rows that carry JSON/TEXT columns blows
        Railway's tiny sort_buffer ('Out of sort memory')."""
        st = (status or "").strip().lower()
        if st in ("pending", "crawling", "done", "error"):
            id_sql = ("SELECT lead_id FROM lead_enrichment WHERE crawl_status=%s "
                      "ORDER BY crawled_at DESC, lead_id DESC LIMIT %s OFFSET %s")
            id_params: tuple = (st, int(limit), int(offset))
        else:
            id_sql = ("SELECT lead_id FROM lead_enrichment "
                      "ORDER BY lead_id DESC LIMIT %s OFFSET %s")
            id_params = (int(limit), int(offset))
        cols = ("le.lead_id, ml.root_domain AS domain, "
                "COALESCE(NULLIF(ml.company_name,''), NULLIF(ml.display_name,''), ml.root_domain) AS company, "
                "le.crawl_status, le.crawled_at, LEFT(le.error_text,300) AS error_text, "
                "CAST(JSON_EXTRACT(le.crawl_json,'$.fetched') AS UNSIGNED) AS pages, "
                "(le.apollo_json IS NOT NULL AND JSON_LENGTH(le.apollo_json) > 0) AS has_apollo, "
                "(le.ai_summary_json IS NOT NULL "
                " AND JSON_EXTRACT(le.ai_summary_json,'$._note') IS NULL "
                " AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(le.ai_summary_json,'$.summary')),'') <> '') AS ai_ok")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(id_sql, id_params)
                ids = [int(r["lead_id"]) for r in (cur.fetchall() or [])]
                if not ids:
                    return []
                order = {lid: i for i, lid in enumerate(ids)}
                ph = ",".join(["%s"] * len(ids))
                cur.execute(
                    f"SELECT {cols} FROM lead_enrichment le "
                    f"JOIN master_leads ml ON ml.id = le.lead_id WHERE le.lead_id IN ({ph})",
                    tuple(ids),
                )
                rows = cur.fetchall() or []
        rows.sort(key=lambda r: order.get(int(r["lead_id"]), 1 << 30))
        for r in rows:
            if r.get("crawled_at") and hasattr(r["crawled_at"], "isoformat"):
                r["crawled_at"] = r["crawled_at"].isoformat()
            r["has_apollo"] = bool(r.get("has_apollo"))
            r["ai_ok"] = bool(r.get("ai_ok"))
            if r.get("pages") is not None:
                r["pages"] = int(r["pages"])
            if r.get("error_text"):
                r["error_text"] = str(r["error_text"])[:300]
        return rows

    @staticmethod
    def requeue_oldest_stale(days: int) -> int:
        """Re-queue the single oldest 'done' lead whose crawl is older than `days`
        back to 'pending' so the crawler keeps running continuously and data stays
        fresh. Returns 1 if one was re-queued, else 0."""
        if not days or int(days) <= 0:
            return 0
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE lead_enrichment SET crawl_status='pending' "
                    "WHERE crawl_status='done' AND crawled_at IS NOT NULL "
                    "AND crawled_at < (NOW() - INTERVAL %s DAY) "
                    "ORDER BY crawled_at ASC LIMIT 1",
                    (int(days),),
                )
                n = cur.rowcount
            conn.commit()
            return int(n or 0)

    @staticmethod
    def get_full(lead_id: int) -> Optional[Dict[str, Any]]:
        """Everything the lead page needs: the master_leads row + its enrichment
        (crawl/apollo/ai JSON parsed) + assigned-BDE username."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT ml.*, le.crawl_status, le.crawl_json, le.apollo_json, "
                    "le.ai_summary_json, le.crawled_at, le.error_text AS enrich_error, "
                    "u.username AS assigned_bde_username, u.full_name AS assigned_bde_name "
                    "FROM master_leads ml "
                    "LEFT JOIN lead_enrichment le ON le.lead_id = ml.id "
                    "LEFT JOIN users u ON ml.assigned_bde_id = u.id "
                    "WHERE ml.id=%s",
                    (lead_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                for k in ("crawl_json", "apollo_json", "ai_summary_json", "payload_json"):
                    if isinstance(row.get(k), (bytes, bytearray)):
                        row[k] = row[k].decode("utf-8", "ignore")
                    if isinstance(row.get(k), str):
                        try:
                            row[k] = json.loads(row[k])
                        except Exception:
                            pass
                for k in ("first_seen_at", "assigned_at", "contacted_at", "crawled_at"):
                    if row.get(k) and hasattr(row[k], "isoformat"):
                        row[k] = row[k].isoformat()
                if row.get("cost_per_lead_usd") is not None:
                    try:
                        row["cost_per_lead_usd"] = float(row["cost_per_lead_usd"])
                    except (TypeError, ValueError):
                        row["cost_per_lead_usd"] = None
                return row


# ── Lead-calls repo (CRM Phase 3 — 3CX, 2026-06-15) ───────────────────────────


class LeadCallsRepo:
    """Per-lead 3CX call records (metadata + recording + transcript). Visible to
    ADMIN/MANAGER for all leads, and to a BDE only for their own leads."""

    @staticmethod
    def match_lead_by_phone(*numbers: str) -> Optional[int]:
        """Find a master_lead whose phone matches any of the given numbers by
        last-9-digit suffix (robust to +61/0 prefixes and formatting)."""
        sufs = []
        for n in numbers:
            d = re.sub(r"\D", "", n or "")
            if len(d) >= 7:
                sufs.append(d[-9:])
        if not sufs:
            return None
        with get_conn() as conn:
            with conn.cursor() as cur:
                for suf in sufs:
                    cur.execute(
                        "SELECT id FROM master_leads "
                        "WHERE phone_e164 IS NOT NULL AND phone_e164<>'' "
                        "AND RIGHT(REGEXP_REPLACE(phone_e164,'[^0-9]',''),9)=%s LIMIT 1",
                        (suf,),
                    )
                    row = cur.fetchone()
                    if row:
                        return int(row["id"])
        return None

    @staticmethod
    def match_bde_by_mobile(*numbers: str) -> Optional[int]:
        """Find the BDE whose saved mobile matches one of the numbers (suffix).
        This is how 'which mobile called which lead' is attributed."""
        sufs = []
        for n in numbers:
            d = re.sub(r"\D", "", n or "")
            if len(d) >= 7:
                sufs.append(d[-9:])
        if not sufs:
            return None
        with get_conn() as conn:
            with conn.cursor() as cur:
                for suf in sufs:
                    cur.execute(
                        "SELECT id FROM users WHERE role='bde' AND mobile_e164 IS NOT NULL "
                        "AND mobile_e164<>'' "
                        "AND RIGHT(REGEXP_REPLACE(mobile_e164,'[^0-9]',''),9)=%s LIMIT 1",
                        (suf,),
                    )
                    row = cur.fetchone()
                    if row:
                        return int(row["id"])
        return None

    @staticmethod
    def record(call: Dict[str, Any]) -> int:
        """Idempotent upsert keyed on call_uuid (safe for webhook retries)."""
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO lead_calls "
                    "(lead_id, call_uuid, direction, from_number, to_number, bde_user_id, "
                    " agent_name, started_at, duration_sec, recording_url, transcript, raw_json) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id), "
                    "  lead_id=COALESCE(VALUES(lead_id), lead_id), "
                    "  direction=VALUES(direction), duration_sec=VALUES(duration_sec), "
                    "  recording_url=COALESCE(NULLIF(VALUES(recording_url),''), recording_url), "
                    "  transcript=COALESCE(NULLIF(VALUES(transcript),''), transcript), "
                    "  bde_user_id=COALESCE(VALUES(bde_user_id), bde_user_id), "
                    "  raw_json=VALUES(raw_json)",
                    (call.get("lead_id"), call.get("call_uuid"),
                     call.get("direction", "unknown"), call.get("from_number"),
                     call.get("to_number"), call.get("bde_user_id"),
                     call.get("agent_name"), call.get("started_at"),
                     int(call.get("duration_sec", 0) or 0), call.get("recording_url"),
                     call.get("transcript"),
                     json.dumps(call.get("raw_json") or {}, default=str)),
                )
                call_id = int(cur.lastrowid)
            conn.commit()
            return call_id

    @staticmethod
    def set_transcript(call_id: int, transcript: str) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE lead_calls SET transcript=%s WHERE id=%s",
                            (transcript, call_id))
            conn.commit()

    @staticmethod
    def list_for_lead(lead_id: int) -> List[Dict[str, Any]]:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT lc.*, u.username AS bde_username, u.full_name AS bde_name, "
                    "u.mobile_e164 AS bde_mobile "
                    "FROM lead_calls lc LEFT JOIN users u ON lc.bde_user_id = u.id "
                    "WHERE lc.lead_id=%s ORDER BY lc.started_at DESC, lc.id DESC",
                    (lead_id,),
                )
                rows = list(cur.fetchall() or [])
        for r in rows:
            for k in ("started_at", "created_at"):
                if r.get(k) and hasattr(r[k], "isoformat"):
                    r[k] = r[k].isoformat()
            if isinstance(r.get("raw_json"), (bytes, bytearray)):
                r["raw_json"] = r["raw_json"].decode("utf-8", "ignore")
        return rows

    @staticmethod
    def count_for_lead(lead_id: int) -> int:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS c FROM lead_calls WHERE lead_id=%s", (lead_id,))
                return int((cur.fetchone() or {}).get("c", 0))


# ── Self-test ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    try:
        init_pool()
        init_schema()
        print(f"users: {UserRepo.count()}   master_leads: {MasterLeadRepo.total_count()}")
        print("db.py self-test OK")
    except (DBUnavailable, DBConfigError) as e:
        print(f"db.py self-test SKIPPED (not configured / unreachable): {e}")
