"""pricing.py — Per-API cost configuration + per-run / per-lead dollar maths.

Why this exists
---------------
The user wants the REAL dollar cost of every run and every lead. The APIs do
not expose "how much you paid", so the user enters it once per API:

    credits bought  ·  amount paid  ·  is it a monthly plan?

From that we derive a per-credit unit price (paid / credits) and multiply by
how many credits each run actually consumed. A run that consumes 0 of an API
pays $0 for it — which is exactly the "ignore SEMrush if it wasn't used this
run" behaviour, applied uniformly to every API (the `monthly` flag therefore
needs no special maths; it's stored for the user's own reference + UI).

Design decisions (locked with the user)
  • per-lead   = run_cost / ALL leads in the run
  • cost is FROZEN at run finalize and stored in the DB; editing prices later
    only affects FUTURE runs (so this module is only consulted at run time)
  • storage    = a local JSON file (`api_pricing.json`) next to this module.
    Resets on a Railway redeploy, but past run costs are already frozen in the
    DB, so history is never lost — the user just re-enters pricing once.

This is a PURE module: no network, no DB, no V5/city_pipeline imports, so it
can be imported anywhere without cycles.
"""
from __future__ import annotations

import json
import os
import threading
import time
from typing import Dict, Optional

# JSON store lives beside this file (project root).
_PRICING_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "api_pricing.json")

# Serialise reads/writes so a Save from the UI can't race a run's cost calc.
_LOCK = threading.RLock()

# Cheap in-process cache so per-run cost calc doesn't hit disk repeatedly.
_CACHE: Optional[dict] = None
_CACHE_MTIME: float = 0.0


# ── Line items ────────────────────────────────────────────────────────────────
# Each line item is one billable credit-pool. `key` must match the keys produced
# by usage-collection at finalize (see V5._v5_try_finalize). `group` lets the UI
# cluster the three Apollo pools under one heading.
#
#   credits : how many credits/units the plan bought       (user input)
#   paid    : how much money was paid for those credits     (user input)
#   monthly : True if it's a recurring monthly subscription (user input, ref only)
LINE_ITEMS = (
    {"key": "serpapi",       "label": "SerpAPI",            "group": "SerpAPI",       "unit": "searches"},
    {"key": "google_places", "label": "Google Places",      "group": "Google Places", "unit": "calls"},
    {"key": "semrush_units", "label": "SEMrush",            "group": "SEMrush",       "unit": "units"},
    {"key": "apollo_email",  "label": "Apollo — Email",     "group": "Apollo",        "unit": "email credits"},
    {"key": "apollo_phone",  "label": "Apollo — Phone",     "group": "Apollo",        "unit": "mobile credits"},
    {"key": "apollo_export", "label": "Apollo — Regular",   "group": "Apollo",        "unit": "export credits"},
    # Optional extras — only billed if enrichment is ON and the user fills them.
    {"key": "lusha",         "label": "Lusha",              "group": "Lusha",         "unit": "credits"},
    {"key": "hunter",        "label": "Hunter.io",          "group": "Hunter.io",     "unit": "requests"},
    {"key": "openai",        "label": "OpenAI",             "group": "OpenAI",        "unit": "calls"},
    {"key": "gemini",        "label": "Gemini",             "group": "Gemini",        "unit": "calls"},
)
_ITEM_KEYS = tuple(it["key"] for it in LINE_ITEMS)


def _default_item() -> dict:
    return {"credits": 0.0, "paid": 0.0, "monthly": False}


def default_pricing() -> dict:
    """A fresh, all-zero pricing config (so cost = $0 until the user fills it)."""
    return {
        "items": {k: _default_item() for k in _ITEM_KEYS},
        "currency": "$",
        "updated_at": None,
    }


# ── Validation / normalisation ────────────────────────────────────────────────

def _coerce_item(raw: object) -> dict:
    """Force any incoming item into {credits:float>=0, paid:float>=0, monthly:bool}."""
    out = _default_item()
    if isinstance(raw, dict):
        try:
            out["credits"] = max(0.0, float(raw.get("credits", 0) or 0))
        except (TypeError, ValueError):
            out["credits"] = 0.0
        try:
            out["paid"] = max(0.0, float(raw.get("paid", 0) or 0))
        except (TypeError, ValueError):
            out["paid"] = 0.0
        out["monthly"] = bool(raw.get("monthly", False))
    return out


def normalize(cfg: object) -> dict:
    """Return a clean config with every known line item present and valid.
    Unknown keys are dropped; missing keys are filled with zeros."""
    base = default_pricing()
    if isinstance(cfg, dict):
        items_in = cfg.get("items") if isinstance(cfg.get("items"), dict) else {}
        for k in _ITEM_KEYS:
            base["items"][k] = _coerce_item(items_in.get(k))
        cur = cfg.get("currency")
        if isinstance(cur, str) and cur.strip():
            base["currency"] = cur.strip()[:4]
        base["updated_at"] = cfg.get("updated_at")
    return base


# ── Persistence ───────────────────────────────────────────────────────────────

def load_pricing() -> dict:
    """Load pricing from disk (cached by mtime). Fail-open to all-zero defaults
    so cost maths never raises just because the file is missing/corrupt."""
    global _CACHE, _CACHE_MTIME
    with _LOCK:
        try:
            mtime = os.path.getmtime(_PRICING_PATH)
        except OSError:
            # No file yet → return defaults (do NOT create until the user saves).
            if _CACHE is None:
                _CACHE = default_pricing()
                _CACHE_MTIME = 0.0
            return json.loads(json.dumps(_CACHE))  # cheap deep copy
        if _CACHE is not None and mtime == _CACHE_MTIME:
            return json.loads(json.dumps(_CACHE))
        try:
            with open(_PRICING_PATH, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            _CACHE = normalize(raw)
        except (OSError, ValueError, json.JSONDecodeError):
            _CACHE = default_pricing()
        _CACHE_MTIME = mtime
        return json.loads(json.dumps(_CACHE))


def save_pricing(cfg: dict) -> dict:
    """Validate + persist a pricing config. Returns the normalised version."""
    global _CACHE, _CACHE_MTIME
    clean = normalize(cfg)
    clean["updated_at"] = int(time.time())
    with _LOCK:
        tmp = _PRICING_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(clean, fh, indent=2)
        os.replace(tmp, _PRICING_PATH)  # atomic on the same volume
        _CACHE = clean
        try:
            _CACHE_MTIME = os.path.getmtime(_PRICING_PATH)
        except OSError:
            _CACHE_MTIME = 0.0
    return json.loads(json.dumps(clean))


# ── Maths ─────────────────────────────────────────────────────────────────────

def unit_price(item: dict) -> float:
    """Dollars per single credit = paid / credits. 0 when credits unset."""
    item = _coerce_item(item)
    return (item["paid"] / item["credits"]) if item["credits"] > 0 else 0.0


def unit_prices(cfg: Optional[dict] = None) -> Dict[str, float]:
    """Flat {item_key: $/credit} map for every line item."""
    cfg = cfg or load_pricing()
    items = cfg.get("items", {})
    return {k: unit_price(items.get(k, {})) for k in _ITEM_KEYS}


def compute_run_cost(usage: Optional[dict], cfg: Optional[dict] = None) -> dict:
    """Total dollar cost of a run from its per-credit-type consumption.

    Args:
        usage: {item_key: credits_consumed_this_run}. Unknown keys ignored;
               missing keys treated as 0 (so a run that never touched SEMrush
               pays $0 for it — the "ignore if unused" rule).
        cfg:   pricing config (defaults to the saved file).

    Returns: {"total": float, "per_item": {key: float}, "currency": str}
    """
    cfg = cfg or load_pricing()
    prices = unit_prices(cfg)
    usage = usage or {}
    per_item: Dict[str, float] = {}
    total = 0.0
    for k in _ITEM_KEYS:
        try:
            consumed = float(usage.get(k, 0) or 0)
        except (TypeError, ValueError):
            consumed = 0.0
        line = round(consumed * prices[k], 6)
        per_item[k] = line
        total += line
    return {"total": round(total, 6), "per_item": per_item, "currency": cfg.get("currency", "$")}


def cost_per_lead(total: float, leads_total: int) -> float:
    """Run cost spread evenly over ALL leads in the run (user-chosen denominator)."""
    try:
        total = float(total or 0)
        n = int(leads_total or 0)
    except (TypeError, ValueError):
        return 0.0
    return round(total / n, 6) if n > 0 else 0.0


# ── Offline self-test ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    # unit price
    assert unit_price({"credits": 100, "paid": 50}) == 0.5
    assert unit_price({"credits": 0, "paid": 50}) == 0.0, "no credits → $0/credit"

    cfg = default_pricing()
    cfg["items"]["serpapi"]      = {"credits": 100, "paid": 50, "monthly": False}   # $0.50/search
    cfg["items"]["apollo_email"] = {"credits": 1000, "paid": 100, "monthly": True}  # $0.10/email
    cfg["items"]["semrush_units"]= {"credits": 1000, "paid": 200, "monthly": True}  # $0.20/unit

    # SerpAPI-only run (no SEMrush touched) → SEMrush contributes $0 even though it's a monthly plan.
    usage = {"serpapi": 10, "apollo_email": 4, "semrush_units": 0}
    rc = compute_run_cost(usage, cfg)
    assert abs(rc["total"] - (10 * 0.5 + 4 * 0.1 + 0)) < 1e-9, rc
    assert rc["per_item"]["semrush_units"] == 0.0, "monthly-but-unused → $0"
    assert abs(cost_per_lead(rc["total"], 5) - (5.4 / 5)) < 1e-9

    # round-trip persistence (write to a temp path, then restore)
    _orig = _PRICING_PATH
    try:
        globals()["_PRICING_PATH"] = _orig + ".selftest"
        saved = save_pricing(cfg)
        assert saved["items"]["serpapi"]["paid"] == 50
        reloaded = load_pricing()
        assert reloaded["items"]["serpapi"]["credits"] == 100
    finally:
        try:
            os.remove(_orig + ".selftest")
        except OSError:
            pass
        globals()["_PRICING_PATH"] = _orig
        globals()["_CACHE"] = None

    print("pricing.py self-test OK")
