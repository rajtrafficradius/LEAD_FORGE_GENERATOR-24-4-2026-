"""discovery_mode.py — Smart 4-mode SEMrush/Google Places discovery routing
(2026-05-21).

Why this exists
---------------
Earlier the Google Places integration ran ADDITIVELY alongside SEMrush. The
real-world need is a smart BACKUP:

  • If SEMrush is available + Google Places is available  →  run BOTH.
  • If SEMrush key missing                                  →  run GOOGLE_ONLY.
  • If Google key missing or non-AU                        →  run SEMRUSH_ONLY.
  • If neither key present                                  →  APOLLO_ONLY fallback.

A separate condition is the MID-RUN PIVOT: SEMrush starts fine but exhausts
its unit budget (or returns silent-scope) partway through. In that case the
pipeline should keep whatever SEMrush already produced AND pivot the
remaining discovery to Google Places.

This module is a pure utility (no network, no client deps) so both
`city_pipeline._discover_domains` (city mode) and
`V5._phase3_domain_discovery` (industry mode) can share one routing brain.

Avoid importing V5 or city_pipeline from here — those modules import V5 at
top-level which creates an import cycle.
"""
from __future__ import annotations

from enum import Enum
from typing import Callable, Optional, Tuple


# ── Mode enum ───────────────────────────────────────────────────────────────


class DiscoveryMode(str, Enum):
    """The routing decision for a single run.

    Inheriting from `str` lets us compare with bare strings AND pass the
    value straight into JSON / log lines without an .value lookup."""

    BOTH = "BOTH"
    SEMRUSH_ONLY = "SEMRUSH_ONLY"
    GOOGLE_ONLY = "GOOGLE_ONLY"
    APOLLO_ONLY = "APOLLO_ONLY"


# ── Public API ──────────────────────────────────────────────────────────────


def detect_initial_mode(api_keys: dict, country: str, disable_semrush: bool = False) -> Tuple[DiscoveryMode, str]:
    """Pick the discovery mode based on which API keys are configured.

    Args:
        api_keys: V5.API_KEYS dict (or any dict with `"semrush"` / `"google_places"` slots).
        country: ISO-style country code from the pipeline (`AU`, `USA`, ...).
        disable_semrush: when True the user has chosen "SerpAPI only" in the UI —
            SEMrush is treated as unavailable even if its key has credits, so the
            run resolves to GOOGLE_ONLY (SerpAPI + Places + Apollo) / APOLLO_ONLY.
            This is how the user fully bypasses SEMrush on demand.

    Returns:
        (mode, reason_str) — reason is a single short human sentence safe to
        print into the run log.
    """
    if disable_semrush:
        semrush_ok = False
    else:
        semrush_ok = bool((api_keys or {}).get("semrush"))
    is_au = (country or "").strip().upper() == "AU"
    google_ok = bool((api_keys or {}).get("google_places")) and is_au

    if semrush_ok and google_ok:
        return DiscoveryMode.BOTH, "SEMrush + Google Places both available"
    if semrush_ok and not google_ok:
        reason = ("Google Places unavailable — "
                  + ("non-AU country" if not is_au else "no GOOGLE_PLACES_API_KEY"))
        return DiscoveryMode.SEMRUSH_ONLY, reason
    _semrush_why = "bypassed by SerpAPI-only toggle" if disable_semrush else "no SEMRUSH_API_KEY"
    if google_ok and not semrush_ok:
        return DiscoveryMode.GOOGLE_ONLY, f"SEMrush unavailable ({_semrush_why})"
    # Neither key present
    return DiscoveryMode.APOLLO_ONLY, f"Neither SEMrush ({_semrush_why}) nor Google Places available — Apollo fallback only"


def should_pivot_to_google(
    api_counter: Optional[dict],
    semrush_silent_scope: bool,
    api_keys: dict,
    country: str,
    current_mode: DiscoveryMode,
) -> bool:
    """Should the pipeline pivot mid-run from a SEMrush mode to GOOGLE_ONLY?

    Returns True iff:
      • We're currently in BOTH or SEMRUSH_ONLY mode (no pivot from pure
        GOOGLE_ONLY or APOLLO_ONLY — those don't run SEMrush at all).
      • Google Places is actually available (key present AND country=AU);
        no point pivoting to a source we can't use.
      • Either SEMrush returned silent (`_semrush_silent_scope = True`) OR
        the per-run unit budget has been exhausted
        (`api_counter["semrush_budget_exhausted_logged"] = True`, set by
        SemrushClient._request when the cap is hit).

    Pure read — does NOT mutate state, does NOT log. Caller does both.
    """
    if current_mode not in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY):
        return False
    is_au = (country or "").strip().upper() == "AU"
    google_ok = bool((api_keys or {}).get("google_places")) and is_au
    if not google_ok:
        return False
    budget_exhausted = bool((api_counter or {}).get("semrush_budget_exhausted_logged", False))
    return bool(semrush_silent_scope) or budget_exhausted


def log_mode_banner(
    log_fn: Callable[[str], None],
    mode: DiscoveryMode,
    reason: str,
    *,
    max_leads: Optional[int] = None,
    country: Optional[str] = None,
) -> None:
    """Emit a 3-line banner the user can spot at a glance in the log feed.

    Format:
        ════════ DISCOVERY MODE: BOTH ════════
           Reason: SEMrush + Google Places both available
           Country=AU | max_leads=15

    Pure side-effect; safe to call from any thread that owns its log_fn.
    """
    if not callable(log_fn):
        return
    mode_str = mode.value if isinstance(mode, DiscoveryMode) else str(mode)
    try:
        log_fn(f"════════ DISCOVERY MODE: {mode_str} ════════")
        log_fn(f"   Reason: {reason}")
        ctx_parts = []
        if country:
            ctx_parts.append(f"Country={country}")
        if max_leads is not None:
            ctx_parts.append(f"max_leads={max_leads}")
        if ctx_parts:
            log_fn("   " + " | ".join(ctx_parts))
    except Exception:
        # Logging must never break the pipeline — swallow any callback errors.
        pass


def log_pivot(
    log_fn: Callable[[str], None],
    *,
    semrush_pool_size: int,
    silent_scope: bool,
    budget_exhausted: bool,
) -> None:
    """Emit the mid-run pivot line. Distinct from the initial banner so the
    user can grep for `PIVOT` alone to find runs where SEMrush gave up."""
    if not callable(log_fn):
        return
    try:
        cause_parts = []
        if silent_scope:
            cause_parts.append("silent_scope=True")
        if budget_exhausted:
            cause_parts.append("budget_exhausted=True")
        cause = ", ".join(cause_parts) or "unspecified"
        log_fn(
            f"   [Discovery] ⚡ PIVOT to GOOGLE_ONLY — SEMrush exhausted "
            f"mid-run ({cause}); keeping {semrush_pool_size} partial SEMrush domain(s)."
        )
    except Exception:
        pass


# ── Offline smoke test ──────────────────────────────────────────────────────
if __name__ == "__main__":
    # Mode-detection matrix
    assert detect_initial_mode({"semrush": "x", "google_places": "y"}, "AU")[0] == DiscoveryMode.BOTH
    assert detect_initial_mode({"semrush": "x", "google_places": ""}, "AU")[0] == DiscoveryMode.SEMRUSH_ONLY
    assert detect_initial_mode({"semrush": "x", "google_places": "y"}, "USA")[0] == DiscoveryMode.SEMRUSH_ONLY
    assert detect_initial_mode({"semrush": "", "google_places": "y"}, "AU")[0] == DiscoveryMode.GOOGLE_ONLY
    assert detect_initial_mode({"semrush": "", "google_places": ""}, "AU")[0] == DiscoveryMode.APOLLO_ONLY
    # Pivot decisions
    assert should_pivot_to_google({"semrush_budget_exhausted_logged": True},
                                  False, {"semrush": "x", "google_places": "y"},
                                  "AU", DiscoveryMode.BOTH) is True
    assert should_pivot_to_google({}, True, {"semrush": "x", "google_places": "y"},
                                  "AU", DiscoveryMode.SEMRUSH_ONLY) is True
    assert should_pivot_to_google({}, True, {"semrush": "x", "google_places": ""},
                                  "AU", DiscoveryMode.BOTH) is False, "no google → no pivot"
    assert should_pivot_to_google({}, False, {"semrush": "x", "google_places": "y"},
                                  "AU", DiscoveryMode.BOTH) is False, "healthy → no pivot"
    assert should_pivot_to_google({}, True, {"semrush": "", "google_places": "y"},
                                  "AU", DiscoveryMode.GOOGLE_ONLY) is False, "already google → no pivot"
    # Banner emits 3 lines when context provided
    captured = []
    log_mode_banner(captured.append, DiscoveryMode.BOTH, "test", max_leads=10, country="AU")
    assert len(captured) == 3 and "DISCOVERY MODE: BOTH" in captured[0]
    print("discovery_mode smoke ok")
