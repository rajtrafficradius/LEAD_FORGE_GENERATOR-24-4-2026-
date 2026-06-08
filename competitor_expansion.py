"""Recursive (BFS) competitor expansion via SEMrush.

Replaces the 1-iteration block at V5.py:4619–4635. For each seed domain,
walks competitors up to MAX_DEPTH levels deep, deduplicates via a visited
set, and early-exits when enough material has been gathered.

Design choices:
  * BFS (not DFS): shallower relationships tend to be closer competitors,
    which is what we want for high-relevance leads.
  * Hard SEMrush call ceiling (COMP_CALL_BUDGET). Each `get_domain_competitors`
    call is a paid credit; a runaway BFS could burn thousands.
  * Early-exit heuristic: stop when projected domain pool ≥ 4× target leads
    (2x for the usual enrichment yield loss, 2x margin for dedup).
  * Pure function: takes collaborators as parameters so it's trivially
    testable without a live SEMrush client.

Environment overrides:
    COMPETITOR_MAX_DEPTH        (int, default 3)
    COMPETITOR_MAX_PER_DOMAIN   (int, default 3)
    COMPETITOR_CALL_BUDGET      (int, default 50)
"""
from __future__ import annotations

import logging
import os
from collections import deque
from typing import Callable, Iterable, List, Optional, Tuple

from utils import root_domain

log = logging.getLogger("leadforge.competitor")


def _env_int(name: str, default: int) -> int:
    try:
        v = int(os.environ.get(name, ""))
        return v if v > 0 else default
    except ValueError:
        return default


MAX_DEPTH = _env_int("COMPETITOR_MAX_DEPTH", 3)
MAX_COMPETITORS_PER_DOMAIN = _env_int("COMPETITOR_MAX_PER_DOMAIN", 3)
COMP_CALL_BUDGET = _env_int("COMPETITOR_CALL_BUDGET", 50)


def _scaled_call_budget(required_leads: int) -> int:
    """2026-05-18 (round 3 v2): scale BFS call budget by required_leads.

    Each competitor call costs ~120 weighted units (domain_adwords_adwords
    limit=3, 3 rows × 40/row). The previous formula scaled at 2× leads
    and capped at 50 — for a 100-lead run that's 50 × 120 = 6 000 units
    spent on competitor-of-competitor-of-competitor expansion, which
    yields diminishing returns past depth 1-2.

    New ceiling: 20 calls regardless of run size; small runs scale tighter.

      required_leads  | budget
        3             | 1
        5             | 1
        10            | 2
        25            | 5
        50            | 10
        100           | 20
        200+          | 20 (cap)

    Saves ~4 000 units on a 50-lead run, ~3 600 on a 250-lead run.
    """
    if not required_leads or required_leads <= 0:
        return COMP_CALL_BUDGET
    return max(1, min(20, required_leads // 5))


def expand_competitors_bfs(
    seed_domains: Iterable[str],
    *,
    fetch_competitors: Callable[[str, int], List[str]],
    is_platform_domain: Callable[[str], bool],
    required_leads: int,
    current_domain_count: int,
    cancelled_check: Optional[Callable[[], bool]] = None,
    visited: Optional[set[str]] = None,
    silent_scope: bool = False,
) -> Tuple[List[str], int, int]:
    """BFS expansion. Returns (new_domains, calls_made, max_depth_reached).

    Parameters:
        seed_domains: starting domains (top paid domains found in Phase 3)
        fetch_competitors: callable(domain, limit) -> list[str]  (wraps SemrushClient)
        is_platform_domain: callable(domain) -> bool — true = skip (google.com etc)
        required_leads: the run's max_leads target (used for early-exit math)
        current_domain_count: how many domains we already have from primary discovery
        cancelled_check: callable() -> bool — true = abort
        visited: optional pre-populated set of domains to avoid re-expanding

    Returns:
        new_domains: list of novel domains (root-normalized, not in primary set)
        calls_made: number of SEMrush calls consumed
        max_depth_reached: the deepest BFS level actually visited (0 if empty)
    """
    visited = visited if visited is not None else set()
    # 2026-05-18: skip the whole BFS in silent-scope mode. SEMrush has no
    # data for the chosen industry+region, so domain_adwords_adwords returns
    # nothing useful — every call is a credit we burn for zero competitors.
    if silent_scope:
        log.info("BFS skipped: silent SEMrush scope")
        return [], 0, 0
    # Normalize all seeds up-front
    seeds_norm = [root_domain(d) for d in seed_domains if d]
    seeds_norm = [d for d in seeds_norm if d and d not in visited]
    if not seeds_norm:
        return [], 0, 0

    queue: "deque[Tuple[str, int]]" = deque((d, 0) for d in seeds_norm)
    new_domains: List[str] = []
    new_seen: set[str] = set(seeds_norm)   # tracks what's already in new_domains
    calls = 0
    max_depth_reached = 0

    # 2026-05-18: scale call budget by required_leads. A 3-lead run gets
    # 6 calls = ~720 units; a 200-lead run keeps the legacy 50.
    scaled_budget = _scaled_call_budget(required_leads)

    # Early-exit threshold: once projected pool ≥ 4× required, stop.
    # For required_leads <= 0 (user asked "all"), never early-exit on size.
    target_threshold = required_leads * 4 if required_leads and required_leads > 0 else 0

    while queue:
        if cancelled_check and cancelled_check():
            log.info("BFS cancelled at depth=%d, calls=%d", max_depth_reached, calls)
            break
        if calls >= scaled_budget:
            log.info("BFS hit call budget (%d) at depth=%d", scaled_budget, max_depth_reached)
            break

        domain, depth = queue.popleft()
        if depth >= MAX_DEPTH:
            continue
        if domain in visited:
            continue
        visited.add(domain)
        if depth > max_depth_reached:
            max_depth_reached = depth

        try:
            kids = fetch_competitors(domain, MAX_COMPETITORS_PER_DOMAIN) or []
        except Exception as e:
            log.warning("fetch_competitors(%s) failed: %s", domain, e)
            kids = []
        calls += 1

        for child in kids:
            if not child:
                continue
            child_norm = root_domain(child)
            if not child_norm or child_norm in visited or child_norm in new_seen:
                continue
            try:
                if is_platform_domain(child_norm):
                    continue
            except Exception:
                # If the caller's classifier throws, err on inclusion rather than exclusion
                pass
            new_seen.add(child_norm)
            new_domains.append(child_norm)
            # Enqueue for next depth
            queue.append((child_norm, depth + 1))

        # Early-exit on sufficiency
        if target_threshold and (current_domain_count + len(new_domains)) >= target_threshold:
            log.info(
                "BFS early-exit: projected pool %d ≥ threshold %d (depth=%d, calls=%d)",
                current_domain_count + len(new_domains), target_threshold,
                max_depth_reached, calls,
            )
            break

    log.info(
        "BFS done: %d new domains, %d SEMrush calls, max_depth_reached=%d",
        len(new_domains), calls, max_depth_reached,
    )
    return new_domains, calls, max_depth_reached


if __name__ == "__main__":
    # Offline smoke: simulate a graph with a fake fetch_competitors
    graph = {
        "a.com": ["b.com", "c.com"],
        "b.com": ["d.com", "e.com"],
        "c.com": ["f.com"],
        "d.com": ["g.com"],
        "e.com": [],
        "f.com": [],
        "g.com": [],
    }

    def fake_fetch(domain: str, limit: int) -> List[str]:
        return graph.get(domain, [])[:limit]

    def not_platform(d: str) -> bool:
        return False

    new, calls, reached = expand_competitors_bfs(
        ["a.com"], fetch_competitors=fake_fetch, is_platform_domain=not_platform,
        required_leads=0, current_domain_count=0,
    )
    # Should visit a,b,c,d,e,f,g
    assert sorted(new) == ["b.com", "c.com", "d.com", "e.com", "f.com", "g.com"], new
    print(f"bfs smoke ok: {len(new)} new, {calls} calls, depth={reached}")
