"""Thread-safe dedup pool shared by primary pipeline + SecondaryAgent.

Purpose:
  * Single source of truth for "which domains are already claimed" during a
    single run so concurrent workers don't re-enrich the same domain.
  * Bridge to the persistent master_leads table so already-harvested domains
    can be skipped without a round-trip per call.

Usage:
    pool = LeadPool(skip_master_known=True)
    for d in candidate_domains:
        if pool.try_claim_domain(d):
            enrich_and_score(d)

"try_claim_domain" is atomic: it both checks and adds, returning True only
if the domain was not already claimed in-run AND not already in master_leads
(when skip_master_known=True). No gap, so two threads racing on the same
domain cannot both get True.
"""
from __future__ import annotations

import logging
import threading
from typing import Iterable, Optional

from utils import root_domain

log = logging.getLogger("leadforge.leadpool")


class LeadPool:
    """Shared in-run dedup + optional master-DB awareness.

    Attributes exposed for inspection (not for mutation):
        domains_seen: set of normalized root domains claimed so far
        master_known_domains: snapshot of known root domains from master_leads
                              (populated on demand via prime_master())
    """

    def __init__(self, skip_master_known: bool = True) -> None:
        self._lock = threading.RLock()
        self.domains_seen: set[str] = set()
        self.master_known_domains: set[str] = set()
        self.skip_master_known = skip_master_known
        self._primed = False

    def prime_master(self, industry: Optional[str] = None, country: Optional[str] = None) -> int:
        """Load known root domains from master_leads into memory. Called once
        per run so per-domain claims don't each hit the DB.

        Returns the number of rows preloaded (0 if DB unavailable)."""
        if self._primed or not self.skip_master_known:
            return len(self.master_known_domains)
        try:
            from db import get_conn
            with get_conn() as conn:
                with conn.cursor() as cur:
                    if industry and country:
                        cur.execute(
                            "SELECT DISTINCT root_domain FROM master_leads "
                            "WHERE industry=%s AND country=%s",
                            (industry, country),
                        )
                    else:
                        cur.execute("SELECT DISTINCT root_domain FROM master_leads")
                    rows = cur.fetchall() or []
            with self._lock:
                self.master_known_domains = {
                    r["root_domain"] for r in rows if r.get("root_domain")
                }
                self._primed = True
            log.info("LeadPool primed: %d known root_domains loaded", len(self.master_known_domains))
            return len(self.master_known_domains)
        except Exception as e:
            # Fail open — don't block the run if master-DB query fails.
            log.warning("LeadPool.prime_master failed (%s) — running without master dedup", e)
            self._primed = True
            return 0

    def try_claim_domain(self, domain: str) -> bool:
        """Atomic claim. Returns True only if this worker is the first to see
        the (root-normalized) domain, and it isn't in master_leads."""
        if not domain:
            return False
        norm = root_domain(domain)
        if not norm:
            return False
        with self._lock:
            if norm in self.domains_seen:
                return False
            if self.skip_master_known and norm in self.master_known_domains:
                self.domains_seen.add(norm)   # record that we saw-and-skipped
                return False
            self.domains_seen.add(norm)
            return True

    def add_domains(self, domains: Iterable[str]) -> int:
        """Bulk-register a set of already-claimed domains. Returns count added."""
        added = 0
        with self._lock:
            for d in domains:
                if not d:
                    continue
                norm = root_domain(d)
                if norm and norm not in self.domains_seen:
                    self.domains_seen.add(norm)
                    added += 1
        return added

    def is_master_known(self, domain: str) -> bool:
        """Read-only check: is this domain already in the persistent master set?"""
        if not domain:
            return False
        return root_domain(domain) in self.master_known_domains

    def stats(self) -> dict:
        with self._lock:
            return {
                "domains_seen": len(self.domains_seen),
                "master_known": len(self.master_known_domains),
                "primed": self._primed,
                "skip_master_known": self.skip_master_known,
            }


if __name__ == "__main__":
    # Quick sanity smoke (no DB required)
    pool = LeadPool(skip_master_known=False)
    assert pool.try_claim_domain("example.com") is True
    assert pool.try_claim_domain("https://WWW.Example.com") is False   # same root
    assert pool.try_claim_domain("other.com") is True
    assert pool.add_domains(["foo.com", "bar.com", "foo.com"]) == 2
    print("lead_pool smoke ok:", pool.stats())
