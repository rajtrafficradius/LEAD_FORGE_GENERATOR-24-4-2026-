"""Secondary discovery agent — fires in parallel with primary enrichment.

Purpose: when the primary pipeline finishes Phase 3 (domain discovery) with a
candidate pool smaller than the run's target, expand the pool using a
NON-OVERLAPPING secondary keyword dictionary. Uses its own SEMrush calls,
respects the shared rate limiter, and coordinates with the primary via
LeadPool so no domain is ever enriched twice.

Activation (caller side):
    if len(primary.domains) < max_leads * ACTIVATION_FLOOR_RATIO:
        agent = SecondaryAgent(industry, database, semrush, lead_pool, log_fn)
        extra_domains = agent.run()
        primary.domains.extend(extra_domains)
        # secondary_agent_used = True → flagged in run_history

The agent is deliberately minimal: it does DISCOVERY ONLY. Enrichment stays
on the primary's ThreadPoolExecutor so the shared Apollo/Lusha rate buckets
remain the single throttle point.
"""
from __future__ import annotations

import logging
import time
from typing import Callable, List, Optional

from lead_pool import LeadPool
from rate_limiter import GLOBAL_LIMITER

log = logging.getLogger("leadforge.secondary")

# Tunables
ACTIVATION_FLOOR_RATIO = 0.6    # if primary pool < this * max_leads, activate
MAX_SECONDARY_DOMAINS = 80      # hard cap: never append more than this
KEYWORDS_PER_RUN = 15           # use up to this many secondary keywords
ADWORDS_DOMAINS_PER_KEYWORD = 8 # keep the per-keyword limit modest


class SecondaryAgent:
    def __init__(
        self,
        industry: str,
        database: str,
        semrush: object,                         # SemrushClient
        lead_pool: LeadPool,
        is_platform_domain: Callable[[str], bool],
        log_fn: Optional[Callable[[str], None]] = None,
        cancelled_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        self.industry = industry
        self.database = database
        self.semrush = semrush
        self.lead_pool = lead_pool
        self.is_platform_domain = is_platform_domain
        self._log = log_fn or (lambda m: log.info(m))
        self._cancelled_check = cancelled_check or (lambda: False)

    def _get_keywords(self) -> List[str]:
        """Load the secondary keywords for this industry. Empty list if the
        secondary_keywords.py file hasn't been generated yet."""
        try:
            from secondary_keywords import get_keywords
        except Exception as e:
            log.warning("secondary_keywords import failed: %s", e)
            return []
        kws = get_keywords(self.industry) or []
        return kws[:KEYWORDS_PER_RUN]

    def run(self) -> List[str]:
        """Synchronously expand the domain pool. Returns novel domains (not
        already seen by the primary pipeline and not in master_leads)."""
        kws = self._get_keywords()
        if not kws:
            self._log(f"[SECONDARY] No keywords registered for industry {self.industry!r} — "
                      f"skip. (Run generate_secondary_keywords.py to populate.)")
            return []

        self._log(f"[SECONDARY] Activated for {self.industry!r} with {len(kws)} keywords. "
                  f"Cap={MAX_SECONDARY_DOMAINS} new domains.")

        new_domains: List[str] = []
        for i, kw in enumerate(kws, start=1):
            if self._cancelled_check():
                self._log("[SECONDARY] Cancelled — stopping discovery.")
                break
            if len(new_domains) >= MAX_SECONDARY_DOMAINS:
                self._log(f"[SECONDARY] Hit cap of {MAX_SECONDARY_DOMAINS} new domains — stop.")
                break

            # Rate-limit the SEMrush call even though SemrushClient has its own
            # limiter — belt+suspenders so two agents don't burst together.
            GLOBAL_LIMITER.acquire("semrush")

            try:
                ad_results = self.semrush.get_adwords_domains(
                    kw, self.database, limit=ADWORDS_DOMAINS_PER_KEYWORD
                )
            except Exception as e:
                self._log(f"[SECONDARY] SEMrush kw={kw!r} failed: {e}")
                ad_results = []

            kept = 0
            for r in ad_results or []:
                d = (r.get("domain") or "").strip()
                if not d:
                    continue
                if self.is_platform_domain(d):
                    continue
                # Atomic claim against shared pool (in-memory + master_leads).
                if not self.lead_pool.try_claim_domain(d):
                    continue
                new_domains.append(d)
                kept += 1
                if len(new_domains) >= MAX_SECONDARY_DOMAINS:
                    break

            if i % 5 == 0 or kept:
                self._log(f"[SECONDARY]   kw {i}/{len(kws)} ({kw!r}): +{kept} new "
                          f"(total {len(new_domains)}/{MAX_SECONDARY_DOMAINS})")
            time.sleep(0.1)  # tiny gap between calls — smooths token bucket drain

        self._log(f"[SECONDARY] Done. Added {len(new_domains)} new domains for enrichment.")
        return new_domains


def should_activate(primary_domain_count: int, max_leads: int) -> bool:
    """Policy: activate only if primary pool would likely miss the target."""
    if max_leads is None or max_leads <= 0:
        # "all leads" mode — always activate for extra coverage if keywords exist
        return True
    return primary_domain_count < max_leads * ACTIVATION_FLOOR_RATIO


if __name__ == "__main__":
    # Offline smoke: build a fake SemrushClient and LeadPool and verify claim logic.
    class FakeSemrush:
        def get_adwords_domains(self, kw, db, limit):
            return [{"domain": f"{kw.replace(' ', '')}-1.com"},
                    {"domain": f"{kw.replace(' ', '')}-2.com"},
                    {"domain": "shared.com"}]   # same across kws → dedup test

    pool = LeadPool(skip_master_known=False)
    pool.add_domains(["already.com"])   # pretend primary had this one

    class FakeSecondaryKeywords:
        data = {"Plumber": ["emergency plumbing", "hot water system repair", "blocked drain cleaning"]}

    # Monkeypatch loader
    import secondary_keywords as sk
    sk.SECONDARY_INDUSTRY_KEYWORDS = FakeSecondaryKeywords.data

    agent = SecondaryAgent(
        industry="Plumber", database="au", semrush=FakeSemrush(),
        lead_pool=pool, is_platform_domain=lambda d: False,
    )
    out = agent.run()
    # Each kw returns 3 domains, 2 unique + 1 "shared.com" — shared counted once
    assert "already.com" not in out
    assert "shared.com" in out
    print("secondary_agent smoke ok:", len(out), "new")
