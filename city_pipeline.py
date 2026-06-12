"""
city_pipeline.py — "Generate leads through cities" mode.

This is the second entry point into the lead generator. Instead of picking
one industry and searching broadly, the user picks a state + tier + city
(or "Australia" as a whole-country sweep) and the pipeline:

  1. Converts max_leads → a market-share percentage cap (min(max_leads, 100)).
  2. Picks the industries that, together, account for that share of the
     state's lead demand — largest share first (see cities_au.py).
  3. For every picked industry, searches ALL of its keywords scoped to the
     selected city via SEMrush adwords + SerpAPI. Domains from both
     INDUSTRY_KEYWORDS and NEW_CITY_EXPANSION_KEYWORDS are eligible.
  4. Delegates the discovered domains into V5's LeadGenerationPipeline with
     preset_keywords / preset_domains so enrichment reuses the existing
     Apollo → Lusha → Hunter → OpenAI → scraper chain.
  5. If the final lead count is below max_leads, runs a competitor-expansion
     loop — pulls SEMrush competitor domains for each returned lead's
     domain, enriches those, repeats until max_leads is met or the
     Apollo-budget / round-cap is hit. This is the "go to each domain and
     look at competitors" loop the spec asks for, bounded so Apollo credits
     aren't burned on endless expansion.

The enrichment toggle is passed straight through to V5.LeadGenerationPipeline
— when OFF the CSV output has name/company/domain/role only, and the
pipeline skips all Apollo enrich_person / Lusha / Hunter / OpenAI calls
(phase-level gating lives inside V5.py).

This file deliberately does NOT re-implement any API client. It imports
them from V5 so credit counters, rate limiters, and retry logic stay
centralized.
"""

from __future__ import annotations

import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from cities_au import (
    AUSTRALIA_STATES,
    AUSTRALIA_NATIONAL,
    select_industries_by_cap,
    get_industry_shares,
)


# Competitor-loop safety caps. These are intentionally conservative so a
# runaway city-mode run can't drain Apollo credits.
# Phase 2 (2026-05-05): Bumped for the 11k tiered keyword bank — a deeper pool
# means each round finds genuinely new advertisers, so we can afford more
# rounds and a larger per-round Apollo budget without burning duplicates.
MAX_COMPETITOR_ROUNDS = 8          # was 5; with bank pool, more rounds = more T2/T3 advertisers
MAX_COMPETITORS_PER_DOMAIN = 6     # was 5; SEMrush returns top N competitors
MAX_NEW_DOMAINS_PER_ROUND = 100    # was 50; doubled to keep up with deeper rediscovery


class CityLeadPipeline:
    """City-mode orchestrator. Builds the keyword + domain set, delegates
    enrichment to V5.LeadGenerationPipeline, runs a bounded competitor-loop
    until max_leads is fulfilled.
    """

    def __init__(
        self,
        state_code: str,          # "NSW" | "VIC" | ... | "AUSTRALIA"
        tier: str,                # "tier_1" | "tier_2" | "tier_3" | "all"
        city: str,                # single city name, or "all"
        min_volume: int,
        max_leads: int,
        output_folder: str,
        enrichment_enabled: bool = True,
        quota_guarantee: bool = True,
        country: str = "AU",
        progress_callback=None,
        log_callback=None,
        disable_semrush: bool = False,
        credit_saver: bool = True,
        paid_only_all: bool = False,
    ):
        self.state_code = (state_code or "AUSTRALIA").upper()
        self.tier = (tier or "all").lower()
        self.city = city or "all"
        self.min_volume = min_volume
        self.max_leads = max_leads
        self.output_folder = output_folder
        self.enrichment_enabled = enrichment_enabled
        self.quota_guarantee = bool(quota_guarantee)
        self.country = country
        # 2026-06-08: "SerpAPI only" toggle — bypass SEMrush for the whole run.
        self.disable_semrush = bool(disable_semrush)
        # 2026-06-09: credit-saving mode (DEFAULT) — run-wide SerpAPI cap.
        self.credit_saver = bool(credit_saver)
        # 2026-06-11: paid-only mode — export ALL confirmed advertisers, no cap.
        self.paid_only_all = bool(paid_only_all) or str(os.environ.get("PAID_ONLY_ALL", "0")).strip() == "1"
        self.progress_callback = progress_callback or (lambda *a: None)
        self.log_callback = log_callback or (lambda *a: None)
        self._cancelled = False
        self._inner_pipeline = None  # filled in run() — used by cancel()
        self._lock = threading.Lock()

        # Resolve the label + target cities (the search-suffix list) up front.
        self._cities_to_search: list = self._resolve_cities()
        self._scope_label = self._build_scope_label()

        # API counters (merged from inner pipelines across rounds)
        self._api_counter = {"apollo": 0, "lusha": 0, "semrush": 0, "serpapi": 0,
                             "openai": 0, "hunter": 0}
        self._apollo_credits_at_start = -1
        self.leads: list = []
        self.keywords: list = []
        self.domains: list = []
        # Stats consumed by the frontend summary panel
        self._competitor_rounds = 0
        self._competitor_domains_added = 0
        # PHASE 2 (2026-04-28) — populated by _discover_domains; passed to V5
        # so confirmed paid advertisers bypass the strict SEMrush gate.
        self._confirmed_paid: set = set()
        self._attempted_domains: set = set()
        # 2026-06-03: domain → the discovery keyword that first surfaced it.
        # Observational metadata for the CSV/UI "Keyword" column. Never used in
        # discovery, scoring, or selection.
        self._domain_discovery_kw: dict = {}
        # Phase 2 (2026-05-15) — credit-safety state. The previous run drained
        # ~25k SEMrush credits on a 3-lead doomed scope by walking all 200
        # discovery probes + 141 Phase-4 domain_overview calls + 12 rediscovery
        # waves × 200 probes. New rules:
        #   _semrush_budget = max_leads * 100 (floor 200, ceiling 2000) — hard
        #     cap on TOTAL SEMrush calls for THIS run. When hit, every probe
        #     site bails. Matches Apollo's mental model (credits per lead).
        #   _semrush_silent_scope — set True by _discover_domains when both
        #     SEMrush and SerpAPI pools yielded 0. Rediscovery loop, Phase 4
        #     paid_traffic gate, and Phase 5f top-up all check this flag.
        #   _apollo_only_domains — the domains that came from Apollo when no
        #     SEMrush/SerpAPI evidence existed. Used by V5._enrich_single_domain
        #     to skip the SEMrush domain_overview (paid_traffic always 0 for
        #     these in SEMrush-silent scopes, so the call is pure waste).
        try:
            _mx = int(self.max_leads or 0)
        except Exception:
            _mx = 0
        self._semrush_budget: int = max(200, min(2000, _mx * 100)) if _mx > 0 else 600
        self._semrush_calls_this_run: int = 0
        self._semrush_silent_scope: bool = False
        self._apollo_only_domains: set = set()

        # 2026-05-18 (round 2): compute the unit-cost budget identical to
        # LeadGenerationPipeline so discovery / Pass 3 / rediscovery all share
        # ONE pool with the inner V5 pipeline. We seed `_api_counter` now;
        # SemrushClient._request reads `_api_counter["semrush_budget"]` and
        # `_api_counter["semrush_units"]` and decides whether to skip.
        if _mx <= 0:
            _unit_budget = 25000
        else:
            _unit_budget = max(300, min(20000, _mx * 100))
        self._semrush_unit_budget = _unit_budget
        self._api_counter["semrush_budget"] = _unit_budget
        self._api_counter["semrush_units"] = 0
        self._api_counter["semrush_units_by_phase"] = {}
        self._api_counter["semrush_skipped"] = 0
        self._api_counter["semrush_budget_alert_75"] = False
        self._api_counter["semrush_budget_exhausted_logged"] = False

    # ── public ──────────────────────────────────────────────────────────────

    def cancel(self):
        self._cancelled = True
        if self._inner_pipeline is not None:
            try:
                self._inner_pipeline.cancel()
            except Exception:
                pass

    def run(self) -> str:
        """Execute city-mode pipeline. Returns path to TOP CSV (or "")."""
        # V5 lives in the same folder — import lazily so cities_au.py and
        # city_pipeline.py remain usable from tests without loading V5's
        # global state.
        import V5 as V5mod

        self._apollo_credits_at_start = self._fetch_apollo_credits(V5mod)
        if self._apollo_credits_at_start > 0:
            self._log(f"Apollo credits at run start: {self._apollo_credits_at_start}")

        # Pick industries by the max_leads → market-share-cap rule
        pct_cap = min(self.max_leads, 100) if self.max_leads > 0 else 100
        picked = select_industries_by_cap(self.state_code, pct_cap)
        self._log(f"[City Mode] scope={self._scope_label} | max_leads={self.max_leads} | "
                  f"pct_cap={pct_cap:.0f}% → {len(picked)} industries selected")

        if not picked:
            self._log("No industries selected — max_leads/pct_cap too small.")
            return ""

        shares = get_industry_shares(self.state_code)
        self._progress(3, f"Picked {len(picked)} industries for {self._scope_label}")
        for i, ind in enumerate(picked[:15]):
            self._log(f"   • {ind}  ({shares.get(ind, 0):.1f}%)")
        if len(picked) > 15:
            self._log(f"   … +{len(picked) - 15} more industries")

        # Gather every keyword from the picked industries (V5's dicts)
        all_keywords = self._collect_keywords(V5mod, picked)
        self._log(f"[City Mode] Collected {len(all_keywords)} keywords across {len(picked)} industries")
        self._progress(8, f"{len(all_keywords)} keywords ready")

        # Discover domains for these keywords scoped to city
        discovered = self._discover_domains(V5mod, all_keywords)
        if self._cancelled:
            return ""
        self._log(f"[City Mode] Discovered {len(discovered)} unique domains across the scope")
        if not discovered:
            self._log("No domains discovered. Try a wider scope (higher tier / whole state).")
            return ""

        self.keywords = all_keywords
        self.domains = list(discovered)

        # Round 1: enrich the initial domain set
        run_leads = self._enrich_round(V5mod, all_keywords, list(discovered), round_num=1)
        if self._cancelled:
            return ""
        self.leads = list(run_leads)

        # Competitor-expansion loop: keep pulling competitor domains until
        # we hit max_leads or round cap or Apollo budget.
        visited_domains = set(d.lower() for d in discovered) | set(self._attempted_domains)
        round_num = 2
        while (self.max_leads > 0
               and len(self.leads) < self.max_leads
               and round_num <= MAX_COMPETITOR_ROUNDS + 1
               and not self._cancelled):
            # Respect Apollo budget — stop if we've already burned the cap
            if self._api_counter.get("apollo", 0) >= int(self.max_leads * 30) * 2:
                self._log("[City Mode] Apollo budget exhausted — stopping competitor loop")
                break
            deficit = self.max_leads - len(self.leads)
            self._log(f"[City Mode] Round {round_num}: need {deficit} more leads, expanding via competitors…")
            new_domains = self._find_competitors(V5mod, self.leads, visited_domains)
            if not new_domains:
                self._log("[City Mode] No new competitor domains found — stopping loop")
                break
            self._log(f"[City Mode] Round {round_num}: +{len(new_domains)} competitor domains to enrich")
            self._competitor_rounds += 1
            self._competitor_domains_added += len(new_domains)
            # Competitor domains come from SEMrush domain_adwords_adwords (paid
            # competitors first) — treat them as confirmed paid advertisers so
            # the strict gate doesn't strip them in the inner pipeline.
            self._confirmed_paid.update((d or "").lower() for d in new_domains if d)
            round_leads = self._enrich_round(V5mod, all_keywords, new_domains, round_num=round_num)
            if round_leads:
                self.leads.extend(round_leads)
            visited_domains.update(d.lower() for d in new_domains)
            visited_domains.update(self._attempted_domains)
            round_num += 1

        # ── RADICAL: Rediscovery phases when still short of quota ──
        # The competitor loop hits diminishing returns once seed domains repeat.
        # This phase runs FRESH discovery passes using DIFFERENT keyword slices
        # that haven't been scanned yet — equivalent to running multiple
        # sub-pipelines and combining results into a single grand CSV.
        # Up to 3 rediscovery phases with offsets 200, 500, 900 into the
        # 2622-keyword pool — each finds NEW Australian paid advertisers
        # not discovered in earlier passes.
        rediscovery_phase = 0
        # Phase 2 (2026-05-05): walk the keyword pool in evenly-spaced strides.
        # Phase 2 (2026-05-15): SHORT-CIRCUIT when SEMrush is silent. If the
        # initial _discover_domains found 0 SEMrush+SerpAPI domains, more
        # keyword slices from the same pool won't help (the scope just has
        # no AU paid-ads data) — running 12 rediscovery waves × 200 SEMrush
        # probes = 2,400 wasted SEMrush calls. Cap to 1 try when silent so
        # we still attempt e.g. a different city cohort via Apollo, but
        # NEVER bombard SEMrush again with the same keyword pool.
        MAX_REDISCOVERY = 12
        if getattr(self, "_semrush_silent_scope", False):
            MAX_REDISCOVERY = 1
            self._log("[City Mode] 🔕 SEMrush-silent scope — rediscovery capped at 1 round "
                      "to save credits. (Repeat probes against the same silent pool would "
                      "drain ~25k SEMrush credits for no new leads.)")
        # Also short-circuit if we've already burned our per-run SEMrush budget.
        if getattr(self, "_semrush_calls_this_run", 0) >= getattr(self, "_semrush_budget", 10**9):
            MAX_REDISCOVERY = 0
            self._log(f"[City Mode] ⛔ Per-run SEMrush budget ({self._semrush_budget}) "
                      f"exhausted during initial discovery — skipping rediscovery entirely.")
        _pool_size = len(all_keywords)
        _stride = max(200, _pool_size // max(MAX_REDISCOVERY, 1)) if MAX_REDISCOVERY > 0 else 200
        keyword_offsets = [(_stride * i) for i in range(1, MAX_REDISCOVERY + 1)]
        while (self.max_leads > 0
               and len(self.leads) < self.max_leads
               and rediscovery_phase < MAX_REDISCOVERY
               and not self._cancelled):
            offset = keyword_offsets[rediscovery_phase] if rediscovery_phase < len(keyword_offsets) else _stride * (rediscovery_phase + 1)
            if offset >= len(all_keywords):
                self._log(f"[City Mode] Rediscovery: no more unseen keywords (offset {offset} > {len(all_keywords)}) — stopping")
                break
            rediscovery_phase += 1
            deficit = self.max_leads - len(self.leads)
            sliced = all_keywords[offset:]
            self._log(f"[City Mode] REDISCOVERY {rediscovery_phase}/{MAX_REDISCOVERY}: "
                      f"need {deficit} more leads — scanning fresh keyword slice "
                      f"[{offset}:{offset+len(sliced)}] ({len(sliced)} new keywords)")
            # Run a fresh _discover_domains pass on the new slice. Reuse the
            # method but feed it a different keyword set so it scans NEW
            # SEMrush phrase_adwords queries the previous passes never touched.
            extra_discovered = self._discover_domains(V5mod, sliced)
            # Drop already-visited domains (these were already enriched)
            extra_new = [d for d in extra_discovered if d.lower() not in visited_domains]
            if not extra_new:
                self._log(f"[City Mode] Rediscovery {rediscovery_phase}: 0 new domains found — trying next slice")
                continue
            self._log(f"[City Mode] Rediscovery {rediscovery_phase}: +{len(extra_new)} fresh domains to enrich")
            self._confirmed_paid.update((d or "").lower() for d in extra_new if d)
            round_num += 1
            extra_leads = self._enrich_round(V5mod, all_keywords, extra_new, round_num=round_num)
            if extra_leads:
                self.leads.extend(extra_leads)
            visited_domains.update(d.lower() for d in extra_new)

        # Export a unified CSV from the merged lead set via V5's existing
        # export logic. We reuse a temporary inner pipeline purely for the
        # Phase 6 sorter + writer so the user's output has every run's leads.
        out_path = self._export_combined(V5mod)
        self._progress(100, f"Done! {len(self.leads)} leads exported")
        return out_path

    # ── internal helpers ────────────────────────────────────────────────────

    def _resolve_cities(self) -> list:
        """Return the list of target city strings (the location suffixes to
        append to keywords during discovery). Uses AUSTRALIA_NATIONAL when
        state_code == 'AUSTRALIA'."""
        if self.state_code == "AUSTRALIA":
            src = AUSTRALIA_NATIONAL
        else:
            src = AUSTRALIA_STATES.get(self.state_code)
            if not src:
                # Unknown state — fallback to whole country
                src = AUSTRALIA_NATIONAL
        if self.tier == "all":
            cities = list(src.get("tier_1", [])) + list(src.get("tier_2", [])) + list(src.get("tier_3", []))
        else:
            cities = list(src.get(self.tier, []))
        if self.city and self.city.lower() != "all":
            # Exact-city scope — drop the rest
            cities = [c for c in cities if c.lower() == self.city.lower()] or [self.city]
        return cities or ["Australia"]

    def _build_scope_label(self) -> str:
        if self.city and self.city.lower() != "all":
            return f"{self.city}, {self.state_code}"
        if self.state_code == "AUSTRALIA":
            return "Australia (whole country)"
        tier_label = {"tier_1": "Tier 1", "tier_2": "Tier 2", "tier_3": "Tier 3"}.get(self.tier, "All tiers")
        return f"{self.state_code} ({tier_label})"

    def _collect_keywords(self, V5mod, industries: list) -> list:
        """Return a deduplicated keyword list drawn from V5's two industry dicts.

        PHASE 2 (2026-04-28) — keywords are now INTERLEAVED across industries
        instead of concatenated. With concatenation and a 25-keyword scan cap,
        all 25 SEMrush probes hit only the FIRST industry — if its keywords
        happen to have weak paid-ad coverage in AU (e.g. "restaurant"), zero
        domains are discovered. Interleaving gives every selected industry
        equal probe budget, so a 25-keyword scan probes ~12 of each in a 2-
        industry run instead of 25 of one and 0 of the other.
        """
        # Phase 2 (2026-05-05): extend per-industry queues with the tiered
        # keyword bank (10k file). Global 1k fallback is appended at the tail
        # below — only deepest rediscovery offsets reach it.
        try:
            import keyword_bank as _kb
        except Exception:
            _kb = None

        # Build per-industry queues, preserving order within each industry.
        per_industry: list[list[str]] = []
        for ind in industries:
            kws = (V5mod.INDUSTRY_KEYWORDS.get(ind)
                   or V5mod.NEW_CITY_EXPANSION_KEYWORDS.get(ind)
                   or [])
            base = [kw for kw in kws if kw and kw.strip()]
            if _kb is not None:
                try:
                    tiers = _kb.get_industry_tiers(ind)
                    seen_local = {kw.strip().lower() for kw in base}
                    for tname in ("TIER_1", "TIER_2", "TIER_3"):
                        for kw in tiers.get(tname, []):
                            k = kw.strip().lower()
                            if k and k not in seen_local:
                                seen_local.add(k)
                                base.append(kw)
                except Exception:
                    pass
            per_industry.append(base)

        # Round-robin merge with global dedup.
        out: list = []
        seen = set()
        max_len = max((len(q) for q in per_industry), default=0)
        for i in range(max_len):
            for q in per_industry:
                if i < len(q):
                    kw = q[i]
                    k = kw.strip().lower()
                    if k and k not in seen:
                        seen.add(k)
                        out.append(kw)

        # Tail-append the global 1k fallback so deep rediscovery offsets walk
        # into broad-market keywords only after every industry-specific tier
        # is exhausted.
        if _kb is not None:
            try:
                for kw in _kb.get_global_fallback():
                    k = kw.strip().lower()
                    if k and k not in seen:
                        seen.add(k)
                        out.append(kw)
            except Exception:
                pass

        return out

    def _discover_domains(self, V5mod, keywords: list) -> set:
        """Pull adwords + SerpAPI + Apollo-org domains for each keyword.

        KEY DESIGN CALL (fixes the 0-domain bug from earlier runs):
          * SEMrush's phrase_adwords is keyword-based — Google Ads
            advertisers bid on ROOT keywords with native location targeting,
            not on location-embedded long-tail phrases like "emergency
            plumber Melbourne". So we call SEMrush with the BASE keyword
            only and get the AU-wide paid-traffic advertiser pool.
          * SerpAPI is the GEO-AWARE layer. For each keyword we query
            "keyword city" so Google returns Melbourne/Sydney/… businesses
            naturally.
          * Apollo mixed_companies/search is the LAST-RESORT fallback — it
            supports native city+state location filters, which rescues runs
            where SEMrush returned 0 paid domains AND SerpAPI results were
            all directory sites filtered by is_platform_domain.

        Counts from each source are logged separately so the failure mode
        is immediately diagnosable if discovery returns zero again.
        """
        semrush = V5mod.SemrushClient(V5mod.API_KEYS["semrush"])
        semrush._disabled = self.disable_semrush   # "SerpAPI only" → no SEMrush calls
        serp = V5mod.SerpApiClient(V5mod.API_KEYS["serpapi"])
        semrush._counter = self._api_counter
        # 2026-05-18 (round 2): tag every call from this discovery client
        # with phase=city_discovery so the per-phase breakdown shows where
        # Pass 1 + Pass 3 credits went. The unit budget is read from the
        # shared counter — no need to set _unit_budget on this instance.
        semrush._current_phase = "city_discovery"
        semrush._log_cb = self._log
        serp._counter = self._api_counter
        serp._log_cb = self._log
        # 2026-06-01: SerpAPI per-key pre-flight check (multi-key support).
        # Calls SerpAPI's free `/account` endpoint per key, logs remaining
        # searches, and marks zero-balance/invalid keys dead BEFORE the
        # discovery sweep so we don't waste latency rotating to dud keys.
        try:
            if hasattr(serp, "precheck_keys") and getattr(serp, "_keys", []):
                _serp_status = serp.precheck_keys(log_fn=self._log)
                _total_left = sum(int(s.get("remaining", 0)) for s in _serp_status)
                _live = sum(1 for s in _serp_status if int(s.get("remaining", 0)) > 0)
                self._log(
                    f"   [SerpAPI/preflight] {_live}/{len(_serp_status)} keys live, "
                    f"{_total_left} searches total remaining across all keys"
                )
        except Exception as _spe:
            self._log(f"   [SerpAPI/preflight] check failed: {_spe}")

        # 2026-06-01: GLOBAL per-run SerpAPI call budget. The prior run made
        # 253 calls and exhausted a 250-search key in ONE go, so the next run
        # had nothing. Cap total calls to a conservative slice of the live
        # balance (keeping a reserve for future runs) scaled by max_leads.
        try:
            _serp_live_total = int(getattr(serp, "total_remaining_searches", lambda: 0)())
            _ml = max(1, int(self.max_leads or 0))
            if _serp_live_total <= 0:
                _serp_run_budget = 0
            else:
                _reserve = min(40, _serp_live_total // 3)
                # credit-saving (default): ≈14 calls/lead, the hard run-wide cap
                # that covers BOTH discovery AND per-person enrichment. regular:
                # thorough but still bounded so one run can't drain the key.
                if self.credit_saver:
                    _target = max(12, _ml * 14)
                else:
                    _target = max(60, _ml * 80)
                _serp_run_budget = max(8, min(_serp_live_total - _reserve, _target))
            serp._call_budget = int(_serp_run_budget)
            serp._calls_used = 0
            self._serp_run_budget = int(_serp_run_budget)
            # SHARED budget set BEFORE any sweep — every SerpApiClient instance in
            # this run (discovery + inner-enrichment + host) reads it from the
            # common _counter, so the cap is truly run-wide.
            self._api_counter["serpapi_budget"] = int(_serp_run_budget)
            self._api_counter["serpapi_budget_warned"] = False
            self._log(
                f"   [SerpAPI] run-wide budget = {_serp_run_budget} calls "
                f"({'credit-saver' if self.credit_saver else 'regular'}, "
                f"max_leads={_ml}, live balance {_serp_live_total}, reserve kept)"
            )
        except Exception as _sbe:
            self._serp_run_budget = 10**9
            self._log(f"   [SerpAPI] budget set failed ({_sbe}); using unbounded")

        db = V5mod.COUNTRY_CONFIG.get(self.country, {}).get("semrush_db", "au")
        gl = V5mod.COUNTRY_CONFIG.get(self.country, {}).get("serpapi_gl", "au")

        # 2026-05-21: 4-mode discovery routing. Decide BOTH / SEMRUSH_ONLY /
        # GOOGLE_ONLY / APOLLO_ONLY up-front based on which API keys exist +
        # whether the scope is AU-eligible for Google Places. Each PASS
        # below is gated by the mode. A mid-run pivot can flip BOTH or
        # SEMRUSH_ONLY into GOOGLE_ONLY when SEMrush exhausts (we keep the
        # partial pool — see the pivot check before PASS 3 below).
        from discovery_mode import (
            DiscoveryMode,
            detect_initial_mode,
            should_pivot_to_google,
            log_mode_banner,
            log_pivot,
        )
        mode, _mode_reason = detect_initial_mode(
            V5mod.API_KEYS, self.country, disable_semrush=self.disable_semrush)
        self._discovery_mode = mode
        log_mode_banner(
            self._log, mode, _mode_reason,
            max_leads=self.max_leads, country=self.country,
        )

        # ── 2026-06-01: PAID-DATA-SOURCE health pre-check (no wasted units) ──
        # The recurring "all my leads show 0 paid traffic" problem is almost
        # always: BOTH SEMrush (units) and ALL SerpAPI keys are out of balance,
        # so nothing can confirm SEMrush/Ahrefs-grade paid traffic. We can't
        # know SEMrush's unit balance without a call (PASS 1 surfaces the 403),
        # but the SerpAPI precheck already ran, and the key presence tells us
        # whether SEMrush is even in play. The DEFINITIVE verdict banner fires
        # after PASS 2 (see `_emit_paid_source_verdict`), once SEMrush's real
        # 403 state is known. Here we just stash inputs.
        _serp_remaining_at_start = int(getattr(serp, "total_remaining_searches", lambda: 0)())
        _serp_keys_n = len(getattr(serp, "_keys", []) or [])
        self._serp_live_at_start = serp._available and (_serp_remaining_at_start > 0 or _serp_keys_n == 0)
        self._semrush_key_present = bool(V5mod.API_KEYS.get("semrush"))

        def _emit_paid_source_verdict():
            
            
            """Fire AFTER PASS 1/2 when SEMrush 403 state is known. Loud banner
            if no paid source is live so the user never silently gets a CSV of
            unverifiable Places/Apollo domains again."""
            _sem_dead = (
                (not self._semrush_key_present)
                or bool(self._api_counter.get("semrush_unavailable", False))
                or bool(getattr(self, "_semrush_silent_scope", False))
            )
            _serp_dead = (not serp._available) or (
                _serp_keys_n > 0 and int(getattr(serp, "total_remaining_searches", lambda: 0)()) <= 0
            )
            self._no_paid_source = bool(_sem_dead and _serp_dead)
            if self._no_paid_source:
                self._log("══════════ ⚠ NO PAID-DATA SOURCE LIVE ══════════")
                self._log("   SEMrush units = 0/403 AND every SerpAPI key is out of searches.")
                self._log("   → This run's leads come from Google Places + Apollo, which find")
                self._log("     BUSINESSES, not confirmed advertisers — most will show 0 paid")
                self._log("     traffic in SEMrush/Ahrefs. ATC still tags any it can confirm.")
                self._log("   → For SEMrush/Ahrefs-verifiable paid leads: refill SEMrush units,")
                self._log("     OR paste live keys into SERPAPI_API_KEYS in .env (free=100 each).")
                self._log("═════════════════════════════════════════════════")
            else:
                self._log(
                    f"   [PaidSource] SEMrush_dead={_sem_dead}, SerpAPI_dead={_serp_dead} "
                    f"— paid verification ACTIVE for this run"
                )
        self._emit_paid_source_verdict = _emit_paid_source_verdict

        pool_semrush: set = set()
        pool_serp: set = set()
        pool_apollo: set = set()

        # 8x buffer so competitor expansion (Pass 4) has room to add 4-5x more domains
        domain_cap = max(200, int(self.max_leads) * 8) if self.max_leads > 0 else 800
        # Phase 2 (2026-05-15): keyword_cap rebalanced for credit safety.
        # The previous floor of 200 was burning ~200 SEMrush calls even for
        # 3-lead runs in scopes where SEMrush has zero AU paid-ads data
        # (local Melbourne trades). New formula scales linearly with
        # max_leads, with a lower floor so small runs don't waste credits
        # probing keywords SEMrush won't answer for anyway.
        #   max_leads=3   → 60 probes  (was 200)
        #   max_leads=10  → 200
        #   max_leads=50  → 1000
        #   max_leads=200 → 1200 (ceiling unchanged)
        if self.max_leads > 0:
            keyword_cap = min(len(keywords), max(40, min(1200, self.max_leads * 20)))
        else:
            keyword_cap = min(len(keywords), 400)
        kws_to_scan = keywords[:keyword_cap]
        self._log(f"   [Discovery] keyword_cap={keyword_cap} of {len(keywords)} collected")

        # ── PASS 1: SEMrush adwords with BASE keyword (no city suffix) ──
        # 2026-05-21: gated by discovery mode — GOOGLE_ONLY / APOLLO_ONLY
        # skip this entire pass and pool_semrush stays empty.
        empty_kw_count = 0
        _EARLY_BAIL_AFTER = 25
        _EARLY_BAIL_EMPTY_RATIO = 0.90
        _pass1_active = mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
        if not _pass1_active:
            self._log(
                f"   [Discovery] PASS 1 (SEMrush phrase_adwords) SKIPPED — "
                f"mode={mode.value}"
            )
        for i, kw in enumerate(kws_to_scan if _pass1_active else []):
            if self._cancelled:
                break
            if len(pool_semrush) >= domain_cap:
                break
            if (getattr(self, "_semrush_calls_this_run", 0)
                    >= getattr(self, "_semrush_budget", 10**9)):
                self._log(f"   [Discovery/SEMrush] ⛔ Per-run SEMrush budget "
                          f"({self._semrush_budget}) reached at probe {i+1} — "
                          f"halting further SEMrush probes for this run.")
                break
            pre = len(pool_semrush)
            try:
                # 2026-06-12: capture ALL advertisers bidding on a keyword, not
                # just the first 10 — phrase_adwords costs 1 unit/row, so a
                # full pull is ≤ +20 units on a populated keyword and the bank
                # is now AdPotentialScore-sorted (highest-yield probes first).
                ad = semrush.get_adwords_domains(
                    kw, db, limit=(15 if self.credit_saver else 30))
                self._semrush_calls_this_run = getattr(self, "_semrush_calls_this_run", 0) + 1
                for r in ad:
                    d = (r.get("domain") or "").strip().lower()
                    if d and not V5mod.is_platform_domain(d):
                        pool_semrush.add(d)
            except Exception as e:
                self._log(f"   [Discovery] SEMrush error on '{kw}': {e}")
            if len(pool_semrush) == pre:
                empty_kw_count += 1
            if (i + 1) % 8 == 0 or i == len(kws_to_scan) - 1:
                self._log(f"   [Discovery/SEMrush] Scanned {i + 1}/{len(kws_to_scan)} keywords → {len(pool_semrush)} domains")
                pct = 8 + int((i + 1) / max(len(kws_to_scan), 1) * 10)
                self._progress(pct, f"SEMrush: {len(pool_semrush)} domains")
            # EARLY-BAIL CHECK — fires after the first 25 probes
            if (i + 1) >= _EARLY_BAIL_AFTER and (empty_kw_count / (i + 1)) >= _EARLY_BAIL_EMPTY_RATIO:
                self._log(f"   [Discovery/SEMrush] ⏸ Early-bail: {empty_kw_count}/{i+1} "
                          f"probes empty (≥{int(_EARLY_BAIL_EMPTY_RATIO*100)}%). SEMrush has no "
                          f"paid-ads data for this scope — skipping the remaining "
                          f"{len(kws_to_scan) - (i+1)} probes to save credits.")
                break
        if empty_kw_count >= max(1, len(kws_to_scan) - 0) and kws_to_scan and len(pool_semrush) == 0:
            err = getattr(semrush, "_last_error", "no error captured")
            self._log(f"   [Discovery/SEMrush] ⚠ All SEMrush probes for this scope returned 0 "
                      f"domains. Last SEMrush response snippet: {err!r}")

        # ── PASS 2: SerpAPI scoped to "keyword + city" (geo-aware) ──
        # Re-arm _available every 5 keywords so one transient error doesn't
        # disable SerpAPI for the rest of the run (matches V5 Phase 3 logic).
        # 2026-05-21: SerpAPI is part of the "SEMrush family" (geo verifier
        # for paid-keyword domains) so it shares the same mode gate. In
        # GOOGLE_ONLY / APOLLO_ONLY we skip it entirely.
        _pass2_active = mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
        if not _pass2_active:
            self._log(
                f"   [Discovery] PASS 2 (SerpAPI geo-verifier) SKIPPED — "
                f"mode={mode.value}"
            )
        serp_keywords = kws_to_scan[:min(len(kws_to_scan), 25)] if _pass2_active else []
        # 2026-05-25: track ads-only domains separately so logs can show the
        # confirmed-paid-advertiser sub-pool. Same SerpAPI call, additional
        # extraction path inside search_keyword_ads_only — see V5.py.
        pool_serp_ads: set = set()
        # 2026-06-01: the regular geo-search returns ORGANIC+local domains
        # (noise for a paid-ads goal), so it must NOT eat the SerpAPI budget
        # that the ads-only sweep (PASS 2a) needs. Temporarily cap PASS 2 to
        # ~25% of the run budget; PASS 2a restores the full budget below.
        _serp_full_budget = int(getattr(serp, "_call_budget", 10**9))
        if _serp_full_budget < 10**8:
            serp._call_budget = max(2, _serp_full_budget // 4)
        for i, kw in enumerate(serp_keywords):
            if self._cancelled:
                break
            if len(pool_serp) >= domain_cap:
                break
            if i % 5 == 0:
                serp._available = True
            if not serp._available:
                continue
            for city in self._cities_to_search:
                if self._cancelled or len(pool_serp) >= domain_cap:
                    break
                scoped = f"{kw} {city}"
                try:
                    serp_d = serp.search_keyword(scoped, gl, num=10)
                    for d in serp_d or []:
                        d = (d or "").strip().lower()
                        if d and not V5mod.is_platform_domain(d):
                            pool_serp.add(d)
                            self._domain_discovery_kw.setdefault(d, kw)  # observational
                except Exception as e:
                    self._log(f"   [Discovery] SerpAPI error on '{scoped}': {e}")
            if (i + 1) % 5 == 0 or i == len(serp_keywords) - 1:
                self._log(f"   [Discovery/SerpAPI] Scanned {i + 1}/{len(serp_keywords)} keywords → {len(pool_serp)} domains")
                pct = 18 + int((i + 1) / max(len(serp_keywords), 1) * 7)
                self._progress(pct, f"SerpAPI: {len(pool_serp)} domains")

        # ── PASS 2a: SerpAPI ads-only sweep (additive, +30-50% paid domains) ──
        # 2026-05-25: same SerpAPI key & billing, but pulls ONLY domains
        # from the `ads[]` block (Google Ads paid placements). These are
        # businesses actively spending money for clicks RIGHT NOW — the
        # strongest possible buyer-intent signal among the free providers.
        # We cap at the top-3 keywords × top-3 cities so a 3-lead run adds
        # at most ~9 extra SerpAPI calls (the 25-call SerpAPI budget is
        # already loose; this stays well under it on AU SMB scopes).
        # 2026-06-01: restore the FULL run budget — the ads-only sweep is the
        # paid-lead source and gets priority over the regular geo-search.
        if _serp_full_budget < 10**8:
            serp._call_budget = _serp_full_budget
        # 2026-06-09 BUGFIX: PASS 2a is the SerpAPI google_ads sweep — the PRIMARY
        # paid-advertiser source. It was wrongly sharing PASS 2's SEMrush-family
        # gate (_pass2_active = BOTH/SEMRUSH_ONLY), so in GOOGLE_ONLY mode (SEMrush
        # dead OR the "SerpAPI only" toggle) it was SKIPPED — the exact mode where
        # SerpAPI IS the paid source. That starved discovery (ads=0 → only Places/
        # Apollo) and capped big runs at a handful of leads. The ads sweep must run
        # whenever SerpAPI is alive — i.e. every mode EXCEPT APOLLO_ONLY (no key).
        _pass2a_active = mode in (
            DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY, DiscoveryMode.GOOGLE_ONLY)
        if _pass2a_active and not self._cancelled and serp._available:
            # 2026-05-28: PRIMARY paid source. The ads[] block is Google's LIVE
            # paid placements — strongest confirmed-advertiser signal. Scales
            # with max_leads; bounded by SERP_ADS_MAX_QUERIES (default 80) AND
            # the global per-run call budget (so the account balance survives).
            _ads_q_cap = int(os.environ.get("SERP_ADS_MAX_QUERIES", "80") or "80")
            # 2026-06-01: never exceed the remaining run budget (full budget
            # minus whatever the regular geo-search already consumed).
            _budget_left_now = max(0, int(getattr(serp, "_call_budget", 10**9)) - int(getattr(serp, "_calls_used", 0)))
            if _budget_left_now < 10**8:
                _ads_q_cap = min(_ads_q_cap, _budget_left_now)
            _ads_kw_n = max(8, min(40, int(self.max_leads or 0) or 8))
            _ads_city_n = 4
            # 2026-06-09: daily-rotating keyword window for SUSTAINABLE volume.
            # Cross-run DB dedup (lifetime 2/domain) means hitting the SAME top
            # keywords every day surfaces the same advertisers → diminishing new
            # leads. We always keep the top half (highest-intent → quality) and
            # rotate the other half by day-of-epoch through the rest of the
            # ~54k keyword bank, so each day's run reaches FRESH advertisers
            # while still covering the best terms. Net: quantity AND quality.
            import time as _t_rot
            _half = max(1, _ads_kw_n // 2)
            _top_kws = kws_to_scan[:_half]
            _rest_pool = kws_to_scan[_half:] or kws_to_scan
            _day = int(_t_rot.time() // 86400)
            _need = max(0, _ads_kw_n - len(_top_kws))
            if _need and _rest_pool:
                _start = (_day * _need) % len(_rest_pool)
                _rot_slice = (_rest_pool[_start:] + _rest_pool[:_start])[:_need]
            else:
                _rot_slice = []
            # de-dup while preserving order (top first, then rotating slice)
            _seen_kw, _ads_kws = set(), []
            for _k in (_top_kws + _rot_slice):
                _kl = (_k or "").strip().lower()
                if _kl and _kl not in _seen_kw:
                    _seen_kw.add(_kl); _ads_kws.append(_k)
            _ads_cities = list(self._cities_to_search or [])[:_ads_city_n]
            _ads_added = 0
            _ads_calls = 0
            # 2026-06-02: AD-FREQUENCY tracking. A domain that appears as an
            # advertiser across MANY distinct (keyword × city) ad queries is a
            # HEAVY, multi-keyword advertiser — exactly the profile SEMrush/
            # Ahrefs reliably report paid_traffic for (>5). A domain seen on a
            # single long-tail query is often a small local advertiser those
            # tools miss. We tally freq here and use it to (a) prioritise
            # heavy advertisers paid-first, (b) optionally hard-filter to
            # freq>=SERP_ADS_MIN_FREQ when the user wants ONLY SEMrush/Ahrefs-
            # grade paid domains. This is the lever toward the ">5" goal.
            from collections import Counter as _Counter
            _ads_freq: "_Counter" = _Counter()
            self._log(
                f"   [Discovery/SerpAPI-ads] budget for this sweep: {_ads_q_cap} queries "
                f"(env cap, capped by {_budget_left_now} calls left in per-run budget)"
            )
            # 2026-06-02: Google Ads are CITY-targeted — pass the city as the
            # SerpApi `location` (e.g. "Sydney, Australia"), NOT baked into the
            # query text + NOT country-level (country-level returns 0 ads).
            # The keyword stays the bare commercial term so the ads engine
            # matches the head term while location handles geo.
            _ads_country_name = (
                V5mod.COUNTRY_CONFIG.get(self.country, {}).get("location_suffix")
                or "Australia"
            )
            for _kw in _ads_kws:
                if self._cancelled or not serp._available or _ads_calls >= _ads_q_cap:
                    break
                for _city in _ads_cities:
                    if (self._cancelled or len(pool_serp) >= domain_cap
                            or _ads_calls >= _ads_q_cap):
                        break
                    _q = _kw.strip()
                    _city_location = f"{_city}, {_ads_country_name}"
                    try:
                        _ad_d = serp.search_keyword_ads_only(_q, gl, num=20, location=_city_location)
                        _ads_calls += 1
                        for d in _ad_d or []:
                            d = (d or "").strip().lower()
                            if d and not V5mod.is_platform_domain(d):
                                if d not in pool_serp:
                                    _ads_added += 1
                                pool_serp.add(d)
                                pool_serp_ads.add(d)
                                _ads_freq[d] += 1   # count distinct ad-query appearances
                                # 2026-06-03: OBSERVATIONAL ONLY — record which
                                # keyword surfaced this domain (for the CSV/UI
                                # "Keyword" column). setdefault → first keyword
                                # wins; changes no discovery logic or selection.
                                self._domain_discovery_kw.setdefault(d, _kw.strip())
                    except Exception as e:
                        self._log(f"   [Discovery/SerpAPI-ads] error on '{_q}': {e}")
                if (_ads_calls % 10 == 0) and _ads_calls:
                    self._progress(20, f"Google Ads sweep: {len(pool_serp_ads)} advertisers ({_ads_calls} queries)")
            # Stash the heavy-advertiser set: domains advertising on >= N
            # distinct queries. Default threshold 2 (configurable). These are
            # ranked paid-FIRST and, in strict mode, are the ONLY ads kept.
            _min_freq = max(1, int(os.environ.get("SERP_ADS_MIN_FREQ", "2") or "2"))
            # Accumulate across rediscovery rounds (don't reset) so the inner
            # pipeline's final ranking sees ALL heavy advertisers found.
            if not isinstance(getattr(self, "_heavy_advertisers", None), set):
                self._heavy_advertisers = set()
            self._heavy_advertisers |= {d for d, n in _ads_freq.items() if n >= _min_freq}
            _strict_freq = str(os.environ.get("STRICT_PAID_ONLY", "0")).strip() == "1"
            if _strict_freq and self._heavy_advertisers:
                # Strict mode: drop single-appearance (likely-untracked) ad
                # domains from the ads pool so only heavy advertisers survive.
                _before_strict = len(pool_serp_ads)
                pool_serp_ads = {d for d in pool_serp_ads if d in self._heavy_advertisers}
                self._log(
                    f"   [Discovery/SerpAPI-ads] STRICT_PAID_ONLY: kept "
                    f"{len(pool_serp_ads)}/{_before_strict} heavy advertisers "
                    f"(>= {_min_freq} ad-queries — the SEMrush/Ahrefs-trackable profile)"
                )
            _top_freq = ", ".join(
                f"{d}×{n}" for d, n in _ads_freq.most_common(5)
            ) or "none"
            self._log(
                f"   [Discovery/SerpAPI-ads] PRIMARY paid sweep: +{_ads_added} live-ad "
                f"domains across {_ads_calls} queries ({len(pool_serp_ads)} confirmed "
                f"advertisers; {len(self._heavy_advertisers)} HEAVY [>= {_min_freq} queries]). "
                f"Top: {_top_freq}"
            )

        # 2026-06-01: SEMrush 403 + SerpAPI exhaustion are both known now —
        # emit the consolidated paid-source verdict banner.
        try:
            self._emit_paid_source_verdict()
        except Exception:
            pass

        # ── PASS 2.5: Google Custom Search JSON API (organic, additive) ──
        # 2026-05-25: free 100 queries/day organic-results layer. Sits
        # next to SerpAPI but uses Google's own Custom Search endpoint with
        # a Programmable Search Engine ID. Cap kept low (default 10 queries)
        # so multiple back-to-back runs don't blow through the daily quota.
        # Domains land in pool_serp because they share the same provenance
        # semantics (organic-search discovered, no SEMrush paid-traffic
        # confirmation). Skipped silently when key/cx not configured.
        pool_cse: set = set()
        try:
            from google_custom_search import (
                GoogleCustomSearchDiscovery,
                MAX_QUERIES as _CSE_MAX_Q,
                MAX_DOMAINS as _CSE_MAX_D,
            )
            _cse_key = (
                V5mod.API_KEYS.get("google_custom_search", "")
                or os.environ.get("GOOGLE_CUSTOM_SEARCH_API_KEY", "")
            ).strip()
            _cse_cx = (
                V5mod.API_KEYS.get("google_custom_search_cx", "")
                or os.environ.get("GOOGLE_CUSTOM_SEARCH_CX", "")
            ).strip()
            # Gate by mode: PASS 2.5 runs in BOTH/SEMRUSH_ONLY/GOOGLE_ONLY
            # (anywhere we'd want extra organic candidates). APOLLO_ONLY
            # mode skips it — that mode is reserved for scopes where the
            # caller wants Apollo-mediated org records only.
            _pass25_mode_active = mode != DiscoveryMode.APOLLO_ONLY
            if _cse_key and _cse_cx and _pass25_mode_active and not self._cancelled:
                _cse_cities = list(self._cities_to_search or [])
                _cse_kws = list(keywords or [])[:5]
                _cse_client = GoogleCustomSearchDiscovery(
                    api_key=_cse_key,
                    cx=_cse_cx,
                    is_platform_domain=V5mod.is_platform_domain,
                    log_fn=self._log,
                )
                pool_cse = _cse_client.discover(
                    keywords=_cse_kws,
                    cities=_cse_cities,
                    country=self.country,
                )
                # Drop overlap with already-discovered domains.
                pool_cse = {
                    d for d in pool_cse
                    if d and d not in pool_semrush and d not in pool_serp
                }
                pool_serp |= pool_cse
                # Track CSE calls in the shared counter so /api/credits and
                # the per-run breakdown reflect CSE usage.
                self._api_counter["google_custom_search"] = int(
                    self._api_counter.get("google_custom_search", 0)
                ) + _cse_client.calls_made
                self._log(
                    f"   [Discovery/GoogleCSE] +{len(pool_cse)} organic domains "
                    f"({_cse_client.calls_made} API calls, cap={_CSE_MAX_Q} "
                    f"queries / {_CSE_MAX_D} domains)"
                )
        except Exception as _cse_exc:
            self._log(f"   [Discovery/GoogleCSE] failed: {_cse_exc}")

        # ── PASS 3: SEMrush competitor expansion ──
        # For every domain found in Pass 1, pull its paid competitors from
        # SEMrush (domain_adwords_adwords). Each call returns up to 10 domains
        # that also run Google Ads in the same category — all confirmed paid
        # advertisers. 100 seeds × 10 competitors = ~400 unique new domains,
        # turning the initial ~100-domain pool into 400-600 for more companies.
        #
        # 2026-05-18: this Pass was the biggest unguarded SEMrush leak in the
        # whole pipeline. 120 seeds × ~400 weighted units each = up to 48 000
        # units burned for a 3-lead run that Pass 1 already discovered enough
        # domains for. New rules:
        #   • silent scope → skip entirely (no paid competitors to find).
        #   • max_leads ≤ 5 → skip entirely (Pass 1 + Apollo fallback are enough).
        #   • max_leads ≤ 25 → 5 seeds, limit=3.
        #   • max_leads ≤ 100 → 30 seeds, limit=5.
        #   • max_leads > 100 (or unlimited) → 120 seeds, limit=10 (original).
        # Plus an early-exit once the pool already has max_leads*5 candidates.
        # 2026-05-18 (round 3): drastic limit cut across the board. Each
        # competitor call bills 40 units/row, so limit=10 = 400 units per
        # seed. Even at 30 seeds × limit=5 that was 6000 units burned in
        # one Pass — for a 50-lead run that's 60% of the run's budget gone
        # before lead enrichment even starts. New table is tightened, AND
        # we add an EARLY-EXIT: when Pass 1 already produced ≥ max_leads*4
        # candidate domains, the Pass-3 expansion is gratuitous noise and
        # is skipped entirely.
        # 2026-05-21: mid-run pivot detection — if SEMrush has exhausted
        # (budget cap or silent scope) AND Google Places is available, flip
        # the rest of discovery into GOOGLE_ONLY mode. We keep `pool_semrush`
        # and `pool_serp` (no partial discard).
        if should_pivot_to_google(
            self._api_counter, getattr(self, "_semrush_silent_scope", False),
            V5mod.API_KEYS, self.country, mode,
        ):
            _was_mode = mode
            mode = DiscoveryMode.GOOGLE_ONLY
            self._discovery_mode = mode
            log_pivot(
                self._log,
                semrush_pool_size=len(pool_semrush),
                silent_scope=bool(getattr(self, "_semrush_silent_scope", False)),
                budget_exhausted=bool(self._api_counter.get(
                    "semrush_budget_exhausted_logged", False)),
            )

        _mx_cp = int(self.max_leads or 0)
        _pool_already_ample = (_mx_cp > 0 and len(pool_semrush) >= _mx_cp * 4)
        if _pool_already_ample:
            _comp_seed_cap, _comp_limit = 0, 0
        elif _mx_cp <= 0:
            _comp_seed_cap, _comp_limit = 50, 5    # unlimited mode: still bounded
        elif _mx_cp <= 5:
            _comp_seed_cap, _comp_limit = 0, 0
        elif _mx_cp <= 25:
            _comp_seed_cap, _comp_limit = 5, 3
        elif _mx_cp <= 100:
            _comp_seed_cap, _comp_limit = 15, 3    # was 30 × 5 = 6000u → now 15 × 3 = 1800u
        else:
            _comp_seed_cap, _comp_limit = 40, 5    # was 120 × 10 = 48000u → now 40 × 5 = 8000u
        # 2026-05-21: also gate Pass-3 by mode — only runs in BOTH/SEMRUSH_ONLY.
        _pass3_mode_active = mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
        if (pool_semrush and not self._cancelled
                and _comp_seed_cap > 0
                and _pass3_mode_active
                and not getattr(self, "_semrush_silent_scope", False)):
            _comp_seeds = list(pool_semrush)[:min(len(pool_semrush), _comp_seed_cap)]
            _comp_added = 0
            # Pool-size early-exit threshold — once we have enough candidates
            # there's no point burning more credits hunting for marginals.
            _pool_target = max(domain_cap, _mx_cp * 5 if _mx_cp > 0 else domain_cap)
            self._log(f"   [Discovery/CompExpand] Expanding {len(_comp_seeds)} seed domains via paid competitors "
                      f"(limit={_comp_limit}, max_leads={_mx_cp})…")
            self._progress(19, f"Competitor expansion: scanning {len(_comp_seeds)} seeds…")
            for _seed in _comp_seeds:
                if self._cancelled or len(pool_semrush) >= _pool_target:
                    break
                try:
                    comps = semrush.get_domain_competitors(_seed, db, limit=_comp_limit)
                    for c in (comps or []):
                        c = (c or "").strip().lower()
                        if (c and c not in pool_semrush and c not in pool_serp
                                and not V5mod.is_platform_domain(c)):
                            pool_semrush.add(c)
                            _comp_added += 1
                except Exception:
                    pass
            self._log(f"   [Discovery/CompExpand] +{_comp_added} competitor domains → {len(pool_semrush)} SEMrush total")
            self._progress(22, f"Discovery pool: {len(pool_semrush)} domains")
        elif pool_semrush and not self._cancelled:
            self._log(
                f"   [Discovery/CompExpand] Skipped — "
                f"pool_ample={_pool_already_ample}, "
                f"pool_size={len(pool_semrush)}, "
                f"max_leads={_mx_cp}, "
                f"silent={getattr(self, '_semrush_silent_scope', False)}"
            )

        # ── PASS 3.5: Google Places Intent Discovery (AU-only, additive) ──
        # 2026-05-18: runs AFTER SEMrush + SerpAPI but BEFORE Apollo fallback.
        # Uses ONE key (GOOGLE_PLACES_API_KEY) — no OAuth, no developer
        # tokens. Returns AU businesses + their websites for high-intent
        # queries like "commercial plumber Sydney Australia". Domains are
        # stored SEPARATELY in `pool_google_intent` and `self._google_intent_domains`
        # so they ride through V5 as unconfirmed candidates and export with
        # `source = Google Intent` — NEVER granted the `confirmed_paid`
        # bypass that pool_semrush domains receive.
        pool_google_intent: set = set()
        try:
            from google_places_intent import (
                GooglePlacesIntentDiscovery,
                MAX_PLACES_CALLS as _GP_MAX_CALLS,
                MAX_DOMAINS as _GP_MAX_DOMAINS,
            )
            # 2026-05-18: read the key from V5's central API_KEYS dict so
            # operators only need to set GOOGLE_PLACES_API_KEY in ONE place
            # (env var, .env file, or the V5.API_KEYS default — same
            # pattern as semrush/apollo/lusha/serpapi).
            _gp_key = (
                V5mod.API_KEYS.get("google_places", "")
                or os.environ.get("GOOGLE_PLACES_API_KEY", "")
            ).strip()
            _gp_country_ok = (self.country or "").strip().upper() == "AU"
            # 2026-05-21: PASS 3.5 active when mode is BOTH or GOOGLE_ONLY.
            # In GOOGLE_ONLY (initial or pivot) Google is now the PRIMARY
            # discovery source, so widen the keyword sweep from 5 → 15 to
            # compensate for the missing SEMrush probes.
            _pass35_mode_active = mode in (DiscoveryMode.BOTH, DiscoveryMode.GOOGLE_ONLY)
            if _gp_key and _gp_country_ok and _pass35_mode_active and not self._cancelled:
                # Use the actual scoped city list (already AU-filtered by
                # the city_pipeline state-tier resolver) + the top primary
                # keywords as the (kw × city) sweep input. NO new SEMrush
                # calls happen here — keywords come from `self.keywords`
                # which is already populated by Phase 1+2 of the host run.
                _gp_cities = list(self._cities_to_search or [])
                _gp_kw_n = 15 if mode == DiscoveryMode.GOOGLE_ONLY else 5
                # Credit-saving: scale the Places keyword axis to the target so a
                # 1-lead run doesn't fire 20+ Places queries (cuts unnecessary
                # Google Places usage). Floor of 3 keeps discovery viable.
                if self.credit_saver:
                    _gp_kw_n = min(_gp_kw_n, max(3, int(self.max_leads or 0) * 3))
                # 2026-06-09: SMART KEYWORD POOL. The Google Places sweep is what
                # picks which businesses we discover; generic travel/hospitality
                # keywords (e.g. "booking ...") return big-brand HOTELS/airlines —
                # which DO advertise (so they pass the paid gate) but are useless
                # B2B leads and read ~0 paid_traffic on the user's own domain
                # checks. We drop those terms so Places returns local SERVICE
                # SMBs (the high-lead-probability pool). Trade keywords never
                # contain these words, so this is safe. Toggle: SMART_KEYWORDS=0.
                _smart_kw = str(os.environ.get("SMART_KEYWORDS", "1")).strip() != "0"
                _low_yield_terms = (
                    "booking", "hotel", "motel", "hostel", "resort", "accommodation",
                    "flight", "airline", "airfare", "cruise", "lastminute",
                    "travel agency", "travel agent", "holiday package", "tour package",
                )
                def _kw_high_yield(k: str) -> bool:
                    kl = (k or "").lower()
                    return not any(t in kl for t in _low_yield_terms)
                _kw_pool = ([k for k in (keywords or []) if _kw_high_yield(k)]
                            if _smart_kw else list(keywords or []))
                if not _kw_pool:                       # never let the filter zero it out
                    _kw_pool = list(keywords or [])
                _gp_kws = _kw_pool[:_gp_kw_n]
                _gp_client = GooglePlacesIntentDiscovery(
                    api_key=_gp_key,
                    is_platform_domain=V5mod.is_platform_domain,
                    log_fn=self._log,
                )
                # 2026-05-26: paid-traffic verifier — mirrors V5's
                # `paid_traffic >= 5` gate at FETCH time. Google itself does
                # NOT expose a per-domain paid-traffic API; the verifier uses
                # the EXISTING SEMrush domain_ranks call as the authoritative
                # source (same data the post-Apollo gate already trusts) and
                # falls back to a SerpAPI ads-only branded query for domains
                # SEMrush has no row for (often the case for AU SMBs).
                # Volume floor = max_leads × 4 so a thin-coverage scope still
                # produces enough candidates for Apollo enrichment.
                from google_places_intent import (
                    PAID_MIN_THRESHOLD as _GP_PAID_MIN,
                    PAID_VERIFY_MAX as _GP_VERIFY_MAX,
                )
                _verify_db = db
                _verify_gl = gl
                _verify_semrush_cap = max(
                    0,
                    int(getattr(self, "_semrush_budget", 0))
                    - int(getattr(self, "_semrush_calls_this_run", 0)),
                )
                _verify_semrush_active = (
                    mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
                    and not getattr(self, "_semrush_silent_scope", False)
                )
                # 2026-05-26: snapshot the already-discovered confirmed-paid
                # pools so the verifier can short-circuit FREE. Any domain
                # we've already seen running ads (pool_serp_ads from PASS 2a)
                # or already confirmed in SEMrush (pool_semrush from PASS 1)
                # gets a free pass without burning another API call.
                _free_verified_pool = set(pool_serp_ads) | set(pool_semrush)
                # 2026-05-28: Google Ads Transparency Center verifier — the
                # PRIMARY paid signal now. FREE, no key, and it catches small
                # AU local advertisers that SEMrush/Ahrefs miss entirely
                # (verified: proximityplumbing.com.au, thelocalplug.com.au —
                # both 0 paid_traffic in SEMrush but confirmed advertisers in
                # Google's own ATC). Matches by business NAME (from the Places
                # displayName), so it needs _gp_client.domain_to_name, which
                # is populated during discover() BEFORE this gate runs.
                _atc = None
                _atc_confirmed: set = set()
                _atc_ids: dict = {}   # domain -> advertiser_id (for CSV proof column)
                if str(os.environ.get("ATC_ENABLED", "1")).strip() != "0":
                    try:
                        from google_ads_transparency import AdsTransparencyVerifier
                        _atc = AdsTransparencyVerifier(country=self.country, log_fn=self._log)
                    except Exception as _atc_e:
                        self._log(f"   [Discovery/ATC] init failed: {_atc_e}")
                        _atc = None

                def _paid_traffic_verifier(domain: str) -> int:
                    """Return int paid_traffic estimate for `domain`.
                    Priority order (cheapest first):
                      0. Ads Transparency Center by business name (FREE, Google
                         ground-truth — primary signal).
                      1. FREE pool — domain already in pool_serp_ads / pool_semrush.
                      2. SEMrush domain_ranks (authoritative; skipped if dead).
                      3. SerpAPI ads-only branded query (skipped if dead).
                    Returns 0 on any failure → caller drops the domain."""
                    if not domain:
                        return 0
                    _dl = domain.lower()
                    # 0. ATC — free, working even when SEMrush/SerpAPI are dead.
                    if _atc is not None:
                        try:
                            _nm = (_gp_client.domain_to_name.get(_dl)
                                   or _dl.split(".")[0].replace("-", " "))
                            _hit = _atc.is_advertiser(_nm) if _nm else None
                            if _hit:
                                _atc_confirmed.add(_dl)
                                _atc_ids[_dl] = _hit.get("advertiser_id", "")
                                return 999   # Google-confirmed advertiser
                        except Exception:
                            pass
                    # 1. Already in a confirmed-paid pool from earlier passes.
                    if _dl in _free_verified_pool:
                        return 999
                    # 2. SEMrush primary verifier.
                    if _verify_semrush_active:
                        try:
                            ov = semrush.get_domain_overview_metrics(domain, _verify_db)
                            _pt = int(ov.get("paid_traffic", 0) or 0)
                            if _pt > 0:
                                return _pt
                        except Exception:
                            pass
                    # 3. SerpAPI fallback — branded query, check if domain
                    # appears in `ads[]`. Same SerpAPI key, 1 unit per call.
                    try:
                        if serp._available:
                            _brand = domain.split(".")[0]
                            _ads = serp.search_keyword_ads_only(_brand, _verify_gl, num=20) or []
                            if _dl in {a.lower() for a in _ads if a}:
                                return 999   # ad-verified
                    except Exception:
                        pass
                    return 0
                # ATC checks are free + fast, so allow many more than the
                # SEMrush/SerpAPI-bound default cap when ATC is active (verify
                # EVERY Places domain we can afford to, since the goal is to
                # keep only confirmed advertisers).
                _verify_cap = (
                    int(os.environ.get("ATC_VERIFY_MAX", "120") or "120")
                    if _atc is not None else _GP_VERIFY_MAX
                )
                # 2026-06-09: in credit-saving mode, don't verify 120 names for a
                # tiny target — scale ATC/verify lookups to the lead count so we
                # stop hammering ATC (and the SerpAPI fallback) unnecessarily.
                if self.credit_saver:
                    _verify_cap = min(_verify_cap, max(15, int(self.max_leads or 0) * 12))
                # 2026-06-01: STRICT_PAID_ONLY=1 → no Places-backfill of
                # non-advertisers (volume_floor=0). Only ATC/SEMrush/SerpAPI
                # -confirmed advertisers survive. Use when you'd rather get
                # FEWER leads than non-advertisers.
                _strict_paid = str(os.environ.get("STRICT_PAID_ONLY", "0")).strip() == "1"
                _volume_floor = 0 if _strict_paid else max(0, int(self.max_leads or 0) * 4)
                if _strict_paid:
                    self._log(
                        "   [Discovery/GooglePlaces] STRICT_PAID_ONLY=1 — "
                        "dropping all non-advertisers (volume_floor=0)"
                    )
                pool_google_intent = _gp_client.discover(
                    keywords=_gp_kws,
                    cities=_gp_cities,
                    country="AU",
                    paid_traffic_verifier=_paid_traffic_verifier,
                    min_paid_traffic=_GP_PAID_MIN,
                    max_verify_calls=_verify_cap,
                    volume_floor=_volume_floor,
                )
                # Drop overlap with already-known pools to keep the label
                # set tight (a domain already discovered via SEMrush stays
                # `confirmed_paid`; we never demote it).
                pool_google_intent = {
                    d for d in pool_google_intent
                    if d and d not in pool_semrush and d not in pool_serp
                }
                # 2026-06-02 (FIX): ATC confirms a business runs SOME Google ad
                # (Search/Display/YouTube/historical), which is BROADER than the
                # SEARCH paid-traffic that Ahrefs/SEMrush measure. So ATC-only
                # domains are kept as confirmed advertisers BUT in a SEPARATE
                # tier (`_atc_only_domains`) — they must NOT be ranked as
                # equal to SerpAPI live-Search-ads domains (which DO show in
                # Ahrefs/SEMrush). `_serp_ads_domains` stays = SerpAPI ads[]
                # block ONLY (true Search advertisers). This is why prior runs
                # surfaced fixatap/pjcplumbing (ATC/organic, 0 Ahrefs paid).
                _atc_in_pool = {d for d in _atc_confirmed if d in pool_google_intent}
                if _atc_in_pool:
                    if not isinstance(getattr(self, "_atc_only_domains", None), set):
                        self._atc_only_domains = set()
                    # Only count as ATC-only if NOT already a SerpAPI Search ad.
                    self._atc_only_domains.update(
                        d for d in _atc_in_pool if d not in getattr(self, "_serp_ads_domains", set())
                    )
                    if not isinstance(getattr(self, "_confirmed_paid", None), set):
                        self._confirmed_paid = set()
                    self._confirmed_paid.update(_atc_in_pool)
                    # Stash advertiser IDs for the CSV proof column.
                    if not isinstance(getattr(self, "_atc_advertiser_ids", None), dict):
                        self._atc_advertiser_ids = {}
                    for _d in _atc_in_pool:
                        if _atc_ids.get(_d):
                            self._atc_advertiser_ids[_d] = _atc_ids[_d]
                self._log(
                    f"   [Discovery/GooglePlaces] +{len(pool_google_intent)} AU domains "
                    f"({_gp_client.calls_made} API calls); "
                    f"{len(_atc_in_pool)} CONFIRMED Google advertisers via Ads "
                    f"Transparency Center ({_atc.calls_made if _atc else 0} ATC lookups)"
                )
            elif not _gp_key:
                # Silent skip — Google Places is optional; absence is fine.
                pass
            elif not _gp_country_ok:
                self._log(
                    f"   [Discovery/GooglePlaces] AU-only — country={self.country!r} skipped"
                )
        except Exception as _gp_exc:
            self._log(f"   [Discovery/GooglePlaces] failed: {_gp_exc}")

        # Stash for the inner V5 pipeline. NEVER added to _confirmed_paid.
        self._google_intent_domains = set(pool_google_intent)

        # ── PASS 4: Apollo mixed_companies/search — guaranteed fallback ──
        # Only spend Apollo credits here if we haven't hit a respectable
        # pool yet. The Apollo pool is city-precise so it rescues runs
        # where SEMrush returned 0 paid advertisers.
        # 2026-05-18: Google Places domains count toward the fallback
        # threshold — if Places already gave us enough AU businesses we
        # don't need to burn Apollo credits.
        _pre_apollo_pool = len(pool_semrush) + len(pool_serp) + len(pool_google_intent)
        # 2026-05-21: APOLLO_ONLY mode always runs the fallback regardless of
        # pool size (the prior PASSes were all skipped). Other modes use the
        # original threshold so a healthy SEMrush/Places run skips Apollo.
        need_fallback = (
            mode == DiscoveryMode.APOLLO_ONLY
            or _pre_apollo_pool < max(10, self.max_leads)
        )
        if need_fallback and not self._cancelled:
            self._log(
                f"   [Discovery/Apollo] SEMrush+SerpAPI+Places yielded only "
                f"{_pre_apollo_pool} — running Apollo org fallback"
            )
            pool_apollo = self._apollo_org_discovery(V5mod, limit_per_call=25)
            self._log(f"   [Discovery/Apollo] → {len(pool_apollo)} domains")

        # 2026-05-18: include Google Places domains in `discovered` so they
        # get enriched by V5's Phase 4. They're labeled separately via
        # `self._google_intent_domains` for export-time source-stamping.
        discovered = pool_semrush | pool_serp | pool_apollo | pool_google_intent

        # ── PASS 5: Vertex AI (Gemini) semantic ranking ────────────────────
        # 2026-05-25: SerpAPI+Google typically yields 200+ raw domains for
        # a 3-lead run. Apollo enrichment is the bottleneck (~1-2s per
        # domain). Rather than blow through the long tail, we Gemini-rank
        # all candidates by B2B buying intent and keep the top N (where N
        # is a generous multiple of max_leads — Phase 5 still drops some).
        # Falls back to identity (input unchanged) when GEMINI_API_KEY
        # isn't set, so existing runs without the key still work.
        try:
            from vertex_ai_ranker import VertexAIRanker
            _v_key = (
                V5mod.API_KEYS.get("gemini", "")
                or os.environ.get("GEMINI_API_KEY", "")
            ).strip()
            if _v_key and discovered and not self._cancelled:
                _ranker = VertexAIRanker(_v_key, log_fn=self._log)
                # Keep enough domains to satisfy Phase 5's filter + per-
                # company cap. Empirically ~10x max_leads survives 5g/5f
                # for AU SMB scopes, but cap at 80 to keep Apollo spend
                # bounded.
                _keep = min(80, max(20, int((self.max_leads or 5) * 10)))
                # Derive a human-readable industry label from the top 3
                # industries the share-cap resolver picked for this state.
                try:
                    _industry_label = ", ".join(
                        select_industries_by_cap(self.state_code, 100)[:3]
                    ) or "small businesses"
                except Exception:
                    _industry_label = "small businesses"
                _ranked = _ranker.rank_domains(
                    discovered,
                    industry=_industry_label,
                    country=self.country,
                    top_n=_keep,
                )
                _kept = {d for d, _ in _ranked}
                # Preserve set provenance — pools shrink to the intersection.
                _before = len(discovered)
                discovered = discovered & _kept
                pool_semrush = pool_semrush & _kept
                pool_serp = pool_serp & _kept
                pool_apollo = pool_apollo & _kept
                pool_google_intent = pool_google_intent & _kept
                self._log(
                    f"   [Discovery/VertexAI] semantic rank: {_before} → {len(discovered)} "
                    f"highest-intent domains (Gemini calls: {_ranker.calls_made})"
                )
                # Track in shared counter so /api/credits + /status live
                # blocks reflect Gemini usage (cost ~ free on tier-1).
                self._api_counter["gemini"] = int(
                    self._api_counter.get("gemini", 0)
                ) + _ranker.calls_made
        except Exception as _ve:
            self._log(f"   [Discovery/VertexAI] failed: {_ve}")
        # Phase 2 (2026-05-15): mark this scope as "SEMrush-silent" if neither
        # SEMrush nor SerpAPI returned domains. The rediscovery loop + Phase 4
        # paid_traffic gate + Phase 5f top-up all check this flag and skip
        # further SEMrush waste — without it, we burn ~25k credits on a
        # doomed 3-lead run before producing 0 leads.
        # Also stash the Apollo-only set so downstream gates can identify
        # which domains came from the fallback path.
        # 2026-05-21: only flag SEMrush-silent if SEMrush was actually run.
        # In GOOGLE_ONLY / APOLLO_ONLY mode PASSes 1+2 are skipped by design,
        # which would falsely look "silent" and pollute downstream V5 phases
        # with the silent-scope flag (they'd then skip overview calls
        # unnecessarily if the user later re-runs with a SEMrush key).
        _semrush_was_attempted = mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
        if _semrush_was_attempted and not pool_semrush and not pool_serp and pool_apollo:
            self._semrush_silent_scope = True
            self._log("   [Discovery] 🔕 SEMrush+SerpAPI silent — flagging scope as "
                      "Apollo-only. Rediscovery loop will skip further SEMrush probing.")
        try:
            if not hasattr(self, "_apollo_only_domains"):
                self._apollo_only_domains = set()
            self._apollo_only_domains.update(d for d in pool_apollo if d)
        except Exception:
            pass

        if not discovered:
            self._log("   [Discovery] ⚠ ZERO domains from all four sources. Possible causes:")
            self._log("     • SEMrush credits exhausted or returning ERROR (check /api/credits)")
            self._log("     • SerpAPI credits exhausted or rate-limited")
            self._log("     • Google Places key missing or quota hit")
            self._log("     • Google Custom Search key/cx missing or daily quota hit")
            self._log("     • Apollo organization search returned no orgs for this scope")
        else:
            # 2026-05-25: SerpAPI breakdown surfaces the ads-only sub-pool +
            # CSE organic sub-pool so the user can see which sub-source the
            # SerpAPI tally came from. CSE domains live INSIDE pool_serp.
            _serp_organic = len(pool_serp) - len(pool_cse) - len(pool_serp_ads)
            self._log(
                f"   [Discovery] TOTAL: {len(discovered)} domains "
                f"(SEMrush={len(pool_semrush)}, "
                f"SerpAPI={len(pool_serp)} [organic={_serp_organic}, "
                f"ads={len(pool_serp_ads)}, cse={len(pool_cse)}], "
                f"GoogleIntent={len(pool_google_intent)}, Apollo={len(pool_apollo)}) "
                f"| final_mode={mode.value}"
            )

        # PHASE 2 (2026-04-28) — preserve provenance. pool_semrush domains came
        # from phrase_adwords so they're confirmed paid advertisers; the inner
        # pipeline uses this set to bypass the strict paid_traffic/organic_keywords
        # gate (re-checking would be redundant). pool_serp / pool_apollo do NOT
        # get this bypass — they remain subject to the user's full quality gate.
        # ACCUMULATE (not overwrite) so multi-pass rediscovery preserves history.
        if not isinstance(getattr(self, "_confirmed_paid", None), set):
            self._confirmed_paid = set()
        self._confirmed_paid.update(pool_semrush)
        # 2026-05-28: SerpAPI ads[] domains are LIVE Google advertisers — just
        # as "confirmed paid" as SEMrush adwords domains. Promote them so they
        # (a) bypass the paid-traffic gate, (b) sort paid-first in the CSV,
        # (c) get the "Google Ads" Traffic Source label. Tracked separately in
        # _serp_ads_domains for the label; folded into _confirmed_paid for the
        # gate/ordering.
        if not isinstance(getattr(self, "_serp_ads_domains", None), set):
            self._serp_ads_domains = set()
        self._serp_ads_domains.update(d for d in pool_serp_ads if d)
        self._confirmed_paid.update(d for d in pool_serp_ads if d)
        return discovered

    def _apollo_org_discovery(self, V5mod, limit_per_call: int = 25) -> set:
        """Query Apollo's mixed_companies/search with city+state location
        filters. Uses the picked industries' NAME tokens as keyword tags.

        Costs 1 Apollo credit per city+industry-slug pair — capped so a
        whole-country run doesn't loop through 8×92 combinations. We use
        the TOP tier-1 city (or the user-specified city) and the top 6
        industries picked for this state.
        """
        import requests
        out: set = set()

        # Build the location strings Apollo expects
        location_map = {"AU": "Australia", "USA": "United States",
                        "UK": "United Kingdom", "India": "India"}
        country_full = location_map.get(self.country, self.country)
        state_name = None
        if self.state_code in AUSTRALIA_STATES:
            state_name = AUSTRALIA_STATES[self.state_code].get("name")

        # PHASE 2 (2026-04-28, revised) — caps + per_page + pages all scale.
        # Earlier expansion of probe matrix alone wasn't enough: each probe
        # still pulled only page-1 with per_page=25, so duplicate companies
        # across the (city × industry) matrix capped unique results at ~75.
        # Now per_page goes to Apollo's max (100) and we walk pages 1..N for
        # large targets, giving 8 × 25 × 100 × 3 = 60 000 raw results in the
        # extreme case — enough to surface 500-800 unique SMB domains.
        if self.max_leads > 0:
            n_cities     = max(3, min(8,  self.max_leads // 25))
            n_industries = max(6, min(25, self.max_leads // 8))
            overall_cap  = max(75, self.max_leads * 4)
            n_pages      = 1 if self.max_leads < 50 else (2 if self.max_leads < 150 else 3)
            per_page     = 100  # Apollo max — was 25
        else:
            n_cities, n_industries, overall_cap = 3, 6, 75
            n_pages, per_page = 1, 25
        cities_to_probe = self._cities_to_search[:n_cities]
        # Pick top industries by share for the selected state
        pct_cap = min(self.max_leads, 100) if self.max_leads > 0 else 100
        industries = select_industries_by_cap(self.state_code, pct_cap)[:n_industries]

        base_url = f"{V5mod.ApolloClient.BASE_URL}/mixed_companies/search"
        headers = {"X-Api-Key": V5mod.API_KEYS.get("apollo", "")}
        headers.update({"Content-Type": "application/json", "Cache-Control": "no-cache"})

        for city in cities_to_probe:
            if self._cancelled:
                break
            # Compose location — "Melbourne, Victoria" is the standard Apollo format
            if state_name:
                location = f"{city}, {state_name}"
            else:
                location = f"{city}, {country_full}"
            for ind in industries:
                if self._cancelled or len(out) >= overall_cap:
                    break
                # First token of the industry name — "Plumber", "Real Estate Agent" → "real estate"
                tag = ind.lower().split("/")[0].strip()
                # Walk multiple pages so duplicate companies across (city × industry)
                # don't ceiling unique results. Stop early when a page returns
                # fewer rows than per_page (Apollo signal of last page) or when
                # the overall_cap is hit.
                for page in range(1, n_pages + 1):
                    if self._cancelled or len(out) >= overall_cap:
                        break
                    payload = {
                        "q_organization_keyword_tags": [tag],
                        "organization_locations": [location],
                        "per_page": per_page,
                        "page": page,
                    }
                    try:
                        r = requests.post(base_url, json=payload, headers=headers, timeout=30)
                        if r.status_code != 200:
                            break
                        self._api_counter["apollo"] = self._api_counter.get("apollo", 0) + 1
                        orgs = (r.json() or {}).get("organizations") or []
                        if not orgs:
                            break  # no more pages
                        added_this_page = 0
                        for org in orgs:
                            d = (org.get("primary_domain") or "").strip().lower()
                            if not d:
                                d = V5mod.extract_domain(org.get("website_url") or "")
                            if d and not V5mod.is_platform_domain(d) and d not in out:
                                out.add(d)
                                added_this_page += 1
                        # Last page heuristic: short page → no more pages
                        if len(orgs) < per_page:
                            break
                    except Exception as e:
                        self._log(f"   [Discovery/Apollo] error for {location} + {tag} (p{page}): {e}")
                        break
        self._log(f"   [Discovery/Apollo] probed {n_cities} cities × {n_industries} industries × {n_pages} pages → {len(out)} unique domains")
        return out

    def _enrich_round(self, V5mod, keywords: list, domains: list, round_num: int) -> list:
        """Delegate enrichment to V5's LeadGenerationPipeline for one
        round. Returns the run's leads (also merges api_counter into ours).
        """
        if not domains:
            return []
        self._attempted_domains.update((d or "").strip().lower() for d in domains if d)
        sub_folder = os.path.join(
            self.output_folder,
            f"round_{round_num}_{datetime.now().strftime('%H%M%S')}",
        )
        os.makedirs(sub_folder, exist_ok=True)

        city_scope = {
            "state": self.state_code,
            "city": self.city,
            "tier": self.tier,
            "label": self._scope_label,
        }

        # Industry label stored on every lead — use the top picked industry as a
        # representative value so existing Phase 6 logging paths don't blow up.
        label_industry = f"City-Mode / {self._scope_label}"

        # Round-1 size drives the inner pipeline's max_leads quota. We give it
        # up to 2× the global deficit so Phase 5 dedup has room to trim.
        if self.max_leads > 0:
            deficit = max(1, self.max_leads - len(self.leads))
            inner_max = max(deficit, int(self.max_leads))
        else:
            inner_max = 0

        inner = V5mod.LeadGenerationPipeline(
            industry=label_industry,
            country=self.country,
            min_volume=self.min_volume,
            min_cpc=0.0,           # paid filter already happened in discovery
            output_folder=sub_folder,
            progress_callback=self._inner_progress(round_num),
            log_callback=lambda m: self._log(m),
            max_leads=inner_max,
            enrichment_enabled=self.enrichment_enabled,
            disable_semrush=self.disable_semrush,
            credit_saver=self.credit_saver,
            paid_only_all=self.paid_only_all,
            preset_keywords=keywords,
            preset_domains=domains,
            confirmed_paid_domains=self._confirmed_paid,
            city_scope=city_scope,
            quota_guarantee=self.quota_guarantee,
            # Phase 2 (2026-05-15): pass the credit-safety flags so V5's Phase 4
            # gate + Phase 5f top-up know whether SEMrush has data for this
            # scope. When silent, V5 skips the SEMrush domain_overview call
            # per Apollo-only domain (was 141 wasted calls on the bad run)
            # and relaxes the strict paid_traffic gate (Apollo-discovered
            # local businesses always show paid_traffic=0 in SEMrush gaps).
            semrush_silent_scope=getattr(self, "_semrush_silent_scope", False),
            apollo_only_domains=getattr(self, "_apollo_only_domains", set()),
            # 2026-05-18 (round 2): hand the inner pipeline our `_api_counter`
            # so SEMrush budget state (cap, units used, per-phase, skip count,
            # alert flags) is SHARED with whatever Pass 1/3 + rediscovery have
            # already spent. Without this, the inner would have its own fresh
            # 300-unit pool on top of the outer's — easy to bust by accident.
            shared_counter=self._api_counter,
            # 2026-05-18 (round 4): Google Places domains for source labeling.
            # Inner V5 stamps lead.source = "Google Intent" on these in the
            # CSV export. They DO NOT enter `_confirmed_paid_domains` so the
            # paid-traffic gate still applies — they only get the silent-
            # scope relaxation that Apollo-only domains receive.
            google_intent_domains=getattr(self, "_google_intent_domains", set()),
            # 2026-05-28: live-Google-Ads domains (SerpAPI ads[] block) for the
            # "Traffic Source = Google Ads" CSV label + paid-first ordering.
            serp_ads_domains=getattr(self, "_serp_ads_domains", set()),
            # 2026-05-28: ATC advertiser-id map for the CSV proof columns.
            atc_advertiser_ids=getattr(self, "_atc_advertiser_ids", {}),
            # 2026-06-02: tiering inputs so the FINAL cut prioritises true
            # Search advertisers (Ahrefs/SEMrush-visible) over ATC-only/organic.
            heavy_advertiser_domains=getattr(self, "_heavy_advertisers", set()),
            atc_only_domains=getattr(self, "_atc_only_domains", set()),
        )
        self._inner_pipeline = inner
        csv_path = inner.run()
        self._inner_pipeline = None

        # Merge api counters so the frontend stats panel shows cumulative usage.
        # 2026-05-18 (round 2): when shared_counter is in use, `inner._api_counter`
        # IS `self._api_counter` (same dict by reference), so merging would
        # double-count every key. Detect and skip in that case.
        if inner._api_counter is not self._api_counter:
            for k, v in (inner._api_counter or {}).items():
                if isinstance(v, (int, float)):
                    self._api_counter[k] = self._api_counter.get(k, 0) + v

        # 2026-05-18 (round 2): with the shared counter wired up, the canonical
        # numbers live in `_api_counter` already — we just mirror them onto
        # the conventional `_inner_*` attrs the /generate-city summary reads.
        # When the inner had its own counter (shared_counter not used; legacy
        # path) we fall back to the per-wave sum logic.
        try:
            if inner._api_counter is self._api_counter:
                self._inner_units_used = int(self._api_counter.get("semrush_units", 0) or 0)
                self._inner_unit_budget = int(self._api_counter.get("semrush_budget", 0) or 0)
                _phases = self._api_counter.get("semrush_units_by_phase", {})
                self._inner_units_by_phase = dict(_phases) if isinstance(_phases, dict) else {}
            else:
                _inner_semrush = getattr(inner, "semrush", None)
                if _inner_semrush is not None:
                    if not hasattr(self, "_inner_units_used"):
                        self._inner_units_used = 0
                        self._inner_unit_budget = 0
                        self._inner_units_by_phase = {}
                    self._inner_units_used += int(getattr(_inner_semrush, "_units_used", 0) or 0)
                    self._inner_unit_budget = max(
                        self._inner_unit_budget,
                        int(getattr(_inner_semrush, "_unit_budget", 0) or 0),
                    )
                    for _ph, _u in (getattr(_inner_semrush, "_units_by_phase", {}) or {}).items():
                        self._inner_units_by_phase[_ph] = self._inner_units_by_phase.get(_ph, 0) + int(_u or 0)
        except Exception:
            pass

        return list(inner.leads or [])

    def _find_competitors(self, V5mod, leads: list, visited: set) -> list:
        """For the top leads of the current pool, pull SEMrush competitor
        domains. Returns a list of at most MAX_NEW_DOMAINS_PER_ROUND new
        domains (not already visited, not platform).

        2026-05-18: gated by silent-scope + max_leads. Each call costs
        ~120 weighted units (limit=3 × 40/row); 40 seeds = 4 800 units —
        more than the entire per-run budget for a 3-lead run. Now:
          • silent scope → return [] (no point probing dead SEMrush).
          • max_leads ≤ 5 → cap seeds to 2, limit to 2.
          • max_leads ≤ 25 → cap seeds to 5.
          • otherwise → original 40 seeds.
        """
        # Silent-scope short-circuit.
        if getattr(self, "_semrush_silent_scope", False):
            return []
        _mx_fc = int(self.max_leads or 0)
        if _mx_fc <= 0:
            _seed_cap, _limit_fc = 40, MAX_COMPETITORS_PER_DOMAIN
        elif _mx_fc <= 5:
            _seed_cap, _limit_fc = 2, 2
        elif _mx_fc <= 25:
            _seed_cap, _limit_fc = 5, MAX_COMPETITORS_PER_DOMAIN
        else:
            _seed_cap, _limit_fc = 40, MAX_COMPETITORS_PER_DOMAIN

        semrush = V5mod.SemrushClient(V5mod.API_KEYS["semrush"])
        semrush._counter = self._api_counter
        # 2026-05-18 (round 2): tag rediscovery-wave calls so the per-phase
        # breakdown distinguishes them from primary discovery and the inner
        # pipeline. Budget still comes from the shared `_api_counter`.
        semrush._current_phase = "city_rediscovery"
        semrush._log_cb = self._log
        db = V5mod.COUNTRY_CONFIG.get(self.country, {}).get("semrush_db", "au")

        # Pick seeds: best-scoring unique domains from current leads
        seen_seeds = set()
        seeds: list = []
        for ld in leads:
            d = (ld.get("domain") or "").strip().lower()
            if d and d not in seen_seeds:
                seen_seeds.add(d)
                seeds.append(d)
            if len(seeds) >= _seed_cap:
                break

        new_domains: list = []
        seen_new = set()
        for seed in seeds:
            if self._cancelled or len(new_domains) >= MAX_NEW_DOMAINS_PER_ROUND:
                break
            try:
                comps = semrush.get_domain_competitors(seed, db, limit=_limit_fc)
            except Exception:
                comps = []
            for c in comps or []:
                c = (c or "").strip().lower()
                if not c or c in visited or c in seen_new:
                    continue
                if V5mod.is_platform_domain(c):
                    continue
                seen_new.add(c)
                new_domains.append(c)
                if len(new_domains) >= MAX_NEW_DOMAINS_PER_ROUND:
                    break
        return new_domains

    def _cap_by_dm_confidence(self, V5mod, leads: list) -> list:
        """City-mode only: reduce to 1 lead per company, unless the top
        lead's role doesn't read as a high-confidence decision maker.

        Uses V5's own _role_hierarchy_score so the scoring stays
        identical to the industry-mode pipeline:

          score >= 85  (owner / founder / C-suite / partner)  -> keep 1
          score 60-84  (VP / head of / director / manager)    -> keep 2
          score <  60  (senior / individual contributor / no
                        role / intern / unknown)               -> keep 3 (current cap)

        Within a company we always sort by hierarchy score first, so the
        "1 kept" is the most senior role the pipeline found.
        """
        from collections import defaultdict
        if not leads:
            return leads

        hscore = V5mod._role_hierarchy_score  # reuse V5's exact scorer

        groups = defaultdict(list)
        no_domain = []
        for ld in leads:
            d = (ld.get("domain") or "").strip().lower()
            if d:
                groups[d].append(ld)
            else:
                no_domain.append(ld)

        # ADAPTIVE CAP: when few unique companies vs target leads, raise per-company
        # cap so quota is reachable. E.g. 50 companies × 145 target → need 3+ each.
        n_companies = len([k for k in groups.keys() if k])
        if (self.max_leads and self.max_leads > 0 and n_companies > 0
                and not self.enrichment_enabled):
            adaptive_floor = (self.max_leads // n_companies) + 2
        else:
            adaptive_floor = 0

        out: list = []
        overflow_pool: list = []  # leads dropped by per-company cap — used to fill quota
        stats = {"kept_1": 0, "kept_2": 0, "kept_3": 0}
        for domain, group in groups.items():
            # Sort within the domain by hierarchy score (highest first);
            # break ties by the lead's existing completeness signals so
            # an owner with phone+email beats an owner with just a name.
            def _tiebreak(ld):
                return (
                    hscore(ld.get("role", "")),
                    1 if ld.get("phone") else 0,
                    1 if ld.get("email") else 0,
                    1 if (ld.get("name") and " " in (ld.get("name") or "")) else 0,
                )
            group.sort(key=_tiebreak, reverse=True)
            top_score = hscore(group[0].get("role", ""))

            if not self.enrichment_enabled:
                # Enrichment-OFF: roles are unverified Apollo titles — be generous.
                # Limits: 3 / 4 / 5 so quota is reachable from fewer companies.
                if top_score >= 85:
                    keep = max(3, adaptive_floor)
                    stats["kept_1"] += 1
                elif top_score >= 60:
                    keep = max(4, adaptive_floor)
                    stats["kept_2"] += 1
                else:
                    keep = max(5, adaptive_floor)
                    stats["kept_3"] += 1
            else:
                if top_score >= 60:
                    keep = 1
                    stats["kept_1"] += 1
                else:
                    keep = 2
                    stats["kept_3"] += 1

            out.extend(group[:keep])
            # Keep overflow as a fallback fill-pool (instead of dropping)
            if len(group) > keep:
                overflow_pool.extend(group[keep:])

        out.extend(no_domain)  # no-domain leads are untouched (can't group)
        # FINAL FILL: if still short of quota, pull from overflow (highest-priority first)
        if (self.max_leads and self.max_leads > 0 and len(out) < self.max_leads
                and overflow_pool):
            overflow_pool.sort(key=lambda ld: hscore(ld.get("role", "")), reverse=True)
            room = self.max_leads - len(out)
            filled = overflow_pool[:room]
            if filled:
                self._log(f"[City Mode] Filling +{len(filled)} leads from overflow pool to reach quota")
                out.extend(filled)
        self._log(
            f"[City Mode] DM-confidence cap (enrichment-ON): "
            f"{stats['kept_1']} companies @ 1 lead (high/med confidence), "
            f"{stats['kept_3']} @ 2 (low confidence)"
        )
        return out

    def _export_combined(self, V5mod) -> str:
        """Merge leads from every round into one CSV pair using V5's
        Phase 6 writer logic. We spin a minimal inner pipeline purely as a
        host for _phase6_export — setting its .leads directly."""
        if not self.leads:
            return ""
        # Dedup by (domain + email) or (domain + name + role)
        seen = set()
        deduped = []
        for ld in self.leads:
            domain = (ld.get("domain") or "").lower()
            email = (ld.get("email") or "").lower()
            name = (ld.get("name") or "").lower()
            role = (ld.get("role") or "").lower()
            key = f"{domain}|{email}" if email else f"{domain}|{name}|{role}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(ld)
        self.leads = deduped

        # 2026-06-03: stamp each lead with the discovery keyword that first
        # surfaced its domain (for the CSV/UI "Keyword" column). Post-hoc,
        # observational — does not affect selection or quality.
        try:
            from utils import root_domain as _rootd
        except Exception:
            _rootd = lambda x: (x or "").strip().lower()
        for _ld in self.leads:
            if _ld.get("_discovery_keyword"):
                continue
            _d = (_ld.get("domain") or "").strip().lower()
            _kw = (self._domain_discovery_kw.get(_d)
                   or self._domain_discovery_kw.get(_rootd(_d))
                   or "")
            if _kw:
                _ld["_discovery_keyword"] = _kw

        if self.quota_guarantee:
            self._log(
                "[City Mode] Quota guarantee active: final strict-first ranking "
                "delegated to V5 Phase 6 reserves and exact-count padding"
            )
        else:
            # Legacy behavior for callers that explicitly disable guarantee mode.
            self.leads = self._cap_by_dm_confidence(V5mod, self.leads)
            if self.max_leads > 0:
                cap = int(self.max_leads * 1.2)
                if len(self.leads) > cap:
                    self.leads = self.leads[:cap]

        os.makedirs(self.output_folder, exist_ok=True)
        # Sanitize the industry slug so Phase 6's f-string is safe.
        slug = re.sub(r"[^\w]+", "_", self._scope_label.lower()).strip("_") or "city"
        host = V5mod.LeadGenerationPipeline(
            industry=f"CityMode_{slug}",
            country=self.country,
            min_volume=self.min_volume,
            min_cpc=0.0,
            output_folder=self.output_folder,
            progress_callback=lambda *a: None,
            log_callback=lambda m: self._log(m),
            max_leads=self.max_leads,
            enrichment_enabled=self.enrichment_enabled,
            quota_guarantee=self.quota_guarantee,
            disable_semrush=self.disable_semrush,
            credit_saver=self.credit_saver,
            paid_only_all=self.paid_only_all,
            # 2026-06-02 (CRITICAL): the FINAL CSV selection happens here, in
            # this host's Phase 6. Without the advertiser sets, host._advertiser_tier
            # saw EMPTY sets → every lead tier 0 → paid-first ordering was a
            # no-op → heavy Search advertisers found in discovery never won the
            # final slots (organic/ATC took them). Pass ALL provenance sets so
            # the final cut ranks true Search advertisers first.
            confirmed_paid_domains=getattr(self, "_confirmed_paid", set()),
            serp_ads_domains=getattr(self, "_serp_ads_domains", set()),
            heavy_advertiser_domains=getattr(self, "_heavy_advertisers", set()),
            atc_only_domains=getattr(self, "_atc_only_domains", set()),
            google_intent_domains=getattr(self, "_google_intent_domains", set()),
            apollo_only_domains=getattr(self, "_apollo_only_domains", set()),
            atc_advertiser_ids=getattr(self, "_atc_advertiser_ids", {}),
        )
        host.leads = list(self.leads)
        try:
            out = host._phase6_export()
        except Exception as e:
            self._log(f"[City Mode] Export failed: {e}")
            return ""
        return out or ""

    def _fetch_apollo_credits(self, V5mod) -> int:
        try:
            host = V5mod.LeadGenerationPipeline(
                industry="_probe", country=self.country, min_volume=0, min_cpc=0.0,
                output_folder=self.output_folder,
                progress_callback=lambda *a: None, log_callback=lambda *a: None,
                max_leads=0,
            )
            return host._fetch_apollo_credits_remaining()
        except Exception:
            return -1

    def _inner_progress(self, round_num: int):
        """Wrap the inner pipeline's progress so Round-N phases map onto
        our overall 0-100% band. Round 1 occupies 28→85%, later rounds
        85→95% (smaller because they're incremental top-ups)."""
        if round_num == 1:
            lo, hi = 28, 85
        else:
            lo, hi = 85, 95
        span = hi - lo

        def _cb(pct: int, status: str = ""):
            mapped = lo + int(max(0, min(100, pct)) / 100 * span)
            self._progress(mapped, status)
        return _cb

    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        try:
            self.log_callback(f"[{ts}] {msg}")
        except Exception:
            pass

    def _progress(self, pct: int, status: str = ""):
        try:
            self.progress_callback(pct, status)
        except Exception:
            pass
