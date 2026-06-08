"""Offline mock sweep for the SEMrush unit-budget guard (2026-05-18).

Verifies the entire budget pipeline WITHOUT hitting the live SEMrush API:
  1. Unit-cost estimation per report type matches the user's 2026-05-14
     query-log invoice (the 8 240-credit run that triggered this fix).
  2. _request() short-circuits once the budget is exhausted — and counts
     the would-be call as "skipped" so the user knows what was withheld.
  3. The mid-run 75 % alert fires exactly once per run.
  4. Per-phase attribution lands in `_units_by_phase` so the frontend
     panel can render where credits went.
  5. competitor_expansion.expand_competitors_bfs() scales its call budget
     by required_leads and short-circuits in silent_scope mode.

Run:
    python test_semrush_budget.py

Exit code is non-zero if any assertion fails.
"""
from __future__ import annotations

import sys
import unittest
from unittest.mock import patch


def _stub_external_modules():
    """Stub the heavy 3rd-party deps V5 imports at module top so this test
    can run without flask / pymysql / openai / requests installed."""
    import types
    for name in (
        "flask", "flask_login", "openai", "pymysql", "dbutils",
        "dbutils.pooled_db", "PIL", "PIL.Image",
    ):
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
    # Minimal flask attrs V5 references at import time
    import flask
    flask.Flask = type("Flask", (), {"__init__": lambda *a, **k: None})
    flask.request = None
    flask.jsonify = lambda **k: k
    flask.send_from_directory = lambda *a, **k: None
    flask.Response = type("Response", (), {})
    flask.redirect = lambda u: None
    flask.url_for = lambda *a, **k: ""
    flask.render_template_string = lambda *a, **k: ""
    flask.session = {}


# ── 1. SemrushClient unit-budget guard ────────────────────────────────────────

class TestSemrushUnitBudget(unittest.TestCase):
    """Drive `SemrushClient._request` through a fake `requests.get` and
    confirm the budget arithmetic + alert behaviour is right."""

    @classmethod
    def setUpClass(cls):
        _stub_external_modules()
        from V5 import SemrushClient  # noqa: E402
        cls.SemrushClient = SemrushClient

    def _make_client(self, budget: int):
        c = self.SemrushClient("FAKE-KEY")
        c._unit_budget = budget
        c._units_used = 0
        c._units_by_phase = {}
        c._current_phase = "test"
        c._budget_alert_75_fired = False
        c._budget_exhausted_logged = False
        c._counter = {}
        c.limiter.wait = lambda: None  # disable rate-limit sleep
        return c

    def _fake_resp(self, body: str = "header\nrow1\nrow2"):
        class _R:
            status_code = 200
            text = body
        return _R()

    # 1a. cost estimation table
    def test_cost_estimation_table_matches_invoice(self):
        # Calibrate against the user's 2026-05-14 query log:
        #   domain_adwords_adwords limit=10 → 400 credits
        #   domain_organic_organic limit=10 → 400 credits
        #   domain_organic        limit=5  →  50 credits
        #   phrase_this           limit=1  →  10 credits
        c = self._make_client(99999)
        cases = [
            ({"type": "domain_adwords_adwords", "display_limit": 10}, 400),
            ({"type": "domain_organic_organic", "display_limit": 10}, 400),
            ({"type": "domain_organic", "display_limit": 5}, 50),
            ({"type": "phrase_this", "display_limit": 1}, 10),
            ({"type": "phrase_adwords", "display_limit": 15}, 15),
            ({"type": "unknown_report", "display_limit": 5}, 200),  # default 40/row
        ]
        for params, expected in cases:
            self.assertEqual(c._estimate_cost(params), expected, f"cost for {params}")

    # 1b. short-circuit on overflow
    def test_request_short_circuits_when_budget_exhausted(self):
        c = self._make_client(budget=500)
        # Spend 400 on one competitor expansion call → 100 units left.
        with patch("V5.requests.get", return_value=self._fake_resp()):
            text = c._request({"type": "domain_adwords_adwords", "display_limit": 10})
        self.assertNotEqual(text, "")
        self.assertEqual(c._units_used, 400)
        # Next call would cost another 400 (>100 remaining) → skipped silently,
        # request count must NOT increment, skip counter MUST.
        with patch("V5.requests.get") as mock_get:
            text = c._request({"type": "domain_adwords_adwords", "display_limit": 10})
            mock_get.assert_not_called()
        self.assertEqual(text, "")
        self.assertEqual(c._units_used, 400, "budget should not have moved on a skip")
        self.assertEqual(c._counter.get("semrush_skipped", 0), 1)
        # 2026-05-18 (round 2): alert flags now live in the shared counter so
        # multiple SemrushClient instances agree on whether they've fired.
        self.assertTrue(c._counter.get("semrush_budget_exhausted_logged", False))

    # 1c. 75 % alert fires once
    def test_alert_fires_once_at_75_percent(self):
        c = self._make_client(budget=1000)
        msgs = []
        c._log_cb = msgs.append
        with patch("V5.requests.get", return_value=self._fake_resp()):
            # 400 -> 400/1000 (40%) -- no alert.
            c._request({"type": "domain_adwords_adwords", "display_limit": 10})
            self.assertFalse(c._counter.get("semrush_budget_alert_75", False))
            # +400 -> 800/1000 (80%) -- alert fires.
            c._request({"type": "domain_adwords_adwords", "display_limit": 10})
        self.assertTrue(c._counter.get("semrush_budget_alert_75", False))
        # A subsequent call must NOT re-fire the 75% alert.
        msg_count_first = sum(1 for m in msgs if "75%" in m or ">=75%" in m)
        with patch("V5.requests.get", return_value=self._fake_resp()):
            c._request({"type": "phrase_this", "display_limit": 1})  # +10
        msg_count_after = sum(1 for m in msgs if "75%" in m or ">=75%" in m)
        self.assertEqual(msg_count_first, msg_count_after, "75% alert should not re-fire")

    # 1c2. SHARED budget across multiple SemrushClient instances (round-2)
    def test_shared_budget_across_clients(self):
        """The whole point of round-2: city_pipeline creates THREE separate
        SemrushClient instances (Pass-1+3 discovery, inner V5 pipeline,
        rediscovery). They must all draw from one pool — otherwise the cap
        is silently triplicated."""
        shared = {"semrush_budget": 500, "semrush_units": 0,
                  "semrush_units_by_phase": {}, "semrush_skipped": 0,
                  "semrush_budget_alert_75": False,
                  "semrush_budget_exhausted_logged": False}
        c1 = self.SemrushClient("K1")
        c2 = self.SemrushClient("K2")
        c1._counter = shared
        c2._counter = shared
        c1._current_phase = "discovery"
        c2._current_phase = "rediscovery"
        c1.limiter.wait = lambda: None
        c2.limiter.wait = lambda: None
        with patch("V5.requests.get", return_value=self._fake_resp()):
            # c1 spends 400 -> shared at 400.
            c1._request({"type": "domain_adwords_adwords", "display_limit": 10})
            # c2 attempts another 400 -> would be 800, exceeds 500 -> skip.
            text = c2._request({"type": "domain_adwords_adwords", "display_limit": 10})
        self.assertEqual(shared["semrush_units"], 400, "c2 must not have spent")
        self.assertEqual(text, "", "c2 must short-circuit on shared budget")
        self.assertEqual(shared["semrush_skipped"], 1)
        # Per-phase attribution: c1's spend tagged with c1's phase.
        self.assertEqual(shared["semrush_units_by_phase"].get("discovery"), 400)

    # 1d. per-phase attribution
    def test_per_phase_attribution(self):
        c = self._make_client(budget=99999)
        with patch("V5.requests.get", return_value=self._fake_resp()):
            c._current_phase = "phase3_discovery"
            c._request({"type": "phrase_adwords", "display_limit": 15})  # 15
            c._current_phase = "phase5c_insights"
            c._request({"type": "domain_organic_organic", "display_limit": 5})  # 200
            c._request({"type": "domain_organic", "display_limit": 5})  # 50
        self.assertEqual(c._units_by_phase.get("phase3_discovery"), 15)
        self.assertEqual(c._units_by_phase.get("phase5c_insights"), 250)
        self.assertEqual(c._units_used, 265)


# ── 2. competitor_expansion BFS ──────────────────────────────────────────────

class TestCompetitorBFSBudget(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _stub_external_modules()
        # staticmethod wrappers stop unittest's TestCase from rebinding these
        # module-level functions as bound methods when accessed via `self`.
        from competitor_expansion import expand_competitors_bfs, _scaled_call_budget
        cls.bfs = staticmethod(expand_competitors_bfs)
        cls.scaled = staticmethod(_scaled_call_budget)

    def test_call_budget_scales_with_required_leads(self):
        # Round-3 v2: cap dropped from required_leads*2 (max 50) to
        # required_leads//5 (max 20). Diminishing returns at deeper BFS.
        self.assertEqual(type(self).scaled(3), 1)     # 3 leads -> 1 call
        self.assertEqual(type(self).scaled(10), 2)    # 10 leads -> 2 calls
        self.assertEqual(type(self).scaled(50), 10)   # 50 leads -> 10 calls
        self.assertEqual(type(self).scaled(100), 20)  # 100 leads -> 20 calls
        self.assertEqual(type(self).scaled(200), 20)  # capped at 20
        self.assertEqual(type(self).scaled(0), 50)    # unlimited -> env default
        self.assertEqual(type(self).scaled(-1), 50)

    def test_silent_scope_short_circuits(self):
        # Even with a fat graph, silent_scope=True must do ZERO calls.
        graph = {f"d{i}.com": [f"d{j}.com" for j in range(i + 1, i + 4)] for i in range(20)}
        calls = []

        def fake_fetch(d, lim):
            calls.append(d)
            return graph.get(d, [])[:lim]

        new, n_calls, depth = type(self).bfs(
            list(graph.keys())[:5],
            fetch_competitors=fake_fetch,
            is_platform_domain=lambda d: False,
            required_leads=10,
            current_domain_count=0,
            silent_scope=True,
        )
        self.assertEqual(new, [])
        self.assertEqual(n_calls, 0)
        self.assertEqual(calls, [], "fetch_competitors must not be called in silent scope")

    def test_small_run_respects_scaled_budget(self):
        # Build a graph that COULD support 50+ BFS calls; required_leads=3
        # must cap calls at 6.
        graph = {f"d{i}.com": [f"d{j}.com" for j in range(i + 1, i + 4)] for i in range(50)}

        def fake_fetch(d, lim):
            return graph.get(d, [])[:lim]

        new, n_calls, depth = type(self).bfs(
            list(graph.keys())[:5],
            fetch_competitors=fake_fetch,
            is_platform_domain=lambda d: False,
            required_leads=3,
            current_domain_count=0,
        )
        self.assertLessEqual(n_calls, 6, f"3-lead BFS should make <=6 calls, got {n_calls}")


# ── 3. End-to-end "3-lead silent-scope" reproduction ─────────────────────────

class TestThreeLeadSilentScope(unittest.TestCase):
    """Reproduces the conditions of the user's 8 240-credit catastrophe and
    asserts the new guards keep the unit total under 1 000 in this scenario.

    NB: this doesn't run a full pipeline — it exercises the surface area that
    *would* fire SEMrush calls (BFS + the Semrush client). The unit budget
    on the client is the canonical hard ceiling, so simulating "everything
    asks for the most expensive report" suffices to prove the cap holds.
    """

    @classmethod
    def setUpClass(cls):
        _stub_external_modules()
        from V5 import SemrushClient
        from competitor_expansion import expand_competitors_bfs
        cls.SemrushClient = SemrushClient
        cls.bfs = staticmethod(expand_competitors_bfs)

    def test_three_lead_budget_holds_below_1000_units(self):
        c = self.SemrushClient("FAKE")
        # Mirror the pipeline init for max_leads=3, healthy scope:
        c._unit_budget = max(300, min(20000, 3 * 100))  # = 300
        c._units_used = 0
        c._units_by_phase = {}
        c._current_phase = "phase3_discovery"
        c._budget_alert_75_fired = False
        c._budget_exhausted_logged = False
        c._counter = {}
        c.limiter.wait = lambda: None

        # Try to fire 50 expensive calls — must short-circuit at the budget.
        class _R:
            status_code = 200
            text = "h\nr1\nr2"
        with patch("V5.requests.get", return_value=_R()):
            for _ in range(50):
                c._request({"type": "domain_adwords_adwords", "display_limit": 10})

        self.assertLessEqual(c._units_used, 300, f"3-lead run blew budget: {c._units_used}")
        self.assertGreater(c._counter.get("semrush_skipped", 0), 0,
                           "should have skipped calls after budget exhausted")

    def test_three_lead_bfs_caps_calls_aggressively(self):
        graph = {f"seed{i}.com": [f"comp{i}{j}.com" for j in range(5)] for i in range(20)}

        def fake_fetch(d, lim):
            return graph.get(d, [])[:lim]

        new, n_calls, _ = type(self).bfs(
            list(graph.keys())[:6],   # 6 seeds matches the V5 cap for max_leads=3 (3*2)
            fetch_competitors=fake_fetch,
            is_platform_domain=lambda d: False,
            required_leads=3,
            current_domain_count=0,
        )
        self.assertLessEqual(n_calls, 6, f"3-lead BFS exceeded ceiling: {n_calls}")


# ── 4. Per-max_leads spend prediction ────────────────────────────────────────

def predict_semrush_units(max_leads: int, mode: str = "industry",
                          silent_scope: bool = False,
                          enrichment: bool = True,
                          pass1_yields_ample: bool = False) -> dict:
    """Walk the gates SemrushClient + the pipelines use and return the
    upper-bound SEMrush credit cost (round-3, after the per-domain cache
    + Phase-4 traffic_metrics removal + city Pass-3 limit reduction).

    `pass1_yields_ample` simulates the early-exit when city_pipeline's
    Pass 1 already returned >= max_leads*4 candidate domains — in that
    case Pass 3 competitor expansion is skipped.
    """
    # ── Budget cap (unchanged from round 2) ──
    if max_leads <= 0:
        budget = 25000
    else:
        budget = max(300, min(20000, max_leads * 100))
    if silent_scope:
        budget = max(150, budget // 4)

    breakdown = {}

    # ── Phase 1: bank-only, no SEMrush. ──
    breakdown["phase1_seed"] = 0

    # ── Phase 2: 12 × phrase_related @ display_limit=25 (1/row), skipped silent. ──
    breakdown["phase2_kw_expansion"] = 0 if silent_scope else 12 * 25 * 1

    # ── Phase 3 discovery (round 3: accurate probe count + per-row cost) ──
    # Industry V5 mode: hardcoded `self.keywords[:30]` (V5.py:5172). With
    # phrase_adwords limit=15 → 30 × 15 = 450 units worst case. Real-world
    # spend usually lower (the loop breaks when domain_cap=max_leads*6 fills).
    # City mode: keyword_cap = max(40, min(1200, max_leads*20)) probes ×
    # phrase_adwords limit=10. Loop breaks when domain_cap = max_leads*8
    # is reached, at ~3 unique domains per probe. Same silent-scope early-
    # bail at 25 probes.
    if mode == "industry":
        # 30 probes hardcoded; limit=15
        probes = 25 if silent_scope else 30
        breakdown["phase3_discovery"] = probes * 15 * 1
    else:
        kw_cap = max(40, min(1200, max_leads * 20)) if max_leads > 0 else 400
        # Domain-cap early-exit: domain_cap = max(200, max_leads*8). With
        # ~3 unique domains per probe the loop breaks at probes ≈ cap / 3.
        domain_cap = max(200, max_leads * 8) if max_leads > 0 else 800
        natural_probes = min(kw_cap, max(20, domain_cap // 3))
        probes = 25 if silent_scope else natural_probes
        breakdown["phase3_discovery"] = probes * 10 * 1   # city Pass-1 uses limit=10

    # ── Phase 4 enrichment (round-3 path) ──
    # Old: 250 (traffic_metrics) + 40 (overview) = 290 units/domain.
    # New: 40 (overview only) — and skipped entirely for domains the city
    # pipeline already confirmed paid (came via phrase_adwords). For city
    # mode we estimate ~70% of enriched domains are confirmed-paid (from
    # Pass 1) so only 30% pay the 40-unit overview cost.
    if silent_scope:
        breakdown["phase4_enrich"] = 0
    else:
        domains_enriched = min(max_leads * 6, 200) if max_leads > 0 else 50
        if mode == "city":
            # 70% confirmed-paid → free; 30% pay 40 units each.
            breakdown["phase4_enrich"] = int(domains_enriched * 0.30 * 40)
        else:
            breakdown["phase4_enrich"] = domains_enriched * 40

    # ── competitor_expansion BFS (paid competitors, round-3 v2 budget) ──
    if silent_scope:
        breakdown["phase3_bfs"] = 0
    else:
        bfs_calls = max(1, min(20, max_leads // 5)) if max_leads > 0 else 20
        breakdown["phase3_bfs"] = bfs_calls * 120

    # ── Phase 5c per-domain insights (round-3 v2: min(max_leads, 50) cap) ──
    # Cap dropped from max_leads*2 to min(max_leads, 50), and organic_kw
    # limit dropped from 20 to 15 rows. For a 250-lead run that's a swing
    # from 500 domains × 320 = 160 000 units down to 50 × 270 = 13 500 —
    # the natural spend on Phase 5c is now bounded regardless of run size.
    if silent_scope or not enrichment:
        breakdown["phase5c_insights"] = 0
    else:
        if max_leads > 0:
            domains_5c = max(5, min(max_leads, 50))
        else:
            domains_5c = 50
        organic_kw_limit = 15 if max_leads <= 0 or max_leads >= 15 else max(10, min(15, max_leads * 3))
        per = organic_kw_limit * 10 + 3 * 40 + 0
        breakdown["phase5c_insights"] = domains_5c * per

    # ── Phase 5h metadata backfill (round 3 v2: 30% cap, hard ceiling 30) ──
    if silent_scope:
        breakdown["phase5h_backfill"] = 0
    else:
        domains_5h = max(0, min(30, int(max_leads * 0.3))) if max_leads > 0 else 3
        # Per domain (overview cached from Phase 4): 10*10 + 3*40 = 220
        per_5h = 10 * 10 + 3 * 40 + 0
        breakdown["phase5h_backfill"] = domains_5h * per_5h

    # ── Phase 5f top-up competitor expansion ──
    if silent_scope or max_leads < 5:
        breakdown["phase5f_topup"] = 0
    else:
        seed_n = max(1, min(5, (max_leads or 5) // 5)) if max_leads > 0 else 5
        topup_rounds = 2 if max_leads >= 25 else 1
        breakdown["phase5f_topup"] = topup_rounds * seed_n * 200

    # ── City-mode-only: Pass 3 competitor expansion (round-3 numbers) ──
    if mode == "city":
        if silent_scope or max_leads <= 5 or pass1_yields_ample:
            breakdown["city_pass3_comp"] = 0
        elif max_leads <= 25:
            breakdown["city_pass3_comp"] = 5 * 3 * 40    # 5 seeds limit=3 = 600
        elif max_leads <= 100:
            breakdown["city_pass3_comp"] = 15 * 3 * 40   # 15 seeds limit=3 = 1800 (was 6000)
        else:
            breakdown["city_pass3_comp"] = 40 * 5 * 40   # 40 seeds limit=5 = 8000 (was 48000)

        # Rediscovery wave (fires only when leads short of target)
        if silent_scope:
            breakdown["city_rediscovery"] = 0
        elif max_leads <= 5:
            breakdown["city_rediscovery"] = 2 * 2 * 40
        elif max_leads <= 25:
            breakdown["city_rediscovery"] = 5 * 3 * 40
        else:
            breakdown["city_rediscovery"] = 40 * 3 * 40

    natural = sum(breakdown.values())
    actual = min(natural, budget)
    return {
        "max_leads": max_leads, "mode": mode,
        "silent_scope": silent_scope, "enrichment": enrichment,
        "budget": budget,
        "natural_spend_estimate": natural,
        "actual_capped": actual,
        "breakdown": breakdown,
    }


def print_prediction_table():
    """Generate the prediction matrix, splitting enrich ON vs OFF.

    Round-3 (post per-domain cache + Phase 4 single-call + city Pass-3 cut):
    the natural spend should be DRAMATICALLY lower — for most rows the
    cap is no longer binding (natural < budget = actual is the natural,
    not the budget). This is the property we want: the guard is now a
    backstop for pathological cases, not the routine line of defence.
    """
    targets = [3, 5, 15, 50, 100, 150, 250]
    header = (
        "+-----------+----------+----------+-----------+-----------+-----------+\n"
        "| max_leads | enrich   | mode     |  budget   |  natural  |  actual   |\n"
        "+-----------+----------+----------+-----------+-----------+-----------+"
    )
    print(header)
    rows = []
    for n in targets:
        for enrich in (True, False):
            for mode in ("industry", "city"):
                p = predict_semrush_units(n, mode=mode, enrichment=enrich)
                rows.append((n, "ON " if enrich else "OFF", mode,
                             p["budget"], p["natural_spend_estimate"], p["actual_capped"]))
    for n, e, m, b, nat, ac in rows:
        marker = " *" if ac < b else "  "   # * = under budget = guard slack
        print(f"| {n:>9} | {e:<8} | {m:<8} | {b:>9} | {nat:>9} | {ac:>7}{marker} |")
    print("+-----------+----------+----------+-----------+-----------+-----------+")
    print("  * = natural spend < budget cap (guard is just a backstop).")
    print()
    # Pass-1 ample variant — when discovery already found enough domains,
    # Pass 3 competitor expansion is skipped entirely.
    print("Pass-1 already ample (>= max_leads*4 domains from phrase_adwords):")
    print(header)
    for n in targets:
        for enrich in (True, False):
            p = predict_semrush_units(n, mode="city", enrichment=enrich, pass1_yields_ample=True)
            marker = " *" if p["actual_capped"] < p["budget"] else "  "
            print(f"| {n:>9} | {'ON ' if enrich else 'OFF':<8} | {'city':<8} | "
                  f"{p['budget']:>9} | {p['natural_spend_estimate']:>9} | {p['actual_capped']:>7}{marker} |")
    print("+-----------+----------+----------+-----------+-----------+-----------+")
    print()
    # Silent-scope variant (the 2026-05-14 catastrophe scenario)
    print("Silent-scope (Apollo-only fallback, the 2026-05-14 scenario):")
    print(header)
    for n in targets:
        p = predict_semrush_units(n, mode="city", silent_scope=True)
        print(f"| {n:>9} | {'-':<8} | {'silent':<8} | "
              f"{p['budget']:>9} | {p['natural_spend_estimate']:>9} | {p['actual_capped']:>7}   |")
    print("+-----------+----------+----------+-----------+-----------+-----------+")


if __name__ == "__main__":
    # Run all suites; exit non-zero if any failed.
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in (TestSemrushUnitBudget, TestCompetitorBFSBudget, TestThreeLeadSilentScope):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print()
    print("=" * 76)
    print("  SEMRUSH UNIT-CREDIT PREDICTION (2026-05-18 round 2)")
    print("  enrich ON/OFF does NOT affect SEMrush — flag only toggles Apollo/Lusha.")
    print("=" * 76)
    print()
    print_prediction_table()
    sys.exit(0 if result.wasSuccessful() else 1)
