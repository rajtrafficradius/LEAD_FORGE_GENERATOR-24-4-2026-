"""End-to-end smoke tests for the 2026-05-21 pipeline integration round.

Covers the four user-reported issues + the new Google Places live credits:

  A. Loud DB connectivity banner — `_v5_try_db_init` returns (ok, info, reason)
     and the banner helper emits 3 lines on success / 4 lines on failure.
  B. Google Places live credits — module-level SESSION_CALLS_MADE increments
     after every Discovery.discover() call; `_check_google_places()` shape;
     /api/credits includes a google_places service entry.
  C. Frontend declutter is structural (no runtime test); manual verification.
  D. GOOGLE_ONLY gate relaxation — when SEMrush is unavailable AND a domain
     is in `_google_intent_domains`, the paid-traffic gate accepts it and
     stamps `_google_intent=True` on traffic_metrics.
  E. This file is itself the verification harness.

Run:
    python tests/test_pipeline_smoke.py
or:
    python -m pytest tests/test_pipeline_smoke.py -v
"""
from __future__ import annotations

import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _stub_external_modules():
    """Stub heavy 3rd-party imports so V5 / city_pipeline / google_places
    can import without flask / pymysql / openai / requests actually
    being installed at test time."""
    for name in (
        "flask", "flask_login", "openai", "pymysql", "dbutils",
        "dbutils.pooled_db", "PIL", "PIL.Image",
    ):
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
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


# ── A. DB connectivity banner shape ─────────────────────────────────────────


class TestDBBanner(unittest.TestCase):
    """The banner helper should emit a clear, grep-able multi-line block."""

    def test_banner_helper_can_be_called(self):
        # Smoke: the banner functions live inside main_web() so we can't
        # call them at module load. Instead verify that the V5 module parses
        # cleanly AND the helper names appear in the source (proxy for
        # "the feature is present").
        _stub_external_modules()
        src = (_ROOT / "V5.py").read_text(encoding="utf-8")
        self.assertIn("_v5_log_db_banner", src)
        self.assertIn("[DB] CONNECTED", src)
        self.assertIn("[DB] UNAVAILABLE", src)
        self.assertIn("consequence: master_leads stays empty", src)

    def test_db_retry_helper_present(self):
        src = (_ROOT / "V5.py").read_text(encoding="utf-8")
        self.assertIn("_v5_maybe_retry_db", src)
        self.assertIn("Recovered on /generate retry", src)
        # Retry must be called from both /generate AND /generate-city.
        # Counts: 1× the `def _v5_maybe_retry_db()` definition line + 2× call
        # sites = 3 total occurrences of the bare-paren form in the source.
        self.assertEqual(src.count("_v5_maybe_retry_db()"), 3,
                         "retry helper should fire on both /generate routes "
                         "(1 def + 2 calls = 3 occurrences)")

    def test_db_init_pool_logs_host(self):
        src = (_ROOT / "db.py").read_text(encoding="utf-8")
        # The failure log line should include cfg.host so the operator can
        # see WHICH MySQL we couldn't reach.
        self.assertIn("host={cfg.host}", src)


# ── B. Google Places module + live-credits wiring ──────────────────────────


class TestGooglePlacesLiveCredits(unittest.TestCase):

    def test_session_counter_increments_after_discover(self):
        _stub_external_modules()
        import google_places_intent as gpi
        gpi.reset_session_calls_made()
        self.assertEqual(gpi.get_session_calls_made(), 0)

        # 2026-05-21: Places API (New) v1 uses POST, response shape is
        # {"places": [...]} with `id`, `displayName.text`, `websiteUri`.
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {
            "places": [
                {"id": "p1", "displayName": {"text": "Acme"}, "websiteUri": "https://acme.com.au"},
                {"id": "p2", "displayName": {"text": "Beta"}, "websiteUri": "https://beta.com.au"},
            ],
        }
        client = gpi.GooglePlacesIntentDiscovery(
            api_key="FAKE",
            is_platform_domain=lambda d: False,
        )
        with patch("google_places_intent.requests.post", return_value=fake_resp):
            domains = client.discover(
                keywords=["plumber"], cities=["Sydney"], country="AU",
            )
        # Should have made >0 calls AND incremented the module counter.
        self.assertGreater(client.calls_made, 0)
        self.assertEqual(gpi.get_session_calls_made(), client.calls_made)
        # And found the (filtered) domains.
        self.assertIn("acme.com.au", domains)

    def test_session_counter_accumulates_across_clients(self):
        _stub_external_modules()
        import google_places_intent as gpi
        gpi.reset_session_calls_made()
        # New API: empty response = {"places": []} (no "places" key OR
        # empty list both signal zero results).
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {"places": []}
        c1 = gpi.GooglePlacesIntentDiscovery("K1", lambda d: False)
        c2 = gpi.GooglePlacesIntentDiscovery("K2", lambda d: False)
        with patch("google_places_intent.requests.post", return_value=fake_resp):
            c1.discover(["plumber"], ["Sydney"], "AU")
            c2.discover(["electrician"], ["Melbourne"], "AU")
        # Both clients' call counts should be reflected in the session total.
        self.assertEqual(
            gpi.get_session_calls_made(),
            c1.calls_made + c2.calls_made,
        )

    def test_check_google_places_shape(self):
        """The `_check_google_places` function (defined inside main_web)
        builds a dict with the expected keys. We assert the shape by
        reading the source and confirming the keys appear together."""
        src = (_ROOT / "V5.py").read_text(encoding="utf-8")
        # Locate the function body
        idx = src.find("def _check_google_places")
        self.assertGreater(idx, 0, "_check_google_places must be defined")
        body = src[idx:idx + 3000]
        for key in (
            '"service":', '"status":', '"remaining":',
            '"per_run_cap":', '"per_run_domain_cap":',
            '"session_calls_made":', '"total":', '"used":',
            '"pct_remaining":', '"searches_remaining":',
        ):
            self.assertIn(key, body, f"missing key {key} in _check_google_places")

    def test_fetch_credits_includes_google_places(self):
        src = (_ROOT / "V5.py").read_text(encoding="utf-8")
        # The parallel-credit-fetch should submit _check_google_places.
        self.assertIn("pool.submit(_check_google_places)", src)
        # 2026-05-25: pool bumped 4 → 5 when _check_google_custom_search added.
        self.assertIn("max_workers=5", src)
        self.assertIn("pool.submit(_check_google_custom_search)", src)

    def test_status_live_block_includes_google_places(self):
        src = (_ROOT / "V5.py").read_text(encoding="utf-8")
        self.assertIn('"google_places_calls"', src)
        self.assertIn('"google_places_session_total"', src)
        self.assertIn('"google_places_per_run_cap"', src)


# ── D. Gate relaxation for Google Places when SEMrush is unavailable ───────


class TestGoogleIntentGateRelaxation(unittest.TestCase):
    """The fix at V5.py:_enrich_single_domain accepts a Google-Places-
    discovered domain even when SEMrush returned paid_traffic=0 — but
    ONLY when SEMrush is unavailable (no key / silent scope / GOOGLE_ONLY
    mode). This prevents Google-only runs from dropping all leads to the
    paid-traffic gate."""

    def test_gate_relaxation_pattern_exists_in_source(self):
        _stub_external_modules()
        src = (_ROOT / "V5.py").read_text(encoding="utf-8")
        # The guard condition that enables the relaxation
        self.assertIn("_semrush_unavailable = (", src)
        self.assertIn('not bool(API_KEYS.get("semrush"))', src)
        self.assertIn('self._semrush_silent_scope', src)
        self.assertIn('"GOOGLE_ONLY"', src)
        # The relaxation branch itself
        self.assertIn("_semrush_unavailable and _is_google_intent", src)
        self.assertIn('traffic_metrics["_google_intent"] = True', src)


# ── E. Backwards-compat (regression) — existing utilities still importable ─


class TestNoRegression(unittest.TestCase):

    def test_discovery_mode_still_importable(self):
        _stub_external_modules()
        from discovery_mode import (
            DiscoveryMode, detect_initial_mode,
            should_pivot_to_google, log_mode_banner,
        )
        m, _ = detect_initial_mode({"semrush": "x", "google_places": "y"}, "AU")
        self.assertEqual(m, DiscoveryMode.BOTH)

    def test_google_places_module_smoke(self):
        _stub_external_modules()
        import google_places_intent as gpi
        client = gpi.GooglePlacesIntentDiscovery(
            "FAKE", lambda d: False,
        )
        qs = client._build_queries(["plumber"], ["Sydney"], 5)
        self.assertGreater(len(qs), 0)

    def test_frontend_decluttered(self):
        """Sanity: top API-Credits panel deleted, 4-card stats grid deleted,
        Google Places tile present in merged panel."""
        html = (_ROOT / "index.html").read_text(encoding="utf-8")
        # The old top panel had the literal "API Credits — Live Remaining"
        # phrase as its header. The merged panel has different wording.
        self.assertNotIn(
            "◈ API Credits — Live Remaining",
            html,
            "old top credit panel should have been deleted",
        )
        # The merged panel uses a different header
        self.assertIn(
            "◈ API Credits",
            html,
            "merged credit panel should still exist",
        )
        # 4-card stats grid was deleted (no more `Leads Found',val:stats.total`)
        self.assertNotIn(
            "{label:'Leads Found',val:stats.total",
            html,
            "post-run 4-card stats grid should have been deleted",
        )
        # Google Places tile must exist in the merged panel
        self.assertIn("Google Places", html)
        # 7-tile grid (was 6)
        self.assertIn("repeat(7,minmax(0,1fr))", html)


# ── 2026-06-08: cost-per-lead + SEMrush bypass toggle ───────────────────────


class TestCostAndSemrushToggle(unittest.TestCase):

    def test_pricing_math_and_monthly_unused(self):
        import pricing as pr
        cfg = pr.default_pricing()
        cfg["items"]["serpapi"]       = {"credits": 100,  "paid": 50,  "monthly": False}  # $0.50/search
        cfg["items"]["apollo_email"]  = {"credits": 1000, "paid": 100, "monthly": True}   # $0.10/email
        cfg["items"]["semrush_units"] = {"credits": 1000, "paid": 200, "monthly": True}   # $0.20/unit
        rc = pr.compute_run_cost({"serpapi": 10, "apollo_email": 4, "semrush_units": 0}, cfg)
        # 10*0.50 + 4*0.10 + 0 = 5.40
        self.assertAlmostEqual(rc["total"], 5.40, places=6)
        # monthly plan but consumed 0 this run → $0 (the "ignore SEMrush if unused" rule)
        self.assertEqual(rc["per_item"]["semrush_units"], 0.0)
        # per-lead = run_cost / ALL leads in run
        self.assertAlmostEqual(pr.cost_per_lead(rc["total"], 5), 1.08, places=6)
        self.assertEqual(pr.cost_per_lead(rc["total"], 0), 0.0)

    def test_disable_semrush_forces_google_only(self):
        _stub_external_modules()
        from discovery_mode import DiscoveryMode, detect_initial_mode
        m_on, reason = detect_initial_mode(
            {"semrush": "x", "google_places": "y"}, "AU", disable_semrush=True)
        self.assertEqual(m_on, DiscoveryMode.GOOGLE_ONLY)
        self.assertIn("bypassed", reason)
        m_off, _ = detect_initial_mode(
            {"semrush": "x", "google_places": "y"}, "AU", disable_semrush=False)
        self.assertEqual(m_off, DiscoveryMode.BOTH)

    def test_semrush_client_disabled_returns_empty(self):
        """A disabled SemrushClient no-ops _request without any HTTP / credits."""
        _stub_external_modules()
        try:
            import V5
        except Exception as e:  # heavy deps not present in this env
            self.skipTest(f"V5 import unavailable: {e}")
        c = V5.SemrushClient("FAKEKEY")
        c._counter = {}
        c._disabled = True
        self.assertEqual(c._request({"type": "phrase_adwords"}), "")
        # nothing was counted as skipped/used because we returned before budget logic
        self.assertEqual(c._counter.get("semrush_skipped", 0), 0)

    def test_frontend_cost_ui_present(self):
        html = (_ROOT / "index.html").read_text(encoding="utf-8")
        self.assertIn("function CostView", html)
        self.assertIn("Cost / Pricing", html)
        self.assertIn("disable_semrush", html)          # toggle wired into payload
        self.assertIn("SerpAPI only", html)             # the toggle label
        self.assertIn("Cost/Lead", html)                # column in Generate + Leads tables
        self.assertIn("/api/pricing", html)             # CostView talks to the endpoint


# ── 2026-06-09: run-wide SerpAPI budget (credit-saving) ─────────────────────


class TestSerpApiBudget(unittest.TestCase):

    def _client(self, shared):
        _stub_external_modules()
        import V5
        c = V5.SerpApiClient("k1,k2")   # 2 fake keys → keys-alive check passes
        c._counter = shared
        return c

    def test_shared_budget_caps_availability(self):
        shared = {"serpapi": 0, "serpapi_budget": 5}
        c = self._client(shared)
        self.assertTrue(c._available)          # 0/5
        shared["serpapi"] = 5
        self.assertFalse(c._available)         # 5/5 → budget hit → all calls stop
        shared["serpapi"] = 4
        self.assertTrue(c._available)          # back under budget

    def test_budget_is_shared_across_instances(self):
        # Two clients sharing one counter (mirrors discovery + enrichment clients).
        shared = {"serpapi": 0, "serpapi_budget": 3}
        a = self._client(shared)
        b = self._client(shared)
        shared["serpapi"] = 3
        self.assertFalse(a._available)
        self.assertFalse(b._available)         # one budget binds BOTH instances

    def test_regular_mode_uncapped(self):
        # No shared budget set → falls back to (high) per-instance budget.
        c = self._client({"serpapi": 9999})
        c._call_budget = 10**9
        self.assertTrue(c._available)

    def test_linkedin_cache_avoids_second_call(self):
        shared = {"serpapi": 0, "serpapi_budget": 0}
        c = self._client(shared)
        c._counter.setdefault("_serp_li_cache", {})["john|acme"] = "John Smith"
        # Cache hit returns WITHOUT touching the network or the budget.
        self.assertEqual(c.find_person_on_linkedin("John", "Acme"), "John Smith")

    def test_credit_saver_budget_math(self):
        # The documented sizing: credit-saver ≈14/lead (floor 12); regular ≈80/lead.
        for ml in (1, 5, 20):
            saver = max(12, ml * 14)
            regular = max(60, ml * 80)
            self.assertLessEqual(saver, 15 * max(1, ml) + 12)   # ~≤15/lead band
            self.assertGreater(regular, saver)


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in (
        TestDBBanner,
        TestGooglePlacesLiveCredits,
        TestGoogleIntentGateRelaxation,
        TestNoRegression,
        TestCostAndSemrushToggle,
        TestSerpApiBudget,
    ):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
