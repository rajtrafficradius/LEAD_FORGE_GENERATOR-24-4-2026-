"""Unit tests for discovery_mode.py (2026-05-21).

Covers:
  • detect_initial_mode — 8-cell matrix: 4 key combos × 2 countries (AU/USA)
  • should_pivot_to_google — matrix of (silent_scope, budget_exhausted,
    google_present, current_mode)
  • log_mode_banner — captures emitted lines and asserts shape
  • log_pivot — captures the mid-run pivot line shape

Run: `python -m pytest tests/test_discovery_mode.py -v`
or:  `python tests/test_discovery_mode.py`
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

# Make `discovery_mode` importable when invoked directly from the tests dir.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from discovery_mode import (
    DiscoveryMode,
    detect_initial_mode,
    should_pivot_to_google,
    log_mode_banner,
    log_pivot,
)


# ── 1. detect_initial_mode matrix ───────────────────────────────────────────


class TestDetectInitialMode(unittest.TestCase):
    """8-cell matrix: {semrush_key?, google_key?, country=AU/USA}."""

    def test_both_keys_au(self):
        mode, reason = detect_initial_mode(
            {"semrush": "x", "google_places": "y"}, "AU")
        self.assertEqual(mode, DiscoveryMode.BOTH)
        self.assertIn("both available", reason.lower())

    def test_both_keys_usa(self):
        # Non-AU → Google is unavailable → SEMRUSH_ONLY
        mode, reason = detect_initial_mode(
            {"semrush": "x", "google_places": "y"}, "USA")
        self.assertEqual(mode, DiscoveryMode.SEMRUSH_ONLY)
        self.assertIn("non-au", reason.lower())

    def test_semrush_only_au(self):
        mode, reason = detect_initial_mode(
            {"semrush": "x", "google_places": ""}, "AU")
        self.assertEqual(mode, DiscoveryMode.SEMRUSH_ONLY)
        self.assertIn("no google_places_api_key", reason.lower())

    def test_semrush_only_usa(self):
        mode, _ = detect_initial_mode(
            {"semrush": "x", "google_places": ""}, "USA")
        self.assertEqual(mode, DiscoveryMode.SEMRUSH_ONLY)

    def test_google_only_au(self):
        mode, reason = detect_initial_mode(
            {"semrush": "", "google_places": "y"}, "AU")
        self.assertEqual(mode, DiscoveryMode.GOOGLE_ONLY)
        self.assertIn("semrush", reason.lower())

    def test_google_only_usa(self):
        # No SEMrush + non-AU → APOLLO_ONLY (Google is AU-only)
        mode, _ = detect_initial_mode(
            {"semrush": "", "google_places": "y"}, "USA")
        self.assertEqual(mode, DiscoveryMode.APOLLO_ONLY)

    def test_apollo_only_au(self):
        mode, _ = detect_initial_mode(
            {"semrush": "", "google_places": ""}, "AU")
        self.assertEqual(mode, DiscoveryMode.APOLLO_ONLY)

    def test_apollo_only_usa(self):
        mode, _ = detect_initial_mode(
            {"semrush": "", "google_places": ""}, "USA")
        self.assertEqual(mode, DiscoveryMode.APOLLO_ONLY)

    def test_defensive_against_none_inputs(self):
        mode, _ = detect_initial_mode(None, None)
        self.assertEqual(mode, DiscoveryMode.APOLLO_ONLY)
        mode, _ = detect_initial_mode({}, "")
        self.assertEqual(mode, DiscoveryMode.APOLLO_ONLY)


# ── 2. should_pivot_to_google matrix ────────────────────────────────────────


class TestShouldPivotToGoogle(unittest.TestCase):
    """Cross-product: (silent_scope T/F) × (budget_exhausted T/F) ×
    (google_present T/F) × (current_mode ∈ all 4)."""

    KEYS_GOOGLE_OK = {"semrush": "x", "google_places": "y"}
    KEYS_NO_GOOGLE = {"semrush": "x", "google_places": ""}

    def test_pivot_on_silent_scope_in_both(self):
        self.assertTrue(should_pivot_to_google(
            {}, True, self.KEYS_GOOGLE_OK, "AU", DiscoveryMode.BOTH))

    def test_pivot_on_budget_exhausted_in_both(self):
        self.assertTrue(should_pivot_to_google(
            {"semrush_budget_exhausted_logged": True}, False,
            self.KEYS_GOOGLE_OK, "AU", DiscoveryMode.BOTH))

    def test_pivot_in_semrush_only(self):
        self.assertTrue(should_pivot_to_google(
            {}, True, self.KEYS_GOOGLE_OK, "AU",
            DiscoveryMode.SEMRUSH_ONLY))

    def test_no_pivot_when_healthy(self):
        self.assertFalse(should_pivot_to_google(
            {}, False, self.KEYS_GOOGLE_OK, "AU", DiscoveryMode.BOTH))

    def test_no_pivot_when_google_unavailable(self):
        # silent_scope=True but no google key → can't pivot
        self.assertFalse(should_pivot_to_google(
            {}, True, self.KEYS_NO_GOOGLE, "AU", DiscoveryMode.BOTH))

    def test_no_pivot_when_country_non_au(self):
        self.assertFalse(should_pivot_to_google(
            {}, True, self.KEYS_GOOGLE_OK, "USA", DiscoveryMode.BOTH))

    def test_no_pivot_from_google_only(self):
        # Already in GOOGLE_ONLY — no pivot needed
        self.assertFalse(should_pivot_to_google(
            {"semrush_budget_exhausted_logged": True}, True,
            self.KEYS_GOOGLE_OK, "AU", DiscoveryMode.GOOGLE_ONLY))

    def test_no_pivot_from_apollo_only(self):
        self.assertFalse(should_pivot_to_google(
            {}, True, self.KEYS_GOOGLE_OK, "AU",
            DiscoveryMode.APOLLO_ONLY))

    def test_defensive_against_none_counter(self):
        # api_counter=None must not raise
        self.assertFalse(should_pivot_to_google(
            None, False, self.KEYS_GOOGLE_OK, "AU", DiscoveryMode.BOTH))


# ── 3. log_mode_banner / log_pivot shape ───────────────────────────────────


class TestLoggingShape(unittest.TestCase):

    def test_banner_emits_three_lines(self):
        captured = []
        log_mode_banner(captured.append, DiscoveryMode.BOTH, "test reason",
                        max_leads=10, country="AU")
        self.assertEqual(len(captured), 3)
        self.assertIn("DISCOVERY MODE: BOTH", captured[0])
        self.assertIn("test reason", captured[1])
        self.assertIn("Country=AU", captured[2])
        self.assertIn("max_leads=10", captured[2])

    def test_banner_skips_context_line_when_empty(self):
        captured = []
        log_mode_banner(captured.append, DiscoveryMode.APOLLO_ONLY, "no keys")
        # No country / max_leads → only banner + reason
        self.assertEqual(len(captured), 2)

    def test_banner_uses_mode_value_string(self):
        captured = []
        log_mode_banner(captured.append, DiscoveryMode.GOOGLE_ONLY, "x")
        self.assertIn("GOOGLE_ONLY", captured[0])

    def test_banner_swallows_log_fn_errors(self):
        # If log_fn raises, banner must NOT propagate (logging is never
        # allowed to crash the pipeline).
        def explosive_log(_):
            raise RuntimeError("nope")
        try:
            log_mode_banner(explosive_log, DiscoveryMode.BOTH, "x",
                            max_leads=1, country="AU")
        except Exception as e:
            self.fail(f"banner propagated log_fn error: {e}")

    def test_banner_no_op_when_log_fn_not_callable(self):
        # Should not raise
        log_mode_banner(None, DiscoveryMode.BOTH, "x")
        log_mode_banner(42, DiscoveryMode.BOTH, "x")

    def test_pivot_log_format(self):
        captured = []
        log_pivot(captured.append, semrush_pool_size=7,
                  silent_scope=True, budget_exhausted=False)
        self.assertEqual(len(captured), 1)
        line = captured[0]
        self.assertIn("PIVOT to GOOGLE_ONLY", line)
        self.assertIn("silent_scope=True", line)
        self.assertIn("keeping 7", line)

    def test_pivot_log_cause_both(self):
        captured = []
        log_pivot(captured.append, semrush_pool_size=0,
                  silent_scope=True, budget_exhausted=True)
        self.assertIn("silent_scope=True", captured[0])
        self.assertIn("budget_exhausted=True", captured[0])

    def test_pivot_log_cause_unspecified(self):
        captured = []
        log_pivot(captured.append, semrush_pool_size=5,
                  silent_scope=False, budget_exhausted=False)
        # Should still emit one line with cause=unspecified
        self.assertIn("unspecified", captured[0])


if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for cls in (TestDetectInitialMode, TestShouldPivotToGoogle, TestLoggingShape):
        suite.addTests(loader.loadTestsFromTestCase(cls))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
