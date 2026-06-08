"""google_custom_search.py — Free organic-results discovery via the
Google Custom Search JSON API (2026-05-25).

PURPOSE
-------
Additive volume layer. Sits next to SerpAPI's geo-aware sweep and Google
Places' AU-business sweep. Where:
  - SerpAPI fetches ads+organic+local for paid queries → 1 SerpAPI credit
    per call (limited budget, ~25 calls/run).
  - Google Places fetches AU businesses with a Places-API record →
    ~$0.032/call (free tier ~10k/month).
  - This module fetches Google's organic search results directly via
    Programmable Search Engine → FREE 100 queries/day on the Custom Search
    JSON API tier, ~10 organic domains per query.

When all three run together a 3-lead run typically goes from ~50 unique
candidate domains to ~100-150, giving Phase 5g + Vertex AI more raw
material to filter from.

AUTH
----
ONE Google API key (`GOOGLE_CUSTOM_SEARCH_API_KEY`) + ONE Programmable
Search Engine ID (`GOOGLE_CUSTOM_SEARCH_CX`). No OAuth, no service
account. Create the search engine at https://programmablesearchengine.google.com
configured to "Search the entire web" so results aren't constrained to
specific sites.

Endpoint used (ONLY this one):
  - GET /customsearch/v1?q=...&cx=...&key=...&gl=au&num=10

Endpoints we deliberately DO NOT use:
  - Google Search API (deprecated)
  - SerpAPI (covered by V5.SerpApiClient already; costs $)
  - Bing Web Search API
  - Brave Search API

Strict caps via env vars:
  GOOGLE_CSE_MAX_QUERIES  default 10 — total HTTPS hits per discover() call
  GOOGLE_CSE_MAX_DOMAINS  default 80 — unique domains kept per call
"""
from __future__ import annotations

import logging
import os
import re
from typing import Callable, Iterable, Optional, Set
from urllib.parse import urlparse

import requests

log = logging.getLogger("leadforge.google_cse")


def _env_int(name: str, default: int) -> int:
    try:
        v = int(os.environ.get(name, "") or "")
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


# Per-run caps. Defaults stay well under the free tier of 100 queries/day so
# multiple back-to-back runs don't exhaust the quota.
MAX_QUERIES = _env_int("GOOGLE_CSE_MAX_QUERIES", 10)
MAX_DOMAINS = _env_int("GOOGLE_CSE_MAX_DOMAINS", 80)

# Module-level session counter mirroring google_places_intent — so the
# /api/credits endpoint can report cumulative CSE usage since process boot
# without needing a separate global registry.
SESSION_CALLS_MADE: int = 0


def get_session_calls_made() -> int:
    return int(SESSION_CALLS_MADE)


def reset_session_calls_made() -> None:
    global SESSION_CALLS_MADE
    SESSION_CALLS_MADE = 0


# Directory/social/aggregator blocklist — same shape as google_places_intent's
# extra blocklist. The host pipeline's is_platform_domain() handles the
# canonical list; this is a tightened AU-focused belt-and-suspenders for
# organic-search noise that frequently outranks small-business sites.
_EXTRA_DIRECTORY_BLOCKLIST: Set[str] = {
    "yellowpages.com.au", "yelp.com.au", "yelp.com",
    "hipages.com.au", "truelocal.com.au", "oneflare.com.au",
    "localsearch.com.au", "serviceseeking.com.au",
    "airtasker.com", "houzz.com.au", "houzz.com",
    "yp.com.au", "startlocal.com.au", "australianbusinessdirectory.com.au",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "pinterest.com",
    "wikipedia.org", "google.com", "trustpilot.com", "productreview.com.au",
    "reddit.com", "quora.com", "medium.com",
}


class GoogleCustomSearchDiscovery:
    """Run a bounded Custom Search JSON API sweep across (kw × city) pairs
    and return discovered organic-result root domains.

    Public surface mirrors GooglePlacesIntentDiscovery so wiring into
    city_pipeline._discover_domains is symmetric:
        __init__(api_key, cx, is_platform_domain, log_fn=None)
        discover(keywords, cities, country) -> set[str]
        .available       bool — True iff api_key AND cx are configured
        .calls_made      int  — HTTPS round-trips this discover() consumed
        .domains_found   Set[str] — root domains returned by last discover()
    """

    BASE_URL = "https://www.googleapis.com/customsearch/v1"

    def __init__(
        self,
        api_key: str,
        cx: str,
        is_platform_domain: Callable[[str], bool],
        log_fn: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.api_key: str = (api_key or "").strip()
        self.cx: str = (cx or "").strip()
        self.is_platform_domain = is_platform_domain
        self._log = log_fn or (lambda m: log.info(m))
        self.calls_made: int = 0
        self.domains_found: Set[str] = set()
        # Both key AND cx are required — CSE rejects requests missing either.
        self.available: bool = bool(self.api_key and self.cx)

    # ── query construction ──────────────────────────────────────────────

    def _build_queries(
        self,
        keywords: Iterable[str],
        cities: Iterable[str],
        max_queries: int,
    ) -> list:
        """Generate up to `max_queries` "<keyword> <city>" strings, ordered
        so the broadest+highest-intent queries run first. Falls back to
        keyword-only queries when cities is empty so industry-mode (no city
        list) still works."""
        kws = [k.strip() for k in keywords if k and isinstance(k, str)]
        cs = [c.strip() for c in cities if c and isinstance(c, str)]
        if not kws:
            return []
        out: list = []
        seen: Set[str] = set()
        # 3 kws × top-4 cities = 12 base queries; trim to max_queries.
        kws_use = kws[:3]
        cs_use = cs[:4] if cs else [""]
        for kw in kws_use:
            for city in cs_use:
                if len(out) >= max_queries:
                    return out
                q = f"{kw} {city}".strip()
                q = re.sub(r"\s+", " ", q)
                if q and q.lower() not in seen:
                    seen.add(q.lower())
                    out.append(q)
        return out

    # ── domain extraction ───────────────────────────────────────────────

    @staticmethod
    def _extract_root_domain(link: str) -> str:
        """Normalize a CSE result link to its root domain."""
        if not link:
            return ""
        s = link.strip()
        if "://" not in s:
            s = "http://" + s
        try:
            parsed = urlparse(s)
            netloc = (parsed.netloc or "").lower()
        except Exception:
            return ""
        if not netloc:
            return ""
        netloc = netloc.split(":")[0]
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc

    def _domain_is_acceptable(self, domain: str) -> bool:
        if not domain:
            return False
        if domain in _EXTRA_DIRECTORY_BLOCKLIST:
            return False
        try:
            if self.is_platform_domain(domain):
                return False
        except Exception:
            return False
        if "." not in domain:
            return False
        return True

    # ── HTTP wrappers ───────────────────────────────────────────────────

    def _search(self, query: str, country_gl: str = "au") -> list:
        """One Custom Search call. CSE max is 10 results per request; we
        always ask for 10. Returns a list of `{"link": "..."}` dicts so the
        downstream loop can stay identical to other providers' shape."""
        if self.calls_made >= MAX_QUERIES:
            return []
        params = {
            "q":   query,
            "cx":  self.cx,
            "key": self.api_key,
            "num": 10,           # API max per call
            "gl":  country_gl,   # Country bias (au, us, uk, …)
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=20)
        except Exception as exc:
            self.calls_made += 1
            self._log(f"[GoogleCSE] HTTP exception for {query!r}: {exc}")
            return []
        self.calls_made += 1
        if resp.status_code != 200:
            try:
                _err = (resp.json().get("error") or {}).get("message", "")[:200]
            except Exception:
                _err = resp.text[:200]
            # 403 = quota exhausted / billing not enabled.
            self._log(f"[GoogleCSE] HTTP {resp.status_code} for {query!r}: {_err}")
            return []
        try:
            data = resp.json() or {}
        except Exception:
            return []
        return data.get("items") or []

    # ── orchestration ───────────────────────────────────────────────────

    def discover(
        self,
        keywords: Iterable[str],
        cities: Iterable[str],
        country: str = "AU",
    ) -> Set[str]:
        """Run the discovery sweep. Returns deduplicated root domains.

        Unlike Google Places, this works for ALL countries — Programmable
        Search's `gl` param biases (not restricts) results, so a UK or US
        run will still get useful organic domains.
        """
        self.calls_made = 0
        self.domains_found = set()
        if not self.available:
            self._log("[GoogleCSE] No GOOGLE_CUSTOM_SEARCH_API_KEY / CX configured — skipping")
            return set()
        queries = self._build_queries(keywords, cities, max_queries=MAX_QUERIES)
        if not queries:
            self._log("[GoogleCSE] no usable (keyword, city) pairs — skipping")
            return set()
        # Country bias for the gl param. Defaults to "au" if no mapping; CSE
        # accepts the same 2-letter codes as SerpAPI.
        gl = (country or "AU").strip().lower() or "au"
        self._log(
            f"[GoogleCSE] {gl.upper()} sweep: {len(queries)} queries, "
            f"cap={MAX_QUERIES} calls / {MAX_DOMAINS} domains"
        )
        for q in queries:
            if len(self.domains_found) >= MAX_DOMAINS:
                break
            if self.calls_made >= MAX_QUERIES:
                break
            for item in self._search(q, country_gl=gl):
                if len(self.domains_found) >= MAX_DOMAINS:
                    break
                link = (item.get("link") or "").strip()
                domain = self._extract_root_domain(link)
                if not domain:
                    continue
                if not self._domain_is_acceptable(domain):
                    continue
                self.domains_found.add(domain)
        global SESSION_CALLS_MADE
        SESSION_CALLS_MADE += int(self.calls_made)
        self._log(
            f"[GoogleCSE] complete: {len(self.domains_found)} organic domains "
            f"({self.calls_made} API calls; session total: {SESSION_CALLS_MADE})"
        )
        return set(self.domains_found)


# ── Offline smoke test ──────────────────────────────────────────────────
if __name__ == "__main__":
    # Shape-only sanity check (no network).
    dummy_is_platform = lambda d: d in {"google.com"}
    cse = GoogleCustomSearchDiscovery("", "", dummy_is_platform)
    qs = cse._build_queries(["plumber", "electrician"], ["Sydney", "Melbourne"], 6)
    assert any("plumber Sydney" in q for q in qs), qs
    assert cse._extract_root_domain("https://www.Acme.com.au/contact") == "acme.com.au"
    assert cse._extract_root_domain("http://yelp.com.au") == "yelp.com.au"
    assert cse._domain_is_acceptable("acme.com.au")
    assert not cse._domain_is_acceptable("yelp.com.au")
    assert not cse._domain_is_acceptable("google.com")
    assert not cse._domain_is_acceptable("nodot")
    # No-key path returns empty without exception.
    out = cse.discover(["plumber"], ["Sydney"], country="AU")
    assert out == set(), out
    print(f"google_custom_search smoke ok: {len(qs)} queries built, filters OK")
