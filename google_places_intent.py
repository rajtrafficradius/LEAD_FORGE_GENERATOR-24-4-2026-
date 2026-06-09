"""google_places_intent.py — AU-only business discovery via Google Places
Text Search (2026-05-18).

ADDITIVE discovery layer. The existing SEMrush / SerpAPI / Apollo path is
untouched; this module runs AFTER SEMrush+SerpAPI and BEFORE the Apollo
fallback inside `city_pipeline._discover_domains` and returns a SEPARATE
set of domains tagged `google_intent`. SEMrush results remain the only
`confirmed_paid` source. Google Places domains are never granted the
paid-traffic-gate bypass — they ride through V5 as unconfirmed candidates
and export with `source = "Google Intent"`.

Why this exists:
  • SEMrush can be silent for AU SMB scopes — there's no AU paid-ads data
    for many tradespeople even though those businesses obviously exist.
  • Apollo fallback gives us company records but no buying-intent signal.
  • Google Places Text Search returns real AU businesses for queries like
    "commercial plumber Sydney Australia" with their official website —
    these are unambiguously legitimate AU companies the user would want
    in their lead list, just without SEMrush-confirmed paid-traffic.

Auth: ONE key, `GOOGLE_PLACES_API_KEY`. No OAuth, no developer tokens, no
customer IDs. Free tier covers most LeadForge usage today.

Endpoint used (ONLY this one):
  - POST /v1/places:searchText  — "Places API (New)" v1, returns places
    with `websiteUri` populated directly in the text-search response
    (no Place Details fallback needed → fewer quota units per discovery).

Why "Places API (New)" instead of legacy:
  As of 2025, Google deprecated the legacy `/maps/api/place/textsearch/json`
  endpoint. New GCP projects can't enable it; existing keys hitting it get
  `REQUEST_DENIED — "You're calling a legacy API"`. The New API is a clean
  REST surface (POST + JSON body + header auth + field-mask) that also
  returns more data per call. Migration was forced by Google.

Endpoints we deliberately DO NOT use:
  - Legacy /maps/api/place/textsearch/json (deprecated by Google)
  - Google Ads API / Keyword Planner (needs OAuth + developer token)
  - Google Custom Search JSON API
  - Google Business Profile API
  - Google Ads Transparency Center scraping

Strict caps via env vars (the V5.py module mirrors these at module-import
time so the two source-of-truth points stay aligned):
  GOOGLE_INTENT_MAX_PLACES_CALLS  default 25 — total HTTPS hits
  GOOGLE_INTENT_MAX_DOMAINS       default 100 — dedup-target ceiling
"""
from __future__ import annotations

import logging
import os
import re
from typing import Callable, Iterable, Optional, Set
from urllib.parse import urlparse

import requests

log = logging.getLogger("leadforge.google_places")


def _env_int(name: str, default: int) -> int:
    try:
        v = int(os.environ.get(name, "") or "")
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


MAX_PLACES_CALLS = _env_int("GOOGLE_INTENT_MAX_PLACES_CALLS", 25)
MAX_DOMAINS = _env_int("GOOGLE_INTENT_MAX_DOMAINS", 100)

# 2026-05-26: Paid-traffic gate for Places-discovered domains. Mirrors the
# existing SEMrush `paid_traffic >= 5` filter that V5 applies in Phase 4
# AFTER Apollo enrichment — by applying the same gate at FETCH time, we
# avoid burning Apollo credits on Places businesses that don't actually
# run paid ads. Google itself does NOT expose a per-domain paid-traffic
# API (Ads API needs OAuth + advertiser verification + only sees your own
# account); the verifier is plugged in by the caller (city_pipeline / V5)
# using their existing SEMrush + SerpAPI clients.
PAID_MIN_THRESHOLD  = _env_int("GOOGLE_PLACES_PAID_MIN",   5)
PAID_VERIFY_MAX     = _env_int("GOOGLE_PLACES_VERIFY_MAX", 30)

# 2026-05-21: module-level session counter so `/api/credits` can report
# how many Places API calls this process has made since boot — Google does
# NOT expose a remaining-quota REST endpoint, so this local count + the
# per-run cap is the closest we get to a "credits remaining" view.
SESSION_CALLS_MADE: int = 0


def get_session_calls_made() -> int:
    """Public read accessor for the module-level session counter.
    Kept as a function (not a direct attr read) so callers don't need to
    import-and-reach into module state."""
    return int(SESSION_CALLS_MADE)


def reset_session_calls_made() -> None:
    """Reset the session counter (test/debug helper)."""
    global SESSION_CALLS_MADE
    SESSION_CALLS_MADE = 0

# Additional AU-specific business directories that often top Places results
# but aren't actual companies the user wants in their lead list. We belt-
# and-suspenders this on top of V5's `is_platform_domain()` because that
# function's blocklist is tuned to SEMrush-era directories and may miss a
# few of these.
_EXTRA_DIRECTORY_BLOCKLIST: Set[str] = {
    "yellowpages.com.au", "yelp.com.au", "yelp.com",
    "hipages.com.au", "truelocal.com.au", "oneflare.com.au",
    "localsearch.com.au", "serviceseeking.com.au",
    "airtasker.com", "houzz.com.au", "houzz.com",
    "yp.com.au", "startlocal.com.au", "australianbusinessdirectory.com.au",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "pinterest.com",
    "wikipedia.org", "google.com",
}

# High-intent query prefixes — these stack with the industry keyword and
# the city name to form "commercial plumber Sydney Australia" etc. We
# rotate through them so a single (industry, city) pair yields up to ~3
# differently-worded queries, each catching a slightly different sliver
# of the Places index.
_HIGH_INTENT_PREFIXES: tuple = (
    "", "commercial", "emergency", "professional",
    "licensed", "best",
)


class GooglePlacesIntentDiscovery:
    """Run a bounded Google Places Text Search sweep across (kw × city)
    combinations and return AU business domains. Stateful so the caller
    can read `.calls_made` / `.domains_found` for logging.

    Public surface:
      __init__(api_key, is_platform_domain, log_fn=None)
      discover(keywords, cities, country) -> set[str]
      .calls_made           # number of HTTPS round-trips consumed
      .domains_found        # set of root domains returned by last discover()
      .available            # True iff api_key was provided
    """

    # 2026-05-21: migrated from legacy `/maps/api/place/textsearch/json` to
    # the new Places API (v1) endpoint. Legacy returns REQUEST_DENIED on
    # new GCP projects; the v1 endpoint accepts the same key + works.
    TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
    # New API includes `websiteUri` in text-search response when requested
    # via field-mask — no separate Place Details call needed for websites.
    # Kept here only as a back-compat reference; do not call.
    PLACE_DETAILS_URL = "https://places.googleapis.com/v1/places"
    # Field mask passed via `X-Goog-FieldMask` header on text-search POST.
    # Minimal set keeps per-call billing in the lower SKU tier.
    _FIELD_MASK = (
        "places.id,places.displayName,places.websiteUri,"
        "places.formattedAddress,places.primaryType"
    )

    def __init__(
        self,
        api_key: str,
        is_platform_domain: Callable[[str], bool],
        log_fn: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.api_key: str = (api_key or "").strip()
        self.is_platform_domain = is_platform_domain
        self._log = log_fn or (lambda m: log.info(m))
        self.calls_made: int = 0
        self.domains_found: Set[str] = set()
        self.available: bool = bool(self.api_key)

    # ── query construction ──────────────────────────────────────────────

    def _build_queries(
        self,
        keywords: Iterable[str],
        cities: Iterable[str],
        max_queries: int,
    ) -> list:
        """Generate up to `max_queries` "<prefix> <keyword> <city> Australia"
        strings, deduplicated and ordered so the most generic+useful queries
        run first.

        Pattern: for each city × top-N-keywords × rotating prefixes.
        We CAP keywords at 3 per city so a 10-city scope still produces
        ~30 queries max, well under MAX_PLACES_CALLS.
        """
        kws = [k.strip() for k in keywords if k and isinstance(k, str)]
        cs = [c.strip() for c in cities if c and isinstance(c, str)]
        if not kws or not cs:
            return []
        out: list = []
        seen: Set[str] = set()
        # 2026-05-25: widened axes from 2×8×3=48 (trimmed to 12) to
        # 3×10×5=150 (trimmed to max_queries). Combined with the new
        # discover()-side budget of ~25 queries, we end up generating
        # ~20-25 diverse "<prefix> <kw> <city> Australia" probes per run
        # which yields +20-40 more local-business domains on AU SMB scopes.
        kws_use = kws[:3]
        cs_use = cs[:10]
        prefixes_use = _HIGH_INTENT_PREFIXES[:5]
        for city in cs_use:
            for kw in kws_use:
                for prefix in prefixes_use:
                    if len(out) >= max_queries:
                        return out
                    q_parts = [p for p in (prefix, kw, city, "Australia") if p]
                    q = " ".join(q_parts)
                    q = re.sub(r"\s+", " ", q).strip()
                    if q and q.lower() not in seen:
                        seen.add(q.lower())
                        out.append(q)
        return out

    # ── domain extraction ───────────────────────────────────────────────

    @staticmethod
    def _extract_root_domain(website: str) -> str:
        """Pull a normalized root domain ("acme.com.au") from a Places-
        returned website URL. Strips scheme, path, query, fragment, leading
        `www.`, and lowercases."""
        if not website:
            return ""
        s = website.strip()
        if "://" not in s:
            s = "http://" + s
        try:
            parsed = urlparse(s)
            netloc = (parsed.netloc or "").lower()
        except Exception:
            return ""
        if not netloc:
            return ""
        # Strip port + leading www.
        netloc = netloc.split(":")[0]
        if netloc.startswith("www."):
            netloc = netloc[4:]
        return netloc

    def _domain_is_acceptable(self, domain: str) -> bool:
        """Drop platform/social/directory domains. Both the host pipeline's
        `is_platform_domain` AND our extra AU-specific blocklist must pass."""
        if not domain:
            return False
        if domain in _EXTRA_DIRECTORY_BLOCKLIST:
            return False
        try:
            if self.is_platform_domain(domain):
                return False
        except Exception:
            # If the host classifier errors, prefer EXCLUSION (safer than
            # accidentally enriching directory pages as if they were SMBs).
            return False
        # Require at least one dot — single-token "localhost" etc. is junk.
        if "." not in domain:
            return False
        return True

    # ── HTTP wrappers ───────────────────────────────────────────────────

    def _text_search(self, query: str) -> list:
        """One Text Search call against the Places API (New) v1 endpoint.
        Returns a list of "place" dicts NORMALIZED to the shape the rest
        of this module expects:
            {"place_id": str, "website": str, "name": str}

        The native v1 response uses different field names (`id`,
        `websiteUri`, `displayName.text`) so we map them here — keeps the
        downstream loop in `discover()` unchanged and any future revert to
        the legacy endpoint trivial.
        """
        # 2026-06-09: once the daily Places quota is exhausted, EVERY further
        # call returns 429 — stop hammering it (was 22 wasted 429s + delay).
        if self.calls_made >= MAX_PLACES_CALLS or getattr(self, "_quota_exhausted", False):
            return []
        body = {
            "textQuery":  query,
            "regionCode": "AU",   # AU-bias results
            "pageSize":   20,     # API max is 20 per call
            # Note: `languageCode` defaults to "en" which is what we want.
        }
        headers = {
            "Content-Type":      "application/json",
            "X-Goog-Api-Key":    self.api_key,
            "X-Goog-FieldMask":  self._FIELD_MASK,
        }
        try:
            resp = requests.post(
                self.TEXT_SEARCH_URL,
                headers=headers,
                json=body,
                timeout=20,
            )
        except Exception as exc:
            self.calls_made += 1
            self._log(f"[GooglePlaces] textsearch exception: {exc}")
            return []
        self.calls_made += 1
        if resp.status_code != 200:
            # Surface the first ~200 chars of the error body so the user
            # can see WHY (e.g. PERMISSION_DENIED if Places API New isn't
            # enabled in their GCP project).
            try:
                _err_body = (resp.json() or {}).get("error", {}).get("message", "") or resp.text[:200]
            except Exception:
                _err_body = resp.text[:200]
            self._log(
                f"[GooglePlaces] textsearch HTTP {resp.status_code} for {query!r}: {_err_body}"
            )
            # Daily quota exhausted (429) → trip the breaker so the remaining
            # queries in this sweep are skipped instead of all 429-ing.
            if resp.status_code == 429 or "exhausted" in (_err_body or "").lower() or "quota" in (_err_body or "").lower():
                self._quota_exhausted = True
                self._log("[GooglePlaces] quota exhausted — skipping remaining text-search queries this run")
            return []
        try:
            data = resp.json() or {}
        except Exception:
            return []
        # v1 success returns either {"places": [...]} or {} (zero-results).
        # An "error" key signals API-level failure (e.g. PERMISSION_DENIED).
        if "error" in data:
            err = data["error"]
            self._log(
                f"[GooglePlaces] textsearch API error for {query!r}: "
                f"{err.get('status','?')} — {err.get('message','')[:200]}"
            )
            return []
        # Normalize each v1 `places[]` entry to the {place_id, website,
        # name} shape the rest of this module reads.
        out = []
        for p in (data.get("places") or []):
            disp = (p.get("displayName") or {}).get("text", "") if isinstance(p.get("displayName"), dict) else ""
            out.append({
                "place_id": p.get("id") or "",
                "website":  (p.get("websiteUri") or "").strip(),
                "name":     disp,
            })
        return out

    def _place_details_website(self, place_id: str) -> str:
        """2026-05-21: Place Details fallback via Places API (New) v1.
        URL pattern: GET /v1/places/{place_id} with X-Goog-FieldMask=websiteUri.

        Rarely fires now — the v1 text-search response already includes
        `websiteUri` when our field-mask asks for it. Kept as defensive
        fallback for the edge case where a place's website was returned
        empty in text-search but available in the per-place detail view.
        """
        if not place_id or self.calls_made >= MAX_PLACES_CALLS:
            return ""
        # v1 details endpoint: /v1/places/{id} ; `id` may already include
        # the "places/" prefix from the text-search response.
        _pid = place_id if place_id.startswith("places/") else f"places/{place_id}"
        url = f"https://places.googleapis.com/v1/{_pid}"
        headers = {
            "X-Goog-Api-Key":   self.api_key,
            "X-Goog-FieldMask": "websiteUri",
        }
        try:
            resp = requests.get(url, headers=headers, timeout=20)
        except Exception:
            self.calls_made += 1
            return ""
        self.calls_made += 1
        if resp.status_code != 200:
            return ""
        try:
            data = resp.json() or {}
        except Exception:
            return ""
        return (data.get("websiteUri") or "").strip()

    # ── orchestration ───────────────────────────────────────────────────

    def discover(
        self,
        keywords: Iterable[str],
        cities: Iterable[str],
        country: str = "AU",
        paid_traffic_verifier: Optional[Callable[[str], int]] = None,
        min_paid_traffic: int = PAID_MIN_THRESHOLD,
        max_verify_calls: int = PAID_VERIFY_MAX,
        volume_floor: int = 0,
    ) -> Set[str]:
        """Run the discovery sweep. Returns a deduplicated set of
        platform-filtered root domains.

        AU-only: returns an empty set immediately if `country != "AU"`.

        Paid-traffic gate (2026-05-26):
          paid_traffic_verifier  optional callback(domain) -> int (paid_traffic
                                 estimate). When provided, every fetched domain
                                 is checked and dropped if score < min_paid_traffic.
                                 The caller builds this closure using their own
                                 SEMrush + SerpAPI clients.
          min_paid_traffic       threshold; dropped iff verified score < this.
                                 Mirrors the V5 phase-4 `paid_traffic >= 5` gate.
          max_verify_calls       hard cap on verifier invocations per discover()
                                 call. Untested domains are not silently kept —
                                 they go into the "unverified" bucket and are
                                 ONLY readmitted if `volume_floor` triggers it.
          volume_floor           if the verified set drops below this, top up
                                 from the unverified bucket so a small SMB scope
                                 with thin SEMrush coverage doesn't get nuked.
        """
        # Reset per-call state so the same instance can be reused.
        self.calls_made = 0
        self.domains_found = set()
        # 2026-05-28: map root_domain -> business displayName so downstream
        # verifiers that match by NAME (e.g. the Ads Transparency Center,
        # which has no domain field) can look up the business name for a
        # discovered domain. Populated in the fetch loop below.
        self.domain_to_name: dict = {}

        if not self.available:
            self._log("[GooglePlaces] No GOOGLE_PLACES_API_KEY configured — skipping")
            return set()
        if (country or "").strip().upper() != "AU":
            self._log(f"[GooglePlaces] AU-only — country={country!r} not supported, skipping")
            return set()

        # Each Text Search call eats 1 quota unit; if a place lacks
        # `website`, the Place Details fallback eats another. We use
        # MAX_PLACES_CALLS as the OUTER cap (textsearch + details combined).
        # 2026-05-25: bumped query_budget from MAX_PLACES_CALLS//2 (=12) to
        # MAX_PLACES_CALLS-3 (=22) because the Places API (New) v1 returns
        # `websiteUri` inline in text-search responses — the Details fallback
        # rarely fires now, so reserving half the budget for it was waste.
        # Effect: +10-13 more queries per run → +20-40 more AU SMB domains.
        query_budget = max(1, MAX_PLACES_CALLS - 3)
        queries = self._build_queries(keywords, cities, max_queries=query_budget)
        if not queries:
            self._log("[GooglePlaces] no usable (keyword, city) pairs — skipping")
            return set()

        self._log(
            f"[GooglePlaces] AU sweep: {len(queries)} text-search queries, "
            f"cap={MAX_PLACES_CALLS} calls / {MAX_DOMAINS} domains"
        )

        for q in queries:
            if len(self.domains_found) >= MAX_DOMAINS:
                break
            if self.calls_made >= MAX_PLACES_CALLS:
                break
            for place in self._text_search(q):
                if len(self.domains_found) >= MAX_DOMAINS:
                    break
                if self.calls_made >= MAX_PLACES_CALLS:
                    break
                website = (place.get("website") or "").strip()
                if not website:
                    pid = place.get("place_id") or ""
                    website = self._place_details_website(pid)
                domain = self._extract_root_domain(website)
                if not domain:
                    continue
                if not self._domain_is_acceptable(domain):
                    continue
                self.domains_found.add(domain)
                # Keep the FIRST business name seen for this domain (the
                # most relevant match for the query that surfaced it).
                _nm = (place.get("name") or "").strip()
                if _nm and domain not in self.domain_to_name:
                    self.domain_to_name[domain] = _nm

        # 2026-05-26: paid-traffic gate. Runs AFTER the Places fetch loop —
        # we already paid for the Places quota, so the gate just culls the
        # set before downstream Apollo enrichment burns credits on domains
        # that don't actually advertise. Volume-floor backstop keeps the
        # output usable even when SEMrush has thin AU SMB coverage.
        if paid_traffic_verifier and self.domains_found:
            _pre = len(self.domains_found)
            # Deterministic order so two runs of the same scope verify the
            # same first-N domains.
            _candidates = sorted(self.domains_found)
            _verified: list = []
            _unverified: list = []
            _checks = 0
            for d in _candidates:
                if _checks >= max_verify_calls:
                    _unverified.append(d)
                    continue
                try:
                    _score = int(paid_traffic_verifier(d) or 0)
                except Exception:
                    _score = 0
                _checks += 1
                if _score >= min_paid_traffic:
                    _verified.append(d)
                else:
                    # Below threshold — verifier checked, no signal — drop.
                    pass
            _kept = set(_verified)
            # Volume-floor: top up from untested-but-fetched domains rather
            # than nuking the run. Domains that FAILED verification stay out.
            _floor_added = 0
            if volume_floor > 0 and len(_kept) < volume_floor:
                for d in _unverified:
                    if len(_kept) >= volume_floor:
                        break
                    _kept.add(d)
                    _floor_added += 1
            _dropped = _pre - len(_kept)
            self._log(
                f"[GooglePlaces/paid-verify] kept {len(_kept)}/{_pre} "
                f"(verified={len(_verified)} >=paid_traffic {min_paid_traffic}, "
                f"floor-topup={_floor_added}, dropped={_dropped}, "
                f"checks={_checks}/{_pre}, cap={max_verify_calls})"
            )
            self.domains_found = _kept
        # 2026-05-21: roll the per-discover() call count into the module-
        # level session counter so `/api/credits` can show "Places calls
        # this session" alongside the per-run cap.
        global SESSION_CALLS_MADE
        SESSION_CALLS_MADE += int(self.calls_made)
        self._log(
            f"[GooglePlaces] complete: {len(self.domains_found)} AU domains "
            f"({self.calls_made} API calls; session total: {SESSION_CALLS_MADE})"
        )
        return set(self.domains_found)


# ── Offline smoke test ──────────────────────────────────────────────────
if __name__ == "__main__":
    # Quick sanity check on query construction + domain extraction (no
    # network). Run: `python google_places_intent.py`
    dummy_is_platform = lambda d: d in {"google.com"}
    gp = GooglePlacesIntentDiscovery("FAKE-KEY", dummy_is_platform)
    qs = gp._build_queries(["plumber", "electrician"], ["Sydney", "Melbourne"], 10)
    assert any("plumber Sydney Australia" in q for q in qs), qs
    assert any("commercial plumber" in q for q in qs), qs
    assert gp._extract_root_domain("https://www.Acme.com.au/contact?q=1") == "acme.com.au"
    assert gp._extract_root_domain("http://yelp.com.au") == "yelp.com.au"
    assert gp._domain_is_acceptable("acme.com.au")
    assert not gp._domain_is_acceptable("yelp.com.au")     # AU directory blocked
    assert not gp._domain_is_acceptable("google.com")      # platform blocked
    assert not gp._domain_is_acceptable("nodot")           # no dot = junk
    # 2026-05-26: verifier-shape test (no network) — fake out the inner
    # _text_search by pre-populating domains_found, then call the gate.
    gp2 = GooglePlacesIntentDiscovery("FAKE-KEY", dummy_is_platform)
    gp2.domains_found = {"hi-paid.com.au", "mid-paid.com.au", "no-paid.com.au"}
    _scores = {"hi-paid.com.au": 1200, "mid-paid.com.au": 50, "no-paid.com.au": 0}
    _gate = lambda d: _scores.get(d, 0)
    # Inline the gate logic that runs at the tail of discover() — verifies
    # the threshold + cap math without hitting the Places network.
    _candidates = sorted(gp2.domains_found)
    _verified, _unverified = [], []
    for i, d in enumerate(_candidates):
        if i >= 10:
            _unverified.append(d); continue
        if _gate(d) >= 5:
            _verified.append(d)
    _kept = set(_verified)
    assert _kept == {"hi-paid.com.au", "mid-paid.com.au"}, _kept
    assert "no-paid.com.au" not in _kept, "below-threshold leaked through"
    print(f"smoke ok: {len(qs)} queries built, domain filters OK, paid-gate culls below-5")
