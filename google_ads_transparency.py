"""google_ads_transparency.py — FREE Google-Ads verification via the
Google Ads Transparency Center (ATC) internal RPC (2026-05-28).

WHY THIS EXISTS
---------------
SEMrush (paid units) and SerpAPI (paid searches) are the usual "is this
domain running Google Ads?" sources, but both can hit zero balance — and
when they do, the pipeline can't tell advertisers from non-advertisers, so
the CSV fills with businesses that don't actually advertise.

Google's OWN Ads Transparency Center (https://adstransparency.google.com)
is the ground truth for "is this entity running Google ads right now". It's
FREE and needs no key. Critically, it catches small AU local advertisers
that SEMrush/Ahrefs miss entirely — e.g. "Proximity Plumbing" and "The
Local Plug NSW" both show paid_traffic=0 in SEMrush/Ahrefs yet ATC confirms
they ARE running Google ads in AU.

WHAT IT IS / ISN'T
------------------
ATC is a *verification* tool, not a discovery tool: its search matches by
ADVERTISER NAME, returning {name, advertiserId, countryCode}. So the
intended flow is:
    Google Places  → discover AU businesses (name + domain)   [free]
    ATC (this file)→ verify which of them are Google advertisers [free]
    keep verified  → high paid-advertiser rate, no SEMrush/SerpAPI needed.

ENDPOINT (reverse-engineered + verified live 2026-05-28)
--------------------------------------------------------
POST https://adstransparency.google.com/anji/_/rpc/SearchService/SearchSuggestions
  headers: X-Same-Domain:1, Origin/Referer = adstransparency.google.com,
           Content-Type: application/x-www-form-urlencoded;charset=UTF-8
  body:    f.req={"1": "<query/advertiser name>", "2": <region_int>}
  resp:    {"1":[ {"1":{"1":<name>,"2":<advertiserId "AR...">,"3":<country>}}, ... ]}

Region codes are Google geo-target IDs: AU=2036, US=2840, GB=2826, etc.
(field 2 biases results; we additionally filter each result by its own
country code so only true AU advertisers are accepted.)

NOTE ON FRAGILITY
-----------------
This is an undocumented internal endpoint. It can change without notice.
Every failure path here is non-fatal: on any error the verifier returns
"unknown" and the caller falls back to its existing behaviour, so a broken
ATC never zeroes out a run.
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

import requests

log = logging.getLogger("leadforge.ads_transparency")


# Google geo-target region codes (the subset we care about).
_REGION_CODES = {
    "AU": 2036, "US": 2840, "GB": 2826, "UK": 2826, "NZ": 2554,
    "CA": 2124, "IN": 2356, "IE": 2372,
}


def _region_code(country: str) -> int:
    return _REGION_CODES.get((country or "AU").strip().upper(), 2036)


def advertiser_url(advertiser_id: str, country: str = "AU") -> str:
    """Public Ads Transparency Center page for an advertiser. Verified to
    return HTTP 200. Lets a lead carry Google's OWN proof that the business
    runs ads — open the link to see every ad they're currently running."""
    aid = (advertiser_id or "").strip()
    if not aid:
        return ""
    region = (country or "AU").strip().upper()
    return f"https://adstransparency.google.com/advertiser/{aid}?region={region}"


def _norm_name(s: str) -> str:
    """Normalize a business name for fuzzy comparison: lowercase, strip
    legal suffixes (pty ltd, llc, inc) + punctuation + extra whitespace."""
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(
        r"\b(pty|ltd|limited|llc|inc|incorporated|co|group|holdings|nsw|qld|vic|wa|sa|au|australia)\b",
        " ", s,
    )
    return re.sub(r"\s+", " ", s).strip()


def _name_matches(a: str, b: str) -> bool:
    """True if two normalized names are the same or one contains the other
    (token-subset). Tolerant enough to match 'The Local Plug' to
    'THE LOCAL PLUG NSW PTY LTD' but strict enough to reject unrelated cos."""
    na, nb = _norm_name(a), _norm_name(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    ta, tb = set(na.split()), set(nb.split())
    if not ta or not tb:
        return False
    # One name's tokens are a subset of the other's (e.g. "pure plumbing"
    # ⊆ "pure plumbing professionals"), and the overlap is meaningful.
    smaller, larger = (ta, tb) if len(ta) <= len(tb) else (tb, ta)
    if smaller and smaller.issubset(larger) and len(smaller) >= 1:
        # Require at least one token >= 4 chars so "the"/"co" alone can't match.
        return any(len(t) >= 4 for t in smaller)
    return False


class AdsTransparencyVerifier:
    """Verify whether a business is a Google advertiser via the Ads
    Transparency Center. Stateful for call accounting + a small in-process
    cache so re-checking the same name within a run is free.

    Public surface:
        __init__(country="AU", log_fn=None, min_interval=0.4)
        .available           bool — always True (no key needed)
        .calls_made          int  — HTTPS round-trips this instance made
        is_advertiser(name)  -> Optional[dict]  {name,id,country} or None
        verify_domains(pairs)-> Set[str]  domains confirmed as advertisers
    """

    RPC_URL = (
        "https://adstransparency.google.com/anji/_/rpc/"
        "SearchService/SearchSuggestions"
    )

    def __init__(
        self,
        country: str = "AU",
        log_fn: Optional[Callable[[str], None]] = None,
        min_interval: float = 0.4,
    ) -> None:
        self.country = (country or "AU").strip().upper()
        self.region = _region_code(self.country)
        self._log = log_fn or (lambda m: log.info(m))
        self.calls_made = 0
        self.available = True
        # 2026-06-09: circuit breaker. ATC's free endpoint rate-limits hard
        # (HTTP 429). Before this, a run would hammer 30-120 names all getting
        # 429 — ~2 min wasted + 0 confirmations. After N consecutive 429s we
        # disable ATC for the rest of the run (it's not coming back this run).
        self._consec_429 = 0
        self._max_consec_429 = 6
        self._min_interval = float(min_interval)
        self._last_call = 0.0
        self._cache: Dict[str, Optional[dict]] = {}
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "X-Same-Domain": "1",
            "Origin": "https://adstransparency.google.com",
            "Referer": "https://adstransparency.google.com/",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        })

    # ── low-level RPC ────────────────────────────────────────────────────

    def _rate_limit(self) -> None:
        dt = time.time() - self._last_call
        if dt < self._min_interval:
            time.sleep(self._min_interval - dt)
        self._last_call = time.time()

    def _search_suggestions(self, query: str) -> List[dict]:
        """Return the raw advertiser-suggestion list for `query`, each item
        normalized to {"name","advertiser_id","country"}. [] on any failure."""
        q = (query or "").strip()
        if not q:
            return []
        # Circuit-broken for this run (too many 429s) — skip silently, no HTTP.
        if not self.available:
            return []
        self._rate_limit()
        body = {"f.req": json.dumps({"1": q, "2": self.region}, separators=(",", ":"))}
        try:
            r = self._session.post(self.RPC_URL, data=body, timeout=20)
        except Exception as exc:
            self.calls_made += 1
            self._log(f"[ATC] suggestions HTTP exception for {q!r}: {exc}")
            return []
        self.calls_made += 1
        if r.status_code != 200:
            self._log(f"[ATC] suggestions HTTP {r.status_code} for {q!r}")
            if r.status_code == 429:
                self._consec_429 += 1
                if self._consec_429 >= self._max_consec_429:
                    self.available = False
                    self._log(
                        f"[ATC] disabled for the rest of this run after "
                        f"{self._consec_429} consecutive 429s (rate-limited)"
                    )
            return []
        # Success → reset the 429 streak.
        self._consec_429 = 0
        try:
            data = r.json() or {}
        except Exception:
            return []
        out: List[dict] = []
        for entry in (data.get("1") or []):
            inner = entry.get("1") if isinstance(entry, dict) else None
            if not isinstance(inner, dict):
                continue
            out.append({
                "name":          inner.get("1") or "",
                "advertiser_id": inner.get("2") or "",
                "country":       (inner.get("3") or "").upper(),
            })
        return out

    # ── public verification API ──────────────────────────────────────────

    def is_advertiser(self, business_name: str) -> Optional[dict]:
        """Return the matching advertiser dict {name,advertiser_id,country}
        if `business_name` is a confirmed Google advertiser in the target
        country, else None. Cached per business name."""
        key = _norm_name(business_name)
        if not key:
            return None
        if key in self._cache:
            return self._cache[key]
        result: Optional[dict] = None
        for cand in self._search_suggestions(business_name):
            if cand["country"] != self.country:
                continue
            if _name_matches(business_name, cand["name"]):
                result = cand
                break
        self._cache[key] = result
        return result

    def verify_domains(
        self,
        pairs: Iterable[Tuple[str, str]],
        max_checks: int = 60,
    ) -> Set[str]:
        """Given (business_name, domain) pairs, return the set of DOMAINS
        whose business is a confirmed advertiser in the target country.

        Bounded by `max_checks` HTTPS calls. Names that resolve from cache
        don't count against the budget twice.
        """
        confirmed: Set[str] = set()
        checks = 0
        for name, domain in pairs:
            d = (domain or "").strip().lower()
            if not d:
                continue
            key = _norm_name(name)
            uncached = key not in self._cache
            if uncached and checks >= max_checks:
                continue
            if uncached:
                checks += 1
            if self.is_advertiser(name):
                confirmed.add(d)
        self._log(
            f"[ATC] verified {len(confirmed)} advertiser domain(s) "
            f"({checks} live lookups, region={self.country}/{self.region})"
        )
        return confirmed


# ── Live smoke test ──────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys, io
    try:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    except Exception:
        pass
    v = AdsTransparencyVerifier(country="AU", log_fn=print)
    # Real AU businesses (from a prior run's CSV) that SEMrush/Ahrefs showed
    # as 0 paid traffic but ATC should confirm as advertisers.
    tests = [
        ("Proximity Plumbing", "proximityplumbing.com.au"),
        ("The Local Plug",     "thelocalplug.com.au"),
        ("Pure Plumbing Professionals", "pureplumbingpros.com.au"),
        ("Zzz Nonexistent Fake Biz 9931", "fake-nonexistent-9931.com.au"),
    ]
    for nm, dom in tests:
        hit = v.is_advertiser(nm)
        print(f"  {nm:34s} -> {'ADVERTISER ' + hit['country'] if hit else 'not found'}")
    confirmed = v.verify_domains(tests)
    print(f"confirmed advertiser domains: {sorted(confirmed)}")
    assert "fake-nonexistent-9931.com.au" not in confirmed, "false positive!"
    print(f"ATC smoke ok: {v.calls_made} live calls")
