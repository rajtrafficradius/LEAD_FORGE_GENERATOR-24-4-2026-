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


_VOCAB: Optional[Set[str]] = None

# Compact builtin vocabulary of words common in AU SMB business names —
# the keyword bank extends this at runtime (see _load_vocab).
_BUILTIN_VOCAB = set("""
the and for all pro plus best top new old big one two
electric electrical electrician electricians plumb plumber plumbers plumbing
gas solar air conditioning aircon heating cooling hvac roof roofing roofer
pest control termite clean cleaning cleaner cleaners build builder builders
building construction constructions carpentry carpenter joinery cabinet
landscape landscaping landscaper garden gardening lawn turf concrete
concreting fence fencing paving tiler tiling paint painting painter painters
lock locksmith security alarm cctv data cabling antenna appliance appliances
hot water watts power tech technology connect connection connections wire
wiring wired spark sparky volt amp energy light lighting led emergency
drain drains drainage pipe piping leak blocked bathroom kitchen renovation
renovations handyman maintenance repair repairs fix fixit install
installation services service solutions group company national local metro
city coast bay north south east west point hill hills park valley beach
sydney melbourne brisbane perth adelaide canberra hobart darwin gold
australia aussie oz auto car care home house pros team crew works mister
guys man men lady plug hub spot place zone line edge next first prime elite
smart easy quick fast rapid speedy express direct premier pacific southern
northern eastern western central united allied trade trades master masters
""".split())


def _load_vocab() -> Set[str]:
    """Vocabulary for domain-stem word segmentation. Builtin trade/business
    words + every word in the scored master keyword bank (19k AdPotential
    keywords → a few thousand unique tokens). Loaded lazily once."""
    global _VOCAB
    if _VOCAB is not None:
        return _VOCAB
    words = set(_BUILTIN_VOCAB)
    try:
        import keyword_bank as _kb
        for kw in _kb.get_scored_keywords(min_score=1.0):
            for t in re.split(r"[^a-z]+", (kw or "").lower()):
                if 3 <= len(t) <= 15:
                    words.add(t)
    except Exception:
        pass
    _VOCAB = words
    return words


def _segment_candidates(stem: str) -> List[str]:
    """Split a squashed domain stem into spaced words so the ATC name
    suggester can resolve it: 'wattsnextelectrical' → 'watts next electrical',
    'thelocalplug' → 'the local plug', 'picaelectrical' → 'pica electrical'.

    DP over the vocabulary minimizing (unknown_chars, segments) — unknown
    chars FIRST, so 'watts next electrical' (all known) beats
    'watts nextelectrical' (fewer segments but 14 unknown chars). At most one
    unknown segment (the brand word: 'pica', 'landmark'). Returns up to two
    candidates: the best all-known split and the best one-unknown split."""
    s = _squash(stem)
    n = len(s)
    if n < 6 or n > 32:
        return []
    vocab = _load_vocab()
    # state (i, u) -> cost (unknown_chars, segments) for prefix s[:i]
    best: Dict[Tuple[int, int], Tuple[int, int]] = {(0, 0): (0, 0)}
    back: Dict[Tuple[int, int], Tuple[int, int, str]] = {}
    for i in range(1, n + 1):
        for j in range(max(0, i - 15), i):
            w = s[j:i]
            if len(w) < 2:
                continue
            known = w in vocab
            if not known and len(w) < 3:
                continue
            for u in (0, 1):
                prev = best.get((j, u))
                if prev is None:
                    continue
                nu = u if known else u + 1
                if nu > 1:
                    continue
                cand = (prev[0] + (0 if known else len(w)), prev[1] + 1)
                cur = best.get((i, nu))
                if cur is None or cand < cur:
                    best[(i, nu)] = cand
                    back[(i, nu)] = (j, u, w)
    outs: List[str] = []
    for u in (0, 1):   # fully-known split first — it's the real business name
        cost = best.get((n, u))
        if not cost or cost[1] < 2:
            continue   # unsegmentable, or single word (raw-stem query covers it)
        parts: List[str] = []
        i, uu = n, u
        while i > 0:
            j, pu, w = back[(i, uu)]
            parts.append(w)
            i, uu = j, pu
        seg = " ".join(reversed(parts))
        if seg not in outs:
            outs.append(seg)
    return outs


def _squash(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _extract_ad_count(field4) -> int:
    """Pull the lower-bound ad count out of the suggestions field "4".
    Seen shapes: {"2":{"1":"200","2":"300"}} (region bucket 200-300) or a bare
    {"1":"5"}. Returns 0 on anything unexpected (never raises)."""
    try:
        node = field4
        if isinstance(node, dict) and "2" in node and isinstance(node["2"], dict):
            node = node["2"]
        if isinstance(node, dict):
            for k in ("1", "2"):
                v = node.get(k)
                if v not in (None, ""):
                    return int(str(v).replace(",", "").strip())
    except Exception:
        pass
    return 0


def _stem_matches_advertiser(squash_stem: str, advertiser_name: str) -> bool:
    """Squashed-form comparison between a domain stem and an advertiser name
    (legal suffixes already stripped by _norm_name): exact, or one is a
    prefix of the other with <=4 trailing chars of slack — covers
    'landmarkelectric' ⇔ 'Landmark Electrical' (extra 'al')."""
    a = squash_stem
    b = _squash(_norm_name(advertiser_name))
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= 6 and b.startswith(a) and (len(b) - len(a)) <= 4:
        return True
    if len(b) >= 6 and a.startswith(b) and (len(a) - len(b)) <= 4:
        return True
    return False


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
        # 2026-06-14: small random jitter so back-to-back lookups don't form a
        # perfectly regular burst pattern that ATC's edge flags for 429.
        try:
            import random as _rnd
            time.sleep(_rnd.uniform(0.0, 0.25))
        except Exception:
            pass
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
                # 2026-06-14: field "4" carries a bucketed AD-COUNT range for the
                # advertiser, e.g. {"2":{"1":"200","2":"300"}} ≈ 200–300 ads on
                # record in ATC. A non-zero count is Google's OWN evidence the
                # entity actively advertises — a free strength/recency proxy that
                # rides along on the suggestions call (no extra RPC). We surface
                # the LOWER bound as `ad_count` (0 when absent).
                "ad_count":      _extract_ad_count(inner.get("4")),
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

    def is_advertiser_domain(
        self,
        domain: str,
        names: Iterable[str] = (),
    ) -> Optional[dict]:
        """Domain-first verification (2026-06-12). Works for domains that
        have NO known business name (Apollo-org / SerpAPI-organic pools the
        name-based flow never covered — e.g. landmarkelectric.com.au, whose
        stem 'landmarkelectric' won't token-match 'Landmark Electric').

        Query order (stops at first hit, typically ≤3 RPCs/domain):
          1. each provided business name (Places displayName etc.)
          2. the hyphen-spaced stem (if the domain has hyphens)
          3. the VOCABULARY-SEGMENTED stem — 'wattsnextelectrical' →
             'watts next electrical' (the suggester only resolves spaced
             names; verified live 2026-06-12: all 6 manually-confirmed
             advertisers resolve once spaced, zero resolve squashed)
          4. the raw stem (single-word brands: 'radi')

        Acceptance: country must match AND either the token-subset name
        match, OR the squashed advertiser name equals/extends the squashed
        domain stem (≤4 chars slack) — 'Landmark Electrical' ⇔
        'landmarkelectric'. Cached per domain."""
        d = (domain or "").strip().lower()
        if not d:
            return None
        cache_key = f"__domain__{d}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        stem = d.split(".")[0]
        squash_stem = _squash(stem)
        vocab = _load_vocab()
        # (query, is_real_name) — real business names (Places displayName)
        # may use the tolerant token-subset match; stem-DERIVED queries must
        # pass the strict squash comparison, otherwise a generic stem like
        # 'sydney plumbing services' token-matches an unrelated advertiser
        # ('GREATER SYDNEY PLUMBING SERVICES') — verified false positive.
        queries: List[Tuple[str, bool]] = [
            (n, True) for n in (names or ()) if n and n.strip()
        ]
        if "-" in stem:
            queries.append((stem.replace("-", " ").strip(), False))
        for seg in _segment_candidates(stem):
            queries.append((seg, False))
        if len(squash_stem) >= 4:
            queries.append((squash_stem, False))
        # Single-word brand stems ('radi') aren't dictionary words; if the
        # stem appears verbatim as a token of the advertiser name ('Radi
        # Safi') that's a brand match. Generic words ('sydney') are excluded
        # because they're in the vocabulary.
        _brand_token_ok = (len(squash_stem) >= 4 and squash_stem not in vocab)
        result: Optional[dict] = None
        seen_q: Set[str] = set()
        for q, is_real in queries:
            if not self.available or result is not None:
                break
            qk = _norm_name(q) or q.lower()
            if qk in seen_q:
                continue
            seen_q.add(qk)
            for cand in self._search_suggestions(q):
                if cand["country"] != self.country:
                    continue
                if is_real and _name_matches(q, cand["name"]):
                    result = cand
                    break
                if _stem_matches_advertiser(squash_stem, cand["name"]):
                    result = cand
                    break
                if (_brand_token_ok
                        and squash_stem in set(_norm_name(cand["name"]).split())):
                    result = cand
                    break
        self._cache[cache_key] = result
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
