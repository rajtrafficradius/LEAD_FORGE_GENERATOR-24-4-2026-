"""
keyword_bank.py — Tiered keyword reservoir for LeadForge V5 Phase 2.

Loads three reservoirs lazily on first use:
  * INDUSTRY_TIERED      from 10k file:  {industry_lower: {"TIER_1": [...], "TIER_2": [...], "TIER_3": [...]}}
  * GLOBAL_FALLBACK      from 1k file:   list[str]   (broad-market fallback)
  * ECOM_INDEX           from JSON:      {"categories": {retailer: {cat_path: [...]}}, ...}
                                          built by build_ecom_index.py from
                                          Bunnings/Kogan/BIGW/Extra trees

Public API:
  get_extended_keywords(industry, base_keywords) -> list[str]
      Returns base_keywords → T1 → T2 → T3 → e-com (token-matched) → global,
      deduplicated, case-insensitive, order-preserving.

  get_industry_tiers(industry) -> dict
  get_ecom_keywords_for_industry(industry, max_n=300) -> list[str]
  get_global_fallback() -> list[str]
  bank_stats() -> dict

Industry name resolution is case-insensitive with fallback to substring match in
either direction (e.g. V5's "Lawyer / Attorney" finds bank's "Family Lawyer").
E-com category matching is token-overlap based (industry words vs category path
words), so "Solar Panel Installation" finds Bunnings's "Solar" / "Garden Solar"
sub-categories without needing an explicit industry mapping.
"""
from __future__ import annotations

import os
import re
import json
import threading
from typing import Iterable

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_KEYWORD_DIR = os.path.join(_BASE_DIR, "KEYWORDS )(")
_FILE_10K = os.path.join(_KEYWORD_DIR, "leadforge_10000_semrush_keyword_bank.txt")
_FILE_1K = os.path.join(_KEYWORD_DIR, "semrush_top_1000_nonoverlap_keywords.txt")
_FILE_ECOM = os.path.join(_KEYWORD_DIR, "ECOM_INDEX.json")

_LINE_RE = re.compile(r"^\[\d+\]\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*(?:\|.*)?$")

_lock = threading.Lock()
_loaded = False
_ecom_loaded = False
INDUSTRY_TIERED: dict[str, dict[str, list[str]]] = {}
GLOBAL_FALLBACK: list[str] = []
ECOM_INDEX: dict = {}                                              # raw json
_ECOM_CAT_TOKEN_INDEX: list[tuple[set, str, str, list[str]]] = []  # (cat_tokens, retailer, cat_path, kws)

# Stopwords for industry/category token matching — pruning these prevents
# spurious matches like "and" / "the" lighting up every category.
_STOPWORDS = {
    "and", "or", "of", "the", "a", "an", "for", "in", "on", "at", "with",
    "to", "by", "from", "&", "/", "australia", "au", "services", "service",
    "shop", "all",
}


def _norm_kw(kw: str) -> str:
    return " ".join((kw or "").lower().split())


def _parse_10k(path: str) -> dict[str, dict[str, list[str]]]:
    out: dict[str, dict[str, list[str]]] = {}
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("##"):
                continue
            m = _LINE_RE.match(line)
            if not m:
                continue
            industry, tier, keyword = m.group(1).strip(), m.group(2).strip().upper(), m.group(3).strip()
            if tier not in ("TIER_1", "TIER_2", "TIER_3"):
                continue
            if not keyword:
                continue
            ind_key = industry.lower()
            bucket = out.setdefault(ind_key, {"TIER_1": [], "TIER_2": [], "TIER_3": []})
            bucket[tier].append(keyword)
    return out


def _parse_1k(path: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8", errors="ignore") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = _LINE_RE.match(line)
            if not m:
                continue
            keyword = m.group(2).strip()
            if not keyword:
                continue
            n = _norm_kw(keyword)
            if n in seen:
                continue
            seen.add(n)
            out.append(keyword)
    return out


def _ensure_loaded() -> None:
    global _loaded, INDUSTRY_TIERED, GLOBAL_FALLBACK
    if _loaded:
        return
    with _lock:
        if _loaded:
            return
        INDUSTRY_TIERED = _parse_10k(_FILE_10K)
        GLOBAL_FALLBACK = _parse_1k(_FILE_1K)
        _loaded = True


# Lightweight English-suffix stemmer — only the rules that matter for matching
# trade-job names ("Plumber", "Electrician", "Painter", "Roofer") to e-com
# category labels ("Plumbing", "Electrical", "Painting & Decorating", "Roofing").
# Order: longer / more specific suffixes first so 'ician' beats 'er' on
# "electrician". Each rule keeps a stem ≥ 4 chars.
_SUFFIX_RULES = [
    ("ician", "ic"),    # electrician → electric, technician → technic
    ("ation", "ate"),   # installation → installate-ish, close enough
    ("tion",  "t"),     # construction → construct
    ("ings",  ""),      # plurals + ing
    ("ing",   ""),      # plumbing → plumb, painting → paint
    ("ers",   ""),      # plumbers → plumb
    ("ors",   ""),      # contractors → contract
    ("als",   ""),      # electricals → electric
    ("ed",    ""),
    ("al",    ""),      # electrical → electric
    ("er",    ""),      # plumber → plumb, painter → paint
    ("or",    ""),      # contractor → contract
]


def _stem(raw: str) -> str:
    """Return the shortest plausible stem for `raw` (>= 4 chars). Used to
    join singular/plural and noun/agent-noun forms so "Plumber" matches
    "Plumbing", "Electrician" matches "Electrical", etc."""
    best = raw
    for suf, repl in _SUFFIX_RULES:
        if raw.endswith(suf):
            s = raw[:-len(suf)] + repl
            if len(s) >= 4 and len(s) < len(best):
                best = s
    return best


def _ind_tokens(s: str) -> set[str]:
    """Tokenize an industry / category label for overlap scoring. Adds the
    stem alongside the raw token so morphological variants match."""
    out: set[str] = set()
    for t in re.split(r"[^a-z0-9]+", (s or "").lower()):
        if not t or len(t) < 2 or t in _STOPWORDS:
            continue
        out.add(t)
        stem = _stem(t)
        if stem != t and len(stem) >= 4 and stem not in _STOPWORDS:
            out.add(stem)
    return out


def _ensure_ecom_loaded() -> None:
    """Load the JSON index built by build_ecom_index.py. Indexes each
    (retailer, category_path) by its tokenized form so industry lookups are
    a single pass over a flat list of ~1k–3k category entries."""
    global _ecom_loaded, ECOM_INDEX, _ECOM_CAT_TOKEN_INDEX
    if _ecom_loaded:
        return
    with _lock:
        if _ecom_loaded:
            return
        try:
            with open(_FILE_ECOM, "r", encoding="utf-8") as fh:
                ECOM_INDEX = json.load(fh)
        except Exception:
            ECOM_INDEX = {}
            _ECOM_CAT_TOKEN_INDEX = []
            _ecom_loaded = True
            return
        flat: list[tuple[set, str, str, list[str]]] = []
        for retailer, cats in (ECOM_INDEX.get("categories") or {}).items():
            for cat_path, kws in (cats or {}).items():
                tokens = _ind_tokens(cat_path)
                if tokens and kws:
                    flat.append((tokens, retailer, cat_path, list(kws)))
        _ECOM_CAT_TOKEN_INDEX = flat
        _ecom_loaded = True


def get_ecom_keywords_for_industry(industry: str, max_n: int = 300) -> list[str]:
    """Return up to `max_n` e-commerce keywords whose retailer-category paths
    share tokens with the given industry name. Ranking: more shared tokens =
    higher rank; ties broken by category specificity (deeper / longer paths
    are slightly preferred — those are usually more on-target).

    Industries with no token overlap (Dentist, Doctor, Lawyer, Accountant)
    return [] — we deliberately avoid diluting their pool with hardware terms.

    Industries with strong overlap (Plumber, Electrician, Solar Panel
    Installation, the Massage Chairs / Safety / Bicycle / Designer Clothing
    multi-vertical) get the most-relevant e-com keywords first.
    """
    _ensure_ecom_loaded()
    if not _ECOM_CAT_TOKEN_INDEX:
        return []
    ind_tokens = _ind_tokens(industry)
    if not ind_tokens:
        return []
    scored: list[tuple[int, int, str, str, list[str]]] = []
    for cat_tokens, retailer, cat_path, kws in _ECOM_CAT_TOKEN_INDEX:
        overlap = ind_tokens & cat_tokens
        if not overlap:
            continue
        # Primary score: number of overlapping tokens (10× weight).
        # Tiebreaker: specificity (longer category path = more specific).
        score = len(overlap) * 10 + max(0, len(cat_tokens) - len(overlap))
        scored.append((score, len(cat_tokens), retailer, cat_path, kws))
    if not scored:
        return []
    # Sort: high score first; for ties prefer DEEPER (more specific) categories.
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    out: list[str] = []
    seen: set[str] = set()
    for _, _, _, _, kws in scored:
        for kw in kws:
            n = _norm_kw(kw)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(kw)
            if len(out) >= max_n:
                return out
    return out


def _resolve_industry_key(industry: str) -> str | None:
    """Resolve an arbitrary industry label to a key in INDUSTRY_TIERED.
    Strategy: exact (lower) > substring either-direction > None."""
    if not industry:
        return None
    target = industry.strip().lower()
    if target in INDUSTRY_TIERED:
        return target
    # Try substring match in either direction
    for key in INDUSTRY_TIERED.keys():
        if key in target or target in key:
            return key
    # Token overlap: if any non-trivial word matches
    target_tokens = {t for t in re.split(r"[^a-z]+", target) if len(t) > 3}
    if target_tokens:
        for key in INDUSTRY_TIERED.keys():
            key_tokens = {t for t in re.split(r"[^a-z]+", key) if len(t) > 3}
            if target_tokens & key_tokens:
                return key
    return None


def get_industry_tiers(industry: str) -> dict[str, list[str]]:
    _ensure_loaded()
    key = _resolve_industry_key(industry)
    if not key:
        return {"TIER_1": [], "TIER_2": [], "TIER_3": []}
    bucket = INDUSTRY_TIERED.get(key, {})
    return {
        "TIER_1": list(bucket.get("TIER_1", [])),
        "TIER_2": list(bucket.get("TIER_2", [])),
        "TIER_3": list(bucket.get("TIER_3", [])),
    }


def get_global_fallback() -> list[str]:
    _ensure_loaded()
    return list(GLOBAL_FALLBACK)


def get_extended_keywords(industry: str, base_keywords: Iterable[str],
                          ecom_max: int = 300) -> list[str]:
    """Return base_keywords + tiered bank + e-com + global fallback, deduped.

    Priority cascade (the V5 pipeline drains in this exact order):
      1. base_keywords        — V5 INDUSTRY_KEYWORDS curated seeds (highest yield)
      2. TIER_1               — high commercial intent + local BOFU advertisers
      3. TIER_2               — commercial expansion + secondary local
      4. TIER_3               — long-tail / semantic / urgency fallback
      5. E-COM (matched)      — Bunnings/Kogan/BIGW/Extra keywords whose category
                                 paths share tokens with the industry name
                                 (capped at ecom_max so e-com can't drown out
                                 the curated tiers; cap also keeps memory and
                                 SEMrush probes bounded for trade industries)
      6. GLOBAL_FALLBACK      — broad 1k market bank (final safety net)

    Industries without any e-com token overlap (Dentist, Doctor, Lawyer,
    Accountant) skip slot 5 entirely — no dilution with hardware terms.

    Dedup is case-insensitive on whitespace-collapsed form. Original casing
    of `base_keywords` is preserved when collisions occur.
    """
    _ensure_loaded()
    out: list[str] = []
    seen: set[str] = set()

    for kw in base_keywords or []:
        n = _norm_kw(kw)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(kw)

    tiers = get_industry_tiers(industry)
    for tier_name in ("TIER_1", "TIER_2", "TIER_3"):
        for kw in tiers.get(tier_name, []):
            n = _norm_kw(kw)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(kw)

    # E-com retailer keywords (Bunnings/Kogan/BIGW/Extra), token-matched
    if ecom_max > 0:
        for kw in get_ecom_keywords_for_industry(industry, max_n=ecom_max):
            n = _norm_kw(kw)
            if not n or n in seen:
                continue
            seen.add(n)
            out.append(kw)

    for kw in GLOBAL_FALLBACK:
        n = _norm_kw(kw)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(kw)

    return out


def bank_stats() -> dict:
    _ensure_loaded()
    _ensure_ecom_loaded()
    industries = sorted(INDUSTRY_TIERED.keys())
    per_industry: dict[str, dict[str, int]] = {}
    total_tiered = 0
    for ind in industries:
        b = INDUSTRY_TIERED[ind]
        counts = {t: len(b.get(t, [])) for t in ("TIER_1", "TIER_2", "TIER_3")}
        per_industry[ind] = counts
        total_tiered += sum(counts.values())
    ecom_stats: dict = {}
    ecom_total = 0
    if ECOM_INDEX and "categories" in ECOM_INDEX:
        for retailer, cats in (ECOM_INDEX.get("categories") or {}).items():
            c = sum(len(v or []) for v in cats.values())
            ecom_stats[retailer] = {
                "categories": len(cats),
                "keywords": c,
            }
            ecom_total += c
    return {
        "industries": industries,
        "per_industry": per_industry,
        "total_tiered_keywords": total_tiered,
        "global_fallback_keywords": len(GLOBAL_FALLBACK),
        "ecom": ecom_stats,
        "ecom_total_keywords": ecom_total,
        "ecom_indexed_categories": len(_ECOM_CAT_TOKEN_INDEX),
    }
