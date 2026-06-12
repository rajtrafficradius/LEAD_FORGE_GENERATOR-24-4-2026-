"""
keyword_bank.py — AdPotentialScore-ranked keyword reservoir for LeadForge V5.

2026-06-12 (Phase: scored consolidation): ALL keyword sources (V5 seeds, 10k
tiered bank, 1k global bank, 42k e-com index) were merged around the Claude-
scored grand table into ONE runtime file:

    KEYWORDS )(\\MASTER_KEYWORD_BANK.json   (built by build_master_keyword_bank.py)

Every lookup now returns keywords ordered by AdPotentialScore DESCENDING.
Only the 19,454 keywords with score > 0 ("active") are served by default;
the ~33.6k score-0 keywords ("reserve") are available ONLY via
get_reserve_keywords() / include_reserve=True — last-resort material.

Tier semantics changed from the legacy hand-built T1/T2/T3 to score bands:
    TIER_1  = AdPotentialScore >= 40   (prime advertisers' keywords)
    TIER_2  = 20–39                    (strong)
    TIER_3  = 1–19                     (long-tail, still ad-positive)
Concatenating T1→T2→T3 therefore yields a strict score-descending walk.

Public API (signatures unchanged from the legacy bank):
  get_extended_keywords(industry, base_keywords, ecom_max=300,
                        include_reserve=False) -> list[str]
  get_industry_tiers(industry) -> {"TIER_1": [...], "TIER_2": [...], "TIER_3": [...]}
  get_ecom_keywords_for_industry(industry, max_n=300) -> list[str]
  get_global_fallback() -> list[str]          # top-1000 scored keywords
  bank_stats() -> dict

New (scored) API:
  get_scored_keywords(min_score=1.0, limit=0, with_scores=False)
      -> the full active bank, score-desc — the 19,454-keyword sweep order.
  get_keyword_score(kw) -> float
  get_reserve_keywords(limit=0) -> list[str]  # score-0, only if necessary
"""
from __future__ import annotations

import os
import re
import json
import threading
from typing import Iterable

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_KEYWORD_DIR = os.path.join(_BASE_DIR, "KEYWORDS )(")
_FILE_MASTER = os.path.join(_KEYWORD_DIR, "MASTER_KEYWORD_BANK.json")

# Score-band boundaries for the tier views (see module docstring).
TIER_1_MIN = 40.0
TIER_2_MIN = 20.0

# Global fallback = top-N active keywords by score (replaces the legacy 1k file).
GLOBAL_FALLBACK_N = 1000

_lock = threading.Lock()
_loaded = False

# Loaded state (all derived from the master JSON):
_ACTIVE: list[list] = []          # [kw, score, cpc, vol, cat] — score DESC
_SCORE_BY_NORM: dict[str, float] = {}
_INDUSTRY_IDX: dict[str, list[int]] = {}   # industry_lower -> sorted active indices
_ECOM_IDX: dict[str, dict[str, list[int]]] = {}  # retailer -> cat_path -> indices
_RESERVE: list[str] = []
_ECOM_CAT_TOKEN_INDEX: list[tuple[set, str, str, list[int]]] = []
_MASTER_META: dict = {}

# Stopwords for industry/category token matching — pruning these prevents
# spurious matches like "and" / "the" lighting up every category.
_STOPWORDS = {
    "and", "or", "of", "the", "a", "an", "for", "in", "on", "at", "with",
    "to", "by", "from", "&", "/", "australia", "au", "services", "service",
    "shop", "all",
}


def _norm_kw(kw: str) -> str:
    return " ".join((kw or "").lower().split())


def _ensure_loaded() -> None:
    global _loaded, _ACTIVE, _SCORE_BY_NORM, _INDUSTRY_IDX, _ECOM_IDX
    global _RESERVE, _ECOM_CAT_TOKEN_INDEX, _MASTER_META
    if _loaded:
        return
    with _lock:
        if _loaded:
            return
        try:
            with open(_FILE_MASTER, "r", encoding="utf-8") as fh:
                master = json.load(fh)
        except Exception:
            master = {}
        _ACTIVE = master.get("active") or []
        _INDUSTRY_IDX = master.get("industries") or {}
        _ECOM_IDX = master.get("ecom") or {}
        _RESERVE = master.get("reserve") or []
        _MASTER_META = {k: master.get(k) for k in ("version", "built_at", "source_csv", "stats")}
        _SCORE_BY_NORM = {_norm_kw(row[0]): float(row[1]) for row in _ACTIVE}
        # Token-index the e-com categories once so industry lookups are a
        # single pass over a flat list of ~1.8k category entries.
        flat: list[tuple[set, str, str, list[int]]] = []
        for retailer, cats in _ECOM_IDX.items():
            for cat_path, idxs in (cats or {}).items():
                tokens = _ind_tokens(cat_path)
                if tokens and idxs:
                    flat.append((tokens, retailer, cat_path, idxs))
        _ECOM_CAT_TOKEN_INDEX = flat
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


def _resolve_industry_key(industry: str) -> str | None:
    """Resolve an arbitrary industry label to a key in the master industry map.
    Strategy: exact (lower) > substring either-direction > token overlap."""
    if not industry:
        return None
    target = industry.strip().lower()
    if target in _INDUSTRY_IDX:
        return target
    for key in _INDUSTRY_IDX.keys():
        if key in target or target in key:
            return key
    target_tokens = {t for t in re.split(r"[^a-z]+", target) if len(t) > 3}
    if target_tokens:
        for key in _INDUSTRY_IDX.keys():
            key_tokens = {t for t in re.split(r"[^a-z]+", key) if len(t) > 3}
            if target_tokens & key_tokens:
                return key
    return None


# ── Scored API ───────────────────────────────────────────────────────────────

def get_scored_keywords(min_score: float = 1.0, limit: int = 0,
                        with_scores: bool = False):
    """The whole active bank in AdPotentialScore-DESC order (the canonical
    19,454-keyword sweep). `min_score` trims the tail; `limit` caps length."""
    _ensure_loaded()
    out = []
    for row in _ACTIVE:
        if float(row[1]) < min_score:
            break  # active is score-desc — nothing below this point qualifies
        out.append((row[0], float(row[1])) if with_scores else row[0])
        if limit and len(out) >= limit:
            break
    return out


def get_keyword_score(kw: str) -> float:
    _ensure_loaded()
    return _SCORE_BY_NORM.get(_norm_kw(kw), 0.0)


def get_reserve_keywords(limit: int = 0) -> list[str]:
    """Score-0 keywords — use ONLY when every scored keyword is exhausted."""
    _ensure_loaded()
    return list(_RESERVE[:limit] if limit else _RESERVE)


# ── Legacy-compatible API (now score-backed) ────────────────────────────────

def get_industry_tiers(industry: str) -> dict[str, list[str]]:
    """Score-banded tiers for an industry, each band score-DESC, so a
    T1→T2→T3 walk is a strict AdPotentialScore-descending iteration."""
    _ensure_loaded()
    tiers: dict[str, list[str]] = {"TIER_1": [], "TIER_2": [], "TIER_3": []}
    key = _resolve_industry_key(industry)
    if not key:
        return tiers
    for i in _INDUSTRY_IDX.get(key, []):  # indices ascend == score descends
        row = _ACTIVE[i]
        s = float(row[1])
        if s >= TIER_1_MIN:
            tiers["TIER_1"].append(row[0])
        elif s >= TIER_2_MIN:
            tiers["TIER_2"].append(row[0])
        else:
            tiers["TIER_3"].append(row[0])
    return tiers


def get_ecom_keywords_for_industry(industry: str, max_n: int = 300) -> list[str]:
    """Up to `max_n` e-commerce keywords whose retailer-category paths share
    tokens with the industry name, ordered by AdPotentialScore DESC across
    all matched categories (ties: more category-token overlap first).

    Industries with no token overlap (Dentist, Doctor, Lawyer, Accountant)
    return [] — we deliberately avoid diluting their pool with hardware terms.
    """
    _ensure_loaded()
    if not _ECOM_CAT_TOKEN_INDEX:
        return []
    ind_tokens = _ind_tokens(industry)
    if not ind_tokens:
        return []
    best_overlap: dict[int, int] = {}  # active idx -> max token overlap
    for cat_tokens, _retailer, _cat_path, idxs in _ECOM_CAT_TOKEN_INDEX:
        overlap = len(ind_tokens & cat_tokens)
        if not overlap:
            continue
        for i in idxs:
            if overlap > best_overlap.get(i, 0):
                best_overlap[i] = overlap
    if not best_overlap:
        return []
    # Active index ascends as score descends, so (idx asc, overlap desc) ==
    # score-desc primary, relevance tiebreak.
    ranked = sorted(best_overlap.items(), key=lambda kv: (kv[0], -kv[1]))
    out: list[str] = []
    seen: set[str] = set()
    for i, _ov in ranked:
        kw = _ACTIVE[i][0]
        n = _norm_kw(kw)
        if not n or n in seen:
            continue
        seen.add(n)
        out.append(kw)
        if len(out) >= max_n:
            break
    return out


def get_global_fallback() -> list[str]:
    """Top-1000 active keywords by AdPotentialScore (replaces the legacy 1k
    non-overlap file as the broad-market safety net)."""
    _ensure_loaded()
    return [row[0] for row in _ACTIVE[:GLOBAL_FALLBACK_N]]


def get_extended_keywords(industry: str, base_keywords: Iterable[str],
                          ecom_max: int = 300,
                          include_reserve: bool = False) -> list[str]:
    """Return base_keywords + scored bank, deduped, AdPotentialScore-first.

    Priority cascade (the V5 pipeline drains in this exact order):
      1. base_keywords        — V5 INDUSTRY_KEYWORDS curated seeds (highest yield)
      2. TIER_1 (score>=40)   — prime confirmed-advertiser keywords
      3. TIER_2 (20–39)       — strong
      4. TIER_3 (1–19)        — ad-positive long tail
      5. E-COM (matched)      — retailer keywords token-matched to the industry,
                                 score-desc, capped at ecom_max
      6. GLOBAL_FALLBACK      — top-1000 scored keywords market-wide
      7. RESERVE (optional)   — score-0 keywords, ONLY when include_reserve=True

    Slots 2-4 concatenated are a strict score-descending walk of the
    industry's keywords. Dedup is case-insensitive on whitespace-collapsed
    form; original casing of `base_keywords` is preserved on collision.
    """
    _ensure_loaded()
    out: list[str] = []
    seen: set[str] = set()

    def _add(kw: str) -> None:
        n = _norm_kw(kw)
        if n and n not in seen:
            seen.add(n)
            out.append(kw)

    for kw in base_keywords or []:
        _add(kw)

    tiers = get_industry_tiers(industry)
    for tier_name in ("TIER_1", "TIER_2", "TIER_3"):
        for kw in tiers.get(tier_name, []):
            _add(kw)

    if ecom_max > 0:
        for kw in get_ecom_keywords_for_industry(industry, max_n=ecom_max):
            _add(kw)

    for kw in get_global_fallback():
        _add(kw)

    if include_reserve:
        for kw in _RESERVE:
            _add(kw)

    return out


def bank_stats() -> dict:
    _ensure_loaded()
    industries = sorted(_INDUSTRY_IDX.keys())
    per_industry: dict[str, dict[str, int]] = {}
    for ind in industries:
        c1 = c2 = c3 = 0
        for i in _INDUSTRY_IDX[ind]:
            s = float(_ACTIVE[i][1])
            if s >= TIER_1_MIN:
                c1 += 1
            elif s >= TIER_2_MIN:
                c2 += 1
            else:
                c3 += 1
        per_industry[ind] = {"TIER_1": c1, "TIER_2": c2, "TIER_3": c3}
    ecom_stats = {
        retailer: {
            "categories": len(cats),
            "keywords": sum(len(v or []) for v in cats.values()),
        }
        for retailer, cats in _ECOM_IDX.items()
    }
    return {
        "master_file": _FILE_MASTER,
        "master_meta": _MASTER_META,
        "industries": industries,
        "per_industry": per_industry,
        "active_keywords": len(_ACTIVE),
        "reserve_keywords": len(_RESERVE),
        "total_tiered_keywords": sum(
            sum(v.values()) for v in per_industry.values()),
        "global_fallback_keywords": min(GLOBAL_FALLBACK_N, len(_ACTIVE)),
        "ecom": ecom_stats,
        "ecom_indexed_categories": len(_ECOM_CAT_TOKEN_INDEX),
    }
