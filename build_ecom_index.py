"""
build_ecom_index.py — one-shot prep that turns the 4 e-commerce keyword
tree files (Bunnings / Kogan / BIGW / Extra) into a single ECOM_INDEX.json
the runtime keyword_bank.py loads.

Output structure (JSON):
{
  "retailers": ["bunnings", "kogan", "bigw", "extra"],
  "categories": {
    "<retailer>": {
      "<category_path_joined_by_/>": [keyword, keyword, ...],
      ...
    }
  },
  "flat_dedupe_seen_against": {
    "v5_industry_keywords": int,
    "v5_new_city_expansion_keywords": int,
    "bank_10k": int,
    "bank_1k": int,
  },
  "stats": {
    "raw_keyword_count": int,
    "after_internal_dedup": int,
    "after_cross_file_dedup": int,
    "dropped_as_duplicates": int,
    "kept_per_retailer": {...},
  }
}

Run from project root:  python build_ecom_index.py
"""
from __future__ import annotations
import os, re, json, sys
from collections import defaultdict
from pathlib import Path

BASE = Path(__file__).parent
KW_DIR = BASE / "KEYWORDS )("

ECOM_FILES = {
    "bunnings": "Bunnings_semrush_paid_ad_keywords_tree.txt",
    "kogan":    "Kogan_semrush_paid_ad_keywords_tree.txt",
    "bigw":     "BIGW_semrush_paid_ad_keywords_tree.txt",
    "extra":    "Extra_top_paid_ads_keyword_niches.txt",
}

# Tree-line shapes:
#   "|-- Category Name"                   (level 1, no preceding bars)
#   "|   |-- Subcategory"                 (level 2)
#   "|   |   |-- Keywords (50)"           (a keyword-group marker, NOT a category)
#   "|   |   |   |-- some keyword here"   (a real keyword, deeper than its marker)
_TREE_RE = re.compile(r'^(?P<bars>(?:\|\s{3})*)\|--\s*(?P<txt>.+?)\s*$')
_KW_MARKER_RE = re.compile(r'^Keywords\s*\(\s*\d+\s*\)\s*$')


def _norm_kw(s: str) -> str:
    """Case-insensitive, whitespace-collapsed canonical form for dedupe."""
    return " ".join((s or "").lower().split())


def parse_tree(path: Path) -> list[tuple[tuple[str, ...], str]]:
    """Walk a Bunnings/Kogan/BIGW/Extra tree and yield (category_path, keyword).
    Tracks category stack by indent level; keyword lines are those that appear
    immediately under a 'Keywords (N)' marker at a deeper level than the marker.
    """
    out: list[tuple[tuple[str, ...], str]] = []
    cat_stack: list[tuple[int, str]] = []
    in_kw_section = False
    kw_section_level = -1

    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            m = _TREE_RE.match(line)
            if not m:
                continue
            bars = m.group("bars")
            txt = m.group("txt").strip()
            level = bars.count("|") + 1

            if _KW_MARKER_RE.match(txt):
                # The path stays; next deeper-level lines are keywords.
                in_kw_section = True
                kw_section_level = level
                continue

            if in_kw_section and level > kw_section_level:
                # This is a keyword leaf
                path_tuple = tuple(name for _, name in cat_stack)
                out.append((path_tuple, txt))
            else:
                # Category — pop stack down to `level-1`, push self
                in_kw_section = False
                kw_section_level = -1
                while cat_stack and cat_stack[-1][0] >= level:
                    cat_stack.pop()
                cat_stack.append((level, txt))
    return out


def load_v5_keywords(seen: set[str]) -> tuple[int, int]:
    """Load V5.INDUSTRY_KEYWORDS + V5.NEW_CITY_EXPANSION_KEYWORDS. Returns
    (industry_count, new_city_count) added to `seen`."""
    sys.path.insert(0, str(BASE))
    try:
        import V5  # noqa
    finally:
        sys.path.pop(0)
    ind_count = 0
    for kws in V5.INDUSTRY_KEYWORDS.values():
        for kw in kws:
            seen.add(_norm_kw(kw))
            ind_count += 1
    new_count = 0
    nce = getattr(V5, "NEW_CITY_EXPANSION_KEYWORDS", {})
    for kws in nce.values():
        for kw in kws:
            seen.add(_norm_kw(kw))
            new_count += 1
    return ind_count, new_count


def load_bank_file(path: Path, seen: set[str], format: str) -> int:
    """Pre-fill `seen` with the existing bank files.
      format='10k' parses "[NNNNN] Industry | TIER | keyword | comment"
      format='1k'  parses "[NNNN] FAMILY | keyword | Basis: ... | Note"
    Returns count added."""
    if not path.exists():
        return 0
    line_re = re.compile(r"^\[\d+\]\s*(.+?)\s*\|\s*(.+?)\s*\|\s*(.+?)\s*(?:\|.*)?$")
    added = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            m = line_re.match(line)
            if not m:
                continue
            if format == "10k":
                # cols: Industry | TIER | keyword
                kw = m.group(3).strip()
            else:  # '1k': cols: Family | keyword | Basis
                kw = m.group(2).strip()
            if kw:
                n = _norm_kw(kw)
                if n and n not in seen:
                    seen.add(n)
                    added += 1
    return added


def main():
    seen: set[str] = set()
    v5_ind, v5_new = load_v5_keywords(seen)
    bank10k = load_bank_file(KW_DIR / "leadforge_10000_semrush_keyword_bank.txt", seen, "10k")
    bank1k  = load_bank_file(KW_DIR / "semrush_top_1000_nonoverlap_keywords.txt", seen, "1k")
    print(f"  V5.INDUSTRY_KEYWORDS:           {v5_ind} keywords")
    print(f"  V5.NEW_CITY_EXPANSION_KEYWORDS: {v5_new} keywords")
    print(f"  bank 10k (deduped new):          {bank10k} keywords")
    print(f"  bank 1k  (deduped new):          {bank1k} keywords")
    print(f"  total seen (after V5 + 10k + 1k): {len(seen)} normalized forms")

    out_cat: dict[str, dict[str, list[str]]] = defaultdict(dict)
    raw_total = 0
    after_internal = 0
    kept_total = 0
    kept_per_retailer: dict[str, int] = {}

    for retailer, fname in ECOM_FILES.items():
        path = KW_DIR / fname
        pairs = parse_tree(path)
        raw_total += len(pairs)
        # internal dedup within this retailer's parse
        internal_seen_norm: set[str] = set()
        kept_in_retailer = 0
        for cat_path, kw in pairs:
            n = _norm_kw(kw)
            if not n:
                continue
            if n in internal_seen_norm:
                continue
            internal_seen_norm.add(n)
            after_internal += 1
            if n in seen:
                continue  # cross-file duplicate — drop
            seen.add(n)
            # category path joined with " / "
            key = " / ".join(cat_path) if cat_path else "(root)"
            out_cat[retailer].setdefault(key, []).append(kw)
            kept_in_retailer += 1
            kept_total += 1
        kept_per_retailer[retailer] = kept_in_retailer
        print(f"  {retailer:10s}  raw={len(pairs):>6d}  kept_after_dedup={kept_in_retailer:>6d}")

    print(f"  total kept across retailers: {kept_total}")

    # Write JSON index
    out = {
        "_schema": "v1.0",
        "_generated_at": __import__("datetime").datetime.now().isoformat(),
        "retailers": list(ECOM_FILES.keys()),
        "categories": {r: dict(out_cat[r]) for r in ECOM_FILES.keys()},
        "stats": {
            "raw_keyword_count": raw_total,
            "after_internal_dedup": after_internal,
            "kept_after_cross_file_dedup": kept_total,
            "dropped_as_duplicates": after_internal - kept_total,
            "kept_per_retailer": kept_per_retailer,
            "v5_industry_keywords": v5_ind,
            "v5_new_city_expansion_keywords": v5_new,
            "bank_10k_unique": bank10k,
            "bank_1k_unique": bank1k,
            "total_seen_normalized_forms": len(seen),
        },
    }

    out_path = KW_DIR / "ECOM_INDEX.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(out, fh, indent=1, ensure_ascii=False)
    print(f"\n  Wrote: {out_path}  ({out_path.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()