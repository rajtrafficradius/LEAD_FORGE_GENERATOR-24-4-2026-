# build_master_keyword_bank.py
# One-shot consolidator: merge EVERY keyword source in the project around the
# Claude-scored grand table (AdPotentialScore) into ONE runtime file:
#
#     KEYWORDS )(\MASTER_KEYWORD_BANK.json
#
# After this runs, keyword_bank.py reads ONLY the master file. The legacy
# runtime files (10k tiered bank, 1k global bank, ECOM_INDEX.json) are
# superseded and can be moved to KEYWORDS )(\_archive_legacy\.
#
# Master layout:
#   active   — the 19,455 keywords with AdPotentialScore > 0, sorted score
#              DESC (ties: original PriorityRank). These are the ONLY keywords
#              the pipeline normally iterates.
#   reserve  — the ~33.6k score-0 keywords, kept ONLY as a last-resort tail
#              (deep rediscovery after every scored keyword is exhausted).
#   industries — industry_lower -> [active indices] (score-desc within industry)
#                joined from the legacy 10k tiered bank + V5 INDUSTRY_KEYWORDS
#                + NEW_CITY_EXPANSION_KEYWORDS so industry-scoped runs still
#                resolve the same names.
#   ecom     — retailer -> category_path -> [active indices] (score-desc),
#              joined from ECOM_INDEX.json so token-matched e-com lookup
#              keeps working, now score-aware.
#
# Usage:  python build_master_keyword_bank.py [path_to_scored_csv]

import ast
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
KEYWORDS_DIR = os.path.join(PROJECT_DIR, "KEYWORDS )(")
ARCHIVE_DIR = os.path.join(KEYWORDS_DIR, "_archive_legacy")

DEFAULT_SCORED_CSV = r"C:\Users\User\Documents\Keyword_bank_scored_FULL claude.csv"
CANONICAL_CSV = os.path.join(KEYWORDS_DIR, "Keyword_bank_scored_FULL.csv")
OUTPUT_PATH = os.path.join(KEYWORDS_DIR, "MASTER_KEYWORD_BANK.json")

V5_PATH = os.path.join(PROJECT_DIR, "V5.py")


def _legacy(name: str) -> str:
    """Legacy source file — prefer the archive copy, fall back to live dir."""
    arch = os.path.join(ARCHIVE_DIR, name)
    return arch if os.path.exists(arch) else os.path.join(KEYWORDS_DIR, name)


def norm(kw: str) -> str:
    return " ".join((kw or "").lower().split())


def _extract_py_dict(source_text, var_name):
    try:
        tree = ast.parse(source_text)
    except SyntaxError:
        return {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == var_name:
                    try:
                        return ast.literal_eval(node.value)
                    except Exception:
                        return {}
    return {}


def main():
    scored_csv = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SCORED_CSV
    if not os.path.exists(scored_csv):
        sys.exit(f"Scored CSV not found: {scored_csv}")

    # ── 1. Load the scored grand table (authoritative) ──────────────────────
    print(f"Reading scored CSV: {scored_csv}")
    rows = []
    with open(scored_csv, "r", encoding="utf-8-sig", newline="") as fh:
        for r in csv.DictReader(fh):
            kw = (r.get("Keyword") or "").strip()
            n = norm(kw)
            if not n or n == "keyword":  # stray header artifact
                continue
            try:
                score = float(r.get("AdPotentialScore") or 0)
            except ValueError:
                score = 0.0
            try:
                rank = int(r.get("PriorityRank") or 10**9)
            except ValueError:
                rank = 10**9
            try:
                cpc = float(r.get("CPC") or 0)
            except ValueError:
                cpc = 0.0
            try:
                vol = int(float(r.get("Volume") or 0))
            except ValueError:
                vol = 0
            rows.append({
                "kw": kw, "n": n, "score": score, "rank": rank,
                "cpc": cpc, "vol": vol,
                "cat": (r.get("Category") or "").strip(),
                "tier_label": (r.get("Tier") or "").strip(),
            })
    # Dedup on normalized form (CSV is already clean, this is belt-and-braces)
    seen, uniq = set(), []
    for r in rows:
        if r["n"] in seen:
            continue
        seen.add(r["n"])
        uniq.append(r)
    rows = uniq
    print(f"  {len(rows):,} unique keywords loaded")

    # ── 2. Split active (score>0, score-desc) vs reserve (score<=0) ─────────
    active = sorted(
        (r for r in rows if r["score"] > 0),
        key=lambda r: (-r["score"], r["rank"]),
    )
    reserve = [r for r in rows if r["score"] <= 0]
    reserve.sort(key=lambda r: r["rank"])
    idx_of = {r["n"]: i for i, r in enumerate(active)}
    print(f"  active (AdPotentialScore>0): {len(active):,}")
    print(f"  reserve (score 0):           {len(reserve):,}")

    # ── 3. Join industry metadata: V5 dicts + legacy 10k tiered bank ────────
    industries: dict[str, list[int]] = {}

    def tag(industry: str, n: str):
        i = idx_of.get(n)
        if i is None:
            return
        bucket = industries.setdefault(industry.strip().lower(), [])
        if not bucket or bucket[-1] != i:  # cheap pre-dedup; full dedup below
            bucket.append(i)

    print("Joining V5 industry dicts …")
    with open(V5_PATH, "r", encoding="utf-8") as fh:
        v5_src = fh.read()
    for var in ("INDUSTRY_KEYWORDS", "NEW_CITY_EXPANSION_KEYWORDS"):
        d = _extract_py_dict(v5_src, var)
        for ind, kws in d.items():
            for kw in kws:
                tag(ind, norm(kw))

    print("Joining legacy 10k tiered bank …")
    bank10k_re = re.compile(r"^\[\d+\]\s*([^|]+?)\s*\|\s*(\w+)\s*\|\s*([^|]+?)\s*(?:\|.*)?$")
    path_10k = _legacy("leadforge_10000_semrush_keyword_bank.txt")
    if os.path.exists(path_10k):
        with open(path_10k, "r", encoding="utf-8", errors="ignore") as fh:
            for line in fh:
                m = bank10k_re.match(line.strip())
                if m and m.group(2).upper().startswith("TIER"):
                    tag(m.group(1), norm(m.group(3)))
    else:
        print(f"  WARNING: 10k bank not found ({path_10k}) — industry map from V5 dicts only")

    # Dedup + re-sort every industry bucket into global score-desc order
    for ind, lst in industries.items():
        industries[ind] = sorted(set(lst))
    print(f"  {len(industries)} industries mapped")

    # ── 4. Join e-com retailer/category metadata (active-only) ──────────────
    print("Joining ECOM_INDEX categories …")
    ecom_out: dict[str, dict[str, list[int]]] = {}
    ecom_path = _legacy("ECOM_INDEX.json")
    ecom_active_n = 0
    if os.path.exists(ecom_path):
        with open(ecom_path, "r", encoding="utf-8") as fh:
            ecom = json.load(fh)
        for retailer, cats in (ecom.get("categories") or {}).items():
            for cat_path, kws in (cats or {}).items():
                idxs = sorted({idx_of[n] for n in (norm(k) for k in kws) if n in idx_of})
                if idxs:
                    ecom_out.setdefault(retailer, {})[cat_path] = idxs
                    ecom_active_n += len(idxs)
    else:
        print(f"  WARNING: ECOM_INDEX.json not found ({ecom_path}) — no e-com slot")
    print(f"  {ecom_active_n:,} active e-com keyword references "
          f"across {sum(len(c) for c in ecom_out.values())} categories")

    # ── 5. Write master ──────────────────────────────────────────────────────
    master = {
        "version": 1,
        "built_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_csv": os.path.basename(scored_csv),
        "stats": {
            "total_keywords": len(rows),
            "active": len(active),
            "reserve": len(reserve),
            "industries": len(industries),
            "score_bands": {
                "TIER_1 (score>=40)": sum(1 for r in active if r["score"] >= 40),
                "TIER_2 (20-39)": sum(1 for r in active if 20 <= r["score"] < 40),
                "TIER_3 (1-19)": sum(1 for r in active if r["score"] < 20),
            },
        },
        # Parallel arrays keep the file compact (~half the size of dicts).
        "active": [[r["kw"], r["score"], r["cpc"], r["vol"], r["cat"]] for r in active],
        "industries": industries,
        "ecom": ecom_out,
        "reserve": [r["kw"] for r in reserve],
    }
    with open(OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(master, fh, ensure_ascii=False, separators=(",", ":"))
    print(f"\nWrote {OUTPUT_PATH}  ({os.path.getsize(OUTPUT_PATH)/1e6:.1f} MB)")

    # Keep a canonical copy of the scored CSV inside the project so the master
    # is reproducible even if the Documents copy moves.
    if os.path.abspath(scored_csv) != os.path.abspath(CANONICAL_CSV):
        import shutil
        shutil.copyfile(scored_csv, CANONICAL_CSV)
        print(f"Copied scored CSV -> {CANONICAL_CSV}")

    print("\nStats:", json.dumps(master["stats"], indent=2))


if __name__ == "__main__":
    main()
