# export_grand_keywords.py
# One-shot script: extract EVERY keyword from all project sources -> grand CSV.
# Output: C:/Users/User/Documents/lead_forge_grand_keywords.csv

import csv
import json
import os
import re
import ast
import sys

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
KEYWORDS_DIR = os.path.join(PROJECT_DIR, "KEYWORDS )(")
OUTPUT_PATH = r"C:\Users\User\Documents\lead_forge_grand_keywords.csv"
V5_PATH = os.path.join(PROJECT_DIR, "V5.py")
BANK_10K_PATH = os.path.join(KEYWORDS_DIR, "leadforge_10000_semrush_keyword_bank.txt")
BANK_1K_PATH  = os.path.join(KEYWORDS_DIR, "semrush_top_1000_nonoverlap_keywords.txt")
ECOM_PATH     = os.path.join(KEYWORDS_DIR, "ECOM_INDEX.json")

FIELDNAMES = ["keyword", "source_type", "industry_category", "tier", "retailer", "comment"]

rows = []

# ── 1. V5.py — INDUSTRY_KEYWORDS ─────────────────────────────────────────────
def _extract_py_dict(source_text, var_name):
    """Extract a top-level dict assignment by name using AST (safe, no exec)."""
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

print("Reading V5.py …")
with open(V5_PATH, "r", encoding="utf-8") as f:
    v5_src = f.read()

industry_kws = _extract_py_dict(v5_src, "INDUSTRY_KEYWORDS")
print(f"  INDUSTRY_KEYWORDS: {sum(len(v) for v in industry_kws.values())} keywords across {len(industry_kws)} industries")

for industry, kw_list in industry_kws.items():
    for kw in kw_list:
        kw = kw.strip()
        if kw:
            rows.append({
                "keyword": kw,
                "source_type": "V5_INDUSTRY_SEEDS",
                "industry_category": industry,
                "tier": "",
                "retailer": "",
                "comment": "Curated BOFU seed keyword",
            })

# ── 2. V5.py — NEW_CITY_EXPANSION_KEYWORDS ───────────────────────────────────
city_exp_kws = _extract_py_dict(v5_src, "NEW_CITY_EXPANSION_KEYWORDS")
print(f"  NEW_CITY_EXPANSION_KEYWORDS: {sum(len(v) for v in city_exp_kws.values())} keywords across {len(city_exp_kws)} industries")

for industry, kw_list in city_exp_kws.items():
    for kw in kw_list:
        kw = kw.strip()
        if kw:
            rows.append({
                "keyword": kw,
                "source_type": "V5_CITY_EXPANSION",
                "industry_category": industry,
                "tier": "",
                "retailer": "",
                "comment": "City-expansion industry keyword",
            })

# ── 3. 10k Tiered Bank ────────────────────────────────────────────────────────
# Format: [00001] Industry | Tier | Keyword | Comment
print("Reading 10k bank …")
bank10k_re = re.compile(
    r"^\[\d+\]\s*([^|]+?)\s*\|\s*(\w+)\s*\|\s*([^|]+?)\s*\|\s*(.+)$"
)
count_10k = 0
with open(BANK_10K_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("##"):
            continue
        m = bank10k_re.match(line)
        if m:
            industry, tier, kw, comment = m.group(1), m.group(2), m.group(3).strip(), m.group(4).strip()
            rows.append({
                "keyword": kw,
                "source_type": "TIERED_BANK_10K",
                "industry_category": industry.strip(),
                "tier": tier.strip(),
                "retailer": "",
                "comment": comment,
            })
            count_10k += 1
print(f"  10k bank: {count_10k} keywords")

# ── 4. 1k Global Bank ─────────────────────────────────────────────────────────
# Format: [ID] Source Family | Keyword | Semrush Basis | Note
print("Reading 1k global bank …")
bank1k_re = re.compile(
    r"^\[\d+\]\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*([^|]+?)\s*\|\s*(.+)$"
)
count_1k = 0
with open(BANK_1K_PATH, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line or line.startswith("#") or line.startswith("##"):
            continue
        m = bank1k_re.match(line)
        if m:
            family, kw, basis, note = m.group(1).strip(), m.group(2).strip(), m.group(3).strip(), m.group(4).strip()
            rows.append({
                "keyword": kw,
                "source_type": "GLOBAL_BANK_1K",
                "industry_category": family,
                "tier": "GLOBAL",
                "retailer": "",
                "comment": f"Basis: {basis} | {note}",
            })
            count_1k += 1
print(f"  1k global bank: {count_1k} keywords")

# ── 5. ECOM_INDEX.json ────────────────────────────────────────────────────────
print("Reading ECOM_INDEX.json …")
with open(ECOM_PATH, "r", encoding="utf-8") as f:
    ecom = json.load(f)

count_ecom = 0
categories_blob = ecom.get("categories", {})
for retailer, cat_dict in categories_blob.items():
    for category_path, kw_list in cat_dict.items():
        for kw in kw_list:
            kw = kw.strip()
            if kw:
                rows.append({
                    "keyword": kw,
                    "source_type": "ECOM_INDEX",
                    "industry_category": category_path,
                    "tier": "ECOM",
                    "retailer": retailer.title(),
                    "comment": f"{retailer.title()} marketplace keyword",
                })
                count_ecom += 1
print(f"  ECOM_INDEX: {count_ecom} keywords")

# ── Write CSV ─────────────────────────────────────────────────────────────────
print(f"\nTotal rows to write: {len(rows):,}")
print(f"Writing -> {OUTPUT_PATH}")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

with open(OUTPUT_PATH, "w", newline="", encoding="utf-8-sig") as f:  # utf-8-sig = BOM for Excel
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
    writer.writeheader()
    writer.writerows(rows)

print("\nDone!")
print(f"  File : {OUTPUT_PATH}")
print(f"  Rows : {len(rows):,}  (headers excluded)")
print()
print("Breakdown:")
from collections import Counter
by_source = Counter(r["source_type"] for r in rows)
for src, cnt in sorted(by_source.items(), key=lambda x: -x[1]):
    print(f"  {src:<30}  {cnt:>7,}")
