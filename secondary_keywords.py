"""Secondary keyword dictionary — NON-OVERLAPPING with V5.py:INDUSTRY_KEYWORDS.

This file starts as a placeholder. To populate it with 30 SEMrush-seeded
keywords per industry, run:

    python generate_secondary_keywords.py

The generator reads primary keywords from V5.py, calls SEMrush's phrase
APIs per industry anchor, filters to CPC > 0.05 and volume > 0, strips any
overlap with the primary dict (case-insensitive), and overwrites this file
with the top 30 per industry sorted by volume * cpc.

Until the generator is run, SECONDARY_INDUSTRY_KEYWORDS is empty and the
SecondaryAgent simply never activates — the pipeline behaves as it did
before Phase 2. This is the correct default: no silent wrong-data path.
"""
from __future__ import annotations

from typing import Dict, List

# Format: { "IndustryName": ["keyword one", "keyword two", ...] }
# Populated by generate_secondary_keywords.py.
SECONDARY_INDUSTRY_KEYWORDS: Dict[str, List[str]] = {}

# Metadata (updated by the generator on each run)
GENERATED_AT: str = ""
GENERATED_WITH_DB: str = ""           # e.g. "au" or "us"
GENERATOR_VERSION: str = "1.0.0"


def has_keywords_for(industry: str) -> bool:
    """True if this industry has a populated secondary keyword list."""
    return bool(SECONDARY_INDUSTRY_KEYWORDS.get(industry))


def get_keywords(industry: str) -> List[str]:
    """Return secondary keywords for the given industry, or [] if not populated."""
    return list(SECONDARY_INDUSTRY_KEYWORDS.get(industry, []))
