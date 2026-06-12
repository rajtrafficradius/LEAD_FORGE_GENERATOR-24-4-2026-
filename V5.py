#!/usr/bin/env python3
"""
Lead Generation Automation Tool — V5.12
========================================
A production-ready B2B lead generation tool optimized for PAID traffic only (Google Ads).

V5.12 ENHANCEMENTS (MAJOR):
✓ PAID-ONLY MODE: Removed get_organic_domains() entirely. Keeps ONLY domains with active Google Ads (paid_traffic != 0)
✓ DOUBLED KEYWORDS: Each of 50+ industries has 20-25 NEW keywords (originals kept exactly)
✓ TOKEN CONSERVATION: Only enrich top N leads with API calls (phone/email per max_leads)
✓ TOKEN TRACKING PANEL: Comprehensive run summary showing:
  - Lead sources: PAID vs ORGANIC breakdown
  - Contact coverage: Phone / Email / Personal Email counts
  - API token usage: SEMrush / Apollo / Lusha / SerpApi / OpenAI per run
  - Enrichment efficiency: How many leads got direct phone & verified emails
✓ PERSONAL EMAIL PRIORITY: Search APIs manually for personal emails, prefer business owners
✓ LOGGING: End-of-run summary with paid/organic/token breakdown

V5.4-V5.11 Features (inherited):
- Partition-based sorting: Name+Email+Phone → Name+Phone → Name+Email → Phone → Email
- Three-tier decision maker engine (HARD_DM / SOFT_DM / TRADE_ROLE_WORDS)
- 60+ personal email domains classifier (gmail, yahoo, hotmail, icloud, bigpond, etc.)
- ThreadPoolExecutor with batched submission (8 workers, 8-domain batches)
- Credit gate with phone validation (_direct_phone flag)
- Company size filter (skip >500 employees — not SMB targets)

Pipeline: SEMrush Keywords → SEMrush/PAID-ONLY Domain Discovery → Apollo/Lusha Enrichment → CSV Export + Token Summary

Requirements: requests, beautifulsoup4
Target: Python 3.11+ / PyCharm 2025.3
"""

import csv
import html as html_mod
import json
import os
import platform
import random
import re
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlencode, urlparse

import requests
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION & CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

API_KEYS = {
    "semrush": os.environ.get("SEMRUSH_API_KEY", ""),
    # 2026-06-01: multi-key support. SERPAPI_API_KEYS (plural, comma/semicolon
    # separated) takes precedence so users can chain multiple free-tier
    # accounts (each = 100 searches/month); falls back to SERPAPI_API_KEY for
    # backwards compatibility. SerpApiClient parses either form transparently.
    "serpapi": (
        os.environ.get("SERPAPI_API_KEYS", "").strip()
        or os.environ.get("SERPAPI_API_KEY", "")
    ),
    "apollo": os.environ.get("APOLLO_API_KEY", ""),
    "lusha": os.environ.get("LUSHA_API_KEY", ""),
    "openai": os.environ.get("OPENAI_API_KEY", ""),
    "hunter": os.environ.get("HUNTER_API_KEY", ""),  # Optional: Hunter.io email enrichment (set env var)
    # 2026-05-18: Google Places key for the additive AU intent layer
    # (see google_places_intent.py + city_pipeline._discover_domains Pass 3.5).
    # ONE key only — no OAuth, no developer tokens, no customer IDs.
    "google_places": os.environ.get("GOOGLE_PLACES_API_KEY", ""),
    # 2026-05-25: Google Gemini key (aistudio.google.com) for the new
    # Vertex AI semantic domain-ranker. Used by vertex_ai_ranker.py to
    # score 200+ raw discovered domains by B2B buying intent BEFORE
    # Apollo enrichment fires — so Apollo credits land on the best
    # candidates first. ONE key, no OAuth, no GCP project setup.
    # Get a key at https://aistudio.google.com/app/apikey (free tier:
    # 1500 requests/day — plenty for typical lead-gen runs).
    "gemini": os.environ.get("GEMINI_API_KEY", ""),
    # 2026-05-25: Google Custom Search JSON API — free tier 100 queries/day.
    # Needs BOTH a key (Google Cloud Console) AND a Programmable Search
    # Engine ID (cx) created at https://programmablesearchengine.google.com
    # configured to "Search the entire web". Used by google_custom_search.py
    # as an additive organic-results discovery layer alongside SerpAPI and
    # Google Places. Leave either blank to disable.
    "google_custom_search":    os.environ.get("GOOGLE_CUSTOM_SEARCH_API_KEY", ""),
    "google_custom_search_cx": os.environ.get("GOOGLE_CUSTOM_SEARCH_CX", ""),
}

# V5.13: Credit cost per API call (for cost tracking panel)
API_CREDIT_COSTS = {
    "semrush": 10,    # ~10 units per API call
    "apollo": 1,      # 1 Apollo export credit per enrichment call
    "lusha": 1,       # 1 Lusha credit per person lookup
    "serpapi": 1,     # 1 SerpApi search credit
    "openai": 0.01,   # ~$0.01 per OpenAI gpt-4o-mini call
    "hunter": 1,      # 1 Hunter.io request credit
    # 2026-05-18: Google Places Text Search billed in tier units; ~$0.032
    # per Text Search call + ~$0.017 per Details fallback on the standard
    # SKU. Free tier covers ~10k Text Searches/month — small here.
    "google_places": 1,
    # 2026-05-25: Gemini API tier-1 is FREE; tier-2 is ~$0.0001/req for
    # gemini-2.0-flash. Cost-per-call is dominated by Apollo anyway —
    # one Gemini batched-rank call eliminates ~10-50 wasted Apollo enrich
    # calls so its net cost is negative.
    "gemini": 0,
    # 2026-05-25: Custom Search JSON API free tier covers 100 queries/day.
    # Above that the paid tier is $5/1000 queries. Tracked but billed at 0
    # because the per-run cap (GOOGLE_CSE_MAX_QUERIES default 10) sits well
    # under the free quota.
    "google_custom_search": 0,
}


# ── 2026-06-08: dollar-cost helpers (shared by /status leads + run finalize) ──
# Single source of truth for "what credits did this run consume" so the live
# per-lead cost shown in the Generate table EXACTLY matches the value frozen
# into the DB at finalize.
def _v5_cost_usage_from_pipeline(pipeline) -> dict:
    """Per-credit-type consumption for a finished/in-flight pipeline."""
    _ctr = getattr(pipeline, "_api_counter", {}) or {}
    return {
        "serpapi":       int(_ctr.get("serpapi", 0) or 0),
        "google_places": int(_ctr.get("google_places", 0) or 0),
        "semrush_units": int(_ctr.get("semrush_units", 0) or 0),
        "apollo_email":  int(getattr(pipeline, "_email_credits_used", 0) or 0),
        "apollo_phone":  int(getattr(pipeline, "_phone_credits_used", 0) or 0),
        "apollo_export": int(_ctr.get("apollo", 0) or 0),
        "lusha":         int(_ctr.get("lusha", 0) or 0),
        "hunter":        int(_ctr.get("hunter", 0) or 0),
        "openai":        int(_ctr.get("openai", 0) or 0),
        "gemini":        int(_ctr.get("gemini", 0) or 0),
    }


def _v5_run_cost(pipeline, leads_total=None):
    """Returns (usage, total_usd, cost_per_lead_usd, per_item). Fail-open to 0s."""
    try:
        import pricing as _pr
        usage = _v5_cost_usage_from_pipeline(pipeline)
        info = _pr.compute_run_cost(usage)
        n = leads_total if leads_total is not None else len(getattr(pipeline, "leads", []) or [])
        cpl = _pr.cost_per_lead(info["total"], n)
        return usage, float(info["total"]), float(cpl), info.get("per_item", {})
    except Exception:
        return {}, 0.0, 0.0, {}

# ── Google Places Intent Discovery (additive AU layer, 2026-05-18) ─────
# Hard caps for the Places sweep. AU-only, city-only. Reads the env vars
# at IMPORT time so the same values are visible both here AND inside
# google_places_intent.py — single source of truth.
#
#   GOOGLE_INTENT_MAX_PLACES_CALLS   default 25 — HTTPS round-trips total
#                                    (textsearch + details fallback combined)
#   GOOGLE_INTENT_MAX_DOMAINS        default 100 — unique AU domains kept
#
# Bump these only if you've enabled GCP paid billing AND need wider coverage.
# Free tier covers the defaults comfortably for the typical AU run.
GOOGLE_INTENT_MAX_PLACES_CALLS = int(os.environ.get("GOOGLE_INTENT_MAX_PLACES_CALLS", "25") or "25")
GOOGLE_INTENT_MAX_DOMAINS      = int(os.environ.get("GOOGLE_INTENT_MAX_DOMAINS", "100") or "100")

# 2026-05-26: Paid-traffic gate applied to Places-discovered domains at
# FETCH time — mirrors V5's existing `paid_traffic >= 5` post-Apollo gate
# so we don't burn Apollo credits on Places businesses that don't run paid
# ads. Verifier is SEMrush domain_ranks (primary) + SerpAPI ads-only
# branded query (fallback). See google_places_intent.discover() and the
# closures in city_pipeline.PASS-3.5 / V5._phase3_domain_discovery.
#   GOOGLE_PLACES_PAID_MIN     default 5 — same threshold as V5 phase-4 gate
#   GOOGLE_PLACES_VERIFY_MAX   default 30 — cap on verifier calls per run
#                              (each call = ~1 SEMrush domain_ranks request OR
#                              1 SerpAPI credit fallback)
GOOGLE_PLACES_PAID_MIN   = int(os.environ.get("GOOGLE_PLACES_PAID_MIN", "5") or "5")
GOOGLE_PLACES_VERIFY_MAX = int(os.environ.get("GOOGLE_PLACES_VERIFY_MAX", "30") or "30")

# V5.13: Rotating User-Agent headers for anti-detection
_ROTATING_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
]

def _get_random_ua() -> str:
    return random.choice(_ROTATING_USER_AGENTS)

# V5.26: Apollo webhook-based phone reveal
# Set WEBHOOK_BASE_URL to your app's public URL (e.g., https://your-app.up.railway.app)
# When set, the Flask endpoint at /api/apollo-phone-callback receives phone data directly.
# When NOT set, a temporary webhook.site token is created automatically for each pipeline run.
WEBHOOK_BASE_URL = os.environ.get("WEBHOOK_BASE_URL", "")
_APOLLO_PHONE_CALLBACK_PATH = "/api/apollo-phone-callback"

# V5.26: Thread-safe phone reveal store — receives async phone data from Apollo webhook
_phone_reveal_store: dict = {}  # person_id -> {"phone_numbers": [...], "received": bool}
_phone_reveal_lock = threading.Lock()
_webhook_site_token: str = ""  # webhook.site token UUID (used when no WEBHOOK_BASE_URL)

def _receive_phone_reveal(person_id: str, phone_numbers: list):
    """Called by webhook handler when Apollo delivers phone data."""
    with _phone_reveal_lock:
        _phone_reveal_store[person_id] = {"phone_numbers": phone_numbers, "received": True}

def _collect_phone_reveal(person_id: str):
    """Collect phone reveal data if available. Returns phone_numbers list or None."""
    with _phone_reveal_lock:
        entry = _phone_reveal_store.get(person_id)
        if entry and entry.get("received"):
            return entry.get("phone_numbers")
    return None

def _register_phone_reveal(person_id: str):
    """Register a person_id as pending phone reveal."""
    with _phone_reveal_lock:
        if person_id not in _phone_reveal_store:
            _phone_reveal_store[person_id] = {"phone_numbers": None, "received": False}

        _phone_reveal_store.clear()

def _create_webhook_site_token() -> str:
    """Create a temporary webhook.site token for receiving Apollo phone callbacks."""
    try:
        resp = requests.post("https://webhook.site/token", timeout=15)
        if resp.status_code == 201:
            return resp.json().get("uuid", "")
    except Exception:
        pass
    return ""

def _poll_webhook_site_phones(token_uuid: str):
    """Poll webhook.site for received Apollo phone reveal data and store it."""
    if not token_uuid:
        return
    try:
        resp = requests.get(
            f"https://webhook.site/token/{token_uuid}/requests?sorting=newest&per_page=50",
            timeout=15
        )
        if resp.status_code == 200:
            import json as _json
            for req in resp.json().get("data", []):
                content = req.get("content", "")
                if not content:
                    continue
                try:
                    payload = _json.loads(content)
                    for person in payload.get("people", []):
                        pid = person.get("id", "")
                        phones = person.get("phone_numbers") or []
                        if pid and phones:
                            _receive_phone_reveal(pid, phones)
                except Exception:
                    pass
    except Exception:
        pass

def _delete_webhook_site_token(token_uuid: str):
    """Delete a webhook.site token after use."""
    if token_uuid:
        try:
            requests.delete(f"https://webhook.site/token/{token_uuid}", timeout=5)
        except Exception:
            pass

def _get_webhook_url() -> str:
    """Get the webhook URL for Apollo phone reveal.
    Uses WEBHOOK_BASE_URL if set (self-hosted), otherwise creates a webhook.site token."""
    global _webhook_site_token
    if WEBHOOK_BASE_URL:
        return WEBHOOK_BASE_URL.rstrip("/") + _APOLLO_PHONE_CALLBACK_PATH
    if not _webhook_site_token:
        _webhook_site_token = _create_webhook_site_token()
    if _webhook_site_token:
        return f"https://webhook.site/{_webhook_site_token}"
    return ""

# V5.7: Credit tracking constants
LUSHA_PLAN_CREDITS = 1000  # Set to your Lusha plan's credit allocation
SEMRUSH_PLAN_TOTAL = 50000  # Default Semrush plan total (used when only remaining is available)
_lusha_calls_total = 0  # Running total of Lusha API calls since server start

COUNTRY_CONFIG = {
    "AU": {
        "name": "Australia",
        "semrush_db": "au",
        "serpapi_gl": "au",
        "phone_code": "+61",
        "phone_regex": r"(?:\+61\s?|0)[2-478](?:[\s.-]?\d){8}",
        "phone_digits": 11,
        "location_suffix": "Australia",
    },
    "USA": {
        "name": "United States",
        "semrush_db": "us",
        "serpapi_gl": "us",
        "phone_code": "+1",
        "phone_regex": r"(?:\+1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}",
        "phone_digits": 11,
        "location_suffix": "United States",
    },
    "UK": {
        "name": "United Kingdom",
        "semrush_db": "uk",
        "serpapi_gl": "uk",
        "phone_code": "+44",
        "phone_regex": r"(?:\+44\s?|0)\d{2,4}[\s.-]?\d{3,4}[\s.-]?\d{3,4}",
        "phone_digits": 12,
        "location_suffix": "United Kingdom",
    },
    "India": {
        "name": "India",
        "semrush_db": "in",
        "serpapi_gl": "in",
        "phone_code": "+91",
        "phone_regex": r"(?:\+91[\s.-]?|0)?[6-9]\d{9}",
        "phone_digits": 12,
        "location_suffix": "India",
    },
}

# Platform domains to filter out during domain discovery
PLATFORM_DOMAINS = {
    "google.com", "google.com.au", "google.co.uk", "google.co.in",
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "linkedin.com", "youtube.com", "tiktok.com", "pinterest.com",
    "yelp.com", "yelp.com.au", "yellowpages.com", "yellowpages.com.au",
    "wikipedia.org", "reddit.com", "quora.com", "medium.com",
    "amazon.com", "ebay.com", "ebay.com.au", "alibaba.com",
    "tripadvisor.com", "trustpilot.com", "bbb.org",
    "apple.com", "microsoft.com", "adobe.com",
    "healthgrades.com", "webmd.com", "zocdoc.com",
    "thumbtack.com", "homeadvisor.com", "angi.com", "angieslist.com",
    "glassdoor.com", "indeed.com", "seek.com.au",
    "truelocal.com.au", "hotfrog.com.au", "startlocal.com.au",
    "whitepages.com.au", "yell.com", "justdial.com", "sulekha.com",
    "indiamart.com", "practo.com", "justlanded.com",
    "crunchbase.com", "bloomberg.com", "forbes.com",
    "gov.au", "nhs.uk", "gov.uk", "gov.in", "fda.gov",
    "healthengine.com.au", "hotdoc.com.au", "ratemds.com",
    "wordofmouth.com.au", "localsearch.com.au",
    "finder.com.au", "canstar.com.au", "productreview.com.au",
    "serviceseeking.com.au", "hipages.com.au", "oneflare.com.au",
    "airtasker.com", "bark.com",
    # Health/medical info sites (not actual practices)
    "healthline.com", "mayoclinic.org", "clevelandclinic.org",
    "my.clevelandclinic.org", "webmd.com", "medicalnewstoday.com",
    "verywellhealth.com", "betterhealth.vic.gov.au",
    # Large retailers/corporates (not SMBs)
    "woolworths.com.au", "chemistwarehouse.com.au", "priceline.com.au",
    "amazon.com.au", "colgate.com.au", "colgate.com",
    "bupa.com", "bupa.com.au", "bupaglobal.com",
    "bupaagedcare.com.au", "bupatravelinsurance.com.au",
    # Educational / government
    "sydney.edu.au", "unimelb.edu.au", "uq.edu.au",
    "monash.edu", "adelaide.edu.au", "unsw.edu.au",
    # News / media sites (not SMBs)
    "news.com.au", "smh.com.au", "theaustralian.com.au", "abc.net.au",
    "9news.com.au", "7news.com.au", "sbs.com.au", "dailytelegraph.com.au",
    "couriermail.com.au", "heraldsun.com.au", "theage.com.au",
    "newcastleherald.com.au", "illawarramercury.com.au", "canberratimes.com.au",
    "brisbanetimes.com.au", "watoday.com.au", "examiner.com.au",
    "perthnow.com.au", "adelaidenow.com.au", "geelongadvertiser.com.au",
    "goldcoastbulletin.com.au", "townsvillebulletin.com.au", "cairnspost.com.au",
    "cnn.com", "bbc.com", "bbc.co.uk", "nytimes.com", "theguardian.com",
    "foxnews.com", "nbcnews.com", "cbsnews.com", "abcnews.go.com",
    "reuters.com", "apnews.com", "usatoday.com", "washingtonpost.com",
    "wsj.com", "huffpost.com", "nypost.com", "latimes.com", "chicagotribune.com",
    "dailymail.co.uk", "telegraph.co.uk", "independent.co.uk",
    "mirror.co.uk", "express.co.uk", "thesun.co.uk", "sky.com", "itv.com",
    "metro.co.uk", "standard.co.uk", "scotsman.com", "walesonline.co.uk",
    "ndtv.com", "timesofindia.indiatimes.com", "hindustantimes.com",
    "thehindu.com", "indianexpress.com", "news18.com", "livemint.com",
    "dnaindia.com", "deccanherald.com", "tribuneindia.com",
    "firstpost.com", "scroll.in", "theprint.in", "thewire.in",
    "buzzfeed.com", "vice.com", "vox.com", "businessinsider.com",
    "techcrunch.com", "theverge.com", "wired.com", "mashable.com",
    "arstechnica.com", "engadget.com", "gizmodo.com",
}

# Non-decision-maker role keywords — leads with these roles get role blanked (lead kept)
NON_DECISION_MAKER_KEYWORDS = {
    "intern", "trainee", "volunteer", "student", "apprentice",
    "janitor", "custodian", "mail room", "filing",
    "warehouse", "driver", "delivery", "labourer", "laborer","plumber"
}

# ── Decision-maker engine (V5.11) ─────────────────────────────────────────────
# HARD_DM: these titles ALWAYS indicate a genuine decision maker regardless of industry
HARD_DM_KEYWORDS = {
    "owner", "co-owner", "business owner",
    "founder", "co-founder", "cofounder",
    "ceo", "chief executive", "managing director", "md",
    "cfo", "cto", "coo", "cmo", "cio", "cpo",
    "chief", "chairman", "chairwoman", "chairperson",
    "president", "vice president", "vp",
    "director", "general manager", "gm",
    "partner", "principal",
    "board member", "board director",
    "svp", "evp", "avp",
}

# SOFT_DM: only a DM if no trade/craft word is also in the title
SOFT_DM_KEYWORDS = {
    "manager", "head of", "head,",
    "executive", "managing", "supervisor",
    "operations manager", "office manager",
    "business development", "account director",
}

# TRADE_ROLE_WORDS: craft/trade words — override SOFT_DM if present in title
# e.g. "Lead Plumber" → NOT a DM; "Managing Director" → DM
TRADE_ROLE_WORDS = {
    "plumber", "plumbing",
    "electrician", "electrical worker",
    "carpenter", "joiner", "cabinetmaker",
    "painter", "decorator",
    "mechanic", "automotive technician",
    "welder", "boilermaker",
    "bricklayer", "stonemason", "concreter",
    "plasterer", "tiler", "renderer",
    "roofer", "guttering",
    "landscaper", "gardener", "arborist",
    "cleaner", "cleaning staff",
    "handyman", "maintenance worker",
    "locksmith", "glazier",
    "hvac technician", "refrigeration mechanic",
    "forklift operator", "crane operator",
    "labourer", "laborer",
    "apprentice", "trainee",
    "installer", "fitter",
    "surveyor", "drafter",
    "estimator",  # only soft DM when combined with trade words
    # V5.25: Additional service/trade roles — these are practitioners, not decision-makers
    "dentist", "general dentist", "dental assistant", "dental hygienist",
    "dental nurse", "dental therapist", "hygienist",
    "photographer", "videographer",
    "accountant", "bookkeeper",
    "nurse", "registered nurse", "nurse practitioner",
    "therapist", "physiotherapist", "chiropractor",
    "pest control technician", "pest technician",
    "real estate agent", "property manager",
    "barber", "hairdresser", "stylist",
    "chef", "cook",
    "driver", "courier",
}

# V5.8: Decision-maker role keywords (combined for backward compat with scoring)
DECISION_MAKER_KEYWORDS = HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS

# V5.29: Decision-maker priority order — higher score = more senior DM
# Used by Phase 5f per-company cap to pick the best DM per company.
# Substring matching: highest-scoring keyword appearing anywhere in role wins.
# Order follows user spec: Owner → Founder → Partner → VP → Head → Director → Manager → Senior
DM_PRIORITY_ORDER = [
    # (keyword_substring, priority_score)  — longer phrases first for correct substring match
    ("business owner", 100),
    ("co-owner", 99),
    ("owner", 98),
    ("co-founder", 96),
    ("cofounder", 96),
    ("founder", 95),
    ("chairman", 93),
    ("chairwoman", 93),
    ("chairperson", 93),
    ("president", 92),
    ("chief executive officer", 91),
    ("chief executive", 91),
    ("ceo", 90),
    # Phase 2 (2026-05-05): joint MD outranks plain managing director (Indian/UK
    # corporate structure where Joint MD is co-equal with MD).
    ("joint managing director", 90),
    ("managing director", 89),
    ("managing partner", 88),
    ("partner", 87),
    ("principal", 86),
    # Phase 2 (2026-05-05): explicit C-suite full-titles alongside acronyms.
    # Substring match still keeps both forms valid.
    ("chief operating officer", 84),
    ("chief financial officer", 84),
    ("chief marketing officer", 84),
    ("chief technology officer", 84),
    ("coo", 84),
    ("cfo", 84),
    ("cto", 84),
    ("cmo", 84),
    ("cio", 84),
    ("cpo", 84),
    ("chief", 83),
    ("svp", 80),
    ("evp", 80),
    ("avp", 79),
    ("vice president", 80),
    ("vp", 78),
    # Phase 2 (2026-05-05): explicit "head of X" + "director of X" entries
    # rank just above plain "head of"/"director" so the most-senior matched
    # phrase wins per-company in _phase5f_dm_cap_and_topup.
    ("head of operations", 78),
    ("head of sales & marketing", 78),
    ("head of sales and marketing", 78),
    ("head of sales", 78),
    ("head of ecommerce", 78),
    ("head of e-commerce", 78),
    ("director of sales", 78),
    ("director of operations", 78),
    ("executive director", 76),
    ("technical director", 76),
    ("co-director", 76),
    ("head of", 75),
    # Phase 2 (2026-05-05): explicit specialist-director titles slot above
    # plain "director" (70). All still pass the HARD-DM gate (>=65).
    ("sales director", 73),
    ("marketing director", 73),
    ("production director", 73),
    ("md", 72),
    ("director", 70),
    ("board member", 68),
    ("board director", 68),
    ("general manager", 65),
    ("gm", 63),
    ("account director", 60),
    ("business development", 58),
    ("operations manager", 56),
    ("office manager", 55),
    ("practice manager", 55),
    ("manager", 50),
    ("supervisor", 48),
    ("executive", 46),
    ("managing", 45),
    ("senior", 40),
]

# V5.29: Negative role patterns — if role contains ANY of these phrases AND
# no HARD DM keyword (priority ≥ 65) matches, lead is NOT a decision maker.
# Covers department/support staff and individual contributors.
NEGATIVE_DM_ROLE_PATTERNS = [
    # Sales department staff
    "sales manager", "sales executive", "sales rep", "sales representative",
    "sales associate", "sales coordinator", "sales consultant", "sales agent",
    "sales specialist", "sales person", "salesperson", "sales support",
    "account manager", "account executive", "account coordinator",
    "account representative", "account specialist",
    # HR / People Ops
    "hr manager", "hr executive", "hr coordinator", "hr specialist",
    "human resources manager", "human resources coordinator",
    "human resources specialist", "human resources executive",
    "people operations", "talent acquisition", "recruiter", "recruitment",
    # Customer-facing support / service
    "customer service", "customer support", "customer success",
    "client services manager", "client services coordinator",
    # Marketing department staff (not director/VP)
    "marketing coordinator", "marketing assistant", "marketing specialist",
    "marketing executive", "marketing manager",
    "content manager", "content designer", "social media manager",
    "digital marketing",
    # Office / admin
    "executive assistant", "personal assistant", "administrative assistant",
    "office assistant", "receptionist", "secretary", "office coordinator",
    "project coordinator", "project assistant",
    # IT support
    "support specialist", "technical support", "it support",
    "help desk", "helpdesk",
    # Generic individual-contributor / junior tokens (single word, word-boundary)
    "assistant", "coordinator", "specialist", "analyst", "designer",
    "developer", "engineer", "technician", "operator", "associate",
    "representative", "rep", "agent", "intern", "trainee", "apprentice",
    "graduate", "junior", "consultant",
    # Trade practitioners that aren't DMs
    "electrician", "plumber", "carpenter", "labourer", "laborer",
]


def _matches_negative_dm_pattern(role: str) -> bool:
    """V5.29: True if role contains any NEGATIVE_DM_ROLE_PATTERNS phrase
    (word-boundary match)."""
    if not role:
        return False
    role_lower = role.lower().strip()
    for neg in NEGATIVE_DM_ROLE_PATTERNS:
        if re.search(r"\b" + re.escape(neg) + r"\b", role_lower):
            return True
    return False


def _calculate_dm_priority(role: str) -> int:
    """V5.29: Return DM priority score for a role, or 0 if not a decision maker.

    Logic:
    1. Find best positive DM-keyword match (substring with word boundary).
    2. If best ≥ 65 (HARD DM tier: director, partner, founder, owner, ceo, vp,
       chief, head-of, general-manager), keep it — overrides any negative pattern.
       e.g. 'Sales Director' → 70 (Director wins despite 'sales').
    3. Otherwise, if role matches any NEGATIVE_DM_ROLE_PATTERNS (assistant,
       coordinator, engineer, sales manager, etc.), return 0.
    4. Otherwise return the soft-DM score (40-60), or 0 if no match at all.

    Examples:
        'Owner'                          → 98
        'Co-Owner'                       → 99
        'Marketing Director & Co-Owner'  → 99 (substring 'co-owner')
        'Sales Manager'                  → 0  (negative pattern wins over manager=50)
        'Director'                       → 70
        'Marketing Director'             → 70
        'Senior Engineer'                → 0  (engineer is negative)
        'Practice Manager'               → 55
    """
    if not role or not role.strip():
        return 0
    role_lower = role.lower().strip()
    # Step 1: find best positive match
    best = 0
    for keyword, score in DM_PRIORITY_ORDER:
        kw = keyword.strip()
        if not kw:
            continue
        if re.search(r"\b" + re.escape(kw) + r"\b", role_lower) and score > best:
            best = score
    # Step 2: HARD DM (>=65) overrides negative patterns
    if best >= 65:
        return best
    # Step 3: negative patterns drop everything below HARD tier
    if _matches_negative_dm_pattern(role_lower):
        return 0
    return best

# V5.13: Words that should NEVER appear as part of a person's name
_NAME_FORBIDDEN_WORDS = (
    HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS | TRADE_ROLE_WORDS |
    NON_DECISION_MAKER_KEYWORDS |
    {
        # Common title words
        "director", "manager", "officer", "executive", "president", "chairman",
        "supervisor", "coordinator", "specialist", "analyst", "consultant",
        "engineer", "technician", "advisor", "associate", "assistant",
        "trainee", "intern", "apprentice", "graduate", "professional",
        # Business/company words often appearing in team page context
        "pty", "ltd", "inc", "llc", "corp", "group", "team", "staff",
        "operations", "services", "solutions", "company", "business",
        "plumbing", "dental", "medical", "clinic", "rescue", "local",
        # Common false positives
        "the", "and", "for", "our", "new", "top", "pro", "all",
    }
)


def _is_valid_person_name(name: str) -> bool:
    """V5.13: Return True only if `name` looks like a genuine person's name.
    Rules:
    - Must have exactly 2 or 3 words (First Last or First Middle Last)
    - Each word must be 2-20 chars, alphabetic (hyphens/apostrophes OK)
    - NO word may be a known title, role, trade, or business keyword
    """
    if not name:
        return False
    words = name.strip().split()
    if not (2 <= len(words) <= 3):
        return False
    for w in words:
        clean = w.replace("'", "").replace("-", "")
        if not clean.isalpha():
            return False
        if not (2 <= len(clean) <= 20):
            return False
        if w.lower() in _NAME_FORBIDDEN_WORDS:
            return False
    return True


def _is_obfuscated_name(name: str) -> bool:
    """V5.19: Detect Apollo's locked-contact obfuscated name format.
    Apollo returns 'FirstName I.' (first name + single letter + period) for contacts
    that haven't been revealed yet, e.g. 'Matt M.', 'Jarod L.', 'Riley H.'.
    Returns True if name matches this pattern — these must be treated as single-name leads.
    """
    if not name or " " not in name:
        return False
    parts = name.strip().split()
    if len(parts) == 2:
        last = parts[1].rstrip(".")
        return len(last) == 1 and last.isalpha()
    return False

# Low-relevance keywords (supports/admin - skip expensive enrichment)
LOW_RELEVANCE_KEYWORDS = {
    "intern", "apprentice", "trainee", "student", "junior",
    "support officer", "support staff", "help desk",
    "receptionist", "secretary", "personal assistant",
    "data entry", "filing", "mail room", "delivery", "driver",
    "warehouse", "janitor", "custodian", "cleaner",
    "customer service representative", "call centre",
}

# ── Personal email domains (V5.11) ────────────────────────────────────────────
# ONLY these domains count as "personal" emails (gmail, yahoo, hotmail etc.)
# Company emails like firstname@company.com are classified as "Work" not "Personal"
PERSONAL_EMAIL_DOMAINS = {
    # Global
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.co.uk", "yahoo.com.au", "yahoo.ca", "yahoo.co.in",
    "ymail.com", "rocketmail.com",
    "hotmail.com", "hotmail.co.uk", "hotmail.com.au", "hotmail.ca",
    "outlook.com", "outlook.com.au", "live.com", "live.com.au",
    "msn.com", "passport.com",
    "icloud.com", "me.com", "mac.com",
    "aol.com",
    "protonmail.com", "proton.me", "pm.me",
    "fastmail.com", "fastmail.fm",
    "zoho.com",
    "tutanota.com", "tutamail.com",
    "hey.com",
    "mail.com", "email.com",
    # Australian ISP / consumer
    "bigpond.com", "bigpond.net.au", "telstra.com",
    "optusnet.com.au",
    "tpg.com.au", "tpg.com",
    "internode.on.net",
    "aapt.net.au",
    "iprimus.com.au",
    "westnet.com.au",
    "dodo.com.au",
    # UK consumer
    "btinternet.com", "btopenworld.com",
    "sky.com", "talktalk.net", "virgin.net",
    "ntlworld.com", "blueyonder.co.uk",
    # Indian consumer
    "rediffmail.com", "indiatimes.com",
    # Other common
    "gmx.com", "gmx.net", "gmx.de",
    "web.de", "t-online.de",
    "seznam.cz",
    "yandex.com", "yandex.ru",
}

# ══════════════════════════════════════════════════════════════════════════════
# INDUSTRY KEYWORD DICTIONARY — 25+ industries with 20-25 keywords each
# ══════════════════════════════════════════════════════════════════════════════

INDUSTRY_KEYWORDS = {
    "Dentist": [
        # Original keywords (V5.12: kept exactly as-is)
        "dental implants", "root canal treatment", "teeth whitening",
        "orthodontist near me", "emergency dentist", "dental clinic",
        "cosmetic dentistry", "dental crown", "wisdom tooth removal",
        "periodontal treatment", "dental veneers", "invisalign provider",
        "pediatric dentist", "teeth cleaning service",
        "best dentist near me", "affordable dental care", "top rated dentist",
        "dental practice", "family dentist", "denture clinic",
        "dental surgery", "tooth extraction near me", "dental check up",
        "sedation dentistry", "dental bridge specialist",
        # V5.12: 20+ NEW KEYWORDS (doubled)
        "dental bonding service", "teeth alignment treatment", "gum disease treatment",
        "dental filling replacement", "laser teeth whitening", "smile makeover dentist",
        "teeth straightening near me", "dental check up and cleaning", "tooth-colored fillings",
        "cavity filling dentist", "gum surgery specialist", "oral hygiene cleaning service",
        "dental bridge replacement", "tooth implant specialist", "professional teeth cleaning",
        "cosmetic smile design", "emergency tooth extraction", "dental crown restoration",
        "full mouth dental implants", "teeth grinding treatment", "affordable implant dentist",
    ],
    "Doctor / General Practitioner": [
        "family doctor near me", "general practitioner clinic", "bulk billing doctor",
        "medical centre", "walk in clinic", "health check up",
        "vaccination clinic", "GP appointment", "after hours doctor",
        "women's health clinic", "men's health check", "pathology services",
        "best GP near me", "doctor accepting new patients", "skin check doctor",
        "travel doctor vaccination", "chronic disease management GP",
        "mental health GP", "telehealth doctor", "occupational health doctor",
        "sports medicine doctor", "urgent care clinic", "allied health centre",
        # V5.14: Doubled keywords
        "GP bulk billing near me", "medical clinic appointment", "doctor online consultation",
        "childhood immunisation GP", "annual health check doctor", "repeat prescription GP",
        "DVA doctor", "aged care GP visit", "care plan GP",
        "STI testing clinic", "mental health care plan GP", "diabetes management doctor",
        "blood pressure check GP", "cancer screening GP", "skin cancer doctor",
        "workplace injury doctor", "immigration medical GP", "private GP clinic",
        "same day GP appointment", "GP accepting medicare", "24 hour medical centre",
    ],
    "Lawyer / Attorney": [
        "family lawyer", "criminal defence lawyer", "personal injury attorney",
        "divorce lawyer near me", "immigration lawyer", "business lawyer",
        "estate planning attorney", "property conveyancer", "employment lawyer",
        "traffic lawyer", "wills and probate", "commercial litigation",
        "best lawyer near me", "affordable legal services", "top rated law firm",
        "corporate lawyer", "intellectual property lawyer", "construction lawyer",
        "medical negligence lawyer", "workers compensation lawyer",
        "debt recovery lawyer", "small business legal advice", "contract lawyer",
        "tax dispute lawyer", "strata lawyer",
        # V5.14: Doubled keywords
        "unfair dismissal lawyer", "defamation lawyer", "shareholder dispute lawyer",
        "franchise lawyer", "leasing lawyer", "property settlement lawyer",
        "de facto relationship lawyer", "child custody lawyer", "criminal charges lawyer",
        "AVO application lawyer", "guardianship lawyer", "elder law attorney",
        "migration agent lawyer", "business acquisition lawyer", "trademark lawyer",
        "privacy law consultant", "discrimination lawyer", "restraint of trade lawyer",
        "litigation lawyer near me", "will dispute lawyer", "power of attorney lawyer",
    ],
    "Accountant": [
        "tax accountant near me", "small business accountant", "bookkeeping services",
        "tax return preparation", "BAS lodgement service", "financial auditing",
        "payroll services", "business advisory", "self managed super fund accountant",
        "company tax planning", "forensic accounting", "xero certified accountant",
        "best accountant near me", "affordable tax services", "CPA near me",
        "startup accountant", "trust accountant", "GST registration accountant",
        "property tax accountant", "tax planning advisor", "cloud accounting service",
        "quarterly BAS preparation", "business structure advice", "capital gains tax accountant",
        # V5.14: Doubled keywords
        "myob accountant", "individual tax return", "negative gearing accountant",
        "investment property tax return", "contractor tax accountant", "ABN registration accountant",
        "crypto tax accountant", "company registration accountant", "business restructure accountant",
        "fringe benefits tax accountant", "R&D tax incentive accountant", "due diligence accountant",
        "family trust tax accountant", "partnership tax return", "sole trader accountant",
        "NFP accountant", "aged care financial accountant", "SMSF audit accountant",
        "accounting software setup", "business sale accountant", "grant application accountant",
    ],
    "Plumber": [
        # Original keywords (V5.12: kept exactly as-is)
        "emergency plumber", "blocked drain plumber", "hot water system repair",
        "gas plumber near me", "bathroom renovation plumber", "leak detection service",
        "pipe relining", "backflow prevention", "plumbing maintenance",
        "sewer repair service", "tap replacement", "toilet repair plumber",
        "best plumber near me", "affordable plumbing service", "24 hour plumber",
        "commercial plumber", "licensed gas fitter", "water heater installation",
        "burst pipe repair", "stormwater drainage plumber", "kitchen plumbing",
        "plumber quote", "rainwater tank installation", "grease trap cleaning",
        # V5.12: 20+ NEW KEYWORDS (doubled)
        "drain cleaning service", "pipe repair plumber", "emergency burst pipe",
        "water leak repair", "gas fitting specialist", "plumbing inspection service",
        "bathroom plumbing installation", "drain blockage clearing", "pipe replacement plumber",
        "hot water service repair", "toilet installation plumber", "roof gutter plumber",
        "underground pipe repair", "commercial plumbing contractor", "plumbing renovation service",
        "water pressure adjustment", "drainage system installation", "emergency plumbing call out",
        "plumbing maintenance plan", "storm water drainage specialist", "water meter plumber",
    ],
    "Electrician": [
        "emergency electrician", "electrical contractor near me", "solar panel installer",
        "switchboard upgrade", "LED lighting installation", "smoke alarm installation",
        "electrical safety inspection", "ceiling fan installation", "EV charger installer",
        "commercial electrician", "security lighting", "power point installation",
        "best electrician near me", "affordable electrical services", "24 hour electrician",
        "licensed electrician", "home rewiring", "electrical fault finding",
        "three phase power installation", "data cabling electrician",
        "outdoor lighting installation", "generator installation", "smart home electrician",
        "industrial electrician", "strata electrician",
        # V5.14: Doubled keywords
        "RCD installation electrician", "underground power connection", "pool electrical inspection",
        "home theatre wiring", "CCTV installation electrician", "intercom system installation",
        "solar battery installation", "energy efficiency electrician", "test and tag service",
        "private power pole installation", "caravan power connection", "switchboard fault repair",
        "emergency lighting electrician", "exit sign installation", "electrical compliance certificate",
        "domestic wiring electrician", "air conditioner wiring", "CBUS home automation",
        "NBN connection electrician", "hot water system wiring", "electrical quote near me",
    ],
    "Real Estate Agent": [
        "real estate agent near me", "property valuation", "house for sale",
        "property management service", "real estate auctioneer", "buyer's agent",
        "commercial real estate", "rental property manager", "land for sale",
        "investment property advisor", "first home buyer agent", "luxury real estate",
        "best real estate agent", "top selling agent", "property appraisal free",
        "sell my house fast", "local real estate office", "real estate agency",
        "property market analysis", "off market properties", "strata management",
        "real estate consultant", "auction specialist agent",
        # V5.14: Doubled keywords
        "townhouse for sale near me", "apartment for sale near me", "unit for rent near me",
        "property leasing agent", "residential property sales", "semi-detached house sale",
        "acreage property for sale", "rural property agent", "estate agent open home",
        "deceased estate sale agent", "downsizing real estate agent", "prestige property agent",
        "property styling service", "tenant finding service", "rental yield analysis",
        "suburb property report", "online property listing", "flat fee real estate agent",
        "seller's agent near me", "subdivision development agent", "house auction result",
    ],
    "Restaurant / Cafe": [
        "restaurant near me", "cafe near me", "fine dining restaurant",
        "pizza delivery", "catering service", "private dining",
        "brunch cafe", "takeaway food", "function venue",
        "restaurant booking", "food delivery service", "organic cafe",
        "best restaurant near me", "top rated cafe", "family restaurant",
        "italian restaurant", "thai restaurant near me", "sushi restaurant",
        "vegan cafe", "breakfast cafe", "coffee roaster cafe",
        "licensed restaurant", "seafood restaurant", "indian restaurant near me",
        # V5.14: Doubled keywords
        "wood fired pizza restaurant", "degustation menu restaurant", "BYO restaurant near me",
        "outdoor dining restaurant", "rooftop bar restaurant", "live music restaurant",
        "gluten free cafe", "dessert cafe near me", "bubble tea cafe",
        "smoothie bar near me", "juice bar near me", "high tea venue",
        "waterfront restaurant near me", "korean BBQ restaurant", "greek restaurant near me",
        "mexican restaurant near me", "Lebanese restaurant near me", "buffet restaurant near me",
        "halal restaurant near me", "pet friendly cafe", "child friendly restaurant",
    ],
    "Gym / Fitness": [
        "gym near me", "personal trainer", "fitness centre",
        "crossfit gym", "yoga studio near me", "pilates classes",
        "boxing gym", "24 hour gym", "group fitness classes",
        "strength training gym", "weight loss program", "martial arts studio",
        "best gym near me", "affordable gym membership", "women's only gym",
        "functional fitness gym", "HIIT classes near me", "spin class",
        "gym with pool", "bootcamp fitness", "senior fitness classes",
        "powerlifting gym", "reformer pilates studio",
        # V5.14: Doubled keywords
        "muay thai gym near me", "BJJ gym near me", "olympic weightlifting gym",
        "no contract gym membership", "gym with childcare", "corporate gym membership",
        "outdoor bootcamp fitness", "obstacle course training", "rock climbing gym",
        "online personal trainer", "virtual fitness classes", "home gym equipment near me",
        "bodybuilding gym near me", "swimming pool fitness", "aqua aerobics classes",
        "senior fitness centre", "post natal fitness class", "kettlebell training gym",
        "functional movement gym", "agility training gym", "sports performance gym",
    ],
    "Auto Repair / Mechanic": [
        "car mechanic near me", "auto repair shop", "car service centre",
        "brake repair", "transmission repair", "tyre replacement",
        "roadworthy certificate", "logbook service", "car air conditioning repair",
        "diesel mechanic", "mobile mechanic", "pre purchase car inspection",
        "best mechanic near me", "affordable car service", "auto electrician",
        "clutch repair", "suspension repair", "wheel alignment near me",
        "car battery replacement", "exhaust repair", "engine diagnostic",
        "hybrid car mechanic", "fleet vehicle servicing",
    ],
    "Salon / Spa / Beauty": [
        "hair salon near me", "beauty salon", "day spa",
        "nail salon", "barber shop near me", "laser hair removal",
        "facial treatment", "massage therapy", "eyebrow threading",
        "bridal hair and makeup", "skin clinic", "waxing salon",
        "best hair salon near me", "affordable beauty treatments", "keratin treatment",
        "balayage specialist", "men's grooming salon", "eyelash extensions",
        "microdermabrasion", "chemical peel treatment", "anti aging facial",
        "hair colour specialist", "scalp treatment", "body contouring spa",
        # V5.14: Doubled keywords
        "spray tan salon", "lash lift near me", "brow lamination near me",
        "lip blush tattoo", "microblading near me", "permanent makeup artist",
        "skin needling clinic", "hydrafacial near me", "LED light therapy salon",
        "IPL hair removal salon", "cryotherapy beauty", "infrared sauna spa",
        "couples spa package", "prenatal massage near me", "teen facial service",
        "men's waxing salon", "beard grooming barber", "natural hair salon",
        "nail art studio", "gel nail removal near me", "hair extension specialist",
    ],
    "Chiropractor": [
        "chiropractor near me", "back pain treatment", "spinal adjustment",
        "sports chiropractor", "neck pain relief", "sciatica treatment",
        "posture correction", "chiropractic clinic", "headache treatment chiropractor",
        "pregnancy chiropractor", "pediatric chiropractor",
        "best chiropractor near me", "affordable chiropractic care",
        "chiropractic adjustment", "lower back pain chiropractor",
        "disc herniation treatment", "whiplash treatment chiropractor",
        "TMJ chiropractor", "chiropractic wellness centre", "spinal decompression therapy",
        "shoulder pain chiropractor", "hip pain chiropractor",
        # V5.14: Doubled keywords
        "knee pain chiropractic", "foot pain chiropractor", "pinched nerve chiropractic",
        "scoliosis chiropractor", "chronic pain chiropractic", "tension headache chiropractor",
        "workplace injury chiropractor", "car accident chiropractor", "sports injury chiropractor",
        "activator chiropractic technique", "manual chiropractic adjustment", "dry needling chiropractor",
        "NDIS chiropractor", "chiropractic massage combo", "chiropractic x-ray near me",
        "vertebral subluxation treatment", "chiropractic family care", "infant chiropractic",
        "sacroiliac joint chiropractor", "functional neurology chiropractor", "corrective chiropractic",
    ],
    "Veterinarian": [
        "vet near me", "emergency vet", "pet vaccination",
        "dog grooming", "cat vet", "animal hospital",
        "pet dental care", "pet surgery", "veterinary clinic",
        "exotic animal vet", "pet microchipping", "puppy health check",
        "best vet near me", "affordable vet clinic", "24 hour emergency vet",
        "mobile vet service", "pet desexing", "senior pet care vet",
        "avian vet", "reptile vet", "pet allergy treatment",
        "veterinary specialist", "pet ultrasound", "dog behaviorist vet",
        # V5.14: Doubled keywords
        "dog vet near me", "cat vet near me", "rabbit vet near me",
        "guinea pig vet", "fish vet near me", "farm animal vet",
        "horse vet near me", "cattle vet service", "pet blood test vet",
        "pet X-ray service", "vet pain management", "pet oncology vet",
        "pet dermatology specialist", "ophthalmology vet", "pet cardiology vet",
        "vet physiotherapy", "pet rehabilitation vet", "acupuncture vet",
        "holistic vet near me", "pet grief support vet", "animal euthanasia vet",
    ],
    "Insurance Agent": [
        "insurance broker near me", "car insurance quote", "home insurance",
        "life insurance advisor", "business insurance", "health insurance broker",
        "income protection insurance", "travel insurance", "landlord insurance",
        "professional indemnity insurance", "workers compensation insurance",
        "best insurance broker", "affordable insurance quotes", "insurance agent near me",
        "commercial vehicle insurance", "public liability insurance",
        "cyber insurance broker", "strata insurance", "trade insurance",
        "fleet insurance broker", "insurance comparison service",
        "general insurance broker", "risk management insurance",
        # V5.14: Doubled keywords
        "building and contents insurance", "marine insurance broker", "aviation insurance",
        "farm insurance broker", "event insurance broker", "group life insurance",
        "key person insurance", "business interruption insurance", "directors officers insurance",
        "management liability insurance", "construction insurance broker", "product liability insurance",
        "engineering insurance", "agribusiness insurance", "SMSF insurance",
        "funeral insurance advisor", "pet insurance broker", "disability insurance",
        "rural property insurance", "hospitality insurance broker", "NFP insurance",
    ],
    "Financial Advisor": [
        "financial planner near me", "investment advisor", "retirement planning",
        "wealth management", "superannuation advice", "mortgage broker",
        "financial planning service", "estate planning advisor", "debt consolidation",
        "self managed super fund advisor", "tax effective investment",
        "best financial advisor near me", "certified financial planner",
        "independent financial advisor", "pension advisor", "portfolio management",
        "financial coach", "business financial planning", "insurance planning advisor",
        "property investment advisor", "succession planning advisor",
        "fee only financial planner", "first home buyer financial advisor",
        # V5.14: Doubled keywords
        "divorce financial planning", "aged care financial advice", "redundancy financial planning",
        "ethical investment advisor", "ESG investment advisor", "impact investing advisor",
        "socially responsible investing", "robo advisor alternative", "shares investment advisor",
        "education savings plan advisor", "child trust fund advisor", "expat financial planning",
        "gig economy financial advisor", "small business exit planning", "employee share plan advisor",
        "share portfolio management", "term deposit advice", "annuity planning advisor",
        "financial hardship advisor", "debt management plan", "bankruptcy financial advice",
    ],
    "Photographer": [
        "wedding photographer", "portrait photographer", "commercial photographer",
        "real estate photographer", "event photographer", "newborn photographer",
        "family photographer", "headshot photographer", "product photography",
        "corporate photographer", "drone photographer",
        "best photographer near me", "affordable photography services",
        "graduation photographer", "maternity photographer", "pet photographer",
        "food photographer", "fashion photographer", "architectural photographer",
        "photo studio near me", "ecommerce product photography",
        "sports photographer", "school photographer",
        # V5.14: Doubled keywords
        "engagement photographer", "anniversary photographer", "boudoir photographer",
        "lifestyle photographer", "documentary photographer", "street photographer",
        "concert photographer", "music band photographer", "automotive photographer",
        "construction site photographer", "industrial photographer", "medical photographer",
        "underwater photographer", "360 degree photographer", "virtual tour photographer",
        "photo editing service", "retouching photographer", "composite photography",
        "photo booth rental", "photo walk photographer", "photography workshop",
    ],
    "Landscaping": [
        "landscaper near me", "garden design service", "lawn mowing service",
        "tree removal", "irrigation installation", "retaining wall builder",
        "landscape architect", "garden maintenance", "artificial turf installer",
        "paving contractor", "outdoor living design", "hedge trimming service",
        "best landscaper near me", "affordable landscaping", "garden makeover",
        "pool landscaping", "native garden design", "commercial landscaping",
        "stump grinding service", "mulching service", "garden lighting installation",
        "deck and pergola builder", "vertical garden installer",
        # V5.14: Doubled keywords
        "garden clean up service", "turf laying service", "lawn care service",
        "garden edging service", "lawn fertiliser treatment", "weed removal garden",
        "tree pruning service", "arborist near me", "palm tree removal",
        "water feature installation", "raised garden bed installation", "succulent garden design",
        "kitchen garden design", "outdoor kitchen landscaper", "bbq area landscaping",
        "fire pit landscaping", "zen garden design", "sloping block landscaping",
        "council approved landscaping", "body corporate garden maintenance", "strata garden service",
    ],
    "HVAC": [
        "air conditioning installation", "heating repair", "HVAC contractor",
        "ducted air conditioning", "split system installation", "furnace repair",
        "commercial HVAC", "air conditioning service", "ventilation system",
        "heat pump installer", "evaporative cooling", "air duct cleaning",
        "best HVAC contractor near me", "affordable air conditioning",
        "refrigerated cooling installation", "gas heating installation",
        "underfloor heating", "air conditioning maintenance plan",
        "commercial refrigeration", "HVAC energy audit", "zone control system",
        "hydronic heating installer", "air purification system",
        # V5.14: Doubled keywords
        "reverse cycle air conditioner", "portable air conditioner install",
        "wall mounted split system", "multi split air conditioning", "cassette air conditioner",
        "industrial ventilation system", "kitchen exhaust fan installation",
        "bathroom exhaust fan install", "server room cooling", "cold room installation",
        "refrigerant regas service", "AC thermostat replacement", "AC compressor repair",
        "ducted gas heating service", "gas log fire installation", "pellet heater installer",
        "zoning system installation", "HVAC BMS control system", "air balancing service",
        "HVAC design consultant", "building HVAC compliance", "AC noise problem repair",
    ],
    "Roofing": [
        "roof repair near me", "roofing contractor", "roof replacement",
        "metal roofing", "tile roof repair", "gutter installation",
        "roof restoration", "commercial roofing", "roof leak repair",
        "colorbond roofing", "roof painting", "roof inspection service",
        "best roofer near me", "affordable roof repair", "flat roof specialist",
        "gutter guard installation", "skylight installation", "roof ventilation",
        "emergency roof repair", "fascia and soffit repair", "roof cleaning service",
        "asbestos roof removal", "terracotta roof restoration",
        # V5.14: Doubled keywords
        "slate roof repair", "zincalume roofing", "polycarbonate roof installation",
        "industrial roof replacement", "school roof repair", "stormwater management roofing",
        "gutter cleaning service", "leaf guard gutters", "gutter replacement near me",
        "roof membrane waterproofing", "heritage slate roofer", "concrete tile roofer",
        "ridge capping repair", "valley iron replacement", "roof truss repair",
        "solar roof installation", "green roof installation", "translucent roofing sheets",
        "factory roof maintenance", "church roof restoration", "roof hatch installation",
    ],
    "Pest Control": [
        "pest control near me", "termite inspection", "cockroach treatment",
        "rodent control", "bed bug treatment", "ant control service",
        "spider treatment", "commercial pest control", "pre purchase pest inspection",
        "possum removal", "wasp nest removal", "flea treatment",
        "best pest control near me", "affordable pest treatment",
        "termite barrier installation", "mosquito control", "bird proofing service",
        "silverfish treatment", "timber pest inspection", "eco friendly pest control",
        "fumigation service", "integrated pest management", "annual pest control plan",
        # V5.14: Doubled keywords
        "snake removal service", "fly control service", "mite treatment service",
        "carpet beetle treatment", "clothes moth treatment", "dermestid beetle control",
        "grain weevil treatment", "stored product pest control", "whitefly treatment",
        "restaurant pest control", "hotel pest management", "warehouse pest control",
        "strata pest control", "body corporate pest inspection", "real estate pest report",
        "thermal termite inspection", "termite monitoring system", "termite bait station",
        "soil treatment termites", "chemical termite barrier", "pre construction termite treatment",
    ],
    "Cleaning Service": [
        "house cleaning service", "commercial cleaning", "carpet cleaning",
        "end of lease cleaning", "office cleaning service", "window cleaning",
        "deep cleaning service", "pressure washing", "tile and grout cleaning",
        "upholstery cleaning", "regular house cleaning", "spring cleaning service",
        "best cleaning service near me", "affordable house cleaning",
        "strata cleaning service", "medical facility cleaning", "gym cleaning service",
        "after construction cleaning", "airbnb cleaning service", "oven cleaning service",
        "blind cleaning service", "school cleaning contractor", "warehouse cleaning",
        # V5.14: Doubled keywords
        "bond cleaning service", "vacate cleaning service", "move in cleaning service",
        "covid cleaning service", "biohazard cleaning", "trauma cleaning service",
        "high pressure cleaning", "concrete cleaning service", "driveway cleaning service",
        "solar panel cleaning", "gutter cleaning service", "industrial cleaning contractor",
        "food factory cleaning", "restaurant kitchen deep clean", "childcare cleaning service",
        "aged care cleaning", "hospital cleaning service", "church cleaning contractor",
        "retail cleaning service", "shopping centre cleaning", "car park cleaning service",
    ],
    "IT Services": [
        "IT support near me", "managed IT services", "computer repair",
        "network setup", "cybersecurity services", "cloud computing solutions",
        "IT consulting", "data recovery service", "business IT support",
        "VoIP phone systems", "server maintenance", "IT helpdesk outsourcing",
        "best IT support near me", "affordable managed IT", "IT security audit",
        "Microsoft 365 setup", "backup and disaster recovery", "wireless network setup",
        "website hosting service", "IT infrastructure management",
        "remote IT support", "IT project management", "software development company",
        # V5.14: Doubled keywords
        "small business IT support", "cloud migration service", "Google Workspace setup",
        "IT outsourcing company", "network security audit", "CCTV IT integration",
        "endpoint protection service", "firewall setup company", "email spam filtering",
        "business continuity IT", "dark web monitoring service", "compliance IT consulting",
        "hardware procurement IT", "SD-WAN solution provider", "virtualisation services",
        "Azure cloud services", "AWS managed services", "Salesforce IT partner",
        "IT onboarding service", "IT documentation company", "phishing simulation service",
    ],
    "Marketing Agency": [
        "digital marketing agency", "SEO services", "social media marketing",
        "PPC management", "content marketing agency", "web design agency",
        "branding agency", "email marketing service", "Google Ads management",
        "video production agency", "PR agency", "lead generation service",
        "best marketing agency near me", "affordable digital marketing",
        "local SEO services", "ecommerce marketing agency", "Facebook Ads agency",
        "marketing strategy consultant", "conversion rate optimization",
        "influencer marketing agency", "LinkedIn marketing service",
        "reputation management agency", "marketing automation service",
        # V5.14: Doubled keywords
        "TikTok marketing agency", "YouTube advertising agency", "programmatic advertising",
        "growth marketing agency", "performance marketing agency", "B2B marketing agency",
        "startup marketing agency", "healthcare marketing agency", "legal marketing agency",
        "trade marketing agency", "events marketing company", "direct mail marketing",
        "SMS marketing service", "chatbot marketing agency", "account based marketing",
        "Amazon advertising agency", "Shopify marketing agency", "keyword research service",
        "white label marketing agency", "nearshore marketing agency", "marketing analytics service",
    ],
    "Construction": [
        "home builder near me", "construction company", "renovation contractor",
        "commercial construction", "custom home builder", "bathroom renovation",
        "kitchen renovation", "extension builder", "granny flat builder",
        "project home builder", "demolition contractor", "concrete contractor",
        "best builder near me", "affordable home renovation", "new home construction",
        "duplex builder", "townhouse builder", "shopfitting contractor",
        "structural steel builder", "civil construction company",
        "industrial construction", "site preparation contractor", "formwork contractor",
        # V5.14: Doubled keywords
        "heritage restoration builder", "pool construction company", "garage builder near me",
        "retaining wall construction", "underpinning specialist", "house raising specialist",
        "owner builder assistance", "knockdown rebuild company", "modular home builder",
        "tiny home builder", "eco-friendly builder", "passive house builder",
        "commercial fitout contractor", "medical fitout builder", "school construction contractor",
        "aged care construction", "hotel construction company", "warehouse builder",
        "earthworks contractor", "drainage contractor", "building surveyor",
    ],
    "Architecture": [
        "architect near me", "residential architect", "commercial architect",
        "interior designer", "building designer", "sustainable architecture",
        "heritage architect", "architectural drafting", "house design service",
        "landscape architect", "3D architectural rendering",
        "best architect near me", "affordable architectural services",
        "dual occupancy architect", "renovation architect", "passive house architect",
        "town planning consultant", "development application architect",
        "multi storey architect", "aged care facility architect",
        "restaurant fit out designer", "retail design architect",
        # V5.14: Doubled keywords
        "custom home design architect", "knockdown rebuild architect", "granny flat architect",
        "industrial building architect", "warehouse conversion architect", "adaptive reuse architect",
        "education facility architect", "healthcare building architect", "hospitality architect",
        "mixed use development architect", "high rise architect", "boutique developer architect",
        "facade design architect", "building extension architect", "rezoning application architect",
        "BIM architect", "parametric design architect", "virtual reality architecture",
        "green building architect", "NABERS rating architect", "NCC compliance architect",
    ],
    "Physiotherapy": [
        "physiotherapist near me", "sports physio", "back pain physiotherapy",
        "post surgery rehabilitation", "neck pain treatment physio",
        "shoulder physio", "knee rehabilitation", "workplace injury physio",
        "dry needling treatment", "hydrotherapy", "exercise physiologist",
        "best physio near me", "affordable physiotherapy", "pelvic floor physio",
        "hand therapy physiotherapist", "vestibular physiotherapy",
        "clinical pilates physio", "paediatric physiotherapy",
        "aged care physiotherapy", "telehealth physiotherapy",
        "chronic pain physiotherapist", "running injury physio",
        # V5.14: Doubled keywords
        "ankle sprain physio", "hip replacement rehab physio", "ACL rehabilitation physio",
        "frozen shoulder treatment", "rotator cuff physio", "tennis elbow treatment",
        "plantar fasciitis physio", "sciatica physiotherapy", "concussion rehabilitation",
        "neurological physiotherapy", "stroke rehabilitation physio", "NDIS physiotherapy",
        "pre surgery physio", "post natal physiotherapy", "lymphoedema physio",
        "oncology physiotherapy", "cardiorespiratory physio", "balance and falls physio",
        "pilates injury prevention", "sports taping physio", "ergonomic assessment physio",
    ],
    "Pharmacy": [
        "pharmacy near me", "compounding pharmacy", "online pharmacy",
        "late night pharmacy", "prescription delivery", "vaccination pharmacy",
        "travel health clinic pharmacy", "medication management",
        "health screening pharmacy", "weight management pharmacy",
        "best pharmacy near me", "24 hour pharmacy", "discount pharmacy",
        "diabetes management pharmacy", "blister pack pharmacy",
        "naturopathic pharmacy", "veterinary compounding pharmacy",
        "sleep apnea pharmacy", "mobility aids pharmacy",
        "hormone compounding pharmacy", "pain management pharmacy",
        # V5.14: Doubled keywords
        "chemist near me", "discount chemist", "script pharmacy",
        "PBS medication pharmacy", "pharmacy delivery service", "flu vaccine pharmacy",
        "COVID vaccine pharmacy", "baby formula pharmacy", "sports nutrition pharmacy",
        "wound care pharmacy", "stoma care pharmacy", "medsafe compounding",
        "fertility medication pharmacy", "HRT pharmacy", "mental health medication pharmacy",
        "antibiotic prescription pharmacy", "healthcare product pharmacy", "pharmacy reward program",
        "pharmacy dispensary service", "accredited pharmacist", "medication review pharmacy",
    ],
    # ── V5.4: 25 NEW INDUSTRIES ──────────────────────────────────────────────
    "Wedding Planner / Event Planner": [
        "wedding planner near me", "event planning services", "wedding coordinator",
        "corporate event planner", "party planner", "wedding venue coordinator",
        "destination wedding planner", "event management company", "wedding stylist",
        "birthday party planner", "fundraiser event planner", "conference organizer",
        "bridal consultant", "wedding day coordinator", "event decorator",
        "engagement party planner", "anniversary event planner", "gala event planner",
        "outdoor wedding planner", "wedding planning services",
        # V5.14: Doubled keywords
        "micro wedding planner", "elopement coordinator", "luxury wedding planner",
        "beach wedding planner", "garden wedding planner", "winery wedding planner",
        "LGBTQ wedding planner", "multicultural wedding planner", "baby shower planner",
        "hen's night planner", "bucks night planner", "school formal organizer",
        "product launch event planner", "trade show event organizer", "awards night organizer",
        "team building event planner", "virtual event planner", "hybrid event organizer",
        "charity gala planner", "sports event organizer", "festival event management",
    ],
    "Tattoo Artist / Body Art": [
        "tattoo shop near me", "custom tattoo artist", "tattoo studio",
        "fine line tattoo artist", "traditional tattoo shop", "realism tattoo artist",
        "tattoo removal service", "watercolor tattoo artist", "minimalist tattoo",
        "portrait tattoo specialist", "sleeve tattoo artist", "japanese tattoo artist",
        "tattoo parlor", "cover up tattoo specialist", "body piercing studio",
        "blackwork tattoo artist", "geometric tattoo", "best tattoo artist near me",
    ],
    "Florist / Flower Shop": [
        "florist near me", "flower delivery service", "wedding florist",
        "funeral flowers delivery", "flower shop", "event floral arrangements",
        "custom bouquet delivery", "same day flower delivery", "floral designer",
        "corporate flower arrangements", "flower subscription service", "dried flower arrangements",
        "bridal bouquet florist", "sympathy flowers delivery", "tropical flower arrangements",
        "flower workshop classes", "wholesale flowers", "seasonal flower arrangements",
    ],
    "Baker / Bakery": [
        "bakery near me", "custom cake shop", "wedding cake baker",
        "artisan bread bakery", "cupcake shop", "gluten free bakery",
        "birthday cake order", "pastry shop", "sourdough bakery",
        "cake decorator near me", "French pastry shop", "vegan bakery",
        "wholesale bakery", "specialty cake shop", "donut shop",
        "patisserie near me", "cake delivery service", "best bakery near me",
    ],
    "Caterer / Catering": [
        "catering service near me", "wedding catering", "corporate catering",
        "event catering company", "BBQ catering", "buffet catering service",
        "private chef catering", "office lunch catering", "cocktail party catering",
        "food truck catering", "halal catering service", "vegan catering",
        "funeral catering service", "breakfast catering", "finger food catering",
        "outdoor event catering", "gourmet catering service", "affordable catering near me",
    ],
    "Personal Trainer": [
        "personal trainer near me", "online personal training", "fitness coach",
        "weight loss personal trainer", "strength training coach", "HIIT trainer",
        "mobile personal trainer", "group fitness trainer", "sports conditioning coach",
        "body transformation trainer", "prenatal fitness trainer", "senior fitness trainer",
        "CrossFit coach", "nutrition and fitness coach", "functional training specialist",
        "private gym trainer", "certified personal trainer", "best personal trainer near me",
    ],
    "Yoga / Pilates Studio": [
        "yoga studio near me", "pilates classes", "hot yoga studio",
        "beginner yoga classes", "prenatal yoga", "aerial yoga studio",
        "reformer pilates near me", "yoga teacher training", "yin yoga classes",
        "corporate yoga instructor", "private yoga lessons", "vinyasa yoga studio",
        "mat pilates classes", "yoga retreat center", "meditation and yoga studio",
        "kids yoga classes", "online yoga classes", "best yoga studio near me",
    ],
    "Massage Therapist": [
        "massage therapist near me", "deep tissue massage", "sports massage therapy",
        "remedial massage", "Swedish massage", "pregnancy massage",
        "lymphatic drainage massage", "hot stone massage", "myotherapy near me",
        "mobile massage service", "couples massage", "relaxation massage",
        "trigger point therapy", "aromatherapy massage", "Thai massage near me",
        "therapeutic massage clinic", "back pain massage", "best massage therapist near me",
    ],
    "Interior Designer": [
        "interior designer near me", "home interior design", "commercial interior design",
        "kitchen design consultant", "bathroom renovation designer", "office interior designer",
        "residential interior styling", "modern interior design", "luxury interior designer",
        "sustainable interior design", "color consultation service", "space planning consultant",
        "interior decorator near me", "home staging service", "furniture selection consultant",
        "restaurant interior design", "hotel interior design", "affordable interior designer",
    ],
    "Web Developer / Web Design": [
        "web developer near me", "website design service", "ecommerce website development",
        "WordPress developer", "Shopify developer", "custom web application",
        "responsive web design", "SEO web design", "landing page design",
        "web development agency", "mobile app developer", "UI UX design service",
        "website maintenance service", "website redesign", "small business website",
        "React developer", "full stack developer", "affordable web design",
        # V5.14: Doubled keywords
        "Vue.js developer", "Next.js developer", "Node.js developer",
        "Laravel developer", "Django web developer", "Ruby on Rails developer",
        "headless CMS developer", "Webflow developer", "Squarespace web designer",
        "website speed optimization", "website accessibility audit", "ADA compliant website",
        "progressive web app developer", "API development service", "SaaS web developer",
        "membership website developer", "booking system developer", "custom CRM developer",
        "website security service", "SSL certificate setup", "website migration service",
    ],
    "Graphic Designer": [
        "graphic designer near me", "logo design service", "brand identity design",
        "print design service", "packaging design", "marketing material design",
        "social media graphic designer", "business card design", "brochure design service",
        "infographic designer", "illustration service", "book cover design",
        "banner design service", "freelance graphic designer", "corporate branding agency",
        "signage design", "menu design service", "affordable graphic design",
    ],
    "Copywriter / Content Writer": [
        "copywriter near me", "content writing service", "SEO copywriting",
        "website content writer", "blog writing service", "advertising copywriter",
        "product description writer", "email copywriter", "social media content writer",
        "technical writer", "press release writing", "brand storytelling",
        "freelance copywriter", "conversion copywriting", "content marketing agency",
        "scriptwriter for business", "ghostwriter", "B2B copywriting service",
    ],
    "Tutor / Education": [
        "tutor near me", "math tutor", "English tutor",
        "online tutoring service", "SAT prep tutor", "science tutor",
        "reading tutor for kids", "university tutor", "language tutor",
        "music tutor", "STEM tutoring", "test preparation tutor",
        "special needs tutor", "private tutor", "homework help service",
        "study skills coach", "academic coaching", "best tutor near me",
    ],
    "Music Teacher / Music School": [
        "music lessons near me", "piano teacher", "guitar lessons",
        "singing lessons", "violin teacher", "drum lessons near me",
        "music school", "private music tutor", "online music lessons",
        "music theory classes", "band coaching", "music production lessons",
        "kids music classes", "adult music lessons", "songwriting workshop",
        "jazz music lessons", "classical music teacher", "best music school near me",
    ],
    "Driving School": [
        "driving school near me", "driving lessons", "learner driver instructor",
        "automatic driving lessons", "defensive driving course", "driving test preparation",
        "truck driving school", "motorcycle riding lessons", "driving instructor",
        "intensive driving course", "driving refresher course", "senior driver assessment",
        "P plate driving lessons", "driving school for teens", "manual driving lessons",
        "road test preparation", "best driving school near me", "affordable driving lessons",
    ],
    "Pet Grooming": [
        "pet grooming near me", "dog grooming salon", "cat grooming service",
        "mobile pet grooming", "puppy grooming", "dog bathing service",
        "pet nail trimming", "dog haircut near me", "luxury pet grooming",
        "breed specific grooming", "pet spa near me", "hypoallergenic dog grooming",
        "show dog grooming", "pet deshedding service", "flea treatment grooming",
        "senior pet grooming", "large dog grooming", "best pet groomer near me",
    ],
    "Locksmith": [
        "locksmith near me", "emergency locksmith", "24 hour locksmith",
        "car locksmith", "residential locksmith", "commercial locksmith",
        "lock change service", "lockout service", "master key system",
        "safe locksmith", "smart lock installation", "lock repair service",
        "key cutting near me", "garage door lock", "deadbolt installation",
        "automotive locksmith", "rekeying service", "affordable locksmith near me",
    ],
    "Moving Company": [
        "moving company near me", "local movers", "interstate removalist",
        "office relocation service", "furniture removalist", "packing service",
        "piano moving service", "storage and moving", "commercial moving company",
        "last minute movers", "small moves specialist", "long distance moving",
        "apartment movers", "house moving service", "moving truck hire",
        "senior moving service", "corporate relocation", "affordable movers near me",
    ],
    "Printing Service": [
        "printing service near me", "business card printing", "banner printing",
        "flyer printing service", "poster printing", "sticker printing",
        "t-shirt printing", "booklet printing", "large format printing",
        "custom printing service", "digital printing near me", "offset printing",
        "brochure printing", "invitation printing", "signage printing",
        "photo printing service", "canvas printing", "same day printing service",
    ],
    "Optometrist / Eye Care": [
        "optometrist near me", "eye exam near me", "prescription glasses",
        "contact lens fitting", "children's eye test", "eye care clinic",
        "optical shop", "progressive lenses", "eye health check",
        "dry eye treatment", "glaucoma screening", "macular degeneration test",
        "sports vision specialist", "bulk billed eye test", "designer eyeglasses",
        "vision therapy", "diabetic eye screening", "best optometrist near me",
    ],
    "Podiatrist": [
        "podiatrist near me", "foot doctor", "ingrown toenail treatment",
        "plantar fasciitis treatment", "custom orthotics", "diabetic foot care",
        "sports podiatry", "heel pain treatment", "bunion treatment",
        "children's podiatrist", "foot pain specialist", "toenail fungus treatment",
        "flat feet treatment", "running injury podiatrist", "biomechanical assessment",
        "podiatric surgery", "foot care clinic", "best podiatrist near me",
    ],
    "Dermatologist": [
        "dermatologist near me", "skin specialist", "acne treatment clinic",
        "skin cancer check", "mole removal", "eczema treatment",
        "psoriasis specialist", "cosmetic dermatology", "anti aging skin treatment",
        "laser skin treatment", "rosacea treatment", "skin biopsy clinic",
        "dermatology clinic", "pediatric dermatologist", "hair loss treatment",
        "skin allergy specialist", "botox dermatologist", "best dermatologist near me",
    ],
    "Home Inspector": [
        "home inspector near me", "building inspection", "pre purchase inspection",
        "pest inspection service", "property inspection report", "new home inspection",
        "commercial building inspection", "pool inspection", "roof inspection service",
        "asbestos inspection", "mold inspection", "home energy audit",
        "structural inspection", "pre sale building report", "strata inspection",
        "termite inspection", "building and pest inspection", "best home inspector near me",
    ],
    "Painter / Decorator": [
        "house painter near me", "interior painting service", "exterior house painting",
        "commercial painter", "residential painting", "wallpaper installation",
        "spray painting service", "cabinet painting", "deck staining",
        "mural artist", "office painting service", "roof painting",
        "fence painting", "texture coating", "color consulting painter",
        "heritage restoration painter", "epoxy floor coating", "affordable painter near me",
    ],
    "Solar Panel Installation": [
        "solar panel installer near me", "solar energy system", "residential solar panels",
        "commercial solar installation", "solar battery storage", "solar power system",
        "solar panel quotes", "rooftop solar panels", "off grid solar system",
        "solar hot water system", "solar inverter installation", "solar panel cleaning service",
        "solar financing options", "solar panel repair", "EV charger installation",
        "solar energy consultant", "green energy solutions", "best solar installer near me",
        # V5.14: Doubled keywords
        "STC solar rebate installer", "feed in tariff solar", "virtual power plant solar",
        "Tesla Powerwall installer", "LG Chem battery installer", "SolarEdge installer",
        "Enphase microinverter installer", "Fronius inverter installer", "Sungrow installer",
        "solar monitoring system", "smart meter solar installer", "solar ground mount",
        "solar carport installation", "agrivoltaic solar", "floating solar installer",
        "solar for business", "solar for farms", "solar for schools",
        "solar finance no deposit", "solar power purchase agreement", "solar leasing service",
    ],
    # Phase 2 (2026-05-05): Multi-vertical AU industry. 128 keywords across four
    # sub-niches (32 each): Massage Chairs, Safety Equipment, Online Bicycle
    # Stores, Designer Clothing Suppliers. Sub-niche grouping is preserved by
    # interleaving so SEMrush/SerpAPI scans hit all four within the first 32
    # probes (see the round-robin layout below).
    "Massage Chairs, Safety Equipment, Online Bicycle Stores & Designer Clothing (AU)": [
        # ── Massage Chairs (32) ─────────────────────────────────────────────
        "massage chair", "electric massage chair", "full body massage chair",
        "shiatsu massage chair", "recliner massage chair", "zero gravity massage chair",
        "4d massage chair", "luxury massage chair", "massage chair sale",
        "massage chair near me", "best massage chair", "premium massage chair",
        "commercial massage chair", "massage chair store", "buy massage chair",
        "massage chair online", "massage chair sydney", "massage chair melbourne",
        "massage chair brisbane", "massage chair perth", "massage chair retailer",
        "massage chair supplier", "massage chair showroom", "massage chair finance",
        "massage chair warranty", "massage chair dealer", "massage chair distributor",
        "massage chair wholesale", "professional massage chair", "home massage chair australia",
        "massage chair clearance", "heated massage chair",
        # ── Safety Equipment (32) ───────────────────────────────────────────
        "safety equipment supplier", "ppe supplier", "personal protective equipment",
        "hi vis vest supplier", "work boots supplier", "safety glasses supplier",
        "hard hat supplier", "safety harness supplier", "fall arrest equipment",
        "respiratory mask supplier", "hearing protection supplier", "safety gloves wholesale",
        "work gloves supplier", "safety boots supplier", "work wear supplier",
        "hi vis clothing supplier", "workplace safety equipment", "industrial safety equipment",
        "construction safety gear", "safety equipment store", "ppe online australia",
        "buy safety equipment", "ppe wholesale", "safety gear sydney",
        "safety gear melbourne", "safety gear brisbane", "ppe perth",
        "safety equipment near me", "workplace ppe supplier", "ppe distributor",
        "safety equipment retailer", "safety equipment warehouse",
        # ── Online Bicycle Stores (32) ──────────────────────────────────────
        "online bicycle store", "online bike shop", "electric bike online",
        "ebike online retailer", "mountain bike online", "road bike online",
        "kids bike online", "bicycle online sale", "bike accessories online",
        "online cycling store", "bicycle parts online", "bike helmet online",
        "bike clothing online", "online bike shop australia", "online bike retailer",
        "bike marketplace australia", "bicycle wholesale online", "online bike accessories store",
        "ebike retailer", "mountain bike retailer", "road bike supplier",
        "bike component online", "bicycle dealer", "online bike clearance",
        "ebike commuter online", "bike rack online", "electric bicycle store",
        "online bike parts australia", "mtb online store", "performance bike online",
        "premium bicycle online", "online bike service",
        # ── Designer Clothing Suppliers (32) ────────────────────────────────
        "designer clothing supplier", "designer fashion supplier", "luxury clothing wholesale",
        "designer apparel supplier", "designer wear supplier", "premium clothing supplier",
        "boutique clothing supplier", "designer fashion wholesale", "designer dress supplier",
        "australian designer clothing", "designer label supplier", "designer womenswear supplier",
        "designer menswear supplier", "australian fashion supplier", "designer clothing wholesaler",
        "luxury fashion supplier", "designer brand supplier", "designer apparel wholesaler",
        "high end clothing supplier", "designer streetwear supplier", "designer accessories supplier",
        "designer kidswear supplier", "australian boutique supplier", "designer fashion distributor",
        "premium fashion supplier", "designer brand wholesale", "contemporary fashion supplier",
        "australian luxury fashion", "designer fashion sydney", "designer fashion melbourne",
        "designer fashion brisbane", "designer fashion perth",
    ],
}

# ── NEW CITY EXPANSION KEYWORDS ───────────────────────────────────────────────
# Additional industry keyword sets for future city-expansion use.
# NOT included in the /industries dropdown (only INDUSTRY_KEYWORDS.keys() is used there).
# Add entries here when expanding to new markets; wire them into INDUSTRY_KEYWORDS later.
NEW_CITY_EXPANSION_KEYWORDS = {
    "Mortgage Broker": [
        "mortgage broker near me", "home loan broker", "mortgage broker",
        "best mortgage broker", "first home buyer mortgage broker", "investment property mortgage broker",
        "refinance mortgage broker", "commercial mortgage broker", "low deposit home loan broker",
        "mortgage pre approval broker", "home loan comparison broker", "construction loan broker",
        "self employed mortgage broker", "bridging loan broker", "bad credit mortgage broker",
        "mortgage advice service", "owner occupier home loan broker", "fixed rate home loan broker",
        "variable rate home loan broker", "home loan refinancing specialist", "mortgage broker free consultation",
        "best home loan rates broker", "property investment loan broker", "mortgage broker for first home buyers",
    ],

    "Conveyancer": [
        "conveyancer near me", "property conveyancer", "best conveyancer",
        "cheap conveyancing", "fixed fee conveyancing", "residential conveyancing lawyer",
        "house purchase conveyancer", "property settlement agent", "conveyancing quote",
        "online conveyancing service", "first home buyer conveyancer", "contract review conveyancer",
        "property sale conveyancer", "commercial conveyancing", "off the plan conveyancer",
        "auction contract review", "urgent conveyancing service", "property transfer conveyancing",
        "title transfer conveyancer", "family transfer conveyancing", "conveyancer for buying house",
        "conveyancer for selling house", "settlement conveyancer", "local conveyancing specialist",
    ],

    "Carpet Cleaning": [
        "carpet cleaning near me", "professional carpet cleaning", "steam carpet cleaning",
        "end of lease carpet cleaning", "cheap carpet cleaning", "same day carpet cleaning",
        "pet stain carpet cleaning", "carpet shampoo service", "upholstery and carpet cleaning",
        "commercial carpet cleaning", "dry carpet cleaning", "carpet sanitising service",
        "rug and carpet cleaning", "mattress and carpet cleaning", "stain removal carpet cleaning",
        "odour removal carpet cleaning", "carpet cleaning quote", "best carpet cleaners",
        "office carpet cleaning", "residential carpet cleaning", "carpet cleaning specials",
        "water damage carpet cleaning", "deep carpet cleaning service", "affordable carpet cleaning",
    ],

    "Bathroom Renovation": [
        "bathroom renovation near me", "bathroom renovator", "bathroom remodeling contractor",
        "small bathroom renovation", "luxury bathroom renovation", "bathroom renovation quote",
        "bathroom renovation cost", "ensuite renovation", "laundry and bathroom renovation",
        "bathroom design and renovation", "custom bathroom renovation", "budget bathroom renovation",
        "bathroom makeover specialist", "walk in shower renovation", "bathroom tiling renovation",
        "bathroom waterproofing renovation", "bathroom vanity installation", "bathroom demolition and renovation",
        "complete bathroom renovation", "modern bathroom renovation", "bathroom renovation company",
        "local bathroom renovators", "bathroom upgrade specialist", "bathroom renovation builder",
    ],

    "Kitchen Renovation": [
        "kitchen renovation near me", "kitchen renovator", "kitchen remodeling contractor",
        "custom kitchen renovation", "kitchen renovation quote", "kitchen renovation cost",
        "luxury kitchen renovation", "small kitchen renovation", "budget kitchen renovation",
        "kitchen cabinets and benchtops", "kitchen makeover specialist", "modern kitchen renovation",
        "open plan kitchen renovation", "kitchen design and renovation", "complete kitchen renovation",
        "kitchen cabinet replacement", "stone benchtop installation", "kitchen island renovation",
        "kitchen refacing service", "apartment kitchen renovation", "kitchen renovation company",
        "local kitchen renovators", "bespoke kitchen renovation", "kitchen renovation builder",
    ],

    "Blinds / Shutters": [
        "blinds installation near me", "plantation shutters near me", "roller blinds installation",
        "outdoor blinds installer", "ziptrak blinds installer", "motorised blinds installation",
        "window shutters installation", "venetian blinds supplier", "custom blinds and shutters",
        "blinds quote", "plantation shutters quote", "best blinds company",
        "affordable blinds installation", "blockout blinds installer", "double roller blinds",
        "security shutters installation", "cafe blinds installer", "alfresco blinds installer",
        "curtains and blinds company", "smart blinds installation", "blinds replacement service",
        "window furnishing specialist", "vertical blinds installation", "local shutters company",
    ],

    "Fencing": [
        "fencing contractor near me", "fence installation", "colorbond fencing",
        "timber fence installation", "pool fencing installer", "aluminium fencing contractor",
        "boundary fence replacement", "privacy fence installer", "security fencing contractor",
        "front yard fencing", "fence repair service", "retaining wall and fencing",
        "rural fencing contractor", "commercial fencing company", "residential fencing company",
        "fencing quote", "automatic gate and fencing", "glass pool fencing",
        "slat fencing installer", "local fence builder", "cheap fencing contractor",
        "best fencing company", "custom fence installation", "gate installation service",
    ],

    "Garage Door Services": [
        "garage door repair near me", "garage door installation", "automatic garage door repair",
        "roller door repair", "garage door motor replacement", "garage door opener installation",
        "garage door spring repair", "sectional garage door installation", "commercial roller door repair",
        "garage door remote replacement", "emergency garage door repair", "garage door servicing",
        "garage door replacement", "panel lift garage door repair", "garage door technician",
        "garage door quote", "garage door maintenance service", "local garage door company",
        "best garage door installer", "garage door automation", "garage door track repair",
        "garage door cable repair", "residential garage door service", "industrial roller door service",
    ],

    "Gutter Cleaning": [
        "gutter cleaning near me", "roof gutter cleaning", "gutter cleaning service",
        "same day gutter cleaning", "commercial gutter cleaning", "residential gutter cleaning",
        "gutter guard installation", "gutter cleaning and repair", "downpipe cleaning service",
        "roof and gutter cleaning", "two storey gutter cleaning", "vacuum gutter cleaning",
        "leaf removal gutter cleaning", "gutter maintenance service", "storm preparation gutter cleaning",
        "gutter inspection service", "cheap gutter cleaning", "best gutter cleaning service",
        "gutter cleaning quote", "roof maintenance service", "blocked gutter cleaning",
        "solar panel and gutter cleaning", "local gutter cleaners", "annual gutter cleaning service",
    ],

    "Property Management": [
        "property management company", "property manager near me", "rental property management",
        "best property management company", "residential property manager", "commercial property management",
        "strata property management", "investment property management", "airbnb property management",
        "full service property management", "property management fees", "landlord property management",
        "tenant placement service", "rental appraisal property manager", "property management quote",
        "local real estate management", "short term rental management", "holiday rental management",
        "vacancy management service", "routine inspection property manager", "property management agency",
        "rent collection service", "leasing and management service", "boutique property management",
    ],

    "Buyers Agent": [
        "buyers agent near me", "buyers advocate", "property buyers agent",
        "best buyers agent", "investment property buyers agent", "first home buyer agent",
        "commercial buyers agent", "buyers agent for auction", "independent buyers agent",
        "buyers agent quote", "property negotiation service", "off market property buyers agent",
        "local buyers agent", "house buying advocate", "best investment buyers agent",
        "buyers agent for apartments", "luxury property buyers agent", "interstate property buyers agent",
        "buyers agent consultation", "real estate buyers advocate", "property search service",
        "buyers agent for first home buyers", "buyers agent for investors", "commercial property acquisition agent",
    ],

    "Psychologist": [
        "psychologist near me", "clinical psychologist", "child psychologist",
        "counselling psychologist", "psychologist for anxiety", "psychologist for depression",
        "trauma psychologist", "relationship psychologist", "couples counselling psychologist",
        "teen psychologist", "adhd psychologist assessment", "autism psychologist assessment",
        "telehealth psychologist", "bulk billing psychologist", "private psychologist",
        "psychology clinic near me", "mental health psychologist", "emdr psychologist",
        "ptsd psychologist", "grief counselling psychologist", "work stress psychologist",
        "psychologist online booking", "best psychologist near me", "local psychology clinic",
    ],

    "Orthodontist": [
        "orthodontist near me", "braces specialist", "invisalign provider",
        "best orthodontist", "adult braces orthodontist", "kids braces orthodontist",
        "clear aligners near me", "emergency orthodontist", "orthodontic consultation",
        "orthodontist payment plans", "ceramic braces specialist", "lingual braces orthodontist",
        "teeth straightening specialist", "orthodontic clinic near me", "retainers orthodontist",
        "jaw alignment orthodontist", "affordable orthodontist", "local invisalign dentist",
        "braces cost consultation", "orthodontic treatment quote", "family orthodontist",
        "early intervention orthodontics", "smile correction specialist", "orthodontic review appointment",
    ],

    "Child Care / Day Care": [
        "child care near me", "day care near me", "early learning centre",
        "childcare centre", "long day care", "preschool near me",
        "kindergarten day care", "day care enrolment", "best childcare near me",
        "childcare vacancies", "infant day care", "toddler day care",
        "school readiness program", "montessori child care", "child care centre fees",
        "local daycare centre", "childcare waitlist", "before and after school care",
        "private childcare centre", "emergency child care", "family day care",
        "early education centre", "child care tour booking", "child care enrol now",
    ],

    "Bookkeeping": [
        "bookkeeper near me", "small business bookkeeping", "xero bookkeeper",
        "bookkeeping services", "bas bookkeeper", "payroll bookkeeping service",
        "registered bas agent", "catch up bookkeeping", "monthly bookkeeping package",
        "outsourced bookkeeping", "bookkeeping and payroll", "virtual bookkeeper",
        "bookkeeping for tradies", "bookkeeping for medical practice", "bookkeeping for ecommerce",
        "accounts payable bookkeeping", "accounts receivable bookkeeping", "gst bookkeeping service",
        "bookkeeping quote", "affordable bookkeeping services", "bookkeeping cleanup service",
        "local bookkeeping firm", "cloud bookkeeping service", "bookkeeping for restaurants",
    ],

    "Cybersecurity Services": [
        "cyber security services", "managed cyber security services", "cyber security company",
        "penetration testing company", "vulnerability assessment services", "soc as a service",
        "managed detection and response", "incident response services", "cyber security audit",
        "cyber security consultant", "network security services", "endpoint security services",
        "email security services", "ransomware protection services", "cyber security for small business",
        "iso 27001 consultant", "essential eight consulting", "dark web monitoring service",
        "security operations centre provider", "cyber security risk assessment", "cyber awareness training for employees",
        "data breach response company", "cloud security services", "cyber security provider near me",
    ],

    "Waterproofing": [
        "waterproofing contractor near me", "bathroom waterproofing", "balcony waterproofing",
        "roof waterproofing", "basement waterproofing", "shower waterproofing service",
        "concrete waterproofing", "external waterproofing contractor", "internal waterproofing service",
        "leaking shower repair", "waterproofing membrane installer", "rising damp treatment",
        "wet area waterproofing", "commercial waterproofing contractor", "residential waterproofing service",
        "waterproofing inspection", "waterproofing quote", "best waterproofing company",
        "retaining wall waterproofing", "planter box waterproofing", "roof leak waterproofing",
        "foundation waterproofing", "waterproofing repair specialist", "local waterproofing contractor",
    ],

    "Security Systems / CCTV": [
        "cctv installation near me", "security camera installation", "home security systems",
        "commercial security systems", "alarm system installation", "monitored alarm installation",
        "intercom installation service", "access control installation", "video doorbell installation",
        "warehouse cctv installation", "office security camera installation", "wireless security camera installer",
        "dahua cctv installer", "hikvision installer", "security system maintenance",
        "cctv repair service", "security system quote", "best cctv company",
        "remote monitoring systems", "smart home security installation", "business alarm systems",
        "after hours security monitoring", "local cctv installer", "ip camera installation",
    ],

    "NDIS Provider": [
        "ndis provider near me", "ndis support services", "ndis disability support",
        "ndis support worker", "ndis personal care services", "ndis respite care",
        "ndis community participation", "ndis transport assistance", "ndis household tasks support",
        "ndis plan management", "ndis support coordination", "ndis therapy services",
        "registered ndis provider", "ndis accommodation support", "sil provider ndis",
        "sta respite ndis", "ndis daily living support", "ndis nursing care",
        "ndis psychosocial support", "ndis provider for children", "local ndis provider",
        "best ndis provider", "ndis service provider quote", "ndis assistance with self care",
    ],

    "Aged Care Services": [
        "aged care services near me", "home care package provider", "in home aged care",
        "private aged care services", "aged care assessment support", "respite care for elderly",
        "personal care for seniors", "dementia care at home", "overnight care for elderly",
        "24 hour home care", "domestic assistance for seniors", "nursing care at home",
        "companionship for elderly", "aged care provider", "home support for seniors",
        "palliative care at home", "allied health aged care", "aged care package management",
        "local aged care provider", "best home care provider", "transport for seniors",
        "meal preparation for elderly", "gardening help for seniors", "post hospital care for elderly",
    ],

    "Skip Bin Hire": [
        "skip bin hire near me", "cheap skip bin hire", "same day skip bin hire",
        "mini skip bin hire", "builders skip bin hire", "green waste skip bin hire",
        "mixed waste skip bin hire", "household rubbish skip hire", "commercial skip bin hire",
        "industrial skip bin hire", "skip bin hire prices", "marrell bin hire",
        "hook bin hire", "walk in skip bin hire", "concrete skip bin hire",
        "soil skip bin hire", "renovation waste skip bin", "office cleanout skip bin",
        "local skip bin company", "best skip bin hire", "weekend skip bin hire",
        "same day rubbish bin hire", "skip bin for deceased estate", "skip bin for moving house",
    ],

    "Pressure Washing / Exterior Cleaning": [
        "pressure washing near me", "house washing service", "driveway pressure cleaning",
        "roof cleaning service", "soft washing service", "exterior house cleaning",
        "commercial pressure cleaning", "concrete pressure washing", "paver cleaning and sealing",
        "deck cleaning service", "solar panel cleaning", "gutter and roof cleaning",
        "mould removal exterior cleaning", "brick cleaning service", "window and exterior cleaning",
        "high pressure cleaning quote", "best pressure washing company", "same day pressure cleaning",
        "warehouse pressure cleaning", "car park pressure cleaning", "body corporate pressure cleaning",
        "facade cleaning service", "local exterior cleaning company", "affordable pressure washing",
    ],

    "Pool Cleaning": [
        "pool cleaning service near me", "pool maintenance service", "green pool cleaning",
        "pool vacuum service", "pool chemical balancing", "salt water pool service",
        "chlorine pool maintenance", "one off pool clean", "weekly pool cleaning service",
        "monthly pool service", "pool equipment servicing", "pool pump repair service",
        "pool filter cleaning", "pool inspection service", "spa and pool maintenance",
        "commercial pool cleaning", "residential pool service", "pool cleaning quote",
        "best pool cleaners near me", "pool opening service", "pool closing service",
        "pool acid wash service", "local pool maintenance company", "pool leak detection service",
    ],

    "Pool Builder": [
        "pool builder near me", "swimming pool builder", "fibreglass pool builder",
        "concrete pool builder", "plunge pool builder", "custom pool builder",
        "luxury pool builder", "pool construction company", "pool design and build",
        "pool installation quote", "inground pool builder", "small backyard pool builder",
        "lap pool builder", "pool and spa builder", "pool renovation builder",
        "pool landscaping and construction", "best pool builders", "local pool construction company",
        "residential pool builder", "commercial pool builder", "pool shell installation",
        "pool excavation contractor", "family pool builder", "outdoor pool designer",
    ],

    "Business Coach": [
        "business coach near me", "small business coach", "executive business coach",
        "business growth coach", "sales coach for business", "leadership coach",
        "business strategy coach", "startup business coach", "marketing coach for small business",
        "business consultant and coach", "profit improvement coach", "accountability coach for business",
        "business coaching program", "local business mentor", "ecommerce business coach",
        "service business coach", "business coach free consultation", "business turnaround consultant",
        "operations coach for business", "team performance coach", "business systems coach",
        "franchise business coach", "best business coach", "business coaching services",
    ],

    "Recruitment Agency": [
        "recruitment agency near me", "staffing agency", "labour recruitment agency",
        "executive search firm", "permanent recruitment agency", "temporary staffing agency",
        "sales recruitment agency", "it recruitment agency", "healthcare recruitment agency",
        "construction recruitment agency", "admin recruitment agency", "finance recruitment agency",
        "marketing recruitment agency", "warehouse staffing agency", "blue collar recruitment",
        "white collar recruitment", "recruitment outsourcing service", "hiring agency for small business",
        "best recruitment agency", "local staffing company", "candidate sourcing service",
        "bulk hiring recruitment", "talent acquisition agency", "recruitment partner",
    ],

    "Labour Hire": [
        "labour hire near me", "temporary labour hire", "construction labour hire",
        "warehouse labour hire", "civil labour hire", "hospitality labour hire",
        "event staff hire", "factory labour hire", "skilled labour hire",
        "general labour hire", "traffic control labour hire", "forklift driver labour hire",
        "trade assistant labour hire", "site labour hire", "casual labour hire",
        "commercial cleaning staff hire", "local labour hire company", "best labour hire agency",
        "same day labour hire", "short term labour hire", "ongoing labour hire",
        "blue collar staffing", "industrial labour hire", "labour hire quote",
    ],

    "Travel Agent": [
        "travel agent near me", "holiday travel agent", "international travel agent",
        "cruise travel agent", "luxury travel agent", "corporate travel management",
        "honeymoon travel agent", "group travel booking", "family holiday packages",
        "flight and hotel packages", "custom travel itinerary", "travel consultant near me",
        "business travel agent", "europe tour travel agent", "south pacific holiday specialist",
        "disney travel agent", "travel agency package deals", "all inclusive holiday packages",
        "local travel agency", "best travel agent", "travel booking service",
        "school group travel planner", "sports team travel booking", "last minute holiday deals",
    ],

    "Cosmetic Clinic": [
        "cosmetic clinic near me", "injectables clinic", "anti wrinkle injections",
        "dermal fillers clinic", "lip filler clinic", "cosmetic doctor near me",
        "skin rejuvenation clinic", "facial contouring clinic", "thread lift clinic",
        "non surgical facelift", "double chin treatment", "facial volume restoration",
        "wrinkle treatment clinic", "tear trough filler clinic", "jawline filler clinic",
        "cheek filler clinic", "best cosmetic clinic", "cosmetic consultation",
        "medical aesthetics clinic", "local injectables clinic", "anti aging clinic",
        "skin tightening clinic", "cosmetic clinic payment plans", "cosmetic treatment packages",
    ],

    "Laser Clinic": [
        "laser clinic near me", "laser hair removal clinic", "ipl hair removal",
        "skin laser clinic", "pigmentation laser treatment", "acne scar laser treatment",
        "vascular laser treatment", "laser tattoo removal", "fractional laser resurfacing",
        "laser genesis treatment", "best laser clinic", "affordable laser hair removal",
        "laser skin rejuvenation", "laser for rosacea", "laser for sun damage",
        "laser carbon peel clinic", "laser consultation", "local laser clinic",
        "medical laser clinic", "permanent hair reduction clinic", "underarm laser hair removal",
        "full body laser clinic", "laser facial clinic", "advanced laser skin treatment",
    ],

    "Tree Service / Arborist": [
        "arborist near me", "tree removal service", "stump grinding service",
        "emergency tree removal", "tree pruning service", "tree lopping near me",
        "palm tree removal", "hedge trimming service", "council tree report arborist",
        "tree risk assessment", "arborist report for council", "storm damage tree removal",
        "commercial tree services", "residential tree services", "land clearing contractor",
        "mulching tree service", "best arborist near me", "local tree removal company",
        "tree trimming quote", "dead tree removal", "large tree removal specialist",
        "root barrier installation", "tree cabling and bracing", "same day arborist service",
    ],

    "Concreting": [
        "concreter near me", "concrete driveway contractor", "concrete slab contractor",
        "exposed aggregate concreting", "concrete patio installer", "shed slab concreting",
        "garage slab contractor", "footpath concreting", "decorative concrete contractor",
        "stamped concrete service", "commercial concreting contractor", "residential concreting contractor",
        "concrete resurfacing service", "concrete repair contractor", "concrete sealing service",
        "house slab concreting", "best concreter near me", "local concrete company",
        "concrete quote", "pathway concreting", "pool surround concreting",
        "retaining wall footings", "driveway crossover concreting", "plain concrete driveway",
    ],

    "Tyre Shop": [
        "tyre shop near me", "tyres near me", "cheap tyres",
        "wheel alignment near me", "mobile tyre fitting", "tyre replacement service",
        "4wd tyres shop", "performance tyres shop", "truck tyres near me",
        "same day tyres", "tyre and wheel package", "puncture repair near me",
        "wheel balancing service", "roadworthy tyres", "all terrain tyres shop",
        "run flat tyres near me", "best tyre shop", "local tyre service",
        "buy tyres online and fitment", "tyre rotation service", "battery and tyres shop",
        "brake and tyre service", "afterpay tyres", "used tyres near me",
    ],

    "Smash Repairs": [
        "smash repairs near me", "panel beating near me", "car body repair shop",
        "accident repair centre", "insurance smash repairs", "bumper repair service",
        "dent repair specialist", "paintless dent removal", "spray painting car repair",
        "hail damage repair", "collision repair centre", "car scratch repair",
        "same day smash repairs", "prestige car body repairs", "fleet smash repairs",
        "truck smash repairs", "motorcycle smash repairs", "best smash repairer",
        "local panel beater", "car paint repair shop", "accident towing and repair",
        "structural car repair", "wheel rim repair service", "windscreen replacement and repair",
    ],

    "Towing Service": [
        "towing service near me", "24 hour towing", "emergency towing service",
        "accident towing near me", "breakdown towing service", "car towing near me",
        "motorbike towing service", "truck towing service", "tilt tray towing",
        "long distance towing", "local tow truck", "cheap towing service",
        "roadside assistance towing", "same day towing", "wreck removal service",
        "cash for unwanted cars towing", "machinery transport towing", "container towing service",
        "parking garage towing", "after hours towing", "best towing company",
        "flat battery roadside assist", "locked out roadside service", "local tow truck near me",
    ],

    "Funeral Director": [
        "funeral director near me", "funeral home near me", "funeral services",
        "cremation services", "burial services", "affordable funerals",
        "prepaid funeral plans", "memorial service planning", "direct cremation service",
        "celebrant funeral service", "funeral director quote", "funeral package prices",
        "local funeral arranger", "religious funeral service", "non religious funeral service",
        "funeral repatriation services", "graveside service planning", "end of life planning",
        "funeral flowers and service", "private family funeral", "best funeral home",
        "24 hour funeral assistance", "funeral transfer service", "compassionate funeral director",
    ],

    "Software Development Agency": [
        "software development company", "custom software development", "app development company",
        "web application development company", "saas development agency", "crm development company",
        "erp software development", "mobile app developers", "mvp development agency",
        "enterprise software development", "python development company", "ai software development company",
        "automation software development", "custom portal development", "api integration development",
        "software development outsourcing", "local software developers", "best software development agency",
        "startup software development", "business software solutions", "product development agency",
        "custom database development", "full stack development company", "software development consultation",
    ],

    "Private School": [
        "private school near me", "independent school near me", "private primary school",
        "private high school", "best private school", "school enrolment near me",
        "college open day", "school scholarship application", "early learning private school",
        "k 12 private school", "co educational private school", "girls private school",
        "boys private school", "faith based private school", "boarding school near me",
        "academic excellence private school", "school tour booking", "private school fees",
        "local independent college", "secondary college enrolment", "private school scholarships",
        "school waiting list application", "best high school near me", "prep to year 12 college",
    ],

    "Plastic Surgeon": [
        "plastic surgeon near me", "cosmetic surgeon near me", "breast augmentation surgeon",
        "rhinoplasty surgeon", "facelift surgeon", "eyelid surgery specialist",
        "tummy tuck surgeon", "liposuction surgeon", "abdominoplasty specialist",
        "breast lift surgeon", "breast reduction surgeon", "chin liposuction surgeon",
        "body contouring surgeon", "revision rhinoplasty surgeon", "otoplasty surgeon",
        "male chest reduction surgeon", "plastic surgery consultation", "best plastic surgeon",
        "local cosmetic surgery clinic", "plastic surgery payment plans", "mummy makeover surgeon",
        "neck lift surgeon", "arm lift surgeon", "skin removal surgery specialist",
    ],

    "Fertility / IVF Clinic": [
        "ivf clinic near me", "fertility clinic near me", "fertility specialist",
        "ivf specialist", "egg freezing clinic", "iui clinic",
        "male fertility clinic", "female fertility specialist", "fertility assessment clinic",
        "ivf consultation", "bulk billing fertility clinic", "low cost ivf clinic",
        "donor egg ivf clinic", "fertility testing near me", "reproductive endocrinologist",
        "fertility treatment centre", "best ivf clinic", "local fertility centre",
        "pregnancy planning specialist", "pcos fertility clinic", "miscarriage fertility specialist",
        "semen analysis clinic", "ovulation induction specialist", "fertility preservation clinic",
    ],
}
# ── END NEW_CITY_EXPANSION_KEYWORDS ───────────────────────────────────────────

# Decision-maker role keywords for sorting (V5.11: uses HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS)
# Defined earlier in the module — this reassignment keeps it consistent
DECISION_MAKER_KEYWORDS = HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS

# ══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════


def safe_str(val) -> str:
    """Safely convert a value to string, treating None as empty string."""
    if val is None:
        return ""
    return str(val).strip()


def is_decision_maker(role: str) -> bool:
    """Check if a role indicates a decision maker."""
    if not role:
        return False
    role_lower = role.lower()
    return any(kw in role_lower for kw in DECISION_MAKER_KEYWORDS)


# V5.20: Role hierarchy for company-level person prioritization
# Higher score = more senior decision maker. Used in Phase 6 domain grouping.
ROLE_HIERARCHY_LEVELS = [
    (100, ["owner", "co-owner", "business owner"]),
    (95,  ["founder", "co-founder", "cofounder"]),
    (90,  ["ceo", "chief executive", "managing director", "md", "cfo", "cto", "coo",
            "cmo", "cio", "cpo", "chief", "chairman", "chairwoman", "chairperson", "president"]),
    (85,  ["partner", "principal"]),
    (80,  ["vice president", "vp", "svp", "evp", "avp"]),
    (75,  ["head of", "head,"]),
    (70,  ["director"]),
    (60,  ["manager", "gm", "general manager"]),
    (50,  ["senior"]),
    (10,  ["intern", "trainee", "apprentice"]),
]


def _role_hierarchy_score(role: str) -> int:
    """V5.20: Score a role based on decision-maker hierarchy.
    Returns 0-100 where higher = more senior decision maker.
    Uses word-boundary matching so 'director' doesn't falsely match 'cto'.
    'Managing Director' matches 'managing director' (90) taking the highest.
    """
    if not role:
        return 0
    role_lower = role.lower()
    best_score = 0
    for level_score, keywords in ROLE_HIERARCHY_LEVELS:
        for kw in keywords:
            # Use regex word boundary for short keywords to avoid substring false positives
            # e.g. "cto" should NOT match inside "director"
            if re.search(r'(?:^|[\s,/&(]|\b)' + re.escape(kw) + r'(?:[\s,/&)]|\b|$)', role_lower):
                best_score = max(best_score, level_score)
    return best_score


def get_full_name(person: dict) -> str:
    """V5.13: Extract the best possible full name from an Apollo/Lusha person dict.
    Checks name, full_name, first_name+last_name, display_name in order.
    """
    if person.get("name") and str(person["name"]).strip():
        return str(person["name"]).strip()
    if person.get("full_name") and str(person["full_name"]).strip():
        return str(person["full_name"]).strip()
    first = safe_str(person.get("first_name"))
    last = safe_str(person.get("last_name"))
    full = f"{first} {last}".strip()
    if full:
        return full
    if person.get("display_name") and str(person["display_name"]).strip():
        return str(person["display_name"]).strip()
    return ""


class RateLimiter:
    """Simple per-API rate limiter with minimum interval between calls."""

    def __init__(self, min_interval: float = 1.0):
        self.min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self._last_call = time.time()


def extract_domain(url: str) -> str:
    """Extract clean domain from a URL."""
    try:
        parsed = urlparse(url if "://" in url else f"https://{url}")
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = domain.lower().strip()
        if domain.startswith("www."):
            domain = domain[4:]
        # Remove port
        if ":" in domain:
            domain = domain.split(":")[0]
        return domain
    except Exception:
        return ""


def domain_to_company_name(domain: str) -> str:
    """Convert domain string to a readable company name.
    e.g., 'smith-dental.com.au' -> 'Smith Dental'
    """
    name = domain.lower().strip()
    for prefix in ("https://", "http://", "www."):
        if name.startswith(prefix):
            name = name[len(prefix):]
    name = name.split("/")[0]
    # Remove TLDs (order matters — longer first)
    tld_patterns = [
        ".com.au", ".co.uk", ".org.au", ".net.au", ".gov.au",
        ".co.in", ".org.in", ".net.in",
        ".co.nz", ".com", ".org", ".net", ".io", ".co",
        ".biz", ".info", ".au", ".uk", ".in", ".us",
    ]
    for tld in tld_patterns:
        if name.endswith(tld):
            name = name[: -len(tld)]
            break
    name = name.replace("-", " ").replace("_", " ").replace(".", " ")
    name = " ".join(name.split())
    return name.title() if name else domain


# ── V5.3: Name extraction utilities (use data we already have) ──────────

# Common business suffixes to strip when extracting person names from company names
_BUSINESS_SUFFIXES = {
    "photography", "photo", "photos", "studio", "studios", "creative", "creatives",
    "design", "designs", "media", "digital", "agency", "group", "co", "company",
    "consulting", "consultancy", "solutions", "services", "enterprises", "pty", "ltd",
    "inc", "llc", "corp", "plumbing", "electrical", "construction", "building",
    "dental", "medical", "health", "wellness", "fitness", "beauty", "salon",
    "law", "legal", "accounting", "finance", "marketing", "events", "catering",
    "landscaping", "cleaning", "painting", "roofing", "interiors", "productions",
    "films", "video", "visuals", "imaging", "images", "pictures", "portraits",
}

# Common first-name abbreviations → full forms (for matching domain/company names)
_NAME_ABBREVIATIONS = {
    "matt": ["matthew", "mathew"], "mike": ["michael"], "chris": ["christopher", "christine", "christina"],
    "rob": ["robert", "robin"], "bob": ["robert"], "dave": ["david"], "dan": ["daniel", "danny"],
    "nick": ["nicholas", "nicolas"], "tom": ["thomas"], "ben": ["benjamin"], "sam": ["samuel", "samantha"],
    "alex": ["alexander", "alexandra"], "max": ["maxwell", "maximilian"], "will": ["william"],
    "jim": ["james"], "joe": ["joseph"], "steve": ["steven", "stephen"], "tony": ["anthony"],
    "kate": ["katherine", "kathryn", "catherine"], "liz": ["elizabeth"], "meg": ["megan", "margaret"],
    "jen": ["jennifer", "jenna"], "pat": ["patrick", "patricia"], "andy": ["andrew"],
    "rick": ["richard"], "dick": ["richard"], "bill": ["william"], "ted": ["edward", "theodore"],
    "pete": ["peter"], "greg": ["gregory"], "tim": ["timothy"], "jon": ["jonathan", "jonathon"],
    "stu": ["stuart", "stewart"], "phil": ["philip", "phillip"], "ed": ["edward", "edmund"],
    "ash": ["ashley", "ashton"], "jake": ["jacob"], "jack": ["jackson", "john"],
    "nate": ["nathan", "nathaniel"], "josh": ["joshua"], "zach": ["zachary"],
    "luke": ["lucas"], "brad": ["bradley"], "drew": ["andrew"],
    "mel": ["melissa", "melanie"], "bec": ["rebecca"], "soph": ["sophia", "sophie"],
    "nat": ["natalie", "natasha", "nathan"], "em": ["emma", "emily"],
    "kel": ["kelly", "kelvin"], "les": ["leslie", "lester"],
    "russ": ["russell"], "mick": ["michael"],
}


def _get_name_variants(first_name: str) -> list[str]:
    """Return all possible full-form variants of a first name (including itself)."""
    lower = first_name.lower()
    variants = [lower]
    if lower in _NAME_ABBREVIATIONS:
        variants.extend(_NAME_ABBREVIATIONS[lower])
    # Also check if any abbreviation maps TO this name (reverse lookup)
    for abbrev, fulls in _NAME_ABBREVIATIONS.items():
        if lower in fulls and abbrev not in variants:
            variants.append(abbrev)
    return variants


def _extract_name_from_company(first_name: str, company_name: str) -> str:
    """V5.3: Extract full name from a company name that contains the person's name.
    e.g., first_name="Matt", company_name="Matthew Cornell Photography" → "Matthew Cornell"
          first_name="Julia", company_name="Julia Nance Photography" → "Julia Nance"
    """
    if not first_name or not company_name:
        return ""
    variants = _get_name_variants(first_name)
    words = company_name.split()
    if len(words) < 2:
        return ""
    # Check if the first word of the company name matches any variant of the person's first name
    first_word = words[0].lower()
    if first_word not in variants:
        return ""
    # Collect name words (skip business suffixes)
    name_words = [words[0]]  # Keep original casing
    for w in words[1:]:
        if w.lower() in _BUSINESS_SUFFIXES:
            break
        if w.lower() in ("&", "and", "the", "of", "by"):
            break
        # Must look like a name (capitalized, alpha, reasonable length)
        if w[0].isupper() and w.replace("'", "").replace("-", "").isalpha() and len(w) <= 20:
            name_words.append(w)
        else:
            break
    if len(name_words) >= 2:
        return " ".join(name_words)
    return ""


def _extract_name_from_domain(first_name: str, domain: str) -> str:
    """V5.3: Extract full name from a domain that encodes the person's name.
    e.g., first_name="Matt", domain="matthewcornell.com.au" → "Matthew Cornell"
          first_name="Julia", domain="julianance.com.au" → "Julia Nance"
    """
    if not first_name or not domain:
        return ""
    # Strip TLDs to get the domain root
    root = domain.lower()
    for tld in [".com.au", ".co.uk", ".org.au", ".net.au", ".co.nz",
                ".com", ".org", ".net", ".io", ".co", ".au", ".uk"]:
        if root.endswith(tld):
            root = root[:-len(tld)]
            break
    # Remove www prefix
    if root.startswith("www."):
        root = root[4:]
    # Remove hyphens for matching (many domains use firstname-lastname)
    root_clean = root.replace("-", "").replace(".", "")
    root_hyphen = root  # keep hyphens for splitting

    variants = _get_name_variants(first_name)

    for variant in variants:
        if root_clean.startswith(variant) and len(root_clean) > len(variant) + 1:
            suffix = root_clean[len(variant):]
            # Filter out business suffixes in domain (e.g. mattphoto.com)
            if suffix in _BUSINESS_SUFFIXES:
                continue
            # Check suffix starts with a letter and is alphabetic (likely a last name)
            if suffix.isalpha() and 2 <= len(suffix) <= 18:
                # Use the variant as the first name (may be fuller than the lead's current name)
                full_first = variant.title()
                return f"{full_first} {suffix.title()}"

    # Also try hyphenated domains: matthew-cornell.com.au
    if "-" in root_hyphen:
        parts = root_hyphen.split("-")
        if len(parts) >= 2 and parts[0] in variants:
            last = parts[1]
            if last.isalpha() and last not in _BUSINESS_SUFFIXES and 2 <= len(last) <= 18:
                return f"{parts[0].title()} {last.title()}"

    return ""


def _extract_name_from_linkedin_url(first_name: str, linkedin_url: str) -> str:
    """V5.3: Extract full name from a LinkedIn URL slug.
    e.g., first_name="Matt", url="linkedin.com/in/matthew-cornell-123abc" → "Matthew Cornell"
    """
    if not first_name or not linkedin_url:
        return ""
    # Extract the slug from the URL
    match = re.search(r"linkedin\.com/in/([^/?]+)", linkedin_url)
    if not match:
        return ""
    slug = match.group(1).lower()
    # Split slug by hyphens, filter out trailing IDs (hex strings, digits)
    parts = slug.split("-")
    name_parts = []
    for p in parts:
        # Stop at numeric/hex suffixes (LinkedIn adds random IDs like "a1b2c3")
        if p.isdigit() or (len(p) >= 5 and all(c in "0123456789abcdef" for c in p)):
            break
        if p.isalpha() and len(p) >= 2:
            name_parts.append(p)
    if len(name_parts) < 2:
        return ""
    # Check if the first part matches any variant of the person's first name
    variants = _get_name_variants(first_name)
    if name_parts[0] not in variants:
        return ""
    # Build the full name from the remaining parts
    return " ".join(p.title() for p in name_parts[:3])  # cap at 3 words (first middle last)


def _format_revenue_value(raw) -> str:
    """Normalize Apollo revenue fields into a readable value for CSV export."""
    if raw is None:
        return ""
    if isinstance(raw, (int, float)) and raw:
        v = float(raw)
        if v >= 1_000_000_000:
            return f"${v/1_000_000_000:.1f}B"
        if v >= 1_000_000:
            return f"${v/1_000_000:.1f}M"
        if v >= 1_000:
            return f"${v/1_000:.0f}K"
        return f"${int(v)}"
    if isinstance(raw, str):
        return raw.strip()
    return ""


def _extract_apollo_revenue(org: dict) -> str:
    """Apollo returns revenue under several organization/person-org keys."""
    if not isinstance(org, dict):
        return ""
    for key in (
        "annual_revenue_printed",
        "organization_revenue_printed",
        "estimated_annual_revenue_printed",
        "revenue_printed",
        "annual_revenue",
        "organization_revenue",
        "estimated_annual_revenue",
        "revenue",
    ):
        val = _format_revenue_value(org.get(key))
        if val:
            return val
    revenue_range = org.get("revenue_range") or org.get("estimated_revenue_range")
    if isinstance(revenue_range, dict):
        low = _format_revenue_value(revenue_range.get("min") or revenue_range.get("lower"))
        high = _format_revenue_value(revenue_range.get("max") or revenue_range.get("upper"))
        if low and high:
            return f"{low}-{high}"
        return low or high
    return ""


def _extract_linkedin_url(obj: dict) -> str:
    """Read LinkedIn URL from common Apollo person/org shapes."""
    if not isinstance(obj, dict):
        return ""
    direct = (
        safe_str(obj.get("linkedin_url"))
        or safe_str(obj.get("linkedin_profile_url"))
        or safe_str(obj.get("linkedin"))
    )
    if direct:
        return direct
    social = obj.get("social") or obj.get("socials") or {}
    if isinstance(social, dict):
        li = social.get("linkedin") or social.get("linkedIn") or {}
        if isinstance(li, dict):
            return safe_str(li.get("url") or li.get("profile_url"))
        return safe_str(li)
    links = obj.get("links") or {}
    if isinstance(links, dict):
        return safe_str(links.get("linkedin") or links.get("linkedin_url"))
    return ""


def format_phone(raw_phone: str, country: str) -> str:
    """Normalize and strictly validate phone number.
    Returns '+' prefixed digits (e.g. '+61XXXXXXXXXX') or '' if invalid.
    The '+' prefix prevents Excel from converting to scientific notation.
    AU: +61 + 10 digits = 12 digit body.  USA: +1 + 10 = 11.
    UK: +44 + 10 = 12.  India: +91 + 10 = 12.
    V5.27: Preserve valid international E.164 numbers with a DIFFERENT country code
    (e.g. a South African +27 number for a contact at an AU company).
    """
    if not raw_phone:
        return ""
    config = COUNTRY_CONFIG.get(country)
    if not config:
        return ""
    code_digits = config["phone_code"].replace("+", "")  # e.g. "61"
    expected_len = config["phone_digits"]  # e.g. 11

    # Strip ALL non-digit characters (removes letters, +, spaces, dashes, etc.)
    digits = re.sub(r"[^\d]", "", str(raw_phone))
    if not digits or len(digits) < 8:
        return ""

    # V5.27: If the original phone string starts with '+' and uses a DIFFERENT country code,
    # it's a valid international E.164 number from another country (e.g. +27 South Africa).
    # Preserve it as-is rather than mangling it with the target country code.
    raw_stripped = str(raw_phone).strip()
    if raw_stripped.startswith("+") and not digits.startswith(code_digits):
        # Accept any E.164-format international number (8–15 digits total)
        if 8 <= len(digits) <= 15:
            return f"+{digits}"
        return ""

    # Strip leading 0 (local format) and prepend country code
    if digits.startswith("0"):
        digits = code_digits + digits[1:]
    # If doesn't start with country code, prepend it
    if not digits.startswith(code_digits):
        digits = code_digits + digits

    # Strict validation: exact length required
    if len(digits) != expected_len:
        return ""
    return f"+{digits}"


def is_valid_email(email: str) -> bool:
    """Basic email validation — filters out obvious non-emails."""
    if not email or "@" not in email:
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(pattern, email):
        return False
    bad_patterns = [
        "example.com", "test.com", "sentry.io", "wixpress.com",
        ".png", ".jpg", ".gif", ".svg", ".webp", ".css", ".js",
        "noreply", "no-reply", "mailer-daemon", "postmaster",
        "schema.org", "sentry", "w3.org", "googleapis",
    ]
    email_lower = email.lower()
    return not any(bp in email_lower for bp in bad_patterns)


# Generic/company email prefixes that indicate a shared inbox, NOT a personal email
GENERIC_EMAIL_PREFIXES = {
    "info", "enquiries", "enquiry", "contact", "reception", "admin",
    "office", "hello", "hi", "help", "support", "sales", "marketing",
    "billing", "accounts", "finance", "hr", "careers", "jobs",
    "team", "general", "mail", "service", "services", "bookings",
    "booking", "appointments", "appointment", "feedback", "media",
    "press", "news", "newsletter", "subscribe", "unsubscribe",
    "webmaster", "postmaster", "abuse", "security", "legal",
    "compliance", "privacy", "orders", "order", "returns", "shipping",
    "dispatch", "warehouse", "operations", "customerservice",
    "customer.service", "customer-service", "customercare",
    "reception", "frontdesk", "front.desk", "front-desk",
    "practice", "clinic", "surgery", "studio", "salon", "shop",
    "store", "manager", "management",
    # City/region inboxes (office contact points, NOT personal)
    "london", "newyork", "new-york", "austin", "paris", "berlin",
    "sydney", "perth", "melbourne", "brisbane", "adelaide",
    "asia", "global", "international", "national", "regional",
    # Business action prefixes
    "newbusiness", "new-business", "enquire", "enq",
    "quote", "quotes", "estimates", "estimate",
    "noreply", "no-reply", "donotreply", "do-not-reply",
    # Department/function prefixes
    "design", "creative", "digital", "agency",
    "partnerships", "partnership", "solutions", "projects",
    "careers", "internship", "volunteer", "work",
    "discovery", "business", "pr",
}


def is_personal_email(email: str) -> bool:
    """V5.11: True ONLY for emails at known personal/consumer domains (gmail, yahoo, hotmail etc.).
    firstname@company.com is NOT personal — it's a work email.
    """
    if not email or "@" not in email:
        return False
    domain = email.lower().split("@")[-1].strip()
    return domain in PERSONAL_EMAIL_DOMAINS


def is_work_email(email: str) -> bool:
    """V5.11: True for first-name-style work emails (firstname@company.com).
    Distinct from generic inbox (info@, contact@) and personal (gmail, yahoo).
    """
    if not email or "@" not in email:
        return False
    domain = email.lower().split("@")[-1].strip()
    if domain in PERSONAL_EMAIL_DOMAINS:
        return False  # personal domain
    local = email.lower().split("@")[0].strip()
    return local not in GENERIC_EMAIL_PREFIXES  # not a generic inbox prefix


def classify_email_smart(email: str, person_name: str = "", company_name: str = "") -> str:
    """V5.4: Smart email classifier — uses name + company cross-referencing.
    Returns 'Personal', 'Generic', or 'Unknown'.

    Rules:
    1. If local part is in GENERIC_EMAIL_PREFIXES → Generic
    2. If local part contains any word from the person's name → Personal
    3. If local part contains any word from the company name → likely Generic (company email)
    4. Otherwise → Unknown (let OpenAI decide)
    """
    if not email or "@" not in email:
        return "Unknown"
    local = email.lower().split("@")[0].strip()

    # Rule 1: Known generic prefixes
    if local in GENERIC_EMAIL_PREFIXES:
        return "Generic"

    # Rule 2: Check if email contains words from the person's name
    if person_name:
        name_words = [w.lower() for w in person_name.split() if len(w) >= 2]
        for nw in name_words:
            # Check if name word appears in local part (handles first.last, firstlast, etc.)
            clean_local = local.replace(".", "").replace("-", "").replace("_", "")
            if nw in clean_local or nw in local:
                return "Personal"

    # Rule 3: Check if email contains words from the company name → likely company/generic email
    if company_name:
        company_words = [w.lower() for w in company_name.split()
                         if len(w) >= 3 and w.lower() not in _BUSINESS_SUFFIXES
                         and w.lower() not in {"the", "and", "of", "by", "for", "at", "in"}]
        for cw in company_words:
            if cw in local:
                return "Generic"

    # Rule 4: Passes generic prefix check → likely personal (heuristic fallback)
    return "Personal" if local not in GENERIC_EMAIL_PREFIXES else "Generic"


def _email_contains_person_name(email_addr: str, first_name: str = "", last_name: str = "") -> bool:
    """V5.20: Check if an email's local part contains the person's first or last name.
    Used to distinguish name-based business emails from generic company emails.
    Case-insensitive. Requires name parts >= 2 chars to avoid false matches.
    """
    if not email_addr or "@" not in email_addr:
        return False
    local = email_addr.lower().split("@")[0]
    clean_local = local.replace(".", "").replace("-", "").replace("_", "")
    first_lower = first_name.lower().strip() if first_name else ""
    last_lower = last_name.lower().strip() if last_name else ""
    if first_lower and len(first_lower) >= 2 and (first_lower in local or first_lower in clean_local):
        return True
    if last_lower and len(last_lower) >= 2 and (last_lower in local or last_lower in clean_local):
        return True
    return False


def _pick_best_email_from_apollo(person: dict, first_name: str = "", last_name: str = "") -> tuple:
    """V5.20: Smart email selection from Apollo person data.
    Returns (best_email, is_from_personal_list).

    Priority (per Apollo Knowledge Base — business emails first):
    1. Primary-tagged business email containing person's name
    2. Any business-tagged email containing person's name
    3. Primary-tagged personal email (gmail/yahoo)
    4. Any personal email (gmail/yahoo)
    5. Work email containing person's name (non-generic)
    6. Fallback to first available non-generic email

    Ensures business emails aren't generic company emails by verifying
    the person's first or last name appears in the local part.
    """
    structured_emails = person.get("emails") or []
    flat_personal = person.get("personal_emails") or []
    org_email = safe_str(person.get("email"))
    contact_email = safe_str(person.get("contact_email"))

    # --- Structured emails with type/tag metadata ---
    if structured_emails:
        business_primary = []
        business_other = []
        personal_primary = []
        personal_other = []

        for em_obj in structured_emails:
            if not isinstance(em_obj, dict):
                continue
            email_addr = em_obj.get("email", "")
            if not email_addr:
                continue
            email_type = (em_obj.get("email_type") or em_obj.get("type") or "").lower()
            email_tag = (em_obj.get("email_tag") or em_obj.get("tag") or em_obj.get("label") or "").lower()
            email_status = (em_obj.get("email_status") or em_obj.get("status") or "").lower()
            is_primary = ("primary" in email_tag or "primary" in email_status
                          or "primary" in email_type
                          or em_obj.get("position") == 0)
            is_business = ("business" in email_type or "professional" in email_type
                           or "work" in email_type or "primary" in email_type)
            is_personal_type = "personal" in email_type

            # V5.22: Business-tagged emails are preferred regardless of name containment.
            # Previously required name in address — too restrictive (e.g. j.smith@co omitted).
            if is_business and not is_personal_email(email_addr):
                (business_primary if is_primary else business_other).append(email_addr)
            elif is_personal_type or is_personal_email(email_addr):
                (personal_primary if is_primary else personal_other).append(email_addr)

        for bucket in [business_primary, business_other, personal_primary, personal_other]:
            if bucket:
                return bucket[0], True

    # --- V5.22: Check org/contact email for BUSINESS format BEFORE flat_personal consumer emails.
    # Apollo's `email` field is the primary work/org email (company domain).
    # Prefer it over personal_emails (gmail/hotmail) which are stored in flat_personal.
    for em in [org_email, contact_email]:
        if em and not is_personal_email(em):  # Company-domain email (not gmail/yahoo/hotmail)
            return em, False  # Business email found — return before checking consumer emails

    # --- Flat personal_emails: prefer name-based business over consumer ---
    if flat_personal:
        name_based = []
        consumer = []
        for em in flat_personal:
            if not em:
                continue
            if is_personal_email(em):                          # gmail/yahoo
                consumer.append(em)
            elif _email_contains_person_name(em, first_name, last_name):
                name_based.append(em)                          # riley@24hrpower.net.au
            else:
                name_based.append(em)                          # non-consumer, non-name — still preferable
        if name_based:
            return name_based[0], True
        if consumer:
            return consumer[0], True

    # --- Org/contact email final fallback (consumer domain) ---
    for em in [org_email, contact_email]:
        if em:
            return em, False

    return "", False


def _pick_best_phone_from_apollo(person: dict, company_phone: str = "") -> tuple:
    """V5.22: Smart phone selection from Apollo person data.
    Returns (best_phone, quality_score). quality_score=0 if no phone found.
    Priority: mobile/direct personal phones OVER company HQ numbers.
    Type priority: mobile > direct > work_direct > personal > home > other > work > work_hq
    This ensures we get the decision-maker's personal/direct number,
    NOT the generic company switchboard number.
    V5.25: Accepts company_phone to exclude company-level numbers. Also checks singular
    phone_number/direct_phone fields as fallback when phone_numbers array is empty.
    """
    _co_digits = re.sub(r'\D', '', company_phone) if company_phone else ""
    phone_numbers = person.get("phone_numbers") or []
    if not phone_numbers:
        # V5.25: Check singular phone fields as fallback — these CAN be personal for revealed contacts
        for field in ["phone_number", "direct_phone_number", "sanitized_phone", "phone"]:
            singular = safe_str(person.get(field))
            if singular:
                singular_digits = re.sub(r'\D', '', singular)
                # Skip if it matches the company phone (generic switchboard)
                if _co_digits and singular_digits == _co_digits:
                    continue
                # Treat singular field as medium quality (25) — not verified as mobile
                return singular, 25
        return "", 0

    # Score each phone by type — higher = more personal/direct
    # Threshold >= 30 means genuinely personal (mobile/direct/personal/home)
    _TYPE_SCORES = {
        "mobile": 50,
        "direct": 40,         # V5.22: Added — Apollo uses "direct" for direct lines
        "work_direct": 40,
        "direct_dial": 40,
        "personal": 35,
        "home": 30,
        "other": 15,
        "work": 15,           # V5.22: Added — "work" is company-level, not personal
        "work_hq": 5,         # company switchboard — least preferred
        "company_hq": 5,
        "corporate": 5,
        "headquarters": 5,
        "main": 5,
    }

    scored_phones = []
    for pn in phone_numbers:
        if not isinstance(pn, dict):
            continue
        number = safe_str(pn.get("sanitized_number") or pn.get("number")
                          or pn.get("raw_number", ""))
        if not number:
            continue

        # V5.26: Handle both standard Apollo format (type) and webhook format (type_cd)
        pn_type = (pn.get("type") or pn.get("type_cd") or "").lower().strip()
        pn_status = (pn.get("status") or pn.get("status_cd") or "").lower()
        pn_label = (pn.get("label") or pn.get("tag") or "").lower()
        is_primary = pn.get("is_primary") or pn.get("position") == 0
        is_default = ("default" in pn_status or "default" in pn_label
                      or "default" in pn_type)

        # Base score from phone type
        score = _TYPE_SCORES.get(pn_type, 20)  # unknown type gets 20

        # Bonus for primary/default flags
        if is_primary:
            score += 3
        if is_default:
            # V5.23: Default phone = Apollo's primary contact number for this person.
            # Boost to 35 for "other/work" defaults so they are treated as _direct_phone
            # (quality >= 30) and cannot be overwritten by enrich_person HQ phones (quality 5).
            # work_hq (5+20=25) remains non-direct. Mobile (50+20=70) stays highest.
            score += 20

        # V5.25: Penalize if this number matches the known company phone
        if _co_digits:
            num_digits = re.sub(r'\D', '', number)
            if num_digits == _co_digits:
                score = min(score, 5)  # Force to lowest tier (company HQ level)

        scored_phones.append((score, number))

    if not scored_phones:
        return "", 0  # V5.24: no scoreable phones — don't fall back to company-level singular field

    # Sort by score descending — highest-scored (most personal) phone wins
    scored_phones.sort(key=lambda x: x[0], reverse=True)
    return scored_phones[0][1], scored_phones[0][0]


def match_email_to_name(email: str, first_name: str, last_name: str) -> bool:
    """Check if an email's local part matches patterns for a person's name."""
    if not email or not first_name:
        return False
    local = email.lower().split("@")[0]
    f = first_name.lower().strip()
    l = last_name.lower().strip() if last_name else ""
    if f and len(f) > 1 and f in local:
        return True
    if l and len(l) > 1 and l in local:
        return True
    return False


def generate_email_candidates(first_name: str, last_name: str, domain: str) -> list:
    """V5.7: Generate likely email addresses from name + domain. No API calls."""
    if not first_name or not domain:
        return []
    f = first_name.lower().strip()
    l = last_name.lower().strip() if last_name else ""
    if l:
        return [
            f"{f}.{l}@{domain}",
            f"{f}{l}@{domain}",
            f"{f[0]}.{l}@{domain}",
            f"{f[0]}{l}@{domain}",
            f"{f}@{domain}",
            f"{l}.{f}@{domain}",
        ]
    return [f"{f}@{domain}"]


def _is_news_domain_heuristic(domain: str) -> bool:
    """Check if a domain looks like a news/media site by common name patterns.
    Only matches whole segments to avoid false positives (e.g. 'newscastle-dental' won't match).
    """
    d = domain.lower().strip()
    if d.startswith("www."):
        d = d[4:]
    # Split domain into segments (e.g. 'newcastle-herald.com.au' -> ['newcastle-herald', 'com', 'au'])
    base = d.split(".")[0]  # just the main domain name part
    # Split by hyphens too for compound names
    parts = set(base.replace("-", " ").replace("_", " ").split())
    news_keywords = {
        "news", "herald", "gazette", "journal", "tribune",
        "chronicle", "telegraph", "observer", "courier",
        "examiner", "mercury", "sentinel", "dispatch", "bulletin",
        "recorder", "advertiser", "times", "post", "press",
        "daily", "morning", "evening", "weekly", "media",
    }
    # Must match a whole word in the domain (not substring)
    return bool(parts & news_keywords)


def is_platform_domain(domain: str) -> bool:
    """Check if a domain is a known platform/directory/non-SMB to skip."""
    d = domain.lower().strip()
    if d.startswith("www."):
        d = d[4:]
    # Exact match or subdomain match against blocklist
    for pd in PLATFORM_DOMAINS:
        if d == pd or d.endswith(f".{pd}"):
            return True
    # Filter educational and government domains globally
    edu_gov_patterns = [".edu.", ".edu", ".gov.", ".gov", ".ac.uk", ".ac.au"]
    for pattern in edu_gov_patterns:
        if pattern in d or d.endswith(pattern):
            return True
    # Filter .org domains (covers .org, .org.au, .org.uk, etc.)
    if ".org" in d:
        return True
    # Heuristic news domain detection (catches domains not in the explicit blocklist)
    if _is_news_domain_heuristic(d):
        return True
    return False


# ══════════════════════════════════════════════════════════════════════════════
# API CLIENTS
# ══════════════════════════════════════════════════════════════════════════════


class SemrushClient:
    """SEMrush API client for keyword expansion AND domain discovery.

    2026-05-18: every call now passes through a centralised unit-budget
    guard (`_request` below). Each SEMrush report type carries a per-row
    cost weight in `_UNIT_COST_PER_ROW`; multiplied by the request's
    `display_limit` it gives the upper-bound credit cost of that single
    call. The pipeline sets `_unit_budget` on this client at start-of-run
    based on `max_leads`; once the running total of *estimated units*
    would exceed the budget, further calls short-circuit returning "".
    A one-time mid-run alert fires at 75 % of budget.

    This is what stops a 3-lead run from burning 8 000+ SEMrush credits
    on per-domain competitor expansions — every expensive call now sees
    the same ceiling, no matter which phase or module triggered it.
    """

    BASE_URL = "https://api.semrush.com/"

    # Per-row cost weights, calibrated against actual SEMrush invoice rows
    # observed in the user's 2026-05-14 query log:
    #   domain_adwords_adwords: 10 rows -> 400 credits  (40/row)
    #   domain_organic_organic: 10 rows -> 400 credits  (40/row)
    #   domain_organic:          5 rows ->  50 credits  (10/row)
    #   phrase_this:             1 row  ->  10 credits  (10/row)
    # Unknown report types fall back to 40/row (conservative upper bound).
    _UNIT_COST_PER_ROW: dict = {
        "phrase_this":              10,
        "phrase_adwords":           1,
        "phrase_organic":           1,
        "phrase_related":           1,
        "phrase_questions":         1,
        "phrase_these":             1,
        "phrase_kdi":               1,
        "phrase_fullsearch":        2,
        "domain_adwords":           40,
        "domain_organic":           10,
        "domain_ranks":             40,
        "domain_adwords_adwords":   40,
        "domain_organic_organic":   40,
        "domain_domains":           40,
    }
    _DEFAULT_COST_PER_ROW = 40

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.8)  # V5.1: Optimized from 1.2
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)
        # 2026-05-18: per-run unit budget. Pipeline overrides at start-of-run.
        # 0 = unlimited (back-compat for offline tools like
        # generate_secondary_keywords.py which we don't want to break).
        self._unit_budget: int = 0
        self._units_used: int = 0
        self._units_by_phase: dict = {}
        self._current_phase: str = "unknown"
        self._budget_alert_75_fired: bool = False
        self._budget_exhausted_logged: bool = False
        # Optional log callback set by the pipeline so mid-run alerts also
        # surface in /status/<job_id>.new_logs (not just print()).
        self._log_cb = None
        # 2026-06-08: hard bypass. When the user picks "SerpAPI only" in the UI
        # the pipeline sets this True and EVERY SEMrush call no-ops (no HTTP,
        # no credits, no counter) — SEMrush is fully skipped even with credits.
        self._disabled = False

    def _estimate_cost(self, params: dict) -> int:
        """Upper-bound credit cost for a single SEMrush call."""
        rtype = (params.get("type") or "").strip()
        per_row = self._UNIT_COST_PER_ROW.get(rtype, self._DEFAULT_COST_PER_ROW)
        try:
            limit = int(params.get("display_limit", 1) or 1)
        except (TypeError, ValueError):
            limit = 1
        return max(per_row, per_row * max(1, limit))

    def _emit_log(self, msg: str) -> None:
        """Route a message to both stdout and the pipeline's log stream.

        2026-05-18: defensive against narrow-charset stdouts (Windows cp1252
        console). If stdout can't encode the message we strip non-ASCII and
        retry — better a degraded log than an exception that bubbles up to
        the surrounding try/except and silently swallows the SEMrush call.
        """
        try:
            print(msg, flush=True)
        except UnicodeEncodeError:
            try:
                print(msg.encode("ascii", "replace").decode("ascii"), flush=True)
            except Exception:
                pass
        cb = self._log_cb
        if callable(cb):
            try:
                cb(msg)
            except Exception:
                pass

    def _shared_get(self, key, default=0):
        """Read shared budget state from the counter dict so every
        SemrushClient instance pointing at the same `_counter` sees the
        same numbers. The counter is passed by REFERENCE everywhere so
        this is the single source of truth across the whole run."""
        return self._counter.get(key, default)

    def _shared_set(self, key, value):
        self._counter[key] = value

    def _shared_phase_increment(self, phase: str, delta: int) -> None:
        """Per-phase breakdown lives inside `_counter` as a nested dict so
        it's shared the same way as the run total."""
        phases = self._counter.get("semrush_units_by_phase")
        if not isinstance(phases, dict):
            phases = {}
            self._counter["semrush_units_by_phase"] = phases
        phases[phase] = phases.get(phase, 0) + int(delta)

    def _request(self, params: dict) -> str:
        """Make a rate-limited request and return raw text.

        2026-05-18 (round 2): every budget read/write goes through the
        shared `_counter` dict, NOT instance attributes. This is what makes
        a single unified budget across multiple SemrushClient instances —
        city_pipeline's discovery + the inner V5 pipeline + city_pipeline's
        rediscovery wave all see and respect the SAME running total.

        Order of evaluation:
          1. Compute upper-bound cost for this call (`_estimate_cost`).
          2. Read current shared total + budget cap.
          3. If `total + cost > cap` → skip, increment skip counter, return "".
          4. Otherwise call SEMrush, on success increment the shared total
             and per-phase bucket, fire the once-per-run 75 % alert when
             the cap is crossed.
        """
        # User chose "SerpAPI only" → SEMrush fully bypassed. Return empty
        # immediately so no HTTP call, no credit spend, no counter touch.
        if getattr(self, "_disabled", False):
            return ""
        est = self._estimate_cost(params)
        # Read budget cap. Prefer the shared counter value (city/inner share
        # one pool); fall back to the instance attr for stand-alone clients
        # (offline tools / tests that don't wire up a shared counter).
        budget = int(self._shared_get("semrush_budget", self._unit_budget) or 0)
        used = int(self._shared_get("semrush_units", 0) or 0)
        # Pre-check the unit budget. Skip the call if it would overflow.
        if budget and (used + est) > budget:
            if not bool(self._shared_get("semrush_budget_exhausted_logged", False)):
                self._shared_set("semrush_budget_exhausted_logged", True)
                self._emit_log(
                    f"[SEMrush] !! Per-run unit budget {budget} exhausted "
                    f"({used} units used). Skipping further calls "
                    f"-- including '{params.get('type','?')}'."
                )
            self._counter["semrush_skipped"] = int(self._counter.get("semrush_skipped", 0)) + 1
            # Keep the instance attribute in sync for any debug-print path.
            self._units_used = used
            return ""

        self.limiter.wait()
        params["key"] = self.api_key
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code == 200:
                if "ERROR" in resp.text[:80]:
                    # Log SEMrush API errors (once per unique message to avoid flooding)
                    err_snippet = resp.text[:120].strip()
                    if not hasattr(self, "_last_error") or self._last_error != err_snippet:
                        self._last_error = err_snippet
                        self._counter["semrush_errors"] = self._counter.get("semrush_errors", 0) + 1
                        print(f"[SEMrush] API error: {err_snippet}", flush=True)
                    return ""
                self._counter["semrush"] = self._counter.get("semrush", 0) + 1
                # 2026-05-18 (round 2): bookkeep through the SHARED counter
                # so multi-client runs share one ledger. Per-phase total is
                # stored as a nested dict inside the counter for the same
                # reason. The actual cost SEMrush bills can be lower than
                # `est` (if fewer rows are returned) so we treat `est` as an
                # upper bound — which is what we want for a hard budget.
                new_total = used + est
                self._shared_set("semrush_units", new_total)
                phase = self._current_phase or "unknown"
                self._shared_phase_increment(phase, est)
                # Mirror to instance attrs for back-compat with code that
                # reads `c._units_used` / `c._units_by_phase` directly.
                self._units_used = new_total
                self._units_by_phase[phase] = self._units_by_phase.get(phase, 0) + est
                # Fire-once 75 % alert, also tracked through shared counter
                # so every client agrees on whether the alert already fired.
                if (budget
                        and not bool(self._shared_get("semrush_budget_alert_75", False))
                        and new_total >= int(budget * 0.75)):
                    self._shared_set("semrush_budget_alert_75", True)
                    self._emit_log(
                        f"[SEMrush] ! Budget warning: {new_total}/{budget} "
                        f"units used (>=75%). Remaining SEMrush calls may be "
                        f"skipped if budget runs out."
                    )
                return resp.text
            else:
                # 2026-05-25: HTTP-error path. 401/403 = account/key broken
                # (suspended, banned, free-tier exhausted, scope removed).
                # 429 = rate limit. 5xx = SEMrush server issues. ALL of
                # these should cascade to "SEMrush unavailable" so the
                # downstream gate-relaxation fires and Google/Apollo/SerpAPI
                # discovered domains aren't all rejected on
                # `paid_traffic=0 < 1`. The shared counter is the single
                # place every gate consults — set the flag once, no need
                # to log every retry's 403.
                _status = int(resp.status_code or 0)
                self._counter["semrush_http_errors"] = int(
                    self._counter.get("semrush_http_errors", 0)
                ) + 1
                if _status in (401, 403):
                    # Persistent — broken until the operator fixes the key.
                    if not bool(self._counter.get("semrush_broken_logged", False)):
                        self._counter["semrush_broken_logged"] = True
                        self._emit_log(
                            f"[SEMrush] ⛔ HTTP {_status} — account/key broken "
                            f"(suspended, exhausted, or invalid). Marking SEMrush "
                            f"as UNAVAILABLE for this run. Domains discovered via "
                            f"Google/Apollo/SerpAPI will bypass the paid-traffic "
                            f"gate (silent-scope fallback)."
                        )
                    # Set the canonical "unavailable" flag in the shared
                    # counter. Pipelines read `_counter["semrush_unavailable"]`
                    # in `_enrich_single_domain` to decide gate relaxation.
                    self._counter["semrush_unavailable"] = True
                else:
                    # Transient (429/5xx) — log first occurrence only to
                    # avoid log spam during a stuck account.
                    if not bool(self._counter.get("semrush_http_err_logged", False)):
                        self._counter["semrush_http_err_logged"] = True
                        self._emit_log(f"[SEMrush] HTTP {_status} (will retry next call)")
        except Exception as exc:
            print(f"[SEMrush] Request exception: {exc}", flush=True)
            self._counter["semrush_http_errors"] = int(
                self._counter.get("semrush_http_errors", 0)
            ) + 1
        return ""

    def get_related_keywords(self, phrase: str, database: str, display_limit: int = 15) -> list[dict]:
        """Get related keywords for a seed phrase."""
        text = self._request({
            "type": "phrase_related",
            "phrase": phrase,
            "database": database,
            "display_limit": display_limit,
            "export_columns": "Ph,Nq,Cp",
        })
        return self._parse_keyword_csv(text)

    def get_organic_domains(self, phrase: str, database: str, limit: int = 10) -> list[dict]:
        """Find domains ranking organically for a keyword.
        Returns list of {'domain': ..., 'url': ...}
        """
        text = self._request({
            "type": "phrase_organic",
            "phrase": phrase,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Ur",
        })
        return self._parse_domain_csv(text)

    def get_adwords_domains(self, phrase: str, database: str, limit: int = 10) -> list[dict]:
        """Find domains running ads for a keyword (high-intent prospects).
        Returns list of {'domain': ..., 'url': ...}
        """
        text = self._request({
            "type": "phrase_adwords",
            "phrase": phrase,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Ur",
        })
        return self._parse_domain_csv(text)

    def _parse_keyword_csv(self, text: str) -> list[dict]:
        results = []
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return results
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) >= 3:
                try:
                    keyword = parts[0].strip()
                    volume = int(parts[1].strip().replace(",", "") or "0")
                    cpc = float(parts[2].strip().replace(",", "") or "0")
                    results.append({"keyword": keyword, "volume": volume, "cpc": cpc})
                except (ValueError, IndexError):
                    continue
        return results

    def _parse_domain_csv(self, text: str) -> list[dict]:
        results = []
        lines = text.strip().split("\n")
        if len(lines) < 2:
            return results
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) >= 2:
                domain = parts[0].strip()
                url = parts[1].strip() if len(parts) > 1 else ""
                # Clean domain
                d = extract_domain(domain) or extract_domain(url)
                if d and not is_platform_domain(d):
                    results.append({"domain": d, "url": url})
        return results

    def has_paid_traffic(self, domain: str, database: str) -> bool:
        """V5.10: Check if a domain runs Google Ads (paid traffic != 0)."""
        text = self._request({
            "type": "domain_adwords",
            "domain": domain,
            "database": database,
            "display_limit": 1,
            "export_columns": "Ph,Po,Nq,Cp",
        })
        lines = [ln.strip() for ln in text.strip().split("\n") if ln.strip()]
        return len(lines) >= 2

    def get_domain_traffic_metrics(self, domain: str, database: str) -> dict:
        """V5.13: Get both organic AND paid traffic metrics for a domain.
        Returns dict with paid_keywords, organic_keywords, paid_traffic, organic_traffic.
        """
        result = {
            "paid_keywords": 0, "organic_keywords": 0,
            "paid_traffic": 0, "organic_traffic": 0,
        }
        # Paid metrics
        paid_text = self._request({
            "type": "domain_adwords",
            "domain": domain,
            "database": database,
            "display_limit": 5,
            "export_columns": "Ph,Po,Nq,Cp",
        })
        paid_lines = [l for l in paid_text.strip().split("\n") if l.strip()]
        result["paid_keywords"] = max(0, len(paid_lines) - 1)
        for line in paid_lines[1:]:
            parts = line.split(";")
            if len(parts) >= 3:
                try:
                    result["paid_traffic"] += int(parts[2].strip().replace(",", "") or "0")
                except ValueError:
                    pass
        # Organic metrics
        org_text = self._request({
            "type": "domain_organic",
            "domain": domain,
            "database": database,
            "display_limit": 5,
            "export_columns": "Ph,Po,Nq",
        })
        org_lines = [l for l in org_text.strip().split("\n") if l.strip()]
        result["organic_keywords"] = max(0, len(org_lines) - 1)
        for line in org_lines[1:]:
            parts = line.split(";")
            if len(parts) >= 3:
                try:
                    result["organic_traffic"] += int(parts[2].strip().replace(",", "") or "0")
                except ValueError:
                    pass
        return result

    def get_domain_organic_keywords(self, domain: str, database: str, limit: int = 30) -> list[dict]:
        """V5.27: Return organic keywords a domain ranks for, with position/url/volume/cpc/intent.
        Columns: Ph (phrase), Po (position), Nq (volume), Cp (cpc), Ur (url), In (intent).
        Returns list of dicts: {keyword, position, volume, cpc, url, intent}
        Intent codes (when returned): 0=commercial/navigational, 1=informational,
        2=navigational, 3=commercial, 4=transactional.
        """
        text = self._request({
            "type": "domain_organic",
            "domain": domain,
            "database": database,
            "display_limit": limit,
            "export_columns": "Ph,Po,Nq,Cp,Ur,In",
        })
        results = []
        lines = [l for l in text.strip().split("\n") if l.strip()]
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) >= 5:
                try:
                    kw = parts[0].strip().strip('"')
                    pos = int(parts[1].strip().replace(",", "") or "0")
                    vol = int(parts[2].strip().replace(",", "") or "0")
                    cpc = float(parts[3].strip().replace(",", "") or "0")
                    url = parts[4].strip() if len(parts) > 4 else ""
                    intent = parts[5].strip() if len(parts) > 5 else ""
                    results.append({
                        "keyword": kw, "position": pos, "volume": vol,
                        "cpc": cpc, "url": url, "intent": intent,
                    })
                except (ValueError, IndexError):
                    continue
        return results

    def get_keyword_overview(self, phrase: str, database: str) -> dict:
        """V5.27: Get keyword overview (volume, CPC, competition).
        Returns {'volume': int, 'cpc': float, 'competition': float}.
        """
        text = self._request({
            "type": "phrase_this",
            "phrase": phrase,
            "database": database,
            "export_columns": "Ph,Nq,Cp,Co",
        })
        lines = [l for l in text.strip().split("\n") if l.strip()]
        if len(lines) < 2:
            return {"volume": 0, "cpc": 0.0, "competition": 0.0}
        parts = lines[1].split(";")
        try:
            return {
                "volume": int(parts[1].strip().replace(",", "") or "0"),
                "cpc": float(parts[2].strip().replace(",", "") or "0") if len(parts) > 2 else 0.0,
                "competition": float(parts[3].strip().replace(",", "") or "0") if len(parts) > 3 else 0.0,
            }
        except (ValueError, IndexError):
            return {"volume": 0, "cpc": 0.0, "competition": 0.0}

    def get_organic_competitors(self, domain: str, database: str, limit: int = 10) -> list[str]:
        """V5.27: Return organic competitor domains for a target domain.
        Uses the domain_organic_organic report.
        """
        text = self._request({
            "type": "domain_organic_organic",
            "domain": domain,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Cr,Or,Ot",
        })
        competitors = []
        for line in text.strip().split("\n")[1:]:
            parts = line.split(";")
            if parts:
                d = extract_domain(parts[0].strip())
                if d and not is_platform_domain(d) and d != domain and d not in competitors:
                    competitors.append(d)
        return competitors[:limit]

    def get_domain_overview_metrics(self, domain: str, database: str) -> dict:
        """V5.28: True domain-wide totals via SEMrush domain_ranks report.
        Returns {organic_traffic, paid_traffic, organic_keywords, paid_keywords}.
        Unlike get_domain_traffic_metrics (which sums only the top N keywords),
        this returns the same numbers shown on the SEMrush Domain Overview UI.
        Columns: Or=organic kw, Ot=organic traffic, Ad=paid kw, At=paid traffic.
        """
        result = {
            "organic_traffic": 0, "paid_traffic": 0,
            "organic_keywords": 0, "paid_keywords": 0,
        }
        try:
            text = self._request({
                "type": "domain_ranks",
                "domain": domain,
                "database": database,
                "export_columns": "Db,Dn,Rk,Or,Ot,Oc,Ad,At,Ac",
            })
            lines = [l for l in text.strip().split("\n") if l.strip()]
            if len(lines) < 2:
                return result
            header = [h.strip() for h in lines[0].split(";")]
            values = lines[1].split(";")
            row = dict(zip(header, values))
            def _num(key):
                raw = (row.get(key) or "0").strip().replace(",", "")
                try:
                    return int(float(raw))
                except (ValueError, TypeError):
                    return 0
            result["organic_keywords"] = _num("Organic Keywords")
            result["organic_traffic"] = _num("Organic Traffic")
            result["paid_keywords"] = _num("Adwords Keywords")
            result["paid_traffic"] = _num("Adwords Traffic")
        except Exception:
            pass
        return result

    def get_domain_adwords_keywords(self, domain: str, database: str, limit: int = 10) -> list[dict]:
        """Phase 2: Return the paid (Google Ads) keywords a domain is bidding on.

        Used to surface NEW keyword opportunities from a strong seed domain —
        i.e. "if acme.com is outbidding everyone for 'emergency plumber sydney',
        other plumbing companies probably are too." Feeds back into the
        keyword pool so Phase 3 discovery has more coverage.

        Returns list of {keyword, position, volume, cpc}.
        """
        text = self._request({
            "type": "domain_adwords",
            "domain": domain,
            "database": database,
            "display_limit": limit,
            "export_columns": "Ph,Po,Nq,Cp",
        })
        results = []
        lines = [l for l in text.strip().split("\n") if l.strip()]
        for line in lines[1:]:
            parts = line.split(";")
            if len(parts) >= 4:
                try:
                    kw = parts[0].strip().strip('"')
                    pos = int(parts[1].strip().replace(",", "") or "0")
                    vol = int(parts[2].strip().replace(",", "") or "0")
                    cpc = float(parts[3].strip().replace(",", "") or "0")
                    if kw:
                        results.append({"keyword": kw, "position": pos, "volume": vol, "cpc": cpc})
                except (ValueError, IndexError):
                    continue
        return results

    def get_domain_competitors(self, domain: str, database: str, limit: int = 5) -> list[str]:
        """V5.13: Get paid traffic competitors for a domain.
        Returns list of competitor domain strings.
        """
        # Paid competitors
        text = self._request({
            "type": "domain_adwords_adwords",
            "domain": domain,
            "database": database,
            "display_limit": limit,
            "export_columns": "Dn,Ad,At,Ac",
        })
        competitors = []
        lines = text.strip().split("\n")
        for line in lines[1:]:
            parts = line.split(";")
            if parts:
                d = extract_domain(parts[0].strip())
                if d and not is_platform_domain(d) and d != domain:
                    competitors.append(d)
        # If paid competitors insufficient, try organic competitors
        if len(competitors) < limit:
            org_text = self._request({
                "type": "domain_organic_organic",
                "domain": domain,
                "database": database,
                "display_limit": limit,
                "export_columns": "Dn,Cr,Or,Ot",
            })
            for line in org_text.strip().split("\n")[1:]:
                parts = line.split(";")
                if parts:
                    d = extract_domain(parts[0].strip())
                    if d and not is_platform_domain(d) and d != domain and d not in competitors:
                        competitors.append(d)
        return competitors[:limit]


class SerpApiClient:
    """SerpApi client — optional fallback for domain discovery."""

    BASE_URL = "https://serpapi.com/search.json"

    def __init__(self, api_key):
        # 2026-06-01: multi-key rotation. `api_key` accepts either a single
        # string (legacy) or a comma/semicolon-separated list of keys. When
        # SerpAPI returns 429 ("run out of searches") for the active key, we
        # transparently advance to the next live key — letting the user chain
        # multiple free-tier accounts (each = 100 searches/month) without
        # touching any caller code. `_available` is a property whose SETTER
        # intercepts `_available = False` from existing 429 handlers and
        # rotates to the next live key; methods don't need to change.
        self._keys: list[str] = []
        if isinstance(api_key, (list, tuple, set)):
            self._keys = [str(k).strip() for k in api_key if str(k).strip()]
        else:
            for part in re.split(r"[,;\s]+", (api_key or "").strip()):
                if part:
                    self._keys.append(part)
        self._dead: set = set()       # indices of exhausted/invalid keys
        self._idx: int = 0
        # Initialize backing field BEFORE any property access. The property
        # body reads `_avail_scope` so this must exist first.
        self._avail_scope: bool = bool(self._keys)
        self.api_key: str = self._keys[self._idx] if self._keys else ""
        self.limiter = RateLimiter(0.8)
        self._counter: dict = {}
        # 2026-06-01: GLOBAL per-run call budget. The 2026-06-01 run made 253
        # SerpAPI calls for a 5-lead run — exhausting a 250-search key in ONE
        # go, so the NEXT run had zero balance. This hard cap (set by the
        # pipeline from the live remaining balance) bounds TOTAL calls across
        # every search method so the key survives multiple runs. Default is
        # effectively unlimited until the pipeline sets it.
        self._call_budget: int = 10**9
        self._calls_used: int = 0

    @property
    def _available(self) -> bool:
        """True iff (a) the caller hasn't externally disabled this client,
        (b) at least one key is still alive, AND (c) the run-wide SerpAPI
        budget isn't spent. Because EVERY public method begins with
        `if not self._available: return …`, making this budget-aware turns it
        into the single chokepoint that caps total SerpAPI spend run-wide
        (credit-saving mode) — discovery AND per-person enrichment alike."""
        if not (bool(self._avail_scope)
                and len(self._dead) < max(1, len(self._keys))
                and bool(self._keys)):
            return False
        return self._budget_left()

    @_available.setter
    def _available(self, val) -> None:
        """Setter intercepts the existing `self._available = False` calls in
        methods on 429 / 'run out of searches' / API error — instead of
        nuking the client, we ROTATE to the next live key. Setting to True
        re-enables (used by callers that re-arm every N keywords)."""
        if bool(val):
            self._avail_scope = True
            return
        # False → treat as "current key exhausted." Rotate.
        if self._keys:
            self._retire_current_key("HTTP 429 / run out of searches")

    def _budget_left(self) -> bool:
        """True if another SerpAPI call is allowed under the per-run budget.

        2026-06-09: prefer the SHARED, cross-instance budget stored in the
        common `_counter` dict. A single run spins up several SerpApiClient
        instances (city discovery + inner enrichment + export host) that all
        share one `_counter`, so a per-instance counter could never enforce a
        true run-wide cap — which is how a max_leads=1 run reached 153 calls.
        `serpapi_budget` (set once per run) + the shared `serpapi` call count
        give a single budget every instance + every method honours. Falls back
        to the legacy per-instance budget when the shared one isn't set."""
        _shared_budget = int((self._counter or {}).get("serpapi_budget", 0) or 0)
        if _shared_budget > 0:
            _shared_used = int((self._counter or {}).get("serpapi", 0) or 0)
            if _shared_used >= _shared_budget:
                if not (self._counter or {}).get("serpapi_budget_warned"):
                    try:
                        self._counter["serpapi_budget_warned"] = True
                    except Exception:
                        pass
                    log_cb = getattr(self, "_log_cb", None)
                    if callable(log_cb):
                        try:
                            log_cb(f"   [SerpAPI] ⛔ run-wide budget {_shared_budget} reached "
                                   f"({_shared_used} calls used) — pausing SerpAPI to stay "
                                   f"within the per-lead credit cap.")
                        except Exception:
                            pass
                return False
            return True
        # Legacy per-instance budget (no shared budget configured).
        if self._calls_used >= self._call_budget:
            if not getattr(self, "_budget_warned", False):
                self._budget_warned = True
                log_cb = getattr(self, "_log_cb", None)
                if callable(log_cb):
                    try:
                        log_cb(f"   [SerpAPI] ⛔ per-run call budget {self._call_budget} "
                               f"reached — pausing further SerpAPI calls to preserve "
                               f"remaining account balance for future runs.")
                    except Exception:
                        pass
            return False
        return True

    # ── multi-key rotation helpers ──────────────────────────────────────────

    def _retire_current_key(self, reason: str = "exhausted") -> None:
        """Mark the active key dead and advance to the next live one.
        Updates `self.api_key` so subsequent calls use the new key without
        per-method changes. Sets `self._available = False` only when no live
        keys remain."""
        if self._keys and self._idx < len(self._keys):
            _tail = self._keys[self._idx][-6:] if self._keys[self._idx] else "?"
            self._dead.add(self._idx)
            # Centralized log so the user can see the rotation happen.
            log_cb = getattr(self, "_log_cb", None)
            if callable(log_cb):
                try: log_cb(f"   [SerpAPI] key …{_tail} {reason} — rotating to next key "
                            f"({len(self._dead)}/{len(self._keys)} keys dead)")
                except Exception: pass
        # Advance to next non-dead key. Use the internal `_avail_scope`
        # backing field to avoid recursing through the property setter.
        for j in range(len(self._keys)):
            ni = (self._idx + 1 + j) % max(1, len(self._keys))
            if ni not in self._dead:
                self._idx = ni
                self.api_key = self._keys[ni]
                self._avail_scope = True
                return
        # No live keys left — disable client. Backing field, not property.
        self._avail_scope = False
        self.api_key = self._keys[self._idx] if self._keys else ""

    def precheck_keys(self, log_fn=None) -> list[dict]:
        """Hit SerpAPI's free /account endpoint per key to learn searches_left.
        Marks zero-balance + invalid keys dead BEFORE the run starts, so the
        first ads-only call doesn't waste latency rotating through dud keys.
        Returns a list of {key_tail, remaining, status} dicts for logging."""
        out = []
        log = log_fn or (lambda m: None)
        for i, k in enumerate(list(self._keys)):
            tail = (k or "")[-6:]
            try:
                r = requests.get("https://serpapi.com/account",
                                 params={"api_key": k}, timeout=15)
                j = r.json() if r.status_code == 200 else {}
                left = int(j.get("searches_left") or j.get("plan_searches_left") or 0)
                status = j.get("account_status") or ("ok" if r.status_code == 200 else f"HTTP {r.status_code}")
            except Exception as e:
                left, status = 0, f"err:{type(e).__name__}"
            out.append({"key_tail": tail, "remaining": left, "status": status})
            if left <= 0 or "active" not in (status or "").lower() and status != "ok":
                self._dead.add(i)
                log(f"   [SerpAPI] key …{tail}: {left} searches left, status={status} → marked dead")
            else:
                log(f"   [SerpAPI] key …{tail}: {left} searches left, status={status}")
        # Cache the total remaining across all keys so PASS 2a can shrink
        # its sweep to never exceed the total budget.
        self._last_precheck_total = sum(int(s.get("remaining", 0)) for s in out)
        # Reset _idx to first live key after pre-check.
        for j, _ in enumerate(self._keys):
            if j not in self._dead:
                self._idx = j
                self.api_key = self._keys[j]
                self._available = True
                return out
        self._available = False
        return out

    def total_remaining_searches(self) -> int:
        """Total searches remaining across all live keys from the last
        precheck. Returns 0 if no precheck has run."""
        return int(getattr(self, "_last_precheck_total", 0) or 0)

    def search_keyword(self, query: str, country_gl: str, num: int = 20) -> list[str]:
        """Search Google and return discovered domains."""
        if not self._available or not self._budget_left():
            return []
        self.limiter.wait()
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": num, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            self._calls_used += 1
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                # Rotated to next key (if any) — retry once on the new key.
                if self._available and self._budget_left():
                    return self.search_keyword(query, country_gl, num)
                return []
            if resp.status_code != 200:
                return []
            data = resp.json()
            if "error" in data:
                self._available = False
                if self._available and self._budget_left():
                    return self.search_keyword(query, country_gl, num)
                return []
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            return self._extract_domains(data)
        except Exception:
            return []

    def search_keyword_ads_only(self, query: str, country_gl: str, num: int = 20,
                                location: str = None) -> list[str]:
        """2026-05-25: Pull ONLY domains advertising in Google Ads for this query.

        Same SerpAPI endpoint as search_keyword(), but we drop organic_results
        and local_results from the response and keep only `ads[]` + `shopping_results[]`.
        These domains are paying for clicks RIGHT NOW so buyer-intent is the
        strongest possible signal among free discovery sources. Used by
        city_pipeline PASS 2a as an additive ads-only sweep that runs cheaply
        alongside the existing geo-aware search_keyword() pass.

        Same SerpAPI call billing as search_keyword (1 unit) — no extra credits.
        """
        if not self._available or not self._budget_left():
            return []
        self.limiter.wait()
        # 2026-06-01: use SerpApi's DEDICATED google_ads engine instead of
        # extracting ads[] from the generic google engine. The google_ads
        # engine ("scrape sponsored results at a higher rate than the
        # standard Google Search API") returns MORE advertiser domains per
        # search credit — important given the limited free/250 balance.
        # Same 1 credit per call. Configurable via SERP_ADS_ENGINE in case
        # the dedicated engine misbehaves (fallback: "google").
        _engine = (os.environ.get("SERP_ADS_ENGINE", "google_ads") or "google_ads").strip()
        params = {
            "engine": _engine,
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": num, "output": "json",
        }
        # 2026-06-02 (KEY FIX): Google Ads are CITY-targeted. The google_ads
        # engine returns 0 ads for a COUNTRY-level location ("Australia") but
        # real advertisers for a CITY-level one ("Sydney, Australia" →
        # auto-resolved to "Sydney, New South Wales, Australia"). Callers MUST
        # pass a city-level `location`; we only fall back to country-level if
        # none given (which yields no ads — logged so it's diagnosable).
        _loc = (location or "").strip() or self._location_for_gl(country_gl)
        if _loc:
            params["location"] = _loc
            params["hl"] = "en"
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            self._calls_used += 1
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                if self._available and self._budget_left():
                    return self.search_keyword_ads_only(query, country_gl, num, location)
                return []
            if resp.status_code != 200:
                # If the dedicated ads engine is rejected (e.g. plan doesn't
                # include it), retry ONCE on the standard google engine.
                if _engine != "google" and self._budget_left():
                    return self._ads_only_via_standard(query, country_gl, num, location)
                return []
            data = resp.json()
            if "error" in data:
                # Engine-level error (e.g. unsupported) → standard fallback.
                if _engine != "google" and self._available and self._budget_left():
                    return self._ads_only_via_standard(query, country_gl, num, location)
                self._available = False
                return []
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            return self._extract_ad_domains(data)
        except Exception:
            return []

    def _ads_only_via_standard(self, query: str, country_gl: str, num: int = 20,
                               location: str = None) -> list[str]:
        """Fallback: extract the ads[] block from the standard google engine
        (used only when the dedicated google_ads engine is unavailable on the
        account's plan). Counts as its own 1-credit call."""
        if not self._available or not self._budget_left():
            return []
        self.limiter.wait()
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": num, "output": "json",
        }
        _loc = (location or "").strip() or self._location_for_gl(country_gl)
        if _loc:
            params["location"] = _loc
            params["hl"] = "en"
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            self._calls_used += 1
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return []
            if resp.status_code != 200:
                return []
            data = resp.json()
            if "error" in data:
                self._available = False
                return []
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            return self._extract_ad_domains(data)
        except Exception:
            return []

    # 2026-06-01: gl(country) → SerpApi `location` string. Pins the google_ads
    # engine to the right COUNTRY so an AU run never inherits SerpApi's US
    # default. Country-level values are valid SerpApi locations.
    _GL_TO_LOCATION = {
        "au": "Australia", "us": "United States", "uk": "United Kingdom",
        "gb": "United Kingdom", "nz": "New Zealand", "ca": "Canada",
        "ie": "Ireland", "in": "India",
    }

    @classmethod
    def _location_for_gl(cls, country_gl: str) -> str:
        return cls._GL_TO_LOCATION.get((country_gl or "").strip().lower(), "")

    @staticmethod
    def _extract_ad_domains(data: dict) -> list[str]:
        """Pull advertiser domains from a SerpApi response's ad blocks.
        Handles BOTH the dedicated google_ads engine and the standard
        engine's ads[] / shopping_results[] shapes."""
        domains = set()
        for ad in (data.get("ads", []) or []):
            d = extract_domain(
                ad.get("link", "") or ad.get("tracking_link", "")
                or ad.get("displayed_link", "")
            )
            if d and not is_platform_domain(d):
                domains.add(d)
        for sh in (data.get("shopping_results", []) or []):
            d = extract_domain(sh.get("link", "") or sh.get("source", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        return list(domains)

    def _raw_search(self, query: str, country_gl: str, num: int = 5) -> dict:
        """V5.15: Return raw SerpAPI JSON (for snippet/title extraction).
        Used by Step 5e for full-name resolution from result snippets."""
        if not self._available:
            return {}
        self.limiter.wait()
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": num, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return {}
            if resp.status_code != 200:
                return {}
            data = resp.json()
            if "error" in data:
                return {}
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            return data
        except Exception:
            return {}

    def search_business_info(self, company_name: str, country_gl: str) -> dict:
        """Search for a company's phone/email via Google knowledge graph."""
        if not self._available:
            return {}
        self.limiter.wait()
        query = f'"{company_name}" phone number email contact'
        params = {
            "q": query, "gl": country_gl, "api_key": self.api_key,
            "num": 5, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=30)
            if resp.status_code != 200:
                return {}
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            data = resp.json()
            info = {}
            kg = data.get("knowledge_graph", {})
            if kg.get("phone"):
                info["phone"] = kg["phone"]
            if kg.get("email"):
                info["email"] = kg["email"]
            for local in data.get("local_results", {}).get("places", []):
                if not info.get("phone") and local.get("phone"):
                    info["phone"] = local["phone"]
            return info
        except Exception:
            return {}

    def find_business_phone(self, domain: str, company_name: str, country_gl: str) -> str:
        """V5.16: Multi-strategy phone finder — tries 3 query patterns to find a business phone."""
        if not self._available:
            return ""
        phone_re = re.compile(r'\+?[\d][\d\s\-\(\)\.]{6,14}[\d]')
        queries = [
            f'"{domain}" phone number contact',
            f'site:{domain} contact phone',
            f'"{company_name}" phone',
        ]
        for q in queries:
            self.limiter.wait()
            params = {"q": q, "gl": country_gl, "api_key": self.api_key, "num": 5, "output": "json"}
            try:
                resp = requests.get(self.BASE_URL, params=params, timeout=20)
                if resp.status_code != 200:
                    continue
                self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
                data = resp.json()
                if "error" in data:
                    self._available = False
                    return ""
                # Knowledge graph
                kg = data.get("knowledge_graph", {})
                if kg.get("phone"):
                    return kg["phone"]
                # Local results
                for place in data.get("local_results", {}).get("places", []):
                    if place.get("phone"):
                        return place["phone"]
                # Answer box
                ab = data.get("answer_box", {})
                if ab.get("phone"):
                    return ab["phone"]
                # Organic snippets
                for r in data.get("organic_results", []):
                    snippet = r.get("snippet", "") + " " + r.get("title", "")
                    m = phone_re.search(snippet)
                    if m:
                        candidate = m.group().strip()
                        if len(re.sub(r'\D', '', candidate)) >= 7:
                            return candidate
            except Exception:
                continue
        return ""

    def find_person_phone(self, person_name: str, domain: str,
                          company: str, country_gl: str) -> str:
        """V5.23: Search for a specific person's phone number via Google.
        Searches trade directories, company team pages, and Google snippets.
        Returns first candidate phone found. Used as Phase 4b fallback when
        Apollo/Lusha have no phone data for an individual."""
        if not self._available or not person_name:
            return ""
        phone_re = re.compile(r'\+?[\d][\d\s\-\(\)\.]{6,14}[\d]')
        queries = [
            f'"{person_name}" site:{domain}',
            f'"{person_name}" "{company}" phone mobile contact',
        ]
        for q in queries:
            data = self._raw_search(q, country_gl, num=5)
            if not data:
                continue
            # Check knowledge graph
            kg = data.get("knowledge_graph", {})
            if kg.get("phone"):
                return kg["phone"]
            # Search organic snippets for phone pattern
            for r in data.get("organic_results", []):
                snippet = r.get("snippet", "") + " " + r.get("title", "")
                m = phone_re.search(snippet)
                if m:
                    candidate = m.group().strip()
                    digits = re.sub(r'\D', '', candidate)
                    if len(digits) >= 7:
                        return candidate
        return ""

    def _extract_domains(self, data: dict) -> list[str]:
        domains = set()
        for result in data.get("organic_results", []):
            d = extract_domain(result.get("link", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        for ad in data.get("ads", []):
            d = extract_domain(ad.get("link", "") or ad.get("tracking_link", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        for place in data.get("local_results", {}).get("places", []):
            d = extract_domain(place.get("website", "") or place.get("link", ""))
            if d and not is_platform_domain(d):
                domains.add(d)
        return list(domains)

    def find_person_full_name(self, first_name: str, company_name: str,
                               domain: str, country_gl: str) -> str:
        """V5.2: Try to find a person's full name by searching Google for
        'FirstName CompanyName site:domain OR linkedin.com/in'."""
        if not self._available or not first_name:
            return ""
        self.limiter.wait()
        query = f'"{first_name}" "{company_name}" site:{domain} OR site:linkedin.com'
        params = {
            "q": query, "gl": country_gl,
            "api_key": self.api_key, "num": 5, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=20)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return ""
            if resp.status_code != 200:
                return ""
            data = resp.json()
            if "error" in data:
                self._available = False
                return ""
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            # Search snippets for "FirstName LastName" patterns
            name_pattern = re.compile(
                rf"\b{re.escape(first_name)}\s+([A-Z][a-z]{{1,20}})\b"
            )
            text_to_search = " ".join(
                r.get("snippet", "") + " " + r.get("title", "")
                for r in data.get("organic_results", [])
            )
            match = name_pattern.search(text_to_search)
            if match:
                return f"{first_name} {match.group(1)}"
            return ""
        except Exception:
            return ""

    def find_person_role(self, full_name: str, company_name: str, domain: str = "") -> str:
        """V5.28: Look up a person's role/title via SerpAPI when Apollo didn't return one.
        Strategy: query LinkedIn for "{full_name} {company}" and parse the title segment.
        LinkedIn titles look like: 'FirstName LastName - Title at Company | LinkedIn'
        Falls back to a non-LinkedIn search if LinkedIn returns nothing useful.
        Returns the parsed title or "" if nothing reliable found.
        """
        if not self._available or not full_name or " " not in full_name:
            return ""
        # Build queries — LinkedIn first, then a generic web search as fallback
        company_term = company_name or domain
        queries = []
        if company_term:
            queries.append(f'"{full_name}" "{company_term}" site:linkedin.com/in')
            queries.append(f'"{full_name}" {company_term}')
        else:
            queries.append(f'"{full_name}" site:linkedin.com/in')

        # Patterns to extract the title from common snippet/title layouts
        # 1) "FirstName LastName - Title at Company | LinkedIn"
        # 2) "FirstName LastName | Title at Company"
        # 3) "FirstName LastName, Title, Company"
        name_re = re.escape(full_name)
        pat_dash_at = re.compile(
            rf"{name_re}\s*[-\u2013\u2014|]\s*([A-Za-z0-9 &/,'\-\.]{{2,80}}?)\s+at\s+",
            re.IGNORECASE,
        )
        pat_dash = re.compile(
            rf"{name_re}\s*[-\u2013\u2014|]\s*([A-Za-z0-9 &/,'\-\.]{{2,80}}?)(?:\s*[\|\u2013\u2014\-]|\s+\u00b7|$)",
            re.IGNORECASE,
        )
        pat_comma = re.compile(
            rf"{name_re}\s*,\s*([A-Za-z0-9 &/'\-\.]{{2,80}}?)\s*(?:,|at|\||$)",
            re.IGNORECASE,
        )

        def _clean(t: str) -> str:
            t = re.sub(r"\s+", " ", t).strip(" -|,.\u2013\u2014")
            # Strip trailing connectors / company fragments
            t = re.sub(r"\s+(?:at|@|\u00b7|\|).*$", "", t, flags=re.IGNORECASE).strip()
            # Drop obvious junk
            if not t or len(t) < 2 or len(t) > 80:
                return ""
            low = t.lower()
            if low in ("linkedin", "profile", "professional profile"):
                return ""
            return t

        for q in queries:
            self.limiter.wait()
            params = {"q": q, "api_key": self.api_key, "num": 5, "output": "json"}
            try:
                resp = requests.get(self.BASE_URL, params=params, timeout=20)
                if resp.status_code == 429 or "run out of searches" in resp.text:
                    self._available = False
                    return ""
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if "error" in data:
                    self._available = False
                    return ""
                self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
                for result in data.get("organic_results", []):
                    title = result.get("title", "") or ""
                    snippet = result.get("snippet", "") or ""
                    for text in (title, snippet):
                        for pat in (pat_dash_at, pat_dash, pat_comma):
                            m = pat.search(text)
                            if m:
                                role = _clean(m.group(1))
                                if role:
                                    return role
            except Exception:
                continue
        return ""

    def find_person_on_linkedin(self, first_name: str, company_name: str) -> str:
        """V5.13: LinkedIn-targeted SerpApi query to extract full name from title snippet.
        Query: '{first_name} {company_name} site:linkedin.com'
        LinkedIn titles are typically: 'FirstName LastName - Title at Company | LinkedIn'
        """
        if not first_name:
            return ""
        # 2026-06-09: shared cache so the same (first name + company) LinkedIn
        # lookup is never paid for twice across rounds/instances (the run shares
        # one _counter). Cache hit = 0 SerpAPI credits.
        _ck = (first_name.strip().lower() + "|" + (company_name or "").strip().lower())
        _cache = self._counter.setdefault("_serp_li_cache", {}) if isinstance(self._counter, dict) else None
        if _cache is not None and _ck in _cache:
            return _cache[_ck]
        if not self._available:        # budget/keys gate AFTER cache check
            return ""
        self.limiter.wait()
        query = f"{first_name} {company_name} site:linkedin.com/in"
        params = {
            "q": query, "api_key": self.api_key, "num": 5, "output": "json",
        }
        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=20)
            if resp.status_code == 429 or "run out of searches" in resp.text:
                self._available = False
                return ""
            if resp.status_code != 200:
                return ""
            data = resp.json()
            if "error" in data:
                self._available = False
                return ""
            self._counter["serpapi"] = self._counter.get("serpapi", 0) + 1
            # LinkedIn titles: "FirstName LastName - Title | LinkedIn"
            # Parse full name from the title (most reliable field)
            name_pattern = re.compile(
                rf"\b({re.escape(first_name)}\s+[A-Z][a-zA-Z\-']{{1,25}})\b"
            )
            _found = ""
            for result in data.get("organic_results", []):
                title = result.get("title", "")
                snippet = result.get("snippet", "")
                for text in (title, snippet):
                    m = name_pattern.search(text)
                    if m:
                        candidate = m.group(1).strip()
                        parts = candidate.split()
                        if len(parts) == 2 and _is_valid_person_name(candidate):
                            _found = candidate
                            break
                if _found:
                    break
            if _cache is not None:        # cache hit AND miss → never re-query
                _cache[_ck] = _found
            return _found
        except Exception:
            return ""


class HunterClient:
    """Hunter.io API client — domain-level email search for personal contact discovery.
    Free plan: 25 requests/month. Set HUNTER_API_KEY env var to enable.
    """

    BASE_URL = "https://api.hunter.io/v2"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(1.5)  # Conservative — avoid rate limiting
        self._available = bool(api_key and len(api_key) > 5)
        self._counter = {}

    def domain_search(self, domain: str, limit: int = 10) -> list[dict]:
        """Search all emails at a domain via Hunter.io domain-search API.
        Returns list of dicts: {first_name, last_name, email, position, confidence}.
        Priority: higher confidence score = more reliable email.
        """
        if not self._available:
            return []
        self.limiter.wait()
        try:
            resp = requests.get(
                f"{self.BASE_URL}/domain-search",
                params={"domain": domain, "api_key": self.api_key, "limit": limit},
                timeout=15,
                headers={"User-Agent": _get_random_ua()},
            )
            if resp.status_code == 200:
                self._counter["hunter"] = self._counter.get("hunter", 0) + 1
                raw = resp.json().get("data", {}).get("emails", [])
                # Sort by confidence descending so best results come first
                raw.sort(key=lambda x: x.get("confidence", 0), reverse=True)
                return raw
            if resp.status_code == 429:
                self._available = False
            return []
        except Exception:
            return []


class ApolloClient:
    """Apollo.io API client for people search and organization enrichment."""

    BASE_URL = "https://api.apollo.io/api/v1"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.25)  # V5.1: Optimized from 0.4 (was 0.6 in V4)
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)
        self._log_cb = None  # V5.32: Optional pipeline log callback for diagnostics

    def _diag(self, msg: str) -> None:
        """V5.32: Write a diagnostic line via pipeline log callback (if attached)
        AND stderr so it's visible in terminal runs too."""
        if self._log_cb:
            try:
                self._log_cb(msg)
            except Exception:
                pass
        import sys as _sys
        print(msg, file=_sys.stderr, flush=True)

    def _headers(self) -> dict:
        return {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
            "X-Api-Key": self.api_key,
        }

    def search_people_by_domain(self, domain: str, per_page: int = 10) -> list[dict]:
        """Search for people at a domain using the new api_search endpoint.
        V5.19: Added reveal_personal_emails to capture any emails Apollo returns in search results.
        V5.26: Removed reveal_phone_number — mixed_people/api_search does NOT support it
        (silently ignored or causes 400). Phone reveal happens in enrich_person instead."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/mixed_people/api_search"
        payload = {
            "q_organization_domains": domain,
            "per_page": per_page,
            "reveal_personal_emails": True,   # V5.19: reveal any available personal emails
        }
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                people = resp.json().get("people", [])
                return people
            return []
        except Exception:
            return []

    def enrich_organization(self, domain: str) -> dict:
        """Get organization-level data including phone number."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/organizations/enrich"
        try:
            resp = requests.get(
                url, params={"domain": domain},
                headers=self._headers(), timeout=30
            )
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                org = resp.json().get("organization", {})
                _rev_str = _extract_apollo_revenue(org)
                return {
                    "company_name": org.get("name", ""),
                    "phone": org.get("phone", ""),
                    "website": org.get("website_url", ""),
                    "industry": org.get("industry", ""),
                    "employees": org.get("estimated_num_employees", ""),
                    "city": org.get("city", ""),
                    "linkedin": _extract_linkedin_url(org),
                    "revenue": _rev_str,  # V5.27: annual revenue
                }
            return {}
        except Exception:
            return {}

    def enrich_person(self, first_name: str, last_name: str, domain: str,
                      linkedin_url: str = "", organization_name: str = "",
                      apollo_id: str = "", company_phone: str = "") -> dict:
        """Try to enrich a person with email/phone. Uses LinkedIn URL for precise matching when available.
        V5.10+: organization_name improves Apollo matching when last_name is absent.
        V5.19: apollo_id enables exact record lookup, bypassing fuzzy name matching.
        V5.26: Webhook-based phone reveal — sends reveal request with webhook_url, phone data
        arrives async at /api/apollo-phone-callback. Falls back to non-reveal if no webhook URL."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/people/match"
        payload = {
            "reveal_personal_emails": True,
        }

        # V5.26: Phone reveal. Apollo returns personal phones via async webhook delivery for
        # most plan tiers. Sync delivery (no webhook_url) is supported on some plans but not
        # all — we request both channels so Apollo can pick whichever it can deliver.
        payload["reveal_phone_number"] = True
        _webhook_url = _get_webhook_url()
        if _webhook_url:
            payload["webhook_url"] = _webhook_url

        # V5.19: If we have the Apollo person ID from the initial search, use it for exact lookup.
        if apollo_id:
            payload["id"] = apollo_id
        if first_name:
            payload["first_name"] = first_name
        # V5.19: Don't pass an obfuscated initial as last_name
        if last_name and not (len(last_name.rstrip(".")) == 1 and last_name.rstrip(".").isalpha()):
            payload["last_name"] = last_name
        if domain:
            payload["domain"] = domain
        if linkedin_url:
            payload["linkedin_url"] = linkedin_url
        if organization_name:
            payload["organization_name"] = organization_name
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            # V5.32: Log first 422/400 per run so the user can see WHY reveal is being stripped
            if resp.status_code in (400, 422):
                if not getattr(self, "_logged_422", False):
                    self._logged_422 = True
                    _err_body = ""
                    try:
                        _err_body = str(resp.json())[:300]
                    except Exception:
                        _err_body = resp.text[:300]
                    # Detect credit exhaustion specifically — this is a fatal account-level issue
                    if "insufficient credits" in _err_body.lower():
                        self._diag(f"[Apollo CRITICAL] EXPORT CREDITS EXHAUSTED on key "
                                   f"{self._api_key[:8]}... — "
                                   f"people/match returns 422. Go to app.apollo.io > Settings > "
                                   f"API Keys and verify this key matches your account with credits. "
                                   f"Body: {_err_body}")
                    else:
                        self._diag(f"[Apollo DIAG] people/match returned {resp.status_code} on reveal. "
                                   f"Payload keys: {list(payload.keys())}. Body: {_err_body}")
                # V5.26: If reveal with webhook causes 400/422, retry without reveal
                payload.pop("reveal_phone_number", None)
                payload.pop("webhook_url", None)
                resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                _raw_json = resp.json()
                person = _raw_json.get("person", {})
                if person:
                    # V5.32: First response per run — log structure so we can see what Apollo returns
                    if not getattr(self, "_logged_first_match", False):
                        self._logged_first_match = True
                        _phones = person.get("phone_numbers") or []
                        _pers_emails = person.get("personal_emails") or []
                        _email_top = person.get("email") or ""
                        self._diag(f"[Apollo DIAG] First people/match response — "
                                   f"first_name={person.get('first_name')!r} "
                                   f"last_name={person.get('last_name')!r} "
                                   f"email={_email_top!r} "
                                   f"personal_emails_count={len(_pers_emails)} "
                                   f"phone_numbers_count={len(_phones)} "
                                   f"id={person.get('id')!r} "
                                   f"keys={list(person.keys())[:20]}")

                    _person_id = person.get("id", "")

                    # V5.26: Register this person for async phone collection
                    if _person_id and _webhook_url:
                        _register_phone_reveal(_person_id)

                    # V5.26: Check if webhook already delivered phone data (fast turnaround)
                    if _person_id and not person.get("phone_numbers"):
                        webhook_phones = _collect_phone_reveal(_person_id)
                        if webhook_phones:
                            person["phone_numbers"] = webhook_phones

                    first = safe_str(person.get('first_name'))
                    last = safe_str(person.get('last_name'))
                    email, _ = _pick_best_email_from_apollo(person, first, last)
                    phone, phone_quality = _pick_best_phone_from_apollo(person, company_phone)
                    result = {
                        "name": f"{first} {last}".strip() if last else first,
                        "role": safe_str(person.get("title")),
                        "email": email,
                        "phone": phone,
                        "_phone_quality": phone_quality,
                        "_apollo_id": _person_id,  # V5.26: Store for later phone collection
                        "company": safe_str((person.get("organization") or {}).get("name")),
                    }
                    return result
            else:
                # V5.32: Log non-200 responses so we can see what Apollo is doing
                if not getattr(self, "_logged_non200", False):
                    self._logged_non200 = True
                    self._diag(f"[Apollo DIAG] people/match returned HTTP {resp.status_code}: "
                               f"{resp.text[:300]}")
            return {}
        except Exception as _e:
            # V5.32: Don't silently swallow exceptions — first one per run is logged
            if not getattr(self, "_logged_exc", False):
                self._logged_exc = True
                self._diag(f"[Apollo DIAG] people/match exception: {_e}")
            return {}

    def search_email_verified_people(self, domain: str, per_page: int = 5) -> list:
        """V5.10+: Find people at domain who Apollo has contact emails for (any status).
        V5.14: Expanded to include 'unverified' status so more contacts with work emails
        are returned for enrichment — previously 'verified'+'likely_to_engage' only.
        Used in Step 2e as a last-resort email recovery pass."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/mixed_people/api_search"
        payload = {
            "q_organization_domains": domain,
            "contact_email_status": ["verified", "likely_to_engage", "unverified"],
            "per_page": per_page,
        }
        try:
            resp = requests.post(url, json=payload, headers=self._headers(), timeout=30)
            if resp.status_code == 200:
                self._counter["apollo"] = self._counter.get("apollo", 0) + 1
                return resp.json().get("people", [])
            return []
        except Exception:
            return []


class LushaClient:
    """Lusha API client — company enrichment and person lookup."""

    BASE_URL = "https://api.lusha.com"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.15)  # V5.1: Optimized from 0.2 (was 0.3 in V4)
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def _headers(self) -> dict:
        return {"api_key": self.api_key, "Content-Type": "application/json"}

    def get_company_info(self, domain: str) -> dict:
        """Get company information from Lusha company API v2."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/v2/company"
        try:
            resp = requests.get(
                url, params={"domain": domain},
                headers=self._headers(), timeout=30
            )
            if resp.status_code == 200:
                self._counter["lusha"] = self._counter.get("lusha", 0) + 1
                global _lusha_calls_total
                _lusha_calls_total += 1
                data = resp.json().get("data", {})
                if data:
                    return {
                        "company_name": data.get("name", ""),
                        "description": data.get("description", ""),
                        "domain": data.get("domain", ""),
                        "employees": data.get("employees", ""),
                        "industry": data.get("mainIndustry", ""),
                        "sub_industry": data.get("subIndustry", ""),
                        "linkedin": data.get("social", {}).get("linkedin", {}).get("url", ""),
                        "city": data.get("location", {}).get("city", ""),
                        "country": data.get("location", {}).get("country", ""),
                        "website": data.get("website", ""),
                    }
            return {}
        except Exception:
            return {}

    def enrich_person(self, first_name: str, last_name: str, company_domain: str) -> dict:
        """Enrich a person via Lusha Person API v2."""
        self.limiter.wait()
        url = f"{self.BASE_URL}/v2/person"
        try:
            resp = requests.get(
                url,
                params={
                    "firstName": first_name,
                    "lastName": last_name,
                    "companyDomain": company_domain,
                },
                headers=self._headers(),
                timeout=30,
            )
            if resp.status_code == 200:
                self._counter["lusha"] = self._counter.get("lusha", 0) + 1
                global _lusha_calls_total
                _lusha_calls_total += 1
                data = resp.json()
                contact = data.get("contact", {})
                if contact and contact.get("data"):
                    person_data = contact["data"]
                    first = safe_str(person_data.get('firstName'))
                    last = safe_str(person_data.get('lastName'))
                    result = {
                        "name": f"{first} {last}".strip() if last else first,
                        "role": safe_str(person_data.get("jobTitle")),
                        "email": "",
                        "phone": "",
                        "company": safe_str((person_data.get("company") or {}).get("name")),
                    }
                    if person_data.get("emails"):
                        # V5.22: Pick BUSINESS email first — prefer company domain over consumer.
                        # Priority: business/primary tagged > work tagged > company domain > consumer.
                        _business_email = ""
                        _work_email = ""
                        _company_email = ""
                        _consumer_email = ""
                        _first_email = ""
                        for em in person_data["emails"]:
                            addr = em.get("email", "")
                            if not addr:
                                continue
                            if not _first_email:
                                _first_email = addr
                            em_type = (em.get("type") or "").lower()
                            if "business" in em_type or "primary" in em_type:
                                if not _business_email:
                                    _business_email = addr
                            elif "work" in em_type:
                                if not _work_email:
                                    _work_email = addr
                            elif is_personal_email(addr):  # gmail/yahoo/hotmail
                                if not _consumer_email:
                                    _consumer_email = addr
                            else:
                                if not _company_email:
                                    _company_email = addr
                        result["email"] = (_business_email or _work_email or
                                           _company_email or _consumer_email or _first_email)
                    if person_data.get("phoneNumbers"):
                        # V5.22: Pick most personal phone from Lusha — prefer mobile/direct over landline/HQ
                        _lusha_type_scores = {"mobile": 50, "direct": 40, "personal": 35, "work": 15, "landline": 10, "other": 20}
                        _lusha_phones = []
                        for pn in person_data["phoneNumbers"]:
                            num = pn.get("number", "")
                            if not num:
                                continue
                            ptype = (pn.get("type") or "other").lower()
                            score = _lusha_type_scores.get(ptype, 20)
                            _lusha_phones.append((score, num))
                        if _lusha_phones:
                            _lusha_phones.sort(key=lambda x: x[0], reverse=True)
                            result["phone"] = _lusha_phones[0][1]
                            result["_phone_quality"] = _lusha_phones[0][0]  # V5.22: track quality
                        else:
                            result["phone"] = person_data["phoneNumbers"][0].get("number", "")
                            result["_phone_quality"] = 20
                    return result
            return {}
        except Exception:
            return {}


class OpenAIEmailVerifier:
    """OpenAI-powered email classification — determines if email is personal or generic."""

    API_URL = "https://api.openai.com/v1/chat/completions"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.limiter = RateLimiter(0.5)
        self._available = bool(api_key and len(api_key) > 10)
        self._counter = {}  # V5.7: Per-run API call counter (set by pipeline)

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def is_personal_email_ai(self, email: str, person_name: str = "", company_name: str = "") -> bool | None:
        """Use OpenAI to classify an email as personal or generic.
        Returns True (personal), False (generic), or None (API error/unavailable).
        """
        if not self._available or not email:
            return None
        self.limiter.wait()
        prompt = (
            f"Classify this email address as 'personal' or 'generic'.\n"
            f"Email: {email}\n"
            f"Person name: {person_name or 'unknown'}\n"
            f"Company: {company_name or 'unknown'}\n\n"
            f"CLASSIFICATION RULES:\n"
            f"1. If the email local part (before @) contains ANY word from the person's name "
            f"(case-insensitive) → PERSONAL (e.g. matt.cornell@ for 'Matt Cornell')\n"
            f"2. If the email local part contains a company name word (not a person's name) → GENERIC "
            f"(e.g. smithdental@ for company 'Smith Dental' but person 'John Doe')\n"
            f"3. Role-based prefixes (info, admin, sales, contact, hello, support, bookings, "
            f"enquiries, reception, practice, studio, office) → GENERIC\n"
            f"4. Unique non-role, non-company words → likely PERSONAL\n\n"
            f"Reply with ONLY one word: 'personal' or 'generic'."
        )
        try:
            resp = requests.post(
                self.API_URL,
                headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 10,
                    "temperature": 0,
                },
                timeout=15,
            )
            if resp.status_code == 401 or resp.status_code == 403:
                self._available = False
                return None
            if resp.status_code == 429:
                return None  # Rate limited, skip but don't disable
            if resp.status_code == 200:
                self._counter["openai"] = self._counter.get("openai", 0) + 1
                answer = resp.json()["choices"][0]["message"]["content"].strip().lower()
                return "personal" in answer
            return None
        except Exception:
            return None

    def infer_personal_email(self, first_name: str, last_name: str,
                              domain: str, company_name: str = "") -> list:
        """V5.7: Use OpenAI to generate likely personal email patterns for a person."""
        if not self._available or not first_name or not domain:
            return []
        self.limiter.wait()
        prompt = (
            f"Given a person's name and their company domain, generate the most likely "
            f"personal email addresses they would use at that domain.\n\n"
            f"Person: {first_name} {last_name}\n"
            f"Domain: {domain}\n"
            f"Company: {company_name or 'unknown'}\n\n"
            f"Common patterns: firstname@domain, firstname.lastname@domain, "
            f"firstnamelastname@domain, f.lastname@domain, flastname@domain\n\n"
            f"Reply with ONLY the email addresses, one per line, most likely first. "
            f"No explanations. Maximum 5 emails."
        )
        try:
            resp = requests.post(
                self.API_URL, headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 100,
                    "temperature": 0.2,
                },
                timeout=15,
            )
            if resp.status_code in (401, 403):
                self._available = False
                return []
            if resp.status_code == 200:
                self._counter["openai"] = self._counter.get("openai", 0) + 1
                text = resp.json()["choices"][0]["message"]["content"].strip()
                candidates = []
                for line in text.split("\n"):
                    line = line.strip().strip("-").strip("*").strip().strip("0123456789.").strip()
                    if "@" in line and "." in line.split("@")[-1]:
                        candidates.append(line.lower())
                return candidates[:5]
            return []
        except Exception:
            return []

    def select_top_service_keywords(self, keywords: list, industry: str,
                                    company_name: str = "", domain: str = "") -> list:
        """V5.27: Pick 1-2 HIGHLY relevant, high-ticket, commercial service keywords
        from a candidate list. Used to surface keyword opportunities the prospect
        is ranking for but NOT in the top 3 positions.

        `keywords` = list of dicts {keyword, position, volume, cpc, url, intent}.
        Returns a subset (1-2 items) of the same dicts — the LLM's picks, preserving
        all original fields so caller can read volume/position/url directly.
        """
        if not self._available or not keywords:
            return []
        # Cap candidates sent to the LLM to control tokens/cost
        candidates = keywords[:25]
        # Build a compact list for the prompt
        lines = []
        for i, kw in enumerate(candidates, start=1):
            lines.append(
                f"{i}. \"{kw.get('keyword','')}\" — pos {kw.get('position','')}, "
                f"vol {kw.get('volume','')}, cpc ${kw.get('cpc','')}"
            )
        kw_block = "\n".join(lines)
        self.limiter.wait()
        prompt = (
            f"You are a B2B lead-gen strategist. A prospect in the '{industry}' "
            f"industry (company: {company_name or 'unknown'}, domain: {domain or 'unknown'}) "
            f"ranks organically for these keywords but NOT in the top 3 positions.\n\n"
            f"Pick the 1 or 2 keywords that represent the MOST valuable, high-ticket, "
            f"commercial SERVICE or PRODUCT opportunity for this business. Prefer:\n"
            f"  • Commercial / transactional intent (people ready to buy / hire)\n"
            f"  • Core service keywords (not blog topics, not generic info queries)\n"
            f"  • High ticket services (implants, installations, major procedures, etc.)\n"
            f"  • Keywords where ranking higher would drive real revenue\n\n"
            f"Skip: informational queries, low-value terms, irrelevant topics, branded terms.\n\n"
            f"Candidate keywords:\n{kw_block}\n\n"
            f"Reply with ONLY the keyword numbers (1 or 2 numbers) separated by commas. "
            f"Example: '3, 7' or '12'. No explanation."
        )
        try:
            resp = requests.post(
                self.API_URL, headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 20,
                    "temperature": 0,
                },
                timeout=20,
            )
            if resp.status_code in (401, 403):
                self._available = False
                return []
            if resp.status_code == 200:
                self._counter["openai"] = self._counter.get("openai", 0) + 1
                answer = resp.json()["choices"][0]["message"]["content"].strip()
                picks = []
                for tok in re.findall(r"\d+", answer):
                    idx = int(tok) - 1
                    if 0 <= idx < len(candidates):
                        picks.append(candidates[idx])
                    if len(picks) >= 2:
                        break
                return picks
        except Exception:
            pass
        return []

    def get_business_niche(self, company_name: str, domain: str,
                            industry_hint: str = "") -> str:
        """V5.27: Use LLM to identify a concise business niche label for a company
        based on its name + domain. Returns a short phrase (2-6 words).
        Examples: 'Cosmetic dental clinic', 'Emergency plumbing services',
        'Residential roofing contractor'.
        """
        if not self._available:
            return ""
        if not company_name and not domain:
            return ""
        self.limiter.wait()
        prompt = (
            f"Identify the specific business niche for this company in 2 to 6 words. "
            f"Be concrete and specific (e.g. 'cosmetic dental clinic', "
            f"'emergency plumbing services', 'luxury residential architect').\n\n"
            f"Company name: {company_name or 'unknown'}\n"
            f"Domain: {domain or 'unknown'}\n"
            f"Industry hint: {industry_hint or 'unknown'}\n\n"
            f"Reply with ONLY the niche phrase — no quotes, no punctuation, "
            f"no explanation. If you cannot determine the niche, reply exactly: NA"
        )
        try:
            resp = requests.post(
                self.API_URL, headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 20,
                    "temperature": 0.1,
                },
                timeout=15,
            )
            if resp.status_code in (401, 403):
                self._available = False
                return ""
            if resp.status_code == 200:
                self._counter["openai"] = self._counter.get("openai", 0) + 1
                answer = resp.json()["choices"][0]["message"]["content"].strip()
                # Strip quotes/backticks/periods
                answer = answer.strip("'\"`. ").strip()
                if not answer or answer.upper() == "NA":
                    return ""
                # Cap length
                return answer[:80]
        except Exception:
            pass
        return ""

    def verify_dm_batch(self, leads: list, industry: str = "") -> dict:
        """V5.29: Batch-classify whether each lead's role is a genuine business
        decision-maker. Returns dict {f"{name}|{role}": bool_is_dm}.

        Used by Phase 5f to recover roles that the rule-based DM filter flagged
        as priority 0 but might still be genuine decision-makers (industry-specific
        titles, foreign-language titles, ambiguous phrases).
        """
        if not self._available or not leads:
            return {}
        batch = leads[:50]  # cap batch to bound token cost
        lines = []
        for i, ld in enumerate(batch, start=1):
            lines.append(
                f"{i}. role='{ld.get('role','')}' "
                f"company='{ld.get('company','')}' "
                f"name='{ld.get('name','')}'"
            )
        prompt = (
            f"For a B2B lead-gen campaign targeting {industry or 'business'} companies, "
            f"identify which contacts are GENUINE business decision-makers — meaning they "
            f"can sign off on vendor purchases or authorize new business engagements.\n\n"
            f"YES — only if the person is clearly one of:\n"
            f"  • Owner, Co-owner, Founder, Co-founder, Partner, Principal\n"
            f"  • CEO, COO, CFO, CTO, CMO, CIO, President, VP, Chief X\n"
            f"  • Managing Director, General Manager, Head of X, Director-level\n\n"
            f"NO — for all of:\n"
            f"  • Assistants, coordinators, specialists, analysts (any kind)\n"
            f"  • Engineers, designers, developers, technicians, operators\n"
            f"  • Sales reps, account managers, customer service, HR staff\n"
            f"  • Receptionists, secretaries, interns, juniors, apprentices\n"
            f"  • Tradespeople practicing the industry (electrician, plumber, dentist)\n"
            f"  • Anyone whose title doesn't clearly indicate executive authority\n\n"
            f"Reply with ONLY a comma-separated list of 'yes' or 'no' in the same "
            f"order as the input below. One token per person. No explanations.\n\n"
            + "\n".join(lines)
        )
        self.limiter.wait()
        try:
            resp = requests.post(
                self.API_URL, headers=self._headers(),
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": min(len(batch) * 6, 400),
                    "temperature": 0,
                },
                timeout=30,
            )
            if resp.status_code in (401, 403):
                self._available = False
                return {}
            if resp.status_code != 200:
                return {}
            self._counter["openai"] = self._counter.get("openai", 0) + 1
            answer = resp.json()["choices"][0]["message"]["content"].strip().lower()
            tokens = [t.strip() for t in re.split(r"[,\n\s]+", answer) if t.strip()]
            result = {}
            for i, ld in enumerate(batch):
                key = f"{ld.get('name','')}|{ld.get('role','')}"
                result[key] = bool(i < len(tokens) and tokens[i].startswith("y"))
            return result
        except Exception:
            return {}

    def verify_leads_batch(self, leads: list) -> None:
        """V5.4: Verify emails using 3-tier classification:
        1. OpenAI AI agent (best quality, uses enhanced prompt with name/company rules)
        2. Smart classifier (name-matching + company-matching heuristic)
        3. Basic generic prefix check (last resort)
        """
        for lead in leads:
            email = lead.get("email", "")
            if not email:
                lead["_email_type"] = ""
                continue

            # V5.7: Skip classification for inferred emails — they're name-based patterns
            if lead.get("_email_inferred"):
                lead["_email_type"] = "Inferred"
                continue

            person_name = lead.get("name", "")
            company_name = lead.get("company", "")

            # Tier 1: Try OpenAI AI agent first (most accurate)
            ai_result = self.is_personal_email_ai(email, person_name, company_name)

            if ai_result is not None:
                lead["_email_type"] = "Personal" if ai_result else "Generic"
            else:
                # Tier 2: Smart classifier with name/company cross-referencing
                smart_result = classify_email_smart(email, person_name, company_name)
                lead["_email_type"] = smart_result if smart_result != "Unknown" else (
                    "Personal" if is_personal_email(email) else "Generic"
                )


class WebScraper:
    """Free web scraper for extracting contact info from company websites."""

    CONTACT_PATHS = [
        "", "/contact",  # V5.1: Optimized to 2 paths (was 3 in V5, 12 in V4)
    ]

    # V5.13: Expanded team/about page paths for full-name extraction
    TEAM_PATHS = [
        "/about", "/about-us", "/team", "/our-team",
        "/staff", "/people", "/meet-the-team", "/meet-us",
        "/who-we-are", "/company", "/management", "/leadership",
        "/our-people", "/team-members", "/our-staff",
    ]

    def __init__(self, country_code: str = "AU"):
        self.country_code = country_code
        self.phone_regex = COUNTRY_CONFIG.get(country_code, COUNTRY_CONFIG["AU"])["phone_regex"]
        self.limiter = RateLimiter(0.3)
        self._headers = {"User-Agent": _get_random_ua()}

    def scrape_domain(self, domain: str) -> dict:
        """Scrape a domain for contact information."""
        result = {"emails": [], "phones": [], "company_name": "", "name_email_pairs": []}
        for path in self.CONTACT_PATHS:
            url = f"https://{domain}{path}"
            page_data = self._scrape_page(url)
            if page_data:
                result["emails"].extend(page_data.get("emails", []))
                result["phones"].extend(page_data.get("phones", []))
                result["name_email_pairs"].extend(page_data.get("name_email_pairs", []))
                if not result["company_name"] and page_data.get("company_name"):
                    result["company_name"] = page_data["company_name"]
        # Deduplicate
        result["emails"] = list(dict.fromkeys(e for e in result["emails"] if is_valid_email(e)))
        result["phones"] = list(dict.fromkeys(result["phones"]))
        # Deduplicate name_email_pairs by email
        seen_pair_emails = set()
        unique_pairs = []
        for pair in result["name_email_pairs"]:
            if pair["email"] not in seen_pair_emails:
                seen_pair_emails.add(pair["email"])
                unique_pairs.append(pair)
        result["name_email_pairs"] = unique_pairs
        return result

    def scrape_team_names(self, domain: str) -> list[dict]:
        """V5.13: Scrape team/about pages to extract staff full names.
        Returns list of {'name': str, 'email': str} dicts.
        Tries first 6 team paths. Uses _is_valid_person_name() guard.
        """
        found = []
        seen_names = set()
        for path in self.TEAM_PATHS[:6]:
            url = f"https://{domain}{path}"
            try:
                self.limiter.wait()
                self._headers["User-Agent"] = _get_random_ua()
                time.sleep(random.uniform(0.5, 1.5))
                resp = requests.get(url, headers=self._headers, timeout=8, allow_redirects=True)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                containers = soup.find_all(
                    ["div", "li", "article", "section"],
                    class_=re.compile(r"team|staff|member|person|profile|card|employee|director|partner|bio|people", re.I),
                )
                for container in containers:
                    container_text = container.get_text(separator=" ", strip=True)
                    name_matches = re.findall(r"\b([A-Z][a-z]{1,20}(?:\s+[A-Z][a-z]{1,20}){1,2})\b", container_text)
                    emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", container_text)
                    for nm in name_matches:
                        words = nm.split()
                        if all(w[0].isupper() and w.replace("'", "").replace("-", "").isalpha() for w in words):
                            # V5.13: Apply name validation guard
                            if _is_valid_person_name(nm):
                                key = nm.lower()
                                if key not in seen_names:
                                    seen_names.add(key)
                                    found.append({
                                        "name": nm,
                                        "email": emails[0] if emails else "",
                                    })
                            break  # One name per container
                # V5.15: Schema.org Person markup parsing
                schema_blocks = soup.find_all(
                    True,
                    attrs={"itemtype": re.compile(r"schema\.org/Person", re.I)}
                )
                for block in schema_blocks:
                    name_tag = block.find(attrs={"itemprop": "name"})
                    nm = name_tag.get_text(strip=True) if name_tag else ""
                    if nm and " " in nm and _is_valid_person_name(nm):
                        key = nm.lower()
                        if key not in seen_names:
                            seen_names.add(key)
                            found.append({"name": nm, "email": ""})

                # V5.15: JSON-LD Person parsing
                for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
                    try:
                        import json as _json
                        data = _json.loads(script.string or "")
                        items = data if isinstance(data, list) else [data]
                        for item in items:
                            if item.get("@type") in ("Person", "Employee"):
                                nm = item.get("name", "")
                                if nm and " " in nm and _is_valid_person_name(nm):
                                    key = nm.lower()
                                    if key not in seen_names:
                                        seen_names.add(key)
                                        found.append({"name": nm, "email": item.get("email", "")})
                    except Exception:
                        pass

                # V5.15: <meta name="author"> tags
                for meta in soup.find_all("meta", attrs={"name": "author"}):
                    nm = meta.get("content", "").strip()
                    if nm and " " in nm and _is_valid_person_name(nm):
                        key = nm.lower()
                        if key not in seen_names:
                            seen_names.add(key)
                            found.append({"name": nm, "email": ""})

                if len(found) >= 20:
                    break
            except Exception:
                continue
        return found[:20]

    def _extract_obfuscated_emails(self, soup: BeautifulSoup, text: str) -> list:
        """V5.13: Extract emails hidden behind common obfuscation techniques."""
        found = []
        # Pattern 1: [at] / (at) substitution
        deobf = re.sub(r'\s*\[at\]\s*|\s*\(at\)\s*|\s+AT\s+', '@', text, flags=re.I)
        deobf = re.sub(r'\s*\[dot\]\s*|\s*\(dot\)\s*|\s+DOT\s+', '.', deobf, flags=re.I)
        found.extend(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", deobf))
        # Pattern 2: HTML entity encoding (&#64; = @, &#46; = .)
        decoded = html_mod.unescape(text)
        found.extend(re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", decoded))
        # Pattern 3: data-email + data-domain split attributes
        for tag in soup.find_all(attrs={"data-email": True}):
            user = tag.get("data-email", "")
            domain = tag.get("data-domain", "")
            if user and domain:
                found.append(f"{user}@{domain}")
        # Pattern 4: mailto in javascript hrefs
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if "mailto:" in href.lower():
                email = re.sub(r"mailto:", "", href, flags=re.I).split("?")[0].strip()
                if email:
                    found.append(email)
        # Pattern 5: Simple JS string concatenation in inline <script>
        for script in soup.find_all("script"):
            script_text = script.get_text()
            concat_pattern = re.findall(
                r"""['"]([a-zA-Z0-9._%+-]+)['"]\s*\+\s*['"]?@['"]?\s*\+\s*['"]([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})['"]""",
                script_text
            )
            for user, dom in concat_pattern:
                found.append(f"{user}@{dom}")
        return [e for e in found if is_valid_email(e)]

    def _scrape_page(self, url: str) -> dict | None:
        self.limiter.wait()
        try:
            self._headers["User-Agent"] = _get_random_ua()
            resp = requests.get(url, headers=self._headers, timeout=10, allow_redirects=True)
            if resp.status_code == 429 or resp.status_code == 503:
                # V5.13: Exponential backoff for rate limiting
                for attempt in range(3):
                    time.sleep(2 ** (attempt + 1))
                    resp = requests.get(url, headers=self._headers, timeout=10, allow_redirects=True)
                    if resp.status_code == 200:
                        break
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator=" ", strip=True)

            # Emails from text + mailto links
            emails = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", text)
            for a_tag in soup.find_all("a", href=True):
                if a_tag["href"].startswith("mailto:"):
                    email = a_tag["href"].replace("mailto:", "").split("?")[0].strip()
                    if email:
                        emails.append(email)
            # V5.13: Extract obfuscated emails
            emails.extend(self._extract_obfuscated_emails(soup, text))
            emails = list(dict.fromkeys(emails))  # deduplicate preserving order

            # Phones from text + tel links
            phones = re.findall(self.phone_regex, text)
            for a_tag in soup.find_all("a", href=True):
                if a_tag["href"].startswith("tel:"):
                    phone = a_tag["href"].replace("tel:", "").strip()
                    if phone:
                        phones.append(phone)

            # Company name
            company_name = ""
            og_name = soup.find("meta", property="og:site_name")
            if og_name and og_name.get("content"):
                company_name = og_name["content"].strip()
            elif soup.title and soup.title.string:
                title_text = soup.title.string.strip()
                for sep in [" | ", " - ", " – ", " — ", " :: ", " : "]:
                    if sep in title_text:
                        company_name = title_text.split(sep)[0].strip()
                        break
                if not company_name:
                    company_name = title_text[:60]

            # Try to find name-email associations from structured HTML
            name_email_pairs = []
            for container in soup.find_all(
                ["div", "li", "article", "section"],
                class_=re.compile(r"team|staff|member|person|profile|card|employee|director|partner", re.I),
            ):
                container_text = container.get_text(separator=" ", strip=True)
                container_emails = re.findall(
                    r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", container_text)
                name_matches = re.findall(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b", container_text)
                if container_emails and name_matches:
                    for ce in container_emails:
                        # V5.13: Apply name validation guard
                        if is_valid_email(ce) and _is_valid_person_name(name_matches[0]):
                            name_email_pairs.append({"name": name_matches[0], "email": ce})

            return {
                "emails": emails[:10], "phones": phones[:10],
                "company_name": company_name, "name_email_pairs": name_email_pairs[:10],
            }
        except Exception:
            return None


# ══════════════════════════════════════════════════════════════════════════════
# V5.13: WHOIS FOUNDER VERIFICATION
# ══════════════════════════════════════════════════════════════════════════════

class WhoisFounderClient:
    """V5.13: WHOIS registrant lookup for founder identification."""

    def __init__(self):
        self._session = requests.Session()

    def get_registrant_name(self, domain: str) -> str:
        """Scrape whois.com for registrant/admin contact name."""
        try:
            time.sleep(random.uniform(1.5, 3.0))
            url = f"https://www.whois.com/whois/{domain}"
            r = self._session.get(url, headers={"User-Agent": _get_random_ua()}, timeout=15)
            if r.status_code != 200:
                return ""
            text = r.text
            for pattern in [
                r"Registrant Name:\s*(.+)",
                r"Admin Name:\s*(.+)",
                r"Tech Name:\s*(.+)",
                r"Registrant Contact Name:\s*(.+)",
            ]:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    name = m.group(1).strip()
                    if any(w in name.lower() for w in [
                        "privacy", "redacted", "proxy", "domain", "whois",
                        "protected", "not disclosed", "n/a", "identity",
                        "registration", "private", "domains by",
                    ]):
                        continue
                    if _is_valid_person_name(name):
                        return name
            return ""
        except Exception:
            return ""

    @staticmethod
    def find_founder_in_leads(whois_name: str, leads: list):
        """Fuzzy-match WHOIS registrant name against existing leads."""
        if not whois_name or not leads:
            return None
        wn_lower = whois_name.lower().split()
        wn_first = wn_lower[0] if wn_lower else ""
        wn_last = wn_lower[-1] if len(wn_lower) > 1 else ""
        for ld in leads:
            ld_name = (ld.get("name") or "").lower().split()
            if not ld_name:
                continue
            ld_first = ld_name[0]
            ld_last = ld_name[-1] if len(ld_name) > 1 else ""
            if ld_first == wn_first and (ld_last == wn_last or wn_last in ld_last or ld_last in wn_last):
                return ld
        return None


# ══════════════════════════════════════════════════════════════════════════════
# V5.8: SMART LEAD RELEVANCE SCORING
# ══════════════════════════════════════════════════════════════════════════════

def _calculate_lead_relevance_score(title: str, has_email: bool = False) -> float:
    """V5.11: Trade-aware decision-maker relevance scoring.

    Score tiers:
    - 95  = HARD decision-maker (owner, CEO, director, founder) — always DM
    - 75  = SOFT decision-maker (manager, head) without trade context
    - 40  = Trade professional (plumber, electrician etc.) — industry worker, NOT DM
    - 20  = Support/admin staff — skip expensive enrichment
    - 30  = No title — neutral

    The trade-override rule: if title contains a TRADE_ROLE_WORD AND no HARD_DM keyword,
    the person is a tradesperson, not a decision maker.
    Example: "Lead Plumber" → score 40 (not DM); "Owner / Plumber" → score 95 (DM).
    """
    if not title:
        return 30

    title_lower = title.lower()

    # Tier 0: Low-relevance admin/support → skip enrichment immediately
    for keyword in LOW_RELEVANCE_KEYWORDS:
        if keyword in title_lower:
            return 20

    # Tier 1: HARD DM keywords → always a genuine decision maker
    for keyword in HARD_DM_KEYWORDS:
        if keyword in title_lower:
            return 95 + (5 if has_email else 0)

    # Check if title contains a trade/craft word
    is_trade_role = any(trade in title_lower for trade in TRADE_ROLE_WORDS)

    if is_trade_role:
        # Trade professional — this is an industry worker, NOT a decision maker
        # Even "Senior Plumber" or "Lead Electrician" is not a DM
        return 40

    # Tier 2: SOFT DM keywords (manager, head, supervisor) — valid DM only without trade context
    for keyword in SOFT_DM_KEYWORDS:
        if keyword in title_lower:
            return 75

    # Default: regular professional
    return 55


def _filter_people_by_relevance(people: list, max_leads: int) -> list:
    """V5.8: Filter and sort people by relevance to reduce API calls.

    - Calculate relevance score for each person
    - Keep top N*2 people (accounts for failures/partial data)
    - Skip low-relevance people entirely (don't make expensive API calls)

    Example: max_leads=20 → keep top ~40-50 people to account for incomplete data
    """
    if not max_leads or max_leads <= 0:
        return people  # No filtering if max_leads not set

    # Score each person
    scored = []
    for person in people:
        title = safe_str(person.get("title", ""))
        has_email = bool(person.get("personal_emails") or person.get("email"))
        score = _calculate_lead_relevance_score(title, has_email)
        # V5.11: Hard DM threshold — only enrich people scoring >= 55 (skip pure trade workers)
        scored.append((score, person))

    # Sort by score descending, keep top N*2.5 (with buffer for failures)
    scored.sort(key=lambda x: x[0], reverse=True)
    keep_count = max(10, int(max_leads * 2.5))  # Keep at least 10, up to 2.5x max_leads

    return [person for _, person in scored[:keep_count]]


# ══════════════════════════════════════════════════════════════════════════════
# LEAD GENERATION PIPELINE
# ══════════════════════════════════════════════════════════════════════════════


class LeadGenerationPipeline:
    """Orchestrates the complete 6-phase lead generation pipeline."""

    # 2026-06-02: major cities per country for the google_ads CITY-level
    # location param (industry mode has no city list of its own). Google Ads
    # are city-targeted, so we sweep these to surface real advertisers.
    _MAJOR_CITIES_BY_COUNTRY = {
        "AU": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
        "USA": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "US": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "UK": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
        "NZ": ["Auckland", "Wellington", "Christchurch"],
        "CA": ["Toronto", "Vancouver", "Montreal", "Calgary"],
    }

    def __init__(
        self,
        industry: str,
        country: str,
        min_volume: int,
        min_cpc: float,
        output_folder: str,
        progress_callback=None,
        log_callback=None,
        max_leads: int = 0,
        enrichment_enabled: bool = True,
        preset_keywords: list | None = None,
        preset_domains: list | None = None,
        confirmed_paid_domains: list | set | None = None,  # PHASE 2 (2026-04-28): see _enrich_single_domain SEMrush gate
        city_scope: dict | None = None,
        quota_guarantee: bool = False,
        lead_pool=None,             # Phase 2: shared LeadPool for dedup across primary + secondary agent
        # Phase 2 (2026-05-15): credit-safety flags from city_pipeline.
        # When silent_scope=True, SEMrush has no AU paid-ads data for this
        # scope — Phase 4 skips the per-domain SEMrush domain_overview call
        # (was 141 wasted calls in the previous 3-lead doomed run) and
        # relaxes the strict paid_traffic<1 gate so Apollo-discovered local
        # businesses can still convert to leads.
        semrush_silent_scope: bool = False,
        apollo_only_domains: set | None = None,
        # 2026-05-18 (round 2): a counter dict passed by REFERENCE from the
        # outer city pipeline so the inner pipeline shares the SAME budget
        # pool. When None we allocate our own (existing standalone path).
        shared_counter: dict | None = None,
        # 2026-05-18 (round 4): AU businesses discovered via Google Places
        # Text Search by city_pipeline (additive layer, NOT confirmed paid).
        # Used ONLY for source-label stamping on the exported CSV and for
        # the silent-scope gate relaxation — never bypasses paid_traffic
        # gate as confirmed paid.
        google_intent_domains: set | None = None,
        # 2026-05-28: domains confirmed running LIVE Google Ads (found in
        # SerpAPI's ads[] block). Subset of confirmed_paid — tracked
        # separately ONLY for the CSV "Traffic Source = Google Ads" label so
        # the user can distinguish live advertisers from SEMrush-paid.
        serp_ads_domains: set | None = None,
        # 2026-05-28: {domain: advertiser_id} from the Ads Transparency
        # Center, for the CSV proof columns (ATC Verified / Advertiser ID /
        # Ad Library URL) — Google's OWN evidence the business runs ads.
        atc_advertiser_ids: dict | None = None,
        # 2026-06-02: advertiser tiering. heavy = domain seen in SerpAPI live
        # Search ads[] on >= N distinct queries (strongest Ahrefs/SEMrush
        # paid-traffic signal). atc_only = ATC-confirmed but NOT in SerpAPI
        # Search ads (may be Display/YouTube/historical → often 0 Ahrefs
        # SEARCH paid). Used by Phase 5f to pick true Search advertisers first.
        heavy_advertiser_domains: set | None = None,
        atc_only_domains: set | None = None,
        # 2026-06-08: "SerpAPI only" toggle. When True the run fully bypasses
        # SEMrush (even if its key has credits) — discovery resolves to
        # GOOGLE_ONLY/APOLLO_ONLY and every SemrushClient call no-ops.
        disable_semrush: bool = False,
        # 2026-06-09: credit-saving mode (DEFAULT). Caps run-wide SerpAPI spend
        # to ~max_leads×14 calls. False = "regular" thorough mode (generous
        # budget) — the UI toggle, default OFF, flips this to False.
        credit_saver: bool = True,
        # 2026-06-11: "paid only — keep all confirmed advertisers" mode. When True,
        # the export keeps EVERY confirmed advertiser (tier>=1) with no max_leads
        # ceiling and drops unverified leads. Max Leads becomes the floor/target.
        paid_only_all: bool = False,
    ):
        self.industry = industry
        self.country = country
        self.disable_semrush = bool(disable_semrush)
        self.credit_saver = bool(credit_saver)
        self.paid_only_all = bool(paid_only_all) or str(os.environ.get("PAID_ONLY_ALL", "0")).strip() == "1"
        self.min_volume = min_volume
        self.min_cpc = min_cpc
        self.output_folder = output_folder
        self.progress_callback = progress_callback or (lambda *a: None)
        self.log_callback = log_callback or (lambda *a: None)
        self.max_leads = max_leads
        # PHASE 2 TOGGLE: when False, skip every Apollo/Lusha/Hunter/OpenAI
        # enrichment step — return just basic info from Apollo's people-search
        # response. Phone/Email columns are blanked in the CSV output.
        self.enrichment_enabled = enrichment_enabled
        # City mode can ask for exact-row exports. Existing industry mode keeps
        # the strict-first behavior by leaving this flag off.
        self.quota_guarantee = bool(quota_guarantee)
        # PHASE 2 CITY MODE: when set, Phases 1-3 are bypassed and the
        # pipeline jumps straight into Phase 4 using these presets.
        # city_scope (optional) = {"state": str, "city": str, "label": str}
        # attached to each lead as _city_scope for reporting.
        self._preset_keywords = list(preset_keywords) if preset_keywords else None
        self._preset_domains = list(preset_domains) if preset_domains else None
        # PHASE 2 (2026-04-28) — domains the caller already verified to be paid
        # advertisers (e.g. came from SEMrush phrase_adwords in city_pipeline).
        # _enrich_single_domain skips its strict paid_traffic / organic_keywords
        # gate for these — re-checking would be double-counting (SEMrush already
        # confirmed they bid on relevant keywords). Apollo-fallback / SerpAPI
        # discoveries are NOT included here, so they DO face the full gate.
        self._confirmed_paid_domains: set = set(
            (d or "").lower() for d in (confirmed_paid_domains or []) if d
        )
        # Phase 2 (2026-05-15): credit-safety state set by city_pipeline.
        # See _enrich_single_domain for usage.
        self._semrush_silent_scope: bool = bool(semrush_silent_scope)
        self._apollo_only_domains: set = set(
            (d or "").lower() for d in (apollo_only_domains or set()) if d
        )
        # 2026-05-18 (round 4): Google Places-discovered AU domains (additive).
        # These ride through the same enrichment chain as any other domain
        # but get a distinct `source = Google Intent` label on the CSV
        # export. They DO NOT enter `_confirmed_paid_domains` — the paid-
        # traffic gate still applies; we only relax it in silent-scope
        # mode (mirroring how Apollo-only domains are handled).
        self._google_intent_domains: set = set(
            (d or "").lower() for d in (google_intent_domains or set()) if d
        )
        # 2026-05-28: live-Google-Ads-confirmed domains (SerpAPI ads[] block).
        # Subset of _confirmed_paid_domains; used for the "Google Ads" label.
        self._serp_ads_domains: set = set(
            (d or "").lower() for d in (serp_ads_domains or set()) if d
        )
        # 2026-05-28: {domain: advertiser_id} for ATC proof columns.
        self._atc_advertiser_ids: dict = {
            (k or "").lower(): v for k, v in (atc_advertiser_ids or {}).items() if k and v
        }
        # 2026-06-02: advertiser tiers for the final paid-first selection.
        self._heavy_advertiser_domains: set = set(
            (d or "").lower() for d in (heavy_advertiser_domains or set()) if d
        )
        self._atc_only_domains: set = set(
            (d or "").lower() for d in (atc_only_domains or set()) if d
        )
        self._city_scope = city_scope or None
        self._cancelled = False
        self._phone_leads_count = 0      # V5.10: phone-bearing leads counter (credit gate)
        self._topup_active = False       # True during Phase 5f top-up — bypasses credit gate
        self._complete_leads_lock = threading.Lock()  # shared lock for both counters
        self._adwords_domains: set = set()   # V5.10+: domains confirmed directly via adwords
        self._organic_domains: set = set()   # V5.12: PAID-ONLY mode (empty set, no organic)
        self._organic_fallback_domains: set = set()  # V5.12: Fallback domains from Apollo org search (organic)
        self._email_credits_used = 0         # V5.10+: API calls that yielded personal email
        self._phone_credits_used = 0         # V5.10+: API calls that yielded direct phone
        self._log_lock = threading.Lock()  # V5.1: Thread-safe logging

        self.config = COUNTRY_CONFIG[country]

        # V5.7: Per-run API call counter
        # 2026-05-18 (round 2): if a shared_counter was provided (city mode),
        # bind to it so the outer pipeline and this inner pipeline share ONE
        # SEMrush budget pool. Otherwise allocate a fresh dict (standalone).
        if shared_counter is not None:
            self._api_counter = shared_counter
            # Make sure the canonical keys exist (for code that does +=).
            for _k in ("apollo", "lusha", "semrush", "serpapi", "openai", "hunter"):
                self._api_counter.setdefault(_k, 0)
        else:
            self._api_counter = {
                "apollo": 0, "lusha": 0, "semrush": 0,
                "serpapi": 0, "openai": 0, "hunter": 0,
            }

        # V5.32: Apollo hard budget — cap total Apollo API calls per run
        self._apollo_budget = int(max_leads * 30) if max_leads > 0 else 999999
        # V5.32: Apollo credits snapshot — filled at run start, used for delta at end
        self._apollo_credits_at_start = -1

        # API clients
        self.semrush = SemrushClient(API_KEYS["semrush"])
        # "SerpAPI only" toggle → hard-bypass every SEMrush call this run.
        self.semrush._disabled = self.disable_semrush
        self.serpapi = SerpApiClient(API_KEYS["serpapi"])
        self.apollo = ApolloClient(API_KEYS["apollo"])
        self.lusha = LushaClient(API_KEYS["lusha"])
        self.hunter = HunterClient(API_KEYS.get("hunter", ""))  # V5.13: Hunter.io email enrichment
        self.openai_verifier = OpenAIEmailVerifier(API_KEYS.get("openai", ""))
        self.scraper = WebScraper(country)
        self.whois_client = WhoisFounderClient()  # V5.13: WHOIS founder verification
        self._competitor_domains_added = 0  # V5.13: Competitor expansion counter
        # Phase 2: new tracking fields (consumed by wsgi.py for run_history)
        self._competitor_depth_reached = 0
        self._competitor_calls_made = 0
        self._secondary_agent_used = False
        self._secondary_domains_added = 0
        self._paid_kw_expansion_added = 0
        self._master_leads_deduped_out = 0      # leads dropped because already in master_leads
        # Shared LeadPool — injected by caller (wsgi.py) so the secondary agent
        # and any future co-runner share one dedup set. Fallback to a standalone
        # instance so stand-alone CLI usage still works.
        try:
            from lead_pool import LeadPool as _LP
            self._lead_pool = lead_pool if lead_pool is not None else _LP(skip_master_known=True)
        except Exception:
            self._lead_pool = None

        # Wire counter reference to each client
        self.semrush._counter = self._api_counter
        self.serpapi._counter = self._api_counter
        self.apollo._counter = self._api_counter
        self.apollo._log_cb = self._log  # V5.32: pipe Apollo diagnostics into run log
        self.lusha._counter = self._api_counter
        self.hunter._counter = self._api_counter
        self.openai_verifier._counter = self._api_counter

        # 2026-05-18: SEMrush per-run unit budget — the ONE knob that caps
        # weighted credit consumption across every phase, every module, every
        # call site. Formula matches the user's stated efficiency target:
        # "for 3 leads, no more than 100-200 credits". Scaling:
        #   max_leads=1     → 300 units     max_leads=10   → 1 000 units
        #   max_leads=3     → 300 units     max_leads=50   → 5 000 units
        #   max_leads=200   → 20 000 units  unlimited (0)  → 25 000 units
        # The ceiling is high enough for genuine 200-lead runs while still
        # being a hard wall against runaway BFS / per-domain insights bursts.
        # If this run is in silent scope, we further halve the budget — there
        # is no signal worth paying for once both SEMrush + SerpAPI returned 0.
        _mx = int(self.max_leads or 0)
        if _mx <= 0:
            _budget = 25000
        else:
            _budget = max(300, min(20000, _mx * 100))
        if self._semrush_silent_scope:
            _budget = max(150, _budget // 4)
        self.semrush._unit_budget = _budget
        self.semrush._units_used = 0
        self.semrush._units_by_phase = {}
        self.semrush._current_phase = "init"
        self.semrush._budget_alert_75_fired = False
        self.semrush._budget_exhausted_logged = False
        self.semrush._log_cb = self._log
        self._semrush_unit_budget = _budget
        # 2026-05-18 (round 2): also publish to the shared counter so EVERY
        # SemrushClient instance that shares this counter (city_pipeline's
        # Pass-3 discovery + rediscovery wave) reads the same budget cap +
        # the same running total. Honour any pre-existing higher budget
        # already in the counter (the outer city pipeline may have set one
        # — we never want to silently DOWNGRADE an outer budget).
        _existing_budget = int(self._api_counter.get("semrush_budget", 0) or 0)
        if _existing_budget <= 0 or _budget > _existing_budget:
            self._api_counter["semrush_budget"] = _budget
        # Don't reset semrush_units if the outer scope has already spent some
        # (it would be a bug to forget that and let inner blow the budget).
        self._api_counter.setdefault("semrush_units", 0)
        self._api_counter.setdefault("semrush_units_by_phase", {})
        self._api_counter.setdefault("semrush_skipped", 0)
        self._api_counter.setdefault("semrush_budget_alert_75", False)
        self._api_counter.setdefault("semrush_budget_exhausted_logged", False)
        self._log(
            f"[SEMrush] Per-run unit budget set to {_budget} "
            f"(max_leads={_mx}, silent_scope={self._semrush_silent_scope}, "
            f"already_used={self._api_counter.get('semrush_units', 0)})"
        )

        # Data stores
        self.keywords: list[str] = []
        self.domains: list[str] = []
        self.leads: list[dict] = []
        # 2026-05-18 (round 3): per-domain SEMrush cache. Phase 4 fetches
        # `domain_overview_metrics` for the gate check; Phase 5c was fetching
        # the SAME report again, and Phase 5h was fetching it a THIRD time.
        # Same for `organic_competitors` (5c + 5h) and `domain_organic_keywords`
        # (5c + 5h). This cache makes each call happen AT MOST ONCE per run
        # per domain — saves up to 280 units * unique-domain count in
        # avoidable re-fetches. Threading: writes are wrapped by callers that
        # hold the domain's row, OR they're serial (Phase 5c/5h run on the
        # main thread). Phase 4 is multi-threaded so we use a lock there.
        self._semrush_domain_cache: dict = {}
        self._semrush_cache_lock = threading.Lock()

    def cancel(self):
        self._cancelled = True

    def _log(self, msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        try:
            self.log_callback(f"[{timestamp}] {msg}")
        except Exception as e:
            print(f"[LOG ERROR] {e}", flush=True)

    def _progress(self, pct: int, status: str = ""):
        try:
            self.progress_callback(pct, status)
        except Exception:
            pass  # Ignore progress callback errors

    def _has_enough_leads(self) -> bool:
        """V5.10: True when we have max_leads phone-bearing leads (credit gate).
        Phone is the expensive signal — Apollo/Lusha enrich_person reveals phone.
        Gate triggers at exactly max_leads phones (+ 20% buffer for cleanup losses).
        Returns False when max_leads=0 (unlimited mode).
        PHASE 2: when enrichment is disabled, phone counter never moves — gate
        on raw lead count instead so we don't enumerate every discovered domain.
        """
        if self.max_leads <= 0:
            return False
        # During Phase 5f top-up, never block — we already know we're short of quota
        if getattr(self, '_topup_active', False):
            return False
        if getattr(self, "quota_guarantee", False):
            # In guarantee mode the raw/phone counter is not a valid stop signal:
            # later filters and domain caps can shrink hundreds of raw people.
            if len(getattr(self, "leads", []) or []) < self.max_leads:
                return False
        with self._complete_leads_lock:
            if not self.enrichment_enabled:
                # 2026-05-25: enrichment-OFF early-exit BUGFIX. The old check
                # only watched `_phone_leads_count`, but enrichment-OFF never
                # reveals phones → counter stays at 0 → loop enumerates EVERY
                # discovered domain (251 for a 3-lead run!) → spends ~5 min
                # of Apollo people-search calls. Stub leads bear no phone
                # either, so they don't help that counter.
                # New rule: stop when EITHER the phone-count quota is met
                # (rare with enrichment-OFF) OR the raw lead pool already
                # has 4× max_leads — that's enough buffer for Phase 5f's
                # DM filter + per-company cap to drop down to max_leads.
                _raw = len(getattr(self, "leads", []) or [])
                return (
                    self._phone_leads_count >= int(self.max_leads * 12)
                    or _raw >= int(self.max_leads * 4)
                )
            # V5.29: 2x buffer (was 1.5x) — Phase 5f drops non-DMs and caps 2/company,
            # so we need a larger raw pool to reliably hit max_leads after filtering.
            return self._phone_leads_count >= int(self.max_leads * 2)

    def _apollo_budget_ok(self) -> bool:
        """V5.32: True if Apollo budget allows more calls."""
        return self._api_counter.get("apollo", 0) < self._apollo_budget

    def _fetch_apollo_credits_remaining(self) -> int:
        """V5.32: Query Apollo for remaining credits. /auth/health only returns
        {healthy, is_logged_in} — tried /usage_stats/api_usage which does return credit pools.
        Returns -1 if query fails (don't block pipeline on this side channel)."""
        headers = {"X-Api-Key": API_KEYS.get("apollo", "")}
        # Endpoint 1: usage_stats/api_usage (correct endpoint for credit balances)
        try:
            r = requests.get(
                "https://api.apollo.io/api/v1/usage_stats/api_usage",
                headers=headers, timeout=10,
            )
            if r.status_code == 200:
                data = r.json() or {}
                # Response shape: {"usage": {"credits_used": N}, "limits": {"credits": N}} or similar
                limits = data.get("limits") or data.get("plan") or {}
                usage = data.get("usage") or {}
                total = int((limits.get("credits") or limits.get("total_credits")
                             or data.get("credit_limit") or 0) or 0)
                used = int((usage.get("credits_used") or usage.get("total_credits_used")
                            or data.get("credits_used") or 0) or 0)
                if total > 0:
                    return max(0, total - used)
        except Exception:
            pass
        return -1

    def run(self) -> str:
        """Execute the full pipeline. Returns path to output CSV."""
        # 2026-05-21: API-key health banner. Logs the status of every key
        # at run start so the user can immediately see in the log feed WHY
        # discovery returned 0 leads (very common cause: a missing key
        # silently dropping a whole discovery source).
        try:
            _health_lines = ["[KEY-HEALTH] API key status at run start:"]
            for _svc, _k in API_KEYS.items():
                _set = bool((_k or "").strip())
                _preview = (_k[:6] + "…" + _k[-3:]) if (_set and len(_k or "") > 12) else ("(set)" if _set else "(MISSING)")
                _health_lines.append(f"   • {_svc:14s} → {('OK ' if _set else 'NO ')}{_preview}")
            self._log("\n".join(_health_lines))
            # Hard warning if no domain-discovery key is available
            _has_semrush = bool((API_KEYS.get("semrush") or "").strip())
            _has_google  = bool((API_KEYS.get("google_places") or "").strip())
            _has_apollo  = bool((API_KEYS.get("apollo") or "").strip())
            if not (_has_semrush or _has_google or _has_apollo):
                self._log("[KEY-HEALTH] ⛔ NO discovery key set (semrush/google/apollo all missing) — "
                          "run will produce 0 leads. Set at least one key in .env or Railway Variables.")
            elif not _has_apollo:
                self._log("[KEY-HEALTH] ⚠ APOLLO_API_KEY missing — enrichment will be unavailable.")
        except Exception as _hk_err:
            self._log(f"[KEY-HEALTH] (banner failed: {_hk_err})")

        # V5.32: Snapshot Apollo credits at run start for accurate delta reporting.
        # If fetch fails (some plan tiers don't expose this endpoint), just track calls via _api_counter.
        self._apollo_credits_at_start = self._fetch_apollo_credits_remaining()
        if self._apollo_credits_at_start > 0:
            self._log(f"Apollo credits at run start: {self._apollo_credits_at_start}")
        try:
            # PHASE 2 CITY MODE: skip discovery phases when presets provided
            if self._preset_keywords is not None or self._preset_domains is not None:
                self.keywords = list(self._preset_keywords or [])
                self.domains = list(self._preset_domains or [])
                # PHASE 2 (2026-04-28) — _adwords_domains is now the AUTHORITATIVE
                # paid-advertiser set used by the strict SEMrush gate to grant
                # bypass. Only the caller-confirmed subset (came from SEMrush
                # phrase_adwords / domain_adwords_adwords) qualifies; previously
                # we stuffed every preset_domain in here, including Apollo-
                # fallback local SMBs, which gave them an undeserved bypass.
                self._adwords_domains = set(
                    d for d in (self._confirmed_paid_domains or set())
                    if d  # guard against empty strings from bad input
                )
                self._organic_domains = set()
                self._log(
                    f"[CITY MODE] Skipping Phases 1-3 — using {len(self.keywords)} preset keywords, "
                    f"{len(self.domains)} preset domains "
                    f"({len(self._adwords_domains)} confirmed paid)"
                )
                self._progress(45, f"{len(self.domains)} city-scoped domains ready")
            else:
                self._phase1_seed_keywords()
                if self._cancelled:
                    return self._export_partial_now()
                self._phase2_semrush_expansion()
                if self._cancelled:
                    return self._export_partial_now()
                self._phase3_domain_discovery()
                if self._cancelled:
                    return self._export_partial_now()
            if not self.domains:
                self._log("No prospect domains found. Try different industry/settings.")
                return ""
            self._phase4_enrichment()
            self._log(f"[VALID-LEADS] After Phase 4 enrichment: {len(self.leads)} lead(s) "
                      f"(target {self.max_leads or 'unlimited'})")
            if self._cancelled:
                return self._export_partial_now()
            # PHASE 2: when enrichment is disabled, skip every step that
            # spends credits on email/phone enrichment, verification, or
            # role-recovery LLM passes. The cheap cleanup phases still run
            # so the basic-info CSV is deduped + sorted.
            if self.enrichment_enabled:
                self._phase4b_targeted_completion()  # V5.16: fills phone for top N outside credit gate
                if self._cancelled:
                    return self._export_partial_now()
            self._phase5_cleanup()
            if self._cancelled:
                return self._export_partial_now()
            if self.enrichment_enabled:
                self._phase5b_openai_verify()
                if self._cancelled:
                    return self._export_partial_now()
                self._phase5c_semrush_insights()  # V5.27: per-domain SEMrush + LLM enrichment
                if self._cancelled:
                    return self._export_partial_now()
            self._phase5d_role_recovery()  # V5.28: SerpAPI fallback for missing roles
            if self._cancelled:
                return self._export_partial_now()
            self._phase5e_drop_blank_identity()  # V5.28: omit leads with no name AND no role
            if self._cancelled:
                return self._export_partial_now()
            self._phase5f_dm_cap_and_topup()  # V5.29: DM filter + 2/company cap + quota top-up
            self._log(f"[VALID-LEADS] After Phase 5f DM-cap: {len(self.leads)} lead(s) "
                      f"(target {self.max_leads or 'unlimited'})")
            if self._cancelled:
                return self._export_partial_now()
            if self.enrichment_enabled:
                self._phase5g_completeness_gate()  # V5.30: retry chain + drop leads below score 2
                if self._cancelled:
                    return self._export_partial_now()
                self._phase5h_metadata_backfill_and_phone_retry()  # V5.31: backfill SEMrush metadata on top-up domains + extra phone retry
                if self._cancelled:
                    return self._export_partial_now()
                self._phase5i_phone_gate()  # V5.32: strict phone gate + refill
                if self._cancelled:
                    return self._export_partial_now()
            return self._phase6_export()
        except Exception as e:
            import traceback as _tb
            full_trace = _tb.format_exc()
            self._log(f"Pipeline error: {e}")
            self._log(f"Traceback:\n{full_trace}")
            print(f"[PIPELINE ERROR] {full_trace}", flush=True)
            # Phase 2 (2026-05-05): salvage partial leads on error.
            # Caller (wsgi /finalize_run) inspects path; partial CSVs are
            # never written to master_leads.
            try:
                return self._export_partial_now() or ""
            except Exception:
                return ""

    # ── Phase 1: Seed Keywords ──────────────────────────────────────────────

    def _set_semrush_phase(self, phase: str) -> None:
        """2026-05-18: tag every SEMrush call with the originating pipeline
        phase. Drives `_units_by_phase` (surfaced in /status summary) and
        gives the user a fine-grained "where did my credits go?" report
        per run."""
        try:
            self.semrush._current_phase = phase
        except Exception:
            pass

    # 2026-05-18 (round 3): cached SEMrush calls. The non-cached variants on
    # SemrushClient are still callable directly — these helpers exist for
    # the hot per-domain code paths (Phase 4 / 5c / 5h) where the same
    # report kept being fetched multiple times for the same domain.

    def _domain_cache_get(self, domain: str, key: str):
        d = (domain or "").strip().lower()
        if not d:
            return None
        with self._semrush_cache_lock:
            entry = self._semrush_domain_cache.get(d)
            return entry.get(key) if entry else None

    def _domain_cache_put(self, domain: str, key: str, value) -> None:
        d = (domain or "").strip().lower()
        if not d:
            return
        with self._semrush_cache_lock:
            entry = self._semrush_domain_cache.setdefault(d, {})
            entry[key] = value

    def _cached_overview(self, domain: str, db: str) -> dict:
        """get_domain_overview_metrics with per-run caching. Phase 4 fills
        this; Phase 5c/5h re-use it instead of re-spending ~40 units."""
        cached = self._domain_cache_get(domain, "overview")
        if cached is not None:
            return cached
        try:
            res = self.semrush.get_domain_overview_metrics(domain, db) or {}
        except Exception:
            res = {}
        self._domain_cache_put(domain, "overview", res)
        return res

    def _cached_organic_keywords(self, domain: str, db: str, limit: int) -> list[dict]:
        """Cached get_domain_organic_keywords. We key by the limit used so a
        bigger limit doesn't return a previously-cached smaller result —
        but we ALSO opportunistically reuse a larger cached result for any
        smaller request (trim instead of refetch)."""
        # Look for any cached result that is at least as big as requested.
        for cached_limit in (40, 30, 20, 15, 10):
            if cached_limit < limit:
                continue
            cached = self._domain_cache_get(domain, f"organic_kw_{cached_limit}")
            if cached is not None:
                return cached[:limit] if limit > 0 else cached
        try:
            res = self.semrush.get_domain_organic_keywords(domain, db, limit=limit) or []
        except Exception:
            res = []
        self._domain_cache_put(domain, f"organic_kw_{limit}", res)
        return res

    def _cached_organic_competitors(self, domain: str, db: str, limit: int) -> list[str]:
        """Cached get_organic_competitors. Same trim-instead-of-refetch
        behaviour as the organic-keywords cache above."""
        for cached_limit in (10, 5, 3):
            if cached_limit < limit:
                continue
            cached = self._domain_cache_get(domain, f"organic_comp_{cached_limit}")
            if cached is not None:
                return cached[:limit] if limit > 0 else cached
        try:
            res = self.semrush.get_organic_competitors(domain, db, limit=limit) or []
        except Exception:
            res = []
        self._domain_cache_put(domain, f"organic_comp_{limit}", res)
        return res

    def _phase1_seed_keywords(self):
        self._set_semrush_phase("phase1_seed")
        self._progress(1, "Phase 1: Generating seed keywords...")
        self._log(f"[Phase 1] START: Generating seed keywords for '{self.industry}'")

        seeds = INDUSTRY_KEYWORDS.get(self.industry, [])
        if not seeds:
            base = self.industry.lower()
            seeds = [
                f"{base} near me", f"best {base}", f"{base} services",
                f"{base} {self.config['location_suffix']}",
                f"professional {base}", f"local {base}",
                f"affordable {base}", f"top {base}",
            ]

        self.keywords = seeds[:]
        self._log(f"   Generated {len(self.keywords)} seed keywords")

        # Phase 2 (2026-05-05): Extend with tiered keyword bank.
        # Order: existing V5 seeds → TIER_1 → TIER_2 → TIER_3 → global 1k fallback.
        # Bank is deduped against seeds (case-insensitive). Lazy import keeps V5
        # importable when keyword_bank.py is missing.
        try:
            import keyword_bank as _kb
            extended = _kb.get_extended_keywords(self.industry, self.keywords)
            if len(extended) > len(self.keywords):
                added = len(extended) - len(self.keywords)
                self._log(f"   [Bank] +{added} tiered keywords (T1/T2/T3 + global) -> {len(extended)} total")
                self.keywords = extended
        except Exception as _e:
            self._log(f"   [Bank] keyword_bank unavailable: {_e} - using base seeds only")

        # API key status check — surface missing keys early so failures are diagnosable
        missing_keys = [name.upper() for name, val in API_KEYS.items() if not val]
        if missing_keys:
            self._log(f"   WARNING: Missing API keys: {', '.join(missing_keys)}. Discovery will be limited.")
        if not API_KEYS.get("semrush"):
            self._log("   WARNING: No SEMRUSH_API_KEY — domain discovery will rely on SerpApi + Apollo fallback.")
        if not API_KEYS.get("serpapi") and not API_KEYS.get("semrush"):
            self._log("   WARNING: Both SEMRUSH_API_KEY and SERPAPI_API_KEY are missing — only Apollo org search available.")

        self._progress(5, f"{len(self.keywords)} seed keywords ready")

    # ── Phase 2: SEMrush Keyword Expansion ──────────────────────────────────

    def _phase2_semrush_expansion(self):
        self._set_semrush_phase("phase2_kw_expansion")
        self._progress(6, "Expanding keywords via SEMrush...")
        self._log("Phase 2: SEMrush keyword expansion")
        # 2026-05-18: silent-scope guard — when the city_pipeline pre-flight
        # already determined SEMrush has no data for this scope, skip the
        # whole phrase_related sweep (~12 × ~25 units = 300 credits wasted).
        if getattr(self, "_semrush_silent_scope", False):
            self._log("   [Phase 2] silent scope — skipping SEMrush keyword expansion")
            return

        db = self.config["semrush_db"]
        expanded = set(self.keywords)
        seeds_to_expand = self.keywords[:12]

        for i, seed in enumerate(seeds_to_expand):
            if self._cancelled:
                return
            self._log(f"   Expanding: '{seed}'")
            results = self.semrush.get_related_keywords(seed, db, display_limit=25)

            added = 0
            for kw_data in results:
                kw = kw_data["keyword"]
                vol = kw_data["volume"]
                cpc = kw_data["cpc"]
                if vol >= self.min_volume and cpc >= self.min_cpc and kw not in expanded:
                    expanded.add(kw)
                    added += 1
                    if len(expanded) >= 80:
                        break

            self._log(f"   -> +{added} keywords (total: {len(expanded)})")
            pct = 6 + int((i + 1) / len(seeds_to_expand) * 14)
            self._progress(pct, f"Keyword expansion: {len(expanded)} keywords")
            if len(expanded) >= 80:
                break

        self.keywords = list(expanded)
        self._log(f"   Total unique keywords: {len(self.keywords)}")
        self._progress(20, f"{len(self.keywords)} keywords ready for search")

    # ── Phase 3: Domain Discovery ───────────────────────────────────────────

    def _phase3_domain_discovery(self):
        self._set_semrush_phase("phase3_discovery")
        self._progress(21, "Discovering business domains...")
        self._log("Phase 3: Domain discovery via SEMrush + SerpApi (V5.10: paid-traffic filter)")

        db = self.config["semrush_db"]
        gl = self.config["serpapi_gl"]

        # 2026-06-01: SerpAPI multi-key pre-flight (industry-mode parity with
        # city_pipeline). Logs per-key searches_left and marks dead keys
        # before the discovery sweep starts.
        try:
            if hasattr(self.serpapi, "precheck_keys") and getattr(self.serpapi, "_keys", []):
                _ind_status = self.serpapi.precheck_keys(log_fn=self._log)
                _ind_total = sum(int(s.get("remaining", 0)) for s in _ind_status)
                _ind_live = sum(1 for s in _ind_status if int(s.get("remaining", 0)) > 0)
                self._log(
                    f"   [SerpAPI/preflight] {_ind_live}/{len(_ind_status)} keys live, "
                    f"{_ind_total} searches total remaining across all keys"
                )
        except Exception as _spe_i:
            self._log(f"   [SerpAPI/preflight] check failed: {_spe_i}")

        # 2026-06-09: RUN-WIDE SerpAPI budget (shared across every SerpApiClient
        # instance via the common _counter). credit-saving (default) caps spend
        # at ~max_leads×14 calls; regular mode is generous but still bounded so
        # one run can't drain the whole key. Always bounded by live balance so a
        # reserve survives for future runs.
        try:
            # In city mode the wrapper already set a run-wide budget BEFORE
            # discovery — never overwrite it (each enrich round must keep the
            # same shared cap, not reset it). Only compute here when unset
            # (industry mode / standalone).
            _existing_budget = int(self._api_counter.get("serpapi_budget", 0) or 0)
            if _existing_budget > 0:
                self.serpapi._call_budget = _existing_budget
            else:
                _serp_live_i = int(getattr(self.serpapi, "total_remaining_searches", lambda: 0)())
                _ml_i = max(1, int(self.max_leads or 0))
                if _serp_live_i <= 0:
                    _serp_budget_i = 0
                else:
                    _res_i = min(40, _serp_live_i // 3)   # keep a reserve for next runs
                    if self.credit_saver:
                        _target_i = max(12, _ml_i * 14)     # ≈14 calls/lead, hard cap
                    else:
                        _target_i = max(60, _ml_i * 80)     # regular: thorough
                    _serp_budget_i = max(8, min(_serp_live_i - _res_i, _target_i))
                self.serpapi._call_budget = int(_serp_budget_i)
                self.serpapi._calls_used = 0
                # Shared budget — the canonical one every instance/method honours.
                self._api_counter["serpapi_budget"] = int(_serp_budget_i)
                self._api_counter["serpapi_budget_warned"] = False
                self._log(f"   [SerpAPI] run-wide budget = {_serp_budget_i} calls "
                          f"({'credit-saver' if self.credit_saver else 'regular'}, "
                          f"max_leads={_ml_i}, live balance {_serp_live_i})")
        except Exception as _sbe_i:
            self._log(f"   [SerpAPI] budget set failed ({_sbe_i})")

        # 2026-05-21: same 4-mode router used by city_pipeline. Industry-mode
        # parity for the SEMrush ↔ Google Places smart fallback. The banner
        # logs which mode is active; the gates below skip SEMrush passes when
        # SEMrush is unavailable and run Google Places when AU+key present.
        from discovery_mode import (
            DiscoveryMode,
            detect_initial_mode,
            should_pivot_to_google,
            log_mode_banner,
            log_pivot,
        )
        _mode, _mode_reason = detect_initial_mode(API_KEYS, self.country)
        self._discovery_mode = _mode
        log_mode_banner(
            self._log, _mode, _mode_reason,
            max_leads=self.max_leads, country=self.country,
        )

        # V5.17: Calculate optimal domain cap based on max_leads — wider net = more phone-bearing leads
        # V5.32: Floor lowered from 50 → 10. For max_leads=1, fetching 50 domains then enriching all
        # of them wasted ~15 min of Apollo calls on leads that would never reach the final CSV.
        # PHASE 2: SEMrush gate (paid_traffic>=5 AND organic_keywords>1000) drops many
        # domains, so widen the net to 6× max_leads (was 4×) — recovers the
        # 50→16 / 175→100 shortfalls seen in earlier runs.
        if self.max_leads > 0:
            optimal_domain_cap = max(15, int(self.max_leads * 6))
            self._log(f"   V5.17 Smart mode: max_leads={self.max_leads}, domain cap={optimal_domain_cap}")
        else:
            optimal_domain_cap = 200
            self._log("   V5.17: max_leads not set, using standard domain discovery (200 cap)")

        # V5.10: Separate paid-confirmed (from adwords endpoint) from organic/SERP candidates
        paid_domains: set = set()   # confirmed running paid ads
        organic_candidates: set = set()  # from organic search / SerpApi — need paid check

        keywords_to_search = self.keywords[:30]
        total_steps = len(keywords_to_search)

        # Phase 2 (2026-05-15+): early-bail parity with city_pipeline. If the
        # first 25 SEMrush probes return ≥90% empty, SEMrush has no paid-ads
        # data for this industry/region — abort the scan to preserve credits.
        # `total_steps` is at most 30 here (`keywords_to_search = self.keywords[:30]`),
        # so the bail saves ~5 probes per industry-mode silent run plus prevents
        # the downstream Phase 4 traffic_metrics burn on Apollo-only fallbacks.
        _empty_kw_count = 0
        _EARLY_BAIL_AFTER = 25
        _EARLY_BAIL_EMPTY_RATIO = 0.90
        # 2026-05-21: skip the SEMrush sweep entirely when mode == GOOGLE_ONLY
        # or APOLLO_ONLY. paid_domains stays empty; Google Places + Apollo
        # later in this phase will fill the pool instead.
        _v5_pass_active = _mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
        if not _v5_pass_active:
            self._log(
                f"   [Phase 3] SEMrush sweep SKIPPED — mode={_mode.value}"
            )
        for i, kw in enumerate(keywords_to_search if _v5_pass_active else []):
            if self._cancelled:
                return

            if not self.serpapi._available and i % 5 == 0:
                self.serpapi._available = True

            # V5.12: PAID-ONLY MODE — Only fetch domains with active Google Ads (paid_traffic != 0)
            # Removed: get_organic_domains() and organic verification entirely
            _pre = len(paid_domains)
            # 2026-06-12: capture ALL advertisers per keyword (phrase_adwords =
            # 1 unit/row; keywords are AdPotentialScore-sorted so early probes
            # are the densest advertiser keywords).
            ad_results = self.semrush.get_adwords_domains(
                kw, db, limit=(15 if self.credit_saver else 30))
            for r in ad_results:
                d = r["domain"]
                paid_domains.add(d)
            if len(paid_domains) == _pre:
                _empty_kw_count += 1

            if (i + 1) % 5 == 0 or i == total_steps - 1:
                self._log(f"   Searched {i + 1}/{total_steps} keywords -> "
                          f"{len(paid_domains)} PAID (Google Ads) domains found")

            pct = 21 + int((i + 1) / total_steps * 20)
            self._progress(pct, f"Found {len(paid_domains)} paid domains")

            if len(paid_domains) >= optimal_domain_cap:
                self._log(f"   Reached paid domain cap ({optimal_domain_cap}). Stopping search.")
                break

            # Early-bail on SEMrush silence (defense-in-depth for industry-mode).
            if ((i + 1) >= _EARLY_BAIL_AFTER
                    and (_empty_kw_count / (i + 1)) >= _EARLY_BAIL_EMPTY_RATIO):
                self._log(
                    f"   [Discovery] ⏸ Early-bail: {_empty_kw_count}/{i + 1} probes "
                    f"empty (≥{int(_EARLY_BAIL_EMPTY_RATIO * 100)}%). SEMrush has no "
                    f"paid-ads data for this industry/region — skipping remaining "
                    f"{total_steps - (i + 1)} probes to save credits."
                )
                # Mark scope silent so Phase 4 skips per-domain SEMrush calls.
                # NOTE: Industry-mode discovers fallback domains through SerpAPI/
                # Apollo paths below — we only mark silent if paid_domains stays 0
                # AND those fallbacks also yield nothing. Stash a hint here; the
                # Apollo-only flag is decided after the fallback fires.
                self._semrush_silent_scope = True
                break

        # 2026-05-21: mid-run pivot check — if SEMrush exhausted (budget cap
        # or silent scope) AND Google Places is available, flip to GOOGLE_ONLY
        # for the rest of this phase. Partial paid_domains are kept.
        if should_pivot_to_google(
            self._api_counter, getattr(self, "_semrush_silent_scope", False),
            API_KEYS, self.country, _mode,
        ):
            _mode = DiscoveryMode.GOOGLE_ONLY
            self._discovery_mode = _mode
            log_pivot(
                self._log,
                semrush_pool_size=len(paid_domains),
                silent_scope=bool(getattr(self, "_semrush_silent_scope", False)),
                budget_exhausted=bool(self._api_counter.get(
                    "semrush_budget_exhausted_logged", False)),
            )

        # Phase 2: Paid-keyword expansion — surface NEW keywords that the top
        # seed paid domains are bidding on, then use those keywords to find
        # additional advertisers. Budget-capped so it never dominates the run.
        # 2026-05-21: SEMrush-family — skip when not in BOTH/SEMRUSH_ONLY.
        paid_kw_domains_added = 0
        _semrush_family_active = _mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
        if _semrush_family_active and len(paid_domains) >= 3:
            top_seeds = list(paid_domains)[:5]
            PKW_CALL_BUDGET = 15   # total SEMrush calls allowed inside this block
            pkw_calls = 0
            new_keywords: set[str] = set()
            for seed in top_seeds:
                if pkw_calls >= PKW_CALL_BUDGET or self._cancelled:
                    break
                try:
                    seed_kws = self.semrush.get_domain_adwords_keywords(seed, db, limit=5)
                except Exception as _e:
                    seed_kws = []
                pkw_calls += 1
                for row in seed_kws or []:
                    kw = (row.get("keyword") or "").strip()
                    vol = int(row.get("volume") or 0)
                    cpc = float(row.get("cpc") or 0.0)
                    # Apply the same CPC floor (self.min_cpc) + volume floor (self.min_volume).
                    # Skip phrases already in our keyword list (case-insensitive).
                    if not kw or vol < self.min_volume or cpc <= self.min_cpc:
                        continue
                    if any(kw.lower() == existing.lower() for existing in self.keywords):
                        continue
                    new_keywords.add(kw)
            # Cap new keywords at 10 to keep per-run budget predictable
            new_keywords_list = list(new_keywords)[:10]
            self.keywords.extend(new_keywords_list)
            for nkw in new_keywords_list:
                if pkw_calls >= PKW_CALL_BUDGET or self._cancelled:
                    break
                try:
                    ad_results = self.semrush.get_adwords_domains(nkw, db, limit=5)
                except Exception:
                    ad_results = []
                pkw_calls += 1
                for r in ad_results or []:
                    d = r.get("domain") or ""
                    if d and d not in paid_domains and not is_platform_domain(d):
                        paid_domains.add(d)
                        paid_kw_domains_added += 1
            if new_keywords_list or paid_kw_domains_added:
                self._log(
                    f"   Phase2 paid-KW expansion: +{len(new_keywords_list)} new keywords, "
                    f"+{paid_kw_domains_added} new domains ({pkw_calls} SEMrush calls)"
                )
        self._paid_kw_expansion_added = paid_kw_domains_added

        # Phase 2: Recursive competitor BFS (replaces V5.13's 1-iteration loop).
        # Walks the competitor graph up to MAX_DEPTH levels with a visited set,
        # early-exits when the domain pool is sufficient, and enforces a hard
        # SEMrush call budget so it can't dominate API credits.
        # 2026-05-21: SEMrush-family — gated by mode.
        competitor_domains_added = 0
        if _semrush_family_active and len(paid_domains) >= 3 and not self._cancelled:
            try:
                from competitor_expansion import expand_competitors_bfs
                # 2026-05-18: cap seed sample by max_leads. Each unique seed
                # becomes the root of a BFS tree, so 20 seeds = 20 starting
                # points * ~3 branches * depth-3 = lots of calls. For a tiny
                # run we want a tiny seed set.
                _mx_seed = self.max_leads if (self.max_leads or 0) > 0 else 20
                _seed_cap = max(2, min(20, _mx_seed * 2))
                seed_sample = list(paid_domains)[:_seed_cap]
                new_competitor_domains, comp_calls, depth_reached = expand_competitors_bfs(
                    seed_sample,
                    fetch_competitors=(
                        lambda d, lim: self.semrush.get_domain_competitors(d, db, limit=lim)
                    ),
                    is_platform_domain=is_platform_domain,
                    required_leads=self.max_leads,
                    current_domain_count=len(paid_domains),
                    cancelled_check=lambda: self._cancelled,
                    silent_scope=getattr(self, "_semrush_silent_scope", False),
                )
                for cd in new_competitor_domains:
                    if cd not in paid_domains:
                        paid_domains.add(cd)
                        competitor_domains_added += 1
                self._competitor_calls_made = comp_calls
                self._competitor_depth_reached = depth_reached
                if competitor_domains_added:
                    self._log(
                        f"   BFS competitor expansion: +{competitor_domains_added} domains, "
                        f"depth={depth_reached}, calls={comp_calls}"
                    )
            except Exception as _e:
                self._log(f"   BFS competitor expansion failed: {_e}")
        self._competitor_domains_added = competitor_domains_added

        # 2026-05-21: Google Places industry-mode block. Active in BOTH and
        # GOOGLE_ONLY modes when country=AU and a key is configured. Mirrors
        # the city_pipeline.PASS 3.5 logic — domains tagged `google_intent`,
        # stored separately so they NEVER enter `_confirmed_paid_domains`.
        # Industry mode has no `_cities_to_search`; we derive a single
        # location anchor from `config["location_suffix"]` (e.g. "Australia"
        # or a city suffix) which still gives Google Places enough geo bias.
        _gp_industry_domains: set = set()
        try:
            if _mode in (DiscoveryMode.BOTH, DiscoveryMode.GOOGLE_ONLY):
                from google_places_intent import (
                    GooglePlacesIntentDiscovery,
                    MAX_PLACES_CALLS as _GP_MAX_CALLS_IND,
                    MAX_DOMAINS as _GP_MAX_DOMAINS_IND,
                )
                _gp_key_ind = (
                    API_KEYS.get("google_places", "")
                    or os.environ.get("GOOGLE_PLACES_API_KEY", "")
                ).strip()
                _gp_au_ok = (self.country or "").strip().upper() == "AU"
                if _gp_key_ind and _gp_au_ok and not self._cancelled:
                    _suffix = (self.config.get("location_suffix") or "Australia").strip()
                    _gp_cities_ind = [_suffix] if _suffix else ["Australia"]
                    _gp_kw_n = 15 if _mode == DiscoveryMode.GOOGLE_ONLY else 5
                    _gp_kws_ind = list(self.keywords or [])[:_gp_kw_n]
                    _gp_client_ind = GooglePlacesIntentDiscovery(
                        api_key=_gp_key_ind,
                        is_platform_domain=is_platform_domain,
                        log_fn=self._log,
                    )
                    # 2026-05-26: mirror city_pipeline's paid-traffic gate at
                    # fetch time. SEMrush domain_ranks primary + SerpAPI ads
                    # fallback. Volume floor = max_leads × 4 keeps thin-SEMrush
                    # scopes usable instead of nuked.
                    from google_places_intent import (
                        PAID_MIN_THRESHOLD as _GP_PAID_MIN_IND,
                        PAID_VERIFY_MAX as _GP_VERIFY_MAX_IND,
                    )
                    _semrush_active_ind = (
                        _mode in (DiscoveryMode.BOTH, DiscoveryMode.SEMRUSH_ONLY)
                        and self.semrush is not None
                    )
                    _serp_ref = self.serpapi
                    _sem_ref = self.semrush
                    _db_ind = db
                    _gl_ind = gl
                    # 2026-05-26: paid_domains (SEMrush-confirmed ad runners
                    # from earlier in this same method) is a free pre-verifier
                    # pool — any Places domain already in it skips both the
                    # SEMrush AND SerpAPI API calls at verification time.
                    _free_paid_pool_ind = {(d or "").lower() for d in paid_domains}
                    # 2026-05-28: ATC verifier — primary free paid signal,
                    # mirrors city_pipeline. Catches AU advertisers SEMrush/
                    # Ahrefs miss. Matches by name via _gp_client_ind.domain_to_name.
                    _atc_ind = None
                    _atc_confirmed_ind: set = set()
                    _atc_ids_ind: dict = {}
                    if str(os.environ.get("ATC_ENABLED", "1")).strip() != "0":
                        try:
                            from google_ads_transparency import AdsTransparencyVerifier
                            _atc_ind = AdsTransparencyVerifier(country=self.country, log_fn=self._log)
                        except Exception as _atc_ie:
                            self._log(f"   [Discovery/ATC] init failed: {_atc_ie}")
                            _atc_ind = None
                    def _ind_paid_verifier(domain: str) -> int:
                        if not domain:
                            return 0
                        _dl = domain.lower()
                        # 0. ATC by business name (free, Google ground-truth).
                        if _atc_ind is not None:
                            try:
                                _nm = (_gp_client_ind.domain_to_name.get(_dl)
                                       or _dl.split(".")[0].replace("-", " "))
                                _hit_i = _atc_ind.is_advertiser(_nm) if _nm else None
                                if _hit_i:
                                    _atc_confirmed_ind.add(_dl)
                                    _atc_ids_ind[_dl] = _hit_i.get("advertiser_id", "")
                                    return 999
                            except Exception:
                                pass
                        if _dl in _free_paid_pool_ind:
                            return 999   # already SEMrush-confirmed → free pass
                        if _semrush_active_ind:
                            try:
                                ov = _sem_ref.get_domain_overview_metrics(domain, _db_ind)
                                _pt = int(ov.get("paid_traffic", 0) or 0)
                                if _pt > 0:
                                    return _pt
                            except Exception:
                                pass
                        try:
                            if _serp_ref and _serp_ref._available:
                                _brand = domain.split(".")[0]
                                _ads = _serp_ref.search_keyword_ads_only(
                                    _brand, _gl_ind, num=20
                                ) or []
                                if _dl in {a.lower() for a in _ads if a}:
                                    return 999
                        except Exception:
                            pass
                        return 0
                    _verify_cap_ind = (
                        int(os.environ.get("ATC_VERIFY_MAX", "120") or "120")
                        if _atc_ind is not None else _GP_VERIFY_MAX_IND
                    )
                    _strict_paid_ind = str(os.environ.get("STRICT_PAID_ONLY", "0")).strip() == "1"
                    _volume_floor_ind = 0 if _strict_paid_ind else max(0, int(self.max_leads or 0) * 4)
                    _gp_industry_domains = _gp_client_ind.discover(
                        keywords=_gp_kws_ind,
                        cities=_gp_cities_ind,
                        country="AU",
                        paid_traffic_verifier=_ind_paid_verifier,
                        min_paid_traffic=_GP_PAID_MIN_IND,
                        max_verify_calls=_verify_cap_ind,
                        volume_floor=_volume_floor_ind,
                    )
                    # ATC-confirmed → confirmed-paid + "Google Ads" label.
                    _atc_in_pool_ind = {d for d in _atc_confirmed_ind if d in _gp_industry_domains}
                    if _atc_in_pool_ind:
                        self._confirmed_paid_domains.update(_atc_in_pool_ind)
                        if not isinstance(getattr(self, "_serp_ads_domains", None), set):
                            self._serp_ads_domains = set()
                        self._serp_ads_domains.update(_atc_in_pool_ind)
                        for _d in _atc_in_pool_ind:
                            if _atc_ids_ind.get(_d):
                                self._atc_advertiser_ids[_d] = _atc_ids_ind[_d]
                        self._log(
                            f"   [Discovery/ATC] {len(_atc_in_pool_ind)} CONFIRMED Google "
                            f"advertisers via Ads Transparency Center "
                            f"({_atc_ind.calls_made if _atc_ind else 0} lookups)"
                        )
                    # Keep these distinct from paid (no `confirmed_paid` bypass).
                    _gp_industry_domains = {
                        d for d in _gp_industry_domains
                        if d and d not in paid_domains
                    }
                    self._log(
                        f"   [Discovery/GooglePlaces] +{len(_gp_industry_domains)} AU "
                        f"domains ({_gp_client_ind.calls_made} API calls, cap="
                        f"{_GP_MAX_CALLS_IND} calls / {_GP_MAX_DOMAINS_IND} domains)"
                    )
        except Exception as _gp_exc_ind:
            self._log(f"   [Discovery/GooglePlaces] industry-mode failed: {_gp_exc_ind}")
        # Stash separately for downstream Phase 4 + CSV labeling.
        self._google_intent_domains.update(
            (d or "").lower() for d in _gp_industry_domains if d
        )

        # 2026-05-25: Vertex AI (Gemini) ranking for industry mode. Mirrors
        # the city_pipeline path — score all combined paid+google domains
        # by buying intent, keep the highest-ranked subset for enrichment.
        # No-op when GEMINI_API_KEY is missing.
        try:
            from vertex_ai_ranker import VertexAIRanker
            _v_key_i = (
                API_KEYS.get("gemini", "")
                or os.environ.get("GEMINI_API_KEY", "")
            ).strip()
            _all_ind = set(paid_domains) | set(_gp_industry_domains)
            if _v_key_i and _all_ind and not self._cancelled:
                _ranker_i = VertexAIRanker(_v_key_i, log_fn=self._log)
                _keep_i = min(60, max(15, int((self.max_leads or 5) * 8)))
                _ranked_i = _ranker_i.rank_domains(
                    _all_ind, industry=self.industry,
                    country=self.country, top_n=_keep_i,
                )
                _kept_i = {d for d, _ in _ranked_i}
                _before_i = len(paid_domains) + len(_gp_industry_domains)
                # Apply the ranker's filter — drop low-intent rows from both pools
                paid_domains = {d for d in paid_domains if d in _kept_i}
                _gp_industry_domains = {d for d in _gp_industry_domains if d in _kept_i}
                self._log(
                    f"   [Discovery/VertexAI] industry rank: {_before_i} → "
                    f"{len(_kept_i)} (Gemini calls: {_ranker_i.calls_made})"
                )
                self._api_counter["gemini"] = int(
                    self._api_counter.get("gemini", 0)
                ) + _ranker_i.calls_made
        except Exception as _ve_i:
            self._log(f"   [Discovery/VertexAI] industry-mode failed: {_ve_i}")

        # V5.12: All domains are PAID — no organic candidates
        self._adwords_domains = set(paid_domains)
        self._organic_domains = set()  # V5.12: Empty (paid-only mode)
        # PHASE 2 (2026-04-28) — industries-mode paid_domains came from
        # get_adwords_domains() so they're confirmed paid advertisers; mirror
        # them into _confirmed_paid_domains so the strict gate grants bypass
        # uniformly across industries-mode and city-mode runs.
        self._confirmed_paid_domains.update((d or "").lower() for d in paid_domains if d)

        pct_after = 41
        self._progress(pct_after, f"Paid traffic check done: {len(paid_domains)} qualified domains")

        # V5.15: Always collect organic domains alongside paid — no longer a fallback-only
        # Organic leads appear with Traffic Source = "Organic" in CSV
        # 2026-05-21: gated by mode — SerpAPI is part of the SEMrush family
        # in industry mode too (used to verify paid signals).
        if len(paid_domains) == 0:
            self._log("   SEMrush returned 0 paid domains. Checking organic sources...")
        organic_supp: set = set()
        if _semrush_family_active and self.serpapi._available:
            for kw in self.keywords[:15]:
                if len(organic_supp) >= 20:
                    break
                serp_domains = self.serpapi.search_keyword(
                    f"{kw} {self.config['location_suffix']}", gl, num=10
                )
                for d in serp_domains:
                    if d not in paid_domains and not is_platform_domain(d):
                        organic_supp.add(d)
            if organic_supp:
                self._organic_domains = organic_supp
                self._log(f"   V5.15 Organic: {len(organic_supp)} organic domains added (Traffic Source = Organic)")

        # ── SerpAPI ads-only sweep — PRIMARY paid source (2026-05-28) ──
        # Pulls ONLY domains from Google's live `ads[]` block. Industry-mode
        # uses location_suffix (e.g. "Australia") instead of a city list.
        # SerpAPI is unlimited for this account, so we scale the sweep with
        # max_leads to harvest a large confirmed-advertiser pool that fills
        # the CSV first (paid-first policy). Bounded by SERP_ADS_MAX_QUERIES.
        ads_supp: set = set()
        if self.serpapi._available and not self._cancelled:
            _ads_q_cap_i = int(os.environ.get("SERP_ADS_MAX_QUERIES", "80") or "80")
            # 2026-06-01: cap by live remaining across all SerpAPI keys.
            _live_budget_i = max(0, int(getattr(self.serpapi, "total_remaining_searches", lambda: 0)()) - 5)
            if _live_budget_i > 0:
                _ads_q_cap_i = min(_ads_q_cap_i, _live_budget_i)
            _ads_kw_n_i = max(8, min(40, int(self.max_leads or 0) or 8))
            _ads_calls_i = 0
            # 2026-06-02: Google Ads are CITY-targeted — industry mode has no
            # city list, so iterate the major cities for this country as the
            # SerpApi `location` (country-level returns 0 ads). Cross keywords
            # × top cities, bounded by the query cap.
            _ads_country_name_i = self.config.get("location_suffix") or "Australia"
            _major_cities_i = self._MAJOR_CITIES_BY_COUNTRY.get(
                (self.country or "AU").upper(), ["Sydney", "Melbourne", "Brisbane"]
            )
            from collections import Counter as _CounterI
            _ads_freq_i = _CounterI()
            for _kw in self.keywords[:_ads_kw_n_i]:
                if self._cancelled or not self.serpapi._available or _ads_calls_i >= _ads_q_cap_i:
                    break
                for _city_i in _major_cities_i:
                    if self._cancelled or not self.serpapi._available or _ads_calls_i >= _ads_q_cap_i:
                        break
                    try:
                        _ad_d = self.serpapi.search_keyword_ads_only(
                            _kw.strip(), gl, num=20,
                            location=f"{_city_i}, {_ads_country_name_i}",
                        )
                        _ads_calls_i += 1
                        for d in _ad_d or []:
                            d = (d or "").strip().lower()
                            if (d and d not in paid_domains and d not in organic_supp
                                    and not is_platform_domain(d)):
                                ads_supp.add(d)
                                _ads_freq_i[d] += 1
                    except Exception as _ae:
                        self._log(f"   SerpAPI-ads error on '{_kw}'/{_city_i}: {_ae}")
            # Heavy advertisers (>= SERP_ADS_MIN_FREQ distinct ad-queries).
            _min_freq_i = max(1, int(os.environ.get("SERP_ADS_MIN_FREQ", "2") or "2"))
            _heavy_i = {d for d, n in _ads_freq_i.items() if n >= _min_freq_i}
            if str(os.environ.get("STRICT_PAID_ONLY", "0")).strip() == "1" and _heavy_i:
                ads_supp = {d for d in ads_supp if d in _heavy_i}
            if ads_supp:
                self._log(
                    f"   [Discovery/SerpAPI-ads] PRIMARY paid sweep: +{len(ads_supp)} "
                    f"live-ad domains across {_ads_calls_i} queries"
                )
        # 2026-05-28: live Google Ads domains are confirmed advertisers — fold
        # into _confirmed_paid_domains (gate bypass + paid-first ordering) and
        # _serp_ads_domains (the "Traffic Source = Google Ads" CSV label).
        if ads_supp:
            self._confirmed_paid_domains.update(ads_supp)
            if not isinstance(getattr(self, "_serp_ads_domains", None), set):
                self._serp_ads_domains = set()
            self._serp_ads_domains.update(ads_supp)

        # ── Google Custom Search JSON API (free organic layer) ────────────
        # 2026-05-25: free 100 queries/day. Same trim+skip pattern as
        # city_pipeline. APOLLO_ONLY mode skips this; other modes use it.
        cse_supp: set = set()
        try:
            if _mode != DiscoveryMode.APOLLO_ONLY and not self._cancelled:
                from google_custom_search import (
                    GoogleCustomSearchDiscovery,
                    MAX_QUERIES as _CSE_MAX_Q_IND,
                    MAX_DOMAINS as _CSE_MAX_D_IND,
                )
                _cse_key_i = (
                    API_KEYS.get("google_custom_search", "")
                    or os.environ.get("GOOGLE_CUSTOM_SEARCH_API_KEY", "")
                ).strip()
                _cse_cx_i = (
                    API_KEYS.get("google_custom_search_cx", "")
                    or os.environ.get("GOOGLE_CUSTOM_SEARCH_CX", "")
                ).strip()
                if _cse_key_i and _cse_cx_i:
                    _suffix_i = (self.config.get("location_suffix") or "").strip()
                    _cse_cities_i = [_suffix_i] if _suffix_i else []
                    _cse_client_i = GoogleCustomSearchDiscovery(
                        api_key=_cse_key_i,
                        cx=_cse_cx_i,
                        is_platform_domain=is_platform_domain,
                        log_fn=self._log,
                    )
                    cse_supp = _cse_client_i.discover(
                        keywords=list(self.keywords or [])[:5],
                        cities=_cse_cities_i,
                        country=self.country,
                    )
                    cse_supp = {
                        d for d in cse_supp
                        if d and d not in paid_domains and d not in organic_supp
                        and d not in ads_supp
                    }
                    self._api_counter["google_custom_search"] = int(
                        self._api_counter.get("google_custom_search", 0)
                    ) + _cse_client_i.calls_made
                    self._log(
                        f"   [Discovery/GoogleCSE] +{len(cse_supp)} organic domains "
                        f"({_cse_client_i.calls_made} API calls, cap={_CSE_MAX_Q_IND} "
                        f"queries / {_CSE_MAX_D_IND} domains)"
                    )
        except Exception as _cse_exc_i:
            self._log(f"   [Discovery/GoogleCSE] industry-mode failed: {_cse_exc_i}")

        # 2026-05-21: merge Google Places industry-mode domains into the
        # enrichment pool so Phase 4 picks them up via Apollo / Lusha.
        # Order: confirmed paid first, then organic, then ads-only + CSE,
        # then google_intent. The Phase 4 paid-traffic gate runs as normal —
        # google_intent / ads-only / CSE domains get the silent-scope
        # relaxation when SEMrush yielded 0 (existing logic in
        # _enrich_single_domain).
        self.domains = (
            list(paid_domains)[:optimal_domain_cap]
            + list(organic_supp)[:20]
            + list(ads_supp)
            + list(cse_supp)
            + list(_gp_industry_domains)
        )

        self._organic_fallback_domains = set()  # kept for compat

        # Phase 2: Prime the shared LeadPool with the primary-discovered domains
        # so any secondary worker (or BFS restart) respects this run's claims.
        # Prime against master_leads ONCE here (then reused for the rest of the run).
        if self._lead_pool is not None:
            try:
                self._lead_pool.prime_master(industry=self.industry, country=self.country)
                self._lead_pool.add_domains(self.domains)
            except Exception as _e:
                self._log(f"   LeadPool prime skipped: {_e}")

        # Phase 2: Secondary agent — activate if primary pool looks short of target.
        # Uses secondary_keywords.py (must be generated once via
        # generate_secondary_keywords.py). If the file is empty the agent
        # no-ops, so this is always safe to call.
        if self._lead_pool is not None and not self._cancelled:
            try:
                from secondary_agent import SecondaryAgent, should_activate
                if should_activate(len(self.domains), self.max_leads):
                    agent = SecondaryAgent(
                        industry=self.industry,
                        database=db,
                        semrush=self.semrush,
                        lead_pool=self._lead_pool,
                        is_platform_domain=is_platform_domain,
                        log_fn=self._log,
                        cancelled_check=lambda: self._cancelled,
                    )
                    extra_domains = agent.run() or []
                    if extra_domains:
                        self.domains.extend(extra_domains)
                        self._secondary_agent_used = True
                        self._secondary_domains_added = len(extra_domains)
            except Exception as _e:
                self._log(f"   Secondary agent skipped: {_e}")

        self._log(f"   Total prospect domains: {len(self.domains)} ({len(paid_domains)} paid)")
        self._progress(45, f"{len(self.domains)} domains ready for enrichment")

    # ── V5.2: Email-based name inference utility ────────────────────────────

    @staticmethod
    def _infer_name_from_email(lead: dict) -> str:
        """V5.3: Try to infer the last name from a personal email address.
        Handles abbreviated names (matt→matthew, chris→christopher) via _NAME_ABBREVIATIONS.
        e.g. matt@matthewcornell.com.au → domain=matthewcornell → "Matthew Cornell"
             sarah.jones@company.com   → local=sarah.jones → "Sarah Jones"
             chris@christopherbrown.com → "Christopher Brown"
        Returns the full name string if inferred, else empty string.
        """
        email = lead.get("email", "")
        name = lead.get("name", "")
        if not email or not name or " " in name:
            return ""
        first = name.lower()
        local = email.lower().split("@")[0]
        email_domain = email.lower().split("@")[1] if "@" in email else ""
        variants = _get_name_variants(first)

        # Pattern 1: local part is "first.last" or "variant.last"
        if "." in local:
            parts = local.split(".")
            if parts[0] in variants and len(parts) > 1 and len(parts[1]) > 1:
                best_first = parts[0].title()  # Use the form from the email
                return f"{best_first} {parts[1].title()}"

        # Pattern 2: local part is "firstlast" or "variantlast" (concatenated)
        for variant in variants:    
            if local.startswith(variant) and len(local) > len(variant) + 1:
                suffix = local[len(variant):]
                if suffix.isalpha() and len(suffix) >= 2:
                    return f"{variant.title()} {suffix.title()}"

        # Pattern 3: email domain contains name (e.g. matthewcornell.com.au)
        clean_domain = email_domain
        for tld in [".com.au", ".co.uk", ".org.au", ".net.au", ".co.nz",
                    ".com", ".org", ".net", ".io", ".co", ".au", ".uk"]:
            if clean_domain.endswith(tld):
                clean_domain = clean_domain[:-len(tld)]
                break
        for variant in variants:
            if clean_domain.startswith(variant) and len(clean_domain) > len(variant) + 1:
                suffix = clean_domain[len(variant):]
                if suffix.isalpha() and 2 <= len(suffix) <= 15:
                    return f"{variant.title()} {suffix.title()}"

        return ""

    # ── Phase 4: Lead Enrichment ────────────────────────────────────────────

    # ── V5.2: Skip-if-complete helper (requires FULL two-word name) ──
    @staticmethod
    def _lead_is_complete(lead):
        """V5.11: A lead is complete when it has FULL name + TRUE personal email (gmail/yahoo) + phone.
        Work emails (firstname@company.com) do NOT stop enrichment — we keep trying for a real
        personal email. Generic emails (info@, contact@) also don't stop enrichment.
        """
        name = lead.get("name", "")
        has_full_name = bool(name) and " " in name
        email = lead.get("email", "")
        has_real_personal_email = bool(email) and is_personal_email(email)  # gmail/yahoo only
        has_phone = bool(lead.get("phone"))
        return has_full_name and has_real_personal_email and has_phone

    def _scrape_contact_pages(self, domain: str) -> str:
        """V5.16: Scrape additional contact/about pages for phone numbers beyond what scrape_domain() tries."""
        extra_paths = ["/contact-us", "/about-us", "/find-us", "/get-in-touch", "/locations", "/our-team"]
        for path in extra_paths:
            url = f"https://{domain}{path}"
            try:
                page_data = self.scraper._scrape_page(url)
                if page_data and page_data.get("phones"):
                    phone = format_phone(page_data["phones"][0], self.country)
                    if phone:
                        return phone
            except Exception:
                continue
        return ""

    def _scrape_contact_pages_full(self, domain: str) -> dict:
        """V5.30: Scrape contact/about/team pages, returning BOTH phones and emails.
        Used by Phase 5g completeness retry when a lead is missing either field."""
        extra_paths = ["/contact", "/contact-us", "/about", "/about-us",
                       "/team", "/our-team", "/find-us", "/get-in-touch",
                       "/locations", "/meet-the-team", "/people", "/staff"]
        result: dict = {"phone": "", "emails": []}
        for path in extra_paths:
            url = f"https://{domain}{path}"
            try:
                page_data = self.scraper._scrape_page(url)
                if not page_data:
                    continue
                if not result["phone"] and page_data.get("phones"):
                    phone = format_phone(page_data["phones"][0], self.country)
                    if phone:
                        result["phone"] = phone
                for em in (page_data.get("emails") or []):
                    em_clean = em.strip().lower()
                    if em_clean and em_clean not in result["emails"]:
                        if domain.lower() in em_clean or is_personal_email(em_clean):
                            result["emails"].append(em_clean)
                if result["phone"] and result["emails"]:
                    break
            except Exception:
                continue
        return result

    def _enrich_single_domain(self, domain, index, total):
        """V5.1: Enrich a single domain. Returns list of leads for this domain.
        V5.8: Smart filtering to reduce API calls for low-relevance leads.
        V5.9: Credit gate — skips domain entirely when enough complete leads collected.
        Thread-safe: does not mutate self.leads, returns results instead."""
        if self._cancelled:
            return []

        # V5.9: Credit gate — if we already have enough complete leads, skip this domain
        if self._has_enough_leads():
            self._log(f"   [{index + 1}/{total}] {domain}: skipped (credit quota reached)")
            return []

        domain_leads = []
        company_name = ""
        company_phone = ""
        company_revenue = ""  # V5.27: Apollo annual revenue
        company_linkedin = ""

        # V5.13: Snapshot API counter for per-domain credit tracking
        _counter_snapshot = {k: v for k, v in self._api_counter.items()}

        # Step 1: Apollo organization enrichment — get company name + phone.
        # CREDIT-LEAK FIX: when enrichment is OFF, skip this paid call entirely.
        # The same fields are available from the nested organization object inside
        # the people-search response below (free with that call), and the
        # 500-employee SMB filter is applied post-people-search instead.
        employees_pre = 0
        if self.enrichment_enabled:
            org_data = self.apollo.enrich_organization(domain)
            if org_data:
                company_name = org_data.get("company_name", "")
                company_phone = org_data.get("phone", "")
                company_revenue = org_data.get("revenue", "") or ""  # V5.27: annual revenue
                company_linkedin = org_data.get("linkedin", "") or ""
                # V5.10: Skip large enterprises (>500 employees) — not SMB targets
                employees_pre = org_data.get("employees") or 0
                try:
                    employees_pre = int(employees_pre)
                except (ValueError, TypeError):
                    employees_pre = 0
                if employees_pre > 500:
                    self._log(f"   [{index + 1}/{total}] {domain}: skipped (large company, {employees_pre} employees)")
                    return []
                if company_name:
                    self._log(f"   [{index + 1}/{total}] {company_name} ({domain})")
                else:
                    self._log(f"   [{index + 1}/{total}] {domain}")
            else:
                self._log(f"   [{index + 1}/{total}] {domain}")
        else:
            # Enrichment OFF: log only — org data will be backfilled from
            # the people-search response (nested `organization` object).
            self._log(f"   [{index + 1}/{total}] {domain}")

        # V5.13: SEMrush traffic metrics for this domain
        db = self.config["semrush_db"]

        # Phase 2 (2026-05-15+): TWO SEMrush calls happen below — traffic_metrics
        # (top-5 keyword stats) AND domain_overview (domain-wide totals). When
        # city_pipeline flagged this scope SEMrush-silent AND this domain came
        # from Apollo's fallback, both calls are guaranteed to return zeros —
        # the entire scope has no SEMrush coverage. Compute `_is_apollo_only`
        # once and skip BOTH calls in that case.
        # Previous oversight: only `domain_overview` (line ~5218) was gated;
        # `get_domain_traffic_metrics` (line ~5197) still fired, wasting one
        # SEMrush call per domain (141 on the doomed Melbourne plumber run).
        _is_apollo_only = (domain or "").lower() in self._apollo_only_domains
        _skip_semrush_overview = self._semrush_silent_scope and _is_apollo_only
        # 2026-05-18 (round 3): for domains the outer city_pipeline already
        # confirmed are paid advertisers (came in via phrase_adwords), skip
        # the overview call entirely. We KNOW paid_traffic > 0 from how the
        # domain was discovered. Saves ~40 units per confirmed-paid domain.
        _is_confirmed_paid = (domain or "").lower() in self._confirmed_paid_domains

        # 2026-05-18 (round 3): the previous version of this code called
        # get_domain_traffic_metrics (~250 units) AND get_domain_overview_metrics
        # (~40 units) per domain, then OVERWROTE every field from the first
        # with the second. The first call was pure waste — its only unique
        # output was a per-keyword sum that was thrown away. Now we call
        # only the overview, cache it for Phases 5c/5h to reuse, and skip
        # entirely for silent-scope-Apollo-only or already-confirmed-paid.
        traffic_metrics: dict = {}
        if _skip_semrush_overview or _is_confirmed_paid:
            # Synthetic overview so downstream code sees populated fields.
            if _is_confirmed_paid:
                traffic_metrics = {"paid_traffic": 1, "_confirmed_paid": True}
        else:
            overview = self._cached_overview(domain, db)
            if overview:
                for _k in ("paid_traffic", "organic_traffic", "paid_keywords", "organic_keywords"):
                    if overview.get(_k):
                        traffic_metrics[_k] = overview[_k]

        try:
            _pt = int(float(traffic_metrics.get("paid_traffic", 0) or 0))
        except (TypeError, ValueError):
            _pt = 0
        try:
            _ok = int(float(traffic_metrics.get("organic_keywords", 0) or 0))
        except (TypeError, ValueError):
            _ok = 0

        # PHASE 2 (2026-04-28, final) — paid-only gate.
        # User's real requirement is "no leads with paid_traffic=0". Earlier
        # AND-ed organic_keywords thresholds (>1000, then >100) over-filtered
        # legitimate paid-advertising SMBs whose organic SEO is naturally
        # weak — Greg Bowden's emyjor.com.au (PT=40, OK=50) was the canonical
        # casualty. Dropping the organic check honors the explicit constraint
        # while restoring yield. Provenance bypass also stays removed so
        # competitor-expansion domains face the same paid-traffic check.
        #
        # Phase 2 (2026-05-15) GATE RELAXATION when SEMrush has no data:
        # If the scope is SEMrush-silent AND this domain came from Apollo
        # (not from SEMrush phrase_adwords), `paid_traffic=0` is uninformative —
        # SEMrush just doesn't index this local-AU business. Without this
        # relaxation, ALL 141 Apollo-discovered domains in the previous
        # doomed run got killed at this gate, producing 0 leads despite the
        # user picking a valid Melbourne scope. The legitimate-paid-advertiser
        # filter is preserved for non-silent scopes.
        # 2026-05-18 (round 4): Google Places-discovered AU businesses get
        # the same silent-scope gate relaxation as Apollo-only domains —
        # SEMrush returns paid_traffic=0 for most local AU SMBs whether
        # or not they actually advertise, so the gate is uninformative in
        # silent scopes. We DO NOT relax outside silent scope: in healthy
        # scopes a Google-Places domain that fails the paid_traffic gate
        # is correctly rejected (means SEMrush has data for the scope but
        # not for THIS specific business — that's a quality signal).
        _is_google_intent = (domain or "").lower() in self._google_intent_domains
        # 2026-05-21 + 2026-05-25: when SEMrush is unavailable for ANY
        # reason — missing key, silent scope, GOOGLE_ONLY mode, or HTTP
        # 401/403 from the API itself — `paid_traffic` is uninformative.
        # The shared counter's `semrush_unavailable` flag is set by
        # SemrushClient._request on the first 401/403 response, which
        # cascades through every domain enriched after that point.
        _semrush_unavailable = (
            not bool(API_KEYS.get("semrush"))
            or self._semrush_silent_scope
            or getattr(self, "_discovery_mode", None) == "GOOGLE_ONLY"
            or bool(self._api_counter.get("semrush_unavailable", False))
        )
        if _pt < 1:
            if self._semrush_silent_scope and _is_apollo_only:
                # Silent SEMrush + Apollo-only domain: accept, mark
                # provenance so Phase 6 sorts these below confirmed-paid.
                traffic_metrics["_silent_scope_apollo"] = True
            elif _semrush_unavailable and _is_google_intent:
                # AU business confirmed by Google Places; SEMrush is broken.
                # Accept, export label = "Google Intent".
                traffic_metrics["_google_intent"] = True
            elif _semrush_unavailable:
                # 2026-05-25 (the big fix): SEMrush is broken AND domain
                # came from a non-SEMrush source (SerpAPI / Apollo / any
                # discovery path). With SEMrush unreachable we can't
                # corroborate paid_traffic anyway, so refusing the domain
                # just because Google's API is down would be perverse —
                # the user already paid for SerpAPI/Apollo/Places to find
                # this business. Accept it. Stamp `_silent_scope_apollo`
                # so it sorts to the bottom of the export (below true
                # paid-confirmed leads).
                traffic_metrics["_silent_scope_apollo"] = True
                self._log(
                    f"   [{index + 1}/{total}] {domain}: accepted under SEMrush-broken "
                    f"fallback (paid_traffic gate bypassed; "
                    f"non-SEMrush discovery)."
                )
            else:
                self._log(
                    f"   [{index + 1}/{total}] {domain}: skipped (paid_traffic={_pt} < 1)"
                )
                return []
        # Even in non-silent scopes, if the domain came from Google Places
        # we still stamp the source label so export reflects provenance.
        # (The lead has passed the gate, so its underlying traffic IS paid
        # in SEMrush data, but provenance was Places — we tell the user
        # both facts so they understand WHY this lead surfaced.)
        if _is_google_intent and "_google_intent" not in traffic_metrics:
            traffic_metrics["_google_intent"] = True

        # Step 2: Apollo people search — get names and roles (V5.18: per_page=25 for wider coverage)
        people = self.apollo.search_people_by_domain(domain, per_page=25)

        # 2026-05-21: STUB lead for Google-Places-discovered domains where
        # Apollo has zero people (very common for small AU SMBs that aren't
        # indexed in Apollo's people DB). Without this fallback, every such
        # domain produces 0 leads and the user sees an empty results table
        # despite Google Places having found legitimate AU businesses.
        # The stub carries domain + best-effort company name (from the
        # domain root) so the lead survives Phase 5e/5g and shows up in
        # the table with `Source = "Google Intent"`. Phone/email stay blank
        # — the user follows up manually via the website.
        # 2026-05-21 (v2) + 2026-05-25 (v3): Extended stub eligibility.
        # Fires when ANY of:
        #   • Google Places provenance (_is_google_intent)
        #   • Apollo-fallback (_is_apollo_only)
        #   • Enrichment toggle OFF (user opted out of reveal anyway)
        #   • SEMrush is broken (_semrush_unavailable) — domain was
        #     accepted via the new SEMrush-broken bypass; stubbing
        #     guarantees the user gets the business-level lead even
        #     when Apollo has no people indexed.
        #   • traffic_metrics carries `_silent_scope_apollo` / `_google_intent`
        #     — these flags mean the domain was accepted via relaxation,
        #     which implies stub-eligible by design.
        _stub_eligible = (
            _is_google_intent
            or _is_apollo_only
            or (not self.enrichment_enabled)
            or _semrush_unavailable
            or bool(traffic_metrics.get("_silent_scope_apollo"))
            or bool(traffic_metrics.get("_google_intent"))
        )
        if (not people) and _stub_eligible:
            try:
                _root = (domain or "").split(".")[0].replace("-", " ").title() or "(business)"
            except Exception:
                _root = "(business)"
            _src_label = (
                "GooglePlaces" if _is_google_intent
                else "Apollo-Org" if _is_apollo_only
                else "EnrichOFF"
            )
            _stub = {
                "name": _root,
                "domain": domain,
                "company": company_name or _root,
                "role": "",
                "email": "",
                "phone": "",
                "source": _src_label,
                "_google_intent": bool(_is_google_intent),
                "_domain_source": "paid",   # placeholder for export label code path
                "_paid_traffic":    int(traffic_metrics.get("paid_traffic", 0) or 0),
                "_organic_traffic": int(traffic_metrics.get("organic_traffic", 0) or 0),
                "_paid_keywords":   int(traffic_metrics.get("paid_keywords", 0) or 0),
                "_organic_keywords":int(traffic_metrics.get("organic_keywords", 0) or 0),
                "_revenue": "",
                "_city_scope": (self._city_scope or {}).get("label", ""),
                "_silent_scope_apollo": bool(traffic_metrics.get("_silent_scope_apollo") or _is_apollo_only),
                "_dm_priority": 50,
                "_stub_lead": True,
            }
            self._log(
                f"   [{index + 1}/{total}] {domain}: Apollo had 0 people — "
                f"creating {_src_label} stub lead (name='{_root}')"
            )
            return [_stub]

        # Revenue fallback: organizations/enrich often returns empty for AU SMBs.
        # The nested organization object inside each person response is more reliable.
        if not company_revenue and people:
            for _p in people:
                company_revenue = _extract_apollo_revenue(_p.get("organization") or {})
                if company_revenue:
                    break
        if not company_linkedin and people:
            for _p in people:
                company_linkedin = _extract_linkedin_url(_p.get("organization") or {})
                if company_linkedin:
                    break

        # CREDIT-LEAK FIX: when enrichment was OFF we skipped enrich_organization
        # above. Backfill company_name / company_phone / employees from the
        # people-search response (free), and apply the 500-employee SMB filter here.
        if not self.enrichment_enabled and people:
            _org_obj = (people[0].get("organization") or {})
            if not company_name:
                company_name = safe_str(_org_obj.get("name"))
            if not company_phone:
                company_phone = safe_str(
                    _org_obj.get("primary_phone", {}).get("number")
                    if isinstance(_org_obj.get("primary_phone"), dict)
                    else _org_obj.get("phone") or ""
                )
            _emp_n = (
                _org_obj.get("estimated_num_employees")
                or _org_obj.get("num_employees")
                or 0
            )
            try:
                _emp_n = int(_emp_n)
            except (ValueError, TypeError):
                _emp_n = 0
            if _emp_n > 500:
                self._log(f"   [{index + 1}/{total}] {domain}: skipped (large company, {_emp_n} employees)")
                return []

        # V5.8: Smart filtering — reduce people list by relevance before expensive enrichment
        # This prevents wasting credits on low-relevance leads (interns, support staff, etc.)
        original_count = len(people)
        if self.max_leads > 0 and len(people) > 10:
            people = _filter_people_by_relevance(people, self.max_leads)
            self._log(f"   V5.8: Filtered {original_count} people → {len(people)} high-relevance (max_leads={self.max_leads})")
        for person in people:
            first = safe_str(person.get("first_name"))
            # V5.13 Bug Fix: Reject company names / possessives used as first name
            if first and ("'" in first or any(c.isdigit() for c in first) or len(first) < 2):
                first = ""
            last = safe_str(person.get("last_name"))
            # Enrichment OFF: use last_name_obfuscated ("Si***h") when Apollo
            # masks the last name with a single initial ("M."). This preserves
            # partial-name leads in the output instead of reducing to first name only.
            if not self.enrichment_enabled:
                _lo = safe_str(person.get("last_name_obfuscated"))
                if _lo and (not last or (len(last.rstrip(".")) == 1 and last.rstrip(".").isalpha())):
                    last = _lo
            title = safe_str(person.get("title"))
            # V5.20: Smart email selection — business-tagged > personal, with name check
            email, email_from_apollo_personal_list = _pick_best_email_from_apollo(
                person, first, last
            )
            # Store generic org email as fallback (will be used only if no personal found)
            org_email = safe_str(person.get("email"))
            generic_email = org_email if (org_email and not is_personal_email(org_email)) else ""
            # V5.25: Smart phone selection — pass company_phone to exclude HQ numbers
            person_phone, phone_quality = _pick_best_phone_from_apollo(person, company_phone)
            if first:
                full_name = get_full_name(person) or (f"{first} {last}".strip() if last else first)
                # V5.18: Only reject if last name looks like a CLEAR title (multi-word role like
                # "managing director") — trust Apollo's last_name field for single-word surnames.
                # Single words like "Director" as a last name are extremely rare and rejecting hurts
                # coverage more than it prevents errors.
                if last and " " in full_name:
                    _last_lower = last.lower()
                    _multi_word_roles = {kw for kw in _NAME_FORBIDDEN_WORDS if " " in kw}
                    _single_bad = {"ceo", "cfo", "cto", "coo", "managing", "executive"}
                    if _last_lower in _multi_word_roles or _last_lower in _single_bad:
                        full_name = first  # Only fall back for obvious title words

                # V5.19: Strip Apollo's obfuscated "FirstName I." format (e.g. "Matt M." → "Matt")
                # When enrichment is ON, strip so downstream name-resolution guards trigger correctly.
                # When enrichment is OFF, keep the partial name — "Matt M." passes _phase5_cleanup
                # (has a space) and surfaces in the CSV rather than being dropped entirely.
                if _is_obfuscated_name(full_name) and self.enrichment_enabled:
                    full_name = full_name.split()[0]

                # V5.3: Also check Apollo's `name` field directly (may have full name)
                apollo_name = safe_str(person.get("name"))
                if apollo_name and " " in apollo_name and " " not in full_name:
                    # V5.19: Skip if the apollo name itself is obfuscated ("Matt M." format)
                    if not _is_obfuscated_name(apollo_name):
                        # V5.13 Bug Fix: Reject if any word after first is a title/role word
                        _role_words = HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS | TRADE_ROLE_WORDS
                        _name_words = apollo_name.lower().split()
                        if not any(w in _role_words for w in _name_words[1:]):
                            full_name = apollo_name

                # V5.3: Extract LinkedIn URL — check both Apollo field name variants
                person_linkedin = (
                    _extract_linkedin_url(person)
                    or _extract_linkedin_url(person.get("organization") or {})
                    or company_linkedin
                )
                lead_company = company_name or safe_str((person.get("organization") or {}).get("name"))

                # V5.9: LinkedIn URL name resolution (reliable — Apollo actual profile URL)
                if " " not in full_name and person_linkedin:
                    resolved = _extract_name_from_linkedin_url(first, person_linkedin)
                    if resolved and " " in resolved:
                        full_name = resolved

                # V5.13: Re-enable company + domain name extraction
                # _extract_name_from_company/_extract_name_from_domain have built-in
                # business-suffix guards — they only match when name is clearly encoded
                if " " not in full_name and first and lead_company:
                    resolved = _extract_name_from_company(first, lead_company)
                    if resolved and " " in resolved:
                        full_name = resolved

                if " " not in full_name and first and domain:
                    resolved = _extract_name_from_domain(first, domain)
                    if resolved and " " in resolved:
                        full_name = resolved

                lead = {
                    "name": full_name,
                    "domain": domain,
                    "company": lead_company,
                    "role": title,
                    "email": email or "",
                    "phone": person_phone or "",
                    "source": "Apollo",
                    "_generic_email": generic_email,  # internal: fallback only
                    "_needs_full_name": " " not in full_name,  # V5.2: track single-name leads
                    "_linkedin_url": person_linkedin,  # V5.3: store for later resolution
                    "_company_linkedin_url": company_linkedin,
                    "_apollo_id": safe_str(person.get("id")),  # V5.19: exact record ID for people/match
                    "_email_verified": email_from_apollo_personal_list,  # V5.10: Apollo-authoritative personal email
                    "_direct_phone": bool(person_phone) and phone_quality >= 30,  # V5.22: True only for mobile/direct/personal/home (not company HQ)
                    "_phone_quality": phone_quality if person_phone else 0,  # V5.22: track quality for comparison
                    "_company_phone": company_phone,  # V5.26: store for async phone collection filtering
                    "_domain_source": "paid" if domain in self._adwords_domains else ("organic" if domain in self._organic_domains else "paid"),  # V5.13
                    "_paid_keywords": traffic_metrics.get("paid_keywords", 0),      # V5.13
                    "_organic_keywords": traffic_metrics.get("organic_keywords", 0),  # V5.13
                    "_paid_traffic": traffic_metrics.get("paid_traffic", 0),          # V5.13
                    "_organic_traffic": traffic_metrics.get("organic_traffic", 0),    # V5.13
                    "_revenue": company_revenue,  # V5.27: Apollo annual revenue
                    "_city_scope": (self._city_scope or {}).get("label", ""),  # PHASE 2 city mode tag
                    # 2026-05-18 (round 4): Google Places provenance flag.
                    # Set when this domain was discovered via Places Text
                    # Search (NOT SEMrush phrase_adwords). CSV exports map
                    # this to Traffic Source = "Google Intent". Never
                    # implies confirmed_paid.
                    "_google_intent": bool(traffic_metrics.get("_google_intent")),
                    "_silent_scope_apollo": bool(traffic_metrics.get("_silent_scope_apollo")),
                }
                domain_leads.append(lead)

        # Step 2a.5: V5.13 — Multi-source pre-enrichment for single-name leads
        # Runs BEFORE Apollo enrich_person (Step 2b) to resolve first-only names.
        # Layer A: SerpAPI LinkedIn query (fast, no export credits)
        # Layer B: Apollo people/search by first_name + domain (gets LinkedIn URL + last name)
        # V5.14: Removed _has_enough_leads() gate — name resolution uses no Apollo export credits
        #        and must run for ALL single-name leads regardless of quota state.
        # PHASE 2 FIX (2026-04-28): Moved this block ABOVE the enrichment-OFF early
        # return. Apollo's basic people-search returns obfuscated names ("Matt M.")
        # that strip to single names; without these layers running, every
        # enrichment-OFF run would feed only single-name leads into _phase5_cleanup
        # and the full-name policy would drop them all (last run: 125 → 0).
        # Layers A + B don't consume Apollo enrich credits — they use SerpAPI
        # search credits and the same free Apollo mixed_people/api_search
        # endpoint already used to find the people in the first place.
        for ld in domain_leads:
            name_str = ld.get("name", "")
            if not name_str or " " in name_str or ld.get("_linkedin_url"):
                continue  # skip: already has full name or LinkedIn URL

            co_name = ld.get("company") or company_name or domain

            # Layer A: SerpAPI LinkedIn-targeted name lookup (SerpAPI search credits).
            # 2026-06-09: soft per-run cap so a small-max_leads run doesn't burn
            # SerpAPI resolving names for the many extra candidates it discovers
            # then discards (was 26 lookups for a 1-lead run). Layer B (FREE
            # Apollo) still runs for any lead skipped here, so the full-name
            # policy is preserved — we just stop paying to name leads we drop.
            _name_budget = (max(15, int(self.max_leads) * 6)
                            if int(self.max_leads or 0) > 0 else 10**9)
            _name_used = getattr(self, "_serp_name_calls", 0)
            if self.serpapi._available and _name_used < _name_budget:
                self._serp_name_calls = _name_used + 1
                linkedin_name = self.serpapi.find_person_on_linkedin(name_str, co_name)
                if linkedin_name and " " in linkedin_name:
                    ld["name"] = linkedin_name
                    ld["_needs_full_name"] = False
                    ld["source"] += "+SerpLI"
                    self._log(f"   [Name] SerpAPI LinkedIn → '{name_str}' resolved to '{linkedin_name}'")
                    continue  # Name resolved, skip Layer B

            # Layer B: Apollo people/search by first_name + domain (no export credits)
            # V5.14: Also searches without email_status filter so unverified contacts are included
            try:
                search_url = f"{self.apollo.BASE_URL}/mixed_people/api_search"
                payload = {
                    "q_person_name": name_str,
                    "q_organization_domains": [domain],
                    "per_page": 5,
                }
                resp = requests.post(
                    search_url, json=payload,
                    headers=self.apollo._headers(), timeout=20
                )
                if resp.status_code == 200:
                    people_results = resp.json().get("people", [])
                    for candidate in people_results:
                        c_first = safe_str(candidate.get("first_name")).lower().strip()
                        if c_first == name_str.lower():
                            c_last = safe_str(candidate.get("last_name")).strip()
                            c_linkedin = safe_str(candidate.get("linkedin_url")).strip()
                            c_apollo_id = safe_str(candidate.get("id"))
                            # V5.19: Always save Apollo ID — enables exact people/match in Step 2b
                            if c_apollo_id and not ld.get("_apollo_id"):
                                ld["_apollo_id"] = c_apollo_id
                            # V5.19: Reject obfuscated last names (single letter like "M." or "L.")
                            last_clean = c_last.rstrip(".")
                            is_obfuscated_last = len(last_clean) == 1 and last_clean.isalpha()
                            if c_last and not is_obfuscated_last and _is_valid_person_name(f"{name_str} {c_last}"):
                                ld["name"] = f"{name_str} {c_last}"
                                ld["_needs_full_name"] = False
                                ld["source"] += "+ApolloSearch"
                                self._log(f"   [Name] Apollo search → '{name_str}' resolved to '{ld['name']}'")
                            if c_linkedin:
                                ld["_linkedin_url"] = c_linkedin
                            break
            except Exception:
                pass

        # PHASE 2 ENRICHMENT GATE: when toggle is OFF, stop here. domain_leads
        # now has name/company/domain/role from Apollo's single people-search
        # call PLUS any full-name resolutions from Layer A/B above. No further
        # Apollo enrich_person, Lusha, Hunter, OpenAI, or scraping calls will
        # run — the CSV export will blank email/phone.
        if not self.enrichment_enabled:
            if self._city_scope:
                for ld in domain_leads:
                    ld["_city_scope"] = self._city_scope.get("label", "")
            # Drive the credit gate on raw-lead count so Phase 4 stops
            # submitting more domains once we've collected ~2× max_leads.
            if self.max_leads > 0 and domain_leads:
                with self._complete_leads_lock:
                    self._phone_leads_count += len(domain_leads)
            return domain_leads

        # Step 2b: V5.7 — Apollo enrich for leads missing personal email
        # V5.18: Phone quota no longer blocks name/email resolution within a domain.
        # Only skip a lead if quota is reached AND it already has full name + any email.
        # This ensures single-name leads and leads missing personal email still get enriched.
        for ld in domain_leads:
            if self._has_enough_leads():
                # At quota — only skip if lead has a full name AND already has some email
                has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
                has_any_email = bool(ld.get("email"))
                if has_full_name and has_any_email:
                    continue  # Lead is reasonably complete — skip at quota
                # Fall through: still resolve name or find first email even at quota
            if self._lead_is_complete(ld):
                continue

            # V5.8: Skip enrichment for low-relevance leads when max_leads is set (credit saving)
            if self.max_leads > 0:
                role = ld.get("role", "").lower()
                is_low_relevance = any(kw in role for kw in LOW_RELEVANCE_KEYWORDS)
                if is_low_relevance:
                    continue  # Skip expensive enrichment for interns, support staff, etc.

            # Phase 2 (2026-05-05): Apollo credit gate. enrich_person costs
            # email + phone + export pools; non-DMs get culled in _phase5f
            # anyway, so paying to enrich them is pure waste. Skip when role
            # is present AND scores 0 in DM priority (definitive non-DM).
            # Empty roles still pass through — Apollo can fill the role.
            if self.max_leads > 0:
                _role_for_dm = ld.get("role", "")
                if _role_for_dm and _role_for_dm.strip():
                    if _calculate_dm_priority(_role_for_dm) == 0:
                        continue

            name = ld.get("name", "")
            needs_name = name and " " not in name
            needs_email = not ld.get("email") or not is_personal_email(ld.get("email", ""))
            if needs_name or needs_email:
                parts = name.split() if name else [""]
                first_n = parts[0] if parts else ""
                last_n = parts[-1] if len(parts) > 1 else ""
                linkedin_url = ld.get("_linkedin_url", "")
                # V5.7: Call for ALL leads with at least a first name
                if first_n:
                    enriched = self.apollo.enrich_person(
                        first_n, last_n, domain, linkedin_url,
                        organization_name=company_name,  # V5.10+: improves Apollo matching
                        apollo_id=ld.get("_apollo_id", ""),  # V5.19: exact record lookup
                        company_phone=company_phone,  # V5.25: exclude company HQ phone
                    )
                    if enriched:
                        # V5.18: Always update name from Apollo match when better data available
                        enriched_name = enriched.get("name", "")
                        # V5.19: Never accept an obfuscated name ("Matt M.") from people/match
                        if enriched_name and _is_obfuscated_name(enriched_name):
                            enriched_name = enriched_name.split()[0]  # Strip initial, keep first name only
                        if enriched_name and " " in enriched_name:
                            if not (ld.get("name") and " " in ld.get("name", "")):
                                # Single-name lead — upgrade to full name
                                ld["name"] = enriched_name
                                ld["_needs_full_name"] = False
                                ld["source"] += "+ApolloName"
                        _enriched_email = enriched.get("email", "")
                        if _enriched_email:
                            _current_email = ld.get("email", "")
                            _enriched_is_consumer = is_personal_email(_enriched_email)
                            _current_is_consumer = is_personal_email(_current_email) if _current_email else False
                            _current_is_business = bool(_current_email) and not _current_is_consumer
                            _current_local = _current_email.split("@")[0].lower() if _current_email else ""
                            _current_is_generic = _current_local in GENERIC_EMAIL_PREFIXES

                            if not _current_email:
                                # No email yet — use whatever enrichment returned
                                ld["email"] = _enriched_email
                                ld["source"] += "+ApolloEmail"
                                if not ld.get("_email_verified"):
                                    ld["_email_verified"] = True
                                    with self._complete_leads_lock:
                                        self._email_credits_used += 1
                            elif not _enriched_is_consumer and _current_is_consumer:
                                # V5.22: Enriched has BUSINESS email, current has consumer — upgrade
                                ld["email"] = _enriched_email
                                ld["source"] += "+ApolloEmail"
                                if not ld.get("_email_verified"):
                                    ld["_email_verified"] = True
                                    with self._complete_leads_lock:
                                        self._email_credits_used += 1
                            elif not _enriched_is_consumer and _current_is_generic:
                                # V5.22: Enriched has business email, current is generic inbox — upgrade
                                ld["email"] = _enriched_email
                                ld["source"] += "+ApolloEmail"
                            elif not _enriched_is_consumer and not _current_is_business:
                                # V5.19: Prefer name-based work email over generic inbox email
                                _local = _enriched_email.split("@")[0].lower()
                                _is_name_based = first_n and first_n.lower() in _local
                                if _is_name_based:
                                    ld["email"] = _enriched_email
                                    ld["source"] += "+ApolloEmail"
                            # V5.22: Never replace a business email with a consumer (gmail/hotmail) email
                        if not ld.get("role") and enriched.get("role"):
                            ld["role"] = enriched["role"]
                        # V5.26: Store apollo_id from enrichment for async phone collection
                        if enriched.get("_apollo_id") and not ld.get("_apollo_id"):
                            ld["_apollo_id"] = enriched["_apollo_id"]
                        # V5.22: Replace phone only if enriched phone has higher quality score
                        if enriched.get("phone"):
                            new_quality = enriched.get("_phone_quality", 20)
                            existing_quality = ld.get("_phone_quality", 0)
                            if not ld.get("phone") or new_quality > existing_quality:
                                ld["phone"] = enriched["phone"]
                                ld["_direct_phone"] = new_quality >= 30
                                ld["_phone_quality"] = new_quality
                                with self._complete_leads_lock:
                                    self._phone_credits_used += 1  # V5.10+

        # Step 2c: V5.6 — LinkedIn-URL-targeted enrichment for remaining single-name leads
        # V5.18: Same quota logic as 2b — still resolve names/emails even at quota.
        for ld in domain_leads:
            if self._has_enough_leads():
                has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
                has_any_email = bool(ld.get("email"))
                if has_full_name and has_any_email:
                    continue
            if self._lead_is_complete(ld):
                continue

            # V5.8: Skip enrichment for low-relevance leads when max_leads is set
            if self.max_leads > 0:
                role = ld.get("role", "").lower()
                is_low_relevance = any(kw in role for kw in LOW_RELEVANCE_KEYWORDS)
                if is_low_relevance:
                    continue  # Skip expensive enrichment

            # Phase 2 (2026-05-05): Apollo DM-gate (same rationale as Step 2b above).
            if self.max_leads > 0:
                _role_for_dm = ld.get("role", "")
                if _role_for_dm and _role_for_dm.strip():
                    if _calculate_dm_priority(_role_for_dm) == 0:
                        continue

            name = ld.get("name", "")
            linkedin_url = ld.get("_linkedin_url", "")
            if not linkedin_url:
                continue  # No LinkedIn URL = can't do precise match
            if name and " " in name and ld.get("email") and is_personal_email(ld["email"]):
                continue  # Already fully enriched
            first_n = name.split()[0] if name else ""
            last_n = name.split()[-1] if name and " " in name else ""
            enriched = self.apollo.enrich_person(
                first_n, last_n, domain, linkedin_url,
                organization_name=company_name,  # V5.10+
                apollo_id=ld.get("_apollo_id", ""),  # V5.19: exact record lookup
                company_phone=company_phone,  # V5.25: exclude company HQ phone
            )
            if enriched:
                # V5.18: Update name from LinkedIn-matched Apollo record
                if enriched.get("name") and " " in enriched["name"] and " " not in ld.get("name", ""):
                    ld["name"] = enriched["name"]
                    ld["_needs_full_name"] = False
                    ld["source"] += "+ApolloLI"
                _2c_email = enriched.get("email", "")
                if _2c_email:
                    _2c_current = ld.get("email", "")
                    _2c_enriched_is_consumer = is_personal_email(_2c_email)
                    _2c_current_is_consumer = is_personal_email(_2c_current) if _2c_current else False
                    _2c_current_is_business = bool(_2c_current) and not _2c_current_is_consumer
                    if not _2c_current:
                        ld["email"] = _2c_email
                        if not ld.get("_email_verified"):
                            ld["_email_verified"] = True
                            with self._complete_leads_lock:
                                self._email_credits_used += 1
                    elif not _2c_enriched_is_consumer and _2c_current_is_consumer:
                        # V5.22: Business email upgrades consumer email
                        ld["email"] = _2c_email
                        if not ld.get("_email_verified"):
                            ld["_email_verified"] = True
                            with self._complete_leads_lock:
                                self._email_credits_used += 1
                    # V5.22: Never replace business with consumer email
                # V5.26: Store apollo_id from enrichment for async phone collection
                if enriched.get("_apollo_id") and not ld.get("_apollo_id"):
                    ld["_apollo_id"] = enriched["_apollo_id"]
                # V5.22: Replace phone only if enriched phone has higher quality score
                if enriched.get("phone"):
                    new_quality = enriched.get("_phone_quality", 20)
                    existing_quality = ld.get("_phone_quality", 0)
                    if not ld.get("phone") or new_quality > existing_quality:
                        ld["phone"] = enriched["phone"]
                        ld["_direct_phone"] = new_quality >= 30
                        ld["_phone_quality"] = new_quality
                        with self._complete_leads_lock:
                            self._phone_credits_used += 1  # V5.10+

        # Step 2d removed (V5.9): Email pattern inference was generating fake emails
        # (firstname.lastname@domain fabrications). Only real emails from Apollo/Lusha/scraping kept.

        # Step 2e: V5.10+ — Personal email second pass via Apollo email-status filter
        # V5.18: Removed phone-quota gate — always run this pass. Increased per_page to 10.
        # Threshold raised: run until we have at least max_leads personal emails (or 3 minimum).
        # V5.32: Skip entirely if Apollo budget exhausted.
        personal_so_far = sum(
            1 for ld in domain_leads if is_personal_email(ld.get("email", ""))
        )
        personal_target = max(3, self.max_leads) if self.max_leads > 0 else 5
        if personal_so_far < personal_target and self._apollo_budget_ok():
            email_candidates = self.apollo.search_email_verified_people(domain, per_page=10)
            for person in email_candidates:
                # V5.18: No quota break — process all candidates for personal emails
                first = safe_str(person.get("first_name"))
                if not first:
                    continue
                last = safe_str(person.get("last_name"))
                li_url = safe_str(person.get("linkedin_url"))
                # Find existing lead for this person
                matching_lead = None
                for ld in domain_leads:
                    ld_first = (ld.get("name") or "").split()[0].lower()
                    if ld_first and ld_first == first.lower():
                        matching_lead = ld
                        break
                if matching_lead and is_personal_email(matching_lead.get("email", "")):
                    continue  # already has personal email
                # V5.22: Skip if matching lead already has a business email (don't downgrade to consumer)
                if matching_lead and matching_lead.get("email") and not is_personal_email(matching_lead.get("email", "")):
                    _ml_local = matching_lead["email"].split("@")[0].lower()
                    if _ml_local not in GENERIC_EMAIL_PREFIXES:
                        continue  # Has a business email — skip consumer email search
                # Run targeted enrich_person for this Apollo-verified-email person
                enriched = self.apollo.enrich_person(
                    first, last, domain, li_url, organization_name=company_name
                )
                if enriched and is_personal_email(enriched.get("email", "")):
                    personal_email = enriched["email"]
                    if matching_lead:
                        _ml_current = matching_lead.get("email", "")
                        _ml_is_business = bool(_ml_current) and not is_personal_email(_ml_current)
                        _ml_local = _ml_current.split("@")[0].lower() if _ml_current else ""
                        _ml_is_generic = _ml_local in GENERIC_EMAIL_PREFIXES
                        # V5.22: Only use consumer email if no business email exists OR current is generic prefix
                        if not _ml_current or _ml_is_generic:
                            matching_lead["email"] = personal_email
                            matching_lead["_email_verified"] = True
                            matching_lead["source"] += "+Apollo2E"
                            with self._complete_leads_lock:
                                self._email_credits_used += 1
                    else:
                        # New person Apollo knows about — add as a lead
                        title = safe_str(person.get("title"))
                        new_ld = {
                            "name": enriched.get("name") or f"{first} {last}".strip(),
                            "domain": domain,
                            "company": company_name or domain_to_company_name(domain),
                            "role": enriched.get("role") or title,
                            "email": personal_email,
                            "phone": enriched.get("phone") or "",
                            "source": "Apollo+Apollo2E",
                            "_email_verified": True,
                            "_direct_phone": bool(enriched.get("phone")) and enriched.get("_phone_quality", 0) >= 30,
                            "_phone_quality": enriched.get("_phone_quality", 0),
                            "_domain_source": "paid" if domain in self._adwords_domains else ("organic" if domain in self._organic_domains else "paid"),  # V5.13
                            "_needs_full_name": False,
                            "_linkedin_url": li_url,
                            "_generic_email": "",
                        }
                        domain_leads.append(new_ld)
                        with self._complete_leads_lock:
                            self._email_credits_used += 1
            self._log(f"   V5.18 Step2e: {sum(1 for ld in domain_leads if is_personal_email(ld.get('email', '')))} "
                      f"personal emails after email-status pass")

        # Step 3: Lusha company data — V5: ALWAYS call Lusha for company info
        # V5.8: Skip if no high-relevance leads found (credit optimization)
        has_high_relevance_leads = any(
            not any(kw in ld.get("role", "").lower() for kw in LOW_RELEVANCE_KEYWORDS)
            for ld in domain_leads
        )
        if self.max_leads > 0 and not has_high_relevance_leads:
            # No high-relevance leads found, skip Lusha call to save credits
            pass
        else:
            lusha_company = self.lusha.get_company_info(domain)
            if lusha_company:
                lusha_co_name = lusha_company.get("company_name", "")
                if lusha_co_name:
                    company_name = lusha_co_name
                    for ld in domain_leads:
                        if not ld.get("company"):
                            ld["company"] = lusha_co_name
                            ld["source"] += "+Lusha"

        # V5.24: Deduplication guard — shared phones are company-level, not personal
        # If 2+ leads from this domain have the same phone, strip it and let Lusha fill.
        _phone_freq: dict = {}
        for ld in domain_leads:
            p = ld.get("phone", "")
            if p:
                _phone_freq[p] = _phone_freq.get(p, 0) + 1
        for ld in domain_leads:
            p = ld.get("phone", "")
            if p and _phone_freq.get(p, 0) >= 2:
                ld["_dedup_stripped_phone"] = p  # V5.24: remember what was stripped
                ld["phone"] = ""
                ld["_phone_quality"] = 0
                ld["_direct_phone"] = False

        # Step 4: Lusha person enrichment — try for leads with a name
        # V5.18: Same quota logic — still enrich single-name leads and email-less leads at quota.
        for ld in domain_leads:
            if self._has_enough_leads():
                has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
                has_any_email = bool(ld.get("email"))
                if has_full_name and has_any_email:
                    continue
            if self._lead_is_complete(ld):  # V5.1: Skip if already complete
                continue
            if not ld.get("name"):
                continue

            # V5.8: Skip Lusha enrichment for low-relevance leads when max_leads is set (major credit save)
            if self.max_leads > 0:
                role = ld.get("role", "").lower()
                is_low_relevance = any(kw in role for kw in LOW_RELEVANCE_KEYWORDS)
                if is_low_relevance:
                    continue  # Skip expensive Lusha enrichment for non-decision-makers

            # Hard Lusha budget cap — prevents runaway spend when Apollo is broken
            # and _has_enough_leads() never fires (phone counter stays at 0).
            # Cap = max_leads * 3 person calls (3x buffer for cleanup losses).
            _lusha_budget = max(self.max_leads * 3, 30) if self.max_leads > 0 else 150
            if self.lusha._counter.get("lusha", 0) >= _lusha_budget:
                self._log(f"   Lusha budget cap reached ({_lusha_budget} calls) — skipping further person enrichment")
                break

            parts = ld["name"].split()
            first_n = parts[0]
            last_n = parts[-1] if len(parts) > 1 else ""
            lusha_person = self.lusha.enrich_person(first_n, last_n, domain)
            if lusha_person:
                lusha_name = lusha_person.get("name", "")
                if lusha_name and " " in lusha_name:
                    if " " not in ld.get("name", ""):
                        ld["name"] = lusha_name
                lusha_email = lusha_person.get("email", "")
                if lusha_email:
                    _l4_current = ld.get("email", "")
                    _l4_lusha_is_consumer = is_personal_email(lusha_email)
                    _l4_current_is_consumer = is_personal_email(_l4_current) if _l4_current else False
                    _l4_current_is_business = bool(_l4_current) and not _l4_current_is_consumer
                    _l4_current_local = _l4_current.split("@")[0].lower() if _l4_current else ""
                    _l4_current_is_generic = _l4_current_local in GENERIC_EMAIL_PREFIXES

                    if not _l4_current:
                        # No email yet — use Lusha email
                        ld["email"] = lusha_email
                        if not _l4_lusha_is_consumer:
                            ld["_email_verified"] = True
                    elif not _l4_lusha_is_consumer and (_l4_current_is_consumer or _l4_current_is_generic):
                        # V5.22: Lusha has BUSINESS email, current has consumer/generic — upgrade
                        ld["email"] = lusha_email
                        ld["_email_verified"] = True
                        with self._complete_leads_lock:
                            self._email_credits_used += 1  # V5.10+
                    elif _l4_lusha_is_consumer and not _l4_current_is_business and not _l4_current:
                        # Lusha has consumer, no current business email
                        ld["email"] = lusha_email
                        ld["_email_verified"] = True
                        with self._complete_leads_lock:
                            self._email_credits_used += 1
                    # V5.22: Never replace business email with consumer (gmail/hotmail) email
                # V5.22: Replace phone using quality comparison — prefer higher quality (mobile over HQ)
                if lusha_person.get("phone"):
                    lusha_quality = lusha_person.get("_phone_quality", 50)  # default 50 (mobile) if not tracked
                    existing_quality = ld.get("_phone_quality", 0)
                    if not ld.get("phone") or lusha_quality > existing_quality or (lusha_quality >= 30 and existing_quality < 30):
                        ld["phone"] = lusha_person["phone"]
                        ld["_direct_phone"] = lusha_quality >= 30
                        ld["_phone_quality"] = lusha_quality
                        with self._complete_leads_lock:
                            self._phone_credits_used += 1  # V5.10+
                if not ld.get("role") and lusha_person.get("role"):
                    ld["role"] = lusha_person["role"]
                ld["source"] += "+Lusha"

        # Step 4b: V5.2 — SerpApi full-name fallback for remaining single-name leads
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            first_only = ld.get("name", "")
            if not first_only:
                continue
            co = ld.get("company") or company_name or domain
            full_name = self.serpapi.find_person_full_name(
                first_only, co, domain, self.config["serpapi_gl"]
            )
            if full_name and " " in full_name:
                ld["name"] = full_name
                ld["_needs_full_name"] = False
                ld["source"] += "+SerpApiName"

        # Step 4b.5: V5.13 — DuckDuckGo instant-answer fallback for still-single-name leads
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            first_only = ld.get("name", "")
            if not first_only:
                continue
            co_name = ld.get("company") or company_name or domain
            try:
                ddg_resp = requests.get(
                    "https://api.duckduckgo.com/",
                    params={"q": f"{first_only} {co_name}", "format": "json", "no_redirect": "1"},
                    timeout=8,
                )
                if ddg_resp.status_code == 200:
                    ddg_data = ddg_resp.json()
                    ddg_text = ddg_data.get("AbstractText", "") + " " + str(ddg_data.get("RelatedTopics", ""))
                    name_pat = re.compile(rf"\b{re.escape(first_only)}\s+([A-Z][a-z]{{2,20}})\b")
                    m = name_pat.search(ddg_text)
                    if m:
                        candidate = f"{first_only} {m.group(1)}"
                        if _is_valid_person_name(candidate):
                            ld["name"] = candidate
                            ld["_needs_full_name"] = False
                            ld["source"] += "+DDG"
            except Exception:
                pass

        # Step 4c: V5.13 — Apollo bulk_match for leads still missing personal email
        missing_email_leads = [
            ld for ld in domain_leads
            if not ld.get("email") or not is_personal_email(ld.get("email", ""))
        ]
        if missing_email_leads and self._apollo_budget_ok():  # V5.32: gated by Apollo budget
            batch_url = f"{self.apollo.BASE_URL}/people/bulk_match"
            match_records = []
            for ld in missing_email_leads[:10]:  # Apollo limit per batch
                parts = ld.get("name", "").split()
                record = {"reveal_personal_emails": True, "reveal_phone_number": True}
                if parts:
                    record["first_name"] = parts[0]
                if len(parts) > 1:
                    record["last_name"] = parts[-1]
                if domain:
                    record["domain"] = domain
                if ld.get("_linkedin_url"):
                    record["linkedin_url"] = ld["_linkedin_url"]
                match_records.append(record)
            if match_records:
                try:
                    resp = requests.post(
                        batch_url,
                        json={"details": match_records},
                        headers=self.apollo._headers(),
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        self.apollo._counter["apollo"] = self.apollo._counter.get("apollo", 0) + 1
                        matches = resp.json().get("matches", [])
                        for i, match in enumerate(matches):
                            if i >= len(missing_email_leads) or not match:
                                continue
                            ld = missing_email_leads[i]
                            person = match.get("person") or match
                            m_first = safe_str(person.get("first_name"))
                            m_last = safe_str(person.get("last_name"))
                            # V5.18: Trust Apollo bulk_match name data — less restrictive validation
                            if m_last and " " not in ld.get("name", ""):
                                candidate = f"{m_first} {m_last}".strip()
                                _last_lower = m_last.lower()
                                _bad_last = {"ceo", "cfo", "cto", "coo", "managing", "executive"}
                                if _last_lower not in _bad_last:
                                    ld["name"] = candidate
                                    ld["_needs_full_name"] = False
                                    ld["source"] += "+BulkName"
                            # V5.22: Prefer business emails over consumer — collect all candidates
                            _bm_current = ld.get("email", "")
                            _bm_current_is_business = bool(_bm_current) and not is_personal_email(_bm_current)
                            _bm_current_local = _bm_current.split("@")[0].lower() if _bm_current else ""
                            _bm_current_is_generic = _bm_current_local in GENERIC_EMAIL_PREFIXES
                            if not _bm_current or _bm_current_is_generic:
                                # No email or generic prefix — find best email from match
                                _match_emails = []
                                for em_field in ["email", "contact_email"]:
                                    em_val = person.get(em_field)
                                    if em_val:
                                        _match_emails.append(em_val)
                                _pe = person.get("personal_emails")
                                if isinstance(_pe, list):
                                    _match_emails.extend([e for e in _pe if e])
                                elif _pe:
                                    _match_emails.append(_pe)
                                # Pick best: business email first, then consumer as fallback
                                _picked = ""
                                for em_val in _match_emails:
                                    if em_val and is_valid_email(str(em_val)) and not is_personal_email(str(em_val)):
                                        _local_val = str(em_val).split("@")[0].lower()
                                        if _local_val not in GENERIC_EMAIL_PREFIXES:
                                            _picked = str(em_val)
                                            break
                                if not _picked:
                                    for em_val in _match_emails:
                                        if em_val and is_valid_email(str(em_val)):
                                            _picked = str(em_val)
                                            break
                                if _picked:
                                    ld["email"] = _picked
                                    ld["_email_verified"] = not is_personal_email(_picked)
                                    ld["source"] += "+BulkEmail"
                except Exception:
                    pass

        # Step 4d: V5.13 — Hunter.io domain-search for emails (second-best after Apollo personal)
        # Hunter.io returns real emails with confidence scores. Match to leads by first+last name.
        # EMAIL PRIORITY: 1=Apollo personal_emails, 2=Hunter.io, 3=Apollo verified search,
        #                 4=Inferred pattern, 5=Scraped company email (last resort)
        if self.hunter._available:  # V5.18: removed quota gate — Hunter provides full names
            hunter_contacts = self.hunter.domain_search(domain, limit=10)
            if hunter_contacts:
                self._log(f"   [Hunter] Found {len(hunter_contacts)} contacts at {domain}")
            for contact in hunter_contacts:
                h_first = safe_str(contact.get("first_name")).lower()
                h_last = safe_str(contact.get("last_name")).lower()
                h_email = safe_str(contact.get("value") or contact.get("email"))
                h_role = safe_str(contact.get("position") or contact.get("type", ""))
                if not h_email or not is_valid_email(h_email):
                    continue
                # Match Hunter contact to an existing lead by first and/or last name
                for ld in domain_leads:
                    if ld.get("_email_verified") and is_personal_email(ld.get("email", "")):
                        continue  # already has best-quality email
                    lead_name = (ld.get("name") or "").lower()
                    lead_parts = lead_name.split()
                    lead_first = lead_parts[0] if lead_parts else ""
                    lead_last = lead_parts[-1] if len(lead_parts) > 1 else ""
                    matched = False
                    if h_first and lead_first and h_first == lead_first:
                        matched = True
                    if h_last and lead_last and h_last == lead_last and h_first == lead_first:
                        matched = True  # strong match
                    # Also update full name if Hunter knows last name and we only have first
                    if matched:
                        _h_current = ld.get("email", "")
                        _h_current_is_business = bool(_h_current) and not is_personal_email(_h_current)
                        _h_current_local = _h_current.split("@")[0].lower() if _h_current else ""
                        _h_current_is_generic = _h_current_local in GENERIC_EMAIL_PREFIXES
                        # V5.22: Only update email if no email or current is generic prefix
                        # Don't overwrite a good business email with Hunter's email
                        if not _h_current or _h_current_is_generic:
                            ld["email"] = h_email
                            ld["_email_verified"] = not is_personal_email(h_email)
                            ld["source"] += "+Hunter"
                            self._log(f"   [Hunter] Email → '{ld.get('name')}': {h_email}")
                        if h_last and lead_first and " " not in (ld.get("name") or ""):
                            candidate = f"{lead_parts[0].capitalize()} {h_last.capitalize()}"
                            # V5.18: Trust Hunter.io name data — only reject obvious title words
                            _bad_last = {"ceo", "cfo", "cto", "coo", "managing", "executive"}
                            if h_last.lower() not in _bad_last and len(h_last) >= 2:
                                ld["name"] = candidate
                                ld["_needs_full_name"] = False
                                self._log(f"   [Hunter] Full name → '{lead_first}' resolved to '{candidate}'")
                        if not ld.get("role") and h_role:
                            ld["role"] = h_role
                        break  # matched to this lead, move to next Hunter contact

        # Step 4e: V5.15 — Hunter.io email-verifier for leads with full name but no email
        # Generates the most likely email candidate and verifies it for free via Hunter.io
        if self.hunter._available:  # V5.18: removed quota gate — Hunter email verification always runs
            for ld in domain_leads:
                if ld.get("email"):
                    continue
                if not ld.get("name") or " " not in ld["name"]:
                    continue
                parts = ld["name"].split()
                candidates = generate_email_candidates(parts[0], parts[-1], domain)
                if not candidates:
                    continue
                try:
                    r = requests.get(
                        f"{self.hunter.BASE_URL}/email-verifier",
                        params={"email": candidates[0], "api_key": API_KEYS.get("hunter", "")},
                        timeout=12,
                    )
                    if r.status_code == 200:
                        result = r.json().get("data", {})
                        if result.get("status") == "valid":
                            ld["email"] = candidates[0]
                            ld["_email_verified"] = True
                            ld["source"] += "+HunterVerify"
                            self._api_counter["hunter"] = self._api_counter.get("hunter", 0) + 1
                            self._log(f"   [Hunter✓] Verified email for {ld.get('name')}: {candidates[0]}")
                except Exception:
                    pass

        # Step 5: Web scraping — always scrape for emails and phones
        scraped = self.scraper.scrape_domain(domain)
        scraped_company = scraped.get("company_name", "")
        scraped_emails = scraped.get("emails", [])
        scraped_phones = scraped.get("phones", [])
        scraped_pairs = scraped.get("name_email_pairs", [])

        if not company_name and scraped_company:
            company_name = scraped_company

        scraped_personal = [e for e in scraped_emails if is_personal_email(e)]
        scraped_generic = [e for e in scraped_emails if not is_personal_email(e)]

        # Step 5b: Try to match scraped emails to specific leads by name
        for ld in domain_leads:
            lead_name = ld.get("name", "")
            if not lead_name or " " not in lead_name:
                continue
            if ld.get("email") and is_personal_email(ld.get("email", "")):
                continue
            parts = lead_name.split()
            first_n = parts[0]
            last_n = parts[-1] if len(parts) > 1 else ""
            matched = False
            for pair in scraped_pairs:
                if match_email_to_name(pair["email"], first_n, last_n):
                    ld["email"] = pair["email"]
                    ld["source"] += "+NameMatch"
                    matched = True
                    break
            if not matched:
                for se in scraped_personal:
                    if match_email_to_name(se, first_n, last_n):
                        ld["email"] = se
                        ld["source"] += "+NameMatch"
                        break

        # Step 5c: V5.2 — Fill missing last names from scraped name-email pairs
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            lead_first = (ld.get("name") or "").split()[0].lower() if ld.get("name") else ""
            if not lead_first:
                continue
            for pair in scraped_pairs:
                scraped_name = pair.get("name", "")
                if (scraped_name and " " in scraped_name and
                        scraped_name.split()[0].lower() == lead_first
                        and _is_valid_person_name(scraped_name)):  # V5.13: name guard
                    ld["name"] = scraped_name
                    ld["_needs_full_name"] = False
                    ld["source"] += "+ScrapeName"
                    break

        # Step 5d: V5.10 — Team/about page name lookup for remaining single-name leads
        # Scrapes /about, /team, /our-team etc. for staff full names — free, no API credits
        single_name_leads = [ld for ld in domain_leads if ld.get("name") and " " not in ld["name"]]
        if single_name_leads:
            team_entries = self.scraper.scrape_team_names(domain)
            if team_entries:
                for ld in single_name_leads:
                    if ld.get("name") and " " in ld["name"]:
                        continue  # already resolved by another step running in parallel
                    first_only = (ld.get("name") or "").split()[0].lower()
                    # V5.13: Use name abbreviation variants (matt → matthew, etc.)
                    first_variants = set(_get_name_variants(first_only))
                    for entry in team_entries:
                        entry_name = entry.get("name", "")
                        if (entry_name and " " in entry_name and
                                entry_name.split()[0].lower() in first_variants
                                and _is_valid_person_name(entry_name)):  # V5.13: name guard
                            ld["name"] = entry_name
                            ld["_needs_full_name"] = False
                            ld["source"] += "+TeamPage"
                            # Grab scraped email if lead still needs one
                            entry_email = entry.get("email", "")
                            if entry_email and is_valid_email(entry_email) and not ld.get("email"):
                                ld["email"] = entry_email
                            break

        # Step 5e: V5.15 — SerpAPI site-search snippet for persistent single-name leads
        # Queries company's own website for first name, parses snippets for "FirstName LastName"
        final_singles = [ld for ld in domain_leads if ld.get("name") and " " not in ld["name"]]
        if final_singles and self.serpapi._available:  # V5.18: removed quota gate — always resolve names
            co_name = company_name or domain_to_company_name(domain)
            for ld in final_singles:
                if ld.get("name") and " " in ld["name"]:
                    continue
                first_only = (ld.get("name") or "").split()[0]
                if not first_only:
                    continue
                try:
                    # Query 1: site-specific search on company domain
                    q = f'site:{domain} "{first_only}"'
                    results = self.serpapi.search_keyword(q, self.config["serpapi_gl"], num=5)
                    # SerpAPI search_keyword returns domains only — use raw API for snippets
                    raw = self.serpapi._raw_search(q, self.config["serpapi_gl"], num=5) if hasattr(self.serpapi, "_raw_search") else {}
                    snippets = []
                    for r in raw.get("organic_results", []):
                        snippets.append(r.get("title", "") + " " + r.get("snippet", ""))
                    # Query 2: LinkedIn-targeted
                    q2 = f'"{first_only}" "{co_name}" site:linkedin.com/in'
                    raw2 = self.serpapi._raw_search(q2, self.config["serpapi_gl"], num=5) if hasattr(self.serpapi, "_raw_search") else {}
                    for r in raw2.get("organic_results", []):
                        snippets.append(r.get("title", "") + " " + r.get("snippet", ""))
                    # Parse snippets for "FirstName LastName" pattern
                    first_cap = first_only.capitalize()
                    pattern = re.compile(
                        r"\b" + re.escape(first_cap) + r"\s+([A-Z][a-z]{2,20})\b"
                    )
                    for text in snippets:
                        m = pattern.search(text)
                        if m:
                            candidate = f"{first_cap} {m.group(1)}"
                            if _is_valid_person_name(candidate):
                                ld["name"] = candidate
                                ld["_needs_full_name"] = False
                                ld["source"] += "+SerpSite"
                                break
                except Exception:
                    pass

        # V5.6 FIX: Assign scraped emails by NAME MATCH only — never round-robin
        # This prevents assigning person A's email to person B
        for ld in domain_leads:
            _sc_current = ld.get("email", "")
            if _sc_current and not is_personal_email(_sc_current):
                _sc_local = _sc_current.split("@")[0].lower()
                if _sc_local not in GENERIC_EMAIL_PREFIXES:
                    continue  # V5.22: Already has a business email — don't overwrite with scraped
            elif _sc_current and is_personal_email(_sc_current):
                continue  # already has a personal email
            lead_name = ld.get("name", "")
            parts = lead_name.split() if lead_name else []
            first_n = parts[0] if parts else ""
            last_n = parts[-1] if len(parts) > 1 else ""
            matched = False
            if first_n:
                for se in scraped_emails:
                    if match_email_to_name(se, first_n, last_n):
                        ld["email"] = se
                        ld["source"] += "+Scrape"
                        matched = True
                        break
            # Only assign a purely generic inbox email if lead has NO email at all
            if not matched and not ld.get("email"):
                for se in scraped_generic:
                    local = se.lower().split("@")[0]
                    if local in GENERIC_EMAIL_PREFIXES:
                        ld["email"] = se
                        ld["source"] += "+Scrape"
                        break
                if not ld.get("email") and ld.get("_generic_email"):
                    ld["email"] = ld["_generic_email"]

        # V5.25: Business email upgrade — when lead has consumer email (gmail/hotmail) and their
        # name appears in it, generate business email from name + company domain.
        # This handles the case where Apollo only reveals consumer emails but the lead actually
        # has a business email (first.last@companydomain) visible on their Apollo profile.
        for ld in domain_leads:
            current_email = ld.get("email", "")
            if not current_email or not is_personal_email(current_email):
                continue  # No email or already has business email — skip
            lead_name = ld.get("name", "")
            if not lead_name or " " not in lead_name:
                continue  # Need full name to generate business email pattern
            parts = lead_name.split()
            first_n = parts[0].lower()
            last_n = parts[-1].lower() if len(parts) > 1 else ""
            local_part = current_email.lower().split("@")[0]
            # Check if ANY of the lead's names appear in the consumer email local part
            name_in_email = False
            for name_word in parts:
                if len(name_word) >= 2 and name_word.lower() in local_part:
                    name_in_email = True
                    break
            if not name_in_email:
                continue  # Name not in email — can't confirm this is truly their personal email
            # Generate business email candidates from name + company domain
            if domain and last_n:
                candidates = [
                    f"{first_n}.{last_n}@{domain}",
                    f"{first_n}@{domain}",
                    f"{first_n}{last_n}@{domain}",
                    f"{first_n[0]}.{last_n}@{domain}",
                ]
                # Use the first.last@domain pattern as the most common business email format
                business_email = candidates[0]
                ld["email"] = business_email
                ld["_email_inferred"] = True
                ld["_email_verified"] = False
                ld["source"] += "+BizEmailGen"
                self._log(f"   V5.25: Business email upgrade for {lead_name}: "
                          f"{current_email} → {business_email}")

        # Phone and company fallback
        # V5.21: Company phone is stored separately — NOT assigned to individual leads.
        # We want personal/mobile phones only. Company HQ number is a last-resort fallback
        # and must never overwrite or prevent personal phone discovery.
        # V5.25: Filter scraped phones to exclude company phone
        _co_digits_for_scrape = re.sub(r'\D', '', company_phone) if company_phone else ""
        _scraped_personal_phones = []
        for sp in scraped_phones:
            sp_digits = re.sub(r'\D', '', sp)
            if _co_digits_for_scrape and sp_digits == _co_digits_for_scrape:
                continue  # Skip company phone from scraped list
            _scraped_personal_phones.append(sp)
        for ld in domain_leads:
            if not ld.get("phone"):
                if _scraped_personal_phones:
                    ld["phone"] = _scraped_personal_phones[0]
                # V5.21: Do NOT assign company_phone here — it's the generic switchboard.
                # Personal phones come from Apollo/Lusha enrich_person (Steps 2c/2d/4b).
            if not ld.get("company"):
                ld["company"] = company_name or domain_to_company_name(domain)

        # Step 6: SerpApi business info fallback (V5.1: skip if ALL leads have phones)
        # V5.25: SerpAPI phone is company-level (Google Knowledge Panel) — mark as non-direct
        needs_phone = any(not ld.get("phone") for ld in domain_leads)
        if needs_phone:
            for ld in domain_leads:
                if not ld.get("phone") and ld.get("company"):
                    info = self.serpapi.search_business_info(ld["company"], self.config["serpapi_gl"])
                    if info.get("phone"):
                        ld["phone"] = info["phone"]
                        ld["_direct_phone"] = False  # V5.25: SerpAPI phone = company level
                        ld["_phone_quality"] = 5  # V5.25: Lowest quality — company number
                        ld["source"] += "+SerpApi"
                    if info.get("email"):
                        serp_email = info["email"]
                        _serp_current = ld.get("email", "")
                        _serp_current_is_business = bool(_serp_current) and not is_personal_email(_serp_current)
                        _serp_current_local = _serp_current.split("@")[0].lower() if _serp_current else ""
                        _serp_current_is_generic = _serp_current_local in GENERIC_EMAIL_PREFIXES
                        # V5.22: Only assign SerpAPI email if no business email exists
                        if not _serp_current or _serp_current_is_generic:
                            ld["email"] = serp_email

        # V5.21: Last-resort company phone fallback — only for leads that got NO phone from any source.
        # This is the generic company number (from org enrichment) and is clearly marked as non-direct.
        # V5.24: Don't re-add a phone we already identified as a shared/company-level number.
        if company_phone:
            _co_digits = re.sub(r'\D', '', company_phone)
            for ld in domain_leads:
                if not ld.get("phone"):
                    _stripped = ld.get("_dedup_stripped_phone", "")
                    if _stripped and re.sub(r'\D', '', _stripped) == _co_digits:
                        continue  # V5.24: company_phone == the shared phone we stripped — skip
                    ld["phone"] = company_phone
                    # NOT marked as _direct_phone — this is the company switchboard

        # Clean up internal fields
        for ld in domain_leads:
            ld.pop("_generic_email", None)

        # Step 6b: V5.9 — Final name resolution via LinkedIn URL only (real data, no fabrication)
        for ld in domain_leads:
            if ld.get("name") and " " in ld["name"]:
                continue  # already has full name
            first_only = ld.get("name", "")
            li_url = ld.get("_linkedin_url", "")
            if first_only and li_url:
                resolved = _extract_name_from_linkedin_url(first_only, li_url)
                if resolved and " " in resolved:
                    ld["name"] = resolved
                    ld["_needs_full_name"] = False
                    ld["source"] += "+LinkedIn"

        # V5.10: Update phone-leads credit gate counter
        # Counts ONLY direct personal phones from Apollo/Lusha enrich_person (not company/scraped phones).
        # Once _phone_leads_count >= max_leads * 1.2, no more enrich_person calls are made.
        if self.max_leads > 0:
            direct_phone_here = sum(
                1 for ld in domain_leads
                if ld.get("_direct_phone") and format_phone(ld.get("phone", ""), self.country) != ""
            )
            if direct_phone_here > 0:
                with self._complete_leads_lock:
                    self._phone_leads_count += direct_phone_here
                self._log(f"   V5.10: +{direct_phone_here} direct phone leads "
                          f"(total: {self._phone_leads_count}/{int(self.max_leads * 1.2)} target)")

        # V5.13: WHOIS founder verification
        if domain_leads:
            self._step_whois_verify(domain, domain_leads)

        # V5.13: Per-domain credit tracking
        domain_credit_cost = sum(
            (self._api_counter.get(svc, 0) - _counter_snapshot.get(svc, 0)) * API_CREDIT_COSTS.get(svc, 0)
            for svc in API_CREDIT_COSTS
        )
        per_lead_cost = domain_credit_cost / max(len(domain_leads), 1)
        for ld in domain_leads:
            ld["_api_credits_used"] = per_lead_cost

        # If Apollo found people, return them
        if domain_leads:
            return domain_leads
        else:
            # Fallback: create a domain-level lead from scraped/org data
            fallback_email = ""
            if scraped_personal:
                fallback_email = scraped_personal[0]
            elif scraped_emails:
                fallback_email = scraped_emails[0]
            fallback = {
                "name": "",
                "domain": domain,
                "company": company_name or domain_to_company_name(domain),
                "role": "",
                "email": fallback_email,
                "phone": company_phone or (scraped_phones[0] if scraped_phones else ""),
                "source": "Org+Scrape",
                "_domain_source": "paid" if domain in self._adwords_domains else ("organic" if domain in self._organic_domains else "paid"),  # V5.13
                "_direct_phone": False,  # V5.12: Scraped phone, not from Apollo search
                "_email_verified": False,  # V5.12: Scraped email, not verified
                "_paid_keywords": traffic_metrics.get("paid_keywords", 0),
                "_organic_keywords": traffic_metrics.get("organic_keywords", 0),
                "_paid_traffic": traffic_metrics.get("paid_traffic", 0),
                "_organic_traffic": traffic_metrics.get("organic_traffic", 0),
                "_api_credits_used": per_lead_cost,
            }
            if fallback["email"] or fallback["phone"]:
                return [fallback]
            return []

    # ── Apollo org search fallback for domain discovery ─────────────────────

    def _apollo_org_discovery_fallback(self) -> set:
        """Use Apollo's organization search as a last-resort domain discovery source.
        Called when both SEMrush and SerpApi return 0 domains.
        Searches Apollo for companies matching the industry in the target country.
        """
        domains = set()
        try:
            url = f"{self.apollo.BASE_URL}/mixed_companies/search"
            # Map country code to Apollo location string
            location_map = {
                "AU": "Australia", "USA": "United States",
                "UK": "United Kingdom", "India": "India",
            }
            location = location_map.get(self.country, self.country)
            # Use first few industry keywords as search tags
            industry_tags = [self.industry.lower().split("/")[0].strip()]
            payload = {
                "q_organization_keyword_tags": industry_tags,
                "organization_locations": [location],
                "per_page": 25,
                "page": 1,
            }
            resp = requests.post(
                url, json=payload,
                headers=self.apollo._headers(), timeout=30
            )
            if resp.status_code == 200:
                self._api_counter["apollo"] = self._api_counter.get("apollo", 0) + 1
                orgs = resp.json().get("organizations", [])
                for org in orgs:
                    d = org.get("primary_domain", "") or extract_domain(org.get("website_url", ""))
                    if d and not is_platform_domain(d):
                        domains.add(d)
                self._log(f"   Apollo org search returned {len(orgs)} orgs → {len(domains)} valid domains")
            else:
                self._log(f"   Apollo org search failed: HTTP {resp.status_code}")
        except Exception as exc:
            self._log(f"   Apollo org search error: {exc}")
        return domains

    def _fetch_retry_domains(self, already_processed: set, max_new: int = 15,
                             round_num: int = 0) -> list:
        """V5.10: Fetch additional paid domains for the retry loop.
        V5.30: Accepts round_num for keyword rotation + competitor expansion from
        existing lead domains. Each round pulls from a different keyword slice so
        successive rounds don't hit the same SEMrush results.
        """
        db = self.config["semrush_db"]
        gl = self.config["serpapi_gl"]
        new_domains = []

        # V5.30: Rotate keyword slice by round. Round 0 pulls 30+, round 1 pulls 50+, etc.
        # Falls back to slicing from the start if we run out.
        total_kws = len(self.keywords)
        slice_start = 30 + (round_num * 20)
        if slice_start >= total_kws:
            # Wrap using different offset to avoid exact repetition
            slice_start = max(0, (round_num * 7) % max(total_kws, 1))
        retry_keywords = self.keywords[slice_start:slice_start + 20] if total_kws else []
        if not retry_keywords:
            retry_keywords = self.keywords[:10]

        for kw in retry_keywords:
            if len(new_domains) >= max_new:
                break
            # 2026-06-12: ALL-advertisers capture (1 unit/row) — same policy as
            # the Phase 3 sweep.
            ad_results = self.semrush.get_adwords_domains(
                kw, db, limit=(15 if self.credit_saver else 30))
            for r in ad_results:
                d = r["domain"]
                if d not in already_processed and d not in new_domains:
                    new_domains.append(d)

        # V5.30: Competitor expansion from current lead domains (most valuable seeds).
        # Pick a rotating window of existing domains as seeds, get their competitors.
        # 2026-05-18: gate the whole block by silent-scope + budget. Each
        # get_domain_competitors call is ~200 units (domain_adwords_adwords,
        # 5 rows × 40/row); 5 seeds × N rediscovery rounds rapidly burns
        # the run budget. For tiny runs (max_leads < 5) skip entirely —
        # primary discovery + Apollo fallback already covers them.
        _topup_silent = getattr(self, "_semrush_silent_scope", False)
        _topup_budget = int(getattr(self.semrush, "_unit_budget", 0) or 0)
        _topup_used = int(getattr(self.semrush, "_units_used", 0) or 0)
        _topup_tight = bool(_topup_budget) and _topup_used >= int(_topup_budget * 0.85)
        _topup_tiny = (self.max_leads or 0) > 0 and self.max_leads < 5
        _skip_topup_comp = _topup_silent or _topup_tight or _topup_tiny
        if len(new_domains) < max_new and not _skip_topup_comp:
            existing = [
                (ld.get("domain") or "").lower() for ld in (self.leads or [])
                if ld.get("domain")
            ]
            existing = list(dict.fromkeys(existing))  # dedup preserving order
            if existing:
                # 2026-05-18: scale seed sample by max_leads so a 5-lead run
                # only pays for 1-2 competitor calls per round, not 5.
                _seed_n = max(1, min(5, (self.max_leads or 5) // 5)) if (self.max_leads or 0) > 0 else 5
                # Rotate window by round
                window_start = (round_num * 3) % len(existing)
                seeds = existing[window_start:window_start + _seed_n]
                if len(seeds) < _seed_n and existing:
                    seeds = (seeds + existing)[:_seed_n]
                for seed in seeds:
                    if len(new_domains) >= max_new:
                        break
                    try:
                        competitors = self.semrush.get_domain_competitors(seed, db, limit=5)
                        for cd in competitors:
                            if (cd not in already_processed and cd not in new_domains
                                    and not is_platform_domain(cd)):
                                new_domains.append(cd)
                            if len(new_domains) >= max_new:
                                break
                    except Exception:
                        continue
        elif len(new_domains) < max_new and _skip_topup_comp:
            self._log(
                f"   _fetch_more_domains: skipping competitor expansion "
                f"(silent={_topup_silent}, tight={_topup_tight}, "
                f"tiny={_topup_tiny}, used={_topup_used}/{_topup_budget})"
            )

        # Supplement with SerpApi ORGANIC search if needed (different query/round).
        # 2026-06-09 WASTE FIX: every domain found here is admitted ONLY if
        # `self.semrush.has_paid_traffic()` confirms it — but in GOOGLE_ONLY /
        # "SerpAPI-only" mode SEMrush is disabled, so that check returns False for
        # ALL of them → this block spent SerpAPI organic searches and added ZERO
        # domains. Skip it entirely when SEMrush can't validate (it's dead weight
        # there); the ads-only sweep (PASS 2a) is the paid source in that mode.
        _semrush_can_validate = not getattr(self.semrush, "_disabled", False)
        if (len(new_domains) < max_new and self.serpapi._available
                and _semrush_can_validate):
            serp_queries = [
                f"best {self.industry} {self.config['location_suffix']}",
                f"top {self.industry} companies {self.config['location_suffix']}",
                f"{self.industry} services near me",
                f"leading {self.industry} providers",
                f"{self.industry} contractors directory",
            ]
            query = serp_queries[round_num % len(serp_queries)]
            serp_domains = self.serpapi.search_keyword(query, gl, num=20)
            for d in serp_domains:
                if d not in already_processed and d not in new_domains:
                    if self.semrush.has_paid_traffic(d, db):
                        new_domains.append(d)
                    if len(new_domains) >= max_new:
                        break

        return new_domains[:max_new]

    def _phase4_enrichment(self):
        """V5.1: Parallel domain enrichment using ThreadPoolExecutor (8 workers).
        V5.10: Retry loop — if phone-leads count < max_leads after first pass, fetch more domains.
        """
        self._set_semrush_phase("phase4_enrich")
        self._progress(46, "Enriching leads (V5.10: phone-targeted credit gate)...")
        self._log("Phase 4: Multi-source lead enrichment (V5.1: ThreadPoolExecutor, 8 workers)")
        self._log("   V5.10: Credits spent only until max_leads phone-bearing leads are found")

        # 2026-06-02: enrich TRUE Search advertisers FIRST. The enrichment-OFF
        # credit gate stops early (~max_leads×4 raw leads), so if confirmed
        # SerpAPI Search advertisers aren't at the front of the list they can
        # be skipped entirely — which is how heavy advertisers never reached
        # the final cut while organic domains did. Sort by advertiser tier
        # (heavy Search → Search → ATC/other → organic) before enriching.
        try:
            self.domains = sorted(
                self.domains,
                key=lambda _d: self._advertiser_tier({"domain": _d}),
                reverse=True,
            )
        except Exception:
            pass

        total = len(self.domains)
        all_domain_leads = []
        completed_count = 0
        BATCH_SIZE = 12  # V5.17: Increased from 8 → 12 workers for faster throughput

        # V5.10: Submit domains in batches so the credit gate can stop between batches.
        # Previously all domains were submitted at once — gate couldn't stop mid-inflight workers.
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            for batch_start in range(0, total, BATCH_SIZE):
                if self._has_enough_leads() or self._cancelled:
                    self._log(f"   Gate reached — stopping domain submission at {batch_start}/{total}")
                    break
                batch = self.domains[batch_start: batch_start + BATCH_SIZE]
                futures = {
                    executor.submit(self._enrich_single_domain, domain, batch_start + i, total): domain
                    for i, domain in enumerate(batch)
                }
                for future in as_completed(futures):
                    if self._cancelled:
                        executor.shutdown(wait=False, cancel_futures=True)
                        return
                    try:
                        result = future.result()
                        if result:
                            all_domain_leads.extend(result)
                    except Exception as e:
                        self._log(f"   ERROR enriching {futures[future]}: {e}")
                    completed_count += 1
                    pct = 46 + int(completed_count / total * 40)
                    self._progress(pct, f"Enriched {completed_count}/{total} domains ({len(all_domain_leads)} leads)")

        # V5.10: Retry loop — if we don't have enough phone-bearing leads, fetch more domains
        if (self.max_leads > 0 and not self._cancelled
                and self._phone_leads_count < self.max_leads):
            deficit = self.max_leads - self._phone_leads_count
            self._log(f"   V5.10: Only {self._phone_leads_count}/{self.max_leads} phone leads found. "
                      f"Retry loop: fetching more domains to fill {deficit} gap...")
            self._progress(86, f"Retry: need {deficit} more phone leads...")

            already_processed = set(self.domains)
            retry_count = min(deficit * 3, 20)  # fetch up to 3x deficit, cap at 20
            retry_domains = self._fetch_retry_domains(already_processed, max_new=retry_count)

            if retry_domains:
                self._log(f"   V5.10 Retry: Processing {len(retry_domains)} additional domains...")
                retry_total = total + len(retry_domains)
                for ri, domain in enumerate(retry_domains):
                    if self._has_enough_leads() or self._cancelled:
                        break
                    try:
                        result = self._enrich_single_domain(domain, total + ri, retry_total)
                        if result:
                            all_domain_leads.extend(result)
                    except Exception as e:
                        self._log(f"   Retry ERROR enriching {domain}: {e}")
                self._log(f"   V5.10 Retry complete: {self._phone_leads_count}/{self.max_leads} phone leads")
            else:
                self._log("   V5.10 Retry: No additional domains found in retry pass.")

        self.leads = all_domain_leads
        self._log(f"   Total raw leads: {len(self.leads)} "
                  f"(phone-bearing: {self._phone_leads_count})")
        self._progress(90, f"{len(self.leads)} raw leads collected")

    # ── V5.13: WHOIS Founder Verification ──────────────────────────────────

    def _step_whois_verify(self, domain: str, domain_leads: list) -> None:
        """V5.13: WHOIS founder verification. Marks verified founder + promotes to front.
        V5.32: If the matched lead has only a first name, UPGRADE it to the WHOIS full name.
        This rescues leads where Apollo returned a locked/first-name-only record but WHOIS
        has the registrant's full name (common for small businesses)."""
        try:
            whois_name = self.whois_client.get_registrant_name(domain)
            if not whois_name:
                return
            self._log(f"   WHOIS: {domain} registrant = {whois_name}")
            match = WhoisFounderClient.find_founder_in_leads(whois_name, domain_leads)
            if match:
                match["_whois_verified"] = True
                match["_founder_verified"] = True
                if not match.get("role") or not any(kw in match["role"].lower() for kw in HARD_DM_KEYWORDS):
                    match["role"] = "Founder (WHOIS Verified)"
                match["source"] += "+WHOIS"
                # V5.32: Upgrade first-name-only lead to full name from WHOIS
                cur_name = (match.get("name") or "").strip()
                if cur_name and " " not in cur_name and " " in whois_name:
                    # WHOIS name has first+last; only upgrade if first names match (already verified by match)
                    _whois_clean = " ".join(whois_name.split())
                    match["name"] = _whois_clean
                    match["_needs_full_name"] = False
                    self._log(f"   WHOIS: Upgraded lead name '{cur_name}' -> '{_whois_clean}'")
                self._log(f"   WHOIS: Matched founder '{match.get('name')}' at {domain}")
        except Exception:
            pass

    # ── Phase 4b: Targeted Completion ──────────────────────────────────────

    def _phase4b_targeted_completion(self):
        """V5.16: Targeted completion pass — fills phone for top N leads outside the credit gate.
        Runs after Phase 4 regardless of _has_enough_leads() state.
        V5.23: Also processes leads with quality=0 phones (company HQ fallback) to find
        personal/direct numbers via SerpAPI person search.
        Priority: (A) Apollo re-enrich via LinkedIn URL, (B) SerpAPI find_business_phone,
        (B2) SerpAPI find_person_phone, (C) contact page scraping.
        """
        if self.max_leads <= 0 or self._cancelled:
            return
        self._progress(88, "V5.16: Targeted completion pass for top leads...")
        self._log("Phase 4b: Targeted completion — filling phone for top-N leads outside credit gate")

        gl = self.config.get("serpapi_gl", "au")
        # V5.23: Include leads with quality=0 (company HQ fallback) — try to find personal phone
        need_phone = [
            ld for ld in self.leads
            if not ld.get("phone") or ld.get("_phone_quality", 0) == 0
        ][:self.max_leads * 2]
        filled = 0

        for ld in need_phone:
            if self._cancelled:
                break
            domain = ld.get("domain", "")
            company = ld.get("company") or domain_to_company_name(domain)
            name = ld.get("name", "")
            _existing_phone = ld.get("phone", "")

            # (A) Apollo enrich retry — only for leads with LinkedIn URL not yet Apollo-enriched
            # V5.32: Also gated by Apollo budget
            if name and ld.get("_linkedin_url") and "apollo" not in ld.get("source", "").lower() and self._apollo_budget_ok():
                try:
                    _parts4b = name.split()
                    enriched = self.apollo.enrich_person(
                        _parts4b[0] if _parts4b else "",
                        _parts4b[-1] if len(_parts4b) > 1 else "",
                        domain,
                        linkedin_url=ld.get("_linkedin_url", ""),
                    )
                    if enriched.get("phone"):
                        ld["phone"] = enriched["phone"]
                        ld["source"] = ld.get("source", "") + "+Apollo4b"
                        with self._complete_leads_lock:
                            self._phone_leads_count += 1
                        filled += 1
                        if enriched.get("email") and not ld.get("email"):
                            ld["email"] = enriched["email"]
                        continue
                except Exception:
                    pass

            # (B) SerpAPI find_business_phone — only for truly empty phone
            if not ld.get("phone") and self.serpapi._available and domain:
                phone = self.serpapi.find_business_phone(domain, company, gl)
                if phone:
                    ld["phone"] = phone
                    ld["source"] = ld.get("source", "") + "+SerpPhone"
                    with self._complete_leads_lock:
                        self._phone_leads_count += 1
                    filled += 1
                    continue

            # (C) Contact page scraping — only for truly empty phone
            if not ld.get("phone") and domain:
                phone = self._scrape_contact_pages(domain)
                if phone:
                    ld["phone"] = phone
                    ld["source"] = ld.get("source", "") + "+ContactPage"
                    with self._complete_leads_lock:
                        self._phone_leads_count += 1
                    filled += 1

            # (D) V5.23: SerpAPI person phone search — for leads with low-quality phone (company HQ)
            # Searches Google for "[person name] [domain/company] phone mobile" to find personal number.
            # Only runs when current phone quality=0 (company fallback) and a full name is known.
            # Only replaces if a DIFFERENT phone is found (avoids redundant same-number replacement).
            if (name and " " in name and self.serpapi._available and domain
                    and ld.get("_phone_quality", 0) < 30):  # V5.24: was == 0; now catches landlines (quality 5-15) too
                person_phone = self.serpapi.find_person_phone(name, domain, company, gl)
                if person_phone:
                    _found_digits = re.sub(r'\D', '', person_phone)
                    _existing_digits = re.sub(r'\D', '', _existing_phone) if _existing_phone else ""
                    if _found_digits != _existing_digits:  # Only update if different number
                        ld["phone"] = person_phone
                        ld["_phone_quality"] = 20   # Unknown type — score as non-HQ
                        ld["_direct_phone"] = False  # Can't confirm it's direct until validated
                        ld["source"] = ld.get("source", "") + "+SerpPersonPhone"
                        filled += 1
                        self._log(f"   [SerpPerson] Found phone for {name}: {person_phone}")

        self._log(f"   Phase 4b: Filled phone for {filled} additional leads "
                  f"(total phone-bearing: {self._phone_leads_count})")

        # V5.17: Phase 4b.5 — fill email for phone-bearing leads that still have no email
        # Uses inferred pattern (first.last@domain) for leads with full name + phone.
        # This ensures the top N leads meet the minimum: name + phone + email.
        email_filled = 0
        for ld in self.leads:
            if not ld.get("phone"):
                continue  # only care about phone-bearing leads
            if ld.get("email"):
                continue  # already has email
            ld_name = ld.get("name", "")
            ld_domain = ld.get("domain", "")
            if not ld_name or " " not in ld_name or not ld_domain:
                continue
            parts = ld_name.split()
            candidates = generate_email_candidates(parts[0], parts[-1], ld_domain)
            if candidates:
                ld["email"] = candidates[0]
                ld["_email_inferred"] = True
                ld["source"] = ld.get("source", "") + "+InferredEmail"
                email_filled += 1
        if email_filled:
            self._log(f"   Phase 4b.5: Inferred email assigned to {email_filled} phone-bearing leads")

        self._progress(90, f"Phase 4b done: {self._phone_leads_count} phone-bearing leads")

        # V5.26: Phase 4c — Async phone collection from Apollo webhook
        # By now, Apollo has had time to process phone reveals submitted during enrichment.
        # Collect phone data from webhook store (self-hosted) or poll webhook.site (relay).
        _wh_url = _get_webhook_url()
        if _wh_url:
            self._log("Phase 4c: Collecting async phone reveals from Apollo...")
            import time as _time

            # V5.27: Multi-round polling with increasing waits.
            # Apollo's webhook delivery is async; some contacts arrive within 5s, others take 15-25s.
            # Poll 3 times (at 5s, 10s, 15s delays) to catch late deliveries (e.g. small companies
            # where Apollo's phone reveal queue is slower).
            _poll_waits = [5, 10, 15]  # seconds between each poll attempt
            for _poll_wait in _poll_waits:
                _time.sleep(_poll_wait)
                # If using webhook.site relay, poll for received data
                if _webhook_site_token:
                    self._log(f"    Polling webhook.site (wait={_poll_wait}s)...")
                    _poll_webhook_site_phones(_webhook_site_token)

            phones_collected = 0
            leads_checked = 0
            for ld in self.leads:
                apollo_id = ld.get("_apollo_id", "")
                if not apollo_id:
                    continue
                # Skip leads that already have a high-quality personal/mobile phone (q >= 30)
                existing_quality = ld.get("_phone_quality", 0)
                if existing_quality >= 30:
                    continue  # Already has a personal/mobile phone — no need to check webhook
                leads_checked += 1
                # Check webhook store for delivered phone data
                webhook_phones = _collect_phone_reveal(apollo_id)
                if webhook_phones:
                    company_phone = ld.get("_company_phone", "")
                    _fake_person = {"phone_numbers": webhook_phones}
                    phone, quality = _pick_best_phone_from_apollo(_fake_person, company_phone)
                    if phone and quality > existing_quality:
                        old_phone = ld.get("phone", "")
                        ld["phone"] = phone
                        ld["_phone_quality"] = quality
                        ld["_direct_phone"] = quality >= 30
                        phones_collected += 1
                        if old_phone:
                            self._log(f"    V5.26 Phone upgrade: {ld.get('name', '?')} {old_phone} -> {phone} (q={existing_quality}->{quality})")
                        else:
                            self._log(f"    V5.26 Phone reveal: {ld.get('name', '?')} -> {phone} (q={quality})")
            self._log(f"    Phase 4c: {phones_collected}/{leads_checked} phones collected from Apollo reveals")

            # V5.27: Post-Phase-4c dedup guard — strip phones shared across 2+ leads from same domain.
            # When Apollo's phone reveal credits are exhausted, the webhook delivers a shared company
            # placeholder (e.g. +61485857016) for multiple contacts. This bypasses the earlier dedup
            # guard (which runs before Phase 4c). Only strip non-personal phones (quality < 30) to
            # avoid incorrectly stripping two people who genuinely share a family/office line.
            import re as _re_dedup
            _4c_domain_phone_counts: dict = {}
            for ld in self.leads:
                p = ld.get("phone", "")
                domain = ld.get("domain", "")
                q = ld.get("_phone_quality", 0)
                if p and domain and q < 30:
                    key = (domain, _re_dedup.sub(r'\D', '', p))
                    _4c_domain_phone_counts[key] = _4c_domain_phone_counts.get(key, 0) + 1
            _4c_stripped = 0
            for ld in self.leads:
                p = ld.get("phone", "")
                domain = ld.get("domain", "")
                q = ld.get("_phone_quality", 0)
                if p and domain and q < 30:
                    key = (domain, _re_dedup.sub(r'\D', '', p))
                    if _4c_domain_phone_counts.get(key, 0) >= 2:
                        self._log(f"    V5.27 Post-4c dedup: {ld.get('name', '?')} stripped shared phone {p} (q={q})")
                        ld["_dedup_stripped_phone"] = p
                        ld["phone"] = ""
                        ld["_phone_quality"] = 0
                        ld["_direct_phone"] = False
                        _4c_stripped += 1
            if _4c_stripped:
                self._log(f"    V5.27 Post-4c dedup: stripped {_4c_stripped} shared phone(s) across leads")

            # Cleanup
            _cleanup_phone_reveals()
            if _webhook_site_token:
                _delete_webhook_site_token(_webhook_site_token)

    # ── Phase 5: Data Cleanup ───────────────────────────────────────────────

    def _phase5_cleanup(self):
        self._progress(91, "Cleaning and deduplicating leads...")
        self._log("Phase 5: Data cleanup")

        cleaned = []
        seen = set()

        for lead in self.leads:
            # Filter .org domains UNLESS lead has email or phone
            if lead.get("domain") and ".org" in lead["domain"].lower():
                if not lead.get("email") and not lead.get("phone"):
                    continue

            # Format and strictly validate phone number
            if lead.get("phone"):
                lead["phone"] = format_phone(lead["phone"], self.country)

            # Clean company name
            if not lead.get("company") or lead["company"] == lead.get("domain", ""):
                lead["company"] = domain_to_company_name(lead.get("domain", ""))

            # V5.13: Clear name if it exactly matches company name (name contamination guard)
            # e.g. "Le Tooth West End" appearing in both Name and Company Name
            if lead.get("name") and lead.get("company"):
                if lead["name"].strip().lower() == lead["company"].strip().lower():
                    lead["name"] = ""

            # Validate email
            if lead.get("email") and not is_valid_email(lead["email"]):
                lead["email"] = ""

            # V5.25: REMOVE leads whose role is purely a practitioner/trade role matching the industry
            # e.g. searching for "dentist" → remove leads with role "Dentist" or "General Dentist"
            # but KEEP "Dentist & Owner" (has DM keyword)
            if lead.get("role"):
                _role_lower = lead["role"].lower().strip()
                _has_dm_keyword = any(kw in _role_lower for kw in HARD_DM_KEYWORDS | SOFT_DM_KEYWORDS)
                _is_trade = any(trade in _role_lower for trade in TRADE_ROLE_WORDS)
                _is_non_dm = any(kw in _role_lower for kw in NON_DECISION_MAKER_KEYWORDS)
                # V5.25: Check if role matches the searched industry (pure practitioner)
                _industry_lower = self.industry.lower().strip()
                _industry_words = set(_industry_lower.replace("/", " ").replace("-", " ").split())
                _role_words_set = set(_role_lower.replace("/", " ").replace("-", " ").split())
                # Role is pure industry match if ALL significant role words are industry words
                # e.g. "dentist" matches industry "dentist", "general dentist" matches too
                _generic_prefixes = {"general", "senior", "junior", "lead", "head", "chief", "principal"}
                _role_significant = _role_words_set - _generic_prefixes - {"&", "and", "of", "the", "a"}
                _is_pure_industry = bool(_role_significant) and _role_significant.issubset(
                    _industry_words | {w + "s" for w in _industry_words} |
                    {w.rstrip("s") for w in _industry_words} |
                    TRADE_ROLE_WORDS
                )
                if not _has_dm_keyword and _is_pure_industry:
                    lead["role"] = ""  # V5.29: blank role but keep lead — practitioner could be business owner
                if not _has_dm_keyword and (_is_trade or _is_non_dm):
                    lead["role"] = ""

            # Keep ANY lead that has email or phone, regardless of other fields
            # Only skip if there is no email AND no phone AND no name
            if not lead.get("email") and not lead.get("phone") and not lead.get("name"):
                continue

            # Deduplicate
            dedup_key = ""
            if lead.get("name") and lead.get("domain"):
                dedup_key = f"{lead['name'].lower()}|{lead['domain'].lower()}"
            elif lead.get("email"):
                dedup_key = lead["email"].lower()
            else:
                dedup_key = f"{lead.get('phone', '')}|{lead.get('domain', '')}"

            if dedup_key and dedup_key in seen:
                continue
            if dedup_key:
                seen.add(dedup_key)

            cleaned.append(lead)

        self.leads = cleaned

        # V5.17: Removed secondary scraped-phone dedup — it was DROPPING leads sharing the same
        # company phone (common when Apollo org returns one number for all staff).
        # The primary dedup above (name|domain, email, phone|domain) is sufficient.
        # All leads with phones are kept — shared company numbers are acceptable.

        # PHASE 2: Full-name policy — every lead in the FINAL CSV must carry
        # a two-word name (matches the Apollo people-page surface).
        # PHASE 2 FIX (2026-04-28): Only drop single-name leads when enrichment
        # is ON. With enrichment OFF, Apollo's basic people-search returns
        # obfuscated names ("Matt M." → "Matt") that the free SerpAPI/Apollo
        # search layers can't always upgrade. Dropping these would yield 0 leads
        # (last enrichment-OFF run: 125 → 0). Master-DB upsert (run with
        # enrichment ON later) will fill in full names for these records.
        if self.enrichment_enabled:
            before_drop = len(self.leads)
            self.leads = [
                ld for ld in self.leads
                if ld.get("name") and " " in ld["name"].strip()
            ]
            dropped_single = before_drop - len(self.leads)
            if dropped_single > 0:
                self._log(f"   Dropped {dropped_single} single-name leads (full-name policy).")
        else:
            single = sum(1 for ld in self.leads
                         if ld.get("name") and " " not in ld["name"].strip())
            if single:
                self._log(
                    f"   Enrichment OFF: keeping {single} single-name lead(s) — "
                    "future enrichment-ON runs will upgrade names via master-DB upsert."
                )

        # PHASE 2: Belt-and-braces SEMrush quality gate — drop any lead whose
        # domain has paid_traffic < 1. _enrich_single_domain already filters
        # at fetch time; this catches anything from secondary paths.
        #
        # 2026-05-27 (CRITICAL FIX): this gate was previously naive — it
        # killed legitimate Google-Intent / Apollo-only / SEMrush-silent
        # leads even when the discovery layer had ALREADY stamped them with
        # bypass flags. A real Sydney 100-lead run produced 4020 raw leads
        # (3922 phone-bearing), all dropped here because the AU scope was
        # SEMrush-silent → 0 paid_traffic across the board. The same logic
        # that exists in _enrich_single_domain (lines ~6088-6125) must
        # mirror here: paid_traffic >= 1 is sufficient OR domain came from
        # a non-SEMrush source OR SEMrush is broken/silent for this scope OR
        # enrichment is OFF (the user explicitly opted out of reveal — the
        # whole point of the OFF mode is to skip Apollo and accept a wider
        # candidate set).
        before_metrics = len(self.leads)
        _scope_semrush_unavailable = (
            not bool(API_KEYS.get("semrush"))
            or bool(getattr(self, "_semrush_silent_scope", False))
            or bool(self._api_counter.get("semrush_unavailable", False))
            or getattr(self, "_discovery_mode", None) == "GOOGLE_ONLY"
        )
        def _meets_metrics(ld):
            try:
                pt = int(float(ld.get("_paid_traffic", 0) or 0))
            except (TypeError, ValueError):
                pt = 0
            if pt >= 1:
                return True
            # Enrichment OFF — the user opted out of expensive reveal.
            # Phase 4 already accepted these leads on broader gates; the
            # strict paid-traffic cut here would void the whole OFF mode.
            if not self.enrichment_enabled:
                return True
            # Lead-level provenance flags (stamped by _enrich_single_domain).
            if ld.get("_google_intent") or ld.get("_silent_scope_apollo"):
                return True
            # Stub leads carry their own provenance — they exist precisely
            # because Apollo had no people; keeping them is the point.
            if ld.get("_stub_lead"):
                return True
            # Domain-pool membership: any non-SEMrush source-of-truth.
            _d = (ld.get("domain") or "").lower()
            if _d:
                if (_d in (self._google_intent_domains or set())
                        or _d in (self._apollo_only_domains or set())
                        or _d in (self._confirmed_paid_domains or set())):
                    return True
            # Scope-wide SEMrush blindness — paid_traffic is uninformative
            # for the whole run, gate is structurally meaningless.
            if _scope_semrush_unavailable:
                return True
            return False
        kept = []
        bypass_reasons = {"enrich_off": 0, "google_intent": 0, "apollo_only": 0,
                          "confirmed_paid": 0, "silent_scope": 0, "stub": 0, "paid_ok": 0}
        for ld in self.leads:
            try:
                pt = int(float(ld.get("_paid_traffic", 0) or 0))
            except (TypeError, ValueError):
                pt = 0
            if _meets_metrics(ld):
                kept.append(ld)
                # Telemetry — categorize which bypass admitted the lead.
                if pt >= 1:
                    bypass_reasons["paid_ok"] += 1
                elif not self.enrichment_enabled:
                    bypass_reasons["enrich_off"] += 1
                elif ld.get("_google_intent"):
                    bypass_reasons["google_intent"] += 1
                elif ld.get("_silent_scope_apollo"):
                    bypass_reasons["apollo_only"] += 1
                elif ld.get("_stub_lead"):
                    bypass_reasons["stub"] += 1
                else:
                    _d = (ld.get("domain") or "").lower()
                    if _d in (self._confirmed_paid_domains or set()):
                        bypass_reasons["confirmed_paid"] += 1
                    elif _d in (self._google_intent_domains or set()):
                        bypass_reasons["google_intent"] += 1
                    elif _d in (self._apollo_only_domains or set()):
                        bypass_reasons["apollo_only"] += 1
                    elif _scope_semrush_unavailable:
                        bypass_reasons["silent_scope"] += 1
        self.leads = kept
        dropped_metrics = before_metrics - len(self.leads)
        if dropped_metrics > 0:
            self._log(
                f"   Dropped {dropped_metrics} leads failing SEMrush "
                f"paid-traffic gate (paid_traffic>=1)."
            )
        # Always emit the bypass breakdown so the user can see WHY leads
        # survived even with 0 paid_traffic on silent AU SMB scopes.
        _b = bypass_reasons
        if sum(_b.values()) > 0:
            self._log(
                f"   Paid-traffic gate admitted {sum(_b.values())} leads: "
                f"paid_ok={_b['paid_ok']}, enrich_off={_b['enrich_off']}, "
                f"google_intent={_b['google_intent']}, apollo_only={_b['apollo_only']}, "
                f"confirmed_paid={_b['confirmed_paid']}, silent_scope={_b['silent_scope']}, "
                f"stub={_b['stub']}"
            )

        self._log(f"   Final leads after cleanup: {len(self.leads)}")
        self._progress(95, f"{len(self.leads)} leads cleaned")

    # ── Phase 5b: OpenAI Email Verification ────────────────────────────────

    def _phase5b_openai_verify(self):
        if not self.leads:
            return
        self._progress(95, "Verifying emails with OpenAI...")
        self._log("Phase 5b: OpenAI email verification")

        # Process in batches of 20
        batch_size = 20
        total = len(self.leads)
        verified = 0
        for start in range(0, total, batch_size):
            if self._cancelled:
                return
            batch = self.leads[start:start + batch_size]
            self.openai_verifier.verify_leads_batch(batch)
            verified += len(batch)
            self._log(f"   Verified {verified}/{total} emails")

        personal_count = sum(1 for ld in self.leads if ld.get("_email_type") == "Personal")
        generic_count = sum(1 for ld in self.leads if ld.get("_email_type") == "Generic")
        self._log(f"   Email types: {personal_count} personal, {generic_count} generic")
        self._progress(96, f"Email verification complete")

    # ── Phase 5c: SEMrush + LLM per-domain insights (V5.27) ────────────────

    def _phase5c_semrush_insights(self):
        """V5.27: Per-domain SEMrush + LLM enrichment.

        For every unique domain in self.leads:
          1. Fetch organic-ranking keywords (position 4+, commercial intent preferred)
          2. Use LLM to pick 1-2 highly relevant high-ticket service keywords
          3. Look up volume for each picked keyword (via keyword overview)
          4. Pull organic competitors (top 2)
          5. Use LLM to label business niche for this lead's company
          6. Use LLM to label business niche for each competitor

        Caches everything by domain so multiple leads at the same company share
        one API spend. Writes results onto each lead as private `_kw1_*`, `_kw2_*`,
        `_competitor_1`, `_competitor_2`, `_business_niche`, etc.
        """
        if not self.leads:
            return
        self._set_semrush_phase("phase5c_insights")
        # 2026-05-18: Phase 5c is the single most expensive per-lead SEMrush
        # phase. For each unique domain it makes 3 separate calls:
        #   get_domain_organic_keywords(limit=40)  → ~400 units
        #   get_organic_competitors(limit=5)       → ~200 units
        #   get_domain_overview_metrics()          → ~40 units
        # That's ~640 units per kept-lead-domain. For a 3-lead run that is
        # the entire budget right there, so we gate it three ways:
        #   1. Silent scope → skip entirely (no signal worth paying for).
        #   2. Budget already past 60 % when Phase 5c starts → skip.
        #   3. Sample limited to top max_leads*2 domains so a top-up burst
        #      doesn't blow the budget on 100+ domains during a 5-lead run.
        if getattr(self, "_semrush_silent_scope", False):
            self._log("Phase 5c: silent scope — skipping per-domain SEMrush insights")
            return
        _budget = int(getattr(self.semrush, "_unit_budget", 0) or 0)
        _used = int(getattr(self.semrush, "_units_used", 0) or 0)
        if _budget and _used >= int(_budget * 0.60):
            self._log(
                f"Phase 5c: budget {_used}/{_budget} ≥60% — skipping per-domain "
                f"insights to preserve credits for Phase 5h backfill"
            )
            return
        self._progress(96, "Phase 5c: SEMrush keyword + competitor insights...")
        self._log("Phase 5c: SEMrush + LLM per-domain enrichment")

        db = self.config["semrush_db"]

        # Collect unique domains
        unique_domains = []
        _seen_dom = set()
        for ld in self.leads:
            d = (ld.get("domain") or "").strip().lower()
            if d and d not in _seen_dom:
                _seen_dom.add(d)
                unique_domains.append(d)
        # 2026-05-18 (round 3): cap tightened to min(max_leads, 50) domains.
        # Previous cap was max_leads * 2 which for a 250-lead run had us
        # running Phase 5c on 500 domains × ~320 units = 160k units — way
        # over budget. Each lead only needs ONE Phase 5c spend (its own
        # domain), and even at 50 leads the user's CSV doesn't gain enough
        # from polishing the 51st-onwards lead's kw picks to be worth more
        # SEMrush credit. Beyond 50, the marginal value drops below the
        # marginal cost.
        _mx = int(self.max_leads or 0)
        _cap_5c = max(min(_mx, 50), 5) if _mx > 0 else 50
        if len(unique_domains) > _cap_5c:
            self._log(
                f"   Phase 5c: trimming {len(unique_domains)} -> {_cap_5c} "
                f"domains (max_leads={_mx}, cap = min(max_leads, 50))"
            )
            unique_domains = unique_domains[:_cap_5c]

        # Domain cache: keyword picks + competitors + niches
        domain_cache = {}
        # Competitor niche cache (competitor domain → niche) — shared across domains
        competitor_niche_cache = {}

        # ── Helpers ──
        # SEMrush intent codes: per SEMrush docs, intent field "In" returns one or
        # more single-digit codes (0..3). We treat 0 (commercial) and 3 (transactional)
        # — and any intent containing "commercial" as plain text — as commercial.
        def _is_commercial(intent_raw) -> bool:
            s = str(intent_raw or "").strip().lower()
            if not s:
                return False
            if "commercial" in s or "transactional" in s:
                return True
            # Numeric codes: "0" or "3" (commercial or transactional)
            for ch in s:
                if ch in ("0", "3"):
                    return True
            return False

        # 2026-05-18 (round 3): cap at 15 rows. Each row bills 10 units, so
        # 15 rows = 150 units per domain. The LLM picker takes the top 2
        # post-filter so 15 candidates is plenty — going to 20+ was burning
        # 50-100 extra units per domain to surface 1-2 borderline picks.
        _organic_kw_limit = 15 if _mx <= 0 or _mx >= 15 else max(10, min(15, _mx * 3))
        for idx, domain in enumerate(unique_domains, start=1):
            if self._cancelled:
                return
            try:
                # 1. Fetch organic keywords (cached helper)
                kws = self._cached_organic_keywords(domain, db, limit=_organic_kw_limit)

                # Filter: position 4+ (outside top 3), volume in [4, 10_000],
                # commercial intent preferred. If SEMrush doesn't return intent,
                # we still accept — it's just a soft filter.
                filtered = []
                for kw in kws:
                    pos = kw.get("position", 0)
                    vol = kw.get("volume", 0)
                    if pos < 4:  # already ranking top 3 — skip
                        continue
                    if vol < 4 or vol > 10000:
                        continue
                    # Prefer commercial if intent present
                    if kw.get("intent"):
                        if not _is_commercial(kw.get("intent")):
                            continue
                    filtered.append(kw)

                # Fallback: if filter is too strict, relax the intent requirement
                if not filtered:
                    for kw in kws:
                        pos = kw.get("position", 0)
                        vol = kw.get("volume", 0)
                        if pos >= 4 and 4 <= vol <= 10000:
                            filtered.append(kw)

                # 2. Use LLM to pick 1-2 keywords
                company_for_domain = ""
                for ld in self.leads:
                    if (ld.get("domain") or "").lower() == domain and ld.get("company"):
                        company_for_domain = ld["company"]
                        break

                picks = []
                if filtered:
                    picks = self.openai_verifier.select_top_service_keywords(
                        filtered, self.industry, company_for_domain, domain
                    )
                    # Fallback: if LLM returns nothing, take top 2 by volume
                    if not picks:
                        picks = sorted(
                            filtered, key=lambda k: k.get("volume", 0), reverse=True
                        )[:2]

                # 3. For each pick, top up volume via keyword overview if missing/zero
                for kw in picks:
                    if not kw.get("volume"):
                        ov = self.semrush.get_keyword_overview(kw.get("keyword", ""), db)
                        if ov.get("volume"):
                            kw["volume"] = ov["volume"]

                # 4. Organic competitors (top 2) — cached helper. We only
                # NEED the top 2 so limit=3 is sufficient (+ buffer for an
                # is_platform_domain filter loss).
                competitors = self._cached_organic_competitors(domain, db, limit=3)
                # Dedupe and trim
                competitors = [c for c in competitors if c][:2]

                # 5. Business niche for this company
                niche = self.openai_verifier.get_business_niche(
                    company_for_domain or domain_to_company_name(domain),
                    domain,
                    self.industry,
                )

                # 6. Competitor niches (use cache)
                competitor_niches = []
                for c_dom in competitors:
                    if c_dom in competitor_niche_cache:
                        competitor_niches.append(competitor_niche_cache[c_dom])
                    else:
                        c_name = domain_to_company_name(c_dom)
                        c_niche = self.openai_verifier.get_business_niche(
                            c_name, c_dom, self.industry
                        )
                        competitor_niche_cache[c_dom] = c_niche
                        competitor_niches.append(c_niche)

                # 7. V5.28: True domain-wide overview metrics. Cached from
                # Phase 4 so this is a no-op for domains we already enriched
                # (saves ~40 units per kept-lead-domain that came through
                # the standard pipeline).
                overview = self._cached_overview(domain, db)

                domain_cache[domain] = {
                    "picks": picks,
                    "competitors": competitors,
                    "competitor_niches": competitor_niches,
                    "niche": niche,
                    "overview": overview,
                }

                self._log(
                    f"   [{idx}/{len(unique_domains)}] {domain}: "
                    f"{len(picks)} kw picks, {len(competitors)} competitors, "
                    f"niche='{niche or 'NA'}'"
                )
            except Exception as exc:
                self._log(f"   [{idx}/{len(unique_domains)}] {domain}: Phase 5c error: {exc}")
                domain_cache[domain] = {
                    "picks": [], "competitors": [], "competitor_niches": [], "niche": "",
                    "overview": {},
                }

        # Write cache onto each lead
        for ld in self.leads:
            d = (ld.get("domain") or "").strip().lower()
            entry = domain_cache.get(d, {})
            picks = entry.get("picks", [])
            competitors = entry.get("competitors", [])
            comp_niches = entry.get("competitor_niches", [])
            # Keyword 1
            if len(picks) >= 1:
                k = picks[0]
                ld["_kw1_keyword"] = k.get("keyword", "")
                ld["_kw1_volume"] = k.get("volume", "")
                ld["_kw1_position"] = k.get("position", "")
                ld["_kw1_url"] = k.get("url", "")
            # Keyword 2
            if len(picks) >= 2:
                k = picks[1]
                ld["_kw2_keyword"] = k.get("keyword", "")
                ld["_kw2_volume"] = k.get("volume", "")
                ld["_kw2_position"] = k.get("position", "")
                ld["_kw2_url"] = k.get("url", "")
            # Competitors
            if len(competitors) >= 1:
                ld["_competitor_1"] = competitors[0]
                if len(comp_niches) >= 1:
                    ld["_competitor_1_niche"] = comp_niches[0]
            if len(competitors) >= 2:
                ld["_competitor_2"] = competitors[1]
                if len(comp_niches) >= 2:
                    ld["_competitor_2_niche"] = comp_niches[1]
            # Niche for this lead's company
            ld["_business_niche"] = entry.get("niche", "") or ""
            # V5.28: Override traffic/keyword totals with true domain-wide values
            ov = entry.get("overview") or {}
            if ov:
                ld["_organic_traffic"] = ov.get("organic_traffic", ld.get("_organic_traffic", 0))
                ld["_paid_traffic"] = ov.get("paid_traffic", ld.get("_paid_traffic", 0))
                ld["_organic_keywords"] = ov.get("organic_keywords", ld.get("_organic_keywords", 0))
                ld["_paid_keywords"] = ov.get("paid_keywords", ld.get("_paid_keywords", 0))

    # ── Phase 5d: Role recovery via SerpAPI/LinkedIn (V5.28) ───────────────

    def _phase5d_role_recovery(self):
        """V5.28: For leads with name+domain but missing role, try a 2-step recovery:
        1) Apollo people/match enrich (uses apollo_id if available, else name+domain).
        2) SerpAPI LinkedIn lookup parsing the title segment of the result snippet.
        Only fills empty roles — never overwrites an existing role.
        """
        if not self.leads:
            return
        targets = [
            ld for ld in self.leads
            if (ld.get("name") and " " in (ld.get("name") or "").strip()
                and not (ld.get("role") or "").strip())
        ]
        if not targets:
            self._log("Phase 5d: no leads need role recovery — skipping")
            return

        self._progress(96, f"Phase 5d: recovering {len(targets)} missing role(s)...")
        self._log(f"Phase 5d: Role recovery for {len(targets)} lead(s)")

        role_cache = {}
        recovered_apollo = 0
        recovered_serp = 0

        for ld in targets:
            if self._cancelled:
                return
            name = (ld.get("name") or "").strip()
            company = (ld.get("company") or "").strip()
            domain = (ld.get("domain") or "").strip().lower()
            cache_key = (name.lower(), domain)

            if cache_key in role_cache:
                cached = role_cache[cache_key]
                if cached:
                    ld["role"] = cached
                continue

            role = ""
            source_tag = ""

            # 1) Apollo people/match (cheap and accurate when person exists)
            # V5.32: gated by Apollo budget
            if not self._apollo_budget_ok():
                continue
            try:
                parts = name.split()
                first = parts[0] if parts else ""
                last = " ".join(parts[1:]) if len(parts) > 1 else ""
                apollo_id = ld.get("_apollo_id", "") or ""
                enriched = self.apollo.enrich_person(
                    first, last, domain,
                    organization_name=company,
                    apollo_id=apollo_id,
                ) or {}
                role = (enriched.get("role") or "").strip()
                if role:
                    source_tag = "ApolloRole"
            except Exception:
                role = ""

            # 2) SerpAPI LinkedIn lookup — only when enrichment is ON. With
            # enrichment OFF the run is meant to skip credit-spending recovery
            # passes (the Apollo role above is the free fallback); this stops
            # SerpAPI being spent on roles during credit-saving/enrichment-OFF runs.
            if (not role) and self.enrichment_enabled and getattr(self, "serpapi", None) and self.serpapi._available:
                try:
                    role = self.serpapi.find_person_role(name, company, domain) or ""
                    if role:
                        source_tag = "SerpRole"
                except Exception:
                    role = ""

            role_cache[cache_key] = role
            if role:
                ld["role"] = role
                src = ld.get("source", "") or ""
                if source_tag and source_tag not in src:
                    ld["source"] = f"{src}+{source_tag}" if src else source_tag
                if source_tag == "ApolloRole":
                    recovered_apollo += 1
                else:
                    recovered_serp += 1
        self._log(
            f"Phase 5d: recovered {recovered_apollo + recovered_serp}/{len(targets)} "
            f"role(s) (Apollo: {recovered_apollo}, SerpAPI: {recovered_serp})"
        )

    # ── Phase 5e: Drop leads missing both name AND role (V5.28) ─────────────

    def _phase5e_drop_blank_identity(self):
        """V5.28: Per spec — if a lead has no name AND no role after all enrichment
        attempts, omit it from the output entirely. Leads with one of the two
        present (but not the other) are retained as-is."""
        if not self.leads:
            return
        before = len(self.leads)
        kept = [
            ld for ld in self.leads
            if (ld.get("name") or "").strip() or (ld.get("role") or "").strip()
        ]
        dropped = before - len(kept)
        if dropped:
            self._log(f"Phase 5e: dropped {dropped} lead(s) missing both name and role")
        self.leads = kept

    # ── Phase 5f: DM Filter + Per-Company Cap + Quota Top-Up ────────────────

    def _phase5f_dm_cap_and_topup(self):
        """V5.29: Decision-maker filter + per-company cap + quota top-up loop.

        Pipeline:
        1. Compute DM priority for every lead (rule-based, with substring matching
           against DM_PRIORITY_ORDER and NEGATIVE_DM_ROLE_PATTERNS).
        2. For ambiguous leads (priority 0 but has name+role), batch-verify with
           OpenAI to recover miscategorized titles.
        3. Drop all leads with priority 0 (genuine non-DMs).
        4. Group surviving leads by domain, sort each group by DM priority,
           keep top 2 per domain. Hold the 3rd in reserve.
        5. If overall total < max_leads, pull from the 3rd-lead reserves
           (sorted by DM priority) until quota met.
        6. If still short of max_leads, run top-up loop: fetch more competitor
           domains, enrich them, re-apply cleanup + DM filter + cap. Up to
           3 rounds.
        7. Companies with zero DM contacts are dropped entirely.
        """
        if not self.leads and not (self.max_leads and self.max_leads > 0):
            return
        self._progress(96, "Phase 5f: DM filter + per-company cap...")
        self._log("Phase 5f: Decision-maker filter + per-company cap (max 2/company)")

        self._apply_dm_filter_and_cap()
        if self._cancelled:
            return

        if self.max_leads and self.max_leads > 0 and len(self.leads) < self.max_leads:
            self._topup_leads_to_quota()

        self._log(
            f"   Phase 5f complete: {len(self.leads)} leads "
            f"(target {self.max_leads or 'unlimited'})"
        )

    def _mark_quota_fill(self, lead: dict, tier: str, reason: str) -> dict:
        """Annotate a reserve row that was kept only to satisfy exact quota."""
        lead["_quota_fill_tier"] = tier
        lead["_quota_fill_reason"] = reason
        return lead

    def _traffic_source_label(self, lead: dict) -> str:
        """2026-06-01: HONEST traffic-source label for the CSV.

        The old logic stamped `_domain_source="paid"` as a catch-all default
        (the `else "paid"` branch), so Apollo- and Places-discovered domains
        with ZERO paid verification were printed as "Paid" — which made the
        user expect SEMrush/Ahrefs paid traffic that was never there. This
        labels each lead by its ACTUAL verification provenance so the CSV
        never overstates a domain's paid status.

        Order = strongest paid evidence first:
          • "Google Ads"          — live advertiser confirmed by SerpAPI ads[]
                                     or the Ads Transparency Center (ground truth).
          • "Paid (SEMrush)"       — SEMrush-confirmed advertiser (adwords index
                                     or paid_traffic>=1). Shows in SEMrush/Ahrefs.
          • "Google Intent (unverified)" — Google Places business, NOT ad-verified.
          • "Apollo (unverified)"  — Apollo org fallback, NOT ad-verified.
          • "Organic"              — organic-search discovered.
          • "Unverified"           — provenance unknown; never claim paid.
        """
        d = (lead.get("domain") or "").strip().lower()
        # 2026-06-02: distinguish TRUE Search advertisers (SerpAPI live ads[]
        # block — what Ahrefs/SEMrush measure) from ATC-only advertisers
        # (Display/YouTube/historical, often 0 Ahrefs SEARCH paid). This is
        # why fixatap/pjcplumbing (ATC/organic) showed "Google Ads" but read 0
        # in the user's Ahrefs.
        if d and d in self._serp_ads_domains:
            return "Google Ads (Search)" if d in self._heavy_advertiser_domains else "Google Ads"
        if d and d in getattr(self, "_atc_only_domains", set()):
            return "Advertiser (ATC — may be Display/historical)"
        # 2026-06-01: did SEMrush ACTUALLY run this scope? If not, we must
        # never print "Paid (SEMrush)" — confirmed-paid domains in a
        # SEMrush-dead run were confirmed by SerpAPI ads[]/ATC, so they are
        # "Google Ads", and the `paid_traffic=1` value is a synthetic sentinel
        # (set at V5.py ~6262 for confirmed-paid domains to skip the SEMrush
        # call), NOT a real SEMrush metric. Trusting it printed a false
        # "Paid (SEMrush)" on Apollo/SerpAPI domains the user then found at 0
        # paid traffic in SEMrush.
        _semrush_ran = (
            bool(API_KEYS.get("semrush"))
            and not bool(self._api_counter.get("semrush_unavailable", False))
            and not bool(getattr(self, "_semrush_silent_scope", False))
        )
        try:
            _pt = int(float(lead.get("_paid_traffic", 0) or 0))
        except (TypeError, ValueError):
            _pt = 0
        if d and (d in self._confirmed_paid_domains or d in self._adwords_domains):
            # Confirmed advertiser. Attribute to the source that COULD have
            # confirmed it: SEMrush only if it actually ran; else SerpAPI/ATC.
            return "Paid (SEMrush)" if _semrush_ran else "Google Ads"
        if _pt >= 1 and _semrush_ran:
            return "Paid (SEMrush)"
        if lead.get("_google_intent") or (d and d in self._google_intent_domains):
            return "Google Intent (unverified)"
        if lead.get("_silent_scope_apollo") or (d and d in self._apollo_only_domains):
            return "Apollo (unverified)"
        if d and d in self._organic_domains:
            return "Organic"
        return "Unverified"

    def _advertiser_tier(self, lead: dict) -> int:
        """2026-06-02: rank a lead by how reliably it shows SEMrush/Ahrefs
        SEARCH paid traffic. Higher = stronger paid-Search evidence.
          3 = HEAVY SerpAPI Search advertiser (ads[] on >=N queries) — top bet
          2 = SerpAPI Search advertiser (in ads[] once) — shows in Ahrefs paid
          1 = ATC-only / other confirmed-paid (Display/historical — may be 0)
          0 = organic / unverified
        Phase 5f uses this so the final quota is filled with TRUE Search
        advertisers first — fixing the run where ATC-only/organic domains
        (fixatap, pjcplumbing) took slots from heavy Search advertisers."""
        d = (lead.get("domain") or "").strip().lower()
        if not d:
            return 0
        if d in self._serp_ads_domains:
            return 3 if d in self._heavy_advertiser_domains else 2
        if d in getattr(self, "_atc_only_domains", set()) or d in self._confirmed_paid_domains:
            return 1
        try:
            if int(float(lead.get("_paid_traffic", 0) or 0)) >= 1:
                return 1
        except (TypeError, ValueError):
            pass
        return 0

    def _is_paid_quota_candidate(self, lead: dict) -> bool:
        domain = (lead.get("domain") or "").strip().lower()
        if lead.get("_domain_source") == "paid" or domain in self._confirmed_paid_domains:
            return True
        try:
            return float(lead.get("_paid_traffic") or 0) >= 1
        except (TypeError, ValueError):
            return False

    def _apply_dm_filter_and_cap(self):
        """V5.29: Single pass; in guarantee mode, keep strict rows first then reserves."""
        if not self.leads:
            return

        before = len(self.leads)

        for lead in self.leads:
            lead["_dm_priority"] = _calculate_dm_priority(lead.get("role", "") or "")

        # OpenAI rescue pass for genuinely ambiguous roles only.
        # Excludes negative-pattern matches (Sales Manager, Assistant, etc.) —
        # those are confidently non-DMs and shouldn't be re-judged by the LLM.
        ambiguous = [
            ld for ld in self.leads
            if ld.get("_dm_priority", 0) == 0
            and (ld.get("role") or "").strip()
            and (ld.get("name") or "").strip()
            and not _matches_negative_dm_pattern(ld.get("role", ""))
        ]
        if ambiguous and getattr(self.openai_verifier, "_available", False):
            try:
                verdicts = self.openai_verifier.verify_dm_batch(
                    ambiguous[:50], self.industry
                )
                upgraded = 0
                for ld in ambiguous[:50]:
                    key = f"{ld.get('name','')}|{ld.get('role','')}"
                    if verdicts.get(key):
                        ld["_dm_priority"] = 60
                        ld["_llm_dm_verified"] = True
                        upgraded += 1
                if upgraded:
                    self._log(f"   Phase 5f: OpenAI upgraded {upgraded} ambiguous leads to DM")
            except Exception as e:
                self._log(f"   Phase 5f: OpenAI DM check error: {e}")

        dm_leads = [ld for ld in self.leads if ld.get("_dm_priority", 0) > 0]
        non_dm_leads = [ld for ld in self.leads if ld.get("_dm_priority", 0) <= 0]
        dropped_non_dm = before - len(dm_leads)

        # Group by domain (lowercased), sort each group by DM priority desc
        by_domain: dict = {}
        for ld in dm_leads:
            d = (ld.get("domain") or "").lower().strip()
            by_domain.setdefault(d, []).append(ld)

        # With enrichment-OFF, roles aren't verified by Lusha/Apollo enrichment,
        # so the DM filter has higher false-negative rate. Use a wider company cap
        # (3 instead of 2) to ensure the quota is reachable with fewer unique companies.
        per_domain_cap = 2 if self.enrichment_enabled else 3
        kept: list = []
        overflow: list = []
        for d, group in by_domain.items():
            # 2026-06-02: within a company, keep the best decision-makers.
            group.sort(key=lambda x: x.get("_dm_priority", 0), reverse=True)
            strict_slice = group[:per_domain_cap]
            for ld in strict_slice:
                ld.pop("_quota_fill_tier", None)
                ld.pop("_quota_fill_reason", None)
            kept.extend(strict_slice)
            overflow.extend(group[per_domain_cap:])
        # 2026-06-02 (PAID-FIRST SELECTION FIX): order `kept` by advertiser
        # tier BEFORE the quota slice, so the final N are TRUE Search
        # advertisers (SerpAPI ads[], heaviest first) — not ATC-only/organic
        # domains that merely carry an Apollo contact. Root cause of the run
        # where 3/5 final leads showed 0 Ahrefs paid traffic: kept[:target]
        # took domain-dict order + DM priority only, ignoring paid tier.
        kept.sort(
            key=lambda x: (self._advertiser_tier(x), x.get("_dm_priority", 0), x.get("lead_score", 0)),
            reverse=True,
        )
        _tier_hist = {}
        for _ld in kept:
            _t = self._advertiser_tier(_ld)
            _tier_hist[_t] = _tier_hist.get(_t, 0) + 1
        self._log(
            f"   Phase 5f advertiser-tier order (of {len(kept)} DM leads): "
            f"heavy-Search={_tier_hist.get(3,0)}, Search={_tier_hist.get(2,0)}, "
            f"ATC/other-paid={_tier_hist.get(1,0)}, unverified={_tier_hist.get(0,0)} "
            f"— top {self.max_leads or 'all'} kept paid-first"
        )

        # 2026-06-11: PAID-ONLY mode — export EVERY confirmed advertiser (tier>=1)
        # and nothing else, with NO max_leads ceiling. Max Leads becomes the floor
        # (it drives how many rediscovery rounds run); within those rounds we keep
        # all confirmed-paid leads instead of slicing to the target and padding
        # with unverified ones. So a max_leads=10 run that surfaces 40 confirmed
        # advertisers exports all 40, and zero non-confirmed leads.
        if getattr(self, "paid_only_all", False):
            _conf = [k for k in kept if self._advertiser_tier(k) >= 1]
            self._log(
                f"   Phase 5f PAID-ONLY: kept ALL {len(_conf)} confirmed advertisers "
                f"of {len(kept)} DM leads (dropped {len(kept) - len(_conf)} unverified); "
                f"no max_leads cap, no unverified padding"
            )
            self.leads = _conf
            return

        if not getattr(self, "quota_guarantee", False):
            # Fill quota from overflow (up to 3rd lead per company) if needed.
            if (self.max_leads and self.max_leads > 0
                    and len(kept) < self.max_leads and overflow):
                overflow.sort(key=lambda x: x.get("_dm_priority", 0), reverse=True)
                room = self.max_leads - len(kept)
                kept.extend(overflow[:room])

            unique_companies = len([k for k in by_domain.keys() if k])
            dropped_cap = len(dm_leads) - len(kept)
            self._log(
                f"   Phase 5f cap: {before} -> {len(kept)} "
                f"(-{dropped_non_dm} non-DM, -{dropped_cap} over-cap, "
                f"{unique_companies} unique companies)"
            )
            self.leads = kept
            return

        target = self.max_leads if self.max_leads and self.max_leads > 0 else before
        selected = list(kept[:target])
        selected_ids = {id(ld) for ld in selected}
        fill_counts = {
            "extra_dm": 0,
            "soft_dm": 0,
            "paid_practitioner": 0,
            "paid_named": 0,
        }

        def add_reserve(candidates, tier: str, reason: str):
            for ld in candidates:
                if len(selected) >= target:
                    break
                if id(ld) in selected_ids:
                    continue
                selected_ids.add(id(ld))
                selected.append(self._mark_quota_fill(ld, tier, reason))
                fill_counts[tier] = fill_counts.get(tier, 0) + 1

        overflow.sort(key=lambda x: x.get("_dm_priority", 0), reverse=True)
        add_reserve(
            overflow,
            "extra_dm",
            "Decision maker beyond the strict per-company cap",
        )

        soft_dm = [
            ld for ld in non_dm_leads
            if (ld.get("name") or "").strip()
            and (ld.get("role") or "").strip()
            and not _matches_negative_dm_pattern(ld.get("role", ""))
        ]
        soft_dm.sort(key=lambda x: (1 if self._is_paid_quota_candidate(x) else 0, x.get("lead_score", 0)), reverse=True)
        add_reserve(
            soft_dm,
            "soft_dm",
            "Named ambiguous-role lead kept to satisfy requested city quota",
        )

        trade_terms = (
            "practitioner", "technician", "specialist", "consultant", "advisor",
            "plumber", "electrician", "mechanic", "cleaner", "roofer", "painter",
            "builder", "agent", "lawyer", "accountant", "doctor", "dentist",
            "therapist", "trainer", "operator", "installer",
        )
        paid_practitioners = [
            ld for ld in non_dm_leads
            if (ld.get("name") or "").strip()
            and self._is_paid_quota_candidate(ld)
            and any(term in (ld.get("role") or "").lower() for term in trade_terms)
        ]
        paid_practitioners.sort(key=lambda x: x.get("lead_score", 0), reverse=True)
        add_reserve(
            paid_practitioners,
            "paid_practitioner",
            "Named practitioner/trade role on a paid-domain company",
        )

        remaining_named_paid = [
            ld for ld in non_dm_leads
            if (ld.get("name") or "").strip()
            and self._is_paid_quota_candidate(ld)
        ]
        remaining_named_paid.sort(key=lambda x: x.get("lead_score", 0), reverse=True)
        add_reserve(
            remaining_named_paid,
            "paid_named",
            "Remaining named paid-domain lead kept to satisfy requested city quota",
        )

        unique_companies = len([k for k in by_domain.keys() if k])
        remaining_drop = max(0, before - len(selected))
        reserve_added = len(selected) - min(len(kept), target)
        self._log(
            f"   Phase 5f quota guarantee: {before} -> {len(selected)} "
            f"(strict={min(len(kept), target)}, reserve_fill={reserve_added}, "
            f"remaining_drop={remaining_drop}, {unique_companies} DM companies)"
        )
        if reserve_added:
            self._log(
                "   Phase 5f quota tiers: "
                f"extra_dm={fill_counts.get('extra_dm', 0)}, "
                f"soft_dm={fill_counts.get('soft_dm', 0)}, "
                f"paid_practitioner={fill_counts.get('paid_practitioner', 0)}, "
                f"paid_named={fill_counts.get('paid_named', 0)}"
            )

        self.leads = selected

    def _topup_leads_to_quota(self, max_rounds: int = 0):
        """V5.30: Fetch more competitor domains until max_leads met or no more available.
        Bounded at max_rounds to prevent infinite runs. Each round uses a different
        keyword slice / competitor seed window to avoid repeat queries.
        V5.32: Default reduced from 10 → 3, added time budget to prevent 40+ min runs.
        PHASE 2: max_rounds now scales with max_leads so large targets aren't capped
        prematurely (175 → previously stalled at ~100 because of 3-round x 30-domain ceiling)."""
        if not self.max_leads or self.max_leads <= 0 or self._cancelled:
            return

        # Default 0 → auto-scale: 3 rounds for tiny targets, +1 per 50 leads thereafter (cap 10)
        if max_rounds <= 0:
            max_rounds = max(3, min(10, 3 + self.max_leads // 50))

        import time as _tup_time
        _tup_start = _tup_time.time()
        # V5.32 budget kept; scales linearly with max_leads (175 leads → ~87 min ceiling).
        _tup_budget_s = max(120, self.max_leads * 30)  # 2 min minimum, 30s per target lead

        already = set((d or "").lower() for d in self.domains)
        # Also include any domains that came in from previous top-up rounds
        for ld in self.leads:
            d = (ld.get("domain") or "").lower()
            if d:
                already.add(d)
        rounds = 0
        consecutive_empty = 0
        self._topup_active = True  # Bypass credit gate — we know we're short of quota

        try:
            while (rounds < max_rounds and len(self.leads) < self.max_leads
                   and not self._cancelled):
                if _tup_time.time() - _tup_start > _tup_budget_s:
                    self._log(f"   Phase 5f top-up: time budget ({_tup_budget_s}s) exhausted -- stopping")
                    break
                rounds += 1
                deficit = self.max_leads - len(self.leads)
                # PHASE 2: per-round domain ceiling now scales with deficit so 175-lead
                # targets don't get stuck at 100 because of the old hard 30-domain cap.
                #   floor 20  ·  ceiling 150 — gives big asks room but stays bounded
                want = min(max(deficit * 4, 20), 150)
                self._log(
                    f"   Phase 5f top-up round {rounds}/{max_rounds}: have {len(self.leads)}/"
                    f"{self.max_leads}, fetching up to {want} more domains"
                )

                new_domains = self._fetch_retry_domains(
                    already, max_new=want, round_num=rounds
                )
                if not new_domains:
                    consecutive_empty += 1
                    self._log(
                        f"   Phase 5f top-up round {rounds}: no new domains "
                        f"(empty streak: {consecutive_empty})"
                    )
                    if consecutive_empty >= 2:
                        self._log("   Phase 5f top-up: 2 consecutive empty rounds — stopping")
                        break
                    continue
                consecutive_empty = 0

                # PHASE 2 (2026-04-28) — _fetch_retry_domains pulls from SEMrush
                # get_adwords_domains() / get_domain_competitors(); both are paid
                # advertiser sources, so register them as confirmed-paid before
                # _enrich_single_domain runs the strict gate.
                for _d in new_domains:
                    _dl = (_d or "").lower()
                    if _dl:
                        self._confirmed_paid_domains.add(_dl)
                        self._adwords_domains.add(_d)

                new_raw = []
                for i, d in enumerate(new_domains):
                    if self._cancelled:
                        break
                    already.add(d.lower())
                    try:
                        r = self._enrich_single_domain(d, i, len(new_domains))
                        if r:
                            new_raw.extend(r)
                    except Exception as e:
                        self._log(f"   Top-up ERROR enriching {d}: {e}")

                if not new_raw:
                    self._log(f"   Phase 5f top-up round {rounds}: 0 leads from {len(new_domains)} new domains")
                    continue

                # Merge with prior leads, re-run cleanup (handles dedup + role blanking),
                # drop blank-identity, then re-apply DM filter + per-company cap.
                self.leads = self.leads + new_raw
                self._phase5_cleanup()
                self._phase5e_drop_blank_identity()
                self._apply_dm_filter_and_cap()
        finally:
            self._topup_active = False  # Re-enable credit gate after top-up

        if len(self.leads) < self.max_leads:
            self._log(
                f"   Phase 5f top-up: exited after {rounds} round(s) — "
                f"final {len(self.leads)}/{self.max_leads}"
            )

    # ── Phase 5g: Final Enrichment Retry + Completeness Gate ───────────────

    def _phase5g_completeness_gate(self):
        """V5.30: For each remaining lead, if missing phone OR email, run the full
        enrichment retry chain (Apollo → Lusha → SerpAPI phone → contact-page scrape
        for both phone+email). Then drop any lead with data_completeness_score < 2.
        Score: name=1, phone=1, email=1. A lead needs at least name + (phone OR email).
        """
        if not self.leads or self._cancelled:
            return
        self._progress(97, "Phase 5g: completeness retry + gate...")
        self._log("Phase 5g: Final enrichment retry for incomplete leads + completeness gate")

        gl = self.config.get("serpapi_gl", "au")
        filled_phone = 0
        filled_email = 0

        for ld in self.leads:
            if self._cancelled:
                break
            has_phone = bool((ld.get("phone") or "").strip())
            has_email = bool((ld.get("email") or "").strip())
            if has_phone and has_email:
                continue

            name = (ld.get("name") or "").strip()
            domain = (ld.get("domain") or "").strip()
            company = ld.get("company") or (domain_to_company_name(domain) if domain else "")
            if not domain:
                continue

            # (1) Apollo person re-enrich — only if full name + not yet Apollo-enriched
            # V5.32: gated by Apollo budget
            if name and " " in name and "apollo" not in ld.get("source", "").lower() and self._apollo_budget_ok():
                try:
                    parts = name.split()
                    enriched = self.apollo.enrich_person(
                        parts[0], parts[-1], domain,
                        linkedin_url=ld.get("_linkedin_url", ""),
                    )
                    if not has_phone and enriched.get("phone"):
                        ld["phone"] = enriched["phone"]
                        ld["source"] = ld.get("source", "") + "+Apollo5g"
                        has_phone = True
                        filled_phone += 1
                    if not has_email and enriched.get("email"):
                        ld["email"] = enriched["email"]
                        ld["source"] = ld.get("source", "") + "+ApolloEmail5g"
                        has_email = True
                        filled_email += 1
                except Exception:
                    pass

            # (2) Lusha person API — retry for missing phone or email (budget-guarded)
            _lusha_budget_5g = max(self.max_leads * 3, 30) if self.max_leads > 0 else 150
            if (not has_phone or not has_email) and name and " " in name and \
                    self.lusha._counter.get("lusha", 0) < _lusha_budget_5g:
                try:
                    parts = name.split()
                    lusha_res = self.lusha.enrich_person(parts[0], parts[-1], domain)
                    if lusha_res:
                        if not has_phone and lusha_res.get("phone"):
                            ld["phone"] = lusha_res["phone"]
                            ld["_phone_quality"] = lusha_res.get("_phone_quality", 20)
                            ld["source"] = ld.get("source", "") + "+Lusha5g"
                            has_phone = True
                            filled_phone += 1
                        if not has_email and lusha_res.get("email"):
                            ld["email"] = lusha_res["email"]
                            ld["source"] = ld.get("source", "") + "+LushaEmail5g"
                            has_email = True
                            filled_email += 1
                except Exception:
                    pass

            # (3) Contact-page scrape — both phone and email in one pass
            if not has_phone or not has_email:
                try:
                    scrape_res = self._scrape_contact_pages_full(domain)
                    if not has_phone and scrape_res.get("phone"):
                        ld["phone"] = scrape_res["phone"]
                        ld["source"] = ld.get("source", "") + "+ContactScrape5g"
                        has_phone = True
                        filled_phone += 1
                    if not has_email and scrape_res.get("emails"):
                        # Prefer a name-matching email if a name is known
                        best_em = ""
                        if name:
                            _name_words = [w.lower() for w in name.split() if len(w) >= 2]
                            for em in scrape_res["emails"]:
                                _local = em.split("@")[0].lower().replace(".", "").replace("-", "").replace("_", "")
                                if any(nw in _local for nw in _name_words):
                                    best_em = em
                                    break
                        if not best_em:
                            best_em = scrape_res["emails"][0]
                        ld["email"] = best_em
                        ld["source"] = ld.get("source", "") + "+ScrapeEmail5g"
                        has_email = True
                        filled_email += 1
                except Exception:
                    pass

            # (4) SerpAPI business phone — last resort for phone
            if not has_phone and self.serpapi._available:
                try:
                    phone = self.serpapi.find_business_phone(domain, company, gl)
                    if phone:
                        ld["phone"] = phone
                        ld["source"] = ld.get("source", "") + "+SerpPhone5g"
                        has_phone = True
                        filled_phone += 1
                except Exception:
                    pass

            # (5) Inferred email pattern — last resort for email
            if not has_email and name and " " in name:
                try:
                    parts = name.split()
                    candidates = generate_email_candidates(parts[0], parts[-1], domain)
                    if candidates:
                        ld["email"] = candidates[0]
                        ld["_email_inferred"] = True
                        ld["source"] = ld.get("source", "") + "+InferredEmail5g"
                        has_email = True
                        filled_email += 1
                except Exception:
                    pass

        if filled_phone or filled_email:
            self._log(
                f"   Phase 5g retry: filled {filled_phone} phone(s), "
                f"{filled_email} email(s)"
            )

        # Completeness gate: drop leads that don't meet the name+(phone|email) floor.
        # 2026-05-21: relaxed for Google Intent + silent-scope Apollo leads
        # AND for enrichment-OFF runs. The user explicitly asked for "leads
        # without email or phone number when enrichment is off through
        # Google itself" — AU SMB businesses often have neither in Apollo's
        # index even when enrichment is on, so the strict gate would empty
        # the result table on every AU run. The relaxation:
        #   • _google_intent flagged → name+domain is enough; phone/email
        #     optional (the user's "lead" is the business itself; they'll
        #     enrich it manually if they want contact info).
        #   • _silent_scope_apollo flagged → same — Apollo found the org
        #     but Apollo's AU mobile/personal-email coverage is weak.
        #   • enrichment_enabled == False → user opted out of reveal, so a
        #     name+domain lead is what they asked for.
        before = len(self.leads)
        kept = []
        dropped_reasons = {"no_name": 0, "no_contact": 0, "no_domain_fallback": 0}
        for ld in self.leads:
            has_name = bool((ld.get("name") or "").strip())
            has_phone = bool((ld.get("phone") or "").strip())
            has_email = bool((ld.get("email") or "").strip())
            has_domain = bool((ld.get("domain") or "").strip())
            score = int(has_name) + int(has_phone) + int(has_email)
            ld["_completeness_score"] = score
            # Relaxation: when contact info is optional (see comment above),
            # we still require name OR domain — a totally blank record helps
            # nobody.
            _relaxed = (
                ld.get("_google_intent")
                or ld.get("_silent_scope_apollo")
                or (not self.enrichment_enabled)
            )
            if not has_name:
                # In relaxed mode a Google-Intent lead with just domain
                # still makes it through — the business website is the
                # user's primary follow-up channel.
                if _relaxed and has_domain:
                    # Stamp a best-effort name from the domain root so
                    # the table row doesn't render "(unnamed)" everywhere.
                    try:
                        _root = (ld.get("domain") or "").split(".")[0].replace("-", " ").title()
                        ld["name"] = _root or "(business)"
                    except Exception:
                        ld["name"] = "(business)"
                    has_name = True
                else:
                    dropped_reasons["no_name"] += 1
                    continue
            if not has_phone and not has_email and not _relaxed:
                dropped_reasons["no_contact"] += 1
                continue
            if not has_domain and not has_phone and not has_email:
                # Even relaxed mode rejects totally empty records.
                dropped_reasons["no_domain_fallback"] += 1
                continue
            # score >= 2 (full enrichment) OR relaxed-mode (name+domain) passes
            kept.append(ld)

        dropped = before - len(kept)
        if dropped:
            self._log(
                f"   Phase 5g gate: dropped {dropped} lead(s) below completeness "
                f"(no_name={dropped_reasons['no_name']}, "
                f"no_contact={dropped_reasons['no_contact']}, "
                f"no_domain_fallback={dropped_reasons.get('no_domain_fallback', 0)})"
            )
        # 2026-05-21: end-of-Phase-5g FUNNEL diagnostic — shows where leads
        # were lost across the pipeline. The user sees this in the log feed
        # IMMEDIATELY after a run and can pinpoint why 0 leads happened.
        try:
            _domains_n = len(getattr(self, "domains", []) or [])
            _stub_leads = sum(1 for _l in (kept or []) if _l.get("_stub_lead"))
            _google_leads = sum(1 for _l in (kept or []) if _l.get("_google_intent"))
            _silent_apollo = sum(1 for _l in (kept or []) if _l.get("_silent_scope_apollo"))
            _has_phone = sum(1 for _l in (kept or []) if (_l.get("phone") or "").strip())
            _has_email = sum(1 for _l in (kept or []) if (_l.get("email") or "").strip())
            self._log(
                "[FUNNEL] domains_enriched={d} → leads_into_5g={b} → "
                "leads_after_5g={k} (dropped_no_name={nn}, dropped_no_contact={nc}). "
                "Of the {k} kept: with_phone={hp}, with_email={he}, "
                "stub={st}, google_intent={gi}, silent_apollo={sa}.".format(
                    d=_domains_n, b=before, k=len(kept),
                    nn=dropped_reasons.get("no_name", 0),
                    nc=dropped_reasons.get("no_contact", 0),
                    hp=_has_phone, he=_has_email,
                    st=_stub_leads, gi=_google_leads, sa=_silent_apollo,
                )
            )
            if len(kept) == 0 and _domains_n > 0:
                self._log(
                    "[FUNNEL] ⚠ 0 leads survived Phase 5g despite "
                    f"{_domains_n} domain(s) entering enrichment. Most likely cause: "
                    "Apollo's mixed_people/api_search returned 0 people for every "
                    "domain (common for small AU SMBs not indexed in Apollo's people DB). "
                    "Fix: enable enrichment toggle OFF (triggers stub-lead fallback), "
                    "OR set GOOGLE_PLACES_API_KEY (Places-discovered domains auto-stub), "
                    "OR pick a city/industry with larger orgs (more people-DB coverage)."
                )
        except Exception:
            pass
        self.leads = kept
        self._log(
            f"   Phase 5g complete: {len(self.leads)} leads pass gate "
            f"(target {self.max_leads or 'unlimited'})"
        )

    # ── Phase 5h: SEMrush metadata backfill + person-phone retry (V5.31) ────

    def _phase5h_metadata_backfill_and_phone_retry(self):
        """V5.31: Additive-only. Top-up (Phase 5f) pulls in new domains AFTER
        Phase 5c runs, so those domains ship with empty _kw1_*, _kw2_*,
        _competitor_*, and domain-overview fields. This pass:
          (A) Re-runs SEMrush domain_organic + organic_competitors + domain_ranks
              ONLY for domains missing _kw1_keyword or _competitor_1, then
              backfills empty fields on leads sharing those domains.
          (B) Tries one more SerpAPI person-phone lookup for leads still
              missing phone after Phase 5g's chain.
        Never drops a lead, never overwrites populated fields.
        """
        if not self.leads:
            return
        self._set_semrush_phase("phase5h_backfill")
        # 2026-05-18: Phase 5h fires 3 SEMrush calls per backfill-eligible
        # domain — same shape as Phase 5c. Silent-scope and budget-aware:
        # if SEMrush is silent we can't backfill anyway, and if budget is
        # already tight Part A skips entirely (Part B's SerpAPI retry still
        # runs because that's a different API + a different credit pool).
        _silent_5h = getattr(self, "_semrush_silent_scope", False)
        _budget_5h = int(getattr(self.semrush, "_unit_budget", 0) or 0)
        _used_5h = int(getattr(self.semrush, "_units_used", 0) or 0)
        _budget_tight = bool(_budget_5h) and _used_5h >= int(_budget_5h * 0.85)
        _skip_part_a = _silent_5h or _budget_tight

        db = self.config["semrush_db"]

        # Part A: SEMrush metadata backfill for top-up domains.
        # 2026-05-18 (round 3): trigger tightened. Was firing for any lead
        # missing _kw1_keyword OR _competitor_1 — that's most top-up leads,
        # which ballooned the cost (for max_leads=250 we'd spend ~27k units
        # backfilling 125 domains). Now we ALSO cap the total backfill set
        # at `max(2, max_leads * 0.3)` so the cost is at most 30% of leads
        # × ~220 units per domain. Domains beyond the cap ship with empty
        # _kw1_*/_competitor_* fields — graceful degradation rather than
        # blow the budget chasing nice-to-haves.
        needs_backfill: dict[str, list] = {}
        for ld in self.leads:
            d = (ld.get("domain") or "").strip().lower()
            if not d:
                continue
            # AND-tighten: require BOTH fields missing (a lead with just a
            # missing _competitor_1 still has kw_picks and isn't worth a
            # 220-unit triple-call to fill a single slot).
            if not ld.get("_kw1_keyword") and not ld.get("_competitor_1"):
                needs_backfill.setdefault(d, []).append(ld)
        _mx_5h = int(self.max_leads or 0)
        if _mx_5h > 0:
            # 2026-05-18 (round 3 v2): hard ceiling of 30 backfill domains
            # even on large runs. Beyond that the marginal benefit of an
            # extra _kw1 field on lead #100+ is dwarfed by the credit cost.
            _backfill_cap = max(2, min(30, int(_mx_5h * 0.3)))
            if len(needs_backfill) > _backfill_cap:
                # Keep the first N (insertion order matches lead priority)
                _keys = list(needs_backfill.keys())[:_backfill_cap]
                self._log(
                    f"Phase 5h: trimming backfill set "
                    f"{len(needs_backfill)} -> {_backfill_cap} "
                    f"domain(s) (cap = 30% of max_leads={_mx_5h})"
                )
                needs_backfill = {k: needs_backfill[k] for k in _keys}

        if needs_backfill and _skip_part_a:
            self._progress(98, "Phase 5h: SEMrush backfill skipped (silent/budget)")
            self._log(
                f"Phase 5h: skipping SEMrush backfill for {len(needs_backfill)} "
                f"domain(s) — silent_scope={_silent_5h}, "
                f"budget_used={_used_5h}/{_budget_5h}"
            )
            needs_backfill = {}
        if needs_backfill:
            self._progress(98, f"Phase 5h: SEMrush backfill on {len(needs_backfill)} domain(s)...")
            self._log(
                f"Phase 5h: SEMrush metadata backfill for {len(needs_backfill)} domain(s)"
            )
            backfilled_domains = 0
            for domain, leads in needs_backfill.items():
                try:
                    # 2026-05-18 (round 3): all three calls now go through
                    # the per-run cache. Domains the pipeline already touched
                    # in Phase 4 / 5c will hit the cache for `ov` (no spend);
                    # `kws`/`comps` may or may not be cached depending on
                    # whether Phase 5c ran for this domain.
                    kws = self._cached_organic_keywords(domain, db, limit=10)
                    comps = self._cached_organic_competitors(domain, db, limit=3)
                    ov = self._cached_overview(domain, db)
                except Exception as e:
                    self._log(f"   {domain}: SEMrush backfill error: {e}")
                    continue
                picks = kws[:2] if kws else []
                any_filled = False
                for ld in leads:
                    if not ld.get("_kw1_keyword") and len(picks) >= 1:
                        k = picks[0]
                        ld["_kw1_keyword"] = k.get("keyword", "")
                        ld["_kw1_volume"] = k.get("volume", "")
                        ld["_kw1_position"] = k.get("position", "")
                        ld["_kw1_url"] = k.get("url", "")
                        any_filled = True
                    if not ld.get("_kw2_keyword") and len(picks) >= 2:
                        k = picks[1]
                        ld["_kw2_keyword"] = k.get("keyword", "")
                        ld["_kw2_volume"] = k.get("volume", "")
                        ld["_kw2_position"] = k.get("position", "")
                        ld["_kw2_url"] = k.get("url", "")
                        any_filled = True
                    if not ld.get("_competitor_1") and len(comps) >= 1:
                        ld["_competitor_1"] = comps[0]
                        any_filled = True
                    if not ld.get("_competitor_2") and len(comps) >= 2:
                        ld["_competitor_2"] = comps[1]
                        any_filled = True
                    if ov:
                        if not ld.get("_organic_traffic"):
                            ld["_organic_traffic"] = ov.get("organic_traffic", 0)
                            any_filled = True
                        if not ld.get("_paid_traffic"):
                            ld["_paid_traffic"] = ov.get("paid_traffic", 0)
                            any_filled = True
                        if not ld.get("_organic_keywords"):
                            ld["_organic_keywords"] = ov.get("organic_keywords", 0)
                            any_filled = True
                        if not ld.get("_paid_keywords"):
                            ld["_paid_keywords"] = ov.get("paid_keywords", 0)
                            any_filled = True
                if any_filled:
                    backfilled_domains += 1
            self._log(f"   Phase 5h: backfilled metadata on {backfilled_domains} domain(s)")

        # Part B: SerpAPI person-phone retry for leads still missing phone
        phone_targets = [
            ld for ld in self.leads
            if not (ld.get("phone") or "").strip()
            and (ld.get("name") or "").strip()
            and " " in (ld.get("name") or "").strip()
            and (ld.get("domain") or "").strip()
        ]
        if phone_targets and getattr(self.serpapi, "_available", False):
            self._log(
                f"Phase 5h: SerpAPI person-phone retry on {len(phone_targets)} lead(s)"
            )
            gl = self.config.get("serpapi_gl", (self.country or "us").lower())
            filled = 0
            for ld in phone_targets:
                try:
                    found = self.serpapi.find_person_phone(
                        ld["name"], ld["domain"], ld.get("company") or "", gl
                    )
                    if found:
                        ld["phone"] = found
                        src = ld.get("source", "") or ""
                        if "PersonPhone5h" not in src:
                            ld["source"] = (src + "+PersonPhone5h").lstrip("+")
                        filled += 1
                except Exception:
                    continue
            if filled:
                self._log(f"   Phase 5h: filled {filled} additional phone(s) via person-phone search")

    # ── Phase 5i: Strict Quality Gate + Refill (V5.32) ─────────────────────

    def _resolve_first_name_leads_before_gate(self):
        """V5.32: Last-chance name resolver. Before Phase 5i drops leads for no_full_name,
        try to resolve each first-name-only lead via:
          (1) Apollo mixed_people/api_search by first_name+domain (no reveal credits)
          (2) Lusha enrich_person with just first name + domain
        Skips leads that already have a full name."""
        if not self.leads:
            return
        targets = [
            ld for ld in self.leads
            if (ld.get("name") or "").strip() and " " not in (ld.get("name") or "").strip()
            and (ld.get("domain") or "").strip()
        ]
        if not targets:
            return
        self._log(f"   Phase 5i pre-gate: resolving full name for {len(targets)} first-name lead(s)")
        resolved_count = 0
        for ld in targets:
            if self._cancelled:
                break
            first_only = (ld.get("name") or "").strip()
            domain = (ld.get("domain") or "").strip()
            if not domain or not first_only:
                continue
            # (1) Apollo mixed_people/api_search — no reveal, just finds full name in records
            try:
                search_url = f"{self.apollo.BASE_URL}/mixed_people/api_search"
                payload = {
                    "q_person_name": first_only,
                    "q_organization_domains": [domain],
                    "per_page": 5,
                }
                resp = requests.post(search_url, json=payload,
                                     headers=self.apollo._headers(), timeout=15)
                if resp.status_code == 200:
                    self._api_counter["apollo"] = self._api_counter.get("apollo", 0) + 1
                    people = resp.json().get("people", [])
                    for cand in people:
                        c_first = safe_str(cand.get("first_name")).strip().lower()
                        c_last = safe_str(cand.get("last_name")).strip()
                        if c_first != first_only.lower() or not c_last:
                            continue
                        last_clean = c_last.rstrip(".")
                        if len(last_clean) == 1 and last_clean.isalpha():
                            continue  # obfuscated "R." — reject
                        full = f"{first_only} {c_last}".strip()
                        if _is_valid_person_name(full):
                            ld["name"] = full
                            ld["_needs_full_name"] = False
                            ld["source"] = (ld.get("source", "") + "+ApolloSearch5i").lstrip("+")
                            resolved_count += 1
                            self._log(f"      [Name5i] '{first_only}' -> '{full}' via Apollo search at {domain}")
                            break
            except Exception:
                pass
            # Check if (1) resolved it
            if " " in (ld.get("name") or ""):
                continue
            # (2) Lusha with first name only — budget-guarded
            _lusha_budget_5i = max(self.max_leads * 3, 30) if self.max_leads > 0 else 150
            if self.lusha._counter.get("lusha", 0) >= _lusha_budget_5i:
                break
            try:
                lusha_res = self.lusha.enrich_person(first_only, "", domain)
                if lusha_res:
                    l_name = safe_str(lusha_res.get("name")).strip()
                    if l_name and " " in l_name:
                        # Verify first name matches
                        if l_name.split()[0].lower() == first_only.lower():
                            ld["name"] = l_name
                            ld["_needs_full_name"] = False
                            ld["source"] = (ld.get("source", "") + "+Lusha5i").lstrip("+")
                            resolved_count += 1
                            self._log(f"      [Name5i] '{first_only}' -> '{l_name}' via Lusha at {domain}")
            except Exception:
                pass
        if resolved_count:
            self._log(f"   Phase 5i pre-gate: resolved {resolved_count}/{len(targets)} full names")

    def _phase5i_phone_gate(self):
        """V5.32: Strict quality gate. Every lead in the final CSV must have:
          1. FULL NAME — first + last (at least 2 name parts, each >= 2 chars)
          2. DIRECT PHONE — not shared with another lead in same company (HQ filter)
          3. PERSONAL/WORK EMAIL — NOT a generic inbox (info@, hello@, team@, etc.)
        Leads failing any check are dropped. If count < max_leads, refill loop
        runs up to 4 rounds, each pulling new domains + full enrichment."""
        if not self.leads or not self.max_leads or self.max_leads <= 0:
            return

        self._log("Phase 5i: Strict quality gate (full name + direct phone + personal email)")

        # V5.32: Last-chance name resolution. For leads with phone + email but only a
        # first name, try Apollo mixed_people/api_search (no reveal credits) and Lusha
        # before dropping. Saves leads where Apollo's enrich_person returned a locked record.
        self._resolve_first_name_leads_before_gate()

        def _is_qualified(ld):
            name = (ld.get("name") or "").strip()
            phone = (ld.get("phone") or "").strip()
            email = (ld.get("email") or "").strip()
            name_parts = [p for p in name.split() if len(p) >= 2]
            if len(name_parts) < 2:
                return False, "no_full_name"
            if not phone:
                return False, "no_phone"
            if not email:
                return False, "no_email"
            local = email.lower().split("@")[0].strip()
            if local in GENERIC_EMAIL_PREFIXES:
                return False, "generic_email"
            return True, ""

        def _drop_shared_phones(leads):
            """If multiple leads in same domain share a phone, keep only the one
            with the highest DM priority. Clears shared phone from others."""
            from collections import defaultdict
            phone_groups = defaultdict(list)
            for ld in leads:
                p = (ld.get("phone") or "").strip()
                d = (ld.get("domain") or "").strip().lower()
                if p and d:
                    phone_groups[(d, p)].append(ld)
            kept_ids = set()
            for (_d, _p), group in phone_groups.items():
                if len(group) == 1:
                    kept_ids.add(id(group[0]))
                else:
                    best = max(group, key=lambda ld: _calculate_dm_priority(ld.get("role", "") or ""))
                    kept_ids.add(id(best))
            dropped = 0
            for ld in leads:
                p = (ld.get("phone") or "").strip()
                d = (ld.get("domain") or "").strip().lower()
                if p and d and id(ld) not in kept_ids:
                    ld["phone"] = ""
                    dropped += 1
            return dropped

        def _apply_gate(leads):
            shared_dropped = _drop_shared_phones(leads)
            if shared_dropped:
                self._log(f"   Phase 5i: cleared {shared_dropped} shared/HQ phone number(s)")
            qualified = []
            reasons = {"no_full_name": 0, "no_phone": 0, "no_email": 0, "generic_email": 0}
            for ld in leads:
                ok, reason = _is_qualified(ld)
                if ok:
                    qualified.append(ld)
                else:
                    reasons[reason] += 1
            return qualified, reasons

        before = len(self.leads)
        self.leads, reasons = _apply_gate(self.leads)
        dropped = before - len(self.leads)
        if dropped:
            self._log(
                f"   Phase 5i: dropped {dropped} lead(s) -- "
                f"no_full_name={reasons['no_full_name']}, "
                f"no_phone={reasons['no_phone']}, "
                f"no_email={reasons['no_email']}, "
                f"generic_email={reasons['generic_email']}"
            )

        if len(self.leads) >= self.max_leads:
            self._log(f"   Phase 5i: {len(self.leads)} qualified leads >= target {self.max_leads} -- OK")
            return

        # V5.32: Scale refill rounds by max_leads + enforce time budget.
        # Small max_leads (1-5) → 1 refill max (prevents 40-min runs for max_leads=1).
        # PHASE 2: 175-lead targets need more refill rounds — scale with target size.
        if self.max_leads <= 5:
            max_refill = 1
        elif self.max_leads <= 50:
            max_refill = 3
        else:
            max_refill = max(4, min(8, self.max_leads // 30))
        import time as _time
        _refill_start = _time.time()
        _refill_budget_s = max(120, self.max_leads * 60)  # 2 min minimum, 1 min per target lead

        for rnd in range(max_refill):
            if self._cancelled:
                break
            if _time.time() - _refill_start > _refill_budget_s:
                self._log(f"   Phase 5i refill: time budget ({_refill_budget_s}s) exhausted -- stopping")
                break
            gap = self.max_leads - len(self.leads)
            self._log(f"   Phase 5i refill round {rnd + 1}/{max_refill}: need {gap} more qualified leads")
            before_qualified = len(self.leads)
            before_refill = len(self.leads)
            # V5.32 reduced this; PHASE 2 lets it auto-scale (max_rounds=0 → derived from max_leads).
            self._topup_leads_to_quota(max_rounds=0)
            if len(self.leads) <= before_refill:
                self._log("   Phase 5i refill: no new leads from top-up -- stopping")
                break
            self._phase5g_completeness_gate()
            self._phase5h_metadata_backfill_and_phone_retry()
            before = len(self.leads)
            self.leads, reasons = _apply_gate(self.leads)
            dropped = before - len(self.leads)
            if dropped:
                self._log(
                    f"   Phase 5i refill: dropped {dropped} more unqualified -- "
                    f"no_full_name={reasons['no_full_name']}, "
                    f"no_phone={reasons['no_phone']}, "
                    f"no_email={reasons['no_email']}, "
                    f"generic_email={reasons['generic_email']}"
                )
            # V5.32: Exit if refill added zero qualified leads — don't waste another round
            if len(self.leads) <= before_qualified:
                self._log("   Phase 5i refill: 0 qualified leads added this round -- stopping")
                break
            if len(self.leads) >= self.max_leads:
                break

        self._log(f"   Phase 5i complete: {len(self.leads)} qualified leads (target {self.max_leads})")

    # ── Partial export (Phase 2, 2026-05-05) ────────────────────────────────
    # Fired on cancellation / error so the user can recover work-in-progress
    # leads. Intentionally minimal: writes whatever's in self.leads to a CSV
    # with `_partial` suffix. NEVER touches master_leads (partial runs must
    # not pollute the master DB) and skips sector filter / quota guarantee
    # duplication. wsgi.py serves the path via /download-partial/<job_id>.

    def _export_partial_now(self) -> str:
        if not getattr(self, "leads", None):
            return ""
        try:
            os.makedirs(self.output_folder, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            slug = re.sub(r"[^\w]+", "_", (self.industry or "run").lower()).strip("_") or "run"
            path = os.path.join(self.output_folder, f"leads_{slug}_{timestamp}_partial.csv")
            cols = ["Name", "Company Name", "Domain", "Role", "Phone Number",
                    "Email", "LinkedIn URL", "Traffic Source", "Source"]
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cols)
                writer.writeheader()
                for ld in self.leads:
                    if not isinstance(ld, dict):
                        continue
                    writer.writerow({
                        "Name": ld.get("name") or "",
                        "Company Name": ld.get("company") or "",
                        "Domain": ld.get("domain") or "",
                        "Role": ld.get("role") or "",
                        "Phone Number": ld.get("phone") or "",
                        "Email": ld.get("email") or "",
                        "LinkedIn URL": ld.get("_linkedin_url") or "",
                        "Traffic Source": self._traffic_source_label(ld),
                        "Source": (
                            (ld.get("source") or "") + ("+GoogleIntent" if ld.get("_google_intent") else "")
                        ),
                    })
            try:
                self._partial_csv_path = path
            except Exception:
                pass
            self._log(
                f"   [PARTIAL] Wrote {len(self.leads)} interim lead(s) to {path} "
                f"(NOT inserted into master_leads)"
            )
            return path
        except Exception as e:
            try:
                self._log(f"   [PARTIAL] Could not write partial CSV: {e}")
            except Exception:
                pass
            return ""

    # ── Phase 6: CSV Export ─────────────────────────────────────────────────

    def _phase6_export(self) -> str:
        self._progress(97, "Sorting and exporting CSV...")
        self._log("Phase 6: CSV export with decision-maker grouping")
        self._log(f"   [Progress] Phase 6 START: {len(self.leads)} valid leads on hand")

        if not self.leads:
            self._log("   No leads to export.")
            return ""

        # ── V5.13 Scoring: Enhanced partition-based with traffic source priority ──
        def _partition_score(lead):
            """V5.15: New 12-tier scoring per user-specified algorithm.

            PRIMARY SORT KEY — Completeness tier (12 = best, 0 = worst):
              12: Name+Domain+Role+Phone+Email(Personal/Verified)
              11: Name+Domain+Role+Phone+Email(Generic/Work)
              10: Name+Domain+Role+Phone  (no email)
               9: Name+Domain+Role+Email(Personal/Verified)  (no phone)
               8: Name+Domain+Role+Email(Generic/Work)  (no phone)
               7: Name+Domain+Role  (no phone, no email)
               6: Name+Domain+Phone+Email(Personal/Verified)  (no role)
               5: Name+Domain+Phone+Email(Generic/Work)  (no role)
               4: Name+Domain+Phone  (no role, no email)
               3: Name+Domain+Email(Personal/Verified)  (no role, no phone)
               2: Name+Domain+Email(Generic/Work)  (no role, no phone)
               1: Name+Domain only
               0: anything else

            SECONDARY — within same tier:
              Paid > Organic (+100 bonus)
            TERTIARY — DM role quality (+20 hard DM, +10 soft DM)
            QUATERNARY — WHOIS founder verified (+5)
            """
            has_name = bool(lead.get("name"))
            has_domain = bool(lead.get("domain"))
            has_role = bool(lead.get("role"))
            has_phone = bool(lead.get("phone"))
            raw_email = lead.get("email", "")
            has_email = bool(raw_email)
            is_paid = lead.get("_domain_source") == "paid"
            is_personal_em = has_email and (
                lead.get("_email_verified") or is_personal_email(raw_email)
            )
            # Generic/Work = has email but NOT personal
            is_any_em = has_email

            # Determine completeness tier
            if has_name and has_domain and has_role and has_phone and is_personal_em:
                tier = 12
            elif has_name and has_domain and has_role and has_phone and is_any_em:
                tier = 11
            elif has_name and has_domain and has_role and has_phone:
                tier = 10
            elif has_name and has_domain and has_role and not has_phone and is_personal_em:
                tier = 9
            elif has_name and has_domain and has_role and not has_phone and is_any_em:
                tier = 8
            elif has_name and has_domain and has_role:
                tier = 7
            elif has_name and has_domain and not has_role and has_phone and is_personal_em:
                tier = 6
            elif has_name and has_domain and not has_role and has_phone and is_any_em:
                tier = 5
            elif has_name and has_domain and not has_role and has_phone:
                tier = 4
            elif has_name and has_domain and not has_role and not has_phone and is_personal_em:
                tier = 3
            elif has_name and has_domain and not has_role and not has_phone and is_any_em:
                tier = 2
            elif has_name and has_domain:
                tier = 1
            else:
                tier = 0

            # Secondary: Paid vs Organic
            paid_bonus = 100 if is_paid else 0

            # Tertiary: DM role quality
            _role_str = (lead.get("role") or "").lower()
            if any(kw in _role_str for kw in HARD_DM_KEYWORDS):
                dm_bonus = 20
            elif any(kw in _role_str for kw in SOFT_DM_KEYWORDS):
                dm_bonus = 10
            else:
                dm_bonus = 0

            # Quaternary: WHOIS founder verified
            whois_bonus = 5 if lead.get("_founder_verified") else 0

            return tier * 1000 + paid_bonus + dm_bonus + whois_bonus

        # Score all leads and store for CSV output
        for lead in self.leads:
            lead["_score"] = _partition_score(lead)
            lead["lead_score"] = lead["_score"]

        self.leads.sort(key=lambda ld: ld["_score"], reverse=True)

        # Log partition counts
        p1 = sum(1 for ld in self.leads if ld["_score"] >= 6000)
        p2 = sum(1 for ld in self.leads if 4000 <= ld["_score"] < 5000)
        p3 = sum(1 for ld in self.leads if 3000 <= ld["_score"] < 4000)
        p4 = sum(1 for ld in self.leads if 2000 <= ld["_score"] < 3000)
        p5 = sum(1 for ld in self.leads if 1000 <= ld["_score"] < 2000)
        self._log(f"   V5.4 Partitions: {p1} Name+Email+Phone | {p2} Name+Phone | "
                  f"{p3} Name+Email | {p4} Phone-only | {p5} Email-only")

        # ── V5.20: Role-hierarchy domain grouping ──
        # Group by domain, sort within each group by role hierarchy score (owner>founder>c-suite>...),
        # keep top 2 per domain (but keep ALL owners+founders if both exist).
        MAX_PER_DOMAIN = 3  # V5.25: User requested max 3 leads per company
        domain_groups = defaultdict(list)
        no_domain = []
        for lead in self.leads:
            d = (lead.get("domain") or "").strip().lower()
            if d:
                domain_groups[d].append(lead)
            else:
                no_domain.append(lead)

        top_section = []
        rest_section = []
        for d, group in domain_groups.items():
            # V5.20: Score each lead by role hierarchy (owner=100, founder=95, ... intern=10)
            for ld in group:
                ld["_hierarchy_score"] = _role_hierarchy_score(ld.get("role", ""))
            group.sort(key=lambda x: (x["_hierarchy_score"], x.get("_score", 0)), reverse=True)

            # DM-confidence cap:
            #   top hierarchy >= 85 (owner/founder/C-suite/partner) -> keep 1
            #   top hierarchy 60-84 (VP/head/director/manager)      -> keep 1 (confident enough)
            #   top hierarchy <  60 (senior/IC/unknown/intern)      -> keep 2 (low confidence only)
            _top_score = group[0]["_hierarchy_score"] if group else 0
            if _top_score >= 60:
                keep_count = 1
            else:
                keep_count = 2

            # Strict-first domain cap. Normal industry mode keeps the old hard
            # drop; quota-guarantee city mode demotes overflow into reserve so
            # the final CSV can still hit the requested row count.
            if len(group) <= keep_count:
                top_section.extend(group)
            else:
                top_section.extend(group[:keep_count])
                overflow = group[keep_count:]
                if getattr(self, "quota_guarantee", False):
                    for ld in overflow:
                        ld.setdefault("_quota_fill_tier", "phase6_domain_overflow")
                        ld.setdefault(
                            "_quota_fill_reason",
                            "Domain overflow retained after strict-first Phase 6 ranking",
                        )
                    rest_section.extend(overflow)
                # Non-guarantee: group[keep_count:] intentionally dropped

            for ld in group:
                ld.pop("_hierarchy_score", None)

        top_section.extend(no_domain)
        top_section.sort(key=lambda ld: ld.get("_score", 0), reverse=True)
        rest_section.sort(key=lambda ld: ld.get("_score", 0), reverse=True)

        def _spread_domains(leads):
            """Ensure no two consecutive leads share the same domain."""
            result = []
            deferred = []
            last_domain = None
            for ld in leads:
                d = (ld.get("domain") or "").strip().lower()
                if d and d == last_domain:
                    deferred.append(ld)
                else:
                    result.append(ld)
                    last_domain = d
            result.extend(deferred)
            return result

        merged_sections = _spread_domains(top_section + rest_section)

        # 2026-05-26: HARD post-merge per-domain cap. Earlier logic at lines
        # ~9462 keeps 1-2 in top_section, but quota_guarantee city-mode
        # slipped overflow back in via rest_section so the same domain could
        # surface 3+ times in the final CSV.
        # 2026-05-28 (TIGHTENED): the bonus-to-3 threshold was >= 80, which
        # fires for ANY senior title — two "Managing Director" rows (each
        # scores 90) on the same domain triggered a 3rd lead, which the user
        # flagged as "3 same domains again". Per the explicit rule ("twice
        # only if necessary, almost NEVER three times"), the 3rd slot is now
        # gated at >= 95 — owner / co-owner / founder ONLY. A company would
        # need TWO owner/founder-tier contacts for a 3rd row, which is
        # genuinely rare. Everything else is a hard cap of 2.
        # 2026-06-03: UNIQUE-DOMAIN output policy (user spec). One lead per
        # domain, period — UNLESS the unique pool can't fill max_leads, in
        # which case allow a 2nd DISTINCT lead for at most `max_leads//20`
        # domains ("≤1 duplicate per 20, only if necessary, very rare").
        # This is OUTPUT dedup only — discovery/enrichment/scoring untouched.
        def _cap_per_domain(leads_in: list, target: int = 0) -> list:
            # Pass 1 — keep the first (highest-ranked) lead per domain; hold
            # 2nd+ DISTINCT leads aside as "spares" for the rare top-up.
            seen_dom: set = set()
            seen_ident: dict = {}
            unique: list = []
            spares: list = []
            for ld in leads_in:
                d = (ld.get("domain") or "").strip().lower()
                if not d:
                    unique.append(ld)           # no domain → never a dup
                    continue
                ident = ((ld.get("email") or "").strip().lower()
                         or f"{(ld.get('name') or '').strip().lower()}|{(ld.get('role') or '').strip().lower()}")
                idents = seen_ident.setdefault(d, set())
                if d not in seen_dom:
                    seen_dom.add(d)
                    idents.add(ident)
                    unique.append(ld)
                elif ident and ident not in idents:
                    idents.add(ident)
                    spares.append(ld)           # distinct 2nd contact, held back
                # else: exact-identity repeat → dropped entirely
            # Pass 2 — only if we can't hit target with unique domains, add a
            # FEW spares back (rarest possible), capped at max_leads//20 doubles.
            _allowed_doubles = (int(target) // 20) if target and int(target) > 0 else 0
            if target and len(unique) < int(target) and _allowed_doubles > 0 and spares:
                out = list(unique)
                doubled = 0
                for sp in spares:
                    if len(out) >= int(target) or doubled >= _allowed_doubles:
                        break
                    out.append(sp)
                    doubled += 1
                if doubled:
                    self._log(
                        f"   Per-domain: {len(unique)} unique < target {target} — "
                        f"added {doubled} rare duplicate(s) (cap {_allowed_doubles}/run)"
                    )
                return out
            return unique

        _pre_cap = len(merged_sections)
        # 2026-06-03: 1 lead/domain (unique), rare top-up only if short of quota.
        merged_sections = _cap_per_domain(merged_sections, target=self.max_leads)
        _capped = _pre_cap - len(merged_sections)
        if _capped:
            self._log(
                f"   V5.34 Per-domain cap: dropped {_capped} duplicate-domain rows "
                f"(unique-domain policy; ≤1 dup per 20 leads, only if needed)"
            )

        # ── 2026-06-02: TIER-BASED paid-first ordering (the decisive cut) ──
        # This is the LAST reorder before the max_leads slice, so it dictates
        # WHICH leads make the final CSV. Rank by advertiser tier so the
        # heaviest TRUE Search advertisers (SerpAPI ads[] on multiple queries —
        # the ones that show SEMrush/Ahrefs paid>5) win the slots, ahead of
        # single-query Search ads, then ATC-only (Display/historical), then
        # everything else. Stable within a tier (preserves prior DM/score
        # order). Fixes: heavy advertisers found in discovery were being
        # out-ranked by organic/ATC domains that merely carried an Apollo
        # contact — so only 2-3/5 final leads actually had paid traffic.
        _pre_tier = list(merged_sections)
        merged_sections = sorted(
            merged_sections,
            key=lambda ld: (
                self._advertiser_tier(ld),
                ld.get("_dm_priority", 0),
                ld.get("lead_score", ld.get("_score", 0)),
            ),
            reverse=True,
        )
        _t = {}
        for _ld in merged_sections:
            _k = self._advertiser_tier(_ld)
            _t[_k] = _t.get(_k, 0) + 1
        self._log(
            f"   Paid-first ordering (tier): heavy-Search={_t.get(3,0)}, "
            f"Search={_t.get(2,0)}, ATC/other={_t.get(1,0)}, none={_t.get(0,0)} "
            f"— top {self.max_leads or 'all'} kept (heaviest Search advertisers first)"
        )

        quota_export_reserve = []
        if getattr(self, "paid_only_all", False):
            # PAID-ONLY: keep EVERY confirmed advertiser (tier>=1), drop unverified,
            # ignore the max_leads ceiling entirely.
            _before_po = len(merged_sections)
            self.leads = [ld for ld in merged_sections if self._advertiser_tier(ld) >= 1]
            self._log(
                f"   Phase 6 PAID-ONLY: exporting all {len(self.leads)} confirmed "
                f"advertisers (dropped {_before_po - len(self.leads)} unverified); no cap"
            )
        elif getattr(self, "quota_guarantee", False) and self.max_leads > 0:
            self.leads = merged_sections[:self.max_leads]
            quota_export_reserve = merged_sections[self.max_leads:]
        else:
            self.leads = merged_sections

        dm_top = len(top_section)
        dm_rest = len(rest_section)
        self._log(f"   V5.25 Domain Sort: {dm_top} top leads (role hierarchy, max {MAX_PER_DOMAIN}/domain) + {dm_rest} remaining")

        # Clean up internal scoring fields (keep lead_score for CSV)
        for lead in self.leads:
            lead.pop("_score", None)

        # ── Phase 2 (2026-05-05): Sector filter w/ 10% removal cap ──────────
        # Rules from product spec:
        #   1. ZERO digital-marketing companies in final CSV (hard exclusion).
        #   2. Allowed sectors (professional services, trade, manufacturing,
        #      retail, e-commerce) take priority — sorted on top.
        #   3. Other sectors (low_priority) are demoted to bottom; only
        #      kept if max_leads quota is not yet met by allowed leads.
        #   4. TOTAL removals (digital_marketing + low_priority cuts) must not
        #      exceed 10% of pre-filter lead count. If removing all DM
        #      companies would exceed the cap, the surplus stays at the
        #      bottom of the CSV (sorted lowest).
        def _classify_sector(_lead) -> str:
            ind = (_lead.get("industry") or "").lower()
            company = (_lead.get("company") or "").lower()
            domain = (_lead.get("domain") or "").lower()
            blob = f"{ind} {company} {domain}"
            DM_TOKENS = (
                "digital marketing", "marketing agency", "ad agency",
                "advertising agency", "seo agency", "ppc agency",
                "google ads agency", "performance marketing",
                "growth agency", "social media agency", "creative agency",
                "media buying agency", "lead generation agency",
                "marketing consultancy", "online marketing agency",
            )
            if any(t in blob for t in DM_TOKENS):
                return "digital_marketing"
            ALLOWED_TOKENS = (
                # Professional services
                "lawyer", "attorney", "solicitor", "accountant", "tax",
                "bookkeep", "doctor", "physician", "dentist", "vet",
                "consult", "engineer", "architect",
                # Trade services
                "plumber", "electrician", "carpent", "painter", "roofer",
                "builder", "concreter", "tiler", "landscap", "garden",
                "arborist", "lock", "pest control", "hvac", "heating",
                "cooling", "air condition", "solar", "handyman",
                "renovation", "construction", "fencing", "glazier",
                "stonemason", "bricklayer", "mechanic", "auto",
                # Manufacturing / industrial
                "manufactur", "factory", "industrial", "fabricat", "mill",
                # Retail / e-commerce
                "retail", "store", "shop", "supplier", "wholesaler",
                "distributor", "ecommerce", "e-commerce", "online",
                "boutique", "merchandise",
                # New AU multi-vertical (massage chairs / safety / bike / fashion)
                "massage chair", "safety equipment", "ppe", "bicycle",
                "bike", "designer clothing", "designer fashion",
            )
            if any(t in blob for t in ALLOWED_TOKENS):
                return "allowed"
            return "low_priority"

        try:
            _pre_filter = list(self.leads)
            _total = len(_pre_filter)
            if _total > 0:
                _allowed = []
                _low_priority = []
                _digital_mkt = []
                for ld in _pre_filter:
                    s = _classify_sector(ld)
                    ld["_sector_class"] = s
                    if s == "digital_marketing":
                        _digital_mkt.append(ld)
                    elif s == "low_priority":
                        _low_priority.append(ld)
                    else:
                        _allowed.append(ld)

                _cap = max(1, int(_total * 0.10))
                # Drop digital-marketing first up to cap; surplus stays at very bottom.
                _drop_count = min(len(_digital_mkt), _cap)
                _dropped_dm = _digital_mkt[:_drop_count]
                _kept_dm_overflow = _digital_mkt[_drop_count:]
                # Re-merge: allowed first, then low_priority, then surplus DM at very bottom.
                self.leads = _allowed + _low_priority + _kept_dm_overflow

                self._log(
                    f"   V5.33 Sector filter: {len(_allowed)} allowed + "
                    f"{len(_low_priority)} low-priority demoted + "
                    f"{len(_kept_dm_overflow)} digital-mkt kept (cap-bound); "
                    f"dropped {len(_dropped_dm)} digital-mkt (10% cap = {_cap})"
                )
                # Strip helper key
                for ld in self.leads:
                    ld.pop("_sector_class", None)
        except Exception as _se:
            self._log(f"   V5.33 Sector filter failed (keeping all leads): {_se}")

        # ── 2026-06-09: FINAL paid-first OUTPUT ordering (user request) ──────
        # After every filter, sort the output so CONFIRMED PAID advertisers sit
        # at the TOP of the CSV + frontend table. When you spot-check the first
        # rows you get paid-traffic leads first: heavy Search advertisers (the
        # ones Ahrefs/SEMrush reliably show paid>0) → single Search ads → ATC-
        # confirmed → unverified last. Stable sort: preserves the sector/DM/score
        # order within each tier (digital-marketing already excluded above).
        # Master-dedup below only REMOVES rows, so this order reaches the CSV.
        try:
            self.leads.sort(key=lambda ld: self._advertiser_tier(ld), reverse=True)
            _tt = {}
            for _ld in self.leads:
                _k = self._advertiser_tier(_ld)
                _tt[_k] = _tt.get(_k, 0) + 1
            self._log(
                f"   [Paid-first OUTPUT] rows ordered paid-first → "
                f"heavy-Search={_tt.get(3,0)}, Search={_tt.get(2,0)}, "
                f"ATC/other-paid={_tt.get(1,0)}, unverified={_tt.get(0,0)} "
                f"(top rows = strongest paid-traffic evidence)"
            )
        except Exception as _pfe:
            self._log(f"   [Paid-first OUTPUT] sort skipped: {_pfe}")

        # ── Phase 2: Master-DB dedup filter ─────────────────────────────────
        # Drop any lead whose (normalized_name, root_domain) is already present
        # in master_leads. Leaves the actual INSERT to the caller (wsgi.py) so
        # the transaction boundary includes run_history finalization.
        # Fails open: if the DB isn't configured/reachable, keep all leads and
        # log — never block export on a DB outage.
        master_existing_keys = set()
        try:
            import os as _os
            if _os.environ.get("SKIP_MASTER_DEDUP") != "1":
                from utils import normalize_master_key, root_domain
                from db import MasterLeadRepo, DBUnavailable
                pairs = []
                for ld in self.leads:
                    nn = normalize_master_key(ld.get("name") or "")
                    rd = root_domain(ld.get("domain") or "")
                    if nn and rd:
                        pairs.append((nn, rd))
                        ld["_master_key"] = (nn, rd)
                # Also build email+domain pairs for secondary dedup
                # (handles obfuscated Apollo names that change between runs)
                email_pairs = []
                for ld in self.leads:
                    em = (ld.get("email") or "").strip().lower()
                    rd2 = root_domain(ld.get("domain") or "")
                    if em and rd2:
                        email_pairs.append((em, rd2))
                        ld["_email_domain_key"] = (em, rd2)

                if pairs:
                    try:
                        existing = MasterLeadRepo.existing_keys(pairs)
                        # Secondary: email+domain lookup for obfuscated-name leads
                        if email_pairs:
                            try:
                                existing_by_email = MasterLeadRepo.existing_by_email_domain(email_pairs)
                            except Exception:
                                existing_by_email = set()
                        else:
                            existing_by_email = set()
                    except DBUnavailable as _dbe:
                        self._log(f"   Master dedup: DB unavailable — keeping all leads ({_dbe})")
                        existing = set()
                        existing_by_email = set()
                    except Exception as _dbe:
                        self._log(f"   Master dedup: query failed ({_dbe}) — keeping all leads")
                        existing = set()
                        existing_by_email = set()
                    if existing or existing_by_email:
                        kept = []
                        dropped = 0
                        for ld in self.leads:
                            mk = ld.get("_master_key")
                            ek = ld.get("_email_domain_key")
                            if (mk and mk in existing) or (ek and ek in existing_by_email):
                                dropped += 1
                                continue
                            kept.append(ld)
                        self._master_leads_deduped_out = dropped
                        self.leads = kept
                        self._log(f"   Master dedup: dropped {dropped} already-seen leads, "
                                  f"{len(self.leads)} fresh leads remain")
                    else:
                        self._master_leads_deduped_out = 0
                    master_existing_keys = existing or set()

                # ── HYBRID cross-run dedup (2026-05-28) ──────────────────
                # User-chosen policy: a company (domain) gets a LIFETIME cap
                # of 2 contacts across ALL runs. The lead-level dedup above
                # already removed exact (name, domain) repeats; this pass
                # additionally drops NEW contacts from a domain once that
                # domain has already yielded >= 2 leads in past runs — so a
                # heavily-covered company stops resurfacing, while a company
                # seen once can still contribute ONE more fresh contact.
                # Leads are already quality-sorted at this point, so the
                # contacts we keep are the best available.
                try:
                    if self.leads:
                        from db import MasterLeadRepo as _MLR_sat
                        _PER_DOMAIN_LIFETIME_CAP = 2
                        _run_domains = []
                        for ld in self.leads:
                            _rd = root_domain(ld.get("domain") or "")
                            if _rd:
                                _run_domains.append(_rd)
                        if _run_domains:
                            try:
                                _existing_counts = _MLR_sat.domain_counts(_run_domains)
                            except Exception:
                                _existing_counts = {}
                            if _existing_counts:
                                _kept_sat = []
                                _kept_per_dom: dict = {}
                                _sat_dropped = 0
                                for ld in self.leads:
                                    _rd = root_domain(ld.get("domain") or "")
                                    if not _rd:
                                        _kept_sat.append(ld)
                                        continue
                                    _already = int(_existing_counts.get(_rd, 0))
                                    _this_run = _kept_per_dom.get(_rd, 0)
                                    if _already + _this_run >= _PER_DOMAIN_LIFETIME_CAP:
                                        _sat_dropped += 1
                                        continue
                                    _kept_sat.append(ld)
                                    _kept_per_dom[_rd] = _this_run + 1
                                if _sat_dropped:
                                    self._master_leads_deduped_out = (
                                        int(getattr(self, "_master_leads_deduped_out", 0) or 0)
                                        + _sat_dropped
                                    )
                                    self.leads = _kept_sat
                                    self._log(
                                        f"   Master dedup (hybrid): dropped {_sat_dropped} "
                                        f"lead(s) from companies already at the "
                                        f"{_PER_DOMAIN_LIFETIME_CAP}-contact lifetime cap; "
                                        f"{len(self.leads)} remain"
                                    )
                except DBUnavailable as _sat_dbe:
                    self._log(f"   Master dedup (hybrid): DB unavailable — skipped ({_sat_dbe})")
                except Exception as _sat_e:
                    self._log(f"   Master dedup (hybrid): skipped ({_sat_e})")
        except ImportError:
            # db/utils unavailable — skip silently
            pass
        finally:
            # Strip helper keys before CSV write
            for ld in self.leads:
                ld.pop("_master_key", None)
                ld.pop("_email_domain_key", None)

        if (getattr(self, "quota_guarantee", False)
                and self.max_leads > 0
                and len(self.leads) < self.max_leads
                and quota_export_reserve):
            seen_export_keys = set()
            for ld in self.leads:
                domain = (ld.get("domain") or "").lower()
                email = (ld.get("email") or "").lower()
                name = (ld.get("name") or "").lower()
                role = (ld.get("role") or "").lower()
                seen_export_keys.add(f"{domain}|{email}" if email else f"{domain}|{name}|{role}")
            added = 0
            for ld in quota_export_reserve:
                if len(self.leads) >= self.max_leads:
                    break
                mk = ld.get("_master_key")
                if mk and mk in master_existing_keys:
                    continue
                domain = (ld.get("domain") or "").lower()
                email = (ld.get("email") or "").lower()
                name = (ld.get("name") or "").lower()
                role = (ld.get("role") or "").lower()
                export_key = f"{domain}|{email}" if email else f"{domain}|{name}|{role}"
                if export_key in seen_export_keys:
                    continue
                seen_export_keys.add(export_key)
                ld.pop("_master_key", None)
                ld.setdefault("_quota_fill_tier", "post_dedup_reserve")
                ld.setdefault(
                    "_quota_fill_reason",
                    "Reserve row added after master dedup to preserve requested quota",
                )
                self.leads.append(ld)
                added += 1
            if added:
                self._log(
                    f"   Quota guarantee: restored {added} reserve lead(s) after master dedup "
                    f"({len(self.leads)}/{self.max_leads})"
                )

        # 2026-05-28: REMOVED the quota row-duplication. It used to copy
        # already-qualified rows verbatim to pad the CSV to the exact
        # max_leads count — which is exactly what made the SAME domain/lead
        # appear 2-3× identically. Per explicit user policy: never duplicate
        # rows, return fewer unique leads instead.
        if (getattr(self, "quota_guarantee", False)
                and self.max_leads > 0
                and len(self.leads) < self.max_leads):
            self._log(
                f"   Quota: {len(self.leads)}/{self.max_leads} unique leads — "
                f"NOT padding with duplicates (one lead per domain policy)."
            )

        # ── 2026-05-28: FINAL HARD per-domain cap (absolute invariant) ──
        # No domain may appear more than 2× in a run, regardless of max_leads.
        # Prefer 1 per domain — keep the first (highest-ranked) occurrence,
        # allow a 2nd only if it's a DISTINCT lead (different name/email),
        # drop everything beyond. This runs AFTER all reserve/quota steps so
        # nothing upstream can violate it.
        _MAX_PER_DOMAIN_HARD = 2
        _dom_kept: dict = {}
        _dom_seen_identity: dict = {}
        _final_leads = []
        _dropped_dupe = 0
        for ld in self.leads:
            d = (ld.get("domain") or "").strip().lower()
            if not d:
                _final_leads.append(ld)
                continue
            ident = (
                (ld.get("email") or "").strip().lower()
                or f"{(ld.get('name') or '').strip().lower()}|{(ld.get('role') or '').strip().lower()}"
            )
            seen_ids = _dom_seen_identity.setdefault(d, set())
            # Drop exact-identity repeats (the duplication symptom) outright.
            if ident and ident in seen_ids:
                _dropped_dupe += 1
                continue
            if _dom_kept.get(d, 0) >= _MAX_PER_DOMAIN_HARD:
                _dropped_dupe += 1
                continue
            _final_leads.append(ld)
            _dom_kept[d] = _dom_kept.get(d, 0) + 1
            if ident:
                seen_ids.add(ident)
        if _dropped_dupe:
            self._log(
                f"   Per-domain hard cap: dropped {_dropped_dupe} row(s) so no "
                f"domain exceeds {_MAX_PER_DOMAIN_HARD}× (prefer 1/domain). "
                f"Final: {len(_final_leads)} leads, "
                f"{len(set((l.get('domain') or '').lower() for l in _final_leads if l.get('domain')))} unique domains."
            )
        self.leads = _final_leads

        os.makedirs(self.output_folder, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        industry_slug = re.sub(r"[^\w]+", "_", self.industry.lower()).strip("_")
        # 2026-05-28: ATC advertiser-page URL builder for the proof column.
        try:
            from google_ads_transparency import advertiser_url as _atc_url_fn
        except Exception:
            _atc_url_fn = lambda aid, country="AU": ""

        fieldnames = ["Name", "Company Name", "Domain", "Role", "Phone Number",
                      "Email", "Inferred Email", "All Emails", "Email Type", "Traffic Source",
                      # 2026-06-03: discovery keyword that surfaced this lead.
                      "Keyword",
                      # 2026-05-28: Google Ads Transparency Center proof — Google's
                      # OWN evidence the business runs ads (catches advertisers
                      # SEMrush/Ahrefs miss). Open the URL to see their live ads.
                      "ATC Verified", "ATC Advertiser ID", "ATC Ad Library URL",
                      "Founder Verified", "LinkedIn URL", "Paid KW", "Organic KW",
                      "Run Timestamp", "Notes", "Quota Fill Tier", "Quota Fill Reason",
                      # V5.27: New business-context columns
                      "prospect_name", "prospect_email", "prospect_website",
                      "business_niche", "phone number", "company_name",
                      "call_context", "callback_notes",
                      "keyword_1", "volume_1", "position_1", "url_1",
                      "keyword_2", "volume_2", "position_2", "url_2",
                      "competitor_1", "competitor_2",
                      "Revenue", "Company LinkedIn URL", "revenue",
                      "organic_traffic", "paid_traffic",
                      "organic_keywords", "paid_keywords"]

        def _write_csv(filepath, leads_subset):
            with open(filepath, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for lead in leads_subset:
                    notes_parts = []
                    if lead.get("source"):
                        notes_parts.append(f"Source: {lead['source']}")
                    # Determine email type for display — V5.14 four-tier classification
                    # Personal = gmail/yahoo/icloud (is_personal_email) OR verified by Apollo OR
                    #            any word from the lead's name appears in the email local part
                    email_type = ""
                    raw_email = lead.get("email", "")
                    if raw_email:
                        if lead.get("_email_inferred"):
                            email_type = "Inferred"
                        elif is_personal_email(raw_email) or lead.get("_email_verified"):
                            email_type = "Personal"
                        else:
                            # V5.14: Check if any name word appears in email local part
                            _name_for_type = lead.get("name", "")
                            _local = raw_email.lower().split("@")[0]
                            _clean_local = _local.replace(".", "").replace("-", "").replace("_", "")
                            _name_words = [w.lower() for w in _name_for_type.split() if len(w) >= 2]
                            if _name_words and any(nw in _clean_local or nw in _local for nw in _name_words):
                                email_type = "Personal"
                            elif is_work_email(raw_email):
                                email_type = "Work"
                            else:
                                email_type = "Generic"
                    # V5.13: Inferred email — generate candidates when no verified email
                    inferred_email = ""
                    lead_name = lead.get("name", "")
                    lead_domain = lead.get("domain", "")
                    if not raw_email and lead_name and " " in lead_name and lead_domain:
                        parts = lead_name.split()
                        candidates = generate_email_candidates(parts[0], parts[-1], lead_domain)
                        if candidates:
                            inferred_email = candidates[0]
                    # V5.13: All emails — collect every email source
                    all_emails_list = []
                    if raw_email:
                        all_emails_list.append(raw_email)
                    if inferred_email and inferred_email not in all_emails_list:
                        all_emails_list.append(inferred_email)
                    generic_email = lead.get("_generic_email", "")
                    if generic_email and generic_email not in all_emails_list:
                        all_emails_list.append(generic_email)
                    # V5.27: prospect website — prefer full https URL from domain
                    _lead_domain = lead.get("domain", "")
                    _prospect_website = f"https://{_lead_domain}" if _lead_domain else ""
                    # V5.27: Revenue — "NA" if not available
                    _revenue_val = lead.get("_revenue", "") or "NA"
                    _linkedin_val = (
                        lead.get("_linkedin_url", "")
                        or lead.get("_company_linkedin_url", "")
                    )
                    # PHASE 2: when enrichment toggle is OFF, blank phone/email
                    # columns — we're only returning name/company/domain/role.
                    _out_phone = lead.get("phone", "") if self.enrichment_enabled else ""
                    _out_email = raw_email if self.enrichment_enabled else ""
                    _out_inferred = inferred_email if self.enrichment_enabled else ""
                    _out_all_emails = " | ".join(all_emails_list) if self.enrichment_enabled else ""
                    _out_email_type = email_type if self.enrichment_enabled else ""
                    row = {
                        "Name": lead.get("name", ""),
                        "Company Name": lead.get("company", ""),
                        "Domain": lead.get("domain", ""),
                        "Role": lead.get("role", ""),
                        "Phone Number": _out_phone,
                        "Email": _out_email,
                        "Inferred Email": _out_inferred,
                        "All Emails": _out_all_emails,
                        "Email Type": _out_email_type,
                        "Traffic Source": self._traffic_source_label(lead),
                        "Keyword": lead.get("_discovery_keyword", "") or "",
                        # 2026-05-28: ATC proof columns — Google's own record that
                        # this business runs ads (more current + complete than the
                        # SEMrush/Ahrefs estimate, which misses small AU advertisers).
                        "ATC Verified": "Yes" if self._atc_advertiser_ids.get((lead.get("domain") or "").lower()) else "",
                        "ATC Advertiser ID": self._atc_advertiser_ids.get((lead.get("domain") or "").lower(), ""),
                        "ATC Ad Library URL": (
                            _atc_url_fn(self._atc_advertiser_ids.get((lead.get("domain") or "").lower(), ""), self.country)
                            if self._atc_advertiser_ids.get((lead.get("domain") or "").lower()) else ""
                        ),
                        "Founder Verified": "Yes" if lead.get("_founder_verified") else "",
                        "LinkedIn URL": _linkedin_val,
                        "Paid KW": lead.get("_paid_keywords", ""),
                        "Organic KW": lead.get("_organic_keywords", ""),
                        "Run Timestamp": timestamp,
                        "Notes": " | ".join(notes_parts),
                        "Quota Fill Tier": lead.get("_quota_fill_tier", ""),
                        "Quota Fill Reason": lead.get("_quota_fill_reason", ""),
                        # V5.27: New business-context columns
                        "prospect_name": lead.get("name", ""),
                        "prospect_email": _out_email,
                        "prospect_website": _prospect_website,
                        "business_niche": lead.get("_business_niche", "") or "NA",
                        "phone number": _out_phone,
                        "company_name": lead.get("company", ""),
                        "call_context": lead.get("_call_context", "") or "fresh",
                        "callback_notes": lead.get("_callback_notes", ""),
                        "keyword_1": lead.get("_kw1_keyword", ""),
                        "volume_1": lead.get("_kw1_volume", ""),
                        "position_1": lead.get("_kw1_position", ""),
                        "url_1": lead.get("_kw1_url", ""),
                        "keyword_2": lead.get("_kw2_keyword", ""),
                        "volume_2": lead.get("_kw2_volume", ""),
                        "position_2": lead.get("_kw2_position", ""),
                        "url_2": lead.get("_kw2_url", ""),
                        "competitor_1": lead.get("_competitor_1", ""),
                        "competitor_2": lead.get("_competitor_2", ""),
                        "Revenue": _revenue_val,
                        "Company LinkedIn URL": lead.get("_company_linkedin_url", ""),
                        "revenue": _revenue_val,
                        "organic_traffic": lead.get("_organic_traffic", 0) or 0,
                        "paid_traffic": lead.get("_paid_traffic", 0) or 0,
                        "organic_keywords": lead.get("_organic_keywords", 0) or 0,
                        "paid_keywords": lead.get("_paid_keywords", 0) or 0,
                    }
                    writer.writerow(row)

        # CSV 1: ALL leads
        all_filename = f"leads_ALL_{industry_slug}_{self.country}_{timestamp}.csv"
        all_filepath = os.path.join(self.output_folder, all_filename)
        _write_csv(all_filepath, self.leads)
        self._log(f"   Saved ALL {len(self.leads)} leads to: {all_filepath}")

        # CSV 2: TOP leads — V5.17: guarantee name+phone+email in top rows
        # Priority tiers (user requirement: name+phone is absolute must, email+role ideally):
        #   T1: full name + phone + email + role  (perfect lead)
        #   T2: full name + phone + email          (no role)
        #   T3: full name + phone + role           (no email)
        #   T4: full name + phone                  (minimum acceptable)
        #   T5: partial name + phone               (fallback)
        #   T6: phone only / email only / name only (last resort)
        if self.max_leads > 0:
            def _has_full_name(ld):
                return bool(ld.get("name")) and " " in ld.get("name", "")
            _t1 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and ld.get("email") and ld.get("role")]
            _t2 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and ld.get("email") and not ld.get("role")]
            _t3 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and not ld.get("email") and ld.get("role")]
            _t4 = [ld for ld in self.leads if _has_full_name(ld) and ld.get("phone") and not ld.get("email") and not ld.get("role")]
            _t5 = [ld for ld in self.leads if not _has_full_name(ld) and ld.get("phone")]
            _t6 = [ld for ld in self.leads if not ld.get("phone")]
            _ordered = (_t1 + _t2 + _t3 + _t4 + _t5 + _t6)
            # PAID-ONLY: export ALL confirmed leads (no max_leads slice).
            if getattr(self, "paid_only_all", False):
                top_leads = _ordered
                top_filename = f"leads_TOP_{len(top_leads)}_{industry_slug}_{self.country}_{timestamp}.csv"
            else:
                top_leads = _ordered[:self.max_leads]
                top_filename = f"leads_TOP_{self.max_leads}_{industry_slug}_{self.country}_{timestamp}.csv"
        else:
            top_leads = self.leads
            top_filename = f"leads_TOP_all_{industry_slug}_{self.country}_{timestamp}.csv"
        top_filepath = os.path.join(self.output_folder, top_filename)
        _write_csv(top_filepath, top_leads)
        # V5.17: Log completeness of top leads (user requirement: name+phone minimum)
        top_with_phone = sum(1 for ld in top_leads if ld.get("phone"))
        top_with_email = sum(1 for ld in top_leads if ld.get("email"))
        top_complete = sum(1 for ld in top_leads if ld.get("phone") and ld.get("email") and " " in ld.get("name",""))
        self._log(f"   Saved TOP {len(top_leads)} leads → {top_with_phone} with phone, "
                  f"{top_with_email} with email, {top_complete} fully complete (name+phone+email)")

        # V5.17: COMPREHENSIVE TOKEN & LEAD SUMMARY
        self._log("\n" + "="*80)
        self._log("V5.17 RUN SUMMARY — TOKEN USAGE & LEAD STATISTICS")
        self._log("="*80)

        # Lead source breakdown (Paid vs Organic)
        paid_leads = sum(1 for ld in self.leads if ld.get("_domain_source") == "paid")
        organic_leads = sum(1 for ld in self.leads if ld.get("_domain_source") == "organic")
        self._log(f"\nLead Sources:")
        self._log(f"  • PAID (Google Ads):     {paid_leads} leads")
        self._log(f"  • ORGANIC:               {organic_leads} leads")
        self._log(f"  • TOTAL:                 {len(self.leads)} leads")

        # Contact data coverage
        with_phone = sum(1 for ld in self.leads if ld.get("phone"))
        with_email = sum(1 for ld in self.leads if ld.get("email"))
        personal_emails = sum(1 for ld in self.leads if ld.get("email") and is_personal_email(ld.get("email", "")))
        work_emails = sum(1 for ld in self.leads if ld.get("email") and is_work_email(ld.get("email", "")))
        generic_emails = with_email - personal_emails - work_emails
        self._log(f"\nContact Data Coverage:")
        self._log(f"  • With Phone:            {with_phone} leads")
        self._log(f"  • With Email:            {with_email} leads")
        self._log(f"    - Personal (Gmail/Yahoo): {personal_emails} leads")
        self._log(f"    - Work (firstname@co):    {work_emails} leads")
        self._log(f"    - Generic (info@):        {generic_emails} leads")

        # API token usage this run
        self._log(f"\nAPI Token Usage This Run:")
        self._log(f"  • SEMrush:               {self._api_counter.get('semrush', 0)} API calls")
        self._log(f"  • Apollo:                {self._api_counter.get('apollo', 0)} API calls (budget: {self._apollo_budget})")
        # V5.32: Apollo credits consumed this run (delta from Apollo's own auth/health API)
        _apollo_end = self._fetch_apollo_credits_remaining()
        if self._apollo_credits_at_start >= 0 and _apollo_end >= 0:
            _delta = self._apollo_credits_at_start - _apollo_end
            self._log(f"    - Apollo credits consumed this run: {_delta} "
                      f"(start: {self._apollo_credits_at_start}, end: {_apollo_end})")
        elif _apollo_end >= 0:
            self._log(f"    - Apollo credits remaining: {_apollo_end}")
        self._log(f"  • Lusha:                 {self._api_counter.get('lusha', 0)} API calls")
        self._log(f"  • SerpApi:               {self._api_counter.get('serpapi', 0)} API calls")
        self._log(f"  • OpenAI:                {self._api_counter.get('openai', 0)} API calls")

        # Enrichment efficiency
        leads_enriched = self._api_counter.get('apollo', 0) + self._api_counter.get('lusha', 0)
        leads_with_phone_enrichment = sum(1 for ld in self.leads if ld.get("_direct_phone"))
        leads_with_email_enrichment = sum(1 for ld in self.leads if ld.get("_email_verified"))
        self._log(f"\nEnrichment Efficiency (for top {self.max_leads or len(self.leads)} leads):")
        self._log(f"  • Leads with direct phone (API enriched): {leads_with_phone_enrichment}")
        self._log(f"  • Leads with verified email (API enriched): {leads_with_email_enrichment}")
        self._log(f"  • Total enrichment API calls:         {leads_enriched}")

        # V5.13: Credit cost summary
        total_credits = sum(
            self._api_counter.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
            for svc in API_CREDIT_COSTS
        )
        founders_verified = sum(1 for ld in self.leads if ld.get("_founder_verified"))
        scraped_emails = sum(1 for ld in self.leads if "Scrape" in (ld.get("source") or "") and ld.get("email"))
        avg_score = sum(ld.get("lead_score", 0) for ld in self.leads) / max(len(self.leads), 1)

        self._log(f"\nV5.13 Summary:")
        self._log(f"  • Leads generated:          {len(self.leads)}")
        self._log(f"  • Average lead score:       {avg_score:.1f}")
        self._log(f"  • Total credits used:       {total_credits:.0f}")
        self._log(f"  • Founders verified (WHOIS):{founders_verified}")
        self._log(f"  • Emails found via website: {scraped_emails}")
        self._log(f"  • Competitor domains added:  {self._competitor_domains_added}")
        self._log("\n" + "="*80)

        self._progress(100, f"Done! {len(top_leads)} top leads + {len(self.leads)} total exported")
        return top_filepath


# ══════════════════════════════════════════════════════════════════════════════
# GUI APPLICATION — Dark Theme
# ══════════════════════════════════════════════════════════════════════════════

# Lazy-load tkinter — only imported when GUI is actually started (main())
# This allows the pipeline/API classes above to be imported without tkinter
tk = None
ttk = None
filedialog = None
messagebox = None

# Color palette
COLORS = {
    "bg_dark": "#1a1a2e",
    "bg_medium": "#16213e",
    "bg_light": "#0f3460",
    "bg_card": "#1f2b47",
    "accent": "#e94560",
    "accent_hover": "#ff6b81",
    "text_primary": "#eaeaea",
    "text_secondary": "#a0a0b0",
    "text_muted": "#6c6c80",
    "success": "#2ecc71",
    "warning": "#f39c12",
    "error": "#e74c3c",
    "input_bg": "#0d1b2a",
    "input_border": "#1b2838",
    "button_bg": "#e94560",
    "button_fg": "#ffffff",
    "progress_bg": "#0d1b2a",
    "progress_fg": "#e94560",
    "log_bg": "#0a0f1a",
}


class LeadGeneratorApp:
    """Main GUI application with dark theme."""

    def __init__(self, root):
        self.root = root
        self.root.title("Lead Generation Pro V5.4")
        self.root.geometry("820x740")
        self.root.minsize(780, 700)
        self.root.configure(bg=COLORS["bg_dark"])

        self.pipeline = None
        self.pipeline_thread = None

        self._build_ui()
        self._center_window()
        # Show API key status check after window is visible
        self.root.after(500, self._show_api_status_window)

    def _center_window(self):
        self.root.update_idletasks()
        w = self.root.winfo_width()
        h = self.root.winfo_height()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = (sw - w) // 2
        y = (sh - h) // 2
        self.root.geometry(f"+{x}+{y}")

    def _show_api_status_window(self):
        """Check all API keys in background and show a compact status popup."""
        win = tk.Toplevel(self.root)
        win.title("API Key Status")
        win.configure(bg=COLORS["bg_dark"])
        win.resizable(False, False)

        tk.Label(win, text="API Key Status", bg=COLORS["bg_dark"], fg=COLORS["accent"],
                 font=("Segoe UI", 12, "bold")).pack(anchor=tk.W, padx=18, pady=(12, 6))

        # Build status rows
        status_labels = {}
        keys_to_check = [
            ("apollo",   "Apollo"),
            ("lusha",    "Lusha"),
            ("semrush",  "SEMrush"),
            ("serpapi",  "SerpApi"),
            ("openai",   "OpenAI"),
        ]
        for key_id, display_name in keys_to_check:
            fr = tk.Frame(win, bg=COLORS["bg_dark"])
            fr.pack(fill=tk.X, padx=24, pady=2)
            tk.Label(fr, text=display_name, bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
                     font=("Segoe UI", 9), width=12, anchor=tk.W).pack(side=tk.LEFT)
            lbl = tk.Label(fr, text="Checking…", bg=COLORS["bg_dark"],
                           fg=COLORS["text_muted"], font=("Segoe UI", 9, "bold"))
            lbl.pack(side=tk.LEFT)
            status_labels[key_id] = lbl

        close_btn = tk.Button(
            win, text="  OK  ",
            bg=COLORS["accent"], fg=COLORS["button_fg"],
            activebackground=COLORS["accent_hover"],
            font=("Segoe UI", 9, "bold"), relief=tk.FLAT, padx=16, pady=4,
            command=win.destroy,
        )
        close_btn.pack(pady=(8, 14))

        # Centre
        win.update_idletasks()
        mw = self.root.winfo_width(); mx = self.root.winfo_x()
        mh = self.root.winfo_height(); my = self.root.winfo_y()
        ww = win.winfo_width(); wh = win.winfo_height()
        win.geometry(f"+{mx + (mw - ww)//2}+{my + (mh - wh)//2}")

        def _check_keys():
            """Run key checks in background thread; update labels via after()."""
            results = {}

            # Apollo
            try:
                key = API_KEYS.get("apollo", "")
                if not key:
                    results["apollo"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://api.apollo.io/api/v1/auth/health",
                        headers={"X-Api-Key": key}, timeout=10,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        credits = data.get("data", {}).get("credits_used_this_month", "?")
                        results["apollo"] = (f"OK  (used: {credits})", True)
                    else:
                        results["apollo"] = (f"Error {r.status_code}", False)
            except Exception as e:
                results["apollo"] = (f"Unreachable", False)

            # Lusha
            try:
                key = API_KEYS.get("lusha", "")
                if not key:
                    results["lusha"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://api.lusha.com/account",
                        headers={"api_key": key}, timeout=10,
                    )
                    if r.status_code == 200:
                        results["lusha"] = ("OK", True)
                    else:
                        results["lusha"] = (f"Error {r.status_code}", False)
            except Exception:
                results["lusha"] = ("Unreachable", False)

            # SEMrush
            try:
                key = API_KEYS.get("semrush", "")
                if not key:
                    results["semrush"] = ("No key", False)
                else:
                    r = requests.get(
                        f"https://api.semrush.com/?type=phrase_this&key={key}&phrase=test&export_columns=Ph,Nq&database=au",
                        timeout=10,
                    )
                    if r.status_code == 200 and "ERROR" not in r.text[:30]:
                        results["semrush"] = ("OK", True)
                    elif "CREDITS" in r.text.upper() or "limit" in r.text.lower():
                        results["semrush"] = ("No credits", False)
                    else:
                        results["semrush"] = (f"Error {r.status_code}", False)
            except Exception:
                results["semrush"] = ("Unreachable", False)

            # SerpApi
            try:
                key = API_KEYS.get("serpapi", "")
                if not key:
                    results["serpapi"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://serpapi.com/account",
                        params={"api_key": key}, timeout=10,
                    )
                    if r.status_code == 200:
                        data = r.json()
                        remaining = data.get("total_searches_left", "?")
                        results["serpapi"] = (f"OK  ({remaining} left)", True)
                    else:
                        results["serpapi"] = (f"Error {r.status_code}", False)
            except Exception:
                results["serpapi"] = ("Unreachable", False)

            # OpenAI
            try:
                key = API_KEYS.get("openai", "")
                if not key:
                    results["openai"] = ("No key", False)
                else:
                    r = requests.get(
                        "https://api.openai.com/v1/models",
                        headers={"Authorization": f"Bearer {key}"}, timeout=10,
                    )
                    if r.status_code == 200:
                        results["openai"] = ("OK", True)
                    elif r.status_code == 401:
                        results["openai"] = ("Invalid key", False)
                    else:
                        results["openai"] = (f"Error {r.status_code}", False)
            except Exception:
                results["openai"] = ("Unreachable", False)

            # Update GUI from main thread
            def _apply():
                if not win.winfo_exists():
                    return
                for key_id, (msg, ok) in results.items():
                    lbl = status_labels.get(key_id)
                    if lbl:
                        lbl.configure(
                            text=("✓ " if ok else "✗ ") + msg,
                            fg=COLORS["success"] if ok else COLORS["error"],
                        )
            win.after(0, _apply)

        import threading as _threading
        _threading.Thread(target=_check_keys, daemon=True).start()

    def _build_ui(self):
        style = ttk.Style()
        style.theme_use("clam")

        style.configure("Dark.TFrame", background=COLORS["bg_dark"])
        style.configure("Card.TFrame", background=COLORS["bg_card"])
        style.configure(
            "Dark.TLabel", background=COLORS["bg_dark"],
            foreground=COLORS["text_primary"], font=("Segoe UI", 10),
        )
        style.configure(
            "CardLabel.TLabel", background=COLORS["bg_card"],
            foreground=COLORS["text_primary"], font=("Segoe UI", 10),
        )
        style.configure(
            "Header.TLabel", background=COLORS["bg_dark"],
            foreground=COLORS["accent"], font=("Segoe UI", 22, "bold"),
        )
        style.configure(
            "SubHeader.TLabel", background=COLORS["bg_dark"],
            foreground=COLORS["text_secondary"], font=("Segoe UI", 10),
        )
        style.configure(
            "Dark.TCombobox",
            fieldbackground=COLORS["input_bg"], background=COLORS["bg_light"],
            foreground=COLORS["text_primary"],
            selectbackground=COLORS["accent"], selectforeground=COLORS["button_fg"],
        )
        style.configure(
            "Dark.Horizontal.TProgressbar",
            troughcolor=COLORS["progress_bg"], background=COLORS["progress_fg"], thickness=8,
        )

        # Main container
        main = ttk.Frame(self.root, style="Dark.TFrame", padding=20)
        main.pack(fill=tk.BOTH, expand=True)

        # Header
        ttk.Label(main, text="Lead Generation Pro", style="Header.TLabel").pack(anchor=tk.W, pady=(0, 2))
        ttk.Label(main, text="Discover & enrich B2B leads automatically", style="SubHeader.TLabel").pack(
            anchor=tk.W, pady=(0, 15)
        )

        # Input Card
        card = ttk.Frame(main, style="Card.TFrame", padding=15)
        card.pack(fill=tk.X, pady=(0, 12))

        # Row 1: Industry + Country
        row1 = ttk.Frame(card, style="Card.TFrame")
        row1.pack(fill=tk.X, pady=(0, 10))

        ind_frame = ttk.Frame(row1, style="Card.TFrame")
        ind_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        ttk.Label(ind_frame, text="Industry", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.industry_var = tk.StringVar(value="Dentist")
        self.industry_combo = ttk.Combobox(
            ind_frame, textvariable=self.industry_var,
            values=sorted(INDUSTRY_KEYWORDS.keys()),
            style="Dark.TCombobox", state="normal", font=("Segoe UI", 10),
        )
        self.industry_combo.pack(fill=tk.X, pady=(3, 0))

        country_frame = ttk.Frame(row1, style="Card.TFrame")
        country_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(country_frame, text="Country", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.country_var = tk.StringVar(value="AU")
        self.country_combo = ttk.Combobox(
            country_frame, textvariable=self.country_var,
            values=["AU", "USA", "UK", "India"],
            style="Dark.TCombobox", state="readonly", font=("Segoe UI", 10),
        )
        self.country_combo.pack(fill=tk.X, pady=(3, 0))

        # Row 2: Volume + CPC
        row2 = ttk.Frame(card, style="Card.TFrame")
        row2.pack(fill=tk.X, pady=(0, 10))

        vol_frame = ttk.Frame(row2, style="Card.TFrame")
        vol_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        ttk.Label(vol_frame, text="Min Search Volume", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.volume_var = tk.StringVar(value="50")
        tk.Entry(
            vol_frame, textvariable=self.volume_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(fill=tk.X, pady=(3, 0))

        cpc_frame = ttk.Frame(row2, style="Card.TFrame")
        cpc_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 10))
        ttk.Label(cpc_frame, text="Min CPC ($)", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.cpc_var = tk.StringVar(value="0.05")  # Phase 2: lowered from 1.0 per user spec
        tk.Entry(
            cpc_frame, textvariable=self.cpc_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(fill=tk.X, pady=(3, 0))

        max_leads_frame = ttk.Frame(row2, style="Card.TFrame")
        max_leads_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
        ttk.Label(max_leads_frame, text="Max Leads (0=all)", style="CardLabel.TLabel").pack(anchor=tk.W)
        self.max_leads_var = tk.StringVar(value="50")
        tk.Entry(
            max_leads_frame, textvariable=self.max_leads_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(fill=tk.X, pady=(3, 0))

        # Row 3: Output folder
        row3 = ttk.Frame(card, style="Card.TFrame")
        row3.pack(fill=tk.X)
        ttk.Label(row3, text="Output Folder", style="CardLabel.TLabel").pack(anchor=tk.W)
        folder_row = ttk.Frame(row3, style="Card.TFrame")
        folder_row.pack(fill=tk.X, pady=(3, 0))

        self.folder_var = tk.StringVar(value=self._default_output_folder())
        tk.Entry(
            folder_row, textvariable=self.folder_var,
            bg=COLORS["input_bg"], fg=COLORS["text_primary"],
            insertbackground=COLORS["text_primary"], font=("Segoe UI", 10),
            relief=tk.FLAT, bd=5,
        ).pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        tk.Button(
            folder_row, text="Browse", bg=COLORS["bg_light"], fg=COLORS["text_primary"],
            font=("Segoe UI", 9), relief=tk.FLAT, padx=10, pady=4,
            command=self._browse_folder,
        ).pack(side=tk.RIGHT)

        # Action Buttons
        btn_frame = ttk.Frame(main, style="Dark.TFrame")
        btn_frame.pack(fill=tk.X, pady=(0, 12))

        self.generate_btn = tk.Button(
            btn_frame, text="   Generate Leads   ",
            bg=COLORS["accent"], fg=COLORS["button_fg"],
            activebackground=COLORS["accent_hover"], activeforeground=COLORS["button_fg"],
            font=("Segoe UI", 12, "bold"), relief=tk.FLAT, padx=25, pady=8,
            cursor="hand2", command=self._on_generate,
        )
        self.generate_btn.pack(side=tk.LEFT)

        self.cancel_btn = tk.Button(
            btn_frame, text="  Cancel  ",
            bg=COLORS["bg_light"], fg=COLORS["text_primary"],
            activebackground=COLORS["bg_medium"],
            font=("Segoe UI", 10), relief=tk.FLAT, padx=15, pady=6,
            state=tk.DISABLED, command=self._on_cancel,
        )
        self.cancel_btn.pack(side=tk.LEFT, padx=(10, 0))

        self.status_var = tk.StringVar(value="Ready")
        tk.Label(
            btn_frame, textvariable=self.status_var,
            bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
            font=("Segoe UI", 10), anchor=tk.E,
        ).pack(side=tk.RIGHT)

        # Progress Bar
        self.progress_var = tk.DoubleVar(value=0)
        ttk.Progressbar(
            main, variable=self.progress_var, maximum=100, mode="determinate",
            style="Dark.Horizontal.TProgressbar",
        ).pack(fill=tk.X, pady=(0, 12))

        # Run Statistics Box (V5.10+)
        stats_card = tk.Frame(main, bg=COLORS["bg_card"], padx=10, pady=6)
        stats_card.pack(fill=tk.X, pady=(0, 8))
        tk.Label(
            stats_card, text="Run Statistics",
            bg=COLORS["bg_card"], fg=COLORS["text_secondary"],
            font=("Segoe UI", 9, "bold"),
        ).pack(anchor=tk.W)
        # Row 1: Lead source breakdown
        row1 = tk.Frame(stats_card, bg=COLORS["bg_card"])
        row1.pack(fill=tk.X, pady=(3, 0))
        self.stat_total_var = tk.StringVar(value="Total: —")
        self.stat_paid_var = tk.StringVar(value="Paid Ads: —")
        self.stat_organic_var = tk.StringVar(value="Organic: —")
        self.stat_phone_var = tk.StringVar(value="w/ Phone: —")
        self.stat_email_var = tk.StringVar(value="Personal Email: —")
        for var, fg in [
            (self.stat_total_var, COLORS["text_primary"]),
            (self.stat_paid_var, COLORS["success"]),
            (self.stat_organic_var, COLORS["warning"]),
            (self.stat_phone_var, COLORS["accent"]),
            (self.stat_email_var, COLORS["text_secondary"]),
        ]:
            tk.Label(row1, textvariable=var, bg=COLORS["bg_card"], fg=fg,
                     font=("Segoe UI", 9), padx=8).pack(side=tk.LEFT)
        # Row 2: API call and credit counters
        row2 = tk.Frame(stats_card, bg=COLORS["bg_card"])
        row2.pack(fill=tk.X, pady=(2, 0))
        self.stat_apollo_var = tk.StringVar(value="Apollo: 0")
        self.stat_lusha_var = tk.StringVar(value="Lusha: 0")
        self.stat_semrush_var = tk.StringVar(value="SEMrush: 0")
        self.stat_phone_cr_var = tk.StringVar(value="Phone Credits: 0")
        self.stat_email_cr_var = tk.StringVar(value="Email Credits: 0")
        for var in [self.stat_apollo_var, self.stat_lusha_var, self.stat_semrush_var,
                    self.stat_phone_cr_var, self.stat_email_cr_var]:
            tk.Label(row2, textvariable=var, bg=COLORS["bg_card"], fg=COLORS["text_muted"],
                     font=("Segoe UI", 8), padx=8).pack(side=tk.LEFT)

        # Log Panel
        ttk.Label(main, text="Activity Log", style="Dark.TLabel").pack(anchor=tk.W, pady=(0, 3))
        log_frame = tk.Frame(main, bg=COLORS["log_bg"])
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(
            log_frame, bg=COLORS["log_bg"], fg=COLORS["text_secondary"],
            font=("Consolas", 9), relief=tk.FLAT, wrap=tk.WORD,
            state=tk.DISABLED, padx=10, pady=8, spacing1=2,
        )
        scrollbar = tk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.log_text.tag_configure("success", foreground=COLORS["success"])
        self.log_text.tag_configure("error", foreground=COLORS["error"])
        self.log_text.tag_configure("warning", foreground=COLORS["warning"])

    def _default_output_folder(self) -> str:
        if platform.system() == "Windows":
            return r"C:\AI LEAD GENERATION AGENT ai code\___LEADS GENERATED___"
        return os.path.join(os.path.expanduser("~"), "LeadGen_Output")

    def _browse_folder(self):
        folder = filedialog.askdirectory(title="Select Output Folder", initialdir=self.folder_var.get())
        if folder:
            self.folder_var.set(folder)

    def _validate_inputs(self) -> bool:
        if not self.industry_var.get().strip():
            messagebox.showwarning("Input Required", "Please enter an industry.")
            return False
        try:
            vol = int(self.volume_var.get())
            if vol < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Input", "Min Search Volume must be a positive number.")
            return False
        try:
            cpc = float(self.cpc_var.get())
            if cpc < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Input", "Min CPC must be a positive number.")
            return False
        try:
            max_leads = int(self.max_leads_var.get())
            if max_leads < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid Input", "Max Leads must be a non-negative integer (0 = unlimited).")
            return False
        if not self.folder_var.get().strip():
            messagebox.showwarning("Input Required", "Please specify an output folder.")
            return False
        return True

    def _on_generate(self):
        if not self._validate_inputs():
            return
        self.generate_btn.configure(state=tk.DISABLED)
        self.cancel_btn.configure(state=tk.NORMAL)
        self.progress_var.set(0)
        self._clear_log()

        self.pipeline = LeadGenerationPipeline(
            industry=self.industry_var.get().strip(),
            country=self.country_var.get().strip(),
            min_volume=int(self.volume_var.get()),
            min_cpc=float(self.cpc_var.get()),
            output_folder=self.folder_var.get().strip(),
            progress_callback=self._update_progress_safe,
            log_callback=self._append_log_safe,
            max_leads=int(self.max_leads_var.get()),
        )
        self.pipeline_thread = threading.Thread(target=self._run_pipeline, daemon=True)
        self.pipeline_thread.start()

    def _run_pipeline(self):
        result_path = self.pipeline.run()
        self.root.after(0, self._on_pipeline_done, result_path)

    def _on_pipeline_done(self, result_path: str):
        self.generate_btn.configure(state=tk.NORMAL)
        self.cancel_btn.configure(state=tk.DISABLED)
        if result_path:
            leads = self.pipeline.leads if self.pipeline else []
            count = len(leads)
            personal_count = sum(1 for ld in leads if is_personal_email(ld.get("email", "")) or ld.get("_email_verified"))
            work_count = sum(1 for ld in leads if not is_personal_email(ld.get("email", "")) and is_work_email(ld.get("email", "")))
            generic_count = sum(1 for ld in leads if ld.get("email") and not is_personal_email(ld.get("email", "")) and not is_work_email(ld.get("email", "")))
            no_email_count = sum(1 for ld in leads if not ld.get("email"))
            paid_count = sum(1 for ld in leads if ld.get("_domain_source") == "paid")
            organic_count = sum(1 for ld in leads if ld.get("_domain_source") == "organic")
            phone_count = sum(1 for ld in leads if ld.get("phone"))
            api = self.pipeline._api_counter if self.pipeline else {}
            phone_cr = getattr(self.pipeline, "_phone_credits_used", 0)
            email_cr = getattr(self.pipeline, "_email_credits_used", 0)
            self.status_var.set(f"Done! {count} leads exported")
            self._refresh_stats()
            self._show_run_summary(
                count=count,
                paid_count=paid_count, organic_count=organic_count,
                phone_count=phone_count,
                personal_count=personal_count, work_count=work_count,
                generic_count=generic_count, no_email_count=no_email_count,
                apollo_calls=api.get("apollo", 0), lusha_calls=api.get("lusha", 0),
                semrush_calls=api.get("semrush", 0), serpapi_calls=api.get("serpapi", 0),
                phone_cr=phone_cr, email_cr=email_cr,
                result_path=result_path,
            )
        elif self.pipeline and self.pipeline._cancelled:
            self.status_var.set("Cancelled")
        else:
            self.status_var.set("Completed (no leads found)")
            messagebox.showwarning(
                "No Results",
                "No leads were found. Try a different industry or lower the search volume/CPC thresholds.",
            )

    def _show_run_summary(self, count, paid_count, organic_count, phone_count,
                          personal_count, work_count, generic_count, no_email_count,
                          apollo_calls, lusha_calls, semrush_calls, serpapi_calls,
                          phone_cr, email_cr, result_path):
        """Show a dedicated Toplevel run-summary panel instead of a plain messagebox."""
        win = tk.Toplevel(self.root)
        win.title("Run Summary")
        win.configure(bg=COLORS["bg_dark"])
        win.resizable(False, False)
        win.grab_set()

        pad = dict(padx=18, pady=6)

        def section(parent, title):
            tk.Label(parent, text=title, bg=COLORS["bg_dark"],
                     fg=COLORS["accent"], font=("Segoe UI", 10, "bold")).pack(anchor=tk.W, padx=18, pady=(10, 2))
            sep = tk.Frame(parent, bg=COLORS["bg_light"], height=1)
            sep.pack(fill=tk.X, padx=18, pady=(0, 4))

        def row(parent, label, value, value_color=None):
            fr = tk.Frame(parent, bg=COLORS["bg_dark"])
            fr.pack(fill=tk.X, padx=24, pady=1)
            tk.Label(fr, text=label, bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
                     font=("Segoe UI", 9), width=28, anchor=tk.W).pack(side=tk.LEFT)
            tk.Label(fr, text=str(value), bg=COLORS["bg_dark"],
                     fg=value_color or COLORS["text_primary"],
                     font=("Segoe UI", 9, "bold")).pack(side=tk.LEFT)

        # Header
        tk.Label(win, text=f"Run Complete — {count} Leads",
                 bg=COLORS["bg_dark"], fg=COLORS["text_primary"], 
                 font=("Segoe UI", 14, "bold")).pack(anchor=tk.W, **pad)

        # Lead Sources
        section(win, "Lead Sources")
        row(win, "Total leads generated", count)
        row(win, "Paid Ads domains", paid_count, COLORS["success"])
        row(win, "Organic domains", organic_count, COLORS["warning"])

        # Contact Data Coverage
        section(win, "Contact Data Coverage")
        row(win, "Leads with phone number", phone_count, COLORS["success"] if phone_count == count else COLORS["warning"])
        row(win, "Personal email (gmail/yahoo)", personal_count, COLORS["success"] if personal_count > 0 else COLORS["error"])
        row(win, "Work email (firstname@company)", work_count, COLORS["warning"])
        row(win, "Generic email (info@/contact@)", generic_count, COLORS["text_muted"])
        row(win, "No email found", no_email_count, COLORS["error"] if no_email_count > 0 else COLORS["text_muted"])

        # API Credit Usage
        section(win, "API Credits Used This Run")
        row(win, "Apollo API calls", apollo_calls)
        row(win, "Lusha API calls", lusha_calls)
        row(win, "SEMrush units", semrush_calls)
        row(win, "SerpApi credits", serpapi_calls)
        row(win, "Credits → phone numbers", phone_cr, COLORS["accent"])
        row(win, "Credits → personal emails", email_cr, COLORS["accent"])

        # Output path
        section(win, "Output Files")
        path_fr = tk.Frame(win, bg=COLORS["bg_dark"])
        path_fr.pack(fill=tk.X, padx=24, pady=(2, 6))
        tk.Label(path_fr, text=result_path, bg=COLORS["bg_dark"], fg=COLORS["text_secondary"],
                 font=("Consolas", 8), wraplength=460, anchor=tk.W, justify=tk.LEFT).pack(anchor=tk.W)

        # Close button
        tk.Button(
            win, text="  Close  ",
            bg=COLORS["accent"], fg=COLORS["button_fg"],
            activebackground=COLORS["accent_hover"],
            font=("Segoe UI", 10, "bold"), relief=tk.FLAT, padx=20, pady=6,
            command=win.destroy,
        ).pack(pady=(6, 16))

        # Centre relative to main window
        win.update_idletasks()
        mw = self.root.winfo_width()
        mh = self.root.winfo_height()
        mx = self.root.winfo_x()
        my = self.root.winfo_y()
        ww = win.winfo_width()
        wh = win.winfo_height()
        win.geometry(f"+{mx + (mw - ww)//2}+{my + (mh - wh)//2}")

    def _on_cancel(self):
        if self.pipeline:
            self.pipeline.cancel()
            self.cancel_btn.configure(state=tk.DISABLED)
            self.status_var.set("Cancelling...")

    def _update_progress_safe(self, pct: int, status: str = ""):
        self.root.after(0, self._update_progress, pct, status)

    def _update_progress(self, pct: int, status: str = ""):
        self.progress_var.set(pct)
        if status:
            self.status_var.set(status)
        self._refresh_stats()

    def _refresh_stats(self):
        """V5.10+: Update the Run Statistics panel from current pipeline state."""
        p = self.pipeline
        if not p:
            return
        leads = getattr(p, "leads", None) or []
        total = len(leads)
        paid_count = sum(1 for ld in leads if ld.get("_domain_source") == "paid")
        organic_count = sum(1 for ld in leads if ld.get("_domain_source") == "organic")
        phone_count = sum(1 for ld in leads if ld.get("phone"))
        email_count = sum(1 for ld in leads if is_personal_email(ld.get("email", "")))
        api = getattr(p, "_api_counter", {})

        self.stat_total_var.set(f"Total: {total}")
        self.stat_paid_var.set(f"Paid Ads: {paid_count}")
        self.stat_organic_var.set(f"Organic: {organic_count}")
        self.stat_phone_var.set(f"w/ Phone: {phone_count}")
        self.stat_email_var.set(f"Personal Email: {email_count}")
        self.stat_apollo_var.set(f"Apollo: {api.get('apollo', 0)}")
        self.stat_lusha_var.set(f"Lusha: {api.get('lusha', 0)}")
        self.stat_semrush_var.set(f"SEMrush: {api.get('semrush', 0)}")
        self.stat_phone_cr_var.set(f"Phone Credits: {getattr(p, '_phone_credits_used', 0)}")
        self.stat_email_cr_var.set(f"Email Credits: {getattr(p, '_email_credits_used', 0)}")

    def _append_log_safe(self, message: str):
        self.root.after(0, self._append_log, message)

    def _append_log(self, message: str):
        self.log_text.configure(state=tk.NORMAL)
        tag = ""
        if "Done" in message or "Saved" in message or "Total" in message:
            tag = "success"
        elif "Error" in message or "error" in message:
            tag = "error"
        elif "Warning" in message or "warning" in message:
            tag = "warning"
        self.log_text.insert(tk.END, message + "\n", tag if tag else ())
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _clear_log(self):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete("1.0", tk.END)
        self.log_text.configure(state=tk.DISABLED)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main_gui():
    import tkinter as tk_mod
    from tkinter import filedialog as fd_mod, messagebox as mb_mod, ttk as ttk_mod

    global tk, ttk, filedialog, messagebox
    tk = tk_mod
    ttk = ttk_mod
    filedialog = fd_mod
    messagebox = mb_mod

    root = tk.Tk()
    LeadGeneratorApp(root)
    root.mainloop()


# ══════════════════════════════════════════════════════════════════════════════
# EMBEDDED WEB SERVER (replaces server.py — run with: python V5.py)
# ══════════════════════════════════════════════════════════════════════════════

def main_web():
    """Start the Flask web server and auto-open the browser."""
    import csv as _csv
    import uuid as _uuid
    import webbrowser

    from flask import Flask, jsonify, request as flask_request, send_from_directory
    from flask_cors import CORS

    _DIR = os.path.dirname(os.path.abspath(__file__))

    # Load .env FIRST so MySQL credentials are available before db.init_pool()
    try:
        from dotenv import load_dotenv as _load_dotenv
        _env_path = os.path.join(_DIR, ".env")
        _load_dotenv(_env_path, override=False)
        print(f"[ENV] Loaded .env from {_env_path}")
    except Exception as _env_err:
        print(f"[ENV] Could not load .env: {_env_err}")

    app = Flask(__name__, static_folder=_DIR)
    CORS(app)

    _jobs = {}
    _credits_cache = {"data": None, "timestamp": 0}
    _CACHE_TTL = 300

    # ── Phase 2: MySQL init (fail-open — pipeline still runs without DB) ──────
    # 2026-05-21: Loud, grep-able boot banner. Previously the failure log
    # was a single line buried in startup noise, so the user didn't notice
    # MySQL was down and wondered why master_leads stayed empty + why
    # re-runs returned the same leads (no cross-run dedup possible).
    _v5_db_ready = False
    _v5_db_retry_attempted = False   # one-shot retry guard for /generate path
    _v5_admin_user_id = 1            # fallback; overwritten below if DB available

    def _v5_log_db_banner(connected: bool, *, reason: str = "", host_info: str = "") -> None:
        """3-line banner mirroring the discovery-mode banner format so the
        DB status is obvious in the startup log AND grep-able with `[DB]`."""
        if connected:
            print("════════ [DB] CONNECTED ════════", flush=True)
            print(f"   host: {host_info or 'configured'}", flush=True)
            print(f"   default user_id for runs: {_v5_admin_user_id}", flush=True)
        else:
            print("════════ [DB] UNAVAILABLE ════════", flush=True)
            print(f"   reason: {reason or 'unknown'}", flush=True)
            print(f"   host: {host_info or 'unknown'}", flush=True)
            print("   consequence: master_leads stays empty; no cross-run dedup; "
                  "CSV-only mode (set MYSQL* env vars and restart to fix).",
                  flush=True)

    def _v5_db_host_info() -> str:
        h = os.environ.get("MYSQLHOST", "") or ""
        p = os.environ.get("MYSQLPORT", "") or ""
        d = os.environ.get("MYSQLDATABASE", "") or ""
        url = os.environ.get("MYSQL_URL", "") or os.environ.get("MYSQL_PUBLIC_URL", "") or ""
        if h:
            return f"{h}:{p or '3306'}/{d or '?'}"
        if url:
            return url.split("@")[-1] if "@" in url else url[:40]
        return "no MYSQL* env vars set"

    def _v5_try_db_init() -> bool:
        """Attempt MySQL pool init + schema; return True on success.
        Used both at module-load AND as a one-shot retry from /generate."""
        nonlocal_ready_flag = {}   # workaround for closure mutation in older Python
        try:
            import db as _db_mod
            _db_mod.init_pool()
            _db_mod.init_schema()
            _users = _db_mod.UserRepo.list_all()
            if _users:
                nonlocal_ready_flag["admin_user_id"] = int(_users[0]["id"])
            nonlocal_ready_flag["ok"] = True
            return True, nonlocal_ready_flag, ""
        except Exception as _e:
            return False, nonlocal_ready_flag, f"{type(_e).__name__}: {_e}"

    _ok, _info, _reason = _v5_try_db_init()
    if _ok:
        _v5_admin_user_id = _info.get("admin_user_id", _v5_admin_user_id)
        _v5_db_ready = True
        _v5_log_db_banner(connected=True, host_info=_v5_db_host_info())
        # Optional one-shot wipe at startup. Set CLEAR_DB_ON_STARTUP=1 in .env,
        # boot once, then unset to avoid wiping every restart.
        if str(os.environ.get("CLEAR_DB_ON_STARTUP", "0")).strip() == "1":
            try:
                import db as _db_mod_wipe
                _wiped = _db_mod_wipe.clear_lead_data()
                print(f"[DB] CLEAR_DB_ON_STARTUP=1 → wiped {_wiped}", flush=True)
            except Exception as _wipe_err:
                print(f"[DB] CLEAR_DB_ON_STARTUP wipe failed: {_wipe_err}", flush=True)
    else:
        _v5_log_db_banner(connected=False, reason=_reason,
                          host_info=_v5_db_host_info())

    def _v5_try_finalize(pipeline, run_id, industry, country, state, start_time, api_usage):
        """Write run_history finish + master_leads rows after a pipeline completes.
        Fail-open: any error is logged but never surfaces to the user."""
        if not _v5_db_ready or run_id is None:
            return
        try:
            import db as _db2
            import pricing as _pricing
            from utils import normalize_master_key as _nmk, root_domain as _rd
            duration = max(0, int(time.time() - start_time))

            # ── Cost accounting (2026-06-08) ──────────────────────────────
            # Build the per-credit-type consumption this run actually incurred,
            # then convert to dollars with the user's saved pricing. Frozen
            # here so editing prices later never changes this run's recorded
            # cost. A source consuming 0 credits (e.g. SEMrush when bypassed)
            # naturally costs $0 — the "ignore if unused" rule.
            _cost_usage, _run_cost_usd, _run_cpl_usd, _cost_per_item = _v5_run_cost(pipeline)

            rows = []
            for ld in getattr(pipeline, "leads", []) or []:
                nn = _nmk(ld.get("name") or "")
                rd = _rd(ld.get("domain") or "")
                if not (nn and rd):
                    continue
                # 2026-05-18 (round 4): master_leads.traffic_source is a
                # constrained ENUM (paid/organic/competitor/secondary) so
                # google_intent maps to "secondary" for DB storage. The
                # CSV export still shows "Google Intent" verbatim — the
                # mapping is DB-only and the run_history payload preserves
                # the original `_google_intent` flag for forensic queries.
                if ld.get("_google_intent"):
                    ts = "secondary"
                else:
                    ts = (ld.get("_domain_source") or "paid").strip().lower()
                    if ts not in ("paid", "organic", "competitor", "secondary"):
                        ts = "paid"
                rows.append({
                    "normalized_name": nn, "root_domain": rd,
                    "display_name": ld.get("name"), "company_name": ld.get("company"),
                    "role": ld.get("role"),
                    "phone_e164": ld.get("phone") or None,
                    "primary_email": ld.get("email") or None,
                    "email_type": (ld.get("email_type") or ld.get("_email_type") or "") or None,
                    "linkedin_url": ld.get("_linkedin_url") or None,
                    "traffic_source": ts,
                    "organic_traffic":  ld.get("_organic_traffic", 0),
                    "paid_traffic":     ld.get("_paid_traffic", 0),
                    "organic_keywords": ld.get("_organic_keywords", 0),
                    "paid_keywords":    ld.get("_paid_keywords", 0),
                    "revenue":          ld.get("_revenue", 0),
                    # Frozen per-lead cost for THIS run (same value for every
                    # lead in the run; blank stays blank for pre-existing rows).
                    "cost_per_lead_usd": _run_cpl_usd,
                    "payload_json": {
                        "name": ld.get("name"), "company": ld.get("company"),
                        "domain": ld.get("domain"), "role": ld.get("role"),
                        "organic_traffic":  ld.get("_organic_traffic", 0),
                        "paid_traffic":     ld.get("_paid_traffic", 0),
                        "organic_keywords": ld.get("_organic_keywords", 0),
                        "paid_keywords":    ld.get("_paid_keywords", 0),
                        "revenue":          ld.get("_revenue", 0),
                    },
                })
            new_inserted = 0
            if state == "done" and rows:
                new_inserted = _db2.MasterLeadRepo.bulk_insert_new(rows, run_id, industry, country)
            # Persist the dollar breakdown alongside the raw counter so the run
            # row carries both "what was consumed" and "what it cost".
            _api_usage_out = dict(api_usage or {})
            _api_usage_out["_cost_usage"]        = _cost_usage
            _api_usage_out["_cost_per_item_usd"] = _cost_per_item
            _api_usage_out["_cost_total_usd"]    = _run_cost_usd
            _api_usage_out["_cost_per_lead_usd"] = _run_cpl_usd
            _db2.RunHistoryRepo.finish(
                run_id=run_id, state=state,
                leads_total=len(getattr(pipeline, "leads", []) or []),
                leads_new=new_inserted,
                leads_deduped_out=int(getattr(pipeline, "_master_leads_deduped_out", 0) or 0),
                duration_seconds=duration,
                secondary_agent_used=bool(getattr(pipeline, "_secondary_agent_used", False)),
                competitor_depth_reached=int(getattr(pipeline, "_competitor_depth_reached", 0) or 0),
                api_usage=_api_usage_out,
                cost_usd=_run_cost_usd,
                cost_per_lead_usd=_run_cpl_usd,
            )
            print(f"[DB] run_id={run_id} finalized: {state}, {new_inserted} new leads in master_leads")
        except Exception as _fe:
            print(f"[DB] _v5_try_finalize error (run_id={run_id}): {_fe}")

    # V5.26: Apollo phone reveal webhook endpoint
    @app.route(_APOLLO_PHONE_CALLBACK_PATH, methods=['POST'])
    def apollo_phone_callback():
        """Receives async phone data from Apollo after a reveal request.
        Apollo payload: {"status":"success","people":[{"id":"...","status":"success","phone_numbers":[...]}]}"""
        data = flask_request.get_json(silent=True)
        if data:
            # Apollo wraps results in a "people" array
            people = data.get("people", [])
            for person in people:
                person_id = person.get("id", "")
                phone_numbers = person.get("phone_numbers") or []
                if person_id and phone_numbers:
                    _receive_phone_reveal(person_id, phone_numbers)
        return jsonify({"status": "ok"}), 200

    # ── Job State ────────────────────────────────────────────────────────────
    class _JobState:
        def __init__(self):
            self.progress = 0
            self.status_text = "Starting..."
            self.state = "running"
            self.logs = []
            self.log_cursor = 0
            self.leads = []
            self.top_csv = ""
            self.all_csv = ""
            self.error = ""
            self.pipeline = None
            self.api_usage = {}  # V5.7: Per-run API call counts
            self.summary = {}   # V5.13: Token usage & lead statistics
            self.start_time = time.time()  # V5.14: Track start for time remaining

    # ── Credit Checkers ──────────────────────────────────────────────────────
    def _check_apollo():
        """V5.32: Expanded — reports overall credits AND specific pools (email, mobile,
        export) that frequently exhaust independently. Mobile-credit exhaustion is the
        usual cause of missing phone reveals."""
        try:
            r = requests.get(
                "https://api.apollo.io/api/v1/auth/health",
                headers={"X-Api-Key": API_KEYS.get("apollo", "")},
                timeout=10,
            )
            if r.status_code == 200:
                data = r.json() or {}
                plan = data.get("plan", {}) or {}
                usage = data.get("usage", {}) or {}
                total = int(plan.get("credits", 10000) or 10000)
                used = int(usage.get("credits_used", 0) or 0)
                remaining = max(0, total - used)
                # V5.32: Capture separate credit pools if Apollo exposes them
                # (field names vary across plans; we check common shapes)
                pools = {}
                for pool_key in ("email_credits", "mobile_credits", "export_credits"):
                    _p_total = plan.get(pool_key) or plan.get(pool_key.replace("_", ""))
                    _p_used = usage.get(pool_key + "_used") or usage.get(pool_key.replace("_credits", "_used"))
                    if _p_total is not None:
                        _p_total = int(_p_total or 0)
                        _p_used = int(_p_used or 0)
                        pools[pool_key] = {
                            "total": _p_total, "used": _p_used,
                            "remaining": max(0, _p_total - _p_used),
                        }
                return {"service": "Apollo", "status": "ok", "total": total, "used": used,
                        "remaining": remaining, "pct_remaining": round(remaining / max(total, 1) * 100, 1),
                        "searches_remaining": remaining // 2,
                        "pools": pools}
            # V5.32: Surface the specific error text (422 = insufficient credits → user-actionable)
            _err = f"HTTP {r.status_code}"
            try:
                _body = r.json()
                if isinstance(_body, dict) and _body.get("error"):
                    _err = f"HTTP {r.status_code}: {_body['error']}"
            except Exception:
                pass
            return {"service": "Apollo", "status": "error", "error": _err,
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
        except Exception as e:
            return {"service": "Apollo", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _check_lusha():
        """V5.7: Validate Lusha key + local call tracking (Lusha has no credit balance API)."""
        try:
            r = requests.get(
                "https://api.lusha.com/v2/company",
                headers={"api_key": API_KEYS.get("lusha", "")},
                params={"domain": "example.com"}, timeout=10,
            )
            if r.status_code in (401, 403):
                return {"service": "Lusha", "status": "error", "error": "API key invalid or expired",
                        "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
            # Key is valid — compute local tracking
            total = LUSHA_PLAN_CREDITS
            used = _lusha_calls_total
            remaining = max(0, total - used)
            pct = round(remaining / max(total, 1) * 100, 1)
            return {"service": "Lusha", "status": "ok", "total": total, "used": used,
                    "remaining": remaining, "pct_remaining": pct,
                    "searches_remaining": remaining // 2,
                    "note": "Locally tracked (resets on server restart)"}
        except Exception as e:
            return {"service": "Lusha", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _check_semrush():
        """V5.7: Use real Semrush API units balance endpoint."""
        try:
            r = requests.get(
                "https://www.semrush.com/users/countapiunits.html",
                params={"key": API_KEYS.get("semrush", "")},
                timeout=10,
            )
            if r.status_code == 200:
                text = r.text.strip()
                try:
                    remaining = int(float(text))
                    total = max(remaining, SEMRUSH_PLAN_TOTAL)
                    used = total - remaining
                    pct = round(remaining / max(total, 1) * 100, 1)
                    return {"service": "Semrush", "status": "ok", "total": total, "used": used,
                            "remaining": remaining, "pct_remaining": pct,
                            "searches_remaining": remaining // 3}
                except ValueError:
                    if "ERROR" in text:
                        return {"service": "Semrush", "status": "error",
                                "error": text[:100],
                                "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
            return {"service": "Semrush", "status": "error",
                    "error": f"HTTP {r.status_code}",
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
        except Exception as e:
            return {"service": "Semrush", "status": "error", "error": str(e),
                    "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _check_google_places():
        """2026-05-21: Google Places does NOT expose a remaining-quota REST
        endpoint, so we report:
          • status='ok' iff a key is configured (else 'no_key')
          • remaining=None (unknowable)
          • per_run_cap / per_run_domain_cap from env-driven module consts
          • session_calls_made from the module-level counter that
            increments at the end of every Discovery.discover() call.
        This gives the user actionable info on how aggressively Places is
        being used without us pretending to know Google's quota."""
        try:
            import google_places_intent as _gpi
            _key_set = bool(API_KEYS.get("google_places"))
            _session_calls = _gpi.get_session_calls_made()
            _per_run_cap = int(getattr(_gpi, "MAX_PLACES_CALLS", 25))
            _per_run_domains = int(getattr(_gpi, "MAX_DOMAINS", 100))
            return {
                "service": "Google Places",
                "status": "ok" if _key_set else "no_key",
                "remaining": None,            # Google has no quota API
                "per_run_cap": _per_run_cap,
                "per_run_domain_cap": _per_run_domains,
                "session_calls_made": _session_calls,
                # The frontend credit card looks for `total`/`used`/`remaining`
                # to render; we surface the per-run cap as `total`, session
                # calls as `used`, and remaining = max(cap - used, 0) so the
                # tile renders consistently with the other services.
                "total": _per_run_cap,
                "used": _session_calls,
                "pct_remaining": max(0, 100 - int((_session_calls / _per_run_cap) * 100)) if _per_run_cap else 0,
                "searches_remaining": max(0, _per_run_cap - _session_calls),
            }
        except Exception as _e:
            return {"service": "Google Places", "status": "error",
                    "error": str(_e), "total": 0, "used": 0,
                    "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _check_google_custom_search():
        """2026-05-25: Custom Search JSON API — no remaining-quota endpoint
        exists. Same shape as _check_google_places: report per-run cap,
        session calls so far, and derive remaining = cap - used. Status is
        'ok' iff BOTH the API key AND the Programmable Search Engine ID (cx)
        are configured — CSE rejects the request when either is missing."""
        try:
            import google_custom_search as _gcs
            _key_ok = bool(API_KEYS.get("google_custom_search"))
            _cx_ok = bool(API_KEYS.get("google_custom_search_cx"))
            _session_calls = _gcs.get_session_calls_made()
            _per_run_cap = int(getattr(_gcs, "MAX_QUERIES", 10))
            _per_run_domains = int(getattr(_gcs, "MAX_DOMAINS", 80))
            _status = "ok" if (_key_ok and _cx_ok) else "no_key"
            return {
                "service": "Google Custom Search",
                "status": _status,
                "remaining": None,
                "per_run_cap": _per_run_cap,
                "per_run_domain_cap": _per_run_domains,
                "session_calls_made": _session_calls,
                "total": _per_run_cap,
                "used": _session_calls,
                "pct_remaining": max(0, 100 - int((_session_calls / _per_run_cap) * 100)) if _per_run_cap else 0,
                "searches_remaining": max(0, _per_run_cap - _session_calls),
            }
        except Exception as _e:
            return {"service": "Google Custom Search", "status": "error",
                    "error": str(_e), "total": 0, "used": 0,
                    "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}

    def _fetch_credits(force=False):
        now = time.time()
        if not force and _credits_cache["data"] and (now - _credits_cache["timestamp"]) < _CACHE_TTL:
            return _credits_cache["data"]
        results = {}
        # 2026-05-21: Google Places added to the parallel credit fetch. It
        # doesn't actually hit the network (no GCP quota endpoint exists),
        # so it returns instantly — still scheduled via the same pool to
        # keep the result-dict shape uniform.
        # 2026-05-25: Google Custom Search added on the same pattern.
        with ThreadPoolExecutor(max_workers=5) as pool:
            futs = {pool.submit(_check_apollo): "apollo",
                    pool.submit(_check_lusha): "lusha",
                    pool.submit(_check_semrush): "semrush",
                    pool.submit(_check_google_places): "google_places",
                    pool.submit(_check_google_custom_search): "google_custom_search"}
            for f in as_completed(futs):
                k = futs[f]
                try:
                    results[k] = f.result()
                except Exception as e:
                    results[k] = {"service": k.title(), "status": "error", "error": str(e),
                                  "total": 0, "used": 0, "remaining": 0, "pct_remaining": 0, "searches_remaining": 0}
        total_searches = sum(r.get("searches_remaining", 0) for r in results.values())
        alerts = []
        for k, r in results.items():
            pct = r.get("pct_remaining", 0)
            if r.get("status") == "error":
                alerts.append({"level": "error", "service": r["service"],
                               "message": f"{r['service']}: {r.get('error', 'Unknown error')}"})
            elif pct <= 10:
                alerts.append({"level": "critical", "service": r["service"],
                               "message": f"{r['service']} credits critically low ({pct}%)"})
            elif pct <= 25:
                alerts.append({"level": "warning", "service": r["service"],
                               "message": f"{r['service']} credits running low ({pct}%)"})
        payload = {"services": results, "total_searches_remaining": total_searches,
                   "alerts": alerts, "cached": False, "timestamp": datetime.now().isoformat()}
        _credits_cache["data"] = payload
        _credits_cache["timestamp"] = now
        return payload

    # ── Routes ───────────────────────────────────────────────────────────────
    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "version": "V5.7"})

    @app.route("/")
    def serve_index():
        return send_from_directory(_DIR, "index.html")

    # ── 2026-06-08: API pricing config (Cost / Pricing tab) ──────────────────
    # The user enters per-API {credits bought, amount paid, monthly?}; we derive
    # $/credit and use it at run finalize to freeze each run's dollar cost.
    @app.route("/api/pricing", methods=["GET"])
    def v5_get_pricing():
        try:
            import pricing as _pr
            cfg = _pr.load_pricing()
            return jsonify({
                "pricing": cfg,
                "line_items": list(_pr.LINE_ITEMS),
                "unit_prices": _pr.unit_prices(cfg),
            })
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/pricing", methods=["POST"])
    def v5_save_pricing():
        try:
            import pricing as _pr
            body = flask_request.get_json(silent=True) or {}
            # Accept either the full {items:...} config or a bare items map.
            cfg = body.get("pricing") if isinstance(body.get("pricing"), dict) else body
            saved = _pr.save_pricing(cfg)
            return jsonify({"ok": True, "pricing": saved, "unit_prices": _pr.unit_prices(saved)})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)}), 500

    # ── Phase 2: DB viewer routes — registered BEFORE catch-all ──────────────
    @app.route("/api/master-stats")
    def v5_master_stats():
        """Master DB total count — used by the Master DB card in index.html."""
        if not _v5_db_ready:
            return jsonify({"total_leads": 0, "error": "db_unavailable",
                            "detail": "MySQL not connected — check .env"}), 200
        try:
            import db as _dbs
            total = _dbs.MasterLeadRepo.total_count()
            return jsonify({"total_leads": total, "cached": False})
        except Exception as e:
            return jsonify({"total_leads": 0, "error": str(e)}), 200

    @app.route("/api/db/debug")
    def v5_db_debug():
        info = {"db_ready": _v5_db_ready, "admin_user_id": _v5_admin_user_id}
        if _v5_db_ready:
            try:
                import db as _db5
                info["master_leads_count"] = _db5.MasterLeadRepo.total_count()
                info["user_count"] = _db5.UserRepo.count()
                with _db5.get_conn() as _c:
                    with _c.cursor() as _cur:
                        _cur.execute("SELECT COUNT(*) AS c FROM run_history")
                        _row = _cur.fetchone()
                        info["run_history_count"] = int(_row["c"]) if _row else 0
            except Exception as _e:
                info["query_error"] = str(_e)
        return jsonify(info)

    @app.route("/api/db/master-leads")
    def v5_db_master_leads():
        page = int(flask_request.args.get("page", 1) or 1)
        page_size = int(flask_request.args.get("page_size", 50) or 50)
        if not _v5_db_ready:
            return jsonify({"error": "db_unavailable", "detail": "MySQL not connected. Check .env file."}), 503
        try:
            import db as _db6
            rows, total = _db6.MasterLeadRepo.list_page(page=page, page_size=page_size)
            for r in rows:
                if r.get("first_seen_at") and hasattr(r["first_seen_at"], "isoformat"):
                    r["first_seen_at"] = r["first_seen_at"].isoformat()
                # cost_per_lead_usd is DECIMAL/NULL — float for JSON, keep None blank.
                if r.get("cost_per_lead_usd") is not None:
                    try:
                        r["cost_per_lead_usd"] = float(r["cost_per_lead_usd"])
                    except (TypeError, ValueError):
                        r["cost_per_lead_usd"] = None
            return jsonify({"rows": rows, "total": total, "page": page, "page_size": page_size,
                            "pages": max(1, (total + page_size - 1) // page_size)})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/db/clear-all", methods=["POST"])
    def v5_db_clear_all():
        """Wipes master_leads and run_history (users preserved). One-shot reset."""
        if not _v5_db_ready:
            return jsonify({"error": "db_unavailable",
                            "detail": "MySQL not connected. Check .env file."}), 503
        try:
            import db as _db8
            deleted = _db8.clear_lead_data()
            return jsonify({"ok": True, "deleted": deleted})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/api/db/run-history")
    def v5_db_run_history():
        page = int(flask_request.args.get("page", 1) or 1)
        page_size = int(flask_request.args.get("page_size", 50) or 50)
        if not _v5_db_ready:
            return jsonify({"error": "db_unavailable", "detail": "MySQL not connected. Check .env file."}), 503
        try:
            import db as _db7
            rows, total = _db7.RunHistoryRepo.list_page(page=page, page_size=page_size)
            for r in rows:
                for k in ("started_at", "finished_at"):
                    if r.get(k) and hasattr(r[k], "isoformat"):
                        r[k] = r[k].isoformat()
                # DECIMAL columns arrive as Decimal — coerce to float (JSON-safe).
                for k in ("cost_usd", "cost_per_lead_usd", "min_cpc"):
                    if r.get(k) is not None:
                        try:
                            r[k] = float(r[k])
                        except (TypeError, ValueError):
                            r[k] = None
            return jsonify({"rows": rows, "total": total, "page": page, "page_size": page_size,
                            "pages": max(1, (total + page_size - 1) // page_size)})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route("/<path:filename>")
    def serve_static(filename):
        # Return JSON for any /api/ path that wasn't matched above
        if filename.startswith("api/"):
            return jsonify({"error": "endpoint_not_found", "path": "/" + filename}), 404
        safe_ext = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".css", ".js", ".webp"}
        if os.path.splitext(filename)[1].lower() in safe_ext:
            return send_from_directory(_DIR, filename)
        return "Not found", 404

    @app.route("/industries")
    def get_industries():
        return jsonify({"industries": list(INDUSTRY_KEYWORDS.keys())})

    @app.route("/api/au-cities")
    def get_au_cities():
        try:
            from cities_au import list_states_payload
            return jsonify(list_states_payload())
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    def _v5_maybe_retry_db() -> None:
        """2026-05-21: one-shot retry on the first /generate(-city) call
        if MySQL was down at boot. The Railway env vars sometimes arrive a
        few seconds after the Flask app starts (esp. when Railway is
        binding the MySQL service reference), so this gives the pipeline
        one more chance to connect before falling back to CSV-only."""
        nonlocal _v5_db_ready, _v5_db_retry_attempted, _v5_admin_user_id
        if _v5_db_ready or _v5_db_retry_attempted:
            return
        _v5_db_retry_attempted = True
        _ok, _info, _reason = _v5_try_db_init()
        if _ok:
            _v5_admin_user_id = _info.get("admin_user_id", _v5_admin_user_id)
            _v5_db_ready = True
            print("[DB] Recovered on /generate retry — master_leads writes will now happen.",
                  flush=True)
            _v5_log_db_banner(connected=True, host_info=_v5_db_host_info())
        else:
            _v5_log_db_banner(connected=False, reason=_reason,
                              host_info=_v5_db_host_info())

    @app.route("/generate", methods=["POST"])
    def generate():
        _v5_maybe_retry_db()
        data = flask_request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
        industry = data.get("industry", "")
        country = data.get("country", "AU")
        min_volume = int(data.get("min_volume", 100))
        min_cpc = float(data.get("min_cpc", 0.05))
        max_leads = int(data.get("max_leads", 0))
        enrichment = bool(data.get("enrichment", True))
        # 2026-06-08: "SerpAPI only" toggle — when True, bypass SEMrush entirely.
        disable_semrush = bool(data.get("disable_semrush", data.get("serp_only", False)))
        credit_saver = bool(data.get("credit_saver", True))
        paid_only_all = bool(data.get("paid_only_all", False))
        if not industry:
            return jsonify({"error": "Industry is required"}), 400
        job_id = str(_uuid.uuid4())[:8]
        job = _JobState()
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        # Phase 2: open run_history row before thread so state=running is visible immediately
        _run_id = [None]
        if _v5_db_ready:
            try:
                import db as _db3
                _run_id[0] = _db3.RunHistoryRepo.start(
                    user_id=_v5_admin_user_id, job_uuid=job_id, industry=industry,
                    country=country, mode="industry", min_volume=min_volume,
                    min_cpc=min_cpc, max_leads=max_leads, enrichment_enabled=enrichment,
                )
                print(f"[DB] run_history started: run_id={_run_id[0]} job_id={job_id}")
            except Exception as _dbe:
                print(f"[DB] run_history.start failed: {_dbe}")

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(message):
            job.logs.append(message)

        pipeline = LeadGenerationPipeline(
            industry=industry, country=country, min_volume=min_volume,
            min_cpc=min_cpc, output_folder=output_folder,
            progress_callback=progress_cb, log_callback=log_cb, max_leads=max_leads,
            enrichment_enabled=enrichment, disable_semrush=disable_semrush,
            credit_saver=credit_saver,
            paid_only_all=paid_only_all,
        )
        job.pipeline = pipeline

        def run():
            try:
                job.progress = 1
                job.status_text = "Initializing pipeline..."
                job.logs.append("[SYSTEM] Pipeline initialized, starting Phase 1...")
                result_path = pipeline.run()
                job.api_usage = pipeline._api_counter.copy()  # V5.7: Capture run cost
                # Live per-lead dollar cost for the Generate table (same maths as
                # the value frozen into the DB at finalize).
                job._cpl_live = _v5_run_cost(pipeline)[2]
                if pipeline._cancelled:
                    job.state = "cancelled"
                    _v5_try_finalize(pipeline, _run_id[0], industry, country, "cancelled", job.start_time, job.api_usage)
                    return
                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as f:
                        reader = _csv.DictReader(f)
                        for row in reader:
                            # 2026-05-21: include source-label fields so the
                            # frontend table can show provenance (Apollo /
                            # Google Intent / Silent-Apollo) per lead.
                            _ts = (row.get("Traffic Source") or "").strip()
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "email": row.get("Email", ""),
                                "phone": row.get("Phone Number", ""),
                                "email_type": row.get("Email Type", ""),
                                "source": row.get("Source", "") or "Apollo",
                                "_google_intent": (_ts == "Google Intent"),
                                "_traffic_source": _ts,
                                "keyword": row.get("Keyword", ""),
                                "cost_per_lead": getattr(job, "_cpl_live", 0.0),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break

                    # V5.13: Build comprehensive summary
                    pipeline_leads = getattr(pipeline, 'leads', [])
                    paid_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source", "paid") == "paid")
                    organic_count = sum(1 for ld in pipeline_leads if ld.get("_domain_source") == "organic")
                    with_phone = sum(1 for lead in job.leads if lead.get("phone"))
                    with_email = sum(1 for lead in job.leads if lead.get("email"))
                    personal_emails = sum(1 for lead in job.leads if lead.get("email_type") == "Personal")
                    direct_phones = sum(1 for ld in pipeline_leads if ld.get("_direct_phone"))
                    verified_emails = sum(1 for ld in pipeline_leads if ld.get("_email_verified"))
                    founder_verified_count = sum(1 for ld in pipeline_leads if ld.get("_founder_verified"))
                    total_credits = sum(
                        job.api_usage.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
                        for svc in API_CREDIT_COSTS
                    )
                    credits_breakdown = {
                        svc: job.api_usage.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
                        for svc in API_CREDIT_COSTS
                    }
                    # 2026-05-18: surface SEMrush per-phase unit breakdown +
                    # the per-run budget so the frontend can render exactly
                    # which phase ate which credits. Falls back to {} when the
                    # client wasn't instrumented (offline tools, etc.).
                    _sm_client = getattr(pipeline, "semrush", None)
                    _sm_units = int(getattr(_sm_client, "_units_used", 0) or 0)
                    _sm_budget = int(getattr(_sm_client, "_unit_budget", 0) or 0)
                    _sm_phases = dict(getattr(_sm_client, "_units_by_phase", {}) or {})
                    _sm_skipped = int(job.api_usage.get("semrush_skipped", 0) or 0)
                    _leads_n = max(len(job.leads), 1)
                    job.summary = {
                        "paid_leads": paid_count,
                        "organic_leads": organic_count,
                        "total_leads": len(job.leads),
                        "with_phone": with_phone,
                        "with_email": with_email,
                        "personal_emails": personal_emails,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "direct_phones": direct_phones,
                        "verified_emails": verified_emails,
                        "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                        "total_credits_used": total_credits,
                        "avg_credits_per_lead": total_credits / max(len(job.leads), 1),
                        "credits_breakdown": credits_breakdown,
                        "founder_verified_count": founder_verified_count,
                        "competitor_domains_added": getattr(pipeline, '_competitor_domains_added', 0),
                        "master_leads_deduped_out": getattr(pipeline, "_master_leads_deduped_out", 0),
                        "semrush_units_used": _sm_units,
                        "semrush_unit_budget": _sm_budget,
                        "semrush_units_per_lead": _sm_units / _leads_n,
                        "semrush_units_by_phase": _sm_phases,
                        "semrush_calls_skipped": _sm_skipped,
                        # 2026-05-21: Google Places per-run total so the
                        # frontend "Credits Used This Run" panel can render
                        # the Google Intent tile without polling /api/credits.
                        "google_places_calls": int(job.api_usage.get("google_places", 0) or 0),
                        "google_places_per_run_cap": _gpi_per_run_cap_for_live(),
                    }
                    _v5_try_finalize(pipeline, _run_id[0], industry, country, "done", job.start_time, job.api_usage)
                    job.state = "done"
                else:
                    # 2026-05-25: even when no CSV is produced (Phase 6
                    # returned "" because self.leads was empty), build a
                    # MINIMAL summary so the frontend's done-branch render
                    # doesn't fall back to a misleading "no data" view.
                    # Without this, the user sees a tiny "Scan complete —
                    # 0 leads found" banner that looks identical to the
                    # pre-run home page and assumes the run failed.
                    _state = "done" if not pipeline._cancelled else "cancelled"
                    _v5_try_finalize(pipeline, _run_id[0], industry, country, _state, job.start_time, job.api_usage)
                    _sm_client_e = getattr(pipeline, "semrush", None)
                    _sm_units_e = int(getattr(_sm_client_e, "_units_used", 0) or 0)
                    _sm_budget_e = int(getattr(_sm_client_e, "_unit_budget", 0) or 0)
                    _sm_phases_e = dict(getattr(_sm_client_e, "_units_by_phase", {}) or {})
                    job.summary = {
                        "paid_leads": 0, "organic_leads": 0, "total_leads": 0,
                        "with_phone": 0, "with_email": 0, "personal_emails": 0,
                        "direct_phones": 0, "verified_emails": 0,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                        "total_credits_used": 0,
                        "avg_credits_per_lead": 0,
                        "competitor_domains_added": getattr(pipeline, '_competitor_domains_added', 0),
                        "master_leads_deduped_out": getattr(pipeline, "_master_leads_deduped_out", 0),
                        "semrush_units_used": _sm_units_e,
                        "semrush_unit_budget": _sm_budget_e,
                        "semrush_units_per_lead": 0,
                        "semrush_units_by_phase": _sm_phases_e,
                        "semrush_calls_skipped": int(job.api_usage.get("semrush_skipped", 0) or 0),
                        "google_places_calls": int(job.api_usage.get("google_places", 0) or 0),
                        "google_places_per_run_cap": _gpi_per_run_cap_for_live(),
                        # NEW: explicit signal so the frontend knows this
                        # was a successful run that simply found 0 leads
                        # (vs. a hard failure that crashed Phase 6).
                        "empty_result": True,
                        "empty_result_reason": (
                            "Pipeline completed but Phase 6 returned no CSV — "
                            "either 0 domains were discovered, or all discovered "
                            "domains were dropped by the paid-traffic gate / "
                            "Phase 5 completeness checks. Check the log feed "
                            "for [FUNNEL] and [KEY-HEALTH] lines for the exact "
                            "drop reason."
                        ),
                    }
                    job.state = _state
            except Exception as e:
                job.error = str(e)
                job.state = "error"
                _v5_try_finalize(pipeline, _run_id[0], industry, country, "error", job.start_time, {})

        threading.Thread(target=run, daemon=True).start()
        return jsonify({"job_id": job_id})

    @app.route("/generate-city", methods=["POST"])
    def generate_city():
        _v5_maybe_retry_db()
        from city_pipeline import CityLeadPipeline
        data = flask_request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400
        state_code = data.get("state_code", "AUSTRALIA")
        tier = data.get("tier", "all")
        city = data.get("city", "all")
        min_volume = int(data.get("min_volume", 100))
        max_leads = int(data.get("max_leads", 0))
        enrichment = bool(data.get("enrichment", True))
        country = data.get("country", "AU")
        # 2026-06-08: "SerpAPI only" toggle — when True, bypass SEMrush entirely.
        disable_semrush = bool(data.get("disable_semrush", data.get("serp_only", False)))
        credit_saver = bool(data.get("credit_saver", True))
        paid_only_all = bool(data.get("paid_only_all", False))
        if max_leads <= 0:
            return jsonify({"error": "Max Leads must be > 0 in City Mode"}), 400

        job_id = str(_uuid.uuid4())[:8]
        job = _JobState()
        _jobs[job_id] = job
        output_folder = os.path.join(_DIR, "output", job_id)
        os.makedirs(output_folder, exist_ok=True)

        _city_run_id = [None]
        _city_industry_label = f"city:{state_code}/{tier}/{city}"
        if _v5_db_ready:
            try:
                import db as _db4
                _city_run_id[0] = _db4.RunHistoryRepo.start(
                    user_id=_v5_admin_user_id, job_uuid=job_id,
                    industry=_city_industry_label,
                    country=country, mode="city", min_volume=min_volume,
                    min_cpc=None, max_leads=max_leads, enrichment_enabled=enrichment,
                )
                print(f"[DB] city run_history started: run_id={_city_run_id[0]}")
            except Exception as _dbe2:
                print(f"[DB] city run_history.start failed: {_dbe2}")

        def progress_cb(pct, status=""):
            job.progress = pct
            if status:
                job.status_text = status

        def log_cb(message):
            job.logs.append(message)

        pipeline = CityLeadPipeline(
            state_code=state_code, tier=tier, city=city,
            min_volume=min_volume, max_leads=max_leads,
            output_folder=output_folder,
            enrichment_enabled=enrichment, country=country,
            progress_callback=progress_cb, log_callback=log_cb,
            disable_semrush=disable_semrush,
            credit_saver=credit_saver,
            paid_only_all=paid_only_all,
        )
        job.pipeline = pipeline

        def run():
            try:
                job.progress = 1
                job.status_text = "Initializing city-mode pipeline..."
                job.logs.append("[SYSTEM] City-mode pipeline initialized…")
                result_path = pipeline.run()
                job.api_usage = dict(pipeline._api_counter)
                # Live per-lead dollar cost for the Generate table (same maths as
                # the value frozen into the DB at finalize).
                job._cpl_live = _v5_run_cost(pipeline)[2]
                if pipeline._cancelled:
                    job.state = "cancelled"
                    _v5_try_finalize(pipeline, _city_run_id[0], _city_industry_label, country, "cancelled", job.start_time, job.api_usage)
                    return
                if result_path and os.path.exists(result_path):
                    with open(result_path, "r", encoding="utf-8") as f:
                        job.top_csv = f.read()
                    with open(result_path, "r", encoding="utf-8") as f:
                        reader = _csv.DictReader(f)
                        for row in reader:
                            # 2026-05-21: include source-label fields so the
                            # frontend table can show provenance (Apollo /
                            # Google Intent / Silent-Apollo) per lead.
                            _ts = (row.get("Traffic Source") or "").strip()
                            job.leads.append({
                                "name": row.get("Name", ""),
                                "company": row.get("Company Name", ""),
                                "domain": row.get("Domain", ""),
                                "role": row.get("Role", ""),
                                "email": row.get("Email", ""),
                                "phone": row.get("Phone Number", ""),
                                "email_type": row.get("Email Type", ""),
                                "source": row.get("Source", "") or "Apollo",
                                "_google_intent": (_ts == "Google Intent"),
                                "_traffic_source": _ts,
                                "keyword": row.get("Keyword", ""),
                                "cost_per_lead": getattr(job, "_cpl_live", 0.0),
                            })
                    for fname in os.listdir(output_folder):
                        if fname.startswith("leads_ALL_") and fname.endswith(".csv"):
                            with open(os.path.join(output_folder, fname), "r", encoding="utf-8") as f:
                                job.all_csv = f.read()
                            break
                    with_phone = sum(1 for lead in job.leads if lead.get("phone"))
                    with_email = sum(1 for lead in job.leads if lead.get("email"))
                    personal_emails = sum(1 for lead in job.leads if lead.get("email_type") == "Personal")
                    total_credits = sum(
                        job.api_usage.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
                        for svc in API_CREDIT_COSTS
                    )
                    credits_breakdown = {
                        svc: job.api_usage.get(svc, 0) * API_CREDIT_COSTS.get(svc, 0)
                        for svc in API_CREDIT_COSTS
                    }
                    # 2026-05-18: same SEMrush per-phase telemetry as
                    # /generate, but values come from CityPipeline's
                    # accumulator across rediscovery waves.
                    _city_units = int(getattr(pipeline, "_inner_units_used", 0) or 0)
                    _city_budget = int(getattr(pipeline, "_inner_unit_budget", 0) or 0)
                    _city_phases = dict(getattr(pipeline, "_inner_units_by_phase", {}) or {})
                    _city_skipped = int(job.api_usage.get("semrush_skipped", 0) or 0)
                    _city_leads_n = max(len(job.leads), 1)
                    job.summary = {
                        "mode": "city",
                        "scope_label": pipeline._scope_label,
                        "state_code": state_code,
                        "tier": tier,
                        "city": city,
                        "enrichment_enabled": enrichment,
                        "paid_leads": len(job.leads),
                        "organic_leads": 0,
                        "total_leads": len(job.leads),
                        "with_phone": with_phone,
                        "with_email": with_email,
                        "personal_emails": personal_emails,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                        "total_credits_used": total_credits,
                        "avg_credits_per_lead": total_credits / max(len(job.leads), 1),
                        "credits_breakdown": credits_breakdown,
                        "competitor_rounds": getattr(pipeline, "_competitor_rounds", 0),
                        "competitor_domains_added": getattr(pipeline, "_competitor_domains_added", 0),
                        "keywords_searched": len(getattr(pipeline, "keywords", [])),
                        "semrush_units_used": _city_units,
                        "semrush_unit_budget": _city_budget,
                        "semrush_units_per_lead": _city_units / _city_leads_n,
                        "semrush_units_by_phase": _city_phases,
                        "semrush_calls_skipped": _city_skipped,
                        # 2026-05-21: Google Places per-run total (city mode).
                        "google_places_calls": int(job.api_usage.get("google_places", 0) or 0),
                        "google_places_per_run_cap": _gpi_per_run_cap_for_live(),
                    }
                    _v5_try_finalize(pipeline, _city_run_id[0], _city_industry_label, country, "done", job.start_time, job.api_usage)
                    job.state = "done"
                else:
                    # 2026-05-25: same minimal-summary fix as /generate above.
                    _cstate = "done" if not pipeline._cancelled else "cancelled"
                    _v5_try_finalize(pipeline, _city_run_id[0], _city_industry_label, country, _cstate, job.start_time, job.api_usage)
                    _ic_units = int(getattr(pipeline, "_inner_units_used", 0) or 0)
                    _ic_budget = int(getattr(pipeline, "_inner_unit_budget", 0) or 0)
                    _ic_phases = dict(getattr(pipeline, "_inner_units_by_phase", {}) or {})
                    job.summary = {
                        "mode": "city",
                        "scope_label": getattr(pipeline, "_scope_label", ""),
                        "state_code": state_code, "tier": tier, "city": city,
                        "enrichment_enabled": enrichment,
                        "paid_leads": 0, "organic_leads": 0, "total_leads": 0,
                        "with_phone": 0, "with_email": 0, "personal_emails": 0,
                        "semrush_tokens": job.api_usage.get("semrush", 0),
                        "apollo_tokens": job.api_usage.get("apollo", 0),
                        "lusha_tokens": job.api_usage.get("lusha", 0),
                        "total_api_calls": sum(v for v in job.api_usage.values() if isinstance(v, (int, float)) and not isinstance(v, bool)),
                        "total_credits_used": 0,
                        "avg_credits_per_lead": 0,
                        "semrush_units_used": _ic_units,
                        "semrush_unit_budget": _ic_budget,
                        "semrush_units_per_lead": 0,
                        "semrush_units_by_phase": _ic_phases,
                        "semrush_calls_skipped": int(job.api_usage.get("semrush_skipped", 0) or 0),
                        "google_places_calls": int(job.api_usage.get("google_places", 0) or 0),
                        "google_places_per_run_cap": _gpi_per_run_cap_for_live(),
                        "empty_result": True,
                        "empty_result_reason": (
                            "City pipeline completed but no leads survived the "
                            "Phase 5 quality gates. Most likely: SEMrush returned "
                            "no paid AU domains for the scope (silent), Apollo "
                            "found orgs but no people in its DB for those domains, "
                            "and enrichment-OFF stub-fallback didn't produce rows. "
                            "Check the log feed for [FUNNEL] and [KEY-HEALTH]."
                        ),
                    }
                    job.state = _cstate
            except Exception as e:
                import traceback as _tb
                _tb.print_exc()
                job.error = str(e)
                job.state = "error"
                _v5_try_finalize(pipeline, _city_run_id[0], _city_industry_label, country, "error", job.start_time, {})

        threading.Thread(target=run, daemon=True).start()
        return jsonify({"job_id": job_id})

    def _gpi_session_total_for_live() -> int:
        """Read the Google Places module's session counter for the /status
        live block. Wrapped so the lookup is robust to import errors."""
        try:
            import google_places_intent as _gpi
            return int(_gpi.get_session_calls_made())
        except Exception:
            return 0

    def _gpi_per_run_cap_for_live() -> int:
        try:
            import google_places_intent as _gpi
            return int(getattr(_gpi, "MAX_PLACES_CALLS", 25))
        except Exception:
            return 25

    @app.route("/status/<job_id>")
    def get_status(job_id):
        job = _jobs.get(job_id)
        if not job:
            return jsonify({"error": "Job not found"}), 404
        new_logs = job.logs[job.log_cursor:]
        job.log_cursor = len(job.logs)
        elapsed = time.time() - job.start_time
        progress = max(job.progress, 1)
        remaining = (elapsed / progress) * (100 - progress) if progress < 100 else 0
        result = {"state": job.state, "progress": job.progress,
                  "status_text": job.status_text, "new_logs": new_logs,
                  "elapsed_seconds": round(elapsed),
                  "time_remaining_seconds": round(remaining)}
        # 2026-05-18 (round 2): expose LIVE SEMrush + Apollo telemetry while
        # the job is still running so the frontend can render a real-time
        # credit counter updating every 500ms. Reads straight from the
        # pipeline's shared `_api_counter` (canonical source of truth).
        # Falls back gracefully when the pipeline hasn't been wired up yet
        # (very first poll after submit).
        try:
            _pl = job.pipeline
            _ctr = getattr(_pl, "_api_counter", None) if _pl is not None else None
            if isinstance(_ctr, dict):
                _phases_live = _ctr.get("semrush_units_by_phase")
                _phases_dict = dict(_phases_live) if isinstance(_phases_live, dict) else {}
                # 2026-05-18 (round 4): Apollo has 3 independently-billed pools
                # (export / email / phone). The pipeline tracks successful
                # email/phone reveals in `_email_credits_used` /
                # `_phone_credits_used` already — surface them here so the
                # frontend can render per-pool live tracking parity with
                # SEMrush. Export pool isn't consumed mid-run (only on CSV
                # finalize) so it stays 0 during the run itself.
                result["live"] = {
                    "semrush_calls": int(_ctr.get("semrush", 0) or 0),
                    "semrush_units_used": int(_ctr.get("semrush_units", 0) or 0),
                    "semrush_unit_budget": int(_ctr.get("semrush_budget", 0) or 0),
                    "semrush_skipped": int(_ctr.get("semrush_skipped", 0) or 0),
                    "semrush_alert_75": bool(_ctr.get("semrush_budget_alert_75", False)),
                    "semrush_units_by_phase": _phases_dict,
                    "apollo_calls": int(_ctr.get("apollo", 0) or 0),
                    "apollo_budget": int(getattr(_pl, "_apollo_budget", 0) or 0),
                    "apollo_email_reveals": int(getattr(_pl, "_email_credits_used", 0) or 0),
                    "apollo_phone_reveals": int(getattr(_pl, "_phone_credits_used", 0) or 0),
                    "apollo_export_used": 0,   # not consumed mid-run
                    "lusha_calls": int(_ctr.get("lusha", 0) or 0),
                    "serpapi_calls": int(_ctr.get("serpapi", 0) or 0),
                    "hunter_calls": int(_ctr.get("hunter", 0) or 0),
                    "openai_calls": int(_ctr.get("openai", 0) or 0),
                    # 2026-05-21: Google Places per-run + session-cumulative.
                    # `google_places_calls` is the per-run count from the
                    # pipeline's shared counter; `session_total` is the
                    # process-wide cumulative from the module global so the
                    # frontend "Live API spend" card can show both.
                    "google_places_calls": int(_ctr.get("google_places", 0) or 0),
                    "google_places_session_total": _gpi_session_total_for_live(),
                    "google_places_per_run_cap": int(_gpi_per_run_cap_for_live()),
                }
        except Exception:
            pass
        if job.state == "done":
            result["leads"] = job.leads
            result["top_csv"] = job.top_csv
            result["all_csv"] = job.all_csv
            result["api_usage"] = job.api_usage
            result["summary"] = job.summary  # V5.13: Token & lead summary
        if job.state == "error":
            result["error"] = job.error
            result["api_usage"] = job.api_usage
        return jsonify(result)

    @app.route("/cancel", methods=["POST"])
    def cancel():
        for jid in reversed(list(_jobs.keys())):
            j = _jobs[jid]
            if j.state == "running" and j.pipeline:
                j.pipeline.cancel()
                return jsonify({"status": "cancelling"})
        return jsonify({"status": "no active job"})

    @app.route("/api/credits")
    def get_credits():
        data = _fetch_credits(force=False)
        data["cached"] = (time.time() - _credits_cache["timestamp"]) > 1
        return jsonify(data)

    @app.route("/api/credits/refresh", methods=["POST"])
    def refresh_credits():
        return jsonify(_fetch_credits(force=True))

    @app.route("/api/test-google-places")
    def test_google_places():
        """2026-05-21: One-shot smoke test of the Google Places key + AU
        Text Search. Hit this from the browser AS-IS to confirm Google is
        actually returning AU businesses with websites.

        Optional query params:
          ?query=...  default: "commercial plumber Sydney Australia"
          ?key=...    overrides API_KEYS["google_places"] for this call

        Returns:
          { ok: bool, query, key_set, google_status, results_count,
            sample_domains: [...], error_message: "..." }
        """
        from urllib.parse import unquote
        q = unquote(flask_request.args.get("query", "") or "commercial plumber Sydney Australia")
        key = (flask_request.args.get("key") or API_KEYS.get("google_places") or "").strip()
        if not key:
            return jsonify({
                "ok": False, "query": q, "key_set": False,
                "error_message": "No GOOGLE_PLACES_API_KEY set in env. "
                                 "Set it in .env (local) or Railway → Variables (prod), "
                                 "OR pass ?key=... to this endpoint to test a key inline.",
            }), 200
        try:
            # 2026-05-21: hits the Places API (New) v1 — same endpoint the
            # production pipeline uses. Legacy /maps/api/place/textsearch
            # is deprecated and returns REQUEST_DENIED on new GCP projects.
            r = requests.post(
                "https://places.googleapis.com/v1/places:searchText",
                headers={
                    "Content-Type":      "application/json",
                    "X-Goog-Api-Key":    key,
                    "X-Goog-FieldMask":  "places.id,places.displayName,places.websiteUri,places.formattedAddress",
                },
                json={"textQuery": q, "regionCode": "AU", "pageSize": 10},
                timeout=20,
            )
            try:
                data = r.json()
            except Exception:
                data = {"raw": r.text[:500]}
            places = data.get("places") or []
            err_obj = data.get("error") or {}
            sample_names, sample_domains = [], []
            for p in places[:10]:
                disp = (p.get("displayName") or {}).get("text", "") if isinstance(p.get("displayName"), dict) else ""
                sample_names.append(disp)
                w = (p.get("websiteUri") or "").strip()
                if w:
                    try:
                        from urllib.parse import urlparse
                        d = (urlparse(w if "://" in w else "http://"+w).netloc or "").lower()
                        if d.startswith("www."): d = d[4:]
                        if d: sample_domains.append(d)
                    except Exception:
                        pass
            return jsonify({
                "ok": (r.status_code == 200 and not err_obj),
                "http_status": r.status_code,
                "query": q,
                "key_set": True,
                "key_preview": key[:8] + "..." + key[-4:] if len(key) > 12 else "(short)",
                "google_status": err_obj.get("status") or ("OK" if r.status_code == 200 else "ERROR"),
                "error_message": err_obj.get("message", ""),
                "results_count": len(places),
                "sample_place_names": sample_names[:5],
                "sample_domains_with_website": sample_domains,
                "api_version": "Places API (New) v1",
                "note": ("If error_message mentions 'legacy API', enable the "
                         "Places API (New) in your GCP console at "
                         "https://console.cloud.google.com/apis/library/places.googleapis.com"),
            }), 200
        except Exception as exc:
            return jsonify({
                "ok": False, "query": q, "key_set": True,
                "error_message": f"Exception: {type(exc).__name__}: {exc}",
            }), 500

    # ── Start ────────────────────────────────────────────────────────────────
    port = int(os.environ.get("PORT", 5001))  # Railway sets PORT env var; local default 5001 (avoids PyMaster on 5000)
    print("=" * 60)
    print("  LeadForge V5.7 — Web Interface")
    print(f"  Server running on port {port}")
    if not os.environ.get("PORT"):  # Only auto-open browser on local dev (no PORT env var = local)
        print(f"  Opening browser at http://localhost:{port}")
        threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    app.run(host="0.0.0.0", port=port, debug=False)

if __name__ == "__main__":
    main_web()
