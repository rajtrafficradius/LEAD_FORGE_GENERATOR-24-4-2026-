"""
cities_au.py — Australian state / tier / city dataset + industry-share weights.

Used by the "Generate leads through cities" mode (see city_pipeline.py).

Two datasets live in this module:

1. AUSTRALIA_STATES
   Mapping: state_code -> {"name": str, "tier_1": [...], "tier_2": [...], "tier_3": [...]}
   Covers the 8 AU states/territories. Tier 1 = capital / largest metros,
   Tier 2 = major regional cities, Tier 3 = smaller regional hubs.
   "Australia" is a synthetic entry meaning "whole country" (all capitals).

2. INDUSTRY_BASE_WEIGHTS + STATE_INDUSTRY_MULTIPLIERS
   Produces a plausible % market-share breakdown of lead-generation demand
   per state and industry. Used to pick which industries to search given a
   max_leads percentage cap. Numbers are approximate/heuristic — calibrated
   from general knowledge of AU state economies (NSW/VIC finance+pro
   services, QLD tourism+trades, WA mining+trades, etc). The helper
   get_industry_shares(state) returns a normalised dict {industry: pct}
   summing to 100.

Nothing in here talks to an API — it's pure data + one normalisation helper.
"""

from __future__ import annotations


# ──────────────────────────────────────────────────────────────────────────────
# STATE / TIER / CITY DATASET
# ──────────────────────────────────────────────────────────────────────────────

AUSTRALIA_STATES: dict = {
    "NSW": {
        "name": "New South Wales",
        "tier_1": ["Sydney"],
        "tier_2": ["Newcastle", "Wollongong", "Central Coast", "Parramatta"],
        "tier_3": [
            "Wagga Wagga", "Albury", "Coffs Harbour", "Port Macquarie",
            "Tamworth", "Orange", "Dubbo", "Bathurst", "Armidale",
            "Goulburn", "Lismore", "Nowra", "Maitland", "Tweed Heads",
        ],
    },
    "VIC": {
        "name": "Victoria",
        "tier_1": ["Melbourne"],
        "tier_2": ["Geelong", "Ballarat", "Bendigo", "Frankston"],
        "tier_3": [
            "Shepparton", "Mildura", "Warrnambool", "Wodonga",
            "Traralgon", "Horsham", "Sale", "Bairnsdale",
            "Pakenham", "Melton", "Cranbourne",
        ],
    },
    "QLD": {
        "name": "Queensland",
        "tier_1": ["Brisbane"],
        "tier_2": ["Gold Coast", "Sunshine Coast", "Townsville", "Cairns", "Toowoomba"],
        "tier_3": [
            "Mackay", "Rockhampton", "Bundaberg", "Hervey Bay",
            "Gladstone", "Maryborough", "Mount Isa", "Ipswich",
            "Logan", "Redcliffe", "Caboolture",
        ],
    },
    "WA": {
        "name": "Western Australia",
        "tier_1": ["Perth"],
        "tier_2": ["Mandurah", "Bunbury", "Fremantle", "Joondalup"],
        "tier_3": [
            "Geraldton", "Kalgoorlie", "Albany", "Karratha",
            "Port Hedland", "Broome", "Busselton", "Esperance",
            "Rockingham",
        ],
    },
    "SA": {
        "name": "South Australia",
        "tier_1": ["Adelaide"],
        "tier_2": ["Mount Gambier", "Whyalla"],
        "tier_3": [
            "Port Lincoln", "Port Augusta", "Murray Bridge",
            "Victor Harbor", "Port Pirie", "Gawler", "Mount Barker",
        ],
    },
    "TAS": {
        "name": "Tasmania",
        "tier_1": ["Hobart"],
        "tier_2": ["Launceston"],
        "tier_3": ["Devonport", "Burnie", "Kingston", "Ulverstone", "George Town"],
    },
    "ACT": {
        "name": "Australian Capital Territory",
        "tier_1": ["Canberra"],
        "tier_2": ["Belconnen", "Woden", "Tuggeranong", "Gungahlin"],
        "tier_3": ["Queanbeyan"],
    },
    "NT": {
        "name": "Northern Territory",
        "tier_1": ["Darwin"],
        "tier_2": ["Alice Springs", "Palmerston"],
        "tier_3": ["Katherine", "Tennant Creek", "Nhulunbuy"],
    },
}

# Synthetic "whole country" bucket. When the user picks "Australia" the
# pipeline should treat every tier-1 city across all states as the target
# set. We compute this dynamically from AUSTRALIA_STATES so additions stay
# in sync.

def _australia_all_tier(tier_key: str) -> list:
    out: list = []
    for s in AUSTRALIA_STATES.values():
        out.extend(s.get(tier_key, []))
    return out


AUSTRALIA_NATIONAL = {
    "name": "Australia",
    "tier_1": _australia_all_tier("tier_1"),
    "tier_2": _australia_all_tier("tier_2"),
    "tier_3": _australia_all_tier("tier_3"),
}


# ──────────────────────────────────────────────────────────────────────────────
# INDUSTRY MARKET-SHARE WEIGHTS
# ──────────────────────────────────────────────────────────────────────────────

# Base (national) weight per industry. Higher = bigger share of lead demand.
# These are rough, order-of-magnitude — they exist to order the industry
# picker so max_leads=30 picks the 30% biggest industries first, not to be
# economically precise.
INDUSTRY_BASE_WEIGHTS: dict = {
    # Trades (high demand, broad market)
    "Plumber": 6.0,
    "Electrician": 6.0,
    "Auto Repair / Mechanic": 5.5,
    "HVAC": 4.5,
    "Roofing": 3.5,
    "Cleaning Service": 4.0,
    "Painter / Decorator": 3.5,
    "Landscaping": 3.5,
    "Construction": 5.0,
    "Pest Control": 2.0,
    "Locksmith": 1.5,
    "Moving Company": 2.0,
    "Concreting": 2.5,
    "Fencing": 2.5,
    "Tree Service / Arborist": 2.0,
    "Carpet Cleaning": 2.0,
    "Gutter Cleaning": 1.5,
    "Pressure Washing / Exterior Cleaning": 1.5,
    "Waterproofing": 1.5,
    "Garage Door Services": 1.5,
    "Bathroom Renovation": 2.5,
    "Kitchen Renovation": 2.5,
    "Blinds / Shutters": 1.5,
    "Tyre Shop": 2.0,
    "Smash Repairs": 2.0,
    "Towing Service": 1.5,
    "Skip Bin Hire": 1.5,
    "Solar Panel Installation": 3.0,
    "Pool Cleaning": 1.5,
    "Pool Builder": 2.0,

    # Healthcare
    "Doctor / General Practitioner": 4.0,
    "Dentist": 4.5,
    "Orthodontist": 1.5,
    "Chiropractor": 2.0,
    "Physiotherapy": 3.0,
    "Veterinarian": 2.5,
    "Optometrist / Eye Care": 2.0,
    "Podiatrist": 1.0,
    "Dermatologist": 1.0,
    "Massage Therapist": 2.0,
    "Pharmacy": 2.5,
    "Psychologist": 2.5,
    "Cosmetic Clinic": 1.5,
    "Laser Clinic": 1.0,
    "Plastic Surgeon": 0.8,
    "Fertility / IVF Clinic": 0.5,

    # Professional services
    "Lawyer / Attorney": 4.0,
    "Accountant": 4.0,
    "Bookkeeping": 2.5,
    "Financial Advisor": 2.5,
    "Insurance Agent": 2.5,
    "Mortgage Broker": 3.0,
    "Conveyancer": 1.5,
    "Buyers Agent": 1.0,
    "Business Coach": 1.0,
    "Recruitment Agency": 2.0,
    "Labour Hire": 1.5,

    # Real estate
    "Real Estate Agent": 5.5,
    "Property Management": 3.0,
    "Home Inspector": 1.0,

    # Education / care
    "Tutor / Education": 2.0,
    "Music Teacher / Music School": 1.0,
    "Driving School": 1.5,
    "Child Care / Day Care": 2.5,
    "Private School": 1.0,
    "NDIS Provider": 2.5,
    "Aged Care Services": 2.0,

    # Digital / marketing
    "IT Services": 3.5,
    "Marketing Agency": 3.0,
    "Web Developer / Web Design": 2.5,
    "Graphic Designer": 1.5,
    "Copywriter / Content Writer": 1.0,
    "Software Development Agency": 1.5,
    "Cybersecurity Services": 1.0,

    # Hospitality / lifestyle
    "Restaurant / Cafe": 5.0,
    "Gym / Fitness": 3.5,
    "Yoga / Pilates Studio": 1.5,
    "Personal Trainer": 2.0,
    "Salon / Spa / Beauty": 4.0,
    "Tattoo Artist / Body Art": 1.0,
    "Pet Grooming": 1.5,
    "Travel Agent": 1.5,

    # Creative / lifecycle
    "Photographer": 2.5,
    "Wedding Planner / Event Planner": 1.5,
    "Florist / Flower Shop": 1.5,
    "Baker / Bakery": 1.5,
    "Caterer / Catering": 2.0,
    "Funeral Director": 1.0,
    "Printing Service": 1.0,

    # Design
    "Architecture": 2.0,
    "Interior Designer": 1.5,

    # Security
    "Security Systems / CCTV": 1.5,
}


# Per-state multipliers on top of the national baseline. Missing keys = 1.0.
# Values >1 = this state over-indexes on that industry; <1 = under-indexes.
# Chosen from general knowledge of the state economies; not ABS-precise.
STATE_INDUSTRY_MULTIPLIERS: dict = {
    "NSW": {  # Finance, law, real estate, corporate services (Sydney heavy)
        "Lawyer / Attorney": 1.35,
        "Accountant": 1.25,
        "Financial Advisor": 1.40,
        "Mortgage Broker": 1.35,
        "Real Estate Agent": 1.30,
        "Property Management": 1.30,
        "Buyers Agent": 1.50,
        "Marketing Agency": 1.30,
        "IT Services": 1.25,
        "Software Development Agency": 1.30,
        "Cybersecurity Services": 1.25,
        "Cosmetic Clinic": 1.25,
        "Plastic Surgeon": 1.30,
        "Restaurant / Cafe": 1.15,
        "Wedding Planner / Event Planner": 1.20,
    },
    "VIC": {  # Healthcare, education, design, finance (Melbourne heavy)
        "Doctor / General Practitioner": 1.15,
        "Dentist": 1.10,
        "Physiotherapy": 1.15,
        "Psychologist": 1.20,
        "Accountant": 1.20,
        "Financial Advisor": 1.15,
        "Architecture": 1.25,
        "Interior Designer": 1.30,
        "Graphic Designer": 1.20,
        "Marketing Agency": 1.20,
        "Restaurant / Cafe": 1.25,
        "Salon / Spa / Beauty": 1.15,
        "Tutor / Education": 1.15,
        "Private School": 1.20,
    },
    "QLD": {  # Tourism, trades, pools, outdoors (warm climate)
        "Plumber": 1.10,
        "Electrician": 1.10,
        "HVAC": 1.30,
        "Roofing": 1.15,
        "Pool Cleaning": 1.70,
        "Pool Builder": 1.65,
        "Landscaping": 1.20,
        "Pressure Washing / Exterior Cleaning": 1.30,
        "Solar Panel Installation": 1.40,
        "Real Estate Agent": 1.15,
        "Tree Service / Arborist": 1.20,
        "Pest Control": 1.25,
        "Tyre Shop": 1.10,
        "Smash Repairs": 1.10,
        "Restaurant / Cafe": 1.15,
        "Travel Agent": 1.25,
    },
    "WA": {  # Mining, construction, trades (Perth + FIFO)
        "Construction": 1.40,
        "Electrician": 1.25,
        "Plumber": 1.15,
        "HVAC": 1.20,
        "Auto Repair / Mechanic": 1.20,
        "Tyre Shop": 1.25,
        "Labour Hire": 1.50,
        "Recruitment Agency": 1.20,
        "Solar Panel Installation": 1.35,
        "Roofing": 1.15,
        "Pool Builder": 1.30,
        "Pool Cleaning": 1.40,
        "Concreting": 1.25,
        "Towing Service": 1.15,
    },
    "SA": {  # Wine, manufacturing, aged care
        "Aged Care Services": 1.25,
        "Landscaping": 1.10,
        "Auto Repair / Mechanic": 1.10,
        "Electrician": 1.05,
        "Plumber": 1.05,
        "Caterer / Catering": 1.15,
        "Funeral Director": 1.15,
    },
    "TAS": {  # Tourism, agriculture, trades
        "Plumber": 1.10,
        "Electrician": 1.10,
        "Tree Service / Arborist": 1.30,
        "Fencing": 1.25,
        "Concreting": 1.15,
        "Restaurant / Cafe": 1.15,
        "Travel Agent": 1.30,
        "Wedding Planner / Event Planner": 1.20,
        "Aged Care Services": 1.20,
        "Funeral Director": 1.20,
    },
    "ACT": {  # Government, IT, consulting
        "IT Services": 1.60,
        "Software Development Agency": 1.55,
        "Cybersecurity Services": 1.90,
        "Marketing Agency": 1.20,
        "Business Coach": 1.25,
        "Recruitment Agency": 1.30,
        "Lawyer / Attorney": 1.20,
        "Accountant": 1.15,
        "Private School": 1.30,
        "Child Care / Day Care": 1.20,
    },
    "NT": {  # Tourism, mining, tradies
        "Plumber": 1.20,
        "Electrician": 1.20,
        "HVAC": 1.40,
        "Auto Repair / Mechanic": 1.25,
        "Towing Service": 1.35,
        "Tyre Shop": 1.25,
        "Labour Hire": 1.30,
        "Travel Agent": 1.40,
        "Pool Cleaning": 1.50,
        "Pool Builder": 1.35,
        "Pest Control": 1.40,
    },
    # Synthetic "whole country" uses base weights (all multipliers = 1)
    "AUSTRALIA": {},
}


def get_industry_shares(state_code: str) -> dict:
    """Return {industry: pct_share} summing to 100 for the given state_code.

    state_code: one of AUSTRALIA_STATES keys, or "AUSTRALIA" for the whole country.
    Unknown codes fall back to the national baseline.
    """
    mults = STATE_INDUSTRY_MULTIPLIERS.get(state_code.upper(), {})
    raw = {ind: base * mults.get(ind, 1.0)
           for ind, base in INDUSTRY_BASE_WEIGHTS.items()}
    total = sum(raw.values()) or 1.0
    return {ind: (w / total) * 100.0 for ind, w in raw.items()}


def select_industries_by_cap(state_code: str, pct_cap: float) -> list:
    """Greedy-pick industries (largest share first) until cumulative share
    reaches pct_cap. Returns a list of industry names.

    pct_cap <= 0   -> empty list
    pct_cap >= 100 -> all industries
    """
    if pct_cap <= 0:
        return []
    shares = get_industry_shares(state_code)
    ordered = sorted(shares.items(), key=lambda kv: kv[1], reverse=True)
    if pct_cap >= 100:
        return [ind for ind, _ in ordered]
    picked: list = []
    cum = 0.0
    for ind, share in ordered:
        picked.append(ind)
        cum += share
        if cum >= pct_cap:
            break
    return picked


def list_states_payload() -> dict:
    """Return a JSON-serialisable payload the frontend dropdown reads.

    Shape:
      {
        "states": [
          {"code": "NSW", "name": "New South Wales",
           "tier_1": [...], "tier_2": [...], "tier_3": [...]},
          ...
        ],
        "australia": {"tier_1": [...], "tier_2": [...], "tier_3": [...]}
      }
    """
    states_list = []
    for code, data in AUSTRALIA_STATES.items():
        states_list.append({
            "code": code,
            "name": data["name"],
            "tier_1": data["tier_1"],
            "tier_2": data["tier_2"],
            "tier_3": data["tier_3"],
        })
    return {
        "states": states_list,
        "australia": {
            "tier_1": AUSTRALIA_NATIONAL["tier_1"],
            "tier_2": AUSTRALIA_NATIONAL["tier_2"],
            "tier_3": AUSTRALIA_NATIONAL["tier_3"],
        },
    }
