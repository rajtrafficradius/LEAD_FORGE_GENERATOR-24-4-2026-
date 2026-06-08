"""
utils.py — Lead Generation Pro
Miscellaneous utility functions for data cleaning, validation,
phone/email normalisation, and CSV post-processing.
"""

import re
import csv
import json
import hashlib
import unicodedata
from datetime import datetime
from typing import Optional


# ── Phone Normalisation ──────────────────────────────────────────────────────

_COUNTRY_DIAL = {
    "AU":    ("+61",  r"(?:\+61|0)([2-9]\d{8})"),
    "USA":   ("+1",   r"(?:\+1)?([2-9]\d{2}[2-9]\d{6})"),
    "UK":    ("+44",  r"(?:\+44|0)([1-9]\d{9,10})"),
    "India": ("+91",  r"(?:\+91|0)([6-9]\d{9})"),
}


def normalise_phone(raw: str, country: str = "AU") -> Optional[str]:
    """
    Normalise a raw phone string to E.164 format.

    >>> normalise_phone("0412 345 678", "AU")
    '+61412345678'
    >>> normalise_phone("(212) 555-0199", "USA")
    '+12125550199'
    """
    if not raw:
        return None
    digits_only = re.sub(r"[^\d+]", "", raw)
    prefix, pattern = _COUNTRY_DIAL.get(country, ("+61", r"(?:\+61|0)([2-9]\d{8})"))
    m = re.search(pattern, digits_only)
    if m:
        return f"{prefix}{m.group(1)}"
    # Fallback: strip leading zeros and prepend prefix
    stripped = digits_only.lstrip("0+")
    if 7 <= len(stripped) <= 12:
        return f"{prefix}{stripped}"
    return None


def is_valid_phone(phone: str) -> bool:
    """Return True if phone looks like a valid E.164 number."""
    return bool(phone and re.match(r"^\+\d{7,15}$", phone.strip()))


# ── Email Utilities ──────────────────────────────────────────────────────────

_GENERIC_PREFIXES = frozenset({
    "info", "hello", "contact", "enquiries", "enquiry", "admin",
    "support", "sales", "office", "reception", "team", "mail",
    "noreply", "no-reply", "donotreply", "webmaster", "accounts",
    "billing", "bookings", "help", "service", "services",
})

_DISPOSABLE_DOMAINS = frozenset({
    "mailinator.com", "guerrillamail.com", "tempmail.com",
    "throwam.com", "yopmail.com", "sharklasers.com",
})


def is_generic_email(email: str) -> bool:
    """Return True if the email looks like a generic/role-based address."""
    if not email or "@" not in email:
        return True
    local = email.lower().split("@")[0]
    return local in _GENERIC_PREFIXES


def is_disposable_email(email: str) -> bool:
    """Return True if the email domain is a known disposable provider."""
    if not email or "@" not in email:
        return False
    domain = email.lower().split("@")[-1]
    return domain in _DISPOSABLE_DOMAINS


def is_valid_email(email: str) -> bool:
    """Basic RFC-5322-ish email validation."""
    return bool(
        email
        and re.match(
            r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$",
            email.strip(),
        )
    )


def extract_domain_from_email(email: str) -> Optional[str]:
    """Return the domain part of an email address."""
    if email and "@" in email:
        return email.lower().split("@")[-1].strip()
    return None


# ── Name Utilities ───────────────────────────────────────────────────────────

_HONORIFICS = {"mr", "mrs", "ms", "dr", "prof", "rev", "sir", "mx"}


def clean_name(raw: str) -> str:
    """
    Strip honorifics, normalise whitespace, title-case.

    >>> clean_name("  dr. john  SMITH  ")
    'John Smith'
    """
    if not raw:
        return ""
    tokens = raw.strip().split()
    filtered = [t for t in tokens if t.lower().rstrip(".") not in _HONORIFICS]
    return " ".join(filtered).title()


def is_full_name(name: str) -> bool:
    """Return True if name contains at least two words (first + last)."""
    return bool(name and len(name.strip().split()) >= 2)


def name_to_initials(name: str) -> str:
    """
    Convert a full name to initials.

    >>> name_to_initials("Matthew James Cornell")
    'M.J.C.'
    """
    return "".join(f"{w[0].upper()}." for w in name.split() if w)


def slug_to_name(slug: str) -> str:
    """
    Convert a URL slug to a title-cased name.

    >>> slug_to_name("matthew-cornell-photography")
    'Matthew Cornell Photography'
    """
    return " ".join(w.capitalize() for w in re.split(r"[-_]", slug) if w)


# ── Domain Utilities ─────────────────────────────────────────────────────────

_TLD_SUFFIXES = {
    ".com.au", ".co.uk", ".co.in", ".com.sg", ".co.nz",
    ".com", ".net", ".org", ".io", ".co", ".biz", ".info",
}


def strip_domain_tld(domain: str) -> str:
    """
    Remove the TLD suffix from a domain, returning the base slug.

    >>> strip_domain_tld("matthewcornell.com.au")
    'matthewcornell'
    """
    d = domain.lower().removeprefix("www.")
    for tld in sorted(_TLD_SUFFIXES, key=len, reverse=True):
        if d.endswith(tld):
            return d[: -len(tld)]
    return d


def domain_from_url(url: str) -> Optional[str]:
    """Extract bare domain (no scheme, no path) from a URL."""
    m = re.match(r"(?:https?://)?([^/\s]+)", url.strip())
    if m:
        return m.group(1).lower().removeprefix("www.")
    return None


def is_valid_domain(domain: str) -> bool:
    """Basic domain format check (no scheme, has dot, no spaces)."""
    return bool(
        domain
        and "." in domain
        and " " not in domain
        and re.match(r"^[a-zA-Z0-9.\-]+$", domain)
    )


# ── CSV Helpers ──────────────────────────────────────────────────────────────

LEAD_FIELDNAMES = [
    "name", "email", "phone", "company", "role", "domain",
    "country", "email_type", "source", "scraped_at",
]


def write_leads_csv(leads: list[dict], filepath: str) -> int:
    """
    Write a list of lead dicts to a CSV file.
    Returns number of rows written.
    """
    if not leads:
        return 0
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEAD_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    return len(leads)


def read_leads_csv(filepath: str) -> list[dict]:
    """Read a leads CSV file, returning a list of dicts."""
    rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def deduplicate_leads(leads: list[dict]) -> list[dict]:
    """
    Remove duplicate leads.
    Deduplication keys: (domain + normalised_name) and (domain + email).
    """
    seen_name_domain: set[str] = set()
    seen_email_domain: set[str] = set()
    result = []
    for lead in leads:
        domain = (lead.get("domain") or "").lower()
        email = (lead.get("email") or "").lower()
        name_key = re.sub(r"\s+", "", (lead.get("name") or "").lower())

        nd_key = f"{domain}|{name_key}"
        ed_key = f"{domain}|{email}"

        if name_key and nd_key in seen_name_domain:
            continue
        if email and ed_key in seen_email_domain:
            continue

        if name_key:
            seen_name_domain.add(nd_key)
        if email:
            seen_email_domain.add(ed_key)
        result.append(lead)
    return result


def merge_leads(primary: dict, secondary: dict) -> dict:
    """
    Merge two lead dicts — primary values win; secondary fills in blanks.
    Source tags are concatenated.
    """
    merged = dict(primary)
    for key, val in secondary.items():
        if key == "source":
            merged["source"] = f"{primary.get('source', '')} {val}".strip()
        elif not merged.get(key) and val:
            merged[key] = val
    return merged


# ── Hashing / Fingerprinting ─────────────────────────────────────────────────

def lead_fingerprint(lead: dict) -> str:
    """
    Generate a stable MD5 fingerprint for a lead based on its core fields.
    Useful for change-detection between pipeline runs.
    """
    key = "|".join([
        (lead.get("domain") or "").lower(),
        (lead.get("email") or "").lower(),
        re.sub(r"\s+", "", (lead.get("name") or "").lower()),
    ])
    return hashlib.md5(key.encode()).hexdigest()


# ── Text Normalisation ───────────────────────────────────────────────────────

def remove_accents(text: str) -> str:
    """
    Remove diacritics/accents from unicode text.

    >>> remove_accents("Ångström Café")
    'Angstrom Cafe'
    """
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def truncate(text: str, max_len: int = 80, ellipsis: str = "…") -> str:
    """Truncate text to max_len characters, appending ellipsis if truncated."""
    if len(text) <= max_len:
        return text
    return text[: max_len - len(ellipsis)] + ellipsis


def safe_filename(text: str) -> str:
    """
    Convert arbitrary text to a safe filename (no special chars).

    >>> safe_filename("Smith & Jones / Dental Co.")
    'Smith___Jones___Dental_Co_'
    """
    return re.sub(r"[^\w\-]", "_", text).strip("_")


# ── Timestamp Helpers ────────────────────────────────────────────────────────

def timestamp_slug() -> str:
    """Return a filesystem-safe timestamp string like '20241205_143022'."""
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def format_duration(seconds: float) -> str:
    """
    Format a duration in seconds to a human-readable string.

    >>> format_duration(754)
    '12m 34s'
    """
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


# ── JSON Helpers ─────────────────────────────────────────────────────────────

def safe_json_get(data: dict, *keys, default=None):
    """
    Safely traverse nested dict/list with a chain of keys.

    >>> safe_json_get({"a": {"b": 42}}, "a", "b")
    42
    >>> safe_json_get({"a": {}}, "a", "b", "c", default="?")
    '?'
    """
    cursor = data
    for key in keys:
        if isinstance(cursor, dict):
            cursor = cursor.get(key)
        elif isinstance(cursor, list) and isinstance(key, int):
            cursor = cursor[key] if key < len(cursor) else None
        else:
            return default
        if cursor is None:
            return default
    return cursor


def flatten_lead(lead: dict) -> dict:
    """
    Flatten a nested Apollo/Lusha API response into a flat lead dict
    matching LEAD_FIELDNAMES schema.
    """
    org = lead.get("organization") or {}
    phone_obj = org.get("primary_phone") or {}
    return {
        "name": " ".join(filter(None, [
            lead.get("first_name"), lead.get("last_name")
        ])).strip() or None,
        "email": lead.get("email"),
        "phone": phone_obj.get("sanitized_number") or lead.get("phone"),
        "company": org.get("name") or lead.get("company_name"),
        "role": lead.get("title"),
        "domain": lead.get("domain"),
        "country": lead.get("country"),
        "email_type": lead.get("email_type", "Unknown"),
        "source": lead.get("source", "Apollo"),
        "scraped_at": lead.get("scraped_at", timestamp_slug()),
    }


# ── Master-DB Dedup Keys ─────────────────────────────────────────────────────
#
# These two helpers define the canonical (normalized_name, root_domain) pair
# used as the UNIQUE KEY on master_leads. Must be deterministic: the same
# logical person+company must map to the same key across runs, regardless of
# casing, leading/trailing whitespace, or common punctuation noise.

_MASTER_KEY_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)   # strip punctuation, keep word chars + spaces
_MASTER_KEY_WS_RE = re.compile(r"\s+")                        # collapse any run of whitespace to one space


def normalize_master_key(name: str) -> str:
    """Canonicalize a person name for master-DB dedup.

    Strips honorifics, diacritics, punctuation, and collapses whitespace to
    one space. Returns lowercase. Matches the UNIQUE KEY contract on
    master_leads.normalized_name.

    >>> normalize_master_key("  Dr. John  SMITH ")
    'john smith'
    >>> normalize_master_key("Renée O'Connor")
    'renee oconnor'
    """
    if not name:
        return ""
    # Reuse clean_name to drop honorifics + title-case, then flatten
    cleaned = clean_name(name)
    deaccented = remove_accents(cleaned)
    no_punct = _MASTER_KEY_PUNCT_RE.sub("", deaccented)
    collapsed = _MASTER_KEY_WS_RE.sub(" ", no_punct).strip().lower()
    return collapsed


def root_domain(domain: str) -> str:
    """Canonicalize a domain for master-DB dedup.

    Lowercases, strips scheme, strips leading 'www.'. Preserves the TLD
    (unlike strip_domain_tld). Handles full URLs gracefully.

    >>> root_domain("https://WWW.SmithDental.com.au/contact")
    'smithdental.com.au'
    >>> root_domain("acme.com")
    'acme.com'
    """
    if not domain:
        return ""
    d = domain_from_url(domain) or domain.lower().strip()
    return d.removeprefix("www.").strip()


# ── Quick self-test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import doctest
    doctest.testmod(verbose=True)

    # Smoke tests
    assert normalise_phone("0412 345 678", "AU") == "+61412345678"
    assert is_valid_email("matt@example.com.au")
    assert not is_valid_email("not-an-email")
    assert is_generic_email("info@example.com")
    assert not is_generic_email("matt@example.com")
    assert is_full_name("Matthew Cornell")
    assert not is_full_name("Matthew")
    assert strip_domain_tld("matthewcornell.com.au") == "matthewcornell"
    assert domain_from_url("https://www.smithdental.com.au/contact") == "smithdental.com.au"
    assert clean_name("  Dr. John SMITH  ") == "John Smith"
    assert format_duration(754) == "12m 34s"
    assert format_duration(3661) == "1h 1m 1s"
    assert safe_json_get({"a": {"b": [10, 20]}}, "a", "b", 1) == 20
    # Master-key smoke
    assert normalize_master_key("  Dr. John  SMITH ") == "john smith"
    assert normalize_master_key("Renée O'Connor") == "renee oconnor"
    assert normalize_master_key("") == ""
    assert root_domain("https://WWW.SmithDental.com.au/contact") == "smithdental.com.au"
    assert root_domain("acme.com") == "acme.com"
    assert root_domain("") == ""
    print("All smoke tests passed.")
