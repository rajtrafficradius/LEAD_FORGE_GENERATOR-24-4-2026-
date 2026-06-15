"""enrichment_worker.py — background lead-enrichment worker (CRM Phase 2).

Runs as a single always-on daemon thread, SEPARATE from the lead-generation
pipeline (it never imports or calls it). For each lead that still needs
enrichment it:

  1. CRAWLS the website with plain Python (requests + BeautifulSoup) — homepage
     plus a few high-signal pages (about / services / products / contact /
     pricing). No OpenAI is used for crawling.
  2. Pulls ALL FREE Apollo organization data (organizations/enrich) — the full
     org object (revenue incl. 0/NA, employees, industry, keywords, founded
     year, location, LinkedIn, SIC, descriptions, technologies…).
  3. Has OpenAI ANALYZE the already-fetched text into a compact "what a caller
     needs" cheat-sheet (business model, what they sell, talking points, etc.).

Results land in the lead_enrichment table. The worker:
  * prioritises leads assigned to a BDE, then newest, then the backlog;
  * pauses while a /generate* run is active (a callback is injected by wsgi) so
    it never competes with a live run for OpenAI/Apollo;
  * is fully fail-open: any error on a lead marks just that row 'error' and the
    loop continues.

Tunables (env): ENRICH_WORKER=0 disables it; ENRICH_POLL_SECONDS,
ENRICH_PER_LEAD_DELAY, ENRICH_MAX_PAGES, ENRICH_MAX_CHARS, ENRICH_OPENAI_MODEL.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests

log = logging.getLogger("leadforge.enrich")

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# High-signal internal pages a salesperson would want summarised.
_PAGE_HINTS = ("about", "about-us", "services", "service", "products", "product",
               "solutions", "pricing", "plans", "contact", "team", "our-work",
               "what-we-do", "capabilities")

MAX_PAGES = int(os.environ.get("ENRICH_MAX_PAGES", "5") or "5")
MAX_CHARS = int(os.environ.get("ENRICH_MAX_CHARS", "12000") or "12000")
POLL_SECONDS = float(os.environ.get("ENRICH_POLL_SECONDS", "20") or "20")
PER_LEAD_DELAY = float(os.environ.get("ENRICH_PER_LEAD_DELAY", "2") or "2")
OPENAI_MODEL = os.environ.get("ENRICH_OPENAI_MODEL", "gpt-4o-mini")

_started = False
_stats = {"done": 0, "errors": 0, "openai_calls": 0, "apollo_calls": 0, "last": ""}


# ── website crawl (requests + BeautifulSoup) ─────────────────────────────────

def _clean_text(html: str) -> Dict[str, str]:
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()
    title = (soup.title.string if soup.title and soup.title.string else "").strip()
    desc = ""
    md = soup.find("meta", attrs={"name": "description"}) or soup.find(
        "meta", attrs={"property": "og:description"})
    if md and md.get("content"):
        desc = md["content"].strip()
    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)
    return {"title": title, "description": desc, "text": text, "_soup": soup}


def _candidate_links(soup, base_url: str) -> List[str]:
    out, seen = [], set()
    host = urlparse(base_url).netloc.lower()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("mailto:", "tel:", "#", "javascript:")):
            continue
        full = urljoin(base_url, href)
        p = urlparse(full)
        if p.netloc.lower() != host:
            continue
        path = (p.path or "/").lower().rstrip("/")
        slug = path.rsplit("/", 1)[-1]
        if any(h in path for h in _PAGE_HINTS):
            if full not in seen:
                seen.add(full)
                out.append((0 if slug in _PAGE_HINTS else 1, full))
    out.sort(key=lambda x: x[0])
    return [u for _, u in out]


def crawl_site(domain: str) -> Dict[str, Any]:
    """Fetch homepage + a few high-signal internal pages. Returns
    {homepage_url, pages:[{url,title,description,text}], combined_text}."""
    sess = requests.Session()
    sess.headers.update({"User-Agent": _UA, "Accept-Language": "en-AU,en;q=0.9"})
    result: Dict[str, Any] = {"homepage_url": "", "pages": [], "combined_text": "",
                              "fetched": 0, "errors": []}
    home_html, home_url = None, ""
    for scheme in ("https://", "http://"):
        try:
            r = sess.get(scheme + domain, timeout=12, allow_redirects=True)
            if r.status_code < 400 and r.text:
                home_html, home_url = r.text, str(r.url)
                break
        except Exception as e:
            result["errors"].append(f"{scheme}: {e}")
    if not home_html:
        return result
    result["homepage_url"] = home_url
    home = _clean_text(home_html)
    soup = home.pop("_soup")
    home["url"] = home_url
    result["pages"].append({k: home[k] for k in ("url", "title", "description", "text")})
    result["fetched"] = 1
    for link in _candidate_links(soup, home_url)[: MAX_PAGES - 1]:
        try:
            r = sess.get(link, timeout=10, allow_redirects=True)
            if r.status_code < 400 and r.text:
                pg = _clean_text(r.text)
                pg.pop("_soup", None)
                pg["url"] = str(r.url)
                result["pages"].append({k: pg[k] for k in ("url", "title", "description", "text")})
                result["fetched"] += 1
            time.sleep(0.6)  # polite
        except Exception as e:
            result["errors"].append(f"{link}: {e}")
    combined = []
    for pg in result["pages"]:
        combined.append(f"[{pg['url']}] {pg['title']} — {pg['description']} {pg['text']}")
    result["combined_text"] = (" \n".join(combined))[:MAX_CHARS]
    return result


# ── Apollo free org data (full object) ───────────────────────────────────────

def apollo_org_full(domain: str, api_key: str) -> Dict[str, Any]:
    """The complete free Apollo organization object for a domain. organizations/
    enrich is the free company-data endpoint (no export credits). Returns {} on
    any failure or when no key is set."""
    if not api_key or not domain:
        return {}
    try:
        r = requests.get(
            "https://api.apollo.io/api/v1/organizations/enrich",
            params={"domain": domain},
            headers={"Cache-Control": "no-cache", "Content-Type": "application/json",
                     "X-Api-Key": api_key},
            timeout=25,
        )
        if r.status_code == 200:
            _stats["apollo_calls"] += 1
            return r.json().get("organization", {}) or {}
    except Exception as e:
        log.debug("apollo_org_full(%s) failed: %s", domain, e)
    return {}


# ── OpenAI analysis of the fetched text ──────────────────────────────────────

_CHEATSHEET_KEYS = ("summary", "business_model", "products_services", "target_market",
                    "size_signals", "likely_pain_points", "talking_points",
                    "decision_maker_titles", "icebreakers")


def analyze_with_openai(company: str, domain: str, crawl_text: str,
                        apollo: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Turn the crawled text (+ a little Apollo context) into a compact caller
    cheat-sheet. Analysis only — the text was already fetched by the crawler."""
    if not api_key:
        return {"_note": "OPENAI_API_KEY not set — analysis skipped"}
    if not crawl_text and not apollo:
        return {"_note": "no website text or Apollo data to analyse"}
    ctx = {
        "company": company or domain,
        "domain": domain,
        "apollo_industry": apollo.get("industry", ""),
        "apollo_employees": apollo.get("estimated_num_employees", ""),
        "apollo_keywords": (apollo.get("keywords") or [])[:25],
        "website_text": (crawl_text or "")[:9000],
    }
    prompt = (
        "You are a sales-call prep assistant. Using ONLY the data provided about a "
        "company, produce a COMPACT cheat-sheet a cold-caller needs before phoning "
        "them. Be concrete and specific to THIS business; do not invent facts. "
        "Return a strict JSON object with these keys: "
        "summary (1-2 sentences), business_model (string), products_services "
        "(array of short strings), target_market (string), size_signals (string), "
        "likely_pain_points (array), talking_points (array of 3-5), "
        "decision_maker_titles (array of likely buyer titles to ask for), "
        "icebreakers (array of 2-3 short openers). "
        "If something is unknown, use an empty string/array — never guess.\n\n"
        f"DATA:\n{json.dumps(ctx, default=str)[:11000]}"
    )
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 900,
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
            },
            timeout=45,
        )
        if resp.status_code == 200:
            _stats["openai_calls"] += 1
            content = resp.json()["choices"][0]["message"]["content"]
            try:
                data = json.loads(content)
            except Exception:
                return {"summary": content[:1500]}
            # Keep only known keys (defensive) + ensure they exist.
            out = {k: data.get(k, "" if k in ("summary", "business_model",
                   "target_market", "size_signals") else []) for k in _CHEATSHEET_KEYS}
            return out
        return {"_note": f"OpenAI HTTP {resp.status_code}"}
    except Exception as e:
        return {"_note": f"OpenAI error: {e}"}


# ── worker loop ──────────────────────────────────────────────────────────────

def _enrich_one(lead: Dict[str, Any]) -> None:
    import db
    lead_id = int(lead["lead_id"])
    domain = (lead.get("root_domain") or "").strip().lower()
    company = lead.get("company_name") or lead.get("display_name") or domain
    try:
        if not domain:
            db.LeadEnrichmentRepo.save_error(lead_id, "no domain on lead")
            return
        crawl = crawl_site(domain)
        apollo_key = os.environ.get("APOLLO_API_KEY", "")
        openai_key = os.environ.get("OPENAI_API_KEY", "")
        apollo = apollo_org_full(domain, apollo_key)
        ai = analyze_with_openai(company, domain, crawl.get("combined_text", ""), apollo, openai_key)
        db.LeadEnrichmentRepo.save_result(lead_id, crawl, apollo, ai)
        _stats["done"] += 1
        _stats["last"] = f"{company} ({domain})"
        log.info("enriched lead %s (%s): %d page(s), apollo=%s, ai=%s",
                 lead_id, domain, crawl.get("fetched", 0), bool(apollo),
                 "ok" if ai and not ai.get("_note") else "skip")
    except Exception as e:
        _stats["errors"] += 1
        try:
            db.LeadEnrichmentRepo.save_error(lead_id, str(e))
        except Exception:
            pass
        log.warning("enrich lead %s failed: %s", lead_id, e)


def _loop(is_busy: Callable[[], bool]) -> None:
    import db
    log.info("enrichment worker started (poll=%ss, max_pages=%s)", POLL_SECONDS, MAX_PAGES)
    # Initial backfill so existing leads get queued.
    try:
        added = db.LeadEnrichmentRepo.backfill_pending()
        if added:
            log.info("enrichment backfill: queued %d existing lead(s)", added)
    except Exception as e:
        log.debug("initial backfill skipped: %s", e)
    idle_ticks = 0
    while True:
        try:
            if is_busy():
                time.sleep(POLL_SECONDS)
                continue
            lead = db.LeadEnrichmentRepo.claim_next()
            if not lead:
                idle_ticks += 1
                # Periodically pick up newly-generated leads, then idle.
                if idle_ticks % 3 == 0:
                    try:
                        db.LeadEnrichmentRepo.backfill_pending()
                    except Exception:
                        pass
                time.sleep(POLL_SECONDS)
                continue
            idle_ticks = 0
            _enrich_one(lead)
            time.sleep(PER_LEAD_DELAY)
        except Exception as e:
            log.warning("enrichment loop error: %s", e)
            time.sleep(POLL_SECONDS)


def get_stats() -> Dict[str, Any]:
    return dict(_stats)


def start_background_worker(is_busy: Optional[Callable[[], bool]] = None) -> bool:
    """Start the daemon worker once. `is_busy` should return True while a
    lead-generation run is active so enrichment pauses. Returns True if started."""
    global _started
    if _started:
        return False
    if str(os.environ.get("ENRICH_WORKER", "1")).strip() == "0":
        log.info("enrichment worker disabled (ENRICH_WORKER=0)")
        return False
    _started = True
    t = threading.Thread(target=_loop, args=(is_busy or (lambda: False),),
                         name="enrichment-worker", daemon=True)
    t.start()
    return True
