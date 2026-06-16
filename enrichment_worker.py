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
# Continuous crawling: when fully caught up, re-crawl the oldest lead whose data
# is older than this many days, so the crawler never sits fully idle and data
# stays fresh. 0 disables the refresh cycle.
REFRESH_DAYS = int(os.environ.get("ENRICH_REFRESH_DAYS", "21") or "21")

_started = False
_paused = False
_stats = {"done": 0, "errors": 0, "openai_calls": 0, "apollo_calls": 0, "last": "",
          "openai_status": "unknown"}
# Live "what is the worker doing right now", surfaced to the Enrichment panel as
# a progress bar. phase ∈ idle|crawling|apollo|analyzing.
_current: Dict[str, Any] = {"phase": "idle"}


def _set_current(**kw) -> None:
    _current.update(kw)


def _idle_current() -> None:
    _current.clear()
    _current["phase"] = "idle"


def pause() -> None:
    global _paused
    _paused = True


def resume() -> None:
    global _paused
    _paused = False


def is_paused() -> bool:
    return _paused


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


def crawl_site(domain: str, progress: Optional[Callable[[int, int, str], None]] = None) -> Dict[str, Any]:
    """Fetch homepage + a few high-signal internal pages. Returns
    {homepage_url, pages:[{url,title,description,text}], combined_text}. `progress`
    (if given) is called as progress(pages_fetched, pages_total, current_url) so
    the worker can surface a live progress bar."""
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
    links = _candidate_links(soup, home_url)[: MAX_PAGES - 1]
    total = 1 + len(links)
    if progress:
        try: progress(1, total, home_url)
        except Exception: pass
    for link in links:
        try:
            r = sess.get(link, timeout=10, allow_redirects=True)
            if r.status_code < 400 and r.text:
                pg = _clean_text(r.text)
                pg.pop("_soup", None)
                pg["url"] = str(r.url)
                result["pages"].append({k: pg[k] for k in ("url", "title", "description", "text")})
                result["fetched"] += 1
            if progress:
                try: progress(result["fetched"], total, link)
                except Exception: pass
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

# String fields vs array fields in the cheat-sheet (drives defensive defaults).
_CS_STR = ("summary", "business_model", "target_audience", "market_position",
           "advertising", "past_activity", "tendencies", "recent_initiatives",
           "call_strategy", "best_time_to_call")
_CS_ARR = ("products", "services", "pain_points", "talking_points", "objections",
           "decision_maker_titles", "icebreakers", "competitors", "buying_signals")
_CHEATSHEET_KEYS = _CS_STR + _CS_ARR


def _empty_sheet() -> Dict[str, Any]:
    out = {k: "" for k in _CS_STR}
    for k in _CS_ARR:
        out[k] = []
    return out


def _basic_summary(company: str, domain: str, apollo: Dict[str, Any],
                   crawl_text: str) -> Dict[str, Any]:
    """A useful cheat-sheet built from Apollo + website data WITHOUT OpenAI, so a
    lead is never blank even when the OpenAI key is missing/invalid. Upgraded to
    the full AI version once a valid key analyses it."""
    ap = apollo or {}
    out = _empty_sheet()
    nm = ap.get("name") or company or domain
    ind = ap.get("industry") or ""
    loc = ", ".join([str(ap.get(k, "")) for k in ("city", "state", "country") if ap.get(k)])
    emp = ap.get("estimated_num_employees")
    rev = ap.get("annual_revenue_printed") or ap.get("organization_revenue_printed") or ""
    desc = ap.get("short_description") or ap.get("seo_description") or ""
    s = nm
    if ind:
        s += f" is a {ind} business"
    if loc:
        s += f" based in {loc}"
    if emp:
        s += f" with about {emp} employees"
    s += "."
    if desc:
        s += " " + desc
    if not s.strip() and crawl_text:
        s = (crawl_text[:300])
    out["summary"] = s[:700]
    out["target_audience"] = ind
    mp = []
    if rev:
        mp.append("Revenue " + str(rev))
    if emp:
        mp.append("~" + str(emp) + " employees")
    if ap.get("founded_year"):
        mp.append("founded " + str(ap.get("founded_year")))
    out["market_position"] = " · ".join(mp)
    kw = [k for k in (ap.get("keywords") or []) if k][:12]
    out["products"] = kw
    out["_source"] = "apollo-fallback"
    return out


def analyze_with_openai(company: str, domain: str, crawl_text: str,
                        apollo: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Turn the crawled website text + Apollo data into a DETAILED but scannable
    sales cheat-sheet. On ANY OpenAI failure it falls back to an Apollo-derived
    summary (+ a _note) so the lead is never blank."""
    def _fallback(note):
        fb = _basic_summary(company, domain, apollo, crawl_text)
        fb["_note"] = note
        return fb
    if not api_key:
        return _fallback("OPENAI_API_KEY not set — showing Apollo data only")
    if not crawl_text and not apollo:
        return {**_empty_sheet(), "_note": "no website text or Apollo data to analyse"}
    ap = apollo or {}
    ctx = {
        "company": company or domain,
        "domain": domain,
        "apollo_industry": ap.get("industry", ""),
        "apollo_keywords": (ap.get("keywords") or [])[:40],
        "apollo_employees": ap.get("estimated_num_employees", ""),
        "apollo_revenue": ap.get("annual_revenue_printed") or ap.get("annual_revenue", ""),
        "apollo_founded": ap.get("founded_year", ""),
        "apollo_location": " ".join([str(ap.get(k, "")) for k in ("city", "state", "country") if ap.get(k)]),
        "apollo_technologies": (ap.get("technologies") or ap.get("current_technologies") or [])[:30],
        "apollo_description": ap.get("short_description") or ap.get("seo_description") or "",
        "website_text": (crawl_text or "")[:9500],
    }
    prompt = (
        "You are an expert B2B sales-call strategist. Using ONLY the data provided "
        "about a company (its website text + Apollo firmographics), produce a "
        "DETAILED but skimmable call-prep cheat-sheet for a salesperson about to "
        "phone them. Be concrete and specific to THIS business; never invent facts "
        "— if something isn't supported by the data, use an empty string/array.\n\n"
        "Return a STRICT JSON object with EXACTLY these keys:\n"
        "  summary            : 2-3 sentence plain-English overview of who they are\n"
        "  business_model     : how they make money\n"
        "  products           : array of concrete products they sell\n"
        "  services           : array of concrete services they offer\n"
        "  target_audience    : who their customers are (segments, geography)\n"
        "  market_position    : size / standing / differentiators vs competitors\n"
        "  advertising        : their marketing & advertising activity/channels you can infer\n"
        "  past_activity      : notable history, milestones, or past doings\n"
        "  recent_initiatives : recent launches/news/initiatives if any\n"
        "  tendencies         : how they operate + likely buying tendencies\n"
        "  buying_signals     : array of signals they may be in-market for a service\n"
        "  competitors        : array of likely competitors\n"
        "  pain_points        : array of likely business pain points\n"
        "  talking_points     : array of 4-6 specific things to mention on the call\n"
        "  objections         : array of likely objections + a short rebuttal each\n"
        "  decision_maker_titles : array of job titles to ask for\n"
        "  icebreakers        : array of 2-3 short, specific openers\n"
        "  call_strategy      : the single best angle/approach for this call\n"
        "  best_time_to_call  : a sensible suggestion if inferable, else empty\n\n"
        f"DATA:\n{json.dumps(ctx, default=str)[:12000]}"
    )
    try:
        resp = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 1700,
                "temperature": 0.2,
                "response_format": {"type": "json_object"},
            },
            timeout=60,
        )
        if resp.status_code == 200:
            _stats["openai_calls"] += 1
            _stats["openai_status"] = "ok"
            content = resp.json()["choices"][0]["message"]["content"]
            try:
                data = json.loads(content)
            except Exception:
                return {**_basic_summary(company, domain, apollo, crawl_text), "summary": content[:2000]}
            out = {k: data.get(k, "") for k in _CS_STR}
            for k in _CS_ARR:
                v = data.get(k, [])
                out[k] = v if isinstance(v, list) else ([v] if v else [])
            return out
        _stats["openai_status"] = ("401 — invalid OPENAI_API_KEY"
                                   if resp.status_code == 401 else f"HTTP {resp.status_code}")
        return _fallback(f"OpenAI HTTP {resp.status_code} — showing Apollo data only")
    except Exception as e:
        _stats["openai_status"] = f"error: {e}"
        return _fallback(f"OpenAI error: {e}")


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
        _set_current(phase="crawling", lead_id=lead_id, domain=domain, company=company,
                     pages=0, pages_total=MAX_PAGES, page_url="", started=time.time())
        def _prog(n, total, url):
            _set_current(pages=n, pages_total=total, page_url=url)
        crawl = crawl_site(domain, progress=_prog)
        apollo_key = os.environ.get("APOLLO_API_KEY", "")
        openai_key = os.environ.get("OPENAI_API_KEY", "")
        _set_current(phase="apollo")
        apollo = apollo_org_full(domain, apollo_key)
        _set_current(phase="analyzing")
        ai = analyze_with_openai(company, domain, crawl.get("combined_text", ""), apollo, openai_key)
        db.LeadEnrichmentRepo.save_result(lead_id, crawl, apollo, ai)
        # Fill blank core columns (company/industry/revenue) so the Database
        # view fills up automatically as enrichment runs.
        try:
            db.MasterLeadRepo.enrich_core_from_apollo(lead_id, apollo)
        except Exception:
            pass
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


def _backfill_failed_ai(db) -> bool:
    """Process ONE already-crawled lead whose AI cheat-sheet is missing/failed,
    reusing stored crawl+Apollo data (no re-crawl). When OpenAI is healthy we
    include leads that currently only have an Apollo fallback (so they get
    upgraded to the full AI cheat-sheet); when the key is bad we touch only leads
    with NO summary yet — so a bad key never re-loops over fallbacks. Returns True
    if a lead was processed."""
    try:
        healthy = _stats.get("openai_status") == "ok"
        ids = db.LeadEnrichmentRepo.failed_ai_ids(limit=1, include_fallback=healthy)
        if not ids:
            return False
        _set_current(phase="analyzing", lead_id=int(ids[0]), domain="", company="",
                     pages=0, pages_total=0, page_url="(re-analysing stored data)",
                     started=time.time())
        _reanalyze_one(int(ids[0]), os.environ.get("OPENAI_API_KEY", ""))
        _stats["last"] = f"AI backfill: lead {ids[0]}"
        return True
    except Exception as e:
        log.debug("AI backfill skipped: %s", e)
        return False


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
            if _paused or is_busy():
                time.sleep(POLL_SECONDS)
                continue
            lead = db.LeadEnrichmentRepo.claim_next()
            if not lead:
                idle_ticks += 1
                # Periodically pick up newly-generated leads.
                if idle_ticks % 3 == 0:
                    try:
                        db.LeadEnrichmentRepo.backfill_pending()
                    except Exception:
                        pass
                # No fresh crawl work → adaptively backfill AI cheat-sheets for
                # already-crawled leads whose AI failed. When OpenAI is healthy we
                # also UPGRADE Apollo-fallback summaries to the full AI version;
                # when the key is bad we only fill leads that have NO summary yet
                # (so a bad key never makes us reprocess fallbacks forever).
                if _backfill_failed_ai(db):
                    idle_ticks = 0
                    time.sleep(PER_LEAD_DELAY)
                    continue
                # Fully caught up → keep crawling CONTINUOUSLY by re-crawling the
                # oldest stale lead (data older than REFRESH_DAYS). Next loop claims
                # and re-crawls it, so the worker never sits idle for long.
                try:
                    if db.LeadEnrichmentRepo.requeue_oldest_stale(REFRESH_DAYS):
                        idle_ticks = 0
                        _idle_current()
                        time.sleep(1)
                        continue
                except Exception:
                    pass
                _idle_current()
                time.sleep(POLL_SECONDS)
                continue
            idle_ticks = 0
            _enrich_one(lead)
            time.sleep(PER_LEAD_DELAY)
        except Exception as e:
            log.warning("enrichment loop error: %s", e)
            time.sleep(POLL_SECONDS)


def get_stats() -> Dict[str, Any]:
    s = dict(_stats); s["paused"] = _paused; s["started"] = _started
    cur = dict(_current)
    if cur.get("started"):
        cur["elapsed"] = round(time.time() - cur["started"], 1)
    s["current"] = cur
    return s


def _reanalyze_one(lead_id: int, openai_key: str) -> bool:
    """Recompute the AI cheat-sheet for ONE already-crawled lead, reusing the
    stored website text + Apollo data (NO re-crawl, NO extra Apollo call).
    Returns True on success."""
    import db
    row = db.LeadEnrichmentRepo.get_crawl_apollo(int(lead_id))
    if not row:
        return False
    crawl = row.get("crawl_json") or {}
    apollo = row.get("apollo_json") or {}
    company = row.get("company_name") or row.get("display_name") or row.get("root_domain") or ""
    ai = analyze_with_openai(company, row.get("root_domain") or "",
                             (crawl or {}).get("combined_text", ""), apollo, openai_key)
    db.LeadEnrichmentRepo.save_ai(int(lead_id), ai)
    try:
        db.MasterLeadRepo.enrich_core_from_apollo(int(lead_id), apollo)
    except Exception:
        pass
    return True


def reanalyze(lead_ids) -> None:
    """Recompute the AI cheat-sheet for already-crawled leads, reusing the stored
    website text + Apollo data (NO re-crawl, NO extra Apollo call). Used to
    backfill leads whose AI failed earlier (e.g. a bad OpenAI key) once a valid
    key is set. Runs detached so the HTTP request returns instantly."""
    def _run():
        openai_key = os.environ.get("OPENAI_API_KEY", "")
        n = 0
        for lid in (lead_ids or []):
            try:
                if _reanalyze_one(int(lid), openai_key):
                    n += 1
            except Exception as e:
                log.warning("reanalyze %s failed: %s", lid, e)
        log.info("reanalyze complete: %d lead(s) refreshed", n)
    threading.Thread(target=_run, name="enrich-reanalyze", daemon=True).start()


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
