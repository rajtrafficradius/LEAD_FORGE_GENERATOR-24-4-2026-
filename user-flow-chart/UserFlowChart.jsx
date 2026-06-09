import React, {
  useState,
  useMemo,
  useRef,
  useEffect,
  useLayoutEffect,
} from "react";

/* ============================================================================
   UserFlowChart — Neo-Brutalist interactive pipeline poster (LeadForge)
   ----------------------------------------------------------------------------
   3 zones: LEFT marketing + file→command mini-flow · CENTER main vertical
   pipeline (hover a node to EXPAND full detail) · RIGHT keyword-priority +
   API-failover mini-flows + manager's checklist · detail info blocks below.
   Zero runtime dependencies. Self-contained styles + Google Fonts.
   ========================================================================== */

const DESIGN_W = 1200;
const DESIGN_H = 1960;
const GRAND_W = 1680;
const GRAND_H = 2440;

// ---- Center pipeline: the LeadForge city lead-discovery flow ----------------
const DEFAULT_NODES = [
  { id: "input",      i: "01", title: "User Input",                    sub: "index.html → /generate-city",   tag: "Input",    color: "#1F4FE0", ink: "light", x: 410, y: 80,   w: 380, h: 108 },
  { id: "keywords",   i: "02", title: "Keyword Bank",                  sub: "~54,000 keywords · cascade",    tag: "Core",     color: "#FF2E7E", ink: "dark",  x: 400, y: 260,  w: 400, h: 120, hero: true },
  { id: "router",     i: "03", title: "Discovery Mode Router",         sub: "discovery_mode.py · can pivot", tag: "Router",   color: "#7C3AED", ink: "light", x: 410, y: 444,  w: 380, h: 108 },
  { id: "discovery",  i: "04", title: "Domain Discovery",              sub: "PASS 1–5 + Paid Verifier",      tag: "Engine",   color: "#FF7A1A", ink: "dark",  x: 370, y: 624,  w: 460, h: 150, engine: true },
  { id: "enrichment", i: "05", title: "Enrichment",                    sub: "names · phones · emails",       tag: "Phase 4",  color: "#14D6C4", ink: "dark",  x: 400, y: 846,  w: 400, h: 120, hero: true },
  { id: "cleanup",    i: "06", title: "Cleanup + Gates",               sub: "dedup · gates · sector filter", tag: "Phase 5",  color: "#FFD23F", ink: "dark",  x: 410, y: 1030, w: 380, h: 108 },
  { id: "decision",   i: "07", title: "Decision-Maker Filter + Rank",  sub: "role hierarchy scoring",        tag: "Phase 5f", color: "#A8E80B", ink: "dark",  x: 400, y: 1210, w: 400, h: 120, hero: true },
  { id: "assembly",   i: "08", title: "Final Assembly",                sub: "tiered · paid-first · cap 1",   tag: "Phase 6",  color: "#19C37D", ink: "dark",  x: 410, y: 1394, w: 380, h: 108 },
  { id: "masterdb",   i: "09", title: "Master DB",                     sub: "Railway MySQL · master_leads",  tag: "Store",    color: "#1F4FE0", ink: "light", x: 250, y: 1576, w: 300, h: 108 },
  { id: "csv",        i: "10", title: "CSV Export",                    sub: "leads_ALL · leads_TOP",         tag: "Export",   color: "#FFD23F", ink: "dark",  x: 650, y: 1576, w: 300, h: 108 },
  { id: "frontend",   i: "11", title: "Frontend Table + Download",     sub: "index.html /status",            tag: "Output",   color: "#FF5252", ink: "dark",  x: 380, y: 1758, w: 440, h: 112 },
];

const DEFAULT_EDGES = [
  { from: "input",      to: "keywords",   pts: [[600, 188], [600, 260]] },
  { from: "keywords",   to: "router",     pts: [[600, 380], [600, 444]] },
  { from: "router",     to: "discovery",  pts: [[600, 552], [600, 624]] },
  { from: "discovery",  to: "enrichment", pts: [[600, 774], [600, 846]] },
  { from: "enrichment", to: "cleanup",    pts: [[600, 966], [600, 1030]] },
  { from: "cleanup",    to: "decision",   pts: [[600, 1138], [600, 1210]] },
  { from: "decision",   to: "assembly",   pts: [[600, 1330], [600, 1394]] },
  { from: "assembly",   to: "masterdb",   pts: [[600, 1502], [600, 1539], [400, 1539], [400, 1576]] },
  { from: "assembly",   to: "csv",        pts: [[600, 1502], [600, 1539], [800, 1539], [800, 1576]] },
  { from: "csv",        to: "frontend",   pts: [[800, 1684], [800, 1721], [600, 1721], [600, 1758]] },
  { from: "masterdb",   to: "csv",        pts: [[550, 1630], [650, 1630]], biDir: true, label: { t: "Sync", x: 600, y: 1630 } },
  { from: "decision",   to: "keywords",   pts: [[800, 1270], [1080, 1270], [1080, 320], [800, 320]], loop: true, label: { t: "↩ Backtrack", x: 1080, y: 795 } },
  { from: "masterdb",   to: "cleanup",    pts: [[250, 1630], [130, 1630], [130, 1084], [410, 1084]], loop: true, label: { t: "↩ Dedup", x: 130, y: 1357 } },
];

// ---- Per-stage detail (info blocks + hover pop-overs) -----------------------
const DEFAULT_INFO = [
  { id: "input", i: "01", title: "User Input", lines: [
    "Two entry modes: BY CITY (city_pipeline.py) or BY INDUSTRY (V5 Phases 1–3)",
    "By City → keywords from the 54k bank, multi-round; By Industry → expands the industry's own keywords + uses a major-cities list for the ads location",
    "Both modes then feed the SAME discovery → enrichment → CSV engine",
    "Scope: city / state / “Australia (whole country)”",
    "Max leads (e.g. 1, 5, 20) · Min search volume",
    "Contact Enrichment toggle — OFF = free names only · ON = paid reveal",
  ]},
  { id: "keywords", i: "02", title: "Keyword Bank", lines: [
    "Sources: keyword_bank.py → INDUSTRY_TIERED (Tier 1 ▸ 2 ▸ 3) + GLOBAL_FALLBACK",
    "KEYWORDS folder: leadforge_10000_semrush (~7,800)",
    "Bunnings / Kogan / BIGW ad-keyword trees (~45,000 e-com)",
    "Extra niches + semrush_top_1000 (~1,500)",
    "TOTAL ≈ 54,000 unique keywords",
    "Cascade: Tier 1 → Tier 2 → Tier 3 → E-com → Global",
    "All chosen industries INTERLEAVED (fair share)",
    "Picks industries by % cap (20% scope → top 9 industries)",
  ]},
  { id: "router", i: "03", title: "Discovery Mode Router", lines: [
    "discovery_mode.py decides up-front from which API keys are alive",
    "Modes: BOTH · SEMRUSH_ONLY · GOOGLE_ONLY · APOLLO_ONLY",
    "⚡ Can PIVOT mid-run (SEMrush dies → switch to GOOGLE_ONLY)",
  ]},
  { id: "discovery", i: "04", title: "Domain Discovery — the engine", lines: [
    "PASS 1 — SEMrush phrase_adwords → domains that BUY Google Ads. ⚡ Early-bail: 25 probes, ≥90% empty → stop (often 0 in AU)",
    "PASS 2 — SerpAPI google → ORGANIC results (“keyword + city”) · redundancy fallback",
    "PASS 2a — SerpAPI google_ads ★ PRIMARY PAID. location = CITY. LIVE Search Ads = real advertisers; freq ≥ 2 = HEAVY advertiser",
    "PASS 2.5 — Google Custom Search (free, optional)",
    "PASS 3 — SEMrush competitor expansion (domain_adwords_adwords)",
    "PASS 3.5 — Google Places searchText → AU businesses + website (AU only)",
    "Paid Verifier (3 sources): Ads Transparency Center (free) · SEMrush domain_ranks (paid_traffic ≥ 5) · SerpAPI ads[] branded query",
    "PASS 4 — Apollo mixed_companies/search → last-resort company fallback (city-filtered)",
    "PASS 5 — Vertex AI / Gemini → ranks domains by buying-intent (free; skips if quota)",
    "Budgets: SEMrush ≈ max_leads×100 (300–2,000) · SerpAPI ~30 runs + 3-key rotation · Places ≤ 25 calls / 100 domains",
  ]},
  { id: "enrichment", i: "05", title: "Enrichment — Phase 4", lines: [
    "NAME + ROLE → Apollo people-search (free; runs even if enrichment OFF)",
    "NAME (full) → SerpAPI LinkedIn lookup (“David” → “David Cusack”)",
    "EMAIL + PHONE → Apollo enrich / Lusha (PAID — only if toggle ON)",
    "EMAIL → Hunter.io (optional) · verify via OpenAI (optional)",
    "PHONE / EMAIL → website contact-page scrape (free)",
    "FOUNDER → WHOIS lookup",
    "⚡ Apollo finds 0 people → STUB lead (name = business / domain) so the domain isn’t lost",
    "Stops early once enough leads found (saves credits)",
  ]},
  { id: "cleanup", i: "06", title: "Cleanup + Gates — Phase 5", lines: [
    "De-duplicate · full-name policy (only when enrichment ON)",
    "PAID-TRAFFIC GATE: keep paid_traffic ≥ 1",
    "⚡ Bypass gate for: Google-Ads / ATC confirmed · enrichment-OFF · silent-scope · stub leads (so silent AU scopes never return 0)",
    "SECTOR FILTER: 0 digital-marketing agencies; trades / pro-services first; ≤ 10% removed",
  ]},
  { id: "decision", i: "07", title: "Decision-Maker Filter + Rank — Phase 5f", lines: [
    "Role hierarchy score:",
    "Owner 100 · Founder 95 · CEO/MD/C-suite 90 · Partner 85",
    "VP 80 · Head 75 · Director 70 · Manager/GM 60 · Senior 50 · Intern 10",
    "Drops non-decision-makers",
    "Advertiser tier (paid-first): heavy-Search ▸ Search ▸ ATC ▸ none",
    "↩ Quota top-up loop if short — new keyword slices (offset 200 ▸ 500 ▸ 900) = backtracking",
  ]},
  { id: "assembly", i: "08", title: "Final Assembly — Phase 6", lines: [
    "Score into tiers (Name+Email+Phone ▸ Name+Phone ▸ …)",
    "Per-domain cap = 1 (rare 2nd only if can’t fill, ≤ 1 per 20)",
    "Paid-first order: heaviest Google-Ads advertisers to the top",
    "Adds Keyword + ATC-proof columns",
  ]},
  { id: "masterdb", i: "09", title: "Master DB", lines: [
    "Railway MySQL · master_leads table",
    "UNIQUE(name, domain)",
    "↩ HYBRID cross-run dedup: lifetime 2 contacts / company across ALL runs",
  ]},
  { id: "csv", i: "10", title: "CSV Export", lines: [
    "leads_ALL_*.csv · leads_TOP_*.csv",
    "Columns: Name · Company · Domain · Role · Phone · Email · Traffic Source · Keyword · ATC Verified · Ad URL",
  ]},
  { id: "frontend", i: "11", title: "Frontend Table + Download", lines: [
    "index.html /status",
    "Columns: Business · Domain · Email · Phone · Source · Keyword · Score",
    "Traffic Source = “Google Ads (Search)” = confirmed paid advertiser",
  ]},
];

const DEFAULT_XCONN = [
  "② Keyword Bank feeds → PASS 1, 2, 2a, 3.5 (all use keywords)",
  "③ Mode Router controls → every PASS (turns each on / off)",
  "Paid Verifier (3.5) connects → ATC + SEMrush + SerpAPI",
  "⑦ Quota top-up loops BACK → ② Keyword Bank (new slices) = backtracking",
  "⑨ Master DB connects → ⑥ (dedup check) AND ⑩ (writes new leads)",
];

// ---- Side content -----------------------------------------------------------
const WHY_WINS = [
  "Paid-intent FIRST — only businesses already buying Google Ads",
  "Triangulated proof — Ads Transparency + SEMrush + SerpAPI",
  "Cost-guarded — early-bail, unit budgets, free fallback layers",
  "AU-tuned — city-level live ads + Google Places",
  "Never returns 0 — silent-scope & stub-lead bypasses",
  "Decision-makers only — role-ranked, deduped across every run",
];

const STATS = [
  { num: "54k", label: "keywords", color: "#FF2E7E" },
  { num: "11+", label: "sources / passes", color: "#1F4FE0" },
  { num: "3×",  label: "paid-proof sources", color: "#19C37D" },
  { num: "1",   label: "lead / domain cap", color: "#FF7A1A" },
];

const FILE_FLOW = [
  { label: "index.html",        note: "UI · /generate-city · /status" },
  { label: "city_pipeline.py",  note: "orchestrator · phases 1–6" },
  { label: "keyword_bank.py",   note: "tiers + GLOBAL_FALLBACK" },
  { label: "KEYWORDS/*.txt",    note: "the 54k keyword files" },
  { label: "discovery_mode.py", note: "selects the live APIs" },
  { label: "api clients",       note: "SEMrush·SerpAPI·Places·Apollo·Gemini" },
  { label: "Railway MySQL",     note: "master_leads" },
  { label: "CSV + /status",     note: "export + live table" },
];

const KEYWORD_FLOW = [
  { rank: "1", label: "V5 in-code keywords",      note: "INDUSTRY_TIERED · highest intent",      color: "#FF2E7E" },
  { rank: "2", label: "semrush_top_1000",         note: "~1,500 · proven terms", via: "exhaust →", color: "#FF7A1A" },
  { rank: "3", label: "leadforge_10000_semrush",  note: "~7,800 · broader",      via: "need more →", color: "#FFD23F" },
  { rank: "4", label: "E-com ad trees",           note: "~45,000 · Bunnings/Kogan/BIGW", via: "broaden →", color: "#A8E80B" },
  { rank: "5", label: "GLOBAL_FALLBACK",          note: "catch-all",             via: "last resort →", color: "#14D6C4" },
];
const KEYWORD_FOOT = "↩ Quota short → re-enter with the next slice (offset 200 ▸ 500 ▸ 900) = backtracking";

const API_FLOW = [
  { rank: "1", label: "SEMrush phrase_adwords", note: "domains buying Google Ads",   color: "#14D6C4" },
  { rank: "2", label: "SerpAPI google_ads",     note: "LIVE ads · 3-key rotate",     via: "⚡ credits out / empty (AU thin)", warn: true, star: true, color: "#FF2E7E" },
  { rank: "3", label: "Google Places",          note: "AU business + website",       via: "⚡ key exhausted → rotate", warn: true, color: "#FF7A1A" },
  { rank: "4", label: "Apollo",                 note: "company fallback",            via: "⚡ still short →", warn: true, color: "#A8E80B" },
  { rank: "5", label: "Gemini rank",            note: "buying-intent (free)",        via: "rank →", color: "#FFD23F" },
];
const API_FOOT = "⚡ Mode PIVOT mid-run — if SEMrush dies the router switches to GOOGLE_ONLY automatically";

// Two entry pipelines — they differ only at the TOP, then merge into the
// same discovery → enrichment → CSV engine (so this stays compact).
const MODE_FLOW = [
  { rank: "A", label: "By City",     note: "city_pipeline.py · builds keywords from the 54k bank · multi-round + offset cycling", color: "#1F4FE0" },
  { rank: "B", label: "By Industry", note: "V5 Phases 1–3 (full run) · expands INDUSTRY_KEYWORDS · ads-sweep loops MAJOR CITIES · single pass", via: "or →", color: "#7C3AED" },
];
const MODE_FOOT = "Both merge at PASS 1–5 discovery → enrichment → CSV (one shared engine)";

const DATA_SOURCES = [
  { n: "SEMrush", c: "#1F4FE0" }, { n: "SerpAPI", c: "#FF2E7E" }, { n: "Google Places", c: "#FF7A1A" },
  { n: "Apollo", c: "#A8E80B" }, { n: "Gemini", c: "#14D6C4" }, { n: "Ads Transparency", c: "#FFD23F" },
  { n: "Hunter.io", c: "#19C37D" }, { n: "Lusha", c: "#7C3AED" }, { n: "WHOIS", c: "#FF5252" }, { n: "OpenAI", c: "#1F4FE0" },
];

const SAMPLE_ROW = [
  ["Business", "Acme Plumbing Co"],
  ["Domain", "acmeplumbing.com.au"],
  ["Role", "Owner"],
  ["Phone", "+61 4xx xxx xxx"],
  ["Email", "sam@acmeplumbing.com.au"],
  ["Source", "Google Ads (Search)"],
  ["Score", "92"],
];

// ---- GRAND combined breakdown (toggled by the button) ----------------------
const GRAND_LEGEND = [
  { n: "Command / Files", c: "#23211E" }, { n: "Keywords", c: "#FF2E7E" }, { n: "Router", c: "#7C3AED" },
  { n: "Discovery API", c: "#1F4FE0" }, { n: "Primary paid", c: "#FF7A1A" }, { n: "Enrichment", c: "#14D6C4" },
  { n: "Fallback", c: "#FF5252" }, { n: "Gate", c: "#FFD23F" }, { n: "Rank", c: "#A8E80B" }, { n: "Output", c: "#19C37D" },
];

const GRAND_BANDS = [
  { t: "A · Command", x: 56, y: 44 },
  { t: "B · Keywords (priority)", x: 56, y: 232 },
  { t: "C · Mode Router", x: 56, y: 600 },
  { t: "D · Discovery & API failover", x: 56, y: 752 },
  { t: "E · Enrichment sources", x: 56, y: 1126 },
  { t: "F · Gates → Rank → Assembly", x: 56, y: 1574 },
  { t: "G · Output", x: 56, y: 2032 },
];

const GRAND_NODES = [
  { id: "gUi", title: "index.html", sub: "UI · /generate-city", color: "#23211E", ink: "light", x: 110, y: 96, w: 212, h: 80, small: true },
  { id: "gPipe", title: "LEADS THROUGH CITIES", sub: "orchestrator · phases 1–6", color: "#23211E", ink: "light", x: 690, y: 80, w: 300, h: 116, engine: true },
  { id: "gKwbank", title: "Keyword Bank", sub: "interleave · % cap", color: "#FF2E7E", ink: "dark", x: 715, y: 300, w: 250, h: 96, hero: true },
  { id: "gKw1", title: "1 · V5 in-code", sub: "highest intent", color: "#FF2E7E", ink: "dark", x: 90, y: 270, w: 200, h: 62, small: true },
  { id: "gKw2", title: "2 · semrush_top_1000", sub: "~1,500", color: "#FF2E7E", ink: "dark", x: 90, y: 348, w: 200, h: 62, small: true },
  { id: "gKw3", title: "3 · leadforge_10000", sub: "~7,800", color: "#FF2E7E", ink: "dark", x: 90, y: 426, w: 200, h: 62, small: true },
  { id: "gKw4", title: "4 · e-com trees", sub: "~45,000", color: "#FF2E7E", ink: "dark", x: 90, y: 504, w: 200, h: 62, small: true },
  { id: "gKw5", title: "5 · GLOBAL_FALLBACK", sub: "catch-all", color: "#FF2E7E", ink: "dark", x: 90, y: 582, w: 200, h: 62, small: true },
  { id: "gMode", title: "Discovery Mode Router", sub: "picks live APIs · pivots", color: "#7C3AED", ink: "light", x: 715, y: 640, w: 250, h: 96 },
  { id: "gEngine", title: "Domain Discovery", sub: "PASS 1–5 engine", color: "#1F4FE0", ink: "light", x: 690, y: 800, w: 300, h: 116, engine: true },
  { id: "gApi1", title: "SEMrush phrase_adwords", sub: "paid ads · PASS 1", color: "#1F4FE0", ink: "light", x: 1380, y: 770, w: 200, h: 64, small: true },
  { id: "gApi2", title: "SerpAPI google_ads", sub: "LIVE ads · 3-key · PASS 2a", color: "#FF7A1A", ink: "dark", x: 1330, y: 854, w: 250, h: 88, hero: true },
  { id: "gApi3", title: "Google Places", sub: "AU biz + site · PASS 3.5", color: "#1F4FE0", ink: "light", x: 1380, y: 972, w: 200, h: 64, small: true },
  { id: "gApi4", title: "Apollo", sub: "company fallback · PASS 4", color: "#FF5252", ink: "dark", x: 1380, y: 1052, w: 200, h: 64, small: true },
  { id: "gApi5", title: "Gemini rank", sub: "intent rank · PASS 5", color: "#14D6C4", ink: "dark", x: 1380, y: 1132, w: 200, h: 64, small: true },
  { id: "gVerify", title: "Paid Verifier", sub: "ATC + SEMrush + SerpAPI", color: "#FF7A1A", ink: "dark", x: 734, y: 1010, w: 212, h: 80 },
  { id: "gEnrich", title: "Enrichment", sub: "names · phones · emails", color: "#14D6C4", ink: "dark", x: 715, y: 1180, w: 250, h: 96, hero: true },
  { id: "gNm1", title: "Apollo people-search", sub: "name + role (free)", color: "#14D6C4", ink: "dark", x: 120, y: 1340, w: 212, h: 72, small: true },
  { id: "gNm3", title: "STUB lead", sub: "⚡ 0 people → domain", color: "#FF5252", ink: "dark", x: 120, y: 1448, w: 212, h: 72, small: true },
  { id: "gEm1", title: "Apollo / Hunter.io", sub: "email", color: "#14D6C4", ink: "dark", x: 530, y: 1340, w: 212, h: 72, small: true },
  { id: "gEm2", title: "OpenAI verify", sub: "validate email", color: "#14D6C4", ink: "dark", x: 530, y: 1448, w: 212, h: 72, small: true },
  { id: "gPh1", title: "Apollo / Lusha", sub: "phone (paid)", color: "#14D6C4", ink: "dark", x: 940, y: 1340, w: 212, h: 72, small: true },
  { id: "gPh2", title: "Website scrape", sub: "⚡ free fallback", color: "#FF5252", ink: "dark", x: 940, y: 1448, w: 212, h: 72, small: true },
  { id: "gFo1", title: "WHOIS", sub: "founder", color: "#14D6C4", ink: "dark", x: 1350, y: 1340, w: 200, h: 72, small: true },
  { id: "gGates", title: "Cleanup + Gates", sub: "dedup · paid gate · sector", color: "#FFD23F", ink: "dark", x: 715, y: 1620, w: 250, h: 96, hero: true },
  { id: "gRank", title: "Decision-Maker Rank", sub: "role hierarchy · paid-first", color: "#A8E80B", ink: "dark", x: 715, y: 1776, w: 250, h: 96, hero: true },
  { id: "gAssembly", title: "Final Assembly", sub: "tiered · cap 1 · paid-first", color: "#19C37D", ink: "dark", x: 734, y: 1932, w: 212, h: 80 },
  { id: "gDB", title: "Master DB", sub: "Railway MySQL", color: "#1F4FE0", ink: "light", x: 520, y: 2090, w: 212, h: 80 },
  { id: "gCsv", title: "CSV Export", sub: "ALL / TOP", color: "#FFD23F", ink: "dark", x: 948, y: 2090, w: 212, h: 80 },
  { id: "gFront", title: "Frontend Table", sub: "/status · paid proof", color: "#FF5252", ink: "dark", x: 715, y: 2250, w: 250, h: 96, hero: true },
];

const GMAP = Object.fromEntries(GRAND_NODES.map((n) => [n.id, n]));
function gAnchor(n, s) {
  const cx = n.x + n.w / 2, cy = n.y + n.h / 2;
  if (s === "t") return [cx, n.y];
  if (s === "b") return [cx, n.y + n.h];
  if (s === "l") return [n.x, cy];
  if (s === "r") return [n.x + n.w, cy];
  return [cx, cy];
}
function gV(a, b) {
  const p = gAnchor(a, "b"), q = gAnchor(b, "t"), my = (p[1] + q[1]) / 2;
  return p[0] === q[0] ? [p, q] : [p, [p[0], my], [q[0], my], q];
}
function gH(a, b) {
  const right = a.x + a.w <= b.x;
  const p = gAnchor(a, right ? "r" : "l"), q = gAnchor(b, right ? "l" : "r"), mx = (p[0] + q[0]) / 2;
  return p[1] === q[1] ? [p, q] : [p, [mx, p[1]], [mx, q[1]], q];
}
function gMid(pts) { const a = pts[0], b = pts[pts.length - 1]; return [(a[0] + b[0]) / 2, (a[1] + b[1]) / 2]; }
const ge = (from, to, o) => {
  o = o || {};
  const pts = o.pts || (o.h ? gH(GMAP[from], GMAP[to]) : gV(GMAP[from], GMAP[to]));
  let label;
  if (o.label) { label = { t: o.label.t || o.label, x: o.label.x, y: o.label.y }; if (label.x === undefined) { const m = gMid(pts); label.x = m[0]; label.y = m[1]; } }
  return { from, to, pts, loop: o.loop, biDir: o.biDir, kind: o.kind, label };
};

const GRAND_EDGES = [
  ge("gUi", "gPipe", { h: true }),
  ge("gPipe", "gKwbank"),
  ge("gKw1", "gKw2"), ge("gKw2", "gKw3"), ge("gKw3", "gKw4"), ge("gKw4", "gKw5"),
  ge("gKw1", "gKwbank", { h: true, label: "interleave" }),
  ge("gKwbank", "gMode"),
  ge("gMode", "gEngine"),
  ge("gEngine", "gApi1", { h: true }),
  ge("gApi1", "gApi2", { kind: "fallback", label: "⚡ credits out" }),
  ge("gApi2", "gApi3", { kind: "fallback", label: "⚡ rotate / empty" }),
  ge("gApi3", "gApi4", { kind: "fallback", label: "⚡ still short" }),
  ge("gApi4", "gApi5", { label: "rank" }),
  ge("gEngine", "gVerify"),
  ge("gVerify", "gEnrich"),
  ge("gEnrich", "gNm1"), ge("gEnrich", "gEm1"), ge("gEnrich", "gPh1"), ge("gEnrich", "gFo1"),
  ge("gNm1", "gNm3", { kind: "fallback", label: "⚡ 0 people" }),
  ge("gEm1", "gEm2"),
  ge("gPh1", "gPh2", { kind: "fallback", label: "⚡ free" }),
  ge("gEnrich", "gGates"),
  ge("gGates", "gRank"),
  ge("gRank", "gAssembly"),
  ge("gAssembly", "gDB"), ge("gAssembly", "gCsv"),
  ge("gDB", "gCsv", { h: true, biDir: true, label: "sync" }),
  ge("gCsv", "gFront"),
  { from: "gRank", to: "gKw1", pts: [[715, 1824], [40, 1824], [40, 302], [90, 302]], loop: true, label: { t: "↩ new slices", x: 40, y: 1060 } },
  { from: "gDB", to: "gGates", pts: [[626, 2090], [626, 1668], [715, 1668]], loop: true, label: { t: "↩ dedup", x: 640, y: 1880 } },
];

// ---- Graph helpers ----------------------------------------------------------
function buildAdjacency(edges) {
  const fwd = {};
  const rev = {};
  edges.forEach((e) => {
    if (e.loop) return;
    (fwd[e.from] = fwd[e.from] || []).push(e.to);
    (rev[e.to] = rev[e.to] || []).push(e.from);
  });
  return { fwd, rev };
}
function walk(start, adj) {
  const seen = new Set();
  const stack = [start];
  while (stack.length) {
    const n = stack.pop();
    (adj[n] || []).forEach((m) => {
      if (!seen.has(m)) { seen.add(m); stack.push(m); }
    });
  }
  return seen;
}
function toPath(pts) {
  return "M" + pts[0].join(",") + pts.slice(1).map((p) => " L" + p.join(",")).join("");
}

// ---- Hooks ------------------------------------------------------------------
function useReducedMotion() {
  const [reduce, setReduce] = useState(false);
  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) return;
    const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
    const update = () => setReduce(mq.matches);
    update();
    if (mq.addEventListener) mq.addEventListener("change", update);
    else mq.addListener(update);
    return () => {
      if (mq.removeEventListener) mq.removeEventListener("change", update);
      else mq.removeListener(update);
    };
  }, []);
  return reduce;
}

function useFitScale(clipRef, pad, designW) {
  const dw = designW || DESIGN_W;
  const [scale, setScale] = useState(1);
  useLayoutEffect(() => {
    const el = clipRef.current;
    if (!el) return;
    const measure = () => {
      const avail = el.clientWidth - pad;
      setScale(Math.max(0.34, Math.min(1, avail / dw)));
    };
    measure();
    let ro;
    if (typeof ResizeObserver !== "undefined") { ro = new ResizeObserver(measure); ro.observe(el); }
    else if (typeof window !== "undefined") { window.addEventListener("resize", measure); }
    return () => {
      if (ro) ro.disconnect();
      else if (typeof window !== "undefined") window.removeEventListener("resize", measure);
    };
  }, [clipRef, pad, dw]);
  return scale;
}

// ---- Mini-flow (side panels) ------------------------------------------------
function MiniFlow({ title, steps, foot, mono }) {
  return (
    <div className={"ufc-panel ufc-mini" + (mono ? " mono" : "")}>
      <span className="ufc-panel-title">{title}</span>
      <div className="ufc-mini-steps">
        {steps.map((s, i) => (
          <React.Fragment key={i}>
            {i > 0 && (
              <div className={"ufc-mini-arr" + (s.warn ? " warn" : "")}>
                <span className="ar" aria-hidden>{s.warn ? "⚡" : "▼"}</span>
                {s.via && <span>{s.via}</span>}
              </div>
            )}
            <div className={"ufc-mini-step" + (s.star ? " star" : "")} style={{ "--c": s.color || "var(--paper)" }}>
              {s.rank && <span className="rk" aria-hidden>{s.rank}</span>}
              <span className="tx"><b>{s.label}</b><em>{s.note}</em></span>
            </div>
          </React.Fragment>
        ))}
      </div>
      {foot && <div className="ufc-mini-foot">{foot}</div>}
    </div>
  );
}

// ---- Styles -----------------------------------------------------------------
const STYLE_ID = "ufc-styles";
const STYLES = `
@import url('https://fonts.googleapis.com/css2?family=Archivo+Black&family=Archivo:wght@600;700;800;900&family=DM+Mono:wght@400;500&display=swap');

.ufc-root{
  --paper:#F3ECDC; --ink:#141210; --grey:#b6ae9c; --warn:#C2410C;
  --grid:rgba(20,18,16,.05);
  font-family:'Archivo',system-ui,-apple-system,sans-serif;
  color:var(--ink); -webkit-font-smoothing:antialiased; text-rendering:optimizeLegibility;
  padding:34px 18px;
}
.ufc-root *,.ufc-root *::before,.ufc-root *::after{box-sizing:border-box;}

.ufc-shell{
  position:relative; max-width:1580px; margin:0 auto; background:var(--paper);
  border:4px solid var(--ink); box-shadow:14px 14px 0 var(--ink);
  background-image:linear-gradient(var(--grid) 1px,transparent 1px),
                   linear-gradient(90deg,var(--grid) 1px,transparent 1px);
  background-size:32px 32px;
}
.ufc-shell::after{
  content:""; position:absolute; inset:0; pointer-events:none; opacity:.05; mix-blend-mode:multiply;
  background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='160' height='160'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='2'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
}

/* ---- Top bar ---- */
.ufc-bar{position:relative; display:flex; align-items:flex-end; justify-content:space-between; gap:22px; flex-wrap:wrap; padding:24px 28px 22px; border-bottom:4px solid var(--ink); background:var(--paper);}
.ufc-kicker{font-family:'DM Mono',monospace; font-size:11px; letter-spacing:.22em; text-transform:uppercase; margin:0 0 8px; opacity:.78;}
.ufc-title{font-family:'Archivo Black',Archivo,sans-serif; font-size:clamp(30px,5vw,54px); line-height:.9; letter-spacing:-.02em; margin:0; text-transform:uppercase;}
.ufc-title .hl{background:#FF2E7E; color:var(--ink); padding:0 .14em; margin-left:.06em; box-shadow:6px 6px 0 var(--ink); display:inline-block; transform:rotate(-1.6deg);}
.ufc-bar-right{display:flex; flex-direction:column; align-items:flex-end; gap:14px;}
.ufc-legend{display:flex; gap:16px; flex-wrap:wrap; font-family:'DM Mono',monospace; font-size:10.5px; letter-spacing:.12em; text-transform:uppercase; align-items:center;}
.ufc-legend span{display:inline-flex; align-items:center; gap:7px; white-space:nowrap;}
.ufc-legend .ln{width:24px; height:0; border-top:4px solid var(--ink); display:inline-block;}
.ufc-legend .ln.dash{border-top-style:dashed;}
.ufc-legend .st{font-size:13px; line-height:1;}
.ufc-replay{font-family:'Archivo Black',sans-serif; text-transform:uppercase; font-size:13px; letter-spacing:.04em; background:#FFD23F; color:var(--ink); border:3px solid var(--ink); box-shadow:5px 5px 0 var(--ink); padding:10px 17px; cursor:pointer; display:inline-flex; gap:9px; align-items:center; transition:transform .08s ease, box-shadow .08s ease;}
.ufc-replay:hover{transform:translate(-1px,-1px); box-shadow:6px 6px 0 var(--ink);}
.ufc-replay:active{transform:translate(4px,4px); box-shadow:1px 1px 0 var(--ink);}
.ufc-replay:focus-visible{outline:3px solid var(--ink); outline-offset:3px;}
.ufc-replay svg{display:block;}
.ufc-sticker{position:absolute; top:-16px; right:34px; transform:rotate(4deg); background:#14D6C4; border:3px solid var(--ink); box-shadow:3px 3px 0 var(--ink); font-family:'DM Mono',monospace; font-weight:500; font-size:11px; letter-spacing:.16em; text-transform:uppercase; padding:6px 11px;}

/* ---- Main 3-zone grid ---- */
.ufc-main{display:grid; grid-template-columns:282px minmax(0,1fr) 282px; gap:26px; padding:26px 26px 10px; align-items:start; position:relative; z-index:3;}
.ufc-side{display:flex; flex-direction:column; gap:18px;}
.ufc-center{min-width:0;}

/* ---- Panels ---- */
.ufc-panel{background:var(--paper); border:3px solid var(--ink); box-shadow:6px 6px 0 var(--ink); padding:16px 17px;}
.ufc-panel-title{font-family:'Archivo Black',sans-serif; font-size:13px; text-transform:uppercase; letter-spacing:.02em; margin:0 0 14px; display:inline-block; background:var(--ink); color:var(--paper); padding:4px 9px;}
.ufc-why{list-style:none; margin:0; padding:0; display:flex; flex-direction:column; gap:11px;}
.ufc-why li{font-family:'DM Mono',monospace; font-size:11.5px; line-height:1.45; padding-left:19px; position:relative;}
.ufc-why li::before{content:"✦"; position:absolute; left:0; top:0; color:#FF2E7E;}
.ufc-stats{display:grid; grid-template-columns:1fr 1fr; gap:10px;}
.ufc-stat{border:2px solid var(--ink); padding:10px 11px; background:var(--paper);}
.ufc-stat b{font-family:'Archivo Black',sans-serif; font-size:24px; display:block; line-height:1; color:var(--c);}
.ufc-stat span{font-family:'DM Mono',monospace; font-size:9.5px; text-transform:uppercase; letter-spacing:.1em; opacity:.8; display:block; margin-top:5px;}
.ufc-check{list-style:none; margin:0; padding:0; display:flex; flex-direction:column; gap:12px;}
.ufc-check li{font-family:'DM Mono',monospace; font-size:11px; line-height:1.45;}
.ufc-check b{display:inline-block; font-family:'Archivo',sans-serif; font-weight:800; font-size:9.5px; text-transform:uppercase; letter-spacing:.08em; background:var(--c); border:2px solid var(--ink); padding:1px 7px; margin-right:7px;}
.ufc-sources{display:flex; flex-wrap:wrap; gap:7px;}
.ufc-source{font-family:'DM Mono',monospace; font-size:10px; letter-spacing:.04em; text-transform:uppercase; border:2px solid var(--ink); border-left:6px solid var(--c); padding:3px 8px; background:var(--paper);}
.ufc-sample{display:flex; flex-direction:column; gap:7px;}
.ufc-sample .row{display:flex; justify-content:space-between; gap:10px; font-family:'DM Mono',monospace; font-size:11px; border-bottom:1px dashed rgba(20,18,16,.25); padding-bottom:5px;}
.ufc-sample .row span:first-child{opacity:.6; text-transform:uppercase; letter-spacing:.06em; font-size:9.5px;}
.ufc-sample .row span:last-child{font-weight:500; text-align:right;}
.ufc-sample .src{background:#19C37D; border:2px solid var(--ink); padding:0 5px;}

/* ---- Mini-flow ---- */
.ufc-mini-steps{display:flex; flex-direction:column;}
.ufc-mini-step{position:relative; border:3px solid var(--ink); background:var(--c); box-shadow:4px 4px 0 var(--ink); padding:9px 11px; display:flex; gap:9px; align-items:flex-start;}
.ufc-mini-step .rk{flex:0 0 auto; font-family:'Archivo Black',sans-serif; font-size:12px; width:22px; height:22px; display:flex; align-items:center; justify-content:center; background:var(--ink); color:var(--paper);}
.ufc-mini-step .tx{min-width:0;}
.ufc-mini-step .tx b{font-family:'Archivo Black',sans-serif; font-size:12px; text-transform:uppercase; display:block; line-height:1.05; letter-spacing:-.01em; word-break:break-word;}
.ufc-mini-step .tx em{font-family:'DM Mono',monospace; font-size:9.5px; font-style:normal; letter-spacing:.03em; opacity:.85; display:block; margin-top:3px; line-height:1.3;}
.ufc-mini.mono .ufc-mini-step{background:var(--paper);}
.ufc-mini.mono .ufc-mini-step .tx b{font-family:'DM Mono',monospace; font-weight:500; text-transform:none; font-size:12px;}
.ufc-mini-step.star::after{content:"★"; position:absolute; top:-11px; right:-9px; background:#FFD23F; border:2px solid var(--ink); width:22px; height:22px; display:flex; align-items:center; justify-content:center; font-size:11px; transform:rotate(8deg); box-shadow:1px 1px 0 var(--ink);}
.ufc-mini-arr{display:flex; align-items:center; gap:8px; padding:5px 0 5px 6px; font-family:'DM Mono',monospace; font-size:9.5px; text-transform:uppercase; letter-spacing:.06em; opacity:.85;}
.ufc-mini-arr .ar{font-size:13px; line-height:1;}
.ufc-mini-arr.warn{color:var(--warn); font-weight:500; opacity:1;}
.ufc-mini-foot{margin-top:13px; border:2px dashed var(--ink); padding:9px 11px; font-family:'DM Mono',monospace; font-size:10px; line-height:1.45; background:rgba(20,18,16,.04);}

/* ---- Stage ---- */
.ufc-stageclip{overflow:visible; padding:8px 4px 16px;}
.ufc-stagewrap{position:relative;}
.ufc-stage{position:absolute; top:0; left:0; transform-origin:top left;}
.ufc-stage-inner{position:absolute; inset:0;}

.ufc-edges{position:absolute; inset:0; overflow:visible; pointer-events:none;}
.ufc-edge{fill:none; stroke:var(--ink); stroke-width:4; stroke-linecap:butt; stroke-linejoin:miter; transition:stroke .18s ease, stroke-width .18s ease, opacity .18s ease;}
.ufc-edge--loop{stroke-dasharray:14 11;}
.ufc-edge.off{stroke:var(--grey); stroke-width:3; opacity:.5;}
.ufc-edge.on{stroke:var(--c); stroke-width:6;}
.ufc-root:not(._reduce) .ufc-edge.on{stroke-dasharray:16 12 !important; animation:ufc-march .5s linear infinite;}
@keyframes ufc-march{to{stroke-dashoffset:-28;}}

/* ---- Nodes ---- */
.ufc-node{position:absolute; border:3px solid var(--ink); background:var(--c); box-shadow:7px 7px 0 var(--ink); display:flex; flex-direction:column; justify-content:center; gap:5px; padding:12px 18px; cursor:pointer; --c:#fff; text-align:center; align-items:center; animation:ufc-pop .52s cubic-bezier(.2,.9,.25,1.15) backwards; transition:transform .18s cubic-bezier(.2,.8,.2,1), box-shadow .18s ease, filter .2s ease, opacity .2s ease;}
.ufc-node:focus{outline:none;}
.ufc-node:focus-visible{outline:3px solid var(--ink); outline-offset:4px;}
.ufc-node-title{font-family:'Archivo Black',sans-serif; text-transform:uppercase; font-size:19px; line-height:1.04; letter-spacing:-.01em;}
.ufc-node-sub{font-family:'DM Mono',monospace; font-size:10px; letter-spacing:.1em; text-transform:uppercase; opacity:.9;}
.ufc-ink-light{color:#fff;} .ufc-ink-dark{color:var(--ink);}
.ufc-node-idx{position:absolute; top:-13px; left:-13px; background:var(--ink); color:var(--paper); font-family:'DM Mono',monospace; font-size:11px; font-weight:500; width:24px; height:24px; display:flex; align-items:center; justify-content:center; border:2px solid var(--paper); z-index:2;}
.ufc-node-tag{position:absolute; bottom:-12px; right:-8px; background:var(--paper); border:2px solid var(--ink); box-shadow:2px 2px 0 var(--ink); font-family:'DM Mono',monospace; font-size:9.5px; font-weight:500; letter-spacing:.12em; text-transform:uppercase; padding:2px 7px; color:var(--ink); z-index:2;}
.ufc-node-plus{position:absolute; bottom:-11px; left:-9px; width:22px; height:22px; background:#FFD23F; border:2px solid var(--ink); display:flex; align-items:center; justify-content:center; font-family:'Archivo Black',sans-serif; font-size:14px; color:var(--ink); z-index:2; transition:transform .18s ease;}
.ufc-node:hover .ufc-node-plus{transform:rotate(45deg);}
.ufc-node--hero{border-width:4px; box-shadow:9px 9px 0 var(--ink);}
.ufc-node--hero .ufc-node-title{font-size:21px;}
.ufc-node--engine{border-width:5px; box-shadow:10px 10px 0 var(--ink);}
.ufc-node--engine::before{content:""; position:absolute; inset:5px; border:2px solid var(--ink); pointer-events:none;}
.ufc-node--engine .ufc-node-title{font-size:24px;}
.ufc-star{position:absolute; top:-16px; right:-14px; background:#FFD23F; border:3px solid var(--ink); width:30px; height:30px; display:flex; align-items:center; justify-content:center; font-size:15px; transform:rotate(9deg); box-shadow:2px 2px 0 var(--ink); color:var(--ink); z-index:2;}
.ufc-node.on{transform:translate(-2px,-3px); box-shadow:11px 13px 0 var(--ink); z-index:5;}
.ufc-node:hover, .ufc-node:focus{z-index:50;}
.ufc-node--hero.on{box-shadow:13px 15px 0 var(--ink);}
.ufc-node--engine.on{box-shadow:14px 15px 0 var(--ink);}
.ufc-node.off{filter:grayscale(1) contrast(.85); opacity:.4;}
.ufc-node.off .ufc-star{opacity:0;}

/* ---- Node hover pop-over (expand for detail) ---- */
.ufc-node-pop{position:absolute; top:calc(100% + 14px); left:50%; transform:translateX(-50%) translateY(-6px); width:max(406px,116%); max-width:616px; background:var(--paper); color:var(--ink); border:3px solid var(--ink); box-shadow:9px 9px 0 var(--ink); padding:18px 21px 20px; text-align:left; opacity:0; pointer-events:none; z-index:40; transition:opacity .16s ease, transform .16s ease;}
.ufc-node-pop::before{content:""; position:absolute; top:-9px; left:calc(50% - 7px); width:13px; height:13px; background:var(--paper); border-left:3px solid var(--ink); border-top:3px solid var(--ink); transform:rotate(45deg);}
.ufc-node-pop-h{display:block; color:var(--ink); font-family:'Archivo Black',sans-serif; font-size:18px; text-transform:uppercase; letter-spacing:.01em; margin-bottom:13px; padding-bottom:11px; border-bottom:2px solid var(--ink);}
.ufc-node-pop ul{list-style:none; margin:0; padding:0; display:flex; flex-direction:column; gap:10px;}
.ufc-node-pop li{color:var(--ink); font-family:'DM Mono',monospace; font-size:17.5px; line-height:1.55; padding-left:21px; position:relative; text-transform:none; letter-spacing:0;}
.ufc-node-pop li::before{content:"▸"; position:absolute; left:0; top:0; opacity:.5;}
.ufc-node:hover .ufc-node-pop, .ufc-node:focus .ufc-node-pop{opacity:1; transform:translateX(-50%) translateY(0); pointer-events:auto;}
.ufc-root._reduce .ufc-node-pop{transition:opacity .12s ease; transform:translateX(-50%) !important;}

/* ---- Edge labels ---- */
.ufc-elabel{position:absolute; transform:translate(-50%,-50%); background:var(--paper); border:2px solid var(--ink); box-shadow:2px 2px 0 var(--ink); font-family:'DM Mono',monospace; font-weight:500; font-size:10px; letter-spacing:.08em; text-transform:uppercase; padding:3px 8px; pointer-events:none; white-space:nowrap; transition:opacity .2s ease, filter .2s ease, background .18s ease;}
.ufc-elabel.on{background:var(--c);}
.ufc-elabel.off{opacity:.4; filter:grayscale(1);}

/* ---- Info blocks ---- */
.ufc-info{padding:6px 26px 30px; position:relative; z-index:1;}
.ufc-info-head{border-top:4px solid var(--ink); padding-top:22px; margin-top:6px; display:flex; flex-direction:column; gap:6px;}
.ufc-info-title{font-family:'Archivo Black',sans-serif; font-size:clamp(22px,3.4vw,34px); text-transform:uppercase; letter-spacing:-.02em; margin:0; line-height:.95;}
.ufc-info-kicker{font-family:'DM Mono',monospace; font-size:11px; letter-spacing:.14em; text-transform:uppercase; opacity:.7; margin:0;}
.ufc-info-grid{display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:18px; margin-top:22px; align-items:start;}
.ufc-info-card{position:relative; background:var(--paper); border:3px solid var(--ink); border-left-width:9px; border-left-color:var(--c); box-shadow:6px 6px 0 var(--ink); padding:16px 18px 18px; --c:#fff; transition:transform .18s cubic-bezier(.2,.8,.2,1), box-shadow .18s ease, filter .2s ease, opacity .2s ease;}
.ufc-info-card-head{display:flex; align-items:center; gap:11px; margin-bottom:13px;}
.ufc-info-idx{background:var(--ink); color:var(--paper); font-family:'DM Mono',monospace; font-size:11px; font-weight:500; width:26px; height:26px; flex:0 0 26px; display:flex; align-items:center; justify-content:center;}
.ufc-info-name{font-family:'Archivo Black',sans-serif; font-size:15px; text-transform:uppercase; letter-spacing:-.01em; margin:0; line-height:1.05;}
.ufc-info-list{list-style:none; margin:0; padding:0; display:flex; flex-direction:column; gap:8px;}
.ufc-info-list li{font-family:'DM Mono',monospace; font-size:11.5px; line-height:1.5; padding-left:15px; position:relative;}
.ufc-info-list li::before{content:"▸"; position:absolute; left:0; top:0; color:var(--ink); opacity:.45;}
.ufc-info-card.on{transform:translate(-2px,-3px); box-shadow:10px 12px 0 var(--ink);}
.ufc-info-card.off{filter:grayscale(1); opacity:.5;}
.ufc-xconn{margin-top:24px; border:3px dashed var(--ink); padding:18px 20px; background:rgba(20,18,16,.03);}
.ufc-xconn h3{font-family:'Archivo Black',sans-serif; font-size:14px; text-transform:uppercase; margin:0 0 13px; letter-spacing:.02em;}
.ufc-xconn ul{list-style:none; margin:0; padding:0; display:grid; grid-template-columns:repeat(auto-fit,minmax(260px,1fr)); gap:9px 26px;}
.ufc-xconn li{font-family:'DM Mono',monospace; font-size:11px; line-height:1.45; padding-left:18px; position:relative;}
.ufc-xconn li::before{content:"⇄"; position:absolute; left:0; top:0;}

/* ---- Footer ---- */
.ufc-foot{padding:15px 28px 22px; border-top:3px solid var(--ink); display:flex; gap:20px; flex-wrap:wrap; align-items:center; font-family:'DM Mono',monospace; font-size:11px; letter-spacing:.07em; text-transform:uppercase;}
.ufc-foot b{font-weight:500; background:var(--ink); color:var(--paper); padding:2px 7px;}

/* ---- Corner registration marks ---- */
.ufc-corner{position:absolute; font-family:'DM Mono',monospace; font-size:15px; line-height:1; opacity:.45; pointer-events:none;}
.ufc-corner.tl{top:8px; left:10px;} .ufc-corner.tr{top:8px; right:10px;}
.ufc-corner.bl{bottom:8px; left:10px;} .ufc-corner.br{bottom:8px; right:10px;}

/* ---- Entrance ---- */
@keyframes ufc-pop{from{opacity:0; transform:translateY(16px) scale(.9) rotate(-2deg);}}
.ufc-root._reduce .ufc-node{animation:none;}
.ufc-root._reduce .ufc-edge{stroke-dasharray:none !important; stroke-dashoffset:0 !important; animation:none !important;}
@media (prefers-reduced-motion: reduce){
  .ufc-node{animation:none !important;} .ufc-edge{animation:none !important;} .ufc-replay{transition:none;}
}

/* ---- Responsive: stack zones, chart first ---- */
@media (max-width:1180px){
  .ufc-main{grid-template-columns:1fr;}
  .ufc-center{order:-1;}
}
@media (max-width:680px){
  .ufc-bar{flex-direction:column; align-items:flex-start;}
  .ufc-bar-right{align-items:flex-start;}
  .ufc-sticker{right:18px;}
}

/* ---- small nodes / fallback edges / band labels (grand chart) ---- */
.ufc-node--sm{padding:8px 10px; gap:3px;}
.ufc-node--sm .ufc-node-title{font-size:13px;}
.ufc-node--sm .ufc-node-sub{font-size:8.5px; letter-spacing:.06em;}
.ufc-edge--fallback{stroke:#C2410C !important; stroke-dasharray:7 6;}
.ufc-band{position:absolute; font-family:'Archivo Black',sans-serif; font-size:22px; text-transform:uppercase; letter-spacing:.02em; color:var(--ink); opacity:.3;}

/* ---- Grand breakdown CTA + section ---- */
.ufc-grand-cta{max-width:1580px; margin:34px auto 0; padding:0 18px; text-align:center;}
.ufc-grand-btn{font-family:'Archivo Black',sans-serif; text-transform:uppercase; font-size:clamp(15px,2.5vw,22px); letter-spacing:.02em; background:#FF2E7E; color:var(--ink); border:4px solid var(--ink); box-shadow:9px 9px 0 var(--ink); padding:18px 32px; cursor:pointer; display:inline-flex; align-items:center; gap:12px; transition:transform .1s ease, box-shadow .1s ease;}
.ufc-grand-btn:hover{transform:translate(-2px,-2px); box-shadow:12px 12px 0 var(--ink);}
.ufc-grand-btn:active{transform:translate(5px,5px); box-shadow:2px 2px 0 var(--ink);}
.ufc-grand-btn:focus-visible{outline:3px solid var(--ink); outline-offset:4px;}
.ufc-grand{margin-top:26px;}
.ufc-grand-bar{flex-direction:column; align-items:flex-start; gap:16px;}
.ufc-rolelegend{display:flex; flex-wrap:wrap; gap:10px 16px;}
.ufc-rolelegend span{display:inline-flex; align-items:center; gap:7px; font-family:'DM Mono',monospace; font-size:10px; letter-spacing:.1em; text-transform:uppercase;}
.ufc-rolelegend i{width:14px; height:14px; border:2px solid var(--ink); background:var(--c); display:inline-block;}
`;

function useInjectStyles() {
  useEffect(() => {
    if (typeof document === "undefined") return;
    if (document.getElementById(STYLE_ID)) return;
    const el = document.createElement("style");
    el.id = STYLE_ID;
    el.textContent = STYLES;
    document.head.appendChild(el);
  }, []);
}

// ---- Reusable stage (used by the grand breakdown) --------------------------
function FlowStage({ nodes, edges, designW, designH, bands }) {
  const reduce = useReducedMotion();
  const [hoverId, setHoverId] = useState(null);
  const [pinId, setPinId] = useState(null);
  const [animKey, setAnimKey] = useState(0);
  const clipRef = useRef(null);
  const pathRefs = useRef([]);
  const scale = useFitScale(clipRef, 8, designW);
  const active = hoverId !== null ? hoverId : pinId;
  const isDim = active !== null;
  const colorById = useMemo(() => Object.fromEntries(nodes.map((n) => [n.id, n.color])), [nodes]);
  const { fwd, rev } = useMemo(() => buildAdjacency(edges), [edges]);
  const highlight = useMemo(() => {
    if (active === null) return new Set();
    const s = new Set([active]);
    walk(active, fwd).forEach((x) => s.add(x));
    walk(active, rev).forEach((x) => s.add(x));
    return s;
  }, [active, fwd, rev]);

  useEffect(() => {
    const paths = pathRefs.current.filter(Boolean);
    if (!paths.length) return;
    if (reduce) { paths.forEach((p) => { p.style.transition = "none"; p.style.strokeDashoffset = "0"; }); return; }
    paths.forEach((p) => {
      const loop = p.dataset.loop === "1";
      let len = 0;
      try { len = p.getTotalLength(); } catch (e) { len = 1400; }
      p.style.transition = "none";
      if (!loop) p.style.strokeDasharray = len + "px";
      p.style.strokeDashoffset = len + "px";
    });
    void paths[0].getBoundingClientRect();
    const raf = requestAnimationFrame(() => {
      paths.forEach((p, i) => {
        p.style.transition = "stroke-dashoffset .6s cubic-bezier(.2,.8,.2,1) " + (0.25 + i * 0.025) + "s";
        p.style.strokeDashoffset = "0";
      });
    });
    return () => cancelAnimationFrame(raf);
  }, [animKey, reduce, edges]);

  const onNodeClick = (e, id) => { e.stopPropagation(); setPinId((p) => (p === id ? null : id)); };
  const onNodeKey = (e, id) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setPinId((p) => (p === id ? null : id)); } };

  return (
    <div className="ufc-stageclip" ref={clipRef}>
      <div className="ufc-stagewrap" style={{ width: designW * scale, height: designH * scale }}>
        <div className="ufc-stage" style={{ transform: "scale(" + scale + ")", width: designW, height: designH }} onClick={() => setPinId(null)}>
          <div className="ufc-stage-inner" key={animKey}>
            <svg className="ufc-edges" viewBox={"0 0 " + designW + " " + designH} width={designW} height={designH} aria-hidden>
              <defs>
                <marker id="ufc-arrow" markerWidth="14" markerHeight="14" refX="9.5" refY="6" orient="auto-start-reverse" markerUnits="userSpaceOnUse">
                  <path d="M1.5,1 L12,6 L1.5,11 Z" fill="context-stroke" />
                </marker>
              </defs>
              {edges.map((e, idx) => {
                const on = isDim && highlight.has(e.from) && highlight.has(e.to);
                const cls = "ufc-edge" + (e.loop ? " ufc-edge--loop" : "") + (e.kind === "fallback" ? " ufc-edge--fallback" : "") + (isDim ? (on ? " on" : " off") : "");
                return (
                  <path key={idx} d={toPath(e.pts)} className={cls} style={{ "--c": colorById[e.from] }}
                    markerEnd="url(#ufc-arrow)" markerStart={e.biDir ? "url(#ufc-arrow)" : undefined}
                    data-loop={e.loop ? "1" : "0"} ref={(el) => (pathRefs.current[idx] = el)} />
                );
              })}
            </svg>

            {bands && bands.map((b, i) => (
              <div key={"b" + i} className="ufc-band" style={{ left: b.x, top: b.y }} aria-hidden>{b.t}</div>
            ))}

            {edges.map((e, idx) => {
              if (!e.label) return null;
              const on = isDim && highlight.has(e.from) && highlight.has(e.to);
              const cls = "ufc-elabel" + (isDim ? (on ? " on" : " off") : "");
              return (<div key={"l" + idx} className={cls} style={{ left: e.label.x, top: e.label.y, "--c": colorById[e.from] }}>{e.label.t}</div>);
            })}

            {nodes.map((n, idx) => {
              const on = highlight.has(n.id);
              const cls = "ufc-node ufc-ink-" + n.ink + (n.hero ? " ufc-node--hero" : "") + (n.engine ? " ufc-node--engine" : "") + (n.small ? " ufc-node--sm" : "") + (isDim ? (on ? " on" : " off") : "");
              return (
                <div key={n.id} className={cls}
                  style={{ left: n.x, top: n.y, width: n.w, height: n.h, "--c": n.color, animationDelay: idx * 0.03 + "s" }}
                  tabIndex={0} role="button" aria-pressed={pinId === n.id} aria-label={n.title + " — " + n.sub}
                  onMouseEnter={() => setHoverId(n.id)} onMouseLeave={() => setHoverId(null)}
                  onFocus={() => setHoverId(n.id)} onBlur={() => setHoverId(null)}
                  onClick={(e) => onNodeClick(e, n.id)} onKeyDown={(e) => onNodeKey(e, n.id)}>
                  {n.hero && <span className="ufc-star" aria-hidden>{"★"}</span>}
                  <span className="ufc-node-title">{n.title}</span>
                  <span className="ufc-node-sub">{n.sub}</span>
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

// ---- Component --------------------------------------------------------------
export default function UserFlowChart({
  nodes = DEFAULT_NODES,
  edges = DEFAULT_EDGES,
  info = DEFAULT_INFO,
  xconn = DEFAULT_XCONN,
  title = "Lead",
  titleAccent = "Forge",
  kicker = "City Lead Discovery · End-to-End Pipeline",
  infoTitle = "Pipeline Detail",
  infoKicker = "Stage by stage — hover or pin a node above to spotlight its card",
}) {
  useInjectStyles();
  const reduce = useReducedMotion();

  const [hoverId, setHoverId] = useState(null);
  const [pinId, setPinId] = useState(null);
  const [animKey, setAnimKey] = useState(0);

  const clipRef = useRef(null);
  const pathRefs = useRef([]);
  const scale = useFitScale(clipRef, 8);

  const active = hoverId !== null ? hoverId : pinId;
  const isDim = active !== null;

  const colorById = useMemo(() => Object.fromEntries(nodes.map((n) => [n.id, n.color])), [nodes]);
  const infoById = useMemo(() => Object.fromEntries(info.map((c) => [c.id, c])), [info]);
  const { fwd, rev } = useMemo(() => buildAdjacency(edges), [edges]);
  const highlight = useMemo(() => {
    if (active === null) return new Set();
    const s = new Set([active]);
    walk(active, fwd).forEach((x) => s.add(x));
    walk(active, rev).forEach((x) => s.add(x));
    return s;
  }, [active, fwd, rev]);

  useEffect(() => {
    const paths = pathRefs.current.filter(Boolean);
    if (!paths.length) return;
    if (reduce) {
      paths.forEach((p) => { p.style.transition = "none"; p.style.strokeDashoffset = "0"; });
      return;
    }
    paths.forEach((p) => {
      const loop = p.dataset.loop === "1";
      let len = 0;
      try { len = p.getTotalLength(); } catch (e) { len = 1200; }
      p.style.transition = "none";
      if (!loop) p.style.strokeDasharray = len + "px";
      p.style.strokeDashoffset = len + "px";
    });
    void paths[0].getBoundingClientRect();
    const raf = requestAnimationFrame(() => {
      paths.forEach((p, i) => {
        p.style.transition = "stroke-dashoffset .6s cubic-bezier(.2,.8,.2,1) " + (0.3 + i * 0.04) + "s";
        p.style.strokeDashoffset = "0";
      });
    });
    return () => cancelAnimationFrame(raf);
  }, [animKey, reduce, edges]);

  const replay = () => { setPinId(null); setHoverId(null); setAnimKey((k) => k + 1); };
  const onNodeClick = (e, id) => { e.stopPropagation(); setPinId((p) => (p === id ? null : id)); };
  const onNodeKey = (e, id) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setPinId((p) => (p === id ? null : id)); }
  };

  const [grandOpen, setGrandOpen] = useState(false);
  const rootClass = "ufc-root" + (isDim ? " is-dim" : "") + (reduce ? " _reduce" : "");

  return (
    <div className={rootClass}>
      <div className="ufc-shell">
        <span className="ufc-corner tl" aria-hidden>+</span>
        <span className="ufc-corner tr" aria-hidden>+</span>
        <span className="ufc-corner bl" aria-hidden>+</span>
        <span className="ufc-corner br" aria-hidden>+</span>

        {/* Top bar */}
        <header className="ufc-bar">
          <span className="ufc-sticker" aria-hidden>{"★"} Live Pipeline</span>
          <div>
            <p className="ufc-kicker">{kicker}</p>
            <h1 className="ufc-title">{title} <span className="hl">{titleAccent}</span></h1>
          </div>
          <div className="ufc-bar-right">
            <div className="ufc-legend" aria-hidden>
              <span><i className="ln" />Flow</span>
              <span><i className="ln dash" />Loop / Feedback</span>
              <span><i className="st">{"⚡"}</i>Fallback</span>
              <span><i className="st">{"★"}</i>Key stage</span>
            </div>
            <button className="ufc-replay" type="button" onClick={replay}>
              <svg width="13" height="13" viewBox="0 0 13 13" fill="currentColor" aria-hidden>
                <path d="M2 1 L12 6.5 L2 12 Z" />
              </svg>
              Replay
            </button>
          </div>
        </header>

        {/* Main 3-zone grid */}
        <div className="ufc-main">
          {/* LEFT */}
          <aside className="ufc-side">
            <div className="ufc-panel">
              <span className="ufc-panel-title">The Edge</span>
              <ul className="ufc-why">
                {WHY_WINS.map((w, k) => <li key={k}>{w}</li>)}
              </ul>
            </div>
            <div className="ufc-panel">
              <span className="ufc-panel-title">By the Numbers</span>
              <div className="ufc-stats">
                {STATS.map((s, k) => (
                  <div className="ufc-stat" key={k} style={{ "--c": s.color }}>
                    <b>{s.num}</b><span>{s.label}</span>
                  </div>
                ))}
              </div>
            </div>
            <div className="ufc-panel">
              <span className="ufc-panel-title">Data Sources</span>
              <div className="ufc-sources">
                {DATA_SOURCES.map((s, k) => (
                  <span className="ufc-source" key={k} style={{ "--c": s.c }}>{s.n}</span>
                ))}
              </div>
            </div>
            <MiniFlow title="File → Command Flow" steps={FILE_FLOW} mono />
          </aside>

          {/* CENTER — pipeline */}
          <div className="ufc-center">
            <div className="ufc-stageclip" ref={clipRef}>
              <div className="ufc-stagewrap" style={{ width: DESIGN_W * scale, height: DESIGN_H * scale }}>
                <div className="ufc-stage" style={{ transform: "scale(" + scale + ")", width: DESIGN_W, height: DESIGN_H }} onClick={() => setPinId(null)}>
                  <div className="ufc-stage-inner" key={animKey}>
                    <svg className="ufc-edges" viewBox={"0 0 " + DESIGN_W + " " + DESIGN_H} width={DESIGN_W} height={DESIGN_H} aria-hidden>
                      <defs>
                        <marker id="ufc-arrow" markerWidth="14" markerHeight="14" refX="9.5" refY="6" orient="auto-start-reverse" markerUnits="userSpaceOnUse">
                          <path d="M1.5,1 L12,6 L1.5,11 Z" fill="context-stroke" />
                        </marker>
                      </defs>
                      {edges.map((e, idx) => {
                        const on = isDim && highlight.has(e.from) && highlight.has(e.to);
                        const cls = "ufc-edge" + (e.loop ? " ufc-edge--loop" : "") + (isDim ? (on ? " on" : " off") : "");
                        return (
                          <path key={idx} d={toPath(e.pts)} className={cls} style={{ "--c": colorById[e.from] }}
                            markerEnd="url(#ufc-arrow)" markerStart={e.biDir ? "url(#ufc-arrow)" : undefined}
                            data-loop={e.loop ? "1" : "0"} ref={(el) => (pathRefs.current[idx] = el)} />
                        );
                      })}
                    </svg>

                    {edges.map((e, idx) => {
                      if (!e.label) return null;
                      const on = isDim && highlight.has(e.from) && highlight.has(e.to);
                      const cls = "ufc-elabel" + (isDim ? (on ? " on" : " off") : "");
                      return (
                        <div key={"lbl" + idx} className={cls} style={{ left: e.label.x, top: e.label.y, "--c": colorById[e.from] }}>
                          {e.label.t}
                        </div>
                      );
                    })}

                    {nodes.map((n, idx) => {
                      const on = highlight.has(n.id);
                      const popCard = infoById[n.id];
                      const cls = "ufc-node ufc-ink-" + n.ink + (n.hero ? " ufc-node--hero" : "") +
                        (n.engine ? " ufc-node--engine" : "") + (isDim ? (on ? " on" : " off") : "");
                      return (
                        <div key={n.id} className={cls}
                          style={{ left: n.x, top: n.y, width: n.w, height: n.h, "--c": n.color, animationDelay: idx * 0.05 + "s" }}
                          tabIndex={0} role="button" aria-pressed={pinId === n.id}
                          aria-label={n.title + " — " + n.sub}
                          onMouseEnter={() => setHoverId(n.id)} onMouseLeave={() => setHoverId(null)}
                          onFocus={() => setHoverId(n.id)} onBlur={() => setHoverId(null)}
                          onClick={(e) => onNodeClick(e, n.id)} onKeyDown={(e) => onNodeKey(e, n.id)}>
                          <span className="ufc-node-idx" aria-hidden>{n.i}</span>
                          {n.hero && <span className="ufc-star" aria-hidden>{"★"}</span>}
                          <span className="ufc-node-title">{n.title}</span>
                          <span className="ufc-node-sub">{n.sub}</span>
                          {n.tag && <span className="ufc-node-tag" aria-hidden>{n.tag}</span>}
                          {popCard && <span className="ufc-node-plus" aria-hidden>+</span>}
                          {popCard && (
                            <div className="ufc-node-pop" role="tooltip">
                              <span className="ufc-node-pop-h">{popCard.title}</span>
                              <ul>{popCard.lines.map((l, k) => <li key={k}>{l}</li>)}</ul>
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* RIGHT */}
          <aside className="ufc-side">
            <MiniFlow title="Two Entry Modes — City vs Industry" steps={MODE_FLOW} foot={MODE_FOOT} />
            <MiniFlow title="Keyword Priority" steps={KEYWORD_FLOW} foot={KEYWORD_FOOT} />
            <MiniFlow title="API Failover" steps={API_FLOW} foot={API_FOOT} />
            <div className="ufc-panel">
              <span className="ufc-panel-title">Sample Lead Row</span>
              <div className="ufc-sample">
                {SAMPLE_ROW.map((r, k) => (
                  <div className="row" key={k}>
                    <span>{r[0]}</span>
                    <span>{r[0] === "Source" ? <span className="src">{r[1]}</span> : r[1]}</span>
                  </div>
                ))}
              </div>
            </div>
          </aside>
        </div>

        {/* Info blocks */}
        <section className="ufc-info">
          <div className="ufc-info-head">
            <h2 className="ufc-info-title">{infoTitle}</h2>
            <p className="ufc-info-kicker">{infoKicker}</p>
          </div>
          <div className="ufc-info-grid">
            {info.map((card) => {
              const on = active === card.id;
              const cls = "ufc-info-card" + (isDim ? (on ? " on" : " off") : "");
              return (
                <article key={card.id} className={cls} style={{ "--c": colorById[card.id] || "#141210" }}>
                  <header className="ufc-info-card-head">
                    <span className="ufc-info-idx">{card.i}</span>
                    <h3 className="ufc-info-name">{card.title}</h3>
                  </header>
                  <ul className="ufc-info-list">
                    {card.lines.map((l, k) => <li key={k}>{l}</li>)}
                  </ul>
                </article>
              );
            })}
          </div>
          {xconn && xconn.length > 0 && (
            <div className="ufc-xconn">
              <h3>Cross-connections — one card feeding many</h3>
              <ul>{xconn.map((l, k) => <li key={k}>{l}</li>)}</ul>
            </div>
          )}
        </section>

        {/* Footer */}
        <footer className="ufc-foot">
          <span><b>Hover</b> any node → expand full detail</span>
          <span><b>Click</b> to pin / unpin</span>
          <span><b>Tab + Enter</b> to navigate by keyboard</span>
        </footer>
      </div>

      <div className="ufc-grand-cta">
        <button className="ufc-grand-btn" type="button" onClick={() => setGrandOpen((o) => !o)} aria-expanded={grandOpen}>
          {grandOpen ? "▲  Hide full breakdown" : "▼  Give full detailed breakdown"}
        </button>
      </div>

      {grandOpen && (
        <div className="ufc-shell ufc-grand">
          <span className="ufc-corner tl" aria-hidden>+</span>
          <span className="ufc-corner tr" aria-hidden>+</span>
          <span className="ufc-corner bl" aria-hidden>+</span>
          <span className="ufc-corner br" aria-hidden>+</span>
          <header className="ufc-bar ufc-grand-bar">
            <div>
              <p className="ufc-kicker">Everything above, combined &amp; expanded</p>
              <h1 className="ufc-title">Full <span className="hl">Breakdown</span></h1>
            </div>
            <div className="ufc-rolelegend" aria-hidden>
              {GRAND_LEGEND.map((r, k) => (<span key={k}><i style={{ "--c": r.c }} />{r.n}</span>))}
            </div>
          </header>
          <FlowStage nodes={GRAND_NODES} edges={GRAND_EDGES} designW={GRAND_W} designH={GRAND_H} bands={GRAND_BANDS} />
          <footer className="ufc-foot">
            <span><b>Flow of command</b> top → bottom</span>
            <span><b>⚡ Fallback</b> · <b>↩ Backtrack</b> · <b>★ Primary</b></span>
            <span><b>Hover</b> any node to trace its path</span>
          </footer>
        </div>
      )}
    </div>
  );
}

export { DEFAULT_NODES, DEFAULT_EDGES, DEFAULT_INFO, DEFAULT_XCONN };
