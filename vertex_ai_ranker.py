"""vertex_ai_ranker.py — Semantic lead-domain ranking via Google Gemini API
(2026-05-25).

PURPOSE
-------
After discovery (SEMrush + SerpAPI + Google Places + Apollo fallback) the
pipeline can have 200+ raw domains for a 3-lead run. Apollo enrichment is
the most expensive downstream step, so we want to enrich the BEST domains
first. This module uses Gemini to semantically rank the candidates by
B2B buying intent, then returns the top-N.

API CHOICE
----------
Uses the Google Gemini API (https://aistudio.google.com/app/apikey) —
ONE key, no OAuth, no service account, no GCP project setup. Free tier
covers 1500 requests/day which is more than enough for typical lead-gen.

For full Vertex AI (service-account auth, Gemini Pro, embeddings, fine-
tuning) swap the endpoint + add the GoogleAuth header. The downstream
ranking logic stays identical.

CONFIG
------
  GEMINI_API_KEY                env var — single key from aistudio.google.com
  VERTEX_MAX_DOMAINS_PER_RANK   env var (default 60) — cap per call
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Callable, Iterable, List, Optional, Tuple

import requests

log = logging.getLogger("leadforge.vertex_ai")


def _env_int(name: str, default: int) -> int:
    try:
        v = int(os.environ.get(name, "") or "")
        return v if v > 0 else default
    except (TypeError, ValueError):
        return default


GEMINI_API_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent"
)
MAX_DOMAINS_PER_RANK = _env_int("VERTEX_MAX_DOMAINS_PER_RANK", 60)

# Domains we never want at the top regardless of model score — directories,
# marketplaces, social pages, etc. The model usually catches these, but
# this hard-deny list is a safety net.
_HARD_DENY = {
    "yellowpages.com.au", "yelp.com.au", "yelp.com", "hipages.com.au",
    "truelocal.com.au", "oneflare.com.au", "localsearch.com.au",
    "serviceseeking.com.au", "airtasker.com", "houzz.com.au", "houzz.com",
    "facebook.com", "instagram.com", "linkedin.com", "twitter.com",
    "x.com", "tiktok.com", "youtube.com", "pinterest.com",
    "wikipedia.org", "google.com", "trustpilot.com", "productreview.com.au",
}


class VertexAIRanker:
    """Score and rank domains by B2B buying intent using Gemini.

    Single batched call per rank_domains() invocation — passes the full
    candidate list + industry context in one prompt, receives a JSON
    array with per-domain scores. Falls back gracefully (input returned
    unchanged with score=50) if the API key is missing or the call fails.

    Public surface:
        __init__(api_key, log_fn=None)
        .available             bool — True iff API key configured
        .calls_made            int — HTTPS round-trips this instance has done
        rank_domains(domains, industry, country, top_n) -> List[(domain,score)]
    """

    def __init__(self, api_key: str, log_fn: Optional[Callable[[str], None]] = None) -> None:
        self.api_key = (api_key or "").strip()
        self._log = log_fn or (lambda m: log.info(m))
        self.available = bool(self.api_key)
        self.calls_made = 0

    def rank_domains(
        self,
        domains: Iterable[str],
        industry: str,
        country: str = "AU",
        top_n: int = 0,
    ) -> List[Tuple[str, int]]:
        """Score every input domain 0-100; return sorted highest-score first.

        Args:
            domains: candidate root domains (any iterable; deduped).
            industry: human-readable industry name (e.g. "Plumber, Electrician").
            country: ISO-style country (used in prompt context only).
            top_n: if > 0, truncate output to N highest-scored.

        Returns:
            list of (domain, score). When API unavailable, returns the
            input list unchanged with score=50 (so downstream code can
            treat the call as identity-on-failure).
        """
        # Dedupe + drop hard-deny set
        seen: set = set()
        clean: List[str] = []
        for d in domains:
            dl = (d or "").strip().lower()
            if dl and dl not in seen and dl not in _HARD_DENY:
                seen.add(dl)
                clean.append(dl)
        if not clean:
            return []
        # Fallback path: no API key configured
        if not self.available:
            self._log(
                "[VertexAI] no GEMINI_API_KEY — skipping semantic rank; "
                "returning input order with score=50"
            )
            return [(d, 50) for d in clean]
        # Cap input — Gemini handles ~60 domains in one prompt comfortably.
        batch = clean[:MAX_DOMAINS_PER_RANK]
        prompt = self._build_prompt(batch, industry, country)
        try:
            r = requests.post(
                f"{GEMINI_API_URL}?key={self.api_key}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {
                        "temperature": 0.2,
                        "responseMimeType": "application/json",
                        "maxOutputTokens": 4096,
                    },
                },
                timeout=30,
            )
        except Exception as exc:
            self.calls_made += 1
            self._log(f"[VertexAI] HTTP exception: {exc} — using input order")
            return [(d, 50) for d in clean]
        self.calls_made += 1
        if r.status_code != 200:
            try:
                _err = (r.json().get("error") or {}).get("message", "")[:200]
            except Exception:
                _err = r.text[:200]
            self._log(f"[VertexAI] HTTP {r.status_code}: {_err}")
            return [(d, 50) for d in clean]
        # Parse model response — Gemini in JSON-mode returns the JSON
        # text inside .candidates[0].content.parts[0].text.
        try:
            data = r.json()
            text = data["candidates"][0]["content"]["parts"][0]["text"]
            scored_raw = json.loads(text)
            if not isinstance(scored_raw, list):
                raise ValueError("model did not return a JSON array")
            score_map = {}
            for item in scored_raw:
                if not isinstance(item, dict):
                    continue
                d = (item.get("domain") or "").strip().lower()
                s = int(item.get("score") or 50)
                if d:
                    score_map[d] = max(0, min(100, s))
        except Exception as exc:
            self._log(f"[VertexAI] parse error: {exc} — using input order")
            return [(d, 50) for d in clean]
        # Apply scores; default 50 for domains the model didn't return
        ranked = sorted(
            [(d, score_map.get(d, 50)) for d in clean],
            key=lambda x: x[1],
            reverse=True,
        )
        out = ranked[:top_n] if top_n and top_n > 0 else ranked
        self._log(
            f"[VertexAI] ranked {len(ranked)} domains "
            f"(top score={ranked[0][1]}, bottom={ranked[-1][1]}); "
            f"keeping top {len(out)}"
        )
        return out

    def _build_prompt(self, domains: List[str], industry: str, country: str) -> str:
        """Compose the ranking prompt. JSON-mode response forces structured
        output so we can parse confidently."""
        return (
            f"You are a B2B lead-qualification expert specializing in {country} "
            f"businesses. Below is a list of website domains discovered for "
            f"the industry context: '{industry}'.\n\n"
            f"Score each domain 0-100 for B2B buying intent. HIGH score (75+) for:\n"
            f"  • Established SMB/medium businesses with their own website\n"
            f"  • Businesses that visibly run paid ads / commercial intent\n"
            f"  • Sites whose name suggests they offer products/services in the industry\n"
            f"  • Local AU service businesses (plumber, electrician, agency, etc.)\n"
            f"LOW score (< 30) for:\n"
            f"  • Aggregator directories (yellowpages, hipages, truelocal, etc.)\n"
            f"  • Marketplaces (eBay, Amazon, Facebook, gumtree)\n"
            f"  • Blogs, news sites, Wikipedia, review aggregators\n"
            f"  • Generic landing pages / parked domains\n\n"
            f"Domains:\n" + "\n".join(f"  - {d}" for d in domains) + "\n\n"
            f"Reply with ONLY a JSON array, one object per domain:\n"
            f'  [{{"domain": "acme.com.au", "score": 85}}, ...]\n'
            f"No prose, no markdown fences — just the JSON."
        )


# ── Offline smoke test ──────────────────────────────────────────────────
if __name__ == "__main__":
    # Shape-only test (no network); confirms imports + dedup + hard-deny.
    r = VertexAIRanker("")  # no key → fallback path
    out = r.rank_domains(
        ["acme.com.au", "yelp.com.au", "ACME.com.au", "bobplumbing.com"],
        industry="Plumber",
        country="AU",
    )
    assert all(isinstance(t, tuple) and len(t) == 2 for t in out), out
    assert "yelp.com.au" not in [d for d, _ in out], "hard-deny failed"
    assert len([d for d, _ in out]) == 2, "dedup failed"  # acme dedup + yelp drop
    print(f"vertex_ai_ranker smoke ok: {len(out)} domains in fallback mode")
