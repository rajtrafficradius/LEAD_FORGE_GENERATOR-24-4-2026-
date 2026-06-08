"""
Parallel multi-industry lead generation orchestrator.

Divides max_leads equally across all specified industries and runs N_WORKERS
pipelines simultaneously. Each worker runs LeadGenerationPipeline for one
industry at a time, targeting quota_per_industry leads. Results are combined,
deduplicated and written to a single CSV at the end.

Usage (via wsgi.py /generate-multi route):
    orchestrator = ParallelLeadOrchestrator(industries=all_inds, max_leads=100, ...)
    combined_csv_path = orchestrator.run()
"""
from __future__ import annotations

import csv
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

log = logging.getLogger(__name__)

# CSV columns written to the combined output — mirrors V5 Phase 6 export
_CSV_FIELDS = [
    "Name", "Company Name", "Domain", "Role",
    "Phone Number", "Email", "Email Type",
    "Industry", "Country", "Source", "Lead Score",
    "Paid Traffic", "Organic Traffic", "Paid Keywords", "Organic Keywords", "Revenue",
]


class ParallelLeadOrchestrator:
    """
    Runs one LeadGenerationPipeline per industry in a thread pool.
    Progress is reported as a fraction of completed industries (0→90%),
    with 90→100% reserved for the CSV-write phase.
    """

    def __init__(
        self,
        industries: list[str],
        max_leads: int,
        country: str = "AU",
        min_volume: int = 100,
        min_cpc: float = 0.05,
        enrichment_enabled: bool = True,
        output_folder: str = "output",
        n_workers: int = 4,
        progress_callback=None,
        log_callback=None,
    ):
        if not industries:
            raise ValueError("industries must not be empty")
        if max_leads <= 0:
            raise ValueError("max_leads must be > 0")

        self.industries = list(industries)
        self.max_leads = max_leads
        self.quota_per_industry = max(1, max_leads // len(self.industries))
        self.country = country
        self.min_volume = min_volume
        self.min_cpc = min_cpc
        self.enrichment_enabled = enrichment_enabled
        self.output_folder = output_folder
        self.n_workers = max(1, min(n_workers, len(self.industries)))
        self._progress = progress_callback or (lambda p, s="": None)
        self._log = log_callback or (lambda m: log.info(m))

        # Shared state
        self.all_leads: list[dict] = []
        self._leads_lock = threading.Lock()
        self._cancelled = False
        self._pipeline_refs: list = []
        self._pipeline_refs_lock = threading.Lock()
        self._api_counter: dict = {}
        self._counter_lock = threading.Lock()

    # ── public ────────────────────────────────────────────────────────────────

    def cancel(self):
        self._cancelled = True
        with self._pipeline_refs_lock:
            refs = list(self._pipeline_refs)
        for p in refs:
            try:
                p.cancel()
            except Exception:
                pass

    def run(self) -> str:
        """Run all industries in parallel, return path to combined CSV (or '' on cancel)."""
        os.makedirs(self.output_folder, exist_ok=True)
        total = len(self.industries)
        self._log(
            f"[PARALLEL] Starting {total} agents across {self.n_workers} workers — "
            f"{self.quota_per_industry} leads/industry (total target: {total * self.quota_per_industry})"
        )
        self._progress(2, f"Launching {total} parallel industry agents…")

        with ThreadPoolExecutor(max_workers=self.n_workers, thread_name_prefix="lead_agent") as ex:
            futures = {
                ex.submit(self._run_one_industry, ind): ind
                for ind in self.industries
            }
            done_count = 0
            for future in as_completed(futures):
                if self._cancelled:
                    break
                done_count += 1
                pct = 2 + int(done_count / total * 85)
                ind_name = futures[future]
                try:
                    leads = future.result()
                    self._log(
                        f"[PARALLEL] {ind_name}: {len(leads)} leads "
                        f"({done_count}/{total} industries done)"
                    )
                    self._progress(pct, f"Done {done_count}/{total}: {ind_name}")
                except Exception as exc:
                    log.error("[PARALLEL] %s raised: %s", ind_name, exc)
                    self._log(f"[PARALLEL] ERROR in {ind_name}: {exc}")

        if self._cancelled:
            self._log("[PARALLEL] Cancelled — no CSV written.")
            return ""

        self._progress(90, "Combining and deduplicating leads…")
        out_path = self._write_combined_csv()
        self._progress(100, f"Done! {len(self.all_leads)} total leads written.")
        return out_path

    # ── internal ──────────────────────────────────────────────────────────────

    def _run_one_industry(self, industry: str) -> list[dict]:
        """Runs a single LeadGenerationPipeline for one industry; returns its leads."""
        if self._cancelled:
            return []

        from V5 import LeadGenerationPipeline

        ind_safe = industry[:30].replace("/", "-").replace(" ", "_")
        ind_output = os.path.join(self.output_folder, f"agent_{ind_safe}")
        os.makedirs(ind_output, exist_ok=True)

        def ind_log(msg):
            self._log(f"[{industry}] {msg}")

        pipeline = LeadGenerationPipeline(
            industry=industry,
            country=self.country,
            min_volume=self.min_volume,
            min_cpc=self.min_cpc,
            output_folder=ind_output,
            progress_callback=None,
            log_callback=ind_log,
            max_leads=self.quota_per_industry,
            enrichment_enabled=self.enrichment_enabled,
        )

        with self._pipeline_refs_lock:
            self._pipeline_refs.append(pipeline)

        try:
            pipeline.run()
        except Exception as exc:
            ind_log(f"Pipeline error: {exc}")
            log.exception("[PARALLEL] pipeline.run() error for %s", industry)

        leads = list(getattr(pipeline, "leads", []) or [])
        for ld in leads:
            ld.setdefault("_industry", industry)
            ld.setdefault("_country", self.country)

        # Accumulate API usage counters
        usage = dict(getattr(pipeline, "_api_counter", {}) or {})
        with self._counter_lock:
            for k, v in usage.items():
                self._api_counter[k] = self._api_counter.get(k, 0) + v

        with self._leads_lock:
            self.all_leads.extend(leads)

        return leads

    def _write_combined_csv(self) -> str:
        """Deduplicate all_leads, cap at max_leads, write CSV, return path."""
        seen: set[tuple] = set()
        unique: list[dict] = []
        for ld in self.all_leads:
            key = (
                (ld.get("name") or "").lower().strip(),
                (ld.get("domain") or "").lower().strip(),
            )
            if key == ("", ""):
                continue
            if key not in seen:
                seen.add(key)
                unique.append(ld)

        # Enforce global cap
        unique = unique[: self.max_leads]
        self.all_leads = unique  # update so callers see the final set

        ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        out_path = os.path.join(self.output_folder, f"leads_parallel_{ts}.csv")

        with open(out_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for ld in unique:
                writer.writerow({
                    "Name": ld.get("name", ""),
                    "Company Name": ld.get("company", ""),
                    "Domain": ld.get("domain", ""),
                    "Role": ld.get("role", ""),
                    "Phone Number": ld.get("phone", ""),
                    "Email": ld.get("email", ""),
                    "Email Type": ld.get("email_type") or ld.get("_email_type", ""),
                    "Industry": ld.get("_industry", ""),
                    "Country": ld.get("_country", self.country),
                    "Source": ld.get("source", ""),
                    "Lead Score": ld.get("lead_score", ""),
                    "Paid Traffic": ld.get("_paid_traffic", 0),
                    "Organic Traffic": ld.get("_organic_traffic", 0),
                    "Paid Keywords": ld.get("_paid_keywords", 0),
                    "Organic Keywords": ld.get("_organic_keywords", 0),
                    "Revenue": ld.get("_revenue", ""),
                })

        self._log(f"[PARALLEL] Combined CSV: {out_path} ({len(unique)} leads)")
        return out_path