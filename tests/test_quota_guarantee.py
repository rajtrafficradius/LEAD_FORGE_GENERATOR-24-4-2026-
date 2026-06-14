import csv
import os
import shutil
import sys
import tempfile
import unittest


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import V5  # noqa: E402
from V5 import LeadGenerationPipeline  # noqa: E402
from city_pipeline import CityLeadPipeline  # noqa: E402


def make_pipeline(max_leads=10, quota_guarantee=True, enrichment_enabled=False):
    out_dir = tempfile.mkdtemp(prefix="leadforge_quota_")
    pipe = LeadGenerationPipeline(
        industry="Quota Test",
        country="AU",
        min_volume=0,
        min_cpc=0.0,
        output_folder=out_dir,
        max_leads=max_leads,
        enrichment_enabled=enrichment_enabled,
        quota_guarantee=quota_guarantee,
    )
    return pipe, out_dir


def lead(idx, domain, role, name_prefix="Person", paid=True):
    return {
        "name": f"{name_prefix} {idx}",
        "company": f"Company {domain}",
        "domain": domain,
        "role": role,
        "phone": "+61 400 000 000",
        "email": f"person{idx}@{domain}",
        "_domain_source": "paid" if paid else "organic",
        "_paid_traffic": 10 if paid else 0,
        "_organic_keywords": 0,
    }


class QuotaGuaranteeTests(unittest.TestCase):
    def tearDown(self):
        out_dirs = list(getattr(self, "_out_dirs", []))
        out_dir = getattr(self, "_out_dir", None)
        if out_dir:
            out_dirs.append(out_dir)
        for out_dir in out_dirs:
            if out_dir and os.path.isdir(out_dir):
                shutil.rmtree(out_dir, ignore_errors=True)

    def test_quota_guarantee_fills_from_reserves_to_target(self):
        pipe, self._out_dir = make_pipeline(max_leads=10, quota_guarantee=True)
        strict = [lead(i, f"strict{i}.com.au", "Owner") for i in range(4)]
        reserves = [
            lead(100, "strict0.com.au", "Director"),
            lead(101, "strict0.com.au", "Founder"),
            lead(102, "soft1.com.au", "Operations Lead"),
            lead(103, "soft2.com.au", "Senior Technician"),
            lead(104, "soft3.com.au", "Consultant"),
            lead(105, "soft4.com.au", "Team Member"),
            lead(106, "soft5.com.au", "Practitioner"),
            lead(107, "soft6.com.au", ""),
        ]
        pipe.leads = strict + reserves

        pipe._apply_dm_filter_and_cap()

        self.assertEqual(10, len(pipe.leads))
        self.assertTrue(any(ld.get("_quota_fill_tier") for ld in pipe.leads))

    def test_run_shape_378_raw_58_strict_exports_148(self):
        pipe, self._out_dir = make_pipeline(max_leads=148, quota_guarantee=True)
        strict = [lead(i, f"strict{i}.com.au", "Owner") for i in range(58)]
        reserves = [
            lead(i, f"reserve{i % 90}.com.au", "Technician")
            for i in range(58, 378)
        ]
        pipe.leads = strict + reserves

        pipe._apply_dm_filter_and_cap()

        self.assertEqual(148, len(pipe.leads))
        self.assertEqual(
            90,
            sum(1 for ld in pipe.leads if ld.get("_quota_fill_tier")),
        )

    def test_non_guarantee_keeps_strict_dm_behavior(self):
        pipe, self._out_dir = make_pipeline(max_leads=10, quota_guarantee=False)
        pipe.leads = [
            lead(1, "company.com.au", "Owner"),
            lead(2, "company.com.au", "Senior Technician"),
            lead(3, "company.com.au", "Team Member"),
            lead(4, "other.com.au", "Practitioner"),
        ]

        pipe._apply_dm_filter_and_cap()

        self.assertEqual(1, len(pipe.leads))
        self.assertEqual("Owner", pipe.leads[0]["role"])

    def test_phase6_guarantee_keeps_domain_overflow_until_target(self):
        pipe, self._out_dir = make_pipeline(max_leads=148, quota_guarantee=True)
        pipe.leads = [
            lead((domain_idx * 3) + person_idx, f"domain{domain_idx}.com.au", "Owner")
            for domain_idx in range(54)
            for person_idx in range(3)
        ]

        top_path = pipe._phase6_export()

        all_paths = [
            os.path.join(self._out_dir, name)
            for name in os.listdir(self._out_dir)
            if name.startswith("leads_ALL_") and name.endswith(".csv")
        ]
        self.assertTrue(all_paths)
        with open(all_paths[0], newline="", encoding="utf-8") as fh:
            all_rows = list(csv.DictReader(fh))
        with open(top_path, newline="", encoding="utf-8") as fh:
            top_rows = list(csv.DictReader(fh))
        self.assertEqual(148, len(all_rows))
        self.assertEqual(148, len(top_rows))
        self.assertIn("Quota Fill Tier", all_rows[0])
        self.assertTrue(any(row["Quota Fill Tier"] for row in all_rows))

    def test_phase6_duplicates_best_rows_when_distinct_pool_is_short(self):
        pipe, self._out_dir = make_pipeline(max_leads=10, quota_guarantee=True)
        pipe.leads = [lead(i, f"short{i}.com.au", "Owner") for i in range(4)]

        top_path = pipe._phase6_export()

        all_paths = [
            os.path.join(self._out_dir, name)
            for name in os.listdir(self._out_dir)
            if name.startswith("leads_ALL_") and name.endswith(".csv")
        ]
        with open(all_paths[0], newline="", encoding="utf-8") as fh:
            all_rows = list(csv.DictReader(fh))
        with open(top_path, newline="", encoding="utf-8") as fh:
            top_rows = list(csv.DictReader(fh))
        self.assertEqual(10, len(all_rows))
        self.assertEqual(10, len(top_rows))
        self.assertEqual(
            6,
            sum(1 for row in all_rows if row["Quota Fill Tier"] == "quota_duplicate"),
        )

    def test_city_final_export_pads_64_to_145(self):
        out_dir = tempfile.mkdtemp(prefix="leadforge_city_quota_")
        self._out_dirs = [out_dir]
        city = CityLeadPipeline(
            state_code="AUSTRALIA",
            tier="all",
            city="all",
            min_volume=5,
            max_leads=145,
            output_folder=out_dir,
            enrichment_enabled=False,
            quota_guarantee=True,
            country="AU",
        )
        city.leads = [lead(i, f"city{i}.com.au", "Owner") for i in range(64)]

        top_path = city._export_combined(V5)

        all_paths = [
            os.path.join(out_dir, name)
            for name in os.listdir(out_dir)
            if name.startswith("leads_ALL_") and name.endswith(".csv")
        ]
        with open(all_paths[0], newline="", encoding="utf-8") as fh:
            all_rows = list(csv.DictReader(fh))
        with open(top_path, newline="", encoding="utf-8") as fh:
            top_rows = list(csv.DictReader(fh))
        self.assertEqual(145, len(all_rows))
        self.assertEqual(145, len(top_rows))
        self.assertEqual(
            81,
            sum(1 for row in all_rows if row["Quota Fill Tier"] == "quota_duplicate"),
        )

    def test_enrichment_off_exports_free_revenue_and_linkedin_fields(self):
        pipe, self._out_dir = make_pipeline(
            max_leads=1,
            quota_guarantee=True,
            enrichment_enabled=False,
        )
        pipe.leads = [{
            "name": "Alex Smith",
            "company": "Example Co",
            "domain": "example.com.au",
            "role": "Owner",
            "_domain_source": "paid",
            "_revenue": "$2.5M",
            "_linkedin_url": "",
            "_company_linkedin_url": "https://www.linkedin.com/company/example-co",
        }]

        top_path = pipe._phase6_export()

        all_paths = [
            os.path.join(self._out_dir, name)
            for name in os.listdir(self._out_dir)
            if name.startswith("leads_ALL_") and name.endswith(".csv")
        ]
        with open(all_paths[0], newline="", encoding="utf-8") as fh:
            all_rows = list(csv.DictReader(fh))
        with open(top_path, newline="", encoding="utf-8") as fh:
            top_rows = list(csv.DictReader(fh))
        for row in (all_rows[0], top_rows[0]):
            self.assertEqual("$2.5M", row["Revenue"])
            self.assertEqual("$2.5M", row["revenue"])
            self.assertEqual(
                "https://www.linkedin.com/company/example-co",
                row["LinkedIn URL"],
            )
            self.assertEqual(
                "https://www.linkedin.com/company/example-co",
                row["Company LinkedIn URL"],
            )

    def test_topup_active_prevents_credit_quota_skip(self):
        guarantee_pipe, out1 = make_pipeline(
            max_leads=5,
            quota_guarantee=True,
            enrichment_enabled=True,
        )
        legacy_pipe, out2 = make_pipeline(
            max_leads=5,
            quota_guarantee=False,
            enrichment_enabled=True,
        )
        self._out_dirs = [out1, out2]
        guarantee_pipe._phone_leads_count = 999
        legacy_pipe._phone_leads_count = 999

        self.assertFalse(guarantee_pipe._has_enough_leads())
        self.assertTrue(legacy_pipe._has_enough_leads())
        legacy_pipe._topup_active = True
        self.assertFalse(legacy_pipe._has_enough_leads())


class PaidOnlyGuaranteeTests(unittest.TestCase):
    """2026-06-12: every CONFIRMED advertiser domain must reach the output,
    even when its only contact was a phone-less stub dropped by the dedup /
    DM-filter / per-domain-cap / master-dedup passes."""

    def tearDown(self):
        out_dir = getattr(self, "_out_dir", None)
        if out_dir and os.path.isdir(out_dir):
            shutil.rmtree(out_dir, ignore_errors=True)

    def _paid_pipe(self, confirmed_domains, max_leads=2):
        out_dir = tempfile.mkdtemp(prefix="leadforge_paidonly_")
        pipe = LeadGenerationPipeline(
            industry="Paid Only Test",
            country="AU",
            min_volume=0,
            min_cpc=0.0,
            output_folder=out_dir,
            max_leads=max_leads,
            enrichment_enabled=False,
            quota_guarantee=True,
            paid_only_all=True,
            serp_ads_domains=set(confirmed_domains),
            confirmed_paid_domains=set(confirmed_domains),
        )
        return pipe, out_dir

    def test_backfill_recovers_dropped_confirmed_domains(self):
        # 6 confirmed advertiser domains; only 2 survive into Phase 6 (the rest
        # lost their only stub to upstream dedup). The snapshot + backfill must
        # restore all 6 to the exported CSV — mirroring the user's run where 38
        # confirmed domains collapsed to 8 output rows.
        confirmed = [f"advertiser{i}.com.au" for i in range(6)]
        pipe, self._out_dir = self._paid_pipe(confirmed)

        # Simulate the Phase-5 snapshot of every confirmed domain's best row
        # (here: phone-less stubs, as Apollo-0 domains produce).
        pipe._confirmed_domain_rows = {
            d: {
                "name": "",
                "company": f"Company {d}",
                "domain": d,
                "role": "",
                "phone": "",
                "email": "",
                "_paid_traffic": 1,
                "_domain_source": "paid",
            }
            for d in confirmed
        }
        # Only 2 confirmed domains made it through the upstream filters.
        pipe.leads = [
            lead(1, "advertiser0.com.au", "Owner"),
            lead(2, "advertiser1.com.au", "Owner"),
        ]

        top_path = pipe._phase6_export()

        with open(top_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        exported_domains = {r["Domain"].lower() for r in rows}
        # All 6 confirmed advertisers present despite max_leads=2.
        for d in confirmed:
            self.assertIn(d, exported_domains, f"{d} missing from output")
        self.assertGreaterEqual(len(rows), 6)

    def test_backfill_noop_when_all_present(self):
        confirmed = ["a.com.au", "b.com.au"]
        pipe, self._out_dir = self._paid_pipe(confirmed)
        pipe._confirmed_domain_rows = {
            d: {"name": "", "company": d, "domain": d, "phone": "",
                "email": "", "_paid_traffic": 1, "_domain_source": "paid"}
            for d in confirmed
        }
        pipe.leads = [lead(1, "a.com.au", "Owner"), lead(2, "b.com.au", "Owner")]
        top_path = pipe._phase6_export()
        with open(top_path, newline="", encoding="utf-8") as fh:
            rows = list(csv.DictReader(fh))
        # No duplicate domains added.
        domains = [r["Domain"].lower() for r in rows]
        self.assertEqual(sorted(set(domains)), sorted(confirmed))


class LogScanSafetyNetTests(unittest.TestCase):
    """2026-06-14: no legitimate (confirmed-advertiser) domain may exist only in
    the logs. The end-of-run safety net must re-inject any confirmed advertiser
    dropped from the lead set — using NO API call for already-confirmed domains."""

    def tearDown(self):
        out_dir = getattr(self, "_out_dir", None)
        if out_dir and os.path.isdir(out_dir):
            shutil.rmtree(out_dir, ignore_errors=True)

    def _city(self):
        self._out_dir = tempfile.mkdtemp(prefix="leadforge_logscan_")
        return CityLeadPipeline(
            state_code="AUSTRALIA", tier="all", city="all",
            min_volume=0, max_leads=2, output_folder=self._out_dir,
            enrichment_enabled=False, quota_guarantee=True,
            paid_only_all=True, disable_semrush=True,
        )

    def test_reinjects_confirmed_domain_left_only_in_logs(self):
        city = self._city()
        # Two confirmed advertisers; only ONE made it into the lead set. The
        # other appears only in the logs → must be re-injected free (no ATC).
        city._serp_ads_domains = {"hiltonplumbing.com.au", "mrflowplumbing.com.au"}
        city._confirmed_paid = set(city._serp_ads_domains)
        city.leads = [{"name": "Joe", "domain": "hiltonplumbing.com.au",
                       "phone": "+61400000000"}]
        city._log_lines = [
            "[1/2] hiltonplumbing.com.au",
            "[2/2] mrflowplumbing.com.au: Apollo had 0 people — stub lead",
        ]
        import V5 as V5mod
        recovered = city._recover_left_out_domains(V5mod)
        domains = {(l.get("domain") or "").lower() for l in city.leads}
        self.assertEqual(1, recovered)
        self.assertIn("mrflowplumbing.com.au", domains)
        # The re-injected row is flagged + carries paid provenance.
        reinjected = [l for l in city.leads if l.get("_safety_net_recovered")]
        self.assertEqual(1, len(reinjected))
        self.assertEqual(1, reinjected[0].get("_paid_traffic"))

    def test_noop_when_all_confirmed_present(self):
        city = self._city()
        city._serp_ads_domains = {"a.com.au", "b.com.au"}
        city._confirmed_paid = set(city._serp_ads_domains)
        city.leads = [{"domain": "a.com.au"}, {"domain": "b.com.au"}]
        city._log_lines = ["scanned a.com.au", "scanned b.com.au"]
        import V5 as V5mod
        recovered = city._recover_left_out_domains(V5mod)
        self.assertEqual(0, recovered)
        self.assertEqual(2, len(city.leads))


if __name__ == "__main__":
    unittest.main()
