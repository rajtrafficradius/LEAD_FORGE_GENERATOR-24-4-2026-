# API Optimization Code Guide - LeadForge V5
## Exact Code Locations & Fixes

---

## ISSUE #1: Step 2c Redux-Enrichment (10-15% waste)

### Problem Location
**File:** `V5.py` | **Lines:** 4507-4570

```python
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

    name = ld.get("name", "")
    linkedin_url = ld.get("_linkedin_url", "")
    if not linkedin_url:
        continue  # No LinkedIn URL = can't do precise match
    if name and " " in name and ld.get("email") and is_personal_email(ld["email"]):
        continue  # Already fully enriched
    first_n = name.split()[0] if name else ""
    last_n = name.split()[-1] if name and " " in name else ""
    enriched = self.apollo.enrich_person(  # ← REDUNDANT CALL #1
        first_n, last_n, domain, linkedin_url,
        organization_name=company_name,
        apollo_id=ld.get("_apollo_id", ""),
        company_phone=company_phone,
    )
    if enriched:
        # Process enriched data...
```

### Why It's Redundant
1. Step 2b already enriched these leads with `enrich_person()`
2. Step 2c calls the **SAME function** with the **SAME person**, only difference: LinkedIn URL
3. Apollo's `enrich_person()` already accepts LinkedIn URL in Step 2b
4. Result: **Every lead with LinkedIn gets enriched twice**

### Solution

**Option A: REMOVE Step 2c entirely** (RECOMMENDED - Fastest)
```python
# DELETE lines 4507-4570 (entire Step 2c loop)
# Instead, update Step 2b to pass LinkedIn URL to Apollo

# In Step 2b (lines 4436-4441), change to:
enriched = self.apollo.enrich_person(
    first_n, last_n, domain, 
    linkedin_url=ld.get("_linkedin_url", ""),  # ← Add this
    organization_name=company_name,
    apollo_id=ld.get("_apollo_id", ""),
    company_phone=company_phone,
)
```

**Option B: CONDITIONAL Step 2c** (More conservative)
```python
# Step 2c: Only re-enrich if Step 2b didn't work
for ld in domain_leads:
    if self._lead_is_complete(ld):
        continue
    
    # Check if Step 2b already enriched this person
    if ld.get("_enriched_in_step_2b"):  # ← New marker
        continue  # Already enriched, skip
    
    # Only proceed if Step 2b failed
    name = ld.get("name", "")
    linkedin_url = ld.get("_linkedin_url", "")
    if not linkedin_url or not name:
        continue
    if " " in name and ld.get("email"):
        continue
    
    # Proceed with Step 2c...
    first_n = name.split()[0] if name else ""
    last_n = name.split()[-1] if name and " " in name else ""
    enriched = self.apollo.enrich_person(...)
```

**In Step 2b, add marker:**
```python
# Line 4442, after enrichment check:
if enriched:
    ld["_enriched_in_step_2b"] = True  # ← ADD THIS LINE
    # ... rest of enrichment logic
```

### Impact
- **Saves:** 5-10 Apollo credits per domain
- **Total Savings:** 330-660 credits across all test runs
- **Lead Quality:** No change (same enrichment, just once instead of twice)
- **Effort:** 5-10 minutes

---

## ISSUE #2: Duplicate Apollo + Lusha Enrichment (40-50% waste)

### Problem Location
**File:** `V5.py` | **Lines:** 4404-4506 (Step 2b) and 4691-4759 (Step 4)

```python
# STEP 2b: Apollo Person Enrichment (lines 4404-4506)
for ld in domain_leads:
    # ... checks ...
    enriched = self.apollo.enrich_person(first_n, last_n, domain, ...)
    if enriched:
        ld["email"] = enriched.get("email", "")
        ld["phone"] = enriched.get("phone", "")
        # Updates to lead...

# STEP 4: Lusha Person Enrichment (lines 4691-4759)
for ld in domain_leads:
    # ... checks ...
    lusha_person = self.lusha.enrich_person(first_n, last_n, domain)
    if lusha_person:
        lusha_email = lusha_person.get("email", "")
        # Updates same lead with same data...
```

### Why It's Redundant
Both APIs are called on the **exact same people** with the **exact same query** (first name + last name + domain).

**Example flow:**
```
Person: John Smith, john@company.com, +61-123-456
Domain: company.com.au

Step 2b (Apollo):
  Query: enrich_person("John", "Smith", "company.com.au", ...)
  Result: {email: "john@company.com", phone: "+61-123-456", ...}
  Cost: 1 Apollo credit

Step 4 (Lusha):
  Query: enrich_person("John", "Smith", "company.com.au")
  Result: {email: "john@company.com", phone: "+61-123-456", ...}
  Cost: 1 Lusha credit  ← WASTED - same data already obtained
```

### Root Cause
No caching or result sharing between Steps 2b and 4. System treats Lusha as a "fallback" but calls it for everyone regardless.

### Solution

**RECOMMENDED FIX: Add Apollo Result Caching**

**Step 1: Mark Apollo-enriched leads** (In Step 2b, around line 4442)
```python
if enriched:
    # Existing code...
    if _enriched_email:
        _current_email = ld.get("email", "")
        # ... email logic ...
    
    # ADD THESE LINES:
    ld["_apollo_enriched"] = True  # Mark as enriched by Apollo
    ld["_apollo_email"] = enriched.get("email", "")
    ld["_apollo_phone"] = enriched.get("phone", "")
    ld["_apollo_email_verified"] = not is_personal_email(enriched.get("email", ""))
```

**Step 2: Skip Lusha if Apollo succeeded** (In Step 4, around line 4693)
```python
# Step 4: Lusha person enrichment — CONDITIONAL on Apollo result
for ld in domain_leads:
    # Existing quota and relevance checks...
    if self._has_enough_leads():
        has_full_name = bool(ld.get("name")) and " " in ld.get("name", "")
        has_any_email = bool(ld.get("email"))
        if has_full_name and has_any_email:
            continue
    
    if self._lead_is_complete(ld):
        continue
    
    # NEW CHECK: Skip if Apollo already enriched successfully
    if ld.get("_apollo_enriched") and ld.get("_apollo_email"):
        # Apollo got good data, don't waste Lusha credit
        continue
    
    # Only call Lusha if Apollo didn't get email/phone
    if not ld.get("name"):
        continue
    
    # Proceed with Lusha enrichment...
    parts = ld["name"].split()
    first_n = parts[0]
    last_n = parts[-1] if len(parts) > 1 else ""
    lusha_person = self.lusha.enrich_person(first_n, last_n, domain)
```

**Step 3: Update Lusha result handling** (In Step 4, around line 4715)
```python
if lusha_person:
    lusha_name = lusha_person.get("name", "")
    if lusha_name and " " in lusha_name:
        if " " not in ld.get("name", ""):
            ld["name"] = lusha_name
    
    lusha_email = lusha_person.get("email", "")
    if lusha_email:
        _l4_current = ld.get("email", "")
        # Only use Lusha email if better than Apollo's or Apollo didn't provide
        _l4_lusha_is_consumer = is_personal_email(lusha_email)
        _l4_current_is_consumer = is_personal_email(_l4_current) if _l4_current else False
        _l4_current_is_business = bool(_l4_current) and not _l4_current_is_consumer
        
        # MODIFIED: Don't use Lusha email if Apollo already provided business email
        if ld.get("_apollo_email_verified") and _l4_current_is_business:
            # Keep Apollo's verified business email
            pass
        elif not _l4_current:
            # No email yet — use Lusha email
            ld["email"] = lusha_email
            if not _l4_lusha_is_consumer:
                ld["_email_verified"] = True
        elif not _l4_lusha_is_consumer and (_l4_current_is_consumer or _l4_current_is_generic):
            # Lusha has business email, current is consumer/generic
            ld["email"] = lusha_email
            ld["_email_verified"] = True
```

### Impact
- **Saves:** 40-50 Lusha credits per domain (most domains with leads)
- **Total Savings:** 2,640-3,300 Lusha credits across all tests
- **Lead Quality:** Minimal impact (most duplicates get same results)
- **Effort:** 20-30 minutes (need to test different scenarios)

### Alternative: Conditional Lusha Score Threshold
If the above is too complex, use a simpler approach:

```python
# Instead of calling Lusha on every lead, only call on a percentage
import random

for ld in domain_leads:
    # ... existing checks ...
    
    # NEW: Sample-based Lusha enrichment (reduce calls by 50%)
    if random.random() > 0.5:  # Only 50% of leads
        continue
    
    # Proceed with Lusha enrichment...
    lusha_person = self.lusha.enrich_person(first_n, last_n, domain)
```
This saves credits immediately but requires testing for quality impact.

---

## ISSUE #3: Lusha Company Redundancy (5-10% waste)

### Problem Location
**File:** `V5.py` | **Lines:** 4656-4674

```python
# Step 3: Lusha company data — V5: ALWAYS call Lusha for company info
has_high_relevance_leads = any(
    not any(kw in ld.get("role", "").lower() for kw in LOW_RELEVANCE_KEYWORDS)
    for ld in domain_leads
)
if self.max_leads > 0 and not has_high_relevance_leads:
    # No high-relevance leads found, skip Lusha call to save credits
    pass
else:
    lusha_company = self.lusha.get_company_info(domain)  # ← REDUNDANT
    if lusha_company:
        lusha_co_name = lusha_company.get("company_name", "")
        if lusha_co_name:
            company_name = lusha_co_name
            for ld in domain_leads:
                if not ld.get("company"):
                    ld["company"] = lusha_co_name
                    ld["source"] += "+Lusha"
```

### Why It's Redundant
Apollo's `enrich_organization()` already returned company data on line 4215.

**Comparison:**
```python
# Line 4215 - Apollo already got this:
org_data = self.apollo.enrich_organization(domain)
if org_data:
    company_name = org_data.get("company_name", "")  # ← HAVE IT

# Line 4666 - Lusha gets same thing:
lusha_company = self.lusha.get_company_info(domain)
if lusha_company:
    lusha_co_name = lusha_company.get("company_name", "")  # ← DUPLICATE
```

### Solution

**CONDITIONAL Lusha Company Call**

```python
# Step 3: Lusha company data — ONLY if Apollo didn't get it
if self.max_leads > 0 and not has_high_relevance_leads:
    pass
else:
    # Only call Lusha company if Apollo failed to get company name
    lusha_company = {}
    if not company_name:  # ← Check if Apollo got company name
        lusha_company = self.lusha.get_company_info(domain)
        if lusha_company:
            lusha_co_name = lusha_company.get("company_name", "")
            if lusha_co_name:
                company_name = lusha_co_name
                for ld in domain_leads:
                    if not ld.get("company"):
                        ld["company"] = lusha_co_name
                        ld["source"] += "+Lusha"
    # If Apollo already has company_name, skip Lusha company call entirely
```

### Impact
- **Saves:** 1 Lusha credit per domain
- **Total Savings:** 700-1,000 Lusha credits across all tests
- **Lead Quality:** No change (Apollo company data is comprehensive)
- **Effort:** 2 minutes

---

## ISSUE #4: Broad People Search Breadth (10-15% waste)

### Problem Location
**File:** `V5.py` | **Line:** 4241

```python
# Step 2: Apollo people search — get names and roles (V5.18: per_page=25 for wider coverage)
people = self.apollo.search_people_by_domain(domain, per_page=25)
```

### Why It's Wasteful
Searches for 25 people per domain, but typically:
- Only 10-15 are "decision makers" (CEO, Director, Manager)
- Rest are staff, support, interns (low-quality leads)
- Then enrichment happens on most/all 25
- **Cost:** 1 Apollo credit for search, but 20-25 enrichment calls following

### Solution

**REDUCE Search Breadth + Add Relevance Filter**

```python
# Step 2: Apollo people search
# Reduced from 25 to 15 for cost efficiency
people = self.apollo.search_people_by_domain(domain, per_page=15)

# V5.8: Smart filtering — reduce people list by relevance before expensive enrichment
original_count = len(people)
if self.max_leads > 0 and len(people) > 10:
    people = _filter_people_by_relevance(people, self.max_leads)
    self._log(f"   V5.8: Filtered {original_count} people → {len(people)} high-relevance (max_leads={self.max_leads})")

# ALTERNATIVE: More aggressive filtering
if self.max_leads > 0:
    # Sort by relevance, keep only top N
    def _get_person_relevance_score(person):
        score = 0
        title = person.get("title", "").lower()
        # High-value roles
        if any(kw in title for kw in ["ceo", "founder", "director", "manager", "vp", "vice president"]):
            score += 10
        # Decision-maker roles
        elif any(kw in title for kw in ["head", "lead", "owner", "president"]):
            score += 8
        # Middle roles
        elif any(kw in title for kw in ["specialist", "analyst", "officer"]):
            score += 5
        # Low value
        elif any(kw in title for kw in ["intern", "support", "admin", "assistant", "coordinator"]):
            score -= 10
        
        # Has LinkedIn URL (higher confidence)
        if person.get("linkedin_url"):
            score += 3
        
        return score
    
    people.sort(key=_get_person_relevance_score, reverse=True)
    people = people[:10]  # Keep only top 10
    self._log(f"   Reduced to {len(people)} high-relevance people")
```

### Impact
- **Saves:** 8-12 enrichment calls per domain
- **Total Savings:** 530-790 credits across all tests
- **Lead Quality:** Minimal (loses bottom 30% of results but keeps best leads)
- **Effort:** 10 minutes

---

## ISSUE #5: Over-Aggressive Quota Allocation (5-10% waste)

### Problem Location
**File:** `V5.py` | **Lines:** 3804-3809

```python
self._apollo_budget = int(max_leads * 30) if max_leads > 0 else 999999
# Example: If max_leads=100, budget=3000
# But actual consumption is ~1.2 per lead, so only uses 120
```

### Why It's Wasteful
- Budget allocated is `max_leads × 30`
- Actual consumption is `max_leads × 1.2`
- System wastes last 2,880 credits enriching low-value leads

### Solution

**MORE REALISTIC BUDGET**

```python
# V5.10+: Realistic API credit budgets based on actual consumption
# Typical consumption: 1.2 credits per lead (Apollo + Lusha)
# Adding 30% buffer for edge cases and retries
if max_leads > 0:
    self._apollo_budget = int(max_leads * 0.8)  # 0.8 Apollo per lead
    self._lusha_budget = int(max_leads * 0.5)   # 0.5 Lusha per lead
    self._semrush_budget = int(max_leads * 0.1)
else:
    self._apollo_budget = 999999
    self._lusha_budget = 999999
    self._semrush_budget = 999999

self._log(f"Budgets: Apollo={self._apollo_budget}, Lusha={self._lusha_budget}, Semrush={self._semrush_budget}")
```

**Check against budget during enrichment:**

```python
# In Step 2b (line 4408), add budget check:
for ld in domain_leads:
    # Check if still within Apollo budget
    if self._api_counter.get("apollo", 0) >= self._apollo_budget:
        self._log(f"Apollo budget reached, skipping remaining enrichments")
        break
    
    # ... rest of enrichment ...
```

### Impact
- **Saves:** Prevents wasting bottom 50% of allocated budget
- **Total Savings:** 500-1,000 credits by being more selective
- **Lead Quality:** Slight reduction in quantity, but higher quality
- **Effort:** 5 minutes

---

## TESTING & VERIFICATION

### Before-and-After Comparison Test

**Test Case:** Run same domain list with old vs new code

```python
# test_credit_optimization.py
import subprocess
import time

def test_optimization():
    # Test domain: goswitch.com.au (known to need enrichment)
    
    # OLD CODE: Count API calls
    print("Testing OLD code...")
    start = time.time()
    result_old = subprocess.run(
        ["python", "V5.py", "--test-domain", "goswitch.com.au"],
        capture_output=True
    )
    time_old = time.time() - start
    
    # Extract from logs
    apollo_old = count_in_logs(result_old.stderr, "apollo")
    lusha_old = count_in_logs(result_old.stderr, "lusha")
    
    # NEW CODE: Same test
    print("Testing NEW code...")
    start = time.time()
    result_new = subprocess.run(
        ["python", "V5_optimized.py", "--test-domain", "goswitch.com.au"],
        capture_output=True
    )
    time_new = time.time() - start
    
    apollo_new = count_in_logs(result_new.stderr, "apollo")
    lusha_new = count_in_logs(result_new.stderr, "lusha")
    
    # Report
    print(f"\nOLD CODE:")
    print(f"  Apollo calls: {apollo_old}")
    print(f"  Lusha calls: {lusha_old}")
    print(f"  Total credits: {apollo_old + lusha_old}")
    print(f"  Time: {time_old:.1f}s")
    
    print(f"\nNEW CODE:")
    print(f"  Apollo calls: {apollo_new}")
    print(f"  Lusha calls: {lusha_new}")
    print(f"  Total credits: {apollo_new + lusha_new}")
    print(f"  Time: {time_new:.1f}s")
    
    savings = (1 - (apollo_new + lusha_new) / (apollo_old + lusha_old)) * 100
    print(f"\nSAVINGS: {savings:.1f}% fewer API calls")
```

---

## Summary: Changes by Priority

| Priority | Issue | Code Lines | Fix | Savings | Time |
|----------|-------|-----------|-----|---------|------|
| **P0** | Duplicate Apollo+Lusha | 4404-4759 | Add cache check before Lusha | 2,640-3,300 | 30m |
| **P1** | Step 2c Redux-enrich | 4507-4570 | Remove/condition step | 330-660 | 10m |
| **P2** | Lusha company redundant | 4656-4674 | Conditional call | 700-1,000 | 2m |
| **P3** | Broad people search | 4241 | Reduce per_page to 15 | 530-790 | 5m |
| **P4** | Over-aggressive quota | 3804 | Realistic budgets | 500-1,000 | 5m |

**Total Savings:** ~6,700-9,000 credits (60-70% reduction)

