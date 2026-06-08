# LeadForge V5 Phone Number Fix — Complete Session Summary

**Date:** 2026-04-09  
**File:** `C:\codex projects\LEAD_FORGE_LEAD_GENERATOR_cache\LEAD_FORGE_LEAD_GENERATOR_v2\V5.py`  
**Versions Applied:** V5.22, V5.23, V5.24  
**Status:** 3 critical fixes implemented — testing required

---

## The Problem (Root Cause Analysis)

**Symptom:**  All leads from the same company showed the **same phone number**, not their personal mobiles.

Example (Fallon Solutions):
- Mark Denning: Should be +61 7 3029 6366 → Got +61485857016 (shared company contact)
- Jackson Whipps: Should be +61 432 009 768 → Got +61485857016 (shared company contact)
- Justin Deyzel: Should be +27 82 448 0186 → Got +61485857016 (shared company contact)

Example (Plumbed Right):
- Samuel Ricciardo: Should be +61 415 489 103 → Got +61499281430 (Michael Treble's number, also company contact)

**Root Cause (3 Layers Deep):**

1. **Apollo's `api_search` returns a shared company contact number for unrevealed people** — not truly empty, but an internal secondary contact number Apollo assigns to all people at an org when personal phone credits are exhausted.

2. **`_pick_best_phone_from_apollo` fell back to the singular `phone_number` field** (line 1632, 1685) which is always company-level. This singular field entered the pipeline with quality score 20 (treated as a fallback contact).

3. **The company_phone fallback (line 4394) ran AFTER dedup** — When the dedup guard stripped the shared phone, the company_phone fallback re-added the exact same number from Apollo's org enrichment (which returns the same secondary contact). The cycle repeated.

---

## The Fixes Applied (V5.24)

### Fix 1: Kill the Singular `phone_number` Fallback
**Lines:** 1632, 1685  
**What:** Removed the fallback to `person.get("phone_number")` — always return `("", 0)` instead.  
**Why:** The singular field is always company-level and pollutes the pipeline with low-quality (20) numbers.

**Before:**
```python
if not phone_numbers:
    phone = safe_str(person.get("phone_number") or "")
    return phone, (20 if phone else 0)
```
**After:**
```python
if not phone_numbers:
    return "", 0  # V5.24: singular phone_number is always company-level
```

---

### Fix 2: Mark and Skip Re-Adding Stripped Phones
**Lines:** 3918, 4394-4401  
**What:** 
1. When dedup strips a shared phone, save it in `_dedup_stripped_phone` (line 3918)
2. When company_phone fallback runs, check if the company_phone matches what we stripped (lines 4397-4399)
3. If they match (same digits), skip that lead instead of re-adding the number

**Why:** Breaks the cycle where the company_phone fallback was re-adding the exact shared number we'd just deduped.

**Code:**
```python
# Line 3918 (in dedup guard):
ld["_dedup_stripped_phone"] = p  # Remember what was stripped

# Lines 4394-4401 (in company_phone fallback):
if company_phone:
    _co_digits = re.sub(r'\D', '', company_phone)
    for ld in domain_leads:
        if not ld.get("phone"):
            _stripped = ld.get("_dedup_stripped_phone", "")
            if _stripped and re.sub(r'\D', '', _stripped) == _co_digits:
                continue  # V5.24: skip — this is the shared phone we stripped
            ld["phone"] = company_phone
```

---

### Fix 3: Expand Phase 4b Person Phone Search to Landlines
**Line:** 4720  
**What:** Changed condition from `== 0` to `< 30` to catch landlines with quality 5-15 (work_hq, work types).  
**Why:** Jarod Lancaster's phone was quality 15 (landline), so Phase 4b SerpAPI person search never triggered. Expanding to `< 30` means any non-personal phone (landline, work phone) gets a chance to be upgraded.

**Before:**
```python
if (name and " " in name and self.serpapi._available and domain
        and ld.get("_phone_quality", 0) == 0):
```
**After:**
```python
if (name and " " in name and self.serpapi._available and domain
        and ld.get("_phone_quality", 0) < 30):  # V5.24: was == 0
```

---

## Complete Phone Field Assignment Order (Full Pipeline)

For reference, here's the complete order of phone assignments in `_enrich_domain()`:

1. **Line 3567:** Apollo initial search → `_pick_best_phone_from_apollo()` → quality tracked
2. **Lines 3738, 3806:** Apollo enrich passes (Steps 2b, 2c) → quality-based replacement (only if higher quality)
3. **Lines 3918-3920:** **V5.24 Dedup Guard** ← **STRIPS shared phones, saves in `_dedup_stripped_phone`**
4. **Line 3983:** Lusha person enrichment (Step 4) → fills if empty or quality wins
5. **Line 4364:** Scraped phone from contact pages (Step 5) → fills if empty
6. **Line 4377:** SerpAPI business info (Step 6) → fills if empty
7. **Lines 4394-4401:** **V5.24 Company phone fallback** ← **NOW SKIPS leads with stripped phones**
8. **Phase 4b (Lines 4643+):**
   - **4683:** Apollo re-enrich via LinkedIn (4b-A)
   - **4698:** SerpAPI find_business_phone (4b-B)
   - **4709:** Contact page scraping (4b-C)
   - **4726:** **SerpAPI find_person_phone (4b-D)** ← **NOW TARGETS quality < 30 landlines too**

---

## Key Code Changes Summary

| Location | Old Behavior | New Behavior | Impact |
|----------|--------------|--------------|--------|
| Line 1632 | Return singular `phone_number` (quality 20) | Return `("", 0)` | Prevents company-level fallback from entering pipeline |
| Line 1685 | Return singular `phone_number` (quality 20) | Return `("", 0)` | Same as above (second occurrence) |
| Line 3918 | Just stripped phone | Save in `_dedup_stripped_phone` | Allows later code to identify what was deduped |
| Lines 4397-4399 | Allow company_phone to fill any empty phone | Skip if company_phone == stripped shared phone | Prevents re-adding the shared number |
| Line 4720 | Only target quality == 0 | Target quality < 30 | Expands SerpAPI person search to also fix landlines |

---

## What Stays Unchanged (Workflow Preservation)

✅ **Email selection logic (V5.22)** — all email priority rules intact  
✅ **Lead count** — still returns exactly 10 leads when max_leads=10  
✅ **All columns** — name, company, domain, role, phone, email, email_type always populated  
✅ **Sorting algorithm** — leads ranked by relevance score  
✅ **Apollo reveal_phone_number + is_default +20** (V5.23) — kept for future credits  
✅ **All Steps 1-6 logic** — untouched except for the 3 targeted phone fixes  

---

## Testing Checklist (Next Session)

Run: `python V5.py` with `max_leads=10`

**Expected results:**
- [ ] Fallon Solutions leads show **different personal phones**, not +61485857016
  - Mark Denning: +61 7 3029 6366 or +61 407 035 983 (mobile)
  - Jackson Whipps: +61 432 009 768
  - Justin Deyzel: +27 82 448 0186
- [ ] Plumbed Right leads show different numbers
  - Michael Treble: +61 499 281 430 ✓ (unchanged — correct)
  - Samuel Ricciardo: +61 415 489 103 (not +61499281430)
- [ ] L&W Plumbing
  - Jarod Lancaster: +61 427 038 497 (not +61731860583)
- [ ] All 10 leads present
- [ ] All columns filled (no blanks)
- [ ] No console errors

---

## Files Modified

- `C:\codex projects\LEAD_FORGE_LEAD_GENERATOR_cache\LEAD_FORGE_LEAD_GENERATOR_v2\V5.py`
  - Lines 1632, 1685: Fix 1a/1b (singular phone_number fallback)
  - Lines 3908-3921: V5.24 Dedup guard with tracking
  - Lines 4389-4401: Fix 2 (company_phone skip logic)
  - Line 4720: Fix 3 (expand SerpAPI person search to landlines)

---

## Important Context for Next Session

**Critical Understanding:**
- Apollo's `api_search` does NOT return personal phones without credits — it returns a secondary company contact number instead
- The `person.get("phone_number")` (singular) field is ALWAYS company-level — never use it
- The `company_phone` (from org enrichment) can be identical to Apollo's shared people contact number — must detect and skip
- Lusha has a 50% hit rate — if it misses, Phase 4b fallbacks are essential
- SerpAPI person search only works when SerpAPI credits are available

**Quality Score Thresholds:**
- quality ≥ 30 = personal phone (mobile, direct, personal, home)
- quality 15-25 = work/landline (not personal)
- quality 5 = HQ/company switchboard
- quality 0 = no phone

**The Pipeline Flow:**
1. Apollo search finds basic data + some personal phones
2. Apollo enrich tries to fill gaps (but often returns company phone)
3. **Dedup + Lusha** fill personal numbers for most leads
4. **Company fallback** provides a last resort (now with skip logic)
5. **Phase 4b** tries harder for remaining leads

---

## Git Status
All changes saved to V5.py in-place. No new files created.

**Syntax:** ✅ Verified clean  
**Changes:** ✅ Applied and saved  
**Testing:** ⏳ Pending

---

## When Running Tests Next Time

1. Verify syntax: `python -c "import ast; ast.parse(open('V5.py').read())"`
2. Run with debug logging to see phone flow (optional):
   - Add `print(f"[DEBUG] {name}: quality={ld.get('_phone_quality')}, phone={ld.get('phone')}")` after Lusha step
   - Add same print after company_phone fallback
3. Compare output phones against Apollo UI screenshots
4. If still broken, check:
   - Are Lusha API calls returning data?
   - Is SerpAPI available (has credits)?
   - Are the dedup guard and company_phone skip logic being triggered? (add `_dedup_stripped_phone` to output columns temporarily to verify)

