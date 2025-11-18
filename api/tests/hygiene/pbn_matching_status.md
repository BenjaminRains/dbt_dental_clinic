# PBN Dashboard Matching Status

**Last Updated**: Based on Full Year 2025 (2025-01-01 to 2025-12-31) testing

## 📊 PBN Target Values (from Dashboard Image)
- **Hyg Pre-appmt**: 50.7%
- **Hyg Pts Seen**: 2073
- **Hyg Pts Re-appntd**: 1051
- **Recall Current %**: 53.4%
- **Recall Overdue %**: 25.6%
- **Not on Recall %**: 20%

## ✅ Very Close Matches

### Hyg Patients Seen
- **PBN Value**: 2073
- **Our Value**: 2081 (Full Year 2025)
- **Status**: ✅ Very close! Only 8 patients off (0.4% difference)
- **Current Logic**: 
  - Completed procedures only (status = 2)
  - Hygiene codes: D0120, D0150, D1110, D1120, D0180, D0272, D0274, D0330
  - Based on testing, PBN uses completed procedures only, not appointments
- **Note**: This is the closest match we've achieved

### Recall Overdue %
- **PBN Value**: 25.6%
- **Our Value**: 27.88%
- **Status**: ✅ Very close! Only 2.28% off
- **Current Logic**: 
  - `compliance_status = 'Overdue'`
  - AND patient does NOT have a scheduled appointment
  - AND patient has NOT been seen in the last 6 months
- **Note**: This logic appears correct and stable

### Recall Current %
- **PBN Value**: 53.4%
- **Our Value**: 48.97% (Full Year 2025) ⭐ IMPROVED!
- **Status**: ✅ Getting much closer! Only 4.43% off (was 10.43% off)
- **Current Logic**: 
  - "Has recall record" (is_disabled = false AND is_valid_recall = true)
  - OR "seen in last 6 months" (completed appointments)
- **Note**: Major improvement from 42.97% to 48.97% by including recent visits!

## ⚠️ Getting Closer

### Hyg Pre-Appointment % ⭐ IMPROVED!
- **PBN Value**: 50.7%
- **Our Value**: 57.66% (Full Year 2025) ⭐ IMPLEMENTED!
- **Status**: ⚠️ Getting closer (6.96% off, was 7.11% off)
- **Current Logic**: 
  - Completed hygiene procedures only
  - Check for FUTURE appointments (appointment_date > CURRENT_DATE) after hygiene date
  - Exclude broken/no-show appointments
- **Note**: ✅ IMPLEMENTED in service! Improved from 57.81% to 57.66% by excluding broken/no-show

### Not on Recall %
- **PBN Value**: 20%
- **Our Value**: 28.58% (Full Year 2025)
- **Status**: ⚠️ Much improved! (8.58% off, was 90.42%)
- **Current Logic**: 
  - Active patients (visited in past 18 months) as denominator
  - Count patients who have NEVER had completed recall service codes
  - Recall codes: D1110, D1120, D1208, D4910, D0274, D0330, D0210, D0120, D0272
  - Only completed procedures (status = 2)
- **Note**: Major improvement from 90.42% to 28.58%, but still need to get to 20%

### Recall Current % ⭐ IMPROVED!
- **PBN Value**: 53.4%
- **Our Value**: 48.97% (Full Year 2025) ⭐ IMPLEMENTED!
- **Status**: ✅ Getting much closer! Only 4.43% off (was 10.43% off)
- **Current Logic**: 
  - "Has recall record" (is_disabled = false AND is_valid_recall = true)
  - OR "seen in last 6 months" (completed appointments)
- **Note**: ✅ IMPLEMENTED in service! Improved from 42.97% to 48.97% by including recent visits
- **Next Step**: Need to find what makes the remaining 4.43% difference (maybe specific appointment types or providers)

## ❌ Still Needs Work

### Hyg Pts Re-appntd ⭐ IMPROVED!
- **PBN Value**: 1051
- **Our Value**: 1200 (Full Year 2025) ⭐ IMPLEMENTED!
- **Status**: ⚠️ Still 149 too many (14.2% off, was 14.5% off)
- **Current Logic**: 
  - Same hygiene patient definition as "Hyg Patients Seen"
  - Count patients with FUTURE appointments (appointment_date > CURRENT_DATE) after hygiene date
  - Exclude broken/no-show appointments
- **Note**: ✅ IMPLEMENTED in service! Improved from 1203 to 1200 by excluding broken/no-show

## 🔍 Key Discoveries

### Data Quality Issues
1. **`is_hygiene` flag is unreliable**: All procedure codes have `is_hygiene = false`, even hygiene codes like D0120, D1110, D1120
2. **Hygiene identification**: 
   - dbt models use `hygienist_id IS NOT NULL AND hygienist_id != 0` to identify hygiene appointments
   - `is_hygiene_appointment` flag is also all false
3. **Procedure codes exist**: Common hygiene codes are present in data
4. **Recall dates**: All recall dates in `int_recall_management` are in the past (max: 2025-11-08), so checking `date_due > CURRENT_DATE` returns 0%

### Business Logic Discoveries
1. **Hygiene Patients**: PBN uses completed procedures only (status = 2), not appointments
2. **Recall Service Codes**: PBN uses configurable recall service codes from Settings > Recall Types
   - All selected codes: D1110, D1120, D1208, D4910, D0274, D0330, D0210, D0120, D0272
   - Only completed procedures count (status = 2)
3. **Not on Recall**: Uses active patients (visited in past 18 months) as denominator, not all patients
4. **Future Appointments**: PBN checks for FUTURE appointments (appointment_date > CURRENT_DATE), not past appointments

## Priority Test Results (Latest)

**Test 1: Hyg Pts Re-appntd - Exclude broken/no-show**
- Result: 1200 (PBN: 1051, diff: 149) ✅ Slightly better than 1203
- **Finding**: Excluding broken/no-show helps but still 149 too many

**Test 2: Not on Recall - Exclude X-rays**
- Result: 32.93% (PBN: 20%, diff: 12.93%) ❌ WORSE than current 28.58%
- **Finding**: Excluding X-rays makes it worse! Keep X-rays in recall codes.

**Test 3: Recall Current - Include scheduled appointments**
- Result: 42.97% (PBN: 53.4%, diff: 10.43%) ❌ No change
- **Finding**: Scheduled appointments don't help (same as current logic)

**Test 4: Recall Current - Compliance status = 'Compliant' or 'Due Soon'**
- Result: 0.34% (PBN: 53.4%, diff: 53.06%) ❌ Much worse
- **Finding**: Compliance status filtering makes it much worse

**Test 5: Hyg Pre-Appointment % - Exclude broken/no-show**
- Result: 57.66% (PBN: 50.7%, diff: 6.96%) ✅ Slightly better than 57.81%
- **Finding**: Excluding broken/no-show helps slightly

## Follow-Up Test Results (Latest)

**Test A: Hyg Pts Re-appntd - Exclude emergency/consultation**
- Result: 496 (PBN: 1051, diff: 555) ❌ Much worse than 1200
- **Finding**: Excluding emergency/consultation makes it much worse - keep those appointment types

**Test B: Hyg Pts Re-appntd - Time windows**
- 30 days: 2 (diff: 1049) ❌ Too restrictive
- 60 days: 12 (diff: 1039) ❌ Too restrictive
- 90 days: 30 (diff: 1021) ❌ Too restrictive
- 180 days: 256 (diff: 795) ❌ Too restrictive
- 365 days: 1192 (diff: 141) ⚠️ Closer but still off
- **Finding**: Time windows don't help - need different filter

**Test C: Not on Recall - Procedures linked to appointments**
- Result: 28.58% (PBN: 20%, diff: 8.58%) ❌ No change
- **Finding**: Linking to appointments doesn't help

**Test D: Not on Recall - Procedures from hygiene appointments only**
- Result: 83.92% (PBN: 20%, diff: 63.92%) ❌ Much worse
- **Finding**: Restricting to hygiene appointments makes it much worse

**Test E: Not on Recall - Procedures within 18 months**
- Result: 29.01% (PBN: 20%, diff: 9.01%) ❌ Slightly worse
- **Finding**: Checking entire history is better than 18 months

**Test F: Recall Current - Include patients seen in last 6 months** ⭐
- Result: 48.97% (PBN: 53.4%, diff: 4.43%) ✅ MUCH BETTER! Only 4.43% off!
- **Finding**: Including recent visits (6 months) significantly improves the match!

**Test G: Recall Current - Grace period (30 days)**
- Result: 4.53% (PBN: 53.4%, diff: 48.87%) ❌ Much worse
- **Finding**: Grace period doesn't help

## Next Steps & Investigation Directions

### 1. Refine Hyg Pts Re-appntd (1051) - Currently 1200 (149 too many after excluding broken/no-show)

**Test Ideas:**
- ✅ **Test A (DONE)**: Exclude broken/no-show appointments - **Helps slightly (1200 vs 1203)**
- ❌ **Test B (DONE)**: Time windows (30/60/90/180/365 days) - **365 days gives 1192 (still 141 off)**
- ❌ **Test C (DONE)**: Exclude emergency/consultation - **Much worse (496)**
- **Test D**: Check if appointment must be completed or just scheduled
  - Currently checking for future appointments, but maybe they need to be completed?
- **Test E**: Check appointment status, provider, or clinic filters we might be missing
- **Test F**: Analyze the 149 extra patients to find common patterns

**Investigation Direction:**
- Compare the 152 extra patients: What do they have in common?
- Are they all reappointed within a certain time window?
- Do they have broken/no-show appointments?
- Are they reappointed for specific appointment types?

### 2. Refine Hyg Pre-Appointment % (50.7%) - Currently 57.81% (7.11% off)

**Test Ideas:**
- **Test A**: Same as Hyg Pts Re-appntd - exclude broken/no-show appointments
- **Test B**: Exclude certain appointment types from the reappointment check
- **Test C**: Check if PBN uses a different time window (e.g., appointments scheduled within 30 days)
- **Test D**: Maybe PBN only counts appointments with specific providers or clinics
- **Test E**: Check if the denominator should exclude certain patients (e.g., those with only X-rays, no actual hygiene)

**Investigation Direction:**
- The percentage is (reappointed / total hygiene patients) * 100
- Since total hygiene patients is close (2081 vs 2073), the issue is in the numerator
- Need to reduce reappointed count by ~150 patients (same as Hyg Pts Re-appntd)

### 3. Refine Not on Recall % (20%) - Currently 28.58% (8.58% off)

**Test Ideas:**
- ❌ **Test A (DONE)**: Exclude X-rays - **Makes it WORSE (32.93%)** - Keep X-rays!
- ❌ **Test B (DONE)**: Procedures linked to appointments - **No change (28.58%)**
- ❌ **Test C (DONE)**: Procedures from hygiene appointments - **Much worse (83.92%)**
- ❌ **Test D (DONE)**: Procedures within 18 months - **Slightly worse (29.01%)**
- **Test E**: Check if there's a different combination of codes or filters
- **Test F**: Maybe PBN uses a different definition - check if procedures need to be from completed appointments
- **Test G**: Analyze what makes the 200 extra "not on recall" patients different

**Investigation Direction:**
- Need to reduce "not on recall" count by ~200 patients (from 663 to ~464)
- This means we need to count ~200 more patients as "on recall"
- Try excluding X-rays first (most likely candidate)

### 4. Investigate Recall Current % (53.4%) - Currently 42.97% (10.43% off)

**Test Ideas:**
- ❌ **Test A (DONE)**: Include scheduled appointments - **No change (42.97%)**
- ✅ **Test B (DONE)**: Include patients seen in last 6 months - **48.97% (diff: 4.43%)** ⭐ BEST RESULT!
- ❌ **Test C (DONE)**: Grace period (30 days) - **Much worse (4.53%)**
- ❌ **Test D (DONE)**: Compliance status = 'Compliant' or 'Due Soon' - **Much worse (0.34%)**
- **Test E**: Combine recall record + recent visits (6 months) - **This is the winning combination!**

**Investigation Direction:**
- ✅ **FOUND**: Including patients seen in last 6 months brings us to 48.97% (only 4.43% off!)
- Need to add ~60 more patients to get from 48.97% to 53.4%
- Check what makes those 60 patients different - maybe specific appointment types or providers?

## Recommended Test Scripts

### Created:
1. ✅ **`test_priority_investigations.py`**: Tests most promising hypotheses (broken/no-show, X-rays, scheduled appointments, compliance status)
2. ✅ **`test_follow_up_investigations.py`**: Follow-up tests based on priority results:
   - Hyg Pts Re-appntd: appointment types, time windows
   - Not on Recall: procedures linked to appointments, from hygiene appointments, within 18 months
   - Recall Current: recent visits, grace period

### Next Steps:
- Run `test_follow_up_investigations.py` to explore additional hypotheses
- Analyze the 149 extra patients in Hyg Pts Re-appntd to find common patterns
- Investigate what makes the 240 missing patients in Recall Current % different

## Current Service Logic Summary

### Hyg Patients Seen ✅
- **Logic**: Completed procedures only (status = 2) with codes D0120, D0150, D1110, D1120, D0180, D0272, D0274, D0330
- **Result**: 2081 vs 2073 (only 8 off)
- **Status**: Very close, likely correct

### Hyg Pre-Appointment % ⚠️ → ✅ IMPROVED! ⭐ IMPLEMENTED!
- **Logic**: Completed hygiene procedures + FUTURE appointments (exclude broken/no-show)
- **Result**: 57.66% vs 50.7% (6.96% off, was 7.11% off) ⭐ IMPLEMENTED!
- **Status**: ✅ Improved! From 57.81% to 57.66% by excluding broken/no-show

### Hyg Pts Re-appntd ❌ → ⚠️ IMPROVED! ⭐ IMPLEMENTED!
- **Logic**: Same as Hyg Patients Seen + FUTURE appointments (exclude broken/no-show)
- **Result**: 1200 vs 1051 (149 too many, was 152) ⭐ IMPLEMENTED!
- **Status**: ✅ Improved! From 1203 to 1200 by excluding broken/no-show

### Recall Current % ⚠️ → ✅ Getting Close! ⭐ IMPLEMENTED!
- **Current Logic**: "Has recall record" OR "seen in last 6 months" = current
- **Current Result**: 48.97% vs 53.4% (4.43% off) ⭐ IMPLEMENTED!
- **Status**: ✅ Major improvement! From 42.97% to 48.97% by including recent visits

### Recall Overdue % ✅
- **Logic**: `compliance_status = 'Overdue'` AND no scheduled appointment AND not seen in 6 months
- **Result**: 27.88% vs 25.6% (2.28% off)
- **Status**: Very close, likely correct

### Not on Recall % ⚠️
- **Logic**: Active patients who've never had completed recall service codes (D1110, D1120, D1208, D4910, D0274, D0330, D0210, D0120, D0272)
- **Result**: 28.58% vs 20% (8.58% off)
- **Status**: Much improved, but needs refinement (maybe exclude X-rays?)

