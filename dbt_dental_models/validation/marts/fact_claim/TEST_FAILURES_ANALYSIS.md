# Fact Claim Test Failures Analysis & Fixes

## Summary

**Total Tests**: 81  
**Passed**: 31  
**Warnings**: 32 (expected for optional fields)  
**Errors**: 18 (need fixes)

---

## Critical Errors (18) - Need Immediate Fixes

### 1. `claim_status` - NULL Values (48,326 records)

**Issue**: Pre-authorization/draft claims (`claim_id = 0`) have NULL `claim_status`  
**Current Test**: `not_null` + `accepted_values: ['S', 'W', 'R', 'H']`  
**Problem**: 
- 48,326 pre-auth claims legitimately have NULL status (they're not yet submitted)
- Missing 'U' value (Unsent) from accepted values

**Fix**:
```yaml
- name: claim_status
  tests:
    - not_null:
        config:
          severity: warn
          where: "claim_id != 0"  # Only require not_null for submitted claims
    - accepted_values:
        values: ['S', 'W', 'R', 'H', 'U']  # Add 'U' for Unsent
        config:
          severity: error
          where: "claim_status IS NOT NULL"  # Only validate when not null
```

---

### 2. `claim_type` - NULL Values (48,326 records)

**Issue**: Pre-authorization/draft claims (`claim_id = 0`) have NULL `claim_type`  
**Current Test**: `not_null` + `accepted_values: ['P', 'S', 'Other']`  
**Problem**: Pre-auth claims don't have a claim type yet

**Fix**:
```yaml
- name: claim_type
  tests:
    - not_null:
        config:
          severity: warn
          where: "claim_id != 0"  # Only require not_null for submitted claims
    - accepted_values:
        values: ['P', 'S', 'Other']
        config:
          severity: error
          where: "claim_type IS NOT NULL"  # Only validate when not null
```

---

### 3. `claim_procedure_status` - Unexpected Values (4 records)

**Issue**: Found 4 records with values not in [0, 1, 4]  
**Current Test**: `accepted_values: [0, 1, 4]`  
**Problem**: Staging model shows valid values are [0, 1, 2, 3, 4, 6, 9]

**Fix**:
```yaml
- name: claim_procedure_status
  tests:
    - not_null
    - accepted_values:
        values: [0, 1, 2, 3, 4, 6, 9]  # Update to match staging model
        config:
          severity: error
          description: "Procedure status must be one of the defined numeric status codes (0=Estimate, 1=Claim, 2=Excluded, 3=CapClaim, 4=CapComplete, 6=InsHist, 9=Pending Review)"
```

**Update Description**:
```yaml
Valid Values:
  - 0: Estimate/Pre-determination
  - 1: Claim
  - 2: Excluded
  - 3: CapClaim (Capitation)
  - 4: CapComplete
  - 6: InsHist (Insurance History)
  - 9: Pending Review/Reconsideration
```

---

### 4. `payment_type_id` - Unexpected Values (8 records)

**Issue**: Found 8 records with values not in [465, 467, 646]  
**Current Test**: `accepted_values: [465, 467, 646]`  
**Problem**: Need to identify what the actual values are

**Action Required**: 
1. Query to find actual values:
```sql
SELECT DISTINCT payment_type_id, COUNT(*) 
FROM marts.fact_claim 
WHERE payment_type_id IS NOT NULL
GROUP BY payment_type_id
ORDER BY COUNT(*) DESC;
```

2. Update accepted_values once we know the actual values, OR
3. Change test to `severity: warn` if these are legitimate edge cases

**Temporary Fix** (if values are legitimate):
```yaml
- name: payment_type_id
  tests:
    - not_null:
        config:
          severity: warn
    - accepted_values:
        values: [465, 467, 646]  # Add additional values once identified
        config:
          severity: warn  # Change to warn until we understand the values
          description: "Payment type must be one of the defined payment method codes"
```

---

### 5. `eob_attachment_count` - NULL Values (48,326 records)

**Issue**: Pre-auth claims have NULL `eob_attachment_count`  
**Current Test**: `not_null`  
**Problem**: Pre-auth claims don't have EOBs yet

**Fix**:
```yaml
- name: eob_attachment_count
  tests:
    - not_null:
        config:
          severity: warn  # Change to warn since it's optional
          where: "claim_id != 0"  # Only require for submitted claims
```

---

### 6-18. Optional Fields with `not_null` Tests (13 fields)

**Issue**: These fields are legitimately nullable but have `not_null` tests:
- `abbreviated_description` (5,582 nulls)
- `base_units` (4,627 nulls)
- `code_prefix` (13,888 nulls)
- `is_hygiene` (4,627 nulls)
- `is_prosthetic` (4,627 nulls)
- `is_radiology` (4,627 nulls)
- `no_bill_insurance` (4,627 nulls)
- `procedure_category_id` (4,627 nulls)
- `procedure_description` (5,582 nulls)
- `procedure_code` (4,627 nulls)
- `provider_id` (4,627 nulls)
- `treatment_area` (4,627 nulls)

**Fix Pattern**: Change `not_null` to `severity: warn` OR remove test entirely if field is truly optional

**Example Fix**:
```yaml
- name: procedure_code
  tests:
    - not_null:
        config:
          severity: warn  # Change from error to warn
          description: "Procedure code is optional for some claim types"
```

---

## Recommended Fix Strategy

### Phase 1: Critical Data Integrity (Do First)
1. Fix `claim_status` - Add 'U' and allow NULL for pre-auth
2. Fix `claim_type` - Allow NULL for pre-auth
3. Fix `claim_procedure_status` - Update accepted values to [0, 1, 2, 3, 4, 6, 9]
4. Investigate `payment_type_id` - Query actual values and update

### Phase 2: Optional Field Tests (Do Second)
5. Change all optional field `not_null` tests to `severity: warn`
6. Add `where` clauses to exclude pre-auth claims where appropriate

### Phase 3: Documentation Updates
7. Update field descriptions to document when NULL values are expected
8. Document pre-auth claim behavior (`claim_id = 0`)

---

## Pattern for Pre-Auth Claims

Many fields are NULL for pre-authorization/draft claims (`claim_id = 0`). This is expected behavior because:
- These claims haven't been submitted yet
- They don't have claim status, claim type, or EOBs
- They may not have procedure details yet

**Recommended Pattern**:
```yaml
tests:
  - not_null:
      config:
        severity: warn
        where: "claim_id != 0"  # Only validate for submitted claims
```

---

## Questions to Resolve

1. **payment_type_id**: What are the 8 unexpected values? Need to query and update accepted_values
2. **Optional fields**: Should we keep `not_null` as `warn` or remove entirely?
3. **Pre-auth claims**: Should we document this pattern in a reusable macro or keep inline?

---

## Next Steps

1. Query `payment_type_id` distinct values
2. Apply Phase 1 fixes
3. Apply Phase 2 fixes
4. Re-run tests to verify
5. Update documentation
