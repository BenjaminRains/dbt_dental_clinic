# Insurance Plan Duplicates Analysis

## Overview
This document analyzes duplicate records in the `stg_opendental__insplan` table, focusing on records that share the same combination of group_number, carrier_id, and employer_id. The analysis reveals several patterns and potential causes for these duplicates.

## Key Findings

### 1. Case Sensitivity and Formatting Issues
- Many duplicates are simply case variations of the same name
- Examples:
  - "UPS" vs "Ups"
  - "COMMUNITY HEALTHCARE SYSTEM" vs "Community Healthcare System"
  - "UNITED STATES STEEL" vs "United States Steel"
- Some have minor formatting differences (spaces, punctuation)
- Example: "Saco Industries, Inc." vs "Saco Industries,Inc."

### 2. Empty Group Numbers
- Several records have empty group numbers but same carrier_id and employer_id
- Example: carrier_id 2163 with employer_id 0 has 4 duplicate records
- These might be placeholder or system-generated records
- Need to determine if empty group numbers are valid in certain scenarios

### 3. Multiple Carriers for Same Group Number
- Some group numbers appear with different carriers
- Example: group number "836357100" (US Steel) appears with carriers:
  - 1459 (7 records)
  - 2014 (3 records)
  - 2121 (12 records)
  - 2266 (2 records)
- This suggests legitimate business relationships with multiple carriers
- Need to document when multiple carriers are allowed for the same group

### 4. Fee Schedule Variations
- Some duplicates have different fee schedule IDs
- Example: US Steel records have different manual_fee_schedule_id values (8278 vs 0)
- This might indicate different plan types or coverage levels
- Need to understand the relationship between fee schedules and plan variations

### 5. PPO Settings
- Some duplicates have different has_ppo_subst_writeoffs settings
- Example: P13168 has one record with has_ppo_subst_writeoffs=true and another with false
- Need to document when different PPO settings are allowed for the same group

### 6. Creation and Update Patterns
- Many records were created in 2020-2021
- Most were updated on "2024-12-18 10:28:49.000"
- Some have future update dates (2025)
- Some records have empty created_at dates but recent update dates
- Need to understand the significance of these patterns

### 7. Hidden Status
- Some duplicates have different is_hidden settings
- Example: Amazon Basic Operations record has one hidden and one visible version
- Need to document rules for hiding/unhiding records

## Recommendations

### 1. Business Rules Documentation
- Document when multiple carriers are allowed for the same group number
- Specify rules for handling case sensitivity in group names
- Define standards for group name formatting
- Create guidelines for empty group numbers

### 2. Data Quality Improvements
- Implement case-insensitive matching for group names
- Create standardized formats for group names
- Document rules for empty group numbers
- Add validation for group name format

### 3. System Behavior Documentation
- Document the purpose of records with empty created_at dates
- Explain the significance of the 2024-12-18 update date
- Define rules for future-dated records
- Create guidelines for record updates

### 4. Fee Schedule Documentation
- Document when different fee schedules are allowed for the same group
- Explain the relationship between fee schedules and carriers
- Define rules for PPO settings
- Create guidelines for fee schedule assignments

## Next Steps
1. Review and validate these findings with business stakeholders
2. Create specific data quality rules based on business requirements
3. Implement automated tests to enforce these rules
4. Consider data cleanup activities for existing duplicates
5. Document standard operating procedures for handling future duplicates

## Questions for Stakeholders
1. Are multiple carriers for the same group number a valid business case?
2. Should empty group numbers be allowed in certain scenarios?
3. What is the significance of the 2024-12-18 update date?
4. How should we handle future-dated records?
5. What are the rules for hiding/unhiding records? 