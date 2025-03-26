# Analysis of Duplicate Group Numbers in Insurance Plans

## Overview
Analysis of duplicate group numbers in the insurance plans data reveals several legitimate patterns as well as potential data quality issues. This document outlines the key findings and recommendations.

## Key Patterns

### 1. Top Duplicate Group Numbers

| Group Number | Count | Pattern Type | Description |
|-------------|-------|--------------|-------------|
| P13168 | 42 | Potential Issue | 40 unique employers, 5 carriers, 36 group names |
| 836357100 | 27 | Legitimate | US Steel - same company across carriers |
| 012000 | 14 | Industry-Specific | Railroad companies sharing group number |
| 105 | 14 | Government | Federal agencies (USPS, etc.) |
| 104 | 12 | Government | Federal agencies |
| 675717 | 12 | Program-Specific | Medicare-related group number |

### 2. Pattern Categories

#### Legitimate Duplicates
- **Company-Specific**: Same company using same group number across different carriers
  - Example: US Steel (836357100) with 2 employers, 6 carriers
  - Typically has consistent group names with minor variations

#### Industry/Sector Patterns
- **Federal Government**: Group numbers 104, 105
  - Multiple federal agencies sharing group numbers
  - Different employer IDs but related organizations
- **Railroad Industry**: Group number 012000
  - Shared across multiple railroad companies
  - Suggests industry-specific numbering system

#### Program-Specific
- **Medicare**: Group number 675717
  - Used across multiple carriers
  - Consistent program but different implementations

#### Potential Data Quality Issues
- **P13168**: Shows concerning pattern
  - 40 different employers
  - 36 different group names
  - Only 5 carriers
  - Requires investigation for potential data entry issues

## Recommendations

### 1. Data Quality
- Implement composite key checks instead of simple group number uniqueness
- Consider combination of:
  - group_number
  - carrier_id
  - employer_id

### 2. Business Rules
- Document legitimate patterns:
  - Federal agency group numbers
  - Medicare group numbers
  - Industry-specific patterns
- Create specific validation rules for known patterns

### 3. Monitoring
- Track changes in group number usage patterns
- Monitor for unexpected combinations of:
  - Multiple employers with same group number
  - Inconsistent group names for same group number
  - Unusual carrier-employer combinations

## Technical Implementation Notes
- Current uniqueness test on group_number alone is insufficient
- Need to account for legitimate duplicate patterns
- Consider implementing pattern-specific tests in dbt

## Next Steps
1. Validate findings with business stakeholders
2. Update data quality tests based on confirmed patterns
3. Investigate potential data quality issues (especially P13168)
4. Document legitimate duplicate patterns in data dictionary 