# Insurance Plan Group Number Analysis Report

## Overview
Analysis of insurance plan group numbers reveals several patterns, including legitimate duplicates and potential data quality issues. This document outlines the key findings and recommendations.

## P13168 Group Number Analysis

### 1. Overview
- Total Records: 42
- Unique Employers: 40
- Unique Carriers: 5

### 2. Carrier Distribution
*Note: Carrier distribution data not available in current analysis*

### 3. Employer Analysis
Top 10 Employers by Group Name:
| Employer Name | Count |
|--------------|-------|
| UPS | 2 |
| Jack Gray Transportation | 1 |
| Pepsi | 1 |
| gary school corp | 1 |
| City Of Gary | 1 |
| Rieth & Riley Construction | 1 |
| Burnham Trucking | 1 |
| Roadway Express | 1 |
| Ball & Foster Glass Co. | 1 |
| Webb Ford | 1 |

### 4. Recommendations

#### Data Quality Issue
- P13168 appears to be used as a default/placeholder group number
- Used across multiple carriers and employer types
- No clear pattern in assignment

#### Immediate Actions
- Flag all existing P13168 records for review
- Prevent new assignments of P13168
- Investigate carrier assignment process

#### Long-term Solutions
- Implement validation rules for group number assignment
- Create documentation for proper group number assignment
- Consider automated group number generation system

## Duplicate Group Number Patterns

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

## Record Creation Timing Analysis

### 1. Overview
- Total Records Analyzed: 403
- Date Range: 2020 - 2025

### 2. Yearly Distribution
| Year | Count |
|------|-------|
| 2020 | 140 |
| 2021 | 153 |
| 2022 | 53 |
| 2023 | 47 |
| 2024 | 40 |
| 2025 | 10 |

### 3. Anomaly Analysis
Future-dated Records (2024-2025):
| insurance_plan_id | group_number | created_at |
|------------------|--------------|------------|
| 13566 | NaN | 2024-01-04 |
| 13583 | NaN | 2024-01-06 |
| 13608 | NaN | 2024-01-10 |
| 13634 | NaN | 2024-01-17 |
| 13656 | NaN | 2024-01-23 |

### 4. Recommendations

#### Data Quality Issues
- 50 records with future creation dates (2024-2025)
- No records before 2020
- Peak in record creation during 2020-2021

#### Immediate Actions
- Review all future-dated records
- Implement validation to prevent future dates
- Investigate the 2020-2021 peak in record creation

#### Long-term Solutions
- Add system-level validation for creation dates
- Implement audit logging for record creation
- Consider adding a "planned effective date" field separate from creation date

### 5. Monthly Patterns
Monthly Creation Patterns:
| Month | Count |
|-------|-------|
| 2020-04 | 4 |
| 2020-05 | 23 |
| 2020-06 | 14 |
| 2020-07 | 29 |
| 2020-08 | 15 |
| 2020-09 | 17 |
| 2020-10 | 15 |
| 2020-11 | 15 |
| 2020-12 | 8 |
| 2021-01 | 23 |

### 6. Record Age Analysis
Record Age Statistics (in days):
| Statistic | Value |
|-----------|-------|
| count | 443.000000 |
| mean | 1242.801354 |
| std | 491.474452 |
| min | 40.000000 |
| 25% | 937.000000 |
| 50% | 1442.000000 |
| 75% | 1620.000000 |
| max | 1800.000000 |

## Technical Implementation Recommendations

### 1. Data Quality Tests
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

## Next Steps
1. Validate findings with business stakeholders
2. Update data quality tests based on confirmed patterns
3. Investigate potential data quality issues (especially P13168)
4. Document legitimate duplicate patterns in data dictionary 