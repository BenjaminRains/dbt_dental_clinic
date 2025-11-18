# PBN Dashboard Comparison Analysis

## Current Status
We've tested multiple interpretations of PBN's calculation logic, but none match their dashboard values.

## PBN Dashboard Values
- **Recall Current %**: 53.4%
- **Hyg Pre-Appointment %**: 81.1%
- **Hyg Patients Seen**: 180
- **Hyg Pts Re-appntd**: 146
- **Recall Overdue %**: 25.6%
- **Not on Recall %**: 20%

## Our Current Values (12 months)
- **Recall Current %**: 0.0% (with strict logic) or 42.97% (has recall program)
- **Hyg Pre-Appointment %**: 34.5% (highest: 43.39% with "ANY appt after hygiene")
- **Hyg Patients Seen**: 884
- **Hyg Pts Re-appntd**: 305
- **Recall Overdue %**: 99.7%
- **Not on Recall %**: 90.4%

## Key Findings

### Recall Current %
- **"Has recall program"**: 42.97% (997/2320) - closest to PBN's 53.4%
- **"Has recall AND not overdue"**: 0.34% (8/2320) - too strict
- **"Completed appt within recall interval"**: 6.42% (149/2320) - too strict
- **Gap**: Need ~241 more patients (10.4 percentage points) to reach 53.4%

### Hyg Pre-Appointment %
- **Highest result**: 43.39% (374/862) with "ANY appt after hygiene (past or future)" for 90 days
- **"Appt SCHEDULED in range"**: 38.86% (335/862) for 90 days
- **"Future appt EXISTS at hygiene time"**: 41.42% (357/862) for 90 days
- **Gap**: Need ~327 more patients (37.7 percentage points) to reach 81.1%

### Hyg Patients Seen
- **12 months**: 884 patients
- **90 days**: 862 patients
- **60 days**: 852 patients
- **30 days**: 672 patients
- **PBN shows**: 180 patients
- **Gap**: PBN likely uses a much shorter time period (maybe 30-60 days?)

## Possible Explanations

1. **Date Range**: PBN might use a different default (e.g., last 30 days, rolling window, or specific period)
2. **Completed Appointments Only**: PBN might only count completed hygiene appointments, not all hygiene appointments
3. **Different Patient Filter**: PBN might filter patients differently (e.g., only active, only with insurance, etc.)
4. **Data Quality**: There might be missing data or different data sources in PBN
5. **Calculation Timing**: PBN might calculate at a different point in time (e.g., end of day vs. real-time)

## Next Steps

1. **Verify Date Range**: Ask PBN what date range they use for these metrics
2. **Check Completed Only**: Test if filtering to only completed hygiene appointments changes the percentages
3. **Verify Patient Count**: The 180 patients seen suggests a much shorter time window (30-60 days)
4. **Check Data Source**: Verify if PBN uses the same data source or has additional filters

