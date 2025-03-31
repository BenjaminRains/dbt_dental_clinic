# Data Quality Report: Tooth Surface Coding Investigation

**Date**: [Current Date]  
**Subject**: Non-standard Surface Codes in Procedure Log  
**Priority**: Medium

## Summary
We have identified 22 dental procedures where numeric values (1, 2, 5, 6) are being used in the 
tooth surface field instead of the standard dental surface notation (BDFILMOURV). This may indicate
 either a data entry issue or an undocumented coding system being used for specific procedures.

## Details

### Affected Records
- Total Records Affected: 22
- Date Range: 2023-05-18 to 2024-06-25
- Primary Procedure Code: 523 (19 records)
- Secondary Procedure Code: 1440 (3 records)

### Current Surface Coding Standards
Valid dental surface codes should only contain:
- B = Buccal
- D = Distal
- F = Facial
- I = Incisal
- L = Lingual
- M = Mesial
- O = Occlusal
- U = Universal
- R = Root
- V = Vestibular

### Observed Issue
Instead of standard surface codes, we're seeing numeric values:
- "1"
- "2" (most common)
- "5"
- "6"

## Potential Causes
1. Data Entry Error: Staff might be entering numeric positions instead of surface codes
2. Special Procedure Coding: Procedures 523 and 1440 might legitimately use a different coding system
3. Training Gap: New staff might not be familiar with standard surface notation
4. System Interface: Possible software interface issue allowing numeric entries

## Impact
- Data inconsistency in reporting
- Potential issues with insurance claims if incorrect surface codes are submitted
- Risk of miscommunication between providers

## Recommended Actions

### Immediate:
- Verify with clinical staff if procedures 523 and 1440 have special surface coding requirements
- Review these 22 specific cases to determine correct surface codes

### Short-term:
- Update documentation if these numeric codes are legitimate
- Provide refresher training on proper surface coding if these are errors

### Long-term:
- Consider adding validation in the entry system to prevent numeric surface codes
- Update surface validation rules if certain procedures require different coding

## Next Steps
Please review this report and indicate if:
1. These numeric codes are legitimate for these specific procedures
2. This is a training/data entry issue that needs to be addressed
3. Additional investigation is needed

For reference, please review procedure IDs: 1121648, 1120466, 1119777 (sample of affected records)
 to confirm proper coding.