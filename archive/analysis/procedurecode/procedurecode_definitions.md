# Dental Procedure Codes

## Overview
This document describes the organization and usage of dental procedure codes in our system. These codes are sourced from OpenDental and follow the ADA's CDT (Code on Dental Procedures and Nomenclature) format with internal customizations.

## Code Structure
Procedure codes follow two formats:
1. **Standard CDT Format**: Dxxxx (e.g., D0140, D1110)
2. **Internal Format**: Some common procedures use 00xxx format (e.g., 00110, 00120)

## Code Prefixes
The first digit after the "D" indicates the procedure category:
- **D0**: Diagnostic (exams, x-rays)
- **D1**: Preventive (cleanings, fluoride)
- **D2**: Restorative (fillings)
- **D4**: Periodontics (gum treatments) - 50% have hygiene flags
- **D5**: Prosthodontics, removable (dentures)
- **D6**: Prosthodontics, fixed (crowns, bridges) - 21% have prosthetic flags
- **D7**: Oral Surgery (extractions)
- **D8**: Orthodontics (braces)
- **D9**: Adjunctive Services (pain treatment, anesthesia)

## Internal Categories
Our system uses internal category IDs that organize procedures differently:
- **Category 622**: Most common procedures (83% of all codes), primarily basic exams
- **Category 250**: Mostly D9 codes for administrative and adjunctive services
- **Category 73**: Standard diagnostic procedures and imaging
- **Category 80**: Implant-related procedures
- **Category 300**: Newer procedures including 3D imaging and COVID-19 vaccines

## Flag Usage
Special flags affect billing and clinical workflows:
- **is_hygiene_flag**: Indicates procedure typically performed by hygienists
- **is_prosthetic_flag**: Affects lab work and insurance processing
- **is_radiology_flag**: Requires radiographic equipment
- **is_multi_visit_flag**: Indicates procedure requires multiple appointments

## Common Procedures
Most frequently used terms in procedure descriptions:
- Crown-related (61 occurrences)
- Implant-related (46 occurrences)
- Partial dentures (45 occurrences)
- Metal components (43 occurrences)