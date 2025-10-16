{% docs __overview__ %}
# OpenDental Analytics Documentation

This dbt project transforms data from the OpenDental practice management system into a clean, analytics-ready data warehouse. The project includes staging models that standardize OpenDental's data conventions, intermediate models for business logic, and mart models for reporting.

## Data Flow
1. **Raw Layer**: OpenDental MySQL source database
2. **Staging Layer**: Type conversions, column renaming, data cleaning
3. **Intermediate Layer**: Business logic, calculations, enrichment
4. **Marts Layer**: Denormalized models optimized for reporting

## Key Concepts
- All models follow snake_case naming conventions
- OpenDental data types are converted to PostgreSQL standards
- Metadata columns track data lineage and transformations
- Incremental models minimize processing time for large tables

{% enddocs %}

{% docs opendental_source %}
OpenDental is a dental practice management system that stores operational data in a MySQL database. This is the source system for all data in this analytics warehouse.
{% enddocs %}

{% docs opendental_boolean_format %}
OpenDental stores boolean values as TINYINT (0/1) in MySQL. In staging models, these are converted to PostgreSQL boolean values (true/false/null) using the `convert_opendental_boolean()` macro.

- `0` → `false`
- `1` → `true`
- `NULL` → `null`

Common boolean patterns:
- `IsHidden`: Indicates soft-deleted or archived records
- `IsActive`: Indicates currently active records
- Consent flags (e.g., `TxtMsgOk`, `PreferContactMethod`)
{% enddocs %}

{% docs opendental_date_format %}
OpenDental uses inconsistent date representations, including sentinel values for "no date":
- `0001-01-01` - Common null date placeholder
- `1900-01-01` - Legacy null date placeholder  
- Empty timestamps

The `clean_opendental_date()` macro converts these to proper NULL values and ensures consistent date/timestamp formats.
{% enddocs %}

{% docs opendental_string_format %}
OpenDental string columns may contain:
- Empty strings (`''`) that should be NULL
- Leading/trailing whitespace
- Inconsistent text encodings

The `clean_opendental_string()` macro standardizes these by trimming whitespace and converting empty strings to NULL.
{% enddocs %}

{% docs audit_metadata %}
Standard audit columns that track record creation and modification in the source system. These are transformed from OpenDental's naming conventions:

- **Created at**: When the record was first created (from `DateEntry` or similar)
- **Updated at**: When the record was last modified (from `DateTStamp` or similar)
- **Created by**: User ID who created the record (from `SecUserNumEntry` or similar)

Additional metadata columns added during transformation:
- `_loaded_at`: When the record was loaded from source to raw layer
- `_transformed_at`: When the record was transformed in dbt
{% enddocs %}

{% docs primary_key %}
Unique identifier for this record. In OpenDental, primary keys typically follow the pattern `[TableName]Num` (e.g., `PatNum`, `AptNum`, `ProcNum`) and are transformed to `[table_name]_id` in staging models.

Primary keys are:
- Auto-incrementing integers
- Unique and not null
- Never reused, even for deleted records
{% enddocs %}

{% docs foreign_key %}
Reference to a primary key in another table. Foreign keys maintain referential relationships between entities.

Note: OpenDental uses `0` to represent "no relationship" rather than NULL in many foreign key columns. Staging models often convert these zeros to NULL for clearer semantics.
{% enddocs %}

{% docs soft_delete %}
OpenDental uses soft deletion for most entities rather than physically removing records. Soft-deleted records are typically marked with:
- `IsHidden = 1` 
- Or status fields indicating deletion

Staging models preserve these flags so downstream models can filter as needed. Most mart models exclude soft-deleted records by default.
{% enddocs %}

{% docs patient_id %}
Unique identifier for a patient. The primary key of the patient table. This is the most commonly referenced foreign key across the system as most entities relate to patients.

OpenDental source column: `PatNum`
{% enddocs %}

{% docs guarantor_id %}
The patient ID of the family guarantor - the person financially responsible for the account. In OpenDental's family structure, one patient serves as the guarantor for their family unit.

When `guarantor_id = patient_id`, the patient is their own guarantor (head of family).

OpenDental source column: `Guarantor`
{% enddocs %}

{% docs provider_id %}
Unique identifier for a provider (dentist, hygienist, or other clinical staff). Providers are assigned to appointments, procedures, and other clinical activities.

OpenDental source column: `ProvNum` or `PriProv` (primary provider)
{% enddocs %}

{% docs clinic_id %}
Unique identifier for a clinic location. Multi-location practices use clinic IDs to segment data by office location.

A value of `0` or `NULL` typically indicates either:
- Single-location practices that don't use clinic tracking
- Records created before clinic tracking was enabled

OpenDental source column: `ClinicNum`
{% enddocs %}

{% docs appointment_status %}
The status of a dental appointment. Common values:
- `1`: Scheduled
- `2`: Complete  
- `3`: Broken (cancelled by patient)
- `4`: Unscheduled (not yet scheduled)
- `5`: ASAP
- `6`: Broken (cancelled by clinic)

Status definitions may vary by practice configuration. See `appointmenttype` table for practice-specific definitions.

OpenDental source column: `AptStatus`
{% enddocs %}

{% docs procedure_status %}
The status of a dental procedure. Common values:
- `1`: Treatment Planned (TP)
- `2`: Complete (C)
- `3`: Existing Current Provider (EC)
- `4`: Existing Other Provider (EO)
- `5`: Referred Out (R)
- `6`: Deleted (D)

Status affects accounting and reporting:
- Only Complete procedures generate production
- TP procedures appear in treatment plans
- Deleted procedures are excluded from most reports

OpenDental source column: `ProcStatus`
{% enddocs %}

{% docs insurance_claim_status %}
The current status of an insurance claim. Common values:
- `U`: Unsent
- `W`: Waiting (sent but not received)
- `S`: Sent
- `R`: Received

Claim status affects A/R aging and collection workflows.

OpenDental source column: `ClaimStatus`
{% enddocs %}

{% docs payment_type %}
The method of payment. Links to the `definition` table for practice-specific payment type definitions.

Common payment types:
- Cash
- Check  
- Credit Card
- Insurance Payment
- ACH/EFT

OpenDental source column: `PayType`
{% enddocs %}

{% docs procedure_code %}
The ADA procedure code (e.g., D0120, D1110, D2391) that identifies the type of dental service performed. Links to the `procedurecode` table for full code details including descriptions and default fees.

Procedure codes follow ADA CDT (Code on Dental Procedures and Nomenclature) standards.

OpenDental source column: `ProcCode` or `CodeNum`
{% enddocs %}

{% docs fee_schedule %}
A fee schedule defines the standard fees for procedure codes. Practices may have multiple fee schedules for:
- Different insurance plans
- PPO vs. fee-for-service
- Different patient categories
- Promotional pricing

OpenDental source column: `FeeSchedNum`
{% enddocs %}

{% docs insurance_plan %}
An insurance plan represents a specific dental insurance product from a carrier. Plans define:
- Coverage percentages by procedure category
- Annual maximums
- Deductibles
- Waiting periods
- Associated fee schedules

OpenDental source column: `PlanNum`
{% enddocs %}

{% docs insurance_subscriber %}
The insurance subscriber (or "insured") is the person who holds the insurance policy. Dependents are covered under the subscriber's plan.

Links patient to insurance plan through the subscriber relationship.

OpenDental source column: `InsSubNum` or `Subscriber`
{% enddocs %}

{% docs claim_procedure %}
The `claimproc` table links procedures to insurance claims and tracks insurance payments. Each procedure can have multiple claimproc records:
- One per insurance plan (primary, secondary, tertiary)
- One for patient portion

ClaimProc status indicates:
- Estimate: Not yet attached to a claim
- CapClaim: Capitation claim
- NotReceived: Claim sent, waiting for payment
- Received: Insurance payment received
- Supplemental: Supplemental payment
{% enddocs %}

{% docs production_value %}
Production represents the value of dental services performed. Production is typically recorded when procedures are completed (status = Complete).

Production metrics are key performance indicators for dental practices:
- **Gross production**: Total value of completed procedures
- **Net production**: After adjustments and writeoffs
- **Collections**: Actual cash received

OpenDental source column: `ProcFee` (procedurelog table)
{% enddocs %}

{% docs adjustment_type %}
Adjustments modify patient account balances outside of procedures and payments. Common adjustment types:
- Write-offs (contractual, bad debt)
- Finance charges
- Refunds
- Account credits
- Courtesy discounts

Links to `definition` table for practice-specific adjustment type definitions.

OpenDental source column: `AdjType`
{% enddocs %}

{% docs family_module %}
OpenDental organizes patients into family units. Each family has one guarantor (financially responsible party) and zero or more dependents.

Family-level concepts:
- Family balance: Total amount owed by the family
- Guarantor: Head of household
- Family relationships: Tracked in `patientlink` table

Most accounting and billing occurs at the family level rather than individual patient level.
{% enddocs %}

{% docs recall_system %}
The recall system manages preventive care reminders (e.g., 6-month cleanings, 1-year exams). Key tables:
- `recall`: Active recall appointments due
- `recalltype`: Types of recalls (cleaning, perio, etc.)
- `recalltrigger`: Procedure codes that trigger recall scheduling

Recall management is critical for patient retention and preventive care compliance.
{% enddocs %}

{% docs treatment_plan %}
Treatment plans document proposed future procedures. A treatment plan consists of:
- `treatplan`: The overall plan
- `proctp` or `treatplanattach`: Procedures included in the plan
- Status tracking (presented, accepted, completed)

Treatment plans help with case acceptance and production forecasting.
{% enddocs %}

{% docs perio_charting %}
Periodontal charting records gum health measurements. Key tables:
- `perioexam`: The exam record
- `periomeasure`: Individual measurements (probing depths, bleeding, etc.)

Perio charting is required for periodontal diagnosis and treatment planning.
{% enddocs %}

{% docs incremental_strategy %}
This model uses incremental materialization to improve performance on large tables. Only new or modified records are processed on subsequent runs.

The model filters for new records using:
- Timestamp comparison (e.g., `DateTStamp > MAX(DateTStamp)`)
- Or ID comparison (e.g., `ProcNum > MAX(procedure_id)`)

To rebuild the full table, run: `dbt run --full-refresh --models <model_name>`
{% enddocs %}

{% docs data_quality_warning %}
⚠️ **Data Quality Note**: This field may contain inconsistent or incomplete data from the source system. Use caution when using this field for critical reporting or business logic.

Common data quality issues include:
- Missing values in required fields
- Inconsistent categorization
- Manual data entry errors
- Historical data migration artifacts
{% enddocs %}

{% docs testing_strategy %}
This model includes data quality tests to ensure:
- **Uniqueness**: Primary keys are unique
- **Not null**: Required fields contain values  
- **Referential integrity**: Foreign keys reference valid records
- **Accepted values**: Categorical fields contain valid values
- **Relationships**: Related records exist in dependent tables

Tests run automatically during `dbt test` and will fail the build if data quality issues are detected.
{% enddocs %}

{% docs snapshot_tracking %}
This is a slowly changing dimension (SCD) table that tracks historical changes to records over time. 

Snapshot columns:
- `dbt_valid_from`: When this version of the record became valid
- `dbt_valid_to`: When this version of the record was superseded (NULL = current)
- `dbt_scd_id`: Unique identifier for this snapshot version
- `dbt_updated_at`: Source system timestamp for the record

Use snapshots to analyze historical states and trends over time.
{% enddocs %}

{% docs cents_to_dollars %}
OpenDental stores some monetary values in cents (integers) rather than decimal dollars. These are converted to decimal dollar amounts in staging models by dividing by 100.

Example: `12500` → `125.00`
{% enddocs %}

{% docs timezone_handling %}
OpenDental stores timestamps in the local timezone of the practice (no timezone awareness). All timestamps in this warehouse are in the practice's local time.

When comparing timestamps across systems or performing date arithmetic, be aware of:
- Daylight saving time transitions
- Date boundaries (midnight calculations)
{% enddocs %}

