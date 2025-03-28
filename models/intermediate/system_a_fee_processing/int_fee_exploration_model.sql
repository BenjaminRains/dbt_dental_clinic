/* DEVELOPMENT

Create Fee Exploration Model:

Develop a separate intermediate model specifically for exploring fee calculations
Join procedurelog, fee, feesched, and adjustment to trace the complete fee history


Fee System Documentation:

Document the observed patterns in how fees flow through the system
Create a diagram showing how base fees are set and modified

Fee Storage vs. Applied Fees:

The fee and feesched tables seem to store the reference fee amounts - essentially defining what "should" be charged for each procedure code
As you noted, the actual fee that gets charged is stored in procedurelog.ProcFee
The fee application process likely involves selecting the appropriate fee from the reference tables based on business rules


Fee Modification System:

After the base ProcFee is established, it appears to be modified through:

Adjustments (tracked in the adjustment table)
Insurance write-offs (likely tracked both in adjustment and claimproc)
Discounts (various types captured in adjustment.AdjType)
*/

-- This query explores the relationship between reference fees and applied fees
WITH FeeComparison AS (
    SELECT 
        pl.procedure_id,
        pl.procedure_code,
        pl.procedure_fee as applied_fee,
        f.fee_amount as reference_fee,
        -- Calculate variance percentage
        CASE 
            WHEN f.fee_amount > 0 THEN 
                ((pl.procedure_fee - f.fee_amount) / f.fee_amount) * 100
            ELSE NULL 
        END as fee_variance_pct,
        -- Categorize fee relationship
        CASE
            WHEN pl.procedure_fee = f.fee_amount THEN 'exact_match'
            WHEN pl.procedure_fee > f.fee_amount THEN 'above_standard'
            WHEN pl.procedure_fee < f.fee_amount THEN 'below_standard'
            WHEN f.fee_amount IS NULL THEN 'no_reference'
            ELSE 'undefined'
        END as fee_relationship,
        -- Add context
        fs.fee_schedule_description,
        pl.procedure_date,
        pl.provider_id
    FROM {{ ref('stg_opendental__procedurelog') }} pl
    LEFT JOIN {{ ref('stg_opendental__fee') }} f 
        ON pl.code_id = f.procedure_code_id
    LEFT JOIN {{ ref('stg_opendental__feesched') }} fs 
        ON f.fee_schedule_id = fs.fee_schedule_id
),

FeeAdjustments AS (
    SELECT 
        procedure_id,
        SUM(adjustment_amount) as total_adjustments,
        COUNT(*) as adjustment_count,
        STRING_AGG(DISTINCT adjustment_category, ', ') as adjustment_types
    FROM {{ ref('stg_opendental__adjustment') }}
    WHERE procedure_id IS NOT NULL
    GROUP BY procedure_id
)

SELECT 
    fc.*,
    fa.total_adjustments,
    fa.adjustment_count,
    fa.adjustment_types,
    -- Calculate effective fee
    fc.applied_fee + COALESCE(fa.total_adjustments, 0) as effective_fee,
    -- Add metadata
    current_timestamp as _loaded_at
FROM FeeComparison fc
LEFT JOIN FeeAdjustments fa ON fc.procedure_id = fa.procedure_id