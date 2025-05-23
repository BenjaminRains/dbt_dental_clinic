-- Basic Data Quality Tests
with patient_validation as (
    select * from {{ ref('stg_opendental__patient') }}
    where patient_status != 2  -- Exclude inactive patients
),

validation_errors as (
    select
        patient_id,
        array_agg(validation_error) as validation_errors
    from (
        select
            patient_id,
            case
                -- Patient ID validation
                when patient_id is null then 'Patient ID cannot be null'
                
                -- Basic demographic validations
                when birth_date > current_date then 'Birth date cannot be in the future'
                when birth_date < '1900-01-01' then 'Birth date seems too old'
                when age < 0 then 'Age cannot be negative'
                when age > 120 and birth_date != '1900-01-01' then 'Age seems unusually high'
                when birth_date = '1900-01-01' and _updated_at > current_timestamp 
                    then 'Default birth date with future update date'
                
                -- Gender validation (assuming 0=Unknown, 1=Male, 2=Female)
                when gender not in (0, 1, 2) then 'Invalid gender code'
                
                -- Status validations
                when patient_status not in (0, 1, 2, 3, 4, 5) then 'Invalid patient status'
                when position_code not in (0, 1, 2, 3, 4) then 'Invalid position code'
                
                -- Financial validations
                when estimated_balance < -10000000 then 'Estimated balance seems too low'
                when estimated_balance > 10000000 then 'Estimated balance seems too high'
                when total_balance > 0 and  -- Only check positive balances
                    (total_balance + 0.01) < (balance_0_30_days + balance_31_60_days + 
                                    balance_61_90_days + balance_over_90_days) 
                    then 'Balance components exceed total'
                when balance_0_30_days < 0 or balance_31_60_days < 0 or 
                     balance_61_90_days < 0 or balance_over_90_days < 0 then 'Negative aging balance'
                
                -- Contact preference validations
                when preferred_confirmation_method not in (0, 2, 4, 8) then 'Invalid confirmation method'
                when preferred_contact_method not in (0, 2, 3, 4, 5, 6, 8) then 'Invalid contact method'
                when preferred_recall_method not in (0, 2, 4, 8) then 'Invalid recall method'
                
                -- Scheduling validations
                when preferred_day_of_week not between 0 and 6 then 'Invalid day of week'
                when schedule_not_before_time > schedule_not_after_time then 'Invalid schedule time range'
                when ask_to_arrive_early_minutes < 0 then 'Early arrival minutes cannot be negative'
                when ask_to_arrive_early_minutes > 120 then 'Early arrival request seems too long'
                
                -- Billing cycle validation
                when billing_cycle_day not between 1 and 31 then 'Invalid billing cycle day'
                
                -- Date validations
                when _created_at > current_timestamp then 'Creation date cannot be in future'
                when _updated_at > current_timestamp then 'Update date cannot be in future'
                when _created_at is not null and _updated_at < _created_at 
                    then 'Update date cannot be before creation date'
                when deceased_datetime is not null and deceased_datetime > current_timestamp 
                    then 'Deceased date cannot be in future'
                
                -- Relationship validations
                when primary_provider_id = secondary_provider_id 
                    and primary_provider_id is not null 
                    and primary_provider_id != 0
                    and _updated_at > current_timestamp - interval '30 days'
                    then 'Primary and secondary provider cannot be the same for recently updated records'
            end as validation_error
        from patient_validation
        where true  -- This allows us to use AND for all subsequent conditions
    ) subq
    where validation_error is not null
    group by patient_id
)

-- Return records that failed validation
select 
    patient_id,
    validation_errors
from validation_errors
order by patient_id
