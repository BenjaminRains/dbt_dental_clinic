{% test income_transfer_analysis(model) %}

with unmatched_transfers as (
    select 
        p.patient_id,
        p.payment_id,
        p.payment_date,
        p.split_amount,
        p.payment_type_id,
        p.payment_notes,
        p.include_in_ar,
        -- Check if there's a matching record
        exists (
            select 1 
            from {{ ref('stg_opendental__paysplit') }} ps2
            where ps2.payment_id = p.payment_id
            and ps2.split_amount = -p.split_amount
            and ps2.payment_notes like '%INCOME TRANSFER%'
        ) as has_matching_record,
        -- Count of records with same payment_id
        (select count(*) 
         from {{ ref('stg_opendental__paysplit') }} ps2
         where ps2.payment_id = p.payment_id) as records_with_same_payment_id,
        -- Sum of amounts for same payment_id
        (select sum(split_amount)
         from {{ ref('stg_opendental__paysplit') }} ps2
         where ps2.payment_id = p.payment_id) as total_amount_for_payment_id
    from {{ model }} p
    where p.payment_notes like '%INCOME TRANSFER%'
    and p.payment_type_id != 0
    and p.include_in_ar = true
    and not exists (
        select 1 
        from {{ ref('stg_opendental__paysplit') }} ps2
        where ps2.payment_id = p.payment_id
        and ps2.split_amount = -p.split_amount
        and ps2.payment_notes like '%INCOME TRANSFER%'
    )
)

select 
    -- Basic counts
    count(*) as total_unmatched_transfers,
    count(distinct patient_id) as unique_patients,
    count(distinct payment_id) as unique_payment_ids,
    
    -- Amount analysis
    min(split_amount) as min_amount,
    max(split_amount) as max_amount,
    avg(split_amount) as avg_amount,
    sum(split_amount) as total_amount,
    
    -- Payment type distribution
    payment_type_id,
    count(*) as count_by_type,
    sum(split_amount) as total_amount_by_type,
    
    -- Date patterns
    date_trunc('month', payment_date) as payment_month,
    count(*) as transfers_per_month,
    
    -- Payment ID patterns
    records_with_same_payment_id,
    count(*) as count_by_payment_id_records,
    
    -- Amount patterns
    case 
        when split_amount < 0 then 'Negative'
        when split_amount > 0 then 'Positive'
        else 'Zero'
    end as amount_sign,
    count(*) as count_by_sign
    
from unmatched_transfers
group by 
    payment_type_id,
    date_trunc('month', payment_date),
    records_with_same_payment_id,
    case 
        when split_amount < 0 then 'Negative'
        when split_amount > 0 then 'Positive'
        else 'Zero'
    end
order by 
    payment_month desc,
    payment_type_id,
    records_with_same_payment_id;

{% endtest %} 