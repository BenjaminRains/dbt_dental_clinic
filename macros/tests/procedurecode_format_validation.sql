{% test validate_procedure_code_format(model, column_name) %}
    -- This test validates that procedure codes follow the expected patterns:
    -- 1. Standard CDT format: D# (e.g., D0140, D2330)
    -- 2. 5-digit numeric format (e.g., 12345)
    -- 3. Custom N-codes (e.g., N4104, N4107) - Non-billable procedures
    -- 4. Custom Z-codes (e.g., Ztoth, Ztoths) - Watchlist/monitoring procedures
    -- 5. 4-digit numeric codes (e.g., 0000, 0909) - Internal tracking codes
    -- 6. Custom alphanumeric codes (e.g., CWTRAY, 00PIC) - Special services
    
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }}::text !~ '^D[0-9]'         -- Not D0-D9 format
      and {{ column_name }}::text !~ '^[0-9]{5}'       -- Not 5-digit format
      and {{ column_name }}::text !~ '^N[0-9]+'        -- Not N-code format
      and {{ column_name }}::text !~ '^Z[a-z]+'        -- Not Z-code format
      and {{ column_name }}::text !~ '^[0-9]{4}$'      -- Not 4-digit format
      and {{ column_name }}::text !~ '^[A-Z0-9]{2,6}$' -- Not custom alphanumeric format
    
{% endtest %} 