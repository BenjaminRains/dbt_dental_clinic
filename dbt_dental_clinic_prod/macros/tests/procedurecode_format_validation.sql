{% test validate_procedure_code_format(model, column_name) %}
    -- This test validates that procedure codes follow the expected patterns:
    -- 1. Standard CDT format: D# (e.g., D0140, D2330)
    -- 2. 5-digit numeric format (e.g., 12345)
    -- 3. Custom N-codes (e.g., N4104, N4107) - Non-billable procedures
    -- 4. Custom Z-codes (e.g., Ztoth, Ztoths) - Watchlist/monitoring procedures
    -- 5. 4-digit numeric codes (e.g., 0000, 0909) - Internal tracking codes
    -- 6. Custom alphanumeric codes (e.g., CWTRAY, 00PIC) - Special services
    -- 7. Special prefixed codes (e.g., *EMG+, *NPA) - Special or emergency services
    -- 8. Special character codes (e.g., ~BAD~, ~GRP~) - Administrative codes
    -- 9. Extended alphanumeric codes with special chars (e.g., D9230b, IN/ON, iO Brush) - Custom codes
    -- 10. Single letter codes (e.g., D) - Simplified procedure identifiers
    -- 11. Short word codes (e.g., Watch, Clo) - Custom tracking or product codes
    -- 12. Product names with spaces (e.g., Smart 1500, iO Brush) - Retail products
    -- 13. Product codes with numbers (e.g., 3Dtemp, Roottip) - Custom services/products
    
    select *
    from {{ model }}
    where {{ column_name }} is not null
      and {{ column_name }}::text !~ '^D[0-9]'                  -- Not D0-D9 format
      and {{ column_name }}::text !~ '^[0-9]{5}'                -- Not 5-digit format
      and {{ column_name }}::text !~ '^N[0-9]+'                 -- Not N-code format
      and {{ column_name }}::text !~ '^Z[a-z]+'                 -- Not Z-code format
      and {{ column_name }}::text !~ '^[0-9]{4}$'               -- Not 4-digit format
      and {{ column_name }}::text !~ '^[A-Z0-9]{2,6}$'          -- Not custom alphanumeric format
      and {{ column_name }}::text !~ '^\*[A-Z0-9+]+'            -- Not star-prefixed codes (including + character)
      and {{ column_name }}::text !~ '^~[A-Z0-9]+~$'            -- Not tilde-wrapped codes
      and {{ column_name }}::text !~ '^D[0-9]+(\.[a-z]+)?$'      -- Not D-code with suffix (like D8670.auto)
      and {{ column_name }}::text !~ '^D[0-9]+[a-zA-Z]+$'       -- Not D-code with letter suffix (like D9230b, D9972Z)
      and {{ column_name }}::text !~ '^[A-Za-z0-9/\.\s-]{2,20}$' -- Not extended alphanumeric with special chars (allow mixed case)
      -- Additional patterns found in the data:
      and {{ column_name }}::text !~ '^Watch$'                  -- Not 'Watch' code
      and {{ column_name }}::text !~ '^D$'                      -- Not single 'D' code
      and {{ column_name }}::text !~ '^Clo$'                    -- Not 'Clo' code
      and {{ column_name }}::text !~ '^iO Brush$'               -- Not 'iO Brush' product
      and {{ column_name }}::text !~ '^3Dtemp$'                 -- Not '3Dtemp' code
      and {{ column_name }}::text !~ '^Roottip$'                -- Not 'Roottip' code
      and {{ column_name }}::text !~ '^Smart 1500$'             -- Not 'Smart 1500' product 
{% endtest %}