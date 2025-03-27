-- Example audit query
select 
    table_name,
    column_name
from information_schema.columns
where table_schema = 'public_staging'
    and (column_name like '%_id' or column_name like '%_num')
order by table_name, column_name;