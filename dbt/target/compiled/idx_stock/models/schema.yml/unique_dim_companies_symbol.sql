
    
    

select
    symbol as unique_field,
    count(*) as n_records

from "airflow"."public_core"."dim_companies"
where symbol is not null
group by symbol
having count(*) > 1


