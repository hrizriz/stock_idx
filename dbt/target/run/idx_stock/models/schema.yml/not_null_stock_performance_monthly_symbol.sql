select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from "airflow"."public_analytics"."stock_performance_monthly"
where symbol is null



      
    ) dbt_internal_test