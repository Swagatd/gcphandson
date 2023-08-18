select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select recorded_at
from `silicon-parity-396203`.`retail`.`stg_accounts_closed`
where recorded_at is null



      
    ) dbt_internal_test