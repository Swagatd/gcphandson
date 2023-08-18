select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select is_open
from `silicon-parity-396203`.`retail`.`dim_accounts`
where is_open is null



      
    ) dbt_internal_test