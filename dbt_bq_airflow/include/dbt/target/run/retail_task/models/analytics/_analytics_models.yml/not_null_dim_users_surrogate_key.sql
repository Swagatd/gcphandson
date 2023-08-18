select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select surrogate_key
from `silicon-parity-396203`.`retail`.`dim_users`
where surrogate_key is null



      
    ) dbt_internal_test