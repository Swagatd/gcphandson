select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select user_id
from `silicon-parity-396203`.`retail`.`stg_accounts_created`
where user_id is null



      
    ) dbt_internal_test