select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select account_id
from `silicon-parity-396203`.`retail`.`stg_accounts_reopened`
where account_id is null



      
    ) dbt_internal_test