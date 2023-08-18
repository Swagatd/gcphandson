select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select account_id
from `silicon-parity-396203`.`monzo`.`stg_accounts_created`
where account_id is null



      
    ) dbt_internal_test