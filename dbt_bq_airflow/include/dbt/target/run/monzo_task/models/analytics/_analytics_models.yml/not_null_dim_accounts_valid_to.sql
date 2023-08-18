select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select valid_to
from `silicon-parity-396203`.`monzo`.`dim_accounts`
where valid_to is null



      
    ) dbt_internal_test