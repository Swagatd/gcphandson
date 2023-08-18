select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select unique_key
from `silicon-parity-396203`.`retail`.`fct_transactions`
where unique_key is null



      
    ) dbt_internal_test