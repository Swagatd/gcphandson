
    
    

with dbt_test__target as (

  select unique_key as unique_field
  from `silicon-parity-396203`.`retail`.`fct_transactions`
  where unique_key is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


