select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select account_surrogate_key as from_field
    from `silicon-parity-396203`.`monzo`.`fct_transactions`
    where account_surrogate_key is not null
),

parent as (
    select surrogate_key as to_field
    from `silicon-parity-396203`.`monzo`.`dim_accounts`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test