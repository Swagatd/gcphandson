-- back compat for old kwarg name
  
  
        
    

    

    merge into `silicon-parity-396203`.`monzo`.`stg_accounts_closed` as DBT_INTERNAL_DEST
        using (
        select
        * from `silicon-parity-396203`.`monzo`.`stg_accounts_closed__dbt_tmp`
        ) as DBT_INTERNAL_SOURCE
        on (FALSE)

    

    when not matched then insert
        (`recorded_at`, `account_id`)
    values
        (`recorded_at`, `account_id`)


    