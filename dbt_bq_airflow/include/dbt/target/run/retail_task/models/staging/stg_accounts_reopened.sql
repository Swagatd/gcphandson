
  
    

    create or replace table `silicon-parity-396203`.`retail`.`stg_accounts_reopened`
    
    

    OPTIONS()
    as (
      

WITH source as (
    
    SELECT
        CAST(reopened_ts AS TIMESTAMP) as recorded_at,
        account_id_hashed as account_id
    FROM
      `silicon-parity-396203`.`retail`.`account_reopened`
    
    

)

SELECT * FROM source
    );
  