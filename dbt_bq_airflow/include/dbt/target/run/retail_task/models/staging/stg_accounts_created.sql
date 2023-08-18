
  
    

    create or replace table `silicon-parity-396203`.`retail`.`stg_accounts_created`
    
    

    OPTIONS()
    as (
      

WITH source as (
    
    SELECT
        CAST(created_ts AS TIMESTAMP) as recorded_at,
        account_type,
        account_id_hashed as account_id,
        user_id_hashed as user_id
    FROM
      `silicon-parity-396203`.`retail`.`account_created`
    
    

)

SELECT * FROM source
    );
  