

WITH source as (
    
    SELECT
        CAST(created_ts AS TIMESTAMP) as recorded_at,
        account_type,
        account_id_hashed as account_id,
        user_id_hashed as user_id
    FROM
      `silicon-parity-396203`.`monzo`.`account_created`
    
    

  -- this filter will only be applied on an incremental run
    WHERE created_ts > (SELECT max(created_ts) FROM `silicon-parity-396203`.`monzo`.`stg_accounts_created`)

    

)

SELECT * FROM source