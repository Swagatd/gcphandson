

WITH source as (
    
    SELECT
        CAST(reopened_ts AS TIMESTAMP) as recorded_at,
        account_id_hashed as account_id
    FROM
      `silicon-parity-396203`.`monzo`.`account_reopened`
    
    

  -- this filter will only be applied on an incremental run
    WHERE reopened_ts > (SELECT max(reopened_ts) FROM `silicon-parity-396203`.`monzo`.`stg_accounts_reopened`)

    

)

SELECT * FROM source