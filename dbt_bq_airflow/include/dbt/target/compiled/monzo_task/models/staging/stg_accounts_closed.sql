

WITH source as (
    
    SELECT
        CAST(closed_ts AS TIMESTAMP) as recorded_at,
        account_id_hashed as account_id
    FROM
      `silicon-parity-396203`.`monzo`.`account_closed`
    
    

  -- this filter will only be applied on an incremental run
    WHERE closed_ts > (SELECT max(closed_ts) FROM `silicon-parity-396203`.`monzo`.`stg_accounts_closed`)

    

)

SELECT * FROM source