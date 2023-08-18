

WITH source as (
    
    SELECT
        CAST(closed_ts AS TIMESTAMP) as recorded_at,
        account_id_hashed as account_id
    FROM
      `silicon-parity-396203`.`retail`.`account_closed`
    
    

)

SELECT * FROM source