{{
    config(
        materialized='incremental',
        tags=['retail', 'staging']
    )
}}

WITH source as (
    
    SELECT
        CAST(reopened_ts AS TIMESTAMP) as recorded_at,
        account_id_hashed as account_id
    FROM
      {{ source('retail', 'account_reopened') }}
    
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    WHERE reopened_ts > (SELECT max(reopened_ts) FROM {{ this }})

    {% endif %}

)

SELECT * FROM source
