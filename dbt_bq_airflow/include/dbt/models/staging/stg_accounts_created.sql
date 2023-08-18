{{
    config(
        materialized='incremental',
        tags=['retail', 'staging']
    )
}}

WITH source as (
    
    SELECT
        CAST(created_ts AS TIMESTAMP) as recorded_at,
        account_type,
        account_id_hashed as account_id,
        user_id_hashed as user_id
    FROM
      {{ source('retail', 'account_created') }}
    
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    WHERE created_ts > (SELECT max(created_ts) FROM {{ this }})

    {% endif %}

)

SELECT * FROM source
