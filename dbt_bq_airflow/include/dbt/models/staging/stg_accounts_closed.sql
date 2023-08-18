{{
    config(
        materialized='incremental',
        tags=['retail', 'staging']
    )
}}

WITH source as (
    
    SELECT
        CAST(closed_ts AS TIMESTAMP) as recorded_at,
        account_id_hashed as account_id
    FROM
      {{ source('retail', 'account_closed') }}
    
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    WHERE closed_ts > (SELECT max(closed_ts) FROM {{ this }})

    {% endif %}

)

SELECT * FROM source
