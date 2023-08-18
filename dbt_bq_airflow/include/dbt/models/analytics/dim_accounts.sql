{{ config(
    materialized="view",
    tags=['retail']
)}}

WITH account_meta AS (

    SELECT
        {{ dbt_utils.star(ref("stg_accounts_created"), except=["recorded_at"]) }}
    FROM
        {{ ref("stg_accounts_created") }}

),

spine AS (

    SELECT DISTINCT
        account_id,
        recorded_at,
        CAST('true' AS BOOLEAN) as is_open
    FROM
        {{ ref("stg_accounts_created") }}

    UNION ALL

    SELECT DISTINCT
        account_id,
        recorded_at,
        CAST('false' AS BOOLEAN) as is_open
    FROM
        {{ ref("stg_accounts_closed") }}

    UNION ALL

    SELECT DISTINCT
        account_id,
        recorded_at,
        CAST('true' AS BOOLEAN) as is_open
    FROM
        {{ ref("stg_accounts_reopened") }}
    
)

SELECT
    {{ dbt_utils.generate_surrogate_key(["spine.account_id", "spine.recorded_at"]) }} AS surrogate_key,
    spine.account_id as natural_key,
    spine.is_open,
    {{ dbt_utils.star(ref("stg_accounts_created"), relation_alias='account_meta', except=["recorded_at", "account_id"]) }},
    spine.recorded_at as valid_from,
    COALESCE(LEAD(spine.recorded_at) OVER (PARTITION BY spine.account_id ORDER BY spine.recorded_at ASC), CURRENT_TIMESTAMP()) AS valid_to
FROM
    spine
    INNER JOIN account_meta ON account_meta.account_id = spine.account_id
ORDER BY
    natural_key,
    valid_from


