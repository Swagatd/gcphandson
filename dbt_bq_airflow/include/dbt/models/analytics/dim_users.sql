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
        user_id,
        recorded_at
    FROM
        {{ ref("stg_accounts_created") }}

    UNION ALL

    SELECT DISTINCT
        account_meta.user_id,
        stg_accounts_closed.recorded_at
    FROM
        {{ ref("stg_accounts_closed") }}
        INNER JOIN account_meta ON account_meta.account_id = stg_accounts_closed.account_id

    UNION ALL

    SELECT DISTINCT
        account_meta.user_id,
        stg_accounts_reopened.recorded_at
    FROM
        {{ ref("stg_accounts_reopened") }}
        INNER JOIN account_meta ON account_meta.account_id = stg_accounts_reopened.account_id
    
),

first_created AS (

    SELECT
        user_id,
        MIN(recorded_at) as recorded_at
    FROM
        {{ ref("stg_accounts_created") }}
    GROUP BY
        1

),

aggregates AS (

    SELECT
        spine.user_id,
        spine.recorded_at,
        COUNT(DISTINCT dim_accounts.natural_key) AS open_account_total,
        STRING_AGG(DISTINCT dim_accounts.natural_key) as open_accounts,
        STRING_AGG(DISTINCT dim_accounts.account_type) as open_account_types
    FROM
        spine
        LEFT JOIN {{ ref('dim_accounts') }} ON dim_accounts.user_id = spine.user_id
            AND dim_accounts.valid_from <= spine.recorded_at
            AND dim_accounts.valid_to > spine.recorded_at
            AND dim_accounts.is_open
    GROUP BY
        1,2

)

SELECT
    {{ dbt_utils.generate_surrogate_key(["aggregates.user_id", "aggregates.recorded_at"]) }} AS surrogate_key,
    aggregates.user_id as natural_key,
    first_created.recorded_at as first_created,
    aggregates.open_account_total,
    aggregates.open_accounts,
    aggregates.open_account_types,
    aggregates.recorded_at as valid_from,
    COALESCE(LEAD(aggregates.recorded_at) OVER (PARTITION BY aggregates.user_id ORDER BY aggregates.recorded_at ASC), CURRENT_TIMESTAMP()) AS valid_to
FROM
    aggregates
    LEFT JOIN first_created ON first_created.user_id = aggregates.user_id
ORDER BY
    natural_key,
    valid_from