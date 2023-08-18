

  create or replace view `silicon-parity-396203`.`monzo`.`dim_users`
  OPTIONS()
  as 

WITH account_meta AS (

    SELECT
        `account_type`,
  `account_id`,
  `user_id`
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_created`

),

spine AS (

    SELECT DISTINCT
        user_id,
        recorded_at
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_created`

    UNION ALL

    SELECT DISTINCT
        account_meta.user_id,
        stg_accounts_closed.recorded_at
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_closed`
        INNER JOIN account_meta ON account_meta.account_id = stg_accounts_closed.account_id

    UNION ALL

    SELECT DISTINCT
        account_meta.user_id,
        stg_accounts_reopened.recorded_at
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_reopened`
        INNER JOIN account_meta ON account_meta.account_id = stg_accounts_reopened.account_id
    
),

first_created AS (

    SELECT
        user_id,
        MIN(recorded_at) as recorded_at
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_created`
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
        LEFT JOIN `silicon-parity-396203`.`monzo`.`dim_accounts` ON dim_accounts.user_id = spine.user_id
            AND dim_accounts.valid_from <= spine.recorded_at
            AND dim_accounts.valid_to > spine.recorded_at
            AND dim_accounts.is_open
    GROUP BY
        1,2

)

SELECT
    to_hex(md5(cast(coalesce(cast(aggregates.user_id as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(aggregates.recorded_at as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS surrogate_key,
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
    valid_from;

