

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
        account_id,
        recorded_at,
        CAST('true' AS BOOLEAN) as is_open
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_created`

    UNION ALL

    SELECT DISTINCT
        account_id,
        recorded_at,
        CAST('false' AS BOOLEAN) as is_open
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_closed`

    UNION ALL

    SELECT DISTINCT
        account_id,
        recorded_at,
        CAST('true' AS BOOLEAN) as is_open
    FROM
        `silicon-parity-396203`.`monzo`.`stg_accounts_reopened`
    
)

SELECT
    to_hex(md5(cast(coalesce(cast(spine.account_id as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(spine.recorded_at as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS surrogate_key,
    spine.account_id as natural_key,
    spine.is_open,
    account_meta.`account_type`,
  account_meta.`user_id`,
    spine.recorded_at as valid_from,
    COALESCE(LEAD(spine.recorded_at) OVER (PARTITION BY spine.account_id ORDER BY spine.recorded_at ASC), CURRENT_TIMESTAMP()) AS valid_to
FROM
    spine
    INNER JOIN account_meta ON account_meta.account_id = spine.account_id
ORDER BY
    natural_key,
    valid_from