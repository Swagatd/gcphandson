

WITH spine AS (

  SELECT 
    CAST(day AS TIMESTAMP) as day
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(DATE('2019-01-14'), DATE('2020-01-01'), INTERVAL 1 DAY)) as day

),

users_daily AS (

  SELECT
    spine.day,
    dim_users.natural_key
  FROM
    spine
    INNER JOIN `silicon-parity-396203`.`retail`.`dim_users` ON dim_users.valid_from <= spine.day
      AND dim_users.valid_to > spine.day
      AND dim_users.open_account_total > 0

),

users_7d AS (

  SELECT
    spine.day as period_start,
    COUNT(DISTINCT users_daily.natural_key) as total_users
  FROM
    spine
    LEFT JOIN users_daily ON users_daily.day >= spine.day
      AND users_daily.day < DATE_ADD(spine.day, INTERVAL 7 DAY)
  GROUP BY
    1

),

transactions_7d AS (

  SELECT
    spine.day as period_start,
    COUNT(fct_transactions.unique_key) as total_transactions,
    COUNT(DISTINCT dim_users.natural_key) as active_users
  FROM
    spine
    LEFT JOIN `silicon-parity-396203`.`retail`.`fct_transactions` ON fct_transactions.recorded_at >= spine.day
      AND fct_transactions.recorded_at < DATE_ADD(spine.day, INTERVAL 7 DAY)
    LEFT JOIN `silicon-parity-396203`.`retail`.`dim_users` ON dim_users.surrogate_key = fct_transactions.user_surrogate_key
  GROUP BY
    1

)

SELECT
  spine.day as period_start,
  DATE_ADD(spine.day, INTERVAL 6 DAY) as period_end,
  users_7d.total_users,
  transactions_7d.active_users,
  transactions_7d.total_transactions,
  ROUND(CAST(active_users/total_users AS DECIMAL), 2) as active_users_7d
FROM
  spine
  LEFT JOIN users_7d ON users_7d.period_start = spine.day
  LEFT JOIN transactions_7d ON transactions_7d.period_start = spine.day
ORDER BY
  1