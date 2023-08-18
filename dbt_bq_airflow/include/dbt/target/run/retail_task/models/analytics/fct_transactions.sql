

  create or replace view `silicon-parity-396203`.`retail`.`fct_transactions`
  OPTIONS()
  as 

SELECT
    to_hex(md5(cast(coalesce(cast(account_transactions.date as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(account_transactions.account_id_hashed as STRING), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(account_transactions.transactions_num as STRING), '_dbt_utils_surrogate_key_null_') as STRING))) AS unique_key,
    CAST(account_transactions.date AS TIMESTAMP) as recorded_at,
    account_transactions.account_id_hashed as account_natural_key,
    account_transactions.transactions_num,
    dim_accounts.surrogate_key as account_surrogate_key,
    dim_users.surrogate_key as user_surrogate_key
FROM
    `silicon-parity-396203`.`retail`.`account_transactions`
    INNER JOIN `silicon-parity-396203`.`retail`.`dim_accounts` ON dim_accounts.natural_key = account_transactions.account_id_hashed
        AND dim_accounts.valid_from <= CAST(account_transactions.date AS TIMESTAMP)
        AND dim_accounts.valid_to > CAST(account_transactions.date AS TIMESTAMP)
    INNER JOIN `silicon-parity-396203`.`retail`.`dim_users` ON dim_users.natural_key = dim_accounts.user_id
        AND dim_users.valid_from <= CAST(account_transactions.date AS TIMESTAMP)
        AND dim_users.valid_to > CAST(account_transactions.date AS TIMESTAMP);

