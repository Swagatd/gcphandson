{{ config(
    materialized="view",
    tags=['retail']
)}}

SELECT
    {{ dbt_utils.generate_surrogate_key(["account_transactions.date", "account_transactions.account_id_hashed", "account_transactions.transactions_num"]) }} AS unique_key,
    CAST(account_transactions.date AS TIMESTAMP) as recorded_at,
    account_transactions.account_id_hashed as account_natural_key,
    account_transactions.transactions_num,
    dim_accounts.surrogate_key as account_surrogate_key,
    dim_users.surrogate_key as user_surrogate_key
FROM
    {{ source('retail', 'account_transactions') }}
    INNER JOIN {{ ref('dim_accounts') }} ON dim_accounts.natural_key = account_transactions.account_id_hashed
        AND dim_accounts.valid_from <= CAST(account_transactions.date AS TIMESTAMP)
        AND dim_accounts.valid_to > CAST(account_transactions.date AS TIMESTAMP)
    INNER JOIN {{ ref('dim_users') }} ON dim_users.natural_key = dim_accounts.user_id
        AND dim_users.valid_from <= CAST(account_transactions.date AS TIMESTAMP)
        AND dim_users.valid_to > CAST(account_transactions.date AS TIMESTAMP)