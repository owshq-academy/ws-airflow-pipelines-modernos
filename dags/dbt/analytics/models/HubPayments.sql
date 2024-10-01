{{ config(
    tags=['stage'],
    materialized='table'
) }}

WITH HubPayments AS (
    SELECT
        uuid,
        order_id,
        cpf,
        amount,
        status,
        payments.payment_method AS method,
        payment_date AS timestamp_ts,
        CURRENT_TIMESTAMP AS load_ts
    FROM {{ source('OwsHQ', 'payments') }}
)
SELECT * FROM HubPayments