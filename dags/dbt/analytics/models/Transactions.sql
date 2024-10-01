{{ config(
    tags=['warehouse'],
    materialized='table'
) }}

WITH HubUsers AS
(
    SELECT *
    FROM {{ ref('HubUsers') }}
),
    HubPayments AS
(
    SELECT *
    FROM {{ ref('HubPayments') }}
)
SELECT u.name,
       u.cpf,
       u.country,
       u.load_ts AS users_ts,
       p.order_id,
       p.amount,
       p.status,
       p.method,
       p.load_ts AS payments_ts
FROM HubUsers AS u
INNER JOIN HubPayments AS p
ON u.cpf = p.cpf