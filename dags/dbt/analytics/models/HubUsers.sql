{{ config(
    tags=['stage'],
    materialized='table'
) }}

WITH HubUsers AS (
    SELECT
        uuid,
        CONCAT(' ', first_name, last_name) AS name,
        CAST(date_birth AS DATE) AS date_birth,
        cpf,
        company_name,
        phone_number,
        city,
        country,
        CURRENT_TIMESTAMP AS load_ts
    FROM {{ source('OwsHQ', 'users') }}
)
SELECT * FROM HubUsers