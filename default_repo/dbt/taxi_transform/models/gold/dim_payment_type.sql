{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['payment_type_group', 'payment_type_id'],     
        on_schema_change='ignore'
    )
}}

SELECT DISTINCT
    payment_type::integer as payment_type_id,
    CAST(CASE 
        WHEN payment_type = 1 THEN 'Credit card'
        WHEN payment_type = 2 THEN 'Cash'
        WHEN payment_type = 3 THEN 'No charge'
        WHEN payment_type = 4 THEN 'Dispute'
        WHEN payment_type = 5 THEN 'Unknown'
        WHEN payment_type = 6 THEN 'Voided trip'
        ELSE 'Empty'
    END as VARCHAR(50)) as payment_type_desc,
    CAST(CASE 
        WHEN payment_type = 1 THEN 'card'
        WHEN payment_type = 2 THEN 'cash'
        ELSE 'other'
    END AS VARCHAR(50)) as payment_type_group
FROM {{ ref('silver_taxi_trips') }}
WHERE payment_type IS NOT NULL