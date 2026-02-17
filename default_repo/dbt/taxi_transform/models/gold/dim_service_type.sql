{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='service_type',
        on_schema_change='ignore'
    )
}}

SELECT DISTINCT
    service_type::varchar(50) as service_type,
    CURRENT_TIMESTAMP as last_updated
FROM {{ ref('silver_taxi_trips') }}
WHERE service_type IS NOT NULL