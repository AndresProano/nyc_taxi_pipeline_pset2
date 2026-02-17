{{
    config(
        materialized='incremental', 
        schema='gold',
        unique_key='trip_id'
    )
}}

WITH silver_data AS (
    SELECT * FROM {{ ref('silver_taxi_trips') }}
)

SELECT
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    pulocationid AS pickup_zone_key,
    dolocationid AS dropoff_zone_key,
    service_type,
    payment_type AS payment_type_id,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    ingest_ts,
    source_month

FROM silver_data
WHERE pickup_datetime IS NOT NULL