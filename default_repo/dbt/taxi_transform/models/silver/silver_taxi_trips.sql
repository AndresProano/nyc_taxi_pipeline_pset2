{{
    config(
        materialized='view',
        schema='silver'
    )
}}

WITH source_data AS (
    SELECT * FROM {{ source('nyc_taxi', 'raw_trips') }}
),

deduplicated_data AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                vendorid, 
                COALESCE(tpep_pickup_datetime, lpep_pickup_datetime),
                pulocationid,
                dolocationid,
                trip_distance,
                total_amount
            ORDER BY COALESCE(tpep_pickup_datetime, lpep_pickup_datetime)
        ) as rn
    FROM source_data 
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime', 'lpep_pickup_datetime', 'pulocationid', 'trip_distance', 'dolocationid', 'fare_amount', 'total_amount']) }} as trip_id,

    service_type,

    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) as pickup_datetime,
    COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) as dropoff_datetime,

    passenger_count,
    trip_distance,
    ratecodeid,
    store_and_fwd_flag,
    pulocationid,
    dolocationid,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    
    ingest_ts,
    source_month

FROM deduplicated_data 
WHERE
    rn = 1 
    AND 
    COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) IS NOT NULL 
    AND COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime) IS NOT NULL
    AND COALESCE(tpep_pickup_datetime, lpep_pickup_datetime) <= COALESCE(tpep_dropoff_datetime, lpep_dropoff_datetime)
    AND trip_distance >= 0
    AND total_amount >= 0