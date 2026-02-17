{{
    config(
        materialized='view', 
        schema='silver'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('nyc_taxi', 'taxi_zones') }}
)

SELECT
    locationid AS zone_id,
    borough,
    _zone,
    service_zone,
    ingest_ts
FROM source