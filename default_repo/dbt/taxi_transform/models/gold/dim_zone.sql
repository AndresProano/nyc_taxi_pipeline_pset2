{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='zone_key'
    )
}}

SELECT
    zone_id as zone_key,
    borough,
    _zone as zone,
    service_zone
FROM {{ ref('silver_taxi_zones') }}