WITH taxi_zone_lookup as (
    SELECT *
    FROM {{ ref('taxi_zone_lookup') }}
),

renamed_zones as (
    SELECT
        LocationID as location_id,
        borough as borough,
        zone as zone,
        service_zone
    FROM taxi_zone_lookup
)

SELECT * FROM renamed_zones