-- Enrich and deduplicate trip data
-- Demonstrates enrichment and surrogate key generation
-- Note: Data quality analysis available in analyses/trips_data_quality.sql

WITH unioned AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
),

payment_types AS (
    SELECT * FROM {{ ref('payment_type_lookup') }}
),

cleaned_and_enriched AS (
    SELECT
        -- Generate unique trip identifier (surrogate key pattern)
        {{ dbt_utils.generate_surrogate_key(['u.vendor_id', 'u.pickup_datetime', 'u.pickup_location_id', 'u.service_type']) }} as trip_id,

        -- Identifiers
        u.vendor_id,
        u.service_type,
        u.rate_code_id,

        -- Location IDs
        u.pickup_location_id,
        u.dropoff_location_id,

        -- Trip timing
        u.pickup_datetime,
        u.dropoff_datetime,
        u.store_and_fwd_flag,

        -- Trip details
        u.passenger_count,
        u.trip_distance,
        u.trip_type,

        -- Payment breakdown
        u.fare_amount,
        u.extra,
        u.mta_tax,
        u.tip_amount,
        u.tolls_amount,
        u.ehail_fee,
        u.improvement_surcharge,
        u.total_amount,

        -- Enrich with payment type description
        coalesce(u.payment_type, 0) as payment_type,
        coalesce(pt.description, 'Unknown') as payment_type_description

    FROM unioned u
    LEFT JOIN payment_types pt
        on coalesce(u.payment_type, 0) = pt.payment_type
)

SELECT * FROM cleaned_and_enriched

-- Deduplicate: if multiple trips match (same vendor, second, location, service), keep first
qualify row_number() over(
    partition by vendor_id, pickup_datetime, pickup_location_id, service_type
    order by dropoff_datetime
) = 1