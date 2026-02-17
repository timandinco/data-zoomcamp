WITH trips_unioned AS (
    SELECT * FROM {{ ref('int_trips_unioned') }}
), 

vendors AS (
    SELECT DISTINCT vendor_id,
    {{ get_vendor_names('vendor_id') }} as vendor_name
    FROM trips_unioned
)

SELECT * FROM vendors