WITH geo_centroid AS (
    SELECT
        geolocation_zip_code_prefix AS zip_code_prefix,
        AVG(geolocation_lat) AS latitude,
        AVG(geolocation_lng) AS longitude
    FROM {{ ref('stg_geolocation') }}
    GROUP BY geolocation_zip_code_prefix
)

SELECT
    zip_code_prefix,
    ROUND(latitude, 6) AS latitude,
    ROUND(longitude, 6) AS longitude
FROM geo_centroid
