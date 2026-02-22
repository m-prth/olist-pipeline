SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng
FROM {{ source('silver', 'geolocation') }}
