WITH date_range AS (
    SELECT UNNEST(generate_series(
        DATE '2016-01-01',
        DATE '2018-12-31',
        INTERVAL 1 DAY
    ))::DATE AS date_key
)

SELECT
    date_key,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(QUARTER FROM date_key) AS quarter,
    EXTRACT(DOW FROM date_key) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_range
