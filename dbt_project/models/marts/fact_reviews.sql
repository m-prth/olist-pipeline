SELECT
    r.review_id,
    r.order_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    date_diff('hour', r.review_creation_date, r.review_answer_timestamp) AS response_time_hours
FROM {{ ref('stg_reviews') }} r
