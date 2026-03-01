SELECT
    review_id,
    order_id,
    CAST(review_score AS INT) AS review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
FROM {{ source('silver', 'order_reviews') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_id) = 1
