SELECT
    p.order_id,
    p.payment_sequential,
    p.payment_type,
    p.payment_installments AS installments,
    p.payment_value
FROM {{ ref('stg_payments') }} p
