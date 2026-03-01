SELECT
    order_id,
    payment_sequential,
    CASE
        WHEN LOWER(payment_type) = 'credit_card' THEN 'Credit Card'
        WHEN LOWER(payment_type) = 'boleto' THEN 'Boleto'
        WHEN LOWER(payment_type) = 'voucher' THEN 'Voucher'
        WHEN LOWER(payment_type) = 'debit_card' THEN 'Debit Card'
        ELSE UPPER(LEFT(payment_type, 1)) || LOWER(SUBSTR(payment_type, 2))
    END AS payment_type,
    payment_installments,
    CAST(payment_value AS DOUBLE) AS payment_value
FROM {{ source('silver', 'order_payments') }}
