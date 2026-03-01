{% test not_empty(model) %}
SELECT 1
FROM {{ model }}
HAVING COUNT(*) = 0
{% endtest %}
