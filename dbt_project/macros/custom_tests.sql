-- macros/custom_tests.sql
{% test custom_positive_total_price(model, column_name) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} IS NULL OR {{ column_name }} <= 0
{% endtest %}

{% test recent_date(model, column_name, days=365) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < DATE_SUB(CURRENT_DATE(), INTERVAL {{ days }} DAY)
{% endtest %}

{% test greater_than_or_equal_to(model, column_name, value) %}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} < {{ value }}
{% endtest %}