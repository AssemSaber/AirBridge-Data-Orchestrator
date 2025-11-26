{% macro to_lower(column_name) %}
    lower({{ column_name }})
{% endmacro %}
