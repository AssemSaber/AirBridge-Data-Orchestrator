{% macro cancel_type(column_name) %}
    {{ return(
        "CASE " ~ column_name ~ 
        " WHEN 'A' THEN 'Carrier' " ~
        " WHEN 'B' THEN 'Weather' " ~
        " WHEN 'C' THEN 'National Air System' " ~
        " WHEN 'D' THEN 'Security' " ~
        " ELSE 'still available' END"
    ) }}
{% endmacro %}
