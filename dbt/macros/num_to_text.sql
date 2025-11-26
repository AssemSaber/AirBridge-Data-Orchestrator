{% macro num_to_txt(column_name) %}
    {{ return(
        "CASE " ~ column_name ~ 
        " WHEN 0 THEN 'No' " ~
        " WHEN 1 THEN 'Yes' " ~
        "END"

    ) }}
{% endmacro %}
