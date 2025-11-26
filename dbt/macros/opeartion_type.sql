{% macro operation_type(column_name) %}
    {{ return(
        "CASE " ~ column_name ~
        " WHEN 'c' THEN 'Create' " ~
        " WHEN 'u' THEN 'Update' " ~
        " WHEN 'd' THEN 'Delete' " ~
        " WHEN 'r' THEN 'Snapshot' " ~
        " ELSE 'Unknown' END"
    ) }}
{% endmacro %}
