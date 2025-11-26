{% test is_positive(model,column_name) %}
    select count(*) as records 
    from {{model}}
    where {{column_name}}< 0
    having count(*)>0
{% endtest %}