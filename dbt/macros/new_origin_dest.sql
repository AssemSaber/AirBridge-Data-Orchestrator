{% macro new_records(id,city,state) %}
with distinct_values as(
    select
    distinct {{city}} as city,{{state}} as state
    from {{ref('stg_flight_status')}}
)
select row_number() over(order by city) as {{id}},
    city,
    state
    from distinct_values

{% endmacro %}
