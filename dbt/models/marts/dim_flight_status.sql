{{config(
    unique_key='status_id'
)}}
with distinct_values as(
    select distinct CANCELLATION_REASON,is_cancelled,is_diverted
    from {{ ref('stg_flight_status') }}
)
select 
row_number() over(order by is_cancelled) as status_id,
CANCELLATION_REASON,
is_cancelled,
is_diverted
from distinct_values