{{config(
    unique_key='origin_id'
)}}

{{ new_records('origin_id','origin_city','origin_state') }}