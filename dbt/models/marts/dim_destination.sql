{{config(
    unique_key='destination_id'
)}}

{{ new_records('destination_id','destination_city','destination_state') }}