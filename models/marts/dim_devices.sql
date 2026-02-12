{{
    config(
        materialized='table'
    )
}}

{#
    Task 2: Data Modeling - Device Dimension

    Goal: Create a dimension table for devices

    Consider including:
        - device_id (natural key)
        - First seen / last seen timestamps
        - Associated user_id (owner)
        - Device metadata if available
        - Any derived attributes

    Document in README:
        - Why you chose these columns
        - What business questions this supports
#}

-- YOUR CODE HERE

select e.device_id,
       min(event_timestamp) as first_seen_timestamp,
       max(event_timestamp) as last_seen_timestamp,
       first(e.user_id) filter (where e.user_id is not null) as owner_user_id,
      last(e.lat) filter (where e.lat is not null) as last_latitude,
      last(e.lng) filter (where e.lng is not null) as last_longitude,
      first(e.model) filter (where e.model is not null) as device_model,
      first(e.install_date) filter (where e.install_date is not null) as first_install_date,
      first(e.city) filter (where e.city is not null) as install_city
   from {{ ref('stg_events') }} as e
   where e.device_id is not null
   group by 1
