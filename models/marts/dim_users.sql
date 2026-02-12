{{
    config(
        materialized='table'
    )
}}

{#
    Task 2: Data Modeling - User Dimension (Optional)

    Goal: Create a dimension table for users

    Consider including:
        - user_id (natural key)
        - First activity timestamp
        - Last activity timestamp
        - Device count
        - Preferred platform (from login events)

    This dimension is optional but recommended.
#}

-- YOUR CODE HERE

select e.user_id,
       first(e.platform) filter (where e.platform is not null) as platform_preference,
       min(e.event_timestamp) as first_activity_timestamp,
       max(e.event_timestamp) as last_activity_timestamp,
       count(distinct e.device_id) as unique_devices
   from {{ ref('stg_events') }} as e
   where user_id is not null
   group by 1

