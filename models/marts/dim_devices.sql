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

select
    1 as placeholder  -- Remove this and implement your dimension
