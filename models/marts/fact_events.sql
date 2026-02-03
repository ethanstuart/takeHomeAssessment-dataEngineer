{{
    config(
        materialized='table'
    )
}}

{#
    Task 2: Data Modeling - Fact Table

    Goal: Create the central fact table for event analytics

    Requirements:
        - Reference staging model (stg_events)
        - Include foreign keys to dimension tables
        - Include relevant measures/metrics
        - Support time-series analysis

    Consider including:
        - event_id (degenerate dimension)
        - Foreign keys: device_id, user_id
        - event_timestamp
        - event_date (for partitioning/filtering)
        - event_type
        - Measures from payload (confidence scores, durations, etc.)

    Document in README:
        - Why you chose this structure
        - What business questions it supports
        - How it would scale as data grows
#}

-- YOUR CODE HERE

select
    1 as placeholder  -- Remove this and implement your fact table
