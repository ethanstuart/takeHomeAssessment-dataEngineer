{{
    config(
        materialized='view'
    )
}}

{#
    Task 1: Data Ingestion & Cleaning

    Goal: Transform raw events into a clean, standardized format

    Requirements:
        1. Normalize timestamps to a consistent format (UTC)
        2. Deduplicate events (hint: check event_id)
        3. Handle missing or invalid fields
        4. Standardize case (e.g., user_id)
        5. Filter out invalid records

    Known data issues to handle:
        - Duplicate event_ids (evt_001, evt_016 appear twice)
        - Multiple timestamp formats:
            * ISO 8601: "2024-01-15T10:30:00Z"
            * ISO with offset: "2024-01-15T08:00:00-05:00"
            * Space-separated: "2024-01-15 10:25:00"
            * US format: "01/15/2024 11:00:00 AM EST"
            * Unix epoch: "1705320000"
            * Human readable: "Jan 16, 2024 10:15:30 AM"
            * European: "17-01-2024 20:15:00"
        - Empty event_id (one record)
        - Null device_id (one record)
        - Case mismatch: "USR_100" vs "usr_100"
        - Invalid timestamp: "not_a_timestamp"

    Considerations:
        - Idempotency: dbt handles this via materialization
        - Document your assumptions in README
        - Consider: should invalid records be filtered or flagged?

    Hint: You may want to use CTEs to break this into steps:
        1. source_data - reference the raw source
        2. normalized - handle timestamp parsing
        3. deduplicated - remove duplicates
        4. final - apply remaining transformations
#}

with source_data as (

    select * from {{ source('raw', 'raw_events') }}

),

-- YOUR CODE HERE: Add transformation CTEs

final as (

    select
        -- YOUR CODE HERE: Define clean output columns
        -- event_id,
        -- event_type,
        -- device_id,
        -- user_id,
        -- event_timestamp,  -- normalized to UTC
        -- payload
        *
    from source_data

)

select * from final
