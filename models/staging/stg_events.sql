{{
    config(
        materialized='view'
    )
}}


with source_data as (

    select * from {{ source('raw', 'raw_events') }}

),
normalize_ts as (
   select
        cast(event_id as varchar) as event_id,   -- I don't love this approach. I'd probably leave it NULL
        cast(event_type as varchar) as event_type,
        cast(device_id as varchar) as device_id,
        lower(cast(user_id as varchar)) as user_id,
        coalesce(
           try_strptime("timestamp", '%Y-%m-%dT%H:%M:%SZ')  AT TIME ZONE 'UTC',
           try_strptime("timestamp", '%Y-%m-%d %H:%M:%S')  AT TIME ZONE 'UTC',
           try_strptime("timestamp", '%Y-%m-%dT%H:%M:%S%z')  AT TIME ZONE 'UTC',
           try_strptime(replace("timestamp", ' EST', ''), '%m/%d/%Y %I:%M:%S %p') + interval '5 hours',
           try_strptime(timestamp, '%b %d, %Y %I:%M:%S %p') AT TIME ZONE 'UTC',
           try_strptime(timestamp, '%Y/%m/%d %H:%M:%S') AT TIME ZONE 'UTC',
           try_strptime(timestamp, '%d-%m-%Y %H:%M:%S') AT TIME ZONE 'UTC',
           case when regexp_matches(timestamp, '^\d{9,10}$')
                then to_timestamp(cast(timestamp as bigint))
                else null
           end
        ) as event_timestamp,
        cast(payload as varchar) as payload,
        cast("location" as varchar) as location,
        cast(device_metadata as varchar) as device_metadata,
        --breaking out individual json objects to their own columns for later use
        cast(payload ->> 'confidence' as numeric) as confidence,
        cast(payload ->> 'zone' as varchar) as zone,
        cast(payload ->> 'firmware_version' as varchar) as firmware_version,
        cast(payload ->> 'object_type' as varchar) as object_type,
        cast(payload ->> 'alert_type' as varchar) as alert_type,
        cast(payload ->> 'severity' as varchar) as severity,
        cast(payload ->> 'ip_address' as varchar) as ip_address,
        cast(payload ->> 'platform' as varchar) as platform,
        cast(payload ->> 'duration_seconds' as varchar) as duration_seconds,
        cast(payload ->> 'reason' as varchar) as reason,
        cast(payload ->> 'setting' as varchar) as setting,
        cast(payload ->> 'old_value' as varchar) as old_value,
        cast(payload ->> 'new_value' as varchar) as new_value,
        cast(payload ->> 'resolution' as varchar) as resolution,
        cast(payload ->> 'session_duration_minutes' as varchar) as session_duration_minutes,              --maybe convert this and combine with other duration col?
        cast(payload ->> 'from_version' as varchar) as from_version,
        cast(payload ->> 'to_version' as varchar) as to_version,
        cast(payload ->> 'update_status' as varchar) as update_status,
        cast(payload ->> 'alert_id' as varchar) as alert_id,
        cast(payload ->> 'dismiss_reason' as varchar) as dismiss_reason   ,
        cast(location ->> 'lat' as numeric) as lat,
        cast(location ->> 'lng' as numeric) as lng,
        cast(location ->> 'city' as varchar) as city,
        cast(device_metadata ->> 'model' as varchar) as model,
        cast(device_metadata ->> 'install_date' as varchar) as install_date
        from source_data
),
dedupe as (
   select *, row_number() over (partition by event_id order by event_timestamp desc) as row_num from normalize_ts     -- adding a row num to dedupe later
)
, final as (
   select event_id, event_type, device_id, user_id, event_timestamp, payload, location, device_metadata, confidence,
          zone, firmware_version, object_type, alert_type, severity, ip_address, platform, duration_seconds, reason,
          setting, old_value, new_value, resolution, session_duration_minutes, from_version, to_version, update_status,
          alert_id, dismiss_reason, lat, lng, city, model, install_date
   from dedupe    --everything but throwaway row_num column
      where row_num = 1             --getting most recent row per event_id
         and event_id is not null       --@assumption
         and event_type <> 'invalid_event'    -- assumption
)
select * from final


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


