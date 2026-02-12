{#
    Task 3: Analytics Queries

    Goal: Write SQL to answer 2-3 of the following business questions

    Choose from:
        1. Daily active devices
        2. Events per device per day
        3. Top event types in the last 7 days
        4. Average events per user by device type
        5. Event count trends over time

    Focus on:
        - Correct joins between fact and dimension tables
        - Readability (use CTEs, meaningful aliases)
        - Performance considerations (mention indexes that would help)

    Instructions:
        - Write each query below with a comment explaining:
            * What question it answers
            * Business value
            * Performance notes (what indexes would help)
        - You can run these with: dbt compile --select analytics_queries
          Then execute the compiled SQL in your database

    Note: Analyses are SQL files that dbt compiles but doesn't execute.
    They're useful for ad-hoc queries that reference your models.
#}


-- =============================================================================
-- Query 1: [Choose a question from the list above]
-- =============================================================================
-- Question: events per device per day
-- Business value: activity for customers
-- Performance notes: could be slow without filtering if there's a lot of data (filter on date later on)
-- =============================================================================

select e.event_date, d.device_id, count(e.*) as events
   from main_marts.dim_devices as d
      left join main_marts.fact_events as e on e.device_id = d.device_id
   group by e.event_date, d.device_id
   order by event_date asc;




-- =============================================================================
-- Query 2: [Choose a question from the list above]
-- =============================================================================
-- Question: Top event types in the last 7 days
-- Business value: Customer behavior, usage for product/marketing ideas
-- Performance notes: there's nothing here because the data is old
-- =============================================================================

select event_type, count(*) as events
   from main_marts.fact_events
   where event_timestamp >= current_timestamp - interval '7 days'
   group by event_type
   order by count(*) desc;



-- =============================================================================
-- Query 3 (Optional): [Choose a question from the list above]
-- =============================================================================
-- Question:
-- Business value:
-- Performance notes:
-- =============================================================================

-- YOUR QUERY HERE
