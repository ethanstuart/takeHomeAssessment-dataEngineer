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
-- Question:
-- Business value:
-- Performance notes:
-- =============================================================================

-- YOUR QUERY HERE
-- Example: select * from {{ ref('fact_events') }} limit 10



-- =============================================================================
-- Query 2: [Choose a question from the list above]
-- =============================================================================
-- Question:
-- Business value:
-- Performance notes:
-- =============================================================================

-- YOUR QUERY HERE



-- =============================================================================
-- Query 3 (Optional): [Choose a question from the list above]
-- =============================================================================
-- Question:
-- Business value:
-- Performance notes:
-- =============================================================================

-- YOUR QUERY HERE
