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

select
    1 as placeholder  -- Remove this and implement your dimension
