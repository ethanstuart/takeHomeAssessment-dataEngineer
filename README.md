# Data Engineer Take-Home Assessment

**Company:** SafeStreets
**Level:** Mid / Senior
**Expected Time:** 2–4 hours
**Submission:** GitHub repository or CodeSubmit
**Required Tools:** dbt (Data Build Tool)

---

## Scenario

SafeStreets collects event data from devices and user actions. Events arrive as raw data and must be transformed into analytics-ready tables using **dbt**.

You are given a raw dataset (`seeds/raw_events.csv`) containing:

- Missing fields
- Duplicate records
- Inconsistent timestamps
- Evolving schema (some events have fields others don't)

**Your task is to build a dbt project that cleans this data and creates a dimensional model.**
**Please do not use AI to write code for you in this project. We want to get as much YOU as possible.**
**Please do not spend any more than 4 hours on this project. Focus on senior level work and complete as much as you can in that timeframe.**

---

## Tasks

### Task 1 — Data Ingestion & Cleaning (Required)

**Goal:** Handle messy, real-world data using dbt.

Implement the staging model `models/staging/stg_events.sql` to:

1. Normalize timestamps to a consistent UTC format
2. Deduplicate events (hint: check `event_id`)
3. Handle missing or invalid fields
4. Standardize data (e.g., case-sensitive user_ids)
5. Filter or flag invalid records

**Deliverable:** Complete `models/staging/stg_events.sql`

---

### Task 2 — Data Modeling (Required)

**Goal:** Design analytics-ready tables using dimensional modeling.

Implement the mart models in `models/marts/`:
- `fact_events.sql` — Central fact table for event analytics
- `dim_devices.sql` — Device dimension
- `dim_users.sql` — User dimension (optional but recommended)

**Deliverable:** Complete the models in `models/marts/`

---

### Task 3 — Analytics Queries (Required)

**Goal:** Write SQL to answer **2–3** of the following questions.

1. Daily active devices
2. Events per device per day
3. Top event types in the last 7 days
4. Average events per user
5. Event count trends over time

**Focus on:**
- Correct joins between fact and dimension tables
- Readability (use CTEs where appropriate)
- Performance considerations

**Deliverable:** Queries in `analyses/analytics_queries.sql`

---

## Project Structure

```
├── README.md                       # This file (add your answers below)
├── dbt_project.yml                 # dbt project configuration
├── profiles.yml.example            # Example dbt profile (copy to ~/.dbt/)
├── seeds/
│   └── raw_events.csv              # Raw input data (loaded via dbt seed)
├── models/
│   ├── staging/
│   │   ├── _staging__sources.yml   # Source definitions
│   │   ├── _staging__models.yml    # Model documentation & tests
│   │   └── stg_events.sql          # YOUR WORK: Staging model
│   └── marts/
│       ├── _marts__models.yml      # Model documentation & tests
│       ├── fact_events.sql         # YOUR WORK: Fact table
│       ├── dim_devices.sql         # YOUR WORK: Device dimension
│       └── dim_users.sql           # YOUR WORK: User dimension
├── analyses/
│   └── analytics_queries.sql       # YOUR WORK: Analytics queries
├── tests/                          # Custom data tests (optional)
└── macros/                         # Custom macros (optional)
```

---

## Getting Started

### Prerequisites

- Python 3.8+
- dbt-core with a database adapter

**Recommended setup (DuckDB - no external database required):**
```bash
    pip install dbt-core dbt-duckdb
```

**Alternative adapters:**
```bash
    pip install dbt-postgres   # For PostgreSQL
    pip install dbt-sqlite     # For SQLite
```

### Setup

1. Clone this repository

2. Copy the example profile and configure:
   ```bash
   cp profiles.yml.example ~/.dbt/profiles.yml
   ```
   Or set `DBT_PROFILES_DIR` to this directory.

3. Verify dbt can connect:
   ```bash
   dbt debug
   ```

4. Load the seed data:
   ```bash
   dbt seed
   ```

5. Run your models:
   ```bash
   dbt run
   ```

6. Run tests:
   ```bash
   dbt test
   ```

### Useful Commands

```bash
    dbt seed                    # Load raw_events.csv into database
    dbt run                     # Build all models
    dbt run --select staging    # Build only staging models
    dbt run --select marts      # Build only mart models
    dbt test                    # Run all tests
    dbt compile                 # Compile SQL without running (useful for analyses)
    dbt docs generate           # Generate documentation
    dbt docs serve              # View documentation in browser
```

---

## Raw Data Issues

The `seeds/raw_events.csv` contains intentional data quality issues for you to handle:

| Issue | Examples |
|-------|----------|
| Duplicate records | `evt_001` and `evt_016` appear twice |
| Missing fields | Some events missing `device_id` or `user_id` |
| Inconsistent timestamps | ISO 8601, Unix epoch, US format, European format |
| Invalid data | Empty `event_id`, null `device_id`, invalid timestamp |
| Case inconsistency | `usr_100` vs `USR_100` |
| Evolving schema | Some events have `location` or `device_metadata` |

---

## Evaluation Criteria

| Criteria | Weight | What We're Looking For |
|----------|--------|------------------------|
| **Correctness** | 30% | Does the solution work? Are edge cases handled? |
| **Data Modeling** | 25% | Is the schema well-designed for analytics? |
| **dbt Best Practices** | 20% | Proper use of staging/marts, tests, documentation |
| **Code Quality** | 15% | Readable, maintainable SQL and project organization |
| **Production Thinking** | 10% | Monitoring, error handling, scalability considerations |

---

## Your Submission

> **Instructions:** Complete the sections below with your work.

### How to Run

<!-- Provide clear instructions for running your solution -->

 




```bash
dbt seed
dbt run
dbt test
```
the queries in analytics_queries.sql will need to be run somewhere else: 
duckdb safestreets.duckdb in terminal
then run



### Assumptions:

I don't want to infer any id's even though I can see that it's probably evt_012 based on timestamps. I don't want to 
make individual band aids in models. It's a bad habit and can quickly get out of control. Other missing data is the same
way.  Depending on the context I'd either throw out the bad records or create my own surrogate id if necessary (concat
the timestamp, event type, and device id).

I might get more specific with naming the payload/other s


fact_events:

- Why you chose this structure
common aggregates are included. Foreign keys are there for easy joining when necessary. Payload and other json columns 
are excluded to reduce complexity for analysts in future. That stuff should be done in upstream models (usually 
intermediate, not staging. I'm a bit lazy becasue this is an assessment and I don't have a need to reuse that logic)
- What business questions it supports
Supports general reporting, ad-hoc requests. Number of events, type of events, timing. All able to aggregate.
- How it would scale as data grows
I'd make it an intermediate model only grabbing new records when data is flowing more. Materializing as a table
every time could get expensive quick, especially since upstream is a view


dim_users:

right now there's only one of each of `platform` but I'd probably do some additional modeling to make sure this 
doesn't explode this table if this were real life

dim_devices
same with user id as platform ^^

- Why you chose these columns
Lot's of metadata that could be interesting for adhoc queries. I could add more (last and first) to each of the 
potentially changable stuff (user id's, locations, install dates, city)
- What business questions this supports
In conjuction with the fact table, questions about where users are, how many devices they have (with dim_user), where 
events are occurring, etc

What I didn't finish:

Tests.

---

## Notes

- Please do not use cloud services or external accounts
- Please do not use AI to write your solutions for this project
- PLEASE DO NOT SPEND MORE THAN 4 HOURS ON THIS PROJECT
- If you run out of time, document what you would have done next
- Questions? Email your recruiter or hiring manager
