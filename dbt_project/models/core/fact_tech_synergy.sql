{{ config(materialized='table') }}

with job_postings as (
    select * from {{ ref('fact_job_postings') }}
),

stackoverflow_trends as (
    -- Get the total question count for each tech tag across the whole archive
    select 
        tag_name,
        sum(question_count) as total_so_questions
    from {{ ref('stg_stackoverflow_tags') }}
    group by 1
),

-- Calculate what % of currently open jobs require each tech
current_job_demand as (
    select 
        'python' as tech_name,
        countif(requires_python) / count(*) as pct_of_jobs
    from job_postings
    union all
    select 
        'sql' as tech_name,
        countif(requires_sql) / count(*) as pct_of_jobs
    from job_postings
    union all
    select 
        'aws' as tech_name,
        countif(requires_aws) / count(*) as pct_of_jobs
    from job_postings
    -- You can add more skills here later (spark, kafka, dbt, etc)
)

select
    j.tech_name,
    round(j.pct_of_jobs * 100, 2) as job_demand_score,
    s.total_so_questions as so_interest_volume,
    -- Normalized interest score (0-100) relative to the top tag (Python is usually largest)
    round(s.total_so_questions / max(s.total_so_questions) over () * 100, 2) as dev_interest_score
from current_job_demand j
left join stackoverflow_trends s 
    on lower(j.tech_name) = lower(s.tag_name)
