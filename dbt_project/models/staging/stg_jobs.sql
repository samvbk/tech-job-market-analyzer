{{ config(materialized='view') }}

with raw_source as (
    select * from {{ source('raw_data', 'external_raw_jobs') }}
)

select
    -- casting types
    cast(job_id as string) as job_id,
    cast(employer_name as string) as employer_name,
    cast(employer_website as string) as employer_website,
    cast(job_publisher as string) as job_publisher,
    cast(job_employment_type as string) as job_employment_type,
    cast(job_title as string) as job_title,
    cast(job_apply_link as string) as job_apply_link,
    cast(job_description as string) as job_description,
    cast(job_is_remote as boolean) as is_remote,
    TIMESTAMP_SECONDS(cast(job_posted_at_timestamp as INT64)) as posted_at,
    
    -- salary data
    cast(job_min_salary as numeric) as min_salary_usd,
    cast(job_max_salary as numeric) as max_salary_usd,
    
    -- Location
    cast(job_city as string) as city,
    cast(job_state as string) as state,
    cast(job_country as string) as country,

    -- Search query & execution date
    cast(search_query as string) as query_keyword,
    cast(execution_date as string) as execution_date

from raw_source

-- Deduplicate identical job postings from the free API (e.g. rapidapi limits can cause overlapping results)
qualify row_number() over (partition by job_id order by posted_at desc) = 1
