{{ config(materialized='table') }}

with stg_jobs as (
    select * from {{ ref('stg_jobs') }}
)

select
    job_id,
    employer_name,
    job_title,
    query_keyword,
    job_employment_type,
    is_remote,
    city,
    state,
    country,
    posted_at,
    min_salary_usd,
    max_salary_usd,
    
    -- Calculate average salary if both min and max are provided
    case 
        when min_salary_usd is not null and max_salary_usd is not null then (min_salary_usd + max_salary_usd) / 2.0
        else null
    end as average_salary_usd,

    -- Extracting specific skills from job description using regex.
    -- BigQuery REGEXP_CONTAINS is case sensitive by default, so we cast to lower case.
    REGEXP_CONTAINS(LOWER(job_description), r'\bpython\b') as requires_python,
    REGEXP_CONTAINS(LOWER(job_description), r'\bsql\b') as requires_sql,
    REGEXP_CONTAINS(LOWER(job_description), r'\baws\b') as requires_aws,
    REGEXP_CONTAINS(LOWER(job_description), r'\bspark\b') as requires_spark,
    REGEXP_CONTAINS(LOWER(job_description), r'\bkafka\b') as requires_kafka,
    REGEXP_CONTAINS(LOWER(job_description), r'\bairflow\b') as requires_airflow,
    REGEXP_CONTAINS(LOWER(job_description), r'\bdbt\b') as requires_dbt,
    REGEXP_CONTAINS(LOWER(job_description), r'\bgcp\b') as requires_gcp,
    REGEXP_CONTAINS(LOWER(job_description), r'\bterraform\b') as requires_terraform,
    REGEXP_CONTAINS(LOWER(job_description), r'\bscala\b') as requires_scala,

    execution_date

from stg_jobs
