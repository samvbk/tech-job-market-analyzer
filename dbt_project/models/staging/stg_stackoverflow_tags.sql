{{ config(materialized='view') }}

with raw_source as (
    -- Access the new table created by Mage
    select * from `tech-job-market-analyzer.tech_job_market_data.raw_stackoverflow_trends`
)

select
    cast(tag_name as string) as tag_name,
    cast(date as date) as creation_date,
    cast(question_count as integer) as question_count
from raw_source
