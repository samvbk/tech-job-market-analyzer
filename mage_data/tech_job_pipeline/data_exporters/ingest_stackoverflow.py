from google.cloud import bigquery
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# GCP settings
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/keys/gcp-key.json'
project_id = 'tech-job-market-analyzer'
dataset_id = 'tech_job_market_data'
bucket_name = 'tech-job-market-analyzer-raw-data-lake'
temp_gcs_path = f'gs://{bucket_name}/temp_stackoverflow/trends.parquet'

@data_exporter
def export_stackoverflow_trends(*args, **kwargs):
    """
    Robust Cross-Region Transfer: 
    1. Query Public Data (US) -> 2. Export to GCS -> 3. Load to BQ (us-central1)
    """
    # 1. Create client for the 'US' region to query the public data
    client_us = bigquery.Client(project=project_id, location='US')
    
    # 2. Extract into a temporary table in your project (but in the US region)
    query = f"""
    SELECT
      t.tag_name,
      EXTRACT(DATE FROM q.creation_date) AS date,
      COUNT(*) AS question_count
    FROM
      `bigquery-public-data.stackoverflow.posts_questions` AS q
    CROSS JOIN
      UNNEST(SPLIT(q.tags, '|')) AS tag_name
    INNER JOIN
      `bigquery-public-data.stackoverflow.tags` AS t ON t.tag_name = tag_name
    WHERE
      q.creation_date >= '2024-01-01'
    GROUP BY
      1, 2
    """
    
    print("Step 1: Running query on Public Dataset (US region)...")
    job_config = bigquery.QueryJobConfig(
        destination=f"{project_id}.{dataset_id}_temp_us.raw_trends", 
        write_disposition="WRITE_TRUNCATE"
    )
    
    # Pre-emptively create a temporary dataset in US if it doesn't exist
    client_us.create_dataset(f"{project_id}.{dataset_id}_temp_us", exists_ok=True)
    
    query_job = client_us.query(query, job_config=job_config)
    query_job.result() # Wait for results
    
    # 3. Export US temp table to GCS (This is safe across regions)
    print(f"Step 2: Exporting results to GCS: {temp_gcs_path}...")
    extract_job = client_us.extract_table(
        f"{project_id}.{dataset_id}_temp_us.raw_trends", 
        temp_gcs_path,
        job_config=bigquery.ExtractJobConfig(destination_format="PARQUET")
    )
    extract_job.result()
    
    # 4. Load from GCS into the final dataset in us-central1
    print(f"Step 3: Loading into final table {project_id}.{dataset_id}.raw_stackoverflow_trends (us-central1)...")
    client_central = bigquery.Client(project=project_id, location='us-central1')
    
    load_job = client_central.load_table_from_uri(
        temp_gcs_path,
        f"{project_id}.{dataset_id}.raw_stackoverflow_trends",
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE"
        )
    )
    load_job.result()
    
    # Clean up temp dataset
    client_us.delete_dataset(f"{project_id}.{dataset_id}_temp_us", delete_contents=True, not_found_ok=True)
    
    print("Done! Big Data synergy successfully synchronized across regions.")
    return True
