from google.cloud import bigquery
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# GCP settings based on Terraform
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/keys/gcp-key.json'
project_id = 'tech-job-market-analyzer'
dataset_id = 'tech_job_market_data'
bucket_name = 'tech-job-market-analyzer-raw-data-lake'

@data_exporter
def create_external_table(*args, **kwargs):
    """
    Creates an external table in BigQuery pointing to the GCS data lake.
    """
    client = bigquery.Client(project=project_id)
    
    # We use hive partitioning since we partitioned by execution_date in GCS
    query = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.external_raw_jobs`
    WITH PARTITION COLUMNS (
        execution_date STRING
    )
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://{bucket_name}/raw_jobs/*'],
        hive_partition_uri_prefix = 'gs://{bucket_name}/raw_jobs'
    )
    """
    
    print(f"Executing query to create external table {project_id}.{dataset_id}.external_raw_jobs...")
    # Execute the query
    job = client.query(query)
    job.result()  # Wait for the job to complete
    
    print("Successfully created/updated the external table in BigQuery!")
    return True
