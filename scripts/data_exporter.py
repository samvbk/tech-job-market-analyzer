import pyarrow as pa
import pyarrow.parquet as pq
import os
from pandas import DataFrame
from datetime import datetime

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# GCP settings based on Terraform deployment
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/keys/gcp-key.json'
project_id = 'tech-job-market-analyzer'
bucket_name = 'tech-job-market-analyzer-raw-data-lake'
table_name = 'raw_jobs'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Export data partitioned by execution date to Google Cloud Storage.
    """
    execution_date = kwargs.get('execution_date')
    if execution_date is None:
        execution_date = datetime.utcnow()
    
    # Create the partition column
    df['execution_date'] = execution_date.strftime('%Y-%m-%d')
    
    # Convert all object columns to strings to prevent PyArrow schema errors with mixed types (like dict/list in raw JSON)
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str)
            
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    print(f"Exporting to GCS: gs://{root_path} partitioned by execution_date={df['execution_date'].iloc[0]}")
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['execution_date'],
        filesystem=gcs
    )
