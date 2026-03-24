import pandas as pd
import requests
import os
import time
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Fetch daily job postings for "Data Engineer" and "Software Engineer"
    """
    api_key = os.getenv('RAPIDAPI_KEY')
    if not api_key or api_key == 'your_rapidapi_jsearch_key_here':
        raise ValueError("Please set a valid RAPIDAPI_KEY in your .env file. Go to https://rapidapi.com/letscrape-6bRBa3QG1q/api/jsearch to get one.")

    url = "https://jsearch.p.rapidapi.com/search"
    headers = {
        "X-RapidAPI-Key": api_key,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    all_jobs = []
    queries = ['"Data Engineer"', '"Software Engineer"']
    
    # We will fetch 2 pages per query to get a good dataset
    num_pages = 2 

    for q in queries:
        for page in range(1, num_pages + 1):
            querystring = {"query": q, "page": str(page), "num_pages": "1", "date_posted": "today"}
            try:
                print(f"Fetching {q} - page {page}...")
                response = requests.get(url, headers=headers, params=querystring)
                response.raise_for_status()
                data = response.json().get('data', [])
                
                # Add a column for search query
                for item in data:
                    item['search_query'] = q.strip('"')
                
                all_jobs.extend(data)
                time.sleep(1.5) # Avoid API rate limits
            except Exception as e:
                print(f"Error fetching data for {q} page {page}: {e}")

    df = pd.DataFrame(all_jobs)
    
    if not df.empty:
        df['extracted_at'] = datetime.utcnow()
        if 'job_id' in df.columns:
            df['job_id'] = df['job_id'].astype(str)

    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
    assert not output.empty, 'The output dataframe is empty'
