# bigquery_uploader.py
import logging
from google.cloud import bigquery
import pandas as pd
from config import PROJECT_ID, DATASET_ID, JSON_CREDENTIALS_PATH

# Initialize BigQuery client using service account credentials
client = bigquery.Client.from_service_account_json(JSON_CREDENTIALS_PATH, project=PROJECT_ID)

def upload_to_bigquery(data_frame: pd.DataFrame, table_name: str):
    """
    Upload the given DataFrame to a specified BigQuery table.
    
    Parameters:
        data_frame (pd.DataFrame): The data to upload.
        table_name (str): The target BigQuery table name.
    """
    logging.info(f"Starting upload to BigQuery table {table_name}...")
    try:
        dataset_ref = client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(table_name)
        
        type_mapping = {
            'datetime64[ns]': 'TIMESTAMP',
            'float64': 'FLOAT',
            'int64': 'INTEGER'
        }
        schema = []
        for col in data_frame.columns:
            if col == 'date':
                schema.append(bigquery.SchemaField(col, 'TIMESTAMP'))
            else:
                dtype = str(data_frame[col].dtype)
                field_type = type_mapping.get(dtype, 'STRING')
                schema.append(bigquery.SchemaField(col, field_type))
                
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition='WRITE_TRUNCATE'
        )
        data_frame.reset_index(drop=True, inplace=True)
        chunk_size = 5000
        for i in range(0, len(data_frame), chunk_size):
            chunk = data_frame.iloc[i:i+chunk_size]
            job = client.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
            job.result()
        
        logging.info(f"Successfully uploaded data to BigQuery table {table_name}")
    except Exception as e:
        logging.error(f"Error uploading to BigQuery table {table_name}: {e}")
