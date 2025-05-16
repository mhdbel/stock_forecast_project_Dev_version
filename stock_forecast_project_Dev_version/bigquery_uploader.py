# bigquery_uploader.py
import logging
from google.cloud import bigquery
import pandas as pd
from config import PROJECT_ID, DATASET_ID, JSON_CREDENTIALS_PATH

client = bigquery.Client.from_service_account_json(JSON_CREDENTIALS_PATH, project=PROJECT_ID)

def upload_to_bigquery(data_frame: pd.DataFrame, table_name: str, chunk_size=5000, write_disposition='WRITE_TRUNCATE'):
    """
    Upload the given DataFrame to a specified BigQuery table.
    
    Parameters:
        data_frame (pd.DataFrame): The data to upload.
        table_name (str): The target BigQuery table name.
        chunk_size (int): Number of rows per chunk. Default is 5000.
        write_disposition (str): Specifies behavior for existing data. Defaults to 'WRITE_TRUNCATE'.
    """
    logging.info(f"Starting upload to BigQuery table {table_name}...")
    try:
        dataset_ref = client.dataset(DATASET_ID)
        table_ref = dataset_ref.table(table_name)
        
        # Build schema based on data types
        schema = []
        type_mapping = {
            'float64': 'FLOAT',
            'int64': 'INTEGER',
            'bool': 'BOOLEAN'
        }
        for col in data_frame.columns:
            if pd.api.types.is_datetime64_any_dtype(data_frame[col].dtype):
                schema.append(bigquery.SchemaField(col, 'TIMESTAMP'))
            else:
                dtype = str(data_frame[col].dtype)
                field_type = type_mapping.get(dtype, 'STRING')
                schema.append(bigquery.SchemaField(col, field_type))
                
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition  # Initial write disposition
        )
        
        first_chunk = True
        for i in range(0, len(data_frame), chunk_size):
            chunk = data_frame.iloc[i:i+chunk_size]
            if not first_chunk:
                # Subsequent chunks should append
                job_config.write_disposition = 'WRITE_APPEND'
            else:
                first_chunk = False
            job = client.load_table_from_dataframe(chunk, table_ref, job_config=job_config)
            job.result()
        
        logging.info(f"Successfully uploaded data to BigQuery table {table_name}")
    except Exception as e:
        logging.error(f"Error uploading to BigQuery table {table_name}: {e}")
        raise
