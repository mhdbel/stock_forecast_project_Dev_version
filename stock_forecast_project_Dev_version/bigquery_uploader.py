# bigquery_uploader.py
import logging
from google.cloud import bigquery
import pandas as pd
import numpy as np # Import numpy for NaN checking if needed and for integer types
from config import PROJECT_ID, DATASET_ID, JSON_CREDENTIALS_PATH

def upload_to_bigquery(data_frame: pd.DataFrame, table_name: str, write_disposition='WRITE_TRUNCATE'):
    """
    Uploads the given DataFrame to a specified BigQuery table.

    Parameters:
        data_frame (pd.DataFrame): The data to upload.
        table_name (str): The target BigQuery table name.
        write_disposition (str): Specifies behavior for existing data.
                                 Defaults to 'WRITE_TRUNCATE' (overwrite).
                                 Other options: 'WRITE_APPEND', 'WRITE_EMPTY'.
    """
    if data_frame.empty:
        logging.warning(f"Input DataFrame for table {table_name} is empty. Skipping BigQuery upload.")
        return

    logging.info(f"Starting upload to BigQuery table {PROJECT_ID}.{DATASET_ID}.{table_name}...")
    logging.debug(f"Input DataFrame columns and dtypes:\n{data_frame.dtypes}")

    try:
        client = bigquery.Client.from_service_account_json(JSON_CREDENTIALS_PATH, project=PROJECT_ID)

        schema = []
        for col_name, dtype in data_frame.dtypes.items():
            field_type = 'STRING' # Default type
            if pd.api.types.is_datetime64_any_dtype(dtype):
                field_type = 'TIMESTAMP'
            elif pd.api.types.is_float_dtype(dtype): # Catches float64, float32
                field_type = 'FLOAT'
            elif pd.api.types.is_integer_dtype(dtype): # Catches int64, int32, etc.
                # Check if the column (even if originally integer) contains NaNs, forcing it to float
                if data_frame[col_name].isnull().any():
                    logging.warning(f"Column '{col_name}' is integer type but contains NaN values. Will be uploaded as FLOAT to BigQuery.")
                    field_type = 'FLOAT'
                else:
                    field_type = 'INTEGER'
            elif pd.api.types.is_bool_dtype(dtype):
                field_type = 'BOOLEAN'

            schema.append(bigquery.SchemaField(col_name, field_type))

        logging.debug(f"Generated BigQuery schema: {schema}")

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=write_disposition
        )

        # Construct the full table reference
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

        job = client.load_table_from_dataframe(data_frame, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete

        full_table_path = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
        logging.info(f"Successfully uploaded {len(data_frame)} rows to BigQuery table {full_table_path}.")

    except FileNotFoundError:
        logging.error(f"BigQuery credentials file not found at {JSON_CREDENTIALS_PATH}. Please check the GOOGLE_APPLICATION_CREDENTIALS environment variable.")
        raise
    except Exception as e:
        logging.error(f"Error uploading to BigQuery table {table_name}: {e}", exc_info=True)
        raise
