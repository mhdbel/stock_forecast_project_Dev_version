import pandas_gbq
from google.cloud import bigquery
from config import PROJECT_ID, DATASET_ID, CREDENTIALS_PATH

class BigQueryUploader:
    def __init__(self):
        self.project_id = PROJECT_ID
        self.dataset_id = DATASET_ID

    def upload_dataframe(self, df, table_name, if_exists='replace'):
        """Uploads a pandas DataFrame to BigQuery."""
        full_table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        print(f"☁️ Uploading {table_name} to BigQuery...")
        
        try:
            pandas_gbq.to_gbq(
                df,
                full_table_id,
                project_id=self.project_id,
                if_exists=if_exists,
                credentials=None # Uses env var if set, or default auth
            )
            print(f"✅ Successfully uploaded {len(df)} rows to {table_name}")
        except Exception as e:
            print(f"❌ Error uploading to BigQuery: {e}")

    def create_dataset_if_not_exists(self):
        client = bigquery.Client(project=self.project_id)
        dataset_ref = client.dataset(self.dataset_id)
        try:
            client.get_dataset(dataset_ref)
            print(f"Dataset {self.dataset_id} already exists.")
        except:
            print(f"Creating dataset {self.dataset_id}...")
            client.create_dataset(dataset_ref)
