import os
from google.cloud import bigquery

# Project Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET_ID = os.getenv("BQ_DATASET_ID", "freemium_funnel_analytics")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "path/to/credentials.json")

# Simulation Settings
SIMULATION_DAYS = 90
DAILY_NEW_USERS = 500  # Approx scale of simulation

# Initialize BigQuery Client
def get_bq_client():
    if CREDENTIALS_PATH:
        return bigquery.Client.from_service_account_json(CREDENTIALS_PATH)
    else:
        return bigquery.Client(project=PROJECT_ID)
