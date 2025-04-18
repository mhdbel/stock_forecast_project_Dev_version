# config.py
from decouple import config

# Project and BigQuery settings
PROJECT_ID = config('BIGQUERY_PROJECT_ID', default='trabot-409617')
DATASET_ID = config('BIGQUERY_DATASET_ID', default='tradbotV1')
TICKER_SYMBOL = config('TICKER_SYMBOL')
START_DATE = config('START_DATE')
END_DATE = config('END_DATE')
JSON_CREDENTIALS_PATH = config('GOOGLE_APPLICATION_CREDENTIALS', default='/path/to/your/credentials.json')
