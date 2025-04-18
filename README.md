# stock_forecast_project_Dev_version

This project demonstrates a complete data pipeline using Python. It downloads historical stock data via yfinance, performs feature engineering (lag features, rolling statistics), calculates technical indicators with stockstats, and uploads the processed data to Google BigQuery.

## Project Structure

- **config.py:** Loads configuration via environment variables.
- **data_downloader.py:** Downloads and preprocesses stock data.
- **feature_engineering.py:** Contains functions to create lag features and rolling statistics.
- **tech_indicators.py:** Computes technical indicators.
- **bigquery_uploader.py:** Handles the upload of processed data to BigQuery.
- **main.py:** Orchestrates the end-to-end pipeline.

## Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/my-stock-pipeline.git


