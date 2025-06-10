# main.py
import logging
import pandas as pd
# Ensure all necessary config variables are imported, including LOG_LEVEL
from config import TICKER_SYMBOL, START_DATE, END_DATE, LOG_LEVEL
from data_downloader import download_stock_data
from feature_engineering import create_lag_features, calculate_rolling_statistics
from tech_indicators import add_technical_indicators
from bigquery_uploader import upload_to_bigquery

# Configure logging using LOG_LEVEL from config and an enhanced format string
# %(funcName)s is added to the format string
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),  # Safely get the logging level
    format='%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s - %(lineno)d - %(message)s'
)

def main():
    try:
        logging.info(f"Starting main processing for ticker: {TICKER_SYMBOL}")
        # Download stock data
        stock_data = download_stock_data(TICKER_SYMBOL, START_DATE, END_DATE)
        if stock_data.empty:
            logging.warning(f"No stock data downloaded for {TICKER_SYMBOL}. Exiting.")
            return
        logging.debug(f"Stock data downloaded. Shape: {stock_data.shape}")

        # Ensure 'date' is in datetime format
        if 'date' not in stock_data.columns:
            logging.error("'date' column missing from downloaded data. Exiting.")
            return
        try:
            stock_data['date'] = pd.to_datetime(stock_data['date'])
        except Exception as e:
            logging.error(f"Could not convert 'date' column to datetime: {e}. Exiting.")
            return

        # Apply feature engineering
        feature_data = stock_data.copy()
        feature_data = create_lag_features(feature_data, 'close', lag_days=5)
        feature_data = calculate_rolling_statistics(feature_data, 'close', window=10)
        feature_data = create_lag_features(feature_data, 'volume', lag_days=5)
        feature_data = calculate_rolling_statistics(feature_data, 'volume', window=10)

        feature_data['day_of_week'] = feature_data['date'].dt.dayofweek
        feature_data['month'] = feature_data['date'].dt.month
        feature_data['year'] = feature_data['date'].dt.year
        logging.debug(f"Feature data after initial engineering. Shape: {feature_data.shape}")

        if feature_data['close'].isnull().any():
            logging.error("The 'close' column contains NaN values after feature engineering. Skipping daily_return calculation and further processing.")
            return
        else:
            feature_data['daily_return'] = feature_data['close'].pct_change()
            feature_data['daily_return'] = feature_data['daily_return'].fillna(0)
            logging.info("Filled NaN values in 'daily_return' with 0.")

            indicator_data = add_technical_indicators(feature_data.copy())
            if indicator_data.empty:
                logging.warning("Indicator data is empty after add_technical_indicators. Skipping BigQuery upload.")
            else:
                logging.debug(f"Data after adding technical indicators. Shape: {indicator_data.shape}")
                logging.debug(f"Columns in indicator_data before upload: {indicator_data.columns.tolist()}")
                upload_to_bigquery(indicator_data, f'{TICKER_SYMBOL}_processed_stock_data')

        logging.info(f"Main processing finished for ticker: {TICKER_SYMBOL}")

    except FileNotFoundError as fnfe:
        logging.error(f"Configuration or credentials file not found: {fnfe}")
    except Exception as e:
        # exc_info=True will add traceback information
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)

if __name__ == "__main__":
    # BasicConfig is now at the top of the script, so it's applied when the module is imported.
    # No need to call it again here unless it's specifically for __main__ execution context,
    # but standard practice is to configure logging once when the application starts.
    main()
