# main.py
import logging
import pandas as pd
from config import TICKER_SYMBOL, START_DATE, END_DATE
from data_downloader import download_stock_data
from feature_engineering import create_lag_features, calculate_rolling_statistics
from tech_indicators import add_technical_indicators
from bigquery_uploader import upload_to_bigquery

def main():
    try:
        # Download stock data
        stock_data = download_stock_data(TICKER_SYMBOL, START_DATE, END_DATE)
        if stock_data.empty:
            logging.warning("No stock data available.")
            return

        # Ensure 'date' is in datetime format
        stock_data['date'] = pd.to_datetime(stock_data['date'])

        # Apply feature engineering
        feature_data = create_lag_features(stock_data.copy(), 'close', lag_days=5)
        feature_data = calculate_rolling_statistics(feature_data, 'close', window=10)
        feature_data = create_lag_features(feature_data, 'volume', lag_days=5)
        feature_data = calculate_rolling_statistics(feature_data, 'volume', window=10)
        feature_data['day_of_week'] = feature_data['date'].dt.dayofweek
        feature_data['month'] = feature_data['date'].dt.month
        feature_data['year'] = feature_data['date'].dt.year

        if feature_data['close'].isna().any():
            logging.error("The 'close' column contains NaN or missing values.")
        else:
            feature_data['daily_return'] = feature_data['close'].pct_change()
            indicator_data = add_technical_indicators(feature_data.copy())
            upload_to_bigquery(feature_data, f'{TICKER_SYMBOL}_feature_data')
            upload_to_bigquery(indicator_data, f'{TICKER_SYMBOL}_technical_indicators')
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()
