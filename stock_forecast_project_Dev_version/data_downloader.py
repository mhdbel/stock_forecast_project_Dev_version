# data_downloader.py
import logging
import pandas as pd
import yfinance as yf

def download_stock_data(ticker, start_date, end_date):
    """
    Download historical stock data for the given ticker symbol.
    
    Parameters:
        ticker (str): The stock ticker symbol.
        start_date (str): The starting date (YYYY-MM-DD).
        end_date (str): The ending date (YYYY-MM-DD).
    
    Returns:
        pd.DataFrame: A DataFrame containing the historical stock data, or empty if an error occurs.
    """
    logging.info(f"Starting download for {ticker}...")
    try:
        ticker_data = yf.Ticker(ticker)
        historical_data = ticker_data.history(period='1d', start=start_date, end=end_date)
        
        if historical_data.empty:
            logging.warning(f"No data found for {ticker} from {start_date} to {end_date}")
            return pd.DataFrame()
        
        # Verify the required columns exist; we need these for downstream calculations.
        required = ['Close', 'Volume', 'Low', 'High']
        for col in required:
            if col not in historical_data.columns:
                logging.error(f"The '{col}' column is missing in the downloaded data for {ticker}.")
                return pd.DataFrame()
        
        historical_data.reset_index(inplace=True)
        historical_data.rename(columns={
            'Date': 'date', 
            'Close': 'close', 
            'Volume': 'volume',
            'Low': 'low',
            'High': 'high'
        }, inplace=True)
        file_path = f'{ticker}_original_data.csv'
        historical_data.to_csv(file_path, index=False)
        logging.info(f"Successfully downloaded and saved stock data for {ticker} to {file_path}")
        return historical_data
    except Exception as e:
        logging.error(f"An error occurred while downloading stock data for {ticker}: {e}")
        return pd.DataFrame()
