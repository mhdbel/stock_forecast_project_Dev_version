# tech_indicators.py
import logging
from stockstats import StockDataFrame as sdf
import pandas as pd

def add_technical_indicators(data_frame: pd.DataFrame) -> pd.DataFrame:
    """
    Add various technical indicators to the input DataFrame.
    
    Parameters:
        data_frame (pd.DataFrame): A DataFrame with at least 'close', 'low', and 'high' columns.
    
    Returns:
        pd.DataFrame: The DataFrame enriched with technical indicators.
    """
    logging.info("Adding technical indicators...")
    required_cols = {'close', 'low', 'high'}
    if not required_cols.issubset(data_frame.columns):
        missing = required_cols - set(data_frame.columns)
        logging.error(f"Missing required columns for technical indicators: {missing}")
        return data_frame
    
    data_frame.set_index('date', inplace=True)
    stock_df = sdf.retype(data_frame.copy())
    stock_df['low_14'] = stock_df['low'].rolling(window=14, min_periods=1).min()
    stock_df['high_14'] = stock_df['high'].rolling(window=14, min_periods=1).max()
    
    indicators = ['macd', 'rsi_14', 'boll', 'close_30_sma', 'close_60_sma',
                  'cci_20', 'stoch_k', 'stoch_d', 'adx', 'wr_14', 'ema_20']
    for ind in indicators:
        try:
            stock_df[ind] = stock_df.get(ind)
        except Exception as e:
            logging.error(f"Error calculating {ind}: {e}")
    
    stock_df.reset_index(inplace=True)
    return stock_df
