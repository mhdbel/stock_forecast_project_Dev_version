# feature_engineering.py
import pandas as pd

def create_lag_features(data_frame: pd.DataFrame, column: str, lag_days: int) -> pd.DataFrame:
    """
    Create lag features for a given column over a specified number of days.
    
    Parameters:
        data_frame (pd.DataFrame): Input DataFrame.
        column (str): The column to create lag features for.
        lag_days (int): Number of lag days.
        
    Returns:
        pd.DataFrame: DataFrame with added lag feature columns.
    """
    for i in range(1, lag_days + 1):
        data_frame[f"{column}_lag_{i}"] = data_frame[column].shift(i)
    return data_frame

def calculate_rolling_statistics(data_frame: pd.DataFrame, column: str, window: int) -> pd.DataFrame:
    """
    Calculate rolling mean and rolling standard deviation for a given column.
    
    Parameters:
        data_frame (pd.DataFrame): Input DataFrame.
        column (str): The column to calculate statistics for.
        window (int): The window size for calculating rolling statistics.
        
    Returns:
        pd.DataFrame: DataFrame with rolling mean and std dev columns added.
    """
    data_frame[f"{column}_rolling_mean"] = data_frame[column].rolling(window=window).mean()
    data_frame[f"{column}_rolling_std"] = data_frame[column].rolling(window=window).std()
    return data_frame
