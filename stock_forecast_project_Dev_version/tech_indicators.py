# tech_indicators.py
import logging
from stockstats import StockDataFrame as sdf
import pandas as pd

def add_technical_indicators(data_frame: pd.DataFrame) -> pd.DataFrame:
    logging.info("Adding technical indicators...")

    # Define required columns for the function to operate correctly.
    # 'volume' is often used by stockstats for many indicators.
    required_cols = {'date', 'close', 'high', 'low', 'volume'}
    if not required_cols.issubset(data_frame.columns):
        missing = required_cols - set(data_frame.columns)
        logging.error(f"Missing required columns for technical indicators: {missing}. Returning original DataFrame.")
        # Return a copy of the original DataFrame to prevent modification issues if it's used elsewhere.
        return data_frame.copy()

    # Work on a copy of the input DataFrame to ensure the original is not modified.
    # This makes the function safer and more predictable.
    df_processed = data_frame.copy()

    # Set 'date' column as index, which is required by stockstats.
    # Ensure 'date' column is in datetime format before setting as index.
    # main.py should handle this, but a check here could be beneficial.
    if not pd.api.types.is_datetime64_any_dtype(df_processed['date']):
        try:
            df_processed['date'] = pd.to_datetime(df_processed['date'])
        except Exception as e:
            logging.error(f"Could not convert 'date' column to datetime: {e}. Returning original DataFrame.")
            return data_frame.copy()

    df_processed.set_index('date', inplace=True)

    # Convert the DataFrame to a StockDataFrame object.
    # sdf.retype() will prepare the DataFrame for stockstats calculations.
    # It expects column names like 'close', 'high', 'low', 'open', 'volume'.
    # Our data_downloader ensures these are lowercase.
    stock_df = sdf.retype(df_processed)

    # Manual calculation of 14-day low and high.
    # These are kept for explicit control, even if stockstats might offer alternatives.
    if 'low' in stock_df.columns: # Check on stock_df as it's the StockDataFrame
        stock_df['low_14'] = stock_df['low'].rolling(window=14, min_periods=1).min()
    else:
        logging.warning("Column 'low' not found for 'low_14' calculation. Adding NaN column for 'low_14'.")
        stock_df['low_14'] = pd.Series([float('nan')] * len(stock_df), index=stock_df.index)

    if 'high' in stock_df.columns: # Check on stock_df
        stock_df['high_14'] = stock_df['high'].rolling(window=14, min_periods=1).max()
    else:
        logging.warning("Column 'high' not found for 'high_14' calculation. Adding NaN column for 'high_14'.")
        stock_df['high_14'] = pd.Series([float('nan')] * len(stock_df), index=stock_df.index)

    # Define the list of technical indicators to be calculated.
    # These names should match the column names stockstats generates.
    indicators_to_calculate = [
        'macd',       # MACD (generates macd, macds, macdh)
        'rsi_14',     # 14-period Relative Strength Index
        'boll',       # Bollinger Bands (generates boll, boll_ub, boll_lb)
        'close_30_sma',# 30-period Simple Moving Average for 'close'
        'close_60_sma',# 60-period Simple Moving Average for 'close'
        'cci_20',     # 20-period Commodity Channel Index
        'stoch_k',    # Stochastic %K (often stockstats uses 'kdjk')
        'stoch_d',    # Stochastic %D (often stockstats uses 'kdjd')
        'adx',        # Average Directional Index (generates adx, adxr)
        'wr_14',      # 14-period Williams %R
        'ema_20'      # 20-period Exponential Moving Average for 'close'
    ]

    # stockstats might use 'kdjk' for %K and 'kdjd' for %D.
    # If 'stoch_k' and 'stoch_d' are not directly generated,
    # we might need to access 'kdjk' and 'kdjd' and then rename or select them.
    # For now, we attempt to access them as specified.

    for indicator_name in indicators_to_calculate:
        try:
            # Accessing the indicator name as a key in the StockDataFrame
            # should trigger its calculation if it's a recognized indicator.
            # The result of the access (the series) is not explicitly used here,
            # as stockstats modifies the DataFrame in place.
            _ = stock_df[indicator_name]

            # Some indicators generate multiple columns (e.g., 'macd' -> 'macds', 'macdh'; 'boll' -> 'boll_ub', 'boll_lb').
            # stockstats typically adds these automatically when the base indicator is called.
            # For example, after `_ = stock_df['macd']`, `stock_df['macds']` should also be available.
            # The current loop processes the `indicators_to_calculate` list. If `macds` or `boll_ub`
            # were explicitly in this list, they would be accessed, which is harmless.

        except KeyError:
            # This occurs if the indicator_name is not recognized by stockstats
            # or if essential underlying data for that indicator is missing (e.g. 'close' for 'close_30_sma').
            logging.warning(f"Indicator '{indicator_name}' not recognized by stockstats or missing required data. Adding NaN column for '{indicator_name}'.")
            # Ensure a column with this name exists, filled with NaNs, for consistent DataFrame structure.
            if indicator_name not in stock_df.columns: # Check if it was created as part of a group or by another means
                 stock_df[indicator_name] = pd.Series([float('nan')] * len(stock_df), index=stock_df.index)
        except Exception as e:
            # Catch any other unexpected errors during indicator calculation.
            logging.error(f"Error calculating or accessing indicator '{indicator_name}': {e}. Adding NaN column for '{indicator_name}'.")
            if indicator_name not in stock_df.columns:
                stock_df[indicator_name] = pd.Series([float('nan')] * len(stock_df), index=stock_df.index)

    # Reset the index so that 'date' becomes a regular column again.
    stock_df.reset_index(inplace=True)

    # Log the columns present in the final DataFrame for debugging.
    logging.debug(f"Columns after adding technical indicators: {stock_df.columns.tolist()}")

    return stock_df
