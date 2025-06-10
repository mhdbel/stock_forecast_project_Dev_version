import pandas as pd
import numpy as np
from stock_forecast_project_Dev_version.feature_engineering import create_lag_features, calculate_rolling_statistics

def test_create_lag_features():
    data = {'close': [10, 12, 15, 14, 16]}
    df = pd.DataFrame(data)

    lag_df = create_lag_features(df.copy(), 'close', lag_days=2)

    assert 'close_lag_1' in lag_df.columns
    assert 'close_lag_2' in lag_df.columns
    pd.testing.assert_series_equal(lag_df['close_lag_1'], pd.Series([np.nan, 10, 12, 15, 14], name='close_lag_1'), check_dtype=False)
    pd.testing.assert_series_equal(lag_df['close_lag_2'], pd.Series([np.nan, np.nan, 10, 12, 15], name='close_lag_2'), check_dtype=False)
    assert len(lag_df) == 5

def test_create_lag_features_empty_df():
    df = pd.DataFrame({'close': []}, dtype=float) # Specify dtype for empty DataFrame
    lag_df = create_lag_features(df.copy(), 'close', lag_days=3)
    # Expect empty columns if input is empty and lags are requested
    assert 'close_lag_1' in lag_df.columns
    assert lag_df['close_lag_1'].empty
    assert len(lag_df) == 0

def test_calculate_rolling_statistics():
    data = {'close': [10, 12, 11, 14, 16, 15, 18, 20]}
    df = pd.DataFrame(data)
    window_size = 3

    stats_df = calculate_rolling_statistics(df.copy(), 'close', window=window_size)

    assert 'close_rolling_mean' in stats_df.columns
    assert 'close_rolling_std' in stats_df.columns

    # Calculate expected values using pandas for consistency
    expected_means = df['close'].rolling(window=window_size).mean().rename('close_rolling_mean')
    expected_stds_pandas = df['close'].rolling(window=window_size).std().rename('close_rolling_std')

    pd.testing.assert_series_equal(stats_df['close_rolling_mean'], expected_means, check_dtype=False)
    pd.testing.assert_series_equal(stats_df['close_rolling_std'], expected_stds_pandas, check_dtype=False)
    assert len(stats_df) == 8
    assert stats_df['close_rolling_mean'].isnull().sum() == window_size - 1
    assert stats_df['close_rolling_std'].isnull().sum() == window_size - 1

def test_calculate_rolling_statistics_empty_df():
    df = pd.DataFrame({'close': []}, dtype=float) # Specify dtype
    stats_df = calculate_rolling_statistics(df.copy(), 'close', window=3)
    assert 'close_rolling_mean' in stats_df.columns
    assert 'close_rolling_std' in stats_df.columns
    assert stats_df['close_rolling_mean'].empty
    assert stats_df['close_rolling_std'].empty
    assert len(stats_df) == 0

def test_calculate_rolling_statistics_small_df():
    # Test with df length less than window size
    data = {'close': [10, 12]}
    df = pd.DataFrame(data)
    stats_df = calculate_rolling_statistics(df.copy(), 'close', window=3)
    assert 'close_rolling_mean' in stats_df.columns
    assert 'close_rolling_std' in stats_df.columns
    assert stats_df['close_rolling_mean'].isnull().all()
    assert stats_df['close_rolling_std'].isnull().all()
    assert len(stats_df) == 2
