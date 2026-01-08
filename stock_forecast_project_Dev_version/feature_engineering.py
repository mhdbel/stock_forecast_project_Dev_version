import pandas as pd

class KPIsFeatureEngineer:
    def __init__(self, daily_metrics_df):
        self.df = daily_metrics_df.sort_values(['partner', 'channel', 'date'])

    def add_rolling_trends(self):
        """Adds 7-day rolling averages to smooth out volatility in KPIs."""
        print("📈 Engineering Rolling KPI Features...")
        
        # Group by segments to calculate rolling stats correctly
        grouper = self.df.groupby(['partner', 'channel'])
        
        self.df['install_rate_7d_avg'] = grouper['install_rate'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        self.df['purchase_rate_7d_avg'] = grouper['purchase_rate'].transform(lambda x: x.rolling(window=7, min_periods=1).mean())
        
        # Lagged features (e.g., did yesterday's activation spike lead to today's purchase?)
        self.df['users_activated_lag1'] = grouper['users_activated'].shift(1)
        
        # Anomaly Detection (Simple Z-score style flag)
        # If conversion is 20% lower than the 7-day average
        self.df['is_conversion_dip'] = self.df['purchase_rate'] < (self.df['purchase_rate_7d_avg'] * 0.8)
        
        return self.df
