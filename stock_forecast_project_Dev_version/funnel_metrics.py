import pandas as pd
import numpy as np

class FunnelMetricsCalculator:
    def __init__(self, raw_events_df):
        self.df = raw_events_df

    def compute_daily_funnel(self):
        """Aggregates events into a daily mart for BI tools."""
        print("📊 Computing Daily Funnel Metrics...")
        
        # Define funnel order
        steps = ['landing_view', 'offer_view', 'install_success', 'account_activation', 'purchase_completed']
        
        # Group by Dimensions
        grouped = self.df.groupby(['date', 'partner', 'channel', 'variant', 'country']).apply(
            lambda x: pd.Series({
                'users_landing': (x['event_name'] == 'landing_view').sum(),
                'users_install': (x['event_name'] == 'install_success').sum(),
                'users_activated': (x['event_name'] == 'account_activation').sum(),
                'users_purchased': (x['event_name'] == 'purchase_completed').sum(),
                'total_revenue': x['revenue_usd'].sum(),
                'total_cogs': x['cost_usd'].sum()
            })
        ).reset_index()

        # Calculate Rates (avoid division by zero)
        grouped['install_rate'] = grouped['users_install'] / grouped['users_landing'].replace(0, 1)
        grouped['activation_rate'] = grouped['users_activated'] / grouped['users_install'].replace(0, 1)
        grouped['purchase_rate'] = grouped['users_purchased'] / grouped['users_activated'].replace(0, 1)
        grouped['gross_margin'] = grouped['total_revenue'] - grouped['total_cogs']
        
        return grouped

    def compute_unit_economics(self):
        """Calculates CAC, ARPU, and LTV proxies."""
        print("💰 Computing Unit Economics...")
        
        # Simplified CAC assumptions dictionary (in real life, this comes from ad spend tables)
        cac_assumptions = {
            'organic': 0.0,
            'paid_social': 15.0,
            'email_lifecycle': 0.5,
            'referral': 5.0
        }
        
        econ = self.compute_daily_funnel() # Reuse base agg
        
        # Map CAC
        econ['estimated_cac_per_install'] = econ['channel'].map(cac_assumptions)
        econ['total_marketing_spend'] = econ['users_install'] * econ['estimated_cac_per_install']
        
        # ROI Metrics
        econ['contribution_profit'] = econ['gross_margin'] - econ['total_marketing_spend']
        econ['arpu'] = econ['total_revenue'] / econ['users_purchased'].replace(0, 1)
        
        return econ

    def compute_cohort_retention(self):
        """Calculates retention based on acquisition date."""
        print("📅 Computing Cohort Retention...")
        
        # Determine Cohort (First Landing Date)
        cohorts = self.df.sort_values('event_timestamp').groupby('user_id').first()['date'].rename('cohort_date')
        df_merged = self.df.merge(cohorts, on='user_id')
        
        # Calculate days since cohort
        df_merged['days_since_first_seen'] = (pd.to_datetime(df_merged['date']) - pd.to_datetime(df_merged['cohort_date'])).dt.days
        
        # Pivot for Retention
        cohort_counts = df_merged.groupby(['cohort_date', 'days_since_first_seen'])['user_id'].nunique().reset_index()
        cohort_pivot = cohort_counts.pivot(index='cohort_date', columns='days_since_first_seen', values='user_id')
        
        # Calculate percentages (Divide by Day 0 count)
        cohort_size = cohort_pivot[0]
        retention = cohort_pivot.divide(cohort_size, axis=0)
        
        return retention.reset_index().fillna(0)
