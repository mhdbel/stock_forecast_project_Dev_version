from events_generator import FreemiumEventGenerator
from funnel_metrics import FunnelMetricsCalculator
from feature_engineering import KPIsFeatureEngineer
from bigquery_uploader import BigQueryUploader
from config import SIMULATION_DAYS, DAILY_NEW_USERS

def run_pipeline():
    print("🚀 Starting Freemium Funnel Analytics Pipeline...")

    # 1. Ingest / Generate Raw Data
    generator = FreemiumEventGenerator(days=SIMULATION_DAYS, daily_users=DAILY_NEW_USERS)
    raw_events_df = generator.generate_events()

    # 2. Compute Business Metrics (The "Transformation" Layer)
    metrics_calc = FunnelMetricsCalculator(raw_events_df)
    
    # A) Daily Funnel & Unit Economics
    daily_econ_df = metrics_calc.compute_unit_economics()
    
    # B) Cohort Retention
    cohort_df = metrics_calc.compute_cohort_retention()

    # 3. Feature Engineering (Smoothing & Trend Detection)
    # Applying this to the daily aggregation table
    feature_eng = KPIsFeatureEngineer(daily_econ_df)
    enriched_daily_df = feature_eng.add_rolling_trends()

    # 4. Warehouse Loading (Enablement Layer)
    uploader = BigQueryUploader()
    uploader.create_dataset_if_not_exists()

    # Upload Tables
    uploader.upload_dataframe(raw_events_df, "raw_events")
    uploader.upload_dataframe(enriched_daily_df, "mart_daily_funnel_metrics")
    uploader.upload_dataframe(cohort_df, "mart_cohort_retention") # Note: Pivot tables might need melting for BQ, but GBQ handles cols fine usually

    print("🎉 Pipeline Run Complete. Data ready for dashboarding.")

if __name__ == "__main__":
    run_pipeline()
