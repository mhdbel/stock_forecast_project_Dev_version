# Freemium Conversion Funnel Analytics Pipeline

> **BigQuery + Python | Events → KPIs → Dashboards**

This project demonstrates a complete analytics pipeline for a freemium product (modeled after eSIM/travel-tech). It generates event-level user journey data, models a conversion funnel, computes cohort retention and unit economics metrics, and uploads curated tables to Google BigQuery for dashboarding and self-serve analytics.

## 🎯 What This Demonstrates

| Analytics Capability | Implementation |
|---------------------|----------------|
| **Data Foundations** | Event schema + daily metrics tables (warehouse-first) |
| **Funnel Intelligence** | Step-by-step leakage analysis (activation → purchase) |
| **Cohorts & Retention** | D1/D7 retention by activation cohort |
| **Segmentation** | Partner/channel/geo/device breakdowns |
| **Pricing/Experimentation** | A/B variant analysis (conversion vs margin tradeoff) |
| **Unit Economics** | Revenue, cost, CAC, gross margin, payback by segment |
| **Self-Serve Enablement** | Curated BigQuery tables ready for Metabase/Looker |

## 📊 Data Model

### Raw Layer
- `raw.events` — PostHog-style event stream with user journeys

### Mart Layer (BI-Ready)
- `mart.daily_funnel_metrics` — Daily conversion rates + revenue
- `mart.daily_funnel_by_partner` — Partner-segmented funnel
- `mart.daily_funnel_by_channel` — Channel-segmented funnel
- `mart.cohort_retention` — D1/D3/D7/D14/D28 retention by cohort
- `mart.cohort_summary` — Weekly cohort summary with LTV metrics
- `mart.channel_economics` — CAC, conversion, margin by channel
- `mart.partner_economics` — Partner profitability analysis
- `mart.variant_economics` — A/B pricing experiment results

## 🚀 Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run pipeline (saves locally)
python main.py

# Run with BigQuery upload
export GCP_PROJECT_ID=your-project
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/credentials.json
python main.py --upload-bq

# Force regenerate events
python main.py --regenerate

PIPELINE SUMMARY REPORT
============================================================

📊 Events Generated: 287,432
   Unique Users: 50,000
   Date Range: 2024-01-01 to 2024-06-15

🎯 Funnel Overview:
   Total Landings: 50,000
   Total Purchases: 4,823
   Overall Conversion: 9.65%
   Total Revenue: $48,182.77
   Total Gross Margin: $36,125.27

💰 Top Channel by Net Margin/User: organic
   Conversion Rate: 12.34%
   Net Margin/User: $0.92

🤝 Top Partner by Conversion: Direct_App
   Conversion Rate: 14.21%
   Revenue/User: $1.42

🧪 A/B Test Results (Pricing):
   control: Conv=9.52%, Rev/User=$0.95, Margin/User=$0.70
   variant_A: Conv=12.84%, Rev/User=$1.03, Margin/User=$0.83
   variant_B: Conv=7.13%, Rev/User=$0.93, Margin/User=$0.60
