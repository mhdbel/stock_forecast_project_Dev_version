import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import uuid
import random

class FreemiumEventGenerator:
    def __init__(self, days=90, daily_users=100):
        self.days = days
        self.daily_users = daily_users
        self.end_date = datetime.today()
        self.start_date = self.end_date - timedelta(days=self.days)

    def get_traffic_source(self):
        """
        Simulates raw GA4 traffic data and maps it to Business Partners.
        This enforces realistic correlations (e.g., Airlines come via Referral/API, not Paid Search).
        """
        traffic_scenarios = [
            # 1. Airline Partner A (High volume, lower friction)
            {
                'source': 'airline_app_checkin', 'medium': 'app_referral', 
                'partner_group': 'Airline_Partner_A', 'channel': 'Embedded_API',
                'weight': 0.35 
            },
            # 2. OTA Partner B (Travel Agency)
            {
                'source': 'ota_confirmation_email', 'medium': 'email', 
                'partner_group': 'OTA_Partner_B', 'channel': 'Affiliate_Referral',
                'weight': 0.25
            },
            # 3. Direct Organic (Kolet Brand)
            {
                'source': 'google', 'medium': 'organic', 
                'partner_group': 'Kolet_Direct', 'channel': 'Organic_Search',
                'weight': 0.15
            },
            # 4. Paid Acquisition (Kolet Marketing)
            {
                'source': 'facebook', 'medium': 'cpc', 
                'partner_group': 'Kolet_Direct', 'channel': 'Paid_Social',
                'weight': 0.15
            },
            # 5. Influencer / niche blog
            {
                'source': 'nomad_list_blog', 'medium': 'referral', 
                'partner_group': 'Influencer_Network', 'channel': 'Content_Partnership',
                'weight': 0.10
            }
        ]
        
        # Select a scenario based on weights
        scenario = random.choices(traffic_scenarios, weights=[x['weight'] for x in traffic_scenarios])[0]
        return scenario

    def generate_events(self):
        print("🚀 Generating synthetic freemium event data with Partner Logic...")
        
        events_data = []
        
        # Funnel Step Probabilities
        conversion_probs = {
            'landing_view': 1.0,
            'offer_view': 0.75,
            'install_success': 0.60,
            'account_activation': 0.80,
            'trial_started': 0.30, 
            'purchase_completed': 0.50 
        }

        devices = ['iOS', 'Android']
        countries = ['US', 'UK', 'FR', 'DE', 'JP', 'AE']
        variants = ['price_A', 'price_B'] # A/B Testing

        current_date = self.start_date
        while current_date <= self.end_date:
            daily_cohort_size = int(np.random.normal(self.daily_users, self.daily_users * 0.1))
            
            for _ in range(daily_cohort_size):
                user_id = str(uuid.uuid4())[:8]
                
                # GET REALISTIC TRAFFIC SOURCE
                traffic = self.get_traffic_source()
                
                user_device = np.random.choice(devices)
                user_country = np.random.choice(countries)
                user_variant = np.random.choice(variants)
                
                # Base Timestamp
                base_time = current_date + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59))
                prev_step_time = base_time
                
                # Simulate Funnel
                for step, prob in conversion_probs.items():
                    # Apply some partner-specific variance (e.g., Airlines convert better on install, worse on purchase)
                    adjusted_prob = prob
                    if traffic['partner_group'] == 'Airline_Partner_A' and step == 'install_success':
                        adjusted_prob += 0.1 # High trust
                    if traffic['partner_group'] == 'Kolet_Direct' and step == 'purchase_completed':
                        adjusted_prob -= 0.1 # Price sensitive

                    if random.random() < adjusted_prob:
                        event_time = prev_step_time + timedelta(seconds=random.randint(10, 300))
                        
                        event = {
                            'event_timestamp': event_time,
                            'date': event_time.date(),
                            'user_id': user_id,
                            'event_name': step,
                            
                            # The "Raw" GA4 Data
                            'traffic_source': traffic['source'],
                            'traffic_medium': traffic['medium'],
                            
                            # The "Mapped" Business Logic
                            'partner': traffic['partner_group'], 
                            'channel': traffic['channel'],
                            
                            'country': user_country,
                            'device': user_device,
                            'variant': user_variant,
                            'revenue_usd': 0.0,
                            'cost_usd': 0.0
                        }
                        
                        # Financials
                        if step == 'purchase_completed':
                            price = 15.00 if user_variant == 'price_A' else 12.00
                            cogs = 4.00 
                            event['revenue_usd'] = price
                            event['cost_usd'] = cogs
                        
                        events_data.append(event)
                        prev_step_time = event_time
                    else:
                        break 
            
            current_date += timedelta(days=1)
            
        df = pd.DataFrame(events_data)
        print(f"✅ Generated {len(df)} events. Mapped {len(df['traffic_source'].unique())} sources to {len(df['partner'].unique())} partners.")
        return df

if __name__ == "__main__":
    gen = FreemiumEventGenerator(days=10, daily_users=50)
    df = gen.generate_events()
    print(df[['traffic_source', 'partner', 'channel']].drop_duplicates())
