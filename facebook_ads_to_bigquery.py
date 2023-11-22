import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
from datetime import datetime, timedelta
import credentials

# Replace with your own credentials
my_app_id = credentials.facebook_app_id
my_app_secret = credentials.facebook_app_secret
my_access_token = credentials.facebook_access_token

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# Step 1: Pull data from Facebook Marketing API

ad_account_id = credentials.facebook_ad_account_id
ad_account = AdAccount('act_{}'.format(ad_account_id))

fields = [
    AdsInsights.Field.date_start,
    AdsInsights.Field.date_stop,
    AdsInsights.Field.account_id,
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.adset_name, 
    AdsInsights.Field.ad_name,
    AdsInsights.Field.impressions,
    AdsInsights.Field.clicks,
    AdsInsights.Field.ctr,
    AdsInsights.Field.cpc,
    AdsInsights.Field.conversions,
    AdsInsights.Field.spend,
]
params = {
    'time_increment': 1,
    'time_range': {'since': (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d'),
                   'until': datetime.now().strftime('%Y-%m-%d')},
}

data = ad_account.get_insights(fields=fields, params=params)

# Step 2: Convert JSON object to DataFrame

df_data = []
for entry in data:
    df_data.append({
        'campaign_name': entry['campaign_name'],
        'conversions': entry['conversions'][0]['7d_click'] if 'conversions' in entry else 0,
        'spend': entry['spend'],
    })

df = pd.DataFrame(df_data)

# Step 3: Send DataFrame to BigQuery

# Use a service account JSON file for authentication
client = bigquery.Client.from_service_account_json(
    'path/to/your/service-account-file.json'
)
table_id = "your-project.your-dataset.your-table"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("campaign_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("conversions", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("spend", bigquery.enums.SqlTypeNames.FLOAT64),
    ],
    # overwrite existing table data
    write_disposition="WRITE_TRUNCATE",
)

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)
job.result()