import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import credentials

# Replace with your own credentials
my_app_id = credentials.facebook_app_id
my_app_secret = credentials.facebook_app_secret
my_access_token = credentials.facebook_access_token

FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

# Step 1: Pull data from Facebook Marketing API

# Create the previous month date range
today = datetime.today()
first_day_current_month = today.replace(day=1)
first_day_previous_month = (first_day_current_month - relativedelta(months=1)).date()
last_day_previous_month = (first_day_current_month - relativedelta(days=1)).date()
first_day_previous_month_iso = first_day_previous_month.isoformat()
last_day_previous_month_iso = last_day_previous_month.isoformat()


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
    'level': 'ad',
    'time_increment': 1,
    'time_range': {'since': "2023-07-01",
                   'until': "2023-10-31"},
}

data = ad_account.get_insights(fields=fields, params=params)

# print(data)

# Step 2: Convert JSON object to DataFrame

df_data = []
for entry in data:
    df_data.append({
        'date_start': entry['date_start'],
        'date_stop': entry['date_stop'],
        'account_id': entry['account_id'],
        'campaign_name': entry['campaign_name'],
        'adset_name': entry['adset_name'],
        'impressions': entry['impressions'],
        'clicks': entry['clicks'],
        'ctr': entry['ctr'],
        'cpc': entry['cpc'] if 'cpc' in entry else 0,
        'conversions': entry['conversions'][0]['7d_click'] if 'conversions' in entry else 0,
        'spend': entry['spend'],
    })

df = pd.DataFrame(df_data)

# Step 2.5: Explicitly specify the datatypes for int features

df['impressions'] = df['impressions'].astype(int)
df['clicks'] = df['clicks'].astype(int)
df['conversions'] = df['conversions'].astype(int)
df['ctr'] = df['ctr'].astype(float)
df['cpc'] = df['cpc'].astype(float)
df['spend'] = df['spend'].astype(float)

# Step 3: Send DataFrame to BigQuery

# Use a service account JSON file for authentication
client = bigquery.Client.from_service_account_json(
    'ecocare-ads-data-26533bc415de.json'
)
table_id = "ecocare-ads-data.ecocare_ads_historical.ecocare_facebook_historical"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("date_start", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("date_stop", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("account_id", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("campaign_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("adset_name", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("impressions", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("clicks", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("ctr", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("cpc", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("conversions", bigquery.enums.SqlTypeNames.INT64),
        bigquery.SchemaField("spend", bigquery.enums.SqlTypeNames.FLOAT64),
    ],
    # overwrite existing table data
    write_disposition="WRITE_APPEND",
)

job = client.load_table_from_dataframe(
    df, table_id, job_config=job_config
)
job.result()