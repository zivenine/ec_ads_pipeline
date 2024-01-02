import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
from google.oauth2 import service_account

# Secret access for get_fb
def access_fb_app_secret():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Name of the secret and the project
    name = "projects/639850821884/secrets/ecocare_fb_app_secret/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Extract the payload as a string.
    app_secret_string = response.payload.data.decode("UTF-8")

    return app_secret_string

def access_fb_access_token():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Name of the secret and the project
    name = "projects/639850821884/secrets/ecocare_fb_access_token/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Extract the payload as a string.
    access_token_string = response.payload.data.decode("UTF-8")

    return access_token_string

def access_ads_service_account():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Name of the secret and the project
    name = "projects/639850821884/secrets/ecocare-ads-data-26533bc415de/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Extract the payload as a string.
    gbq_service_account_string = response.payload.data.decode("UTF-8")

    return gbq_service_account_string

def get_fb():
    # Access the secret 
    app_secret = access_fb_app_secret()
    access_token = access_fb_access_token()
    service_account_info = access_ads_service_account()
    service_account_json = json.loads(service_account_info)

    # Replace with your own credentials
    my_app_id = '674029601373497'
    my_app_secret = app_secret
    my_access_token = access_token

    FacebookAdsApi.init(my_app_id, my_app_secret, my_access_token)

    # Step 1: Pull data from Facebook Marketing API

    # Create the previous month date range
    today = datetime.today()
    first_day_current_month = today.replace(day=1)
    first_day_previous_month = (first_day_current_month - relativedelta(months=1)).date()
    last_day_previous_month = (first_day_current_month - relativedelta(days=1)).date()
    first_day_previous_month_iso = first_day_previous_month.isoformat()
    last_day_previous_month_iso = last_day_previous_month.isoformat()

    ad_account_id = '482437558918376'
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
        'time_range': {'since': first_day_previous_month_iso,
                    'until': last_day_previous_month_iso},
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

    # Use the service account info to create the client
    serv_acc_creds = service_account.Credentials.from_service_account_info(service_account_json)
    client = bigquery.Client(credentials=serv_acc_creds)
    table_id = "ecocare-ads-data.ecocare_ads_data.ecocare_facebook_ads_campaign"

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
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()


get_fb()