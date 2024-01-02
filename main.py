from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,)
from google.cloud import bigquery
from google.cloud import secretmanager
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
import time
import json

"""
The snippets which manage the secrets for credentials
"""
# Secret access for get_ga4
def access_ga4_service_account():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Name of the secret and the project
    name = "projects/639850821884/secrets/ecocare-ads-data-e8c8eaeeb5de/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Extract the payload as a string.
    secret_string_1 = response.payload.data.decode("UTF-8")

    return secret_string_1

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

""""
The snippet to send GA4 data to BigQuery. 
"""
def get_ga4():
    # Access the secret 
    secret = access_ga4_service_account()
    
    # Path to your service account keys
    ga_service_account_path = secret
    ga_service_account_json = json.loads(ga_service_account_path)
    bq_service_account_path = secret
    bq_service_account_json = json.loads(bq_service_account_path)
    property_id = '287076883'

    # Authenticate with Google Analytics Data API
    ga_credentials = service_account.Credentials.from_service_account_info(ga_service_account_json)
    ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)

    # Authenticate with BigQuery
    bq_credentials = service_account.Credentials.from_service_account_info(bq_service_account_json)
    bq_client = bigquery.Client(credentials=bq_credentials, project=bq_credentials.project_id)

    # Create the previous month date range
    today = datetime.today()
    first_day_current_month = today.replace(day=1)
    first_day_previous_month = str((first_day_current_month - relativedelta(months=1)).date())
    last_day_previous_month = str((first_day_current_month - relativedelta(days=1)).date())

    # Define GA4 report request
    report_request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date"),
                    Dimension(name="sessionSource"),
                    Dimension(name="sessionMedium")],
        metrics=[
            Metric(name="totalUsers"),
            Metric(name="activeUsers"),
            Metric(name="newUsers"),
            Metric(name="addToCarts"),
            Metric(name="checkouts"),
            Metric(name="transactions"),
            Metric(name="conversions"),
            Metric(name="userConversionRate"),
            Metric(name="totalRevenue"),
        ],
        date_ranges=[DateRange(start_date=first_day_previous_month, end_date=last_day_previous_month)],
    )

    # Run GA4 report
    response = ga_client.run_report(report_request)

    # print(response)

    # Extract data
    data_for_bq = []
    for row in response.rows:
        data_for_bq.append({"date": row.dimension_values[0].value,
                            "sessionSource": row.dimension_values[1].value,
                            "sessionMedium": row.dimension_values[2].value,
                            "totalUsers": int(row.metric_values[0].value),
                            "active_users": int(row.metric_values[1].value),
                            "newUsers": int(row.metric_values[2].value),
                            "addToCarts": int(row.metric_values[3].value),
                            "checkouts": int(row.metric_values[4].value),
                            "transactions": float(row.metric_values[5].value),
                            "conversions": float(row.metric_values[6].value),
                            "userConversionRate": float(row.metric_values[7].value),
                            "totalRevenue": float(row.metric_values[8].value)})  

    # Define BigQuery dataset and table
    table_id = 'ecocare-ads-data.ecocare_ads_data.ecocare_ga4_all_sources'

    # Create a BigQuery dataset if not exists
    # dataset = bigquery.Dataset(f"{dataset_id}")
    # dataset.location = "US"
    # bq_client.create_dataset(dataset, exists_ok=True)

    # Create a BigQuery table if not exists
    schema = [
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("sessionSource", "STRING"),
        bigquery.SchemaField("sessionMedium", "STRING"),
        bigquery.SchemaField("totalUsers", "INTEGER"),
        bigquery.SchemaField("active_users", "INTEGER"),
        bigquery.SchemaField("newUsers", "INTEGER"),
        bigquery.SchemaField("addToCarts", "INTEGER"),
        bigquery.SchemaField("checkouts", "INTEGER"),
        bigquery.SchemaField("transactions", "FLOAT"),
        bigquery.SchemaField("conversions", "FLOAT"),
        bigquery.SchemaField("userConversionRate", "FLOAT"),
        bigquery.SchemaField("totalRevenue", "FLOAT")
    ]
    table = bigquery.Table(f"{table_id}", schema=schema)
    # table = bq_client.create_table(table, exists_ok=True)

    # Insert data into BigQuery
    errors = bq_client.insert_rows_json(table, data_for_bq)
    if errors == []:
        print("Data loaded into BigQuery successfully.")
    else:
        print("Errors occurred while loading data into BigQuery:", errors)

"""
The snippet which sends data from Facebook to BigQuery
"""
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


# Call the function
def get_the_data():
    get_ga4()
    time.sleep(300)
    get_fb()


get_the_data()