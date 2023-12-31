from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
    Metric,
    MetricType,)
from google.cloud import bigquery
import Working.credentials as credentials

# Path to your service account keys
ga_service_account_path = 'ecocare-ads-data-e8c8eaeeb5de.json'
bq_service_account_path = 'ecocare-ads-data-e8c8eaeeb5de.json'
property_id = credentials.ga4_property_id
project_id = 'ecocare-ads-data'

# Authenticate with Google Analytics Data API
ga_credentials = service_account.Credentials.from_service_account_file(ga_service_account_path)
ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)

# Authenticate with BigQuery
bq_credentials = service_account.Credentials.from_service_account_file(bq_service_account_path)
bq_client = bigquery.Client(credentials=bq_credentials, project=bq_credentials.project_id)

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
        date_ranges=[DateRange(start_date="2022-11-01", end_date="2023-10-31")],
    )

# Run GA4 report
response = ga_client.run_report(report_request)

print(response)

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
# print(data_for_bq)

# Define BigQuery dataset and table
dataset_id = 'ecocare-ads-data.ecocare_ads_historical'
table_id = 'ecocare-ads-data.ecocare_ads_historical.ecocare_ga4_historical'

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
