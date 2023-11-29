from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
from google.cloud import bigquery

# Path to your service account keys
ga_service_account_path = 'ecocare-ads-data-e8c8eaeeb5de.json'
bq_service_account_path = 'ecocare-ads-data-e8c8eaeeb5de.json'

# Authenticate with Google Analytics Data API
ga_credentials = service_account.Credentials.from_service_account_file(ga_service_account_path)
ga_client = BetaAnalyticsDataClient(credentials=ga_credentials)

# Authenticate with BigQuery
bq_credentials = service_account.Credentials.from_service_account_file(bq_service_account_path)
bq_client = bigquery.Client(credentials=bq_credentials, project=bq_credentials.project_id)

# Define GA4 report request
report_request = RunReportRequest(
    property='properties/YOUR_GA4_PROPERTY_ID',
    dimensions=[{'name': 'city'}],
    metrics=[{'name': 'activeUsers'}],
    date_ranges=[{'start_date': '2020-03-31', 'end_date': '2020-04-01'}]
)

# Run GA4 report
response = ga_client.run_report(report_request)

# Extract data
data_for_bq = []
for row in response.rows:
    data_for_bq.append({"city": row.dimension_values[0].value, "active_users": int(row.metric_values[0].value)})
    
print(data_for_bq)

# Define BigQuery dataset and table
dataset_id = 'ecocare-ads-data.ecocare_ads_data'
table_id = 'ecocare-ads-data.ecocare_ads_data.ecocare_ga4_all_sources'

# Create a BigQuery dataset if not exists
dataset = bigquery.Dataset(f"{bq_client.project}.{dataset_id}")
dataset.location = "US"
bq_client.create_dataset(dataset, exists_ok=True)

# Create a BigQuery table if not exists
schema = [
    bigquery.SchemaField("city", "STRING"),
    bigquery.SchemaField("active_users", "INTEGER"),
]
table = bigquery.Table(f"{bq_client.project}.{dataset_id}.{table_id}", schema=schema)
table = bq_client.create_table(table, exists_ok=True)

# Insert data into BigQuery
errors = bq_client.insert_rows_json(table, data_for_bq)
if errors == []:
    print("Data loaded into BigQuery successfully.")
else:
    print("Errors occurred while loading data into BigQuery:", errors)
