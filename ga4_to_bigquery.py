from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest
from google.oauth2 import service_account
from google.cloud import storage
import json

# Replace with your GA4 property ID and your bucket name
GA_PROPERTY_ID = 'YOUR-GA4-PROPERTY-ID'
BUCKET_NAME = 'your-bucket-name'
KEY_PATH = 'ecocare-ads-data-e8c8eaeeb5de.json'
FILE_NAME = 'ga4_metrics.json'

# Authenticate with Google Analytics Data API
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
ga_client = BetaAnalyticsDataClient(credentials=credentials)

# Define the report request for GA4
report_request = RunReportRequest(
    property=f"properties/{GA_PROPERTY_ID}",
    dimensions=[{'name': 'date'},
                {'name': 'firstUserSourceMedium'}],
    metrics=[{'name': 'totalUsers'},
             {'name': 'conversions'}],
    date_ranges=[{'start_date': '2021-01-01', 'end_date': 'today'}]
)

# Run the report request
try:
    response = ga_client.run_report(report_request)
    print("Report result:")
    print(response)

    # Convert the response to JSON
    report_data = []
    for row in response.rows:
        report_data.append({
            "date": row.dimension_values[0].value,
            "source/medium": row.dimension_values[1].value,
            "totalUsers": row.metric_values[0].value,
            "conversions": row.metric_values[1].value
        })
    report_json = json.dumps(report_data, indent=2)

    # Now let's upload this data to Google Cloud Storage
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(FILE_NAME)

    # Upload the JSON data
    blob.upload_from_string(report_json, content_type='application/json')
    print(f"Data uploaded to {BUCKET_NAME}/{FILE_NAME} in Google Cloud Storage")

except Exception as e:
    print(f"An error occurred: {e}")
