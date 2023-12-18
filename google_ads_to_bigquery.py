import logging
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from google.oauth2 import service_account
import credentials

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def google_ads_to_bigquery():
    try:
        # Initialize Google Ads Client
        google_ads_client = GoogleAdsClient.load_from_storage('google_ads_config.yaml')

        # Initialize BigQuery Client
        creds = service_account.Credentials.from_service_account_file('ecocare-ads-data-26533bc415de.json')
        bigquery_client = bigquery.Client(credentials=creds, project=credentials.bq_project_id)

        # Define GAQL Query
        query = """
            SELECT campaign.id, campaign.name, metrics.impressions, metrics.clicks, metrics.ctr, metrics.average_cpc,
                metrics.conversions, metrics.cost_per_conversion, metrics.cost_micros
            FROM campaign
            WHERE segments.date DURING LAST_30_DAYS
        """

        # Fetch Data from Google Ads
        # search_request = google_ads_client.service.google_ads.search(
        #    customer_id=credentials.google_ads_customer_id,
        #    query=query
        #)
        
        # Fetch Data from Google Ads using Search
        google_ads_service = google_ads_client.get_service('GoogleAdsService', version='v15')  # Replace 'vX' with the appropriate API version
        search_request = google_ads_service.search(customer_id=credentials.google_ads_customer_id, query=query)


        # Prepare Data for BigQuery
        rows_to_insert = []
        for row in search_request:
            campaign_data = {
                'campaign_id': row.campaign.id,
                'campaign_name': row.campaign.name,
                'impressions': row.metrics.impressions,
                'clicks': row.metrics.clicks,
                'ctr': row.metrics.ctr,
                'cpc': row.metrics.average_cpc,
                'conv': row.metrics.conversions,
                'cost_per_conv': row.metrics.cost_per_conversion,
                'spend': row.cost_micros
            }
            rows_to_insert.append(campaign_data)

        # Send Data to BigQuery
        dataset_id = credentials.google_ads_dataset_id
        table_id = credentials.google_ads_table_id
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)
        errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

        # Error Handling for BigQuery Insertion
        if errors:
            logging.error(f'Errors occurred while inserting rows: {errors}')
        else:
            logging.info('Data successfully inserted into BigQuery.')

    except GoogleAdsException as google_ads_error:
        # Handle Google Ads API errors
        logging.error(f'Request with ID "{google_ads_error.request_id}" failed with status '
                      f'"{google_ads_error.error.code().name}" and includes the following errors:')
        for error in google_ads_error.failure.errors:
            logging.error(f'\tError with message "{error.message}".')
            if error.location:
                for field_path_element in error.location.field_path_elements:
                    logging.error(f'\t\tOn field: {field_path_element.field_name}')
    except GoogleCloudError as bigquery_error:
        # Handle BigQuery errors
        logging.error(f'An error occurred with BigQuery: {bigquery_error}')
    except Exception as e:
        # Handle other exceptions
        logging.error(f'An unexpected error occurred: {e}')

google_ads_to_bigquery()