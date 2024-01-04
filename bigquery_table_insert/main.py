from google.cloud import bigquery 
from google.cloud import secretmanager 
from google.oauth2 import service_account
import json
import time

"""
Service accounts
"""
# Access GA4 service account from service file info in secret manager 
def access_ga4_service_acc():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Name of the secret and the project
    name = "projects/639850821884/secrets/ecocare-ads-data-e8c8eaeeb5de/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Extract the payload as a string.
    ga4_service_acc_string = response.payload.data.decode("UTF-8")

    return ga4_service_acc_string


# Access Ads service account from service file info in secret manager
def access_ads_service_acc():
    # Create the Secret Manager client.
    client = secretmanager.SecretManagerServiceClient()

    # Name of the secret and the project
    name = "projects/639850821884/secrets/ecocare-ads-data-26533bc415de/versions/latest"

    # Access the secret version.
    response = client.access_secret_version(request={"name": name})

    # Extract the payload as a string.
    ads_service_acc_string = response.payload.data.decode("UTF-8")

    return ads_service_acc_string


"""
Functions to transfer the data
"""
# Function to transfer GA4 data from monthly table to historical table 
def append_ga4_data():
    secret = access_ga4_service_acc()

    # Path to your service account keys
    ga4_service_account_path = secret
    ga4_service_account_json = json.loads(ga4_service_account_path)

    # Construct a BigQuery client object.
    ga4_credentials = service_account.Credentials.from_service_account_info(ga4_service_account_json)
    ga4_client = bigquery.Client(credentials=ga4_credentials, project=ga4_credentials.project_id)

    # Your query to append data from the source table to the destination table
    query = """
    INSERT INTO `ecocare-ads-data.ecocare_ads_historical.ecocare_ga4_historical`
    SELECT * FROM `ecocare-ads-data.ecocare_ads_data.ecocare_ga4_all_sources`
    WHERE id = 2;
    """

    # Start the query, passing in the necessary configuration
    query_job = ga4_client.query(query)

    try:
        # Wait for the query to complete
        query_job.result()
        print("GA4 query completed successfully")
    except Exception as e:
        print("GA4 query failed: ", e)


# Function to transfer Facebook ads data from monthly table to historical table 
def append_facebook_data():
    secret = access_ads_service_acc()
    
    # Path to your service account keys
    ads_service_account_path = secret
    ads_service_account_json = json.loads(ads_service_account_path)
    
    # Construct a BigQuery client object.
    ads_credentials = service_account.Credentials.from_service_account_info(ads_service_account_json)
    ads_client = bigquery.Client(credentials=ads_credentials, project=ads_credentials.project_id)

    # Your query to append data from the source table to the destination table
    query = """
    INSERT INTO `ecocare-ads-data.ecocare_ads_historical.ecocare_facebook_historical`
    SELECT * FROM `ecocare-ads-data.ecocare_ads_data.ecocare_facebook_ads_campaign`
    WHERE id = 2;
    """

    # Start the query, passing in the necessary configuration
    query_job = ads_client.query(query)

    try:
        # Wait for the query to complete
        query_job.result()
        print("Facebook query completed successfully")
    except Exception as e:
        print("Facebook query failed: ", e)

# Function to run both GA4 and Facebook ads data transfer functions 
def append_data(request):
    append_ga4_data()
    time.sleep(60)
    append_facebook_data()
    return "Tables transferred successfully"