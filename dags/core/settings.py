from google.cloud import secretmanager

def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Access the secret version from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Set your GCP project ID
PROJECT_ID = "<YOUR_PROJECT_ID>"

# Fetch secrets from Secret Manager
ZILLOW_TOKEN = access_secret_version(PROJECT_ID, "ZILLOW_TOKEN")
GCS_BUCKET_NAME = access_secret_version(PROJECT_ID, "GCS_BUCKET_NAME")
DATASET_ID = access_secret_version(PROJECT_ID, "DATASET_ID")
TABLE_ID_MAIN = access_secret_version(PROJECT_ID, "TABLE_ID_MAIN")
TABLE_ID_ATTRIBUTE = access_secret_version(PROJECT_ID, "TABLE_ID_ATTRIBUTE")
TABLE_ID_PRICE = access_secret_version(PROJECT_ID, "TABLE_ID_PRICE")
TABLE_ID_RESO = access_secret_version(PROJECT_ID, "TABLE_ID_RESO")
TABLE_ID_SCHOOL = access_secret_version(PROJECT_ID, "TABLE_ID_SCHOOL")
TABLE_ID_TAX = access_secret_version(PROJECT_ID, "TABLE_ID_TAX")
REPOSITORY_ID = access_secret_version(PROJECT_ID, "REPOSITORY_ID")
WORKSPACE_ID = access_secret_version(PROJECT_ID, "WORKSPACE_ID")


SEARCH_URL = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
PROPERTY_DETAIL_URL = "https://zillow-com1.p.rapidapi.com/property"


