from google.cloud import secretmanager
from dotenv import load_dotenv
import os
# Load environment variables
load_dotenv()

def access_secret_version(project_id, secret_id, version_id="latest"):
    """
    Access the secret version from Google Cloud Secret Manager.
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# Set your GCP project ID
PROJECT_ID = os.environ['PROJECT_ID']

# Fetch secrets from Secret Manager
ZILLOW_TOKEN = access_secret_version(PROJECT_ID, "ZILLOW_TOKEN")
GCS_BUCKET_NAME = access_secret_version(PROJECT_ID, "GCS_BUCKET_NAME")

REPOSITORY_ID = access_secret_version(PROJECT_ID, "REPOSITORY_ID")
WORKSPACE_ID = access_secret_version(PROJECT_ID, "WORKSPACE_ID")

SEARCH_URL = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
PROPERTY_DETAIL_URL = "https://zillow-com1.p.rapidapi.com/property"

DATASET_BATCH_MAIN = os.environ['DATASET_BATCH_MAIN']
# ---------------------------------------------- #
CLIMATE_INFO = os.environ['CLIMATE_INFO']
LISTING_SUBTYPE = os.environ['LISTING_SUBTYPE']
MORTGAGE_INFO = os.environ['MORTGAGE_INFO']
NEARBY_HOMES = os.environ['NEARBY_HOMES']
PRICE_HISTORY = os.environ['PRICE_HISTORY']
PROPERTY_LISTINGS = os.environ['PROPERTY_LISTINGS']
SCHOOL_INFO = os.environ['SCHOOL_INFO']
TAX_INFO = os.environ['TAX_INFO']

DATASET_BATCH_RESO = os.environ['CLIMATE_INFO']
# ---------------------------------------------- #
AMENITIES_COMMUNITY_FEATURES = os.environ['AMENITIES_COMMUNITY_FEATURES']
ASSOCIATIONS_FEES = os.environ['ASSOCIATIONS_FEES']
BUILDING_INTERIOR_FEATURES = os.environ['BUILDING_INTERIOR_FEATURES']
LOCATION_AREA_INFO = os.environ['LOCATION_AREA_INFO']
MARKET_PRICING_INFO = os.environ['MARKET_PRICING_INFO']
MISCELLANEOUS_FEATURES = os.environ['MISCELLANEOUS_FEATURES']
PROPERTY_INFO = os.environ['PROPERTY_INFO']
UTILITY_ENERGY_FEATURES = os.environ['UTILITY_ENERGY_FEATURES']
RESO_NESTED_INFO = os.environ['RESO_NESTED_INFO']