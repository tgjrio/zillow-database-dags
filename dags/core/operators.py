from .configurations import settings
import logging
from core.services.api_requests import make_request_with_retries
from core.configurations.storage import save_to_gcs, load_data_to_bigquery, load_data_to_bigquery_append
import json
from .services import dataScraper as ds
from .services import resoScraper as rs
from google.cloud import storage
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # You can change to logging.DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(funcName)s]',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def process_zpids_for_sale(**kwargs):
    bucket_name = 'zillow_raw'
    file_name = f"for_sale/zpid_list.json"

    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download and load the data
    zpid_list = json.loads(blob.download_as_string())
    logging.info(f"Retrieved {len(zpid_list)} zpids from GCS.")

    headers = {
        "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
    }

    # Initialize datasets
    datasets_main = {
        "main_table_data_list": [],
        "property_climate_data": [],
        "property_tax_data": [],
        "property_near_data": [],
        "property_price_history_data": [],
        "property_school_data": [],
        "property_detail_data": [],
        "property_list_sub_data": [],
        "property_mortgage_data": [],
        "property_reso_data": [],
    }

    datasets_reso = {
        "reso_property_basic_info": [],
        "reso_location_area_info": [],
        "reso_utility_energy_features": [],
        "reso_building_interior_features": [],
        "reso_market_pricing_info": [],
        "reso_miscellaneous_features": [],
    }

    total_properties = len(zpid_list)
    start_time = time.time()

    for idx, zpid in enumerate(zpid_list, start=1):
        # Log progress every 100 iterations
        if idx % 100 == 0:
            elapsed_time = time.time() - start_time
            logging.info(
                f"Processing ZPID {zpid} ({idx}/{total_properties}). {total_properties - idx} properties left. Elapsed time: {elapsed_time:.2f} seconds"
            )

        querystring = {"zpid": zpid}
        data = make_request_with_retries(settings.PROPERTY_DETAIL_URL, querystring, headers, 5)

        if data:
            try:
                datasets_main["main_table_data_list"].extend(ds.extract_base_data(data))
            except Exception as e:
                logging.error(f"Error during data extraction for ZPID {zpid}: {e}")
        else:
            logging.warning(f"Failed to fetch or process data for ZPID {zpid}")

    # Process extracted data into respective main datasets
    for each in datasets_main["main_table_data_list"]:
        datasets_main["property_climate_data"].extend(ds.extract_climate_data(each))
        datasets_main["property_tax_data"].extend(ds.extract_tax_history(each))
        datasets_main["property_near_data"].extend(ds.extract_nearby_homes(each))
        datasets_main["property_price_history_data"].extend(ds.extract_price_history(each))
        datasets_main["property_school_data"].extend(ds.extract_schools_nearby(each))
        datasets_main["property_detail_data"].extend(ds.extract_property_details(each))
        datasets_main["property_list_sub_data"].extend(ds.extract_listing_subtype(each))
        datasets_main["property_mortgage_data"].extend(ds.extract_mortgage_info(each))
        datasets_main["property_reso_data"].extend(ds.extract_reso_facts(each)) 

        # Extract RESO facts into property_reso_data
        datasets_main["property_reso_data"].extend(ds.extract_reso_facts(each))

    # Now process RESO datasets from property_reso_data
    for each in datasets_main["property_reso_data"]:
        datasets_reso["reso_property_basic_info"].append(rs.extract_property_basic_info(each))
        datasets_reso["reso_location_area_info"].append(rs.extract_location_area_info(each))
        datasets_reso["reso_utility_energy_features"].append(rs.extract_utilities_info(each))
        datasets_reso["reso_building_interior_features"].append(rs.extract_building_interior_info(each))
        datasets_reso["reso_market_pricing_info"].append(rs.extract_market_pricing_info(each))
        datasets_reso["reso_miscellaneous_features"].append(rs.extract_miscellaneous_info(each))

    # Save and load datasets for `DATASET_BATCH_MAIN`
    main_table_mapping = {
        "property_climate_data": "CLIMATE_INFO",
        "property_tax_data": "TAX_INFO",
        "property_school_data": "SCHOOL_INFO",
        "property_mortgage_data": "MORTGAGE_INFO",
        "property_price_history_data": "PRICE_HISTORY",
        "property_near_data": "NEARBY_HOMES",
        "property_list_sub_data": "LISTING_SUBTYPE",
        "property_detail_data": "PROPERTY_LISTINGS",
    }

    for name, table_alias in main_table_mapping.items():
        try:
            gcs_path = f"sale/{name}/data.json"
            save_to_gcs(bucket_name, gcs_path, datasets_main[name])
            load_data_to_bigquery(
                f"{settings.PROJECT_ID}.{settings.DATASET_BATCH_MAIN}.{getattr(settings, table_alias)}",
                f"gs://{bucket_name}/{gcs_path}"
            )
            logging.info(f"Successfully processed and loaded data for {name}")
        except Exception as e:
            logging.error(f"Error processing {name}: {e}")

    # Save and load datasets for `DATASET_BATCH_RESO`
    reso_table_mapping = {
        "reso_property_basic_info": "PROPERTY_INFO",
        "reso_location_area_info": "LOCATION_AREA_INFO",
        "reso_utility_energy_features": "UTILITY_ENERGY_FEATURES",
        "reso_building_interior_features": "BUILDING_INTERIOR_FEATURES",
        "reso_market_pricing_info": "MARKET_PRICING_INFO",
        "reso_miscellaneous_features": "MISCELLANEOUS_FEATURES",
    }

    for name, table_alias in reso_table_mapping.items():
        try:
            gcs_path = f"sale/{name}/data.json"
            save_to_gcs(bucket_name, gcs_path, datasets_reso[name])
            load_data_to_bigquery_append(
                f"{settings.PROJECT_ID}.{settings.DATASET_BATCH_RESO}.{getattr(settings, table_alias)}",
                f"gs://{bucket_name}/{gcs_path}"
            )
            logging.info(f"Successfully processed and loaded data for {name}")
        except Exception as e:
            logging.error(f"Error processing {name}: {e}")

def process_zpids_recently_sold(**kwargs):
    bucket_name = 'zillow_raw'
    file_name = f"recently_sold/zpid_list.json"

    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Download and load the data
    zpid_list = json.loads(blob.download_as_string())
    logging.info(f"Retrieved {len(zpid_list)} zpids from GCS.")

    headers = {
        "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
    }

    # Initialize datasets
    datasets_main = {
        "main_table_data_list": [],
        "property_climate_data": [],
        "property_tax_data": [],
        "property_near_data": [],
        "property_price_history_data": [],
        "property_school_data": [],
        "property_detail_data": [],
        "property_list_sub_data": [],
        "property_mortgage_data": [],
        "property_reso_data": [],
    }

    datasets_reso = {
        "reso_property_basic_info": [],
        "reso_location_area_info": [],
        "reso_utility_energy_features": [],
        "reso_building_interior_features": [],
        "reso_market_pricing_info": [],
        "reso_miscellaneous_features": [],
    }

    total_properties = len(zpid_list)
    start_time = time.time()

    for idx, zpid in enumerate(zpid_list, start=1):
        # Log progress every 100 iterations
        if idx % 100 == 0:
            elapsed_time = time.time() - start_time
            logging.info(
                f"Processing ZPID {zpid} ({idx}/{total_properties}). {total_properties - idx} properties left. Elapsed time: {elapsed_time:.2f} seconds"
            )

        querystring = {"zpid": zpid}
        data = make_request_with_retries(settings.PROPERTY_DETAIL_URL, querystring, headers, 5)

        if data:
            try:
                datasets_main["main_table_data_list"].extend(ds.extract_base_data(data))
            except Exception as e:
                logging.error(f"Error during data extraction for ZPID {zpid}: {e}")

    # Process extracted data into main datasets
    for each in datasets_main["main_table_data_list"]:
        datasets_main["property_climate_data"].extend(ds.extract_climate_data(each))
        datasets_main["property_tax_data"].extend(ds.extract_tax_history(each))
        datasets_main["property_near_data"].extend(ds.extract_nearby_homes(each))
        datasets_main["property_price_history_data"].extend(ds.extract_price_history(each))
        datasets_main["property_school_data"].extend(ds.extract_schools_nearby(each))
        datasets_main["property_detail_data"].extend(ds.extract_property_details(each))
        datasets_main["property_list_sub_data"].extend(ds.extract_listing_subtype(each))
        datasets_main["property_mortgage_data"].extend(ds.extract_mortgage_info(each))
        datasets_main["property_reso_data"].extend(ds.extract_reso_facts(each))

    # Process RESO datasets from property_reso_data
    for each in datasets_main["property_reso_data"]:
        datasets_reso["reso_property_basic_info"].append(rs.extract_property_basic_info(each))
        datasets_reso["reso_location_area_info"].append(rs.extract_location_area_info(each))
        datasets_reso["reso_utility_energy_features"].append(rs.extract_utilities_info(each))
        datasets_reso["reso_building_interior_features"].append(rs.extract_building_interior_info(each))
        datasets_reso["reso_market_pricing_info"].append(rs.extract_market_pricing_info(each))
        datasets_reso["reso_miscellaneous_features"].append(rs.extract_miscellaneous_info(each))

    # Save and load datasets for `DATASET_BATCH_MAIN`
    main_table_mapping = {
        "property_climate_data": "CLIMATE_INFO",
        "property_tax_data": "TAX_INFO",
        "property_school_data": "SCHOOL_INFO",
        "property_mortgage_data": "MORTGAGE_INFO",
        "property_price_history_data": "PRICE_HISTORY",
        "property_near_data": "NEARBY_HOMES",
        "property_list_sub_data": "LISTING_SUBTYPE",
        "property_detail_data": "PROPERTY_LISTINGS",
    }

    for name, table_alias in main_table_mapping.items():
        try:
            gcs_path = f"sold/{name}/data.json"
            save_to_gcs(bucket_name, gcs_path, datasets_main[name])
            load_data_to_bigquery(
                f"{settings.PROJECT_ID}.{settings.DATASET_BATCH_MAIN}.{getattr(settings, table_alias)}",
                f"gs://{bucket_name}/{gcs_path}"
            )
            logging.info(f"Successfully processed and loaded data for {name}")
        except Exception as e:
            logging.error(f"Error processing {name}: {e}")

    # Save and load datasets for `DATASET_BATCH_RESO`
    reso_table_mapping = {
        "reso_property_basic_info": "PROPERTY_INFO",
        "reso_location_area_info": "LOCATION_AREA_INFO",
        "reso_utility_energy_features": "UTILITY_ENERGY_FEATURES",
        "reso_building_interior_features": "BUILDING_INTERIOR_FEATURES",
        "reso_market_pricing_info": "MARKET_PRICING_INFO",
        "reso_miscellaneous_features": "MISCELLANEOUS_FEATURES",
    }

    for name, table_alias in reso_table_mapping.items():
        try:
            gcs_path = f"sold/{name}/data.json"
            save_to_gcs(bucket_name, gcs_path, datasets_reso[name])
            load_data_to_bigquery_append(
                f"{settings.PROJECT_ID}.{settings.DATASET_BATCH_RESO}.{getattr(settings, table_alias)}",
                f"gs://{bucket_name}/{gcs_path}"
            )
            logging.info(f"Successfully processed and loaded data for {name}")
        except Exception as e:
            logging.error(f"Error processing {name}: {e}")