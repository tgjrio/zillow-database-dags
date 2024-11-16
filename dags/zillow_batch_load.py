from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
from core.api_requests import make_request_with_retries
from core.data_extraction import (
    extract_property_details,
    extract_price_history,
    extract_school_info,
    extract_attribution_info,
    extract_tax_history,
    extract_reso_facts,
    preprocess_json_file
)
from google.cloud.dataform_v1beta1.types import WorkflowInvocation

from core.storage import save_to_gcs, load_data_to_bigquery
from core import settings, regions
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformGetCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor
from datetime import datetime, timedelta

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    'zillow_batch_load',
    default_args=default_args,
    description='DAG to fetch and process Zillow data with dynamic region support',
    schedule_interval=None,
    start_date=datetime(2024, 11, 14),
    catchup=False,
) as dag:

    def fetch_zpids(location, status_type="ForSale", home_type="Houses", days_on_market="7"):
        headers = {
            "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        querystring = {
            "location": location,
            "status_type": status_type,
            "home_type": home_type,
            "daysOn": days_on_market,
            "page": 1
        }

        data = make_request_with_retries(settings.SEARCH_URL, querystring, headers, 5)
        zpid_list = []

        if data and isinstance(data, dict):
            total_pages = data.get('totalPages', 1)
            for page in range(1, total_pages + 1):
                querystring['page'] = page
                page_data = make_request_with_retries(settings.SEARCH_URL, querystring, headers, 5)
                if page_data and isinstance(page_data, dict):
                    properties = page_data.get('props', [])
                    zpid_list.extend([prop.get('zpid') for prop in properties if prop.get('zpid')])
                    logging.info(f"Fetched page {page} of {total_pages} for location '{location}'")
                else:
                    logging.warning(f"Skipping page {page} due to repeated failures or invalid data structure.")
        else:
            logging.error(f"Initial request for location '{location}' failed.")

        logging.info(f"Total zpids fetched for location '{location}': {len(zpid_list)}")
        return zpid_list

    def fetch_and_process_zpids(**kwargs):
        zpid_list = kwargs['ti'].xcom_pull(task_ids='fetch_zpids_task')
        headers = {
            "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }

        main_table_data_list = []
        price_history_data_list = []
        school_info_data_list = []
        attribution_info_list = []
        tax_history_list = []
        reso_facts_list = []

        for zpid in zpid_list:
            querystring = {"zpid": zpid}
            data = make_request_with_retries(settings.PROPERTY_DETAIL_URL, querystring, headers, 5)

            if data:
                try:
                    main_table_data_list.extend(extract_property_details(data))
                    price_history_data_list.extend(extract_price_history(data))
                    school_info_data_list.extend(extract_school_info(data))
                    attribution_info_list.extend(extract_attribution_info(data))
                    tax_history_list.extend(extract_tax_history(data))
                    reso_facts_list.extend(extract_reso_facts(data))

                    logging.info(f"Data for ZPID {zpid} processed and added to lists")
                except Exception as e:
                    logging.error(f"Error encountered during data processing for ZPID {zpid}: {e}")
            else:
                logging.warning(f"Failed to fetch or process data for ZPID {zpid}")

        if main_table_data_list:
            save_to_gcs(settings.GCS_BUCKET_NAME, "ingest/property_main/main_table_data.json", main_table_data_list)
        if price_history_data_list:
            save_to_gcs(settings.GCS_BUCKET_NAME, "ingest/property_price_history/price_history_data.json", price_history_data_list)
        if school_info_data_list:
            save_to_gcs(settings.GCS_BUCKET_NAME, "ingest/property_school_info/school_info_data.json", school_info_data_list)
        if attribution_info_list:
            save_to_gcs(settings.GCS_BUCKET_NAME, "ingest/property_attribute_info/attribution_info.json", attribution_info_list)
        if tax_history_list:
            save_to_gcs(settings.GCS_BUCKET_NAME, "ingest/property_tax_history/tax_history.json", tax_history_list)
        if reso_facts_list:
            save_to_gcs(settings.GCS_BUCKET_NAME, "ingest/property_reso_info/reso_facts.json", reso_facts_list)

        load_data_to_bigquery(f"{settings.PROJECT_ID}.{settings.DATASET_ID}.{settings.TABLE_ID_MAIN}", f"gs://{settings.GCS_BUCKET_NAME}/ingest/property_main/main_table_data.json")
        load_data_to_bigquery(f"{settings.PROJECT_ID}.{settings.DATASET_ID}.{settings.TABLE_ID_PRICE}", f"gs://{settings.GCS_BUCKET_NAME}/ingest/property_price_history/price_history_data.json")
        load_data_to_bigquery(f"{settings.PROJECT_ID}.{settings.DATASET_ID}.{settings.TABLE_ID_SCHOOL}", f"gs://{settings.GCS_BUCKET_NAME}/ingest/property_school_info/school_info_data.json")
        load_data_to_bigquery(f"{settings.PROJECT_ID}.{settings.DATASET_ID}.{settings.TABLE_ID_ATTRIBUTE}", f"gs://{settings.GCS_BUCKET_NAME}/ingest/property_attribute_info/attribution_info.json")
        load_data_to_bigquery(f"{settings.PROJECT_ID}.{settings.DATASET_ID}.{settings.TABLE_ID_TAX}", f"gs://{settings.GCS_BUCKET_NAME}/ingest/property_tax_history/tax_history.json")

        preprocess_json_file(f"gs://{settings.GCS_BUCKET_NAME}/ingest/property_reso_info/reso_facts.json", "cleaned_reso_facts.json")
        load_data_to_bigquery(f"{settings.PROJECT_ID}.{settings.DATASET_ID}.{settings.TABLE_ID_RESO}", f"gs://{settings.GCS_BUCKET_NAME}/path/to/cleaned_reso_facts.json")

    def get_locations(**kwargs):
        # Retrieve region from DAG run configuration
        dag_run_conf = kwargs.get("dag_run").conf or {}
        region_name = dag_run_conf.get("region", "ATLANTA_REGION")

        # Dynamically fetch the region from the regions module using getattr
        location_list = getattr(regions, region_name, regions.ATLANTA_REGION)
        all_zpids = []
        for location in location_list:
            logging.info(f"Fetching zpids for location: {location}")
            zpids = fetch_zpids(location)
            all_zpids.extend(zpids)
            logging.info(f"Total zpids collected so far: {len(all_zpids)}")
        return all_zpids

    fetch_zpids_task = PythonOperator(
        task_id='fetch_zpids_task',
        python_callable=get_locations,
        provide_context=True
    )

    fetch_and_process_task = PythonOperator(
        task_id='fetch_and_process_task',
        python_callable=fetch_and_process_zpids,
        provide_context=True
    )

    # Task 1: Create a Dataform compilation result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id='create_compilation_result',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        compilation_result={
            "git_commitish": "main",
            "workspace": (
                f"projects/{settings.PROJECT_ID}/locations/us-east1/repositories/{settings.REPOSITORY_ID}/"
                f"workspaces/{settings.WORKSPACE_ID}"
            ),
        }
    )

    # Task 2: Get the Dataform compilation result
    get_compilation_result = DataformGetCompilationResultOperator(
        task_id='get_compilation_result',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        compilation_result_id="{{ task_instance.xcom_pull('create_compilation_result')['name'].split('/')[-1] }}",
    )

    # Task 3: Create a Dataform workflow invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}"
        },
    )

    # Task 4: Monitor the Dataform workflow invocation until completion
    monitor_workflow_invocation = DataformWorkflowInvocationStateSensor(
        task_id='monitor_workflow_invocation',
        project_id=settings.PROJECT_ID,
        region="us-east1",
        repository_id=settings.REPOSITORY_ID,
        workflow_invocation_id="{{ task_instance.xcom_pull('create_workflow_invocation')['name'].split('/')[-1] }}",
        timeout=3600,  # Timeout in seconds
        poke_interval=30,  # Interval between status checks
        expected_statuses={WorkflowInvocation.State.SUCCEEDED}
    )


    fetch_zpids_task >> fetch_and_process_task >> create_compilation_result >> get_compilation_result >> create_workflow_invocation >> monitor_workflow_invocation