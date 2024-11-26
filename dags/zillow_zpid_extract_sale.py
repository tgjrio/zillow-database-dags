from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

import logging
from datetime import datetime, timedelta
from dags.core.zillow.fetch import fetch_zpids_for_sale
from core.zillow.configurations import regions
from google.cloud import storage   
import json

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'zillow_zpid_extract_sale',
    default_args=default_args,
    description='DAG to fetch and process Zillow data with dynamic region support',
    schedule_interval='0 5 * * * ', # Runs at 10:00 AM UTC (5:00 AM Eastern Standard Time)
    start_date=datetime(2024, 11, 14),
    catchup=False,

) as dag:

    def get_locations(**kwargs):
        # Retrieve region and home_type from DAG run configuration
        dag_run_conf = kwargs.get("dag_run").conf or {}
        region_name = dag_run_conf.get("region", "US_CITIES")
        days_on_market = dag_run_conf.get("days_on_market", '1')
        status_type = dag_run_conf.get("status_type", "ForSale")

        # Dynamically fetch the region from the regions module using getattr
        location_list = getattr(regions, region_name, regions.US_CITIES)
        all_zpids = []
        for location in location_list:
            logging.info(f"Fetching zpids for location: {location}")
            zpids = fetch_zpids_for_sale(location, days_on_market=days_on_market, status_type=status_type)
            all_zpids.extend(zpids)
            logging.info(f"Total zpids collected so far: {len(all_zpids)}")
        
        # Save the all_zpids list to GCS as a JSON file
        bucket_name = "zillow_raw"
        file_name = f"for_sale/zpid_list.json"

        # Initialize the GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Convert list to JSON and upload
        blob.upload_from_string(
            data=json.dumps(all_zpids),
            content_type='application/json'
        )

    fetch_zpids_task = PythonOperator(
        task_id='fetch_zpid_for_sale',
        python_callable=get_locations,
        provide_context=True
    )

    trigger_downstream_dag = TriggerDagRunOperator(
        task_id='trigger-zillow-for-sale',
        trigger_dag_id='zillow_for_sale'
    )

    fetch_zpids_task >> trigger_downstream_dag
