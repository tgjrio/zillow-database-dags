import os
import json
import logging
from google.cloud import storage, bigquery

# Initialize clients
bq_client = bigquery.Client()
gcs_client = storage.Client()

def save_to_gcs(bucket_name, prefix, data):
    """Save data as a JSON file to GCS."""
    try:
        bucket = gcs_client.bucket(bucket_name)
        blob = bucket.blob(prefix)
        
        # Write data to a temporary local file
        temp_file = f"/tmp/{prefix.replace('/', '_')}.json"
        with open(temp_file, 'w') as f:
            for record in data:
                f.write(json.dumps(record) + '\n')  # Newline-delimited JSON

        # Upload the file to GCS
        blob.upload_from_filename(temp_file)
        logging.info(f"Data saved to GCS bucket {bucket_name} with prefix {prefix}")
        
        # Clean up the temporary file
        os.remove(temp_file)
    except Exception as e:
        logging.error(f"Error saving data to GCS: {e}")

def load_data_to_bigquery(table_id, gcs_uri):
    """Load data from GCS to BigQuery with WRITE_TRUNCATE mode."""
    try:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )
        load_job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete
        if load_job.errors:
            logging.error(f"Errors during load to {table_id}: {load_job.errors}")
        else:
            logging.info(f"Data successfully loaded to {table_id}")
    except Exception as e:
        logging.error(f"Error loading data to BigQuery: {e}")