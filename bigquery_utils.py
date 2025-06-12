import os
import json
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import secretmanager
from config import BIGQUERY_SERVICE_ACCOUNT_KEY_PATH, BIGQUERY_PROJECT_ID, SECRET_ID

logger = logging.getLogger(__name__)

_bq_client = None

def get_bigquery_client():
    """Initializes and returns a BigQuery client."""
    global _bq_client
    if _bq_client is None:
        try:
            if os.path.exists(BIGQUERY_SERVICE_ACCOUNT_KEY_PATH):
                logger.info(f"Using service account key from {BIGQUERY_SERVICE_ACCOUNT_KEY_PATH}")
                credentials = service_account.Credentials.from_service_account_file(
                    BIGQUERY_SERVICE_ACCOUNT_KEY_PATH
                )
                _bq_client = bigquery.Client(
                    project=BIGQUERY_PROJECT_ID, # Use explicit project ID if configured
                    credentials=credentials
                )
            else:
                logger.warning(f"Service account key file not found at {BIGQUERY_SERVICE_ACCOUNT_KEY_PATH}. Attempting default credentials.")
                # Fallback to default credentials (e.g., Cloud Run's service account)
                _bq_client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            raise
    return _bq_client

def read_secret(config):
    path = f"projects/{SECRET_ID}/secrets/{config}/versions/latest"
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(request={"name": path})
    return response.payload.data.decode("UTF-8")

def get_bigquery_client_from_secret():
    """Initializes and returns a BigQuery client using a secret from Secret Manager."""
    global _bq_client
    if _bq_client is None:
        try:
            _bq_client = bigquery.Client.from_service_account_json("service_account.json")
            # _bq_client = bigquery.Client.from_service_account_info(json.loads(read_secret("airflow-bigquery-config")))
        except Exception as e:
            logger.error(f"Error reading secret for BigQuery client: {e}")
            raise
    else:
        logger.info(f"Re-Using BigQuery client")
    return _bq_client


def execute_bigquery_query(query: str, project_id: str = None): # type: ignore
    """Executes a BigQuery query."""
    client = get_bigquery_client()
    try:
        logger.info(f"Executing BigQuery query:\n{query}")
        # If project_id is provided, use it for the query context
        if project_id:
            job_config = bigquery.QueryJobConfig(default_dataset_project=project_id)
            query_job = client.query(query, job_config=job_config)
        else:
            query_job = client.query(query)
        
        results = query_job.result()  # Waits for the job to complete
        logger.info(f"BigQuery query completed. Rows affected/processed: {results.total_rows}")
        
        # You might want to return results or just success status
        return True, results.total_rows # Or a list of rows
    except Exception as e:
        logger.error(f"Error executing BigQuery query: {e}")
        return False, str(e)
    
def execute_raw_query(query):
    """Executes a raw BigQuery query and returns the results."""
    client = get_bigquery_client_from_secret()
    try:
        logger.info(f"Executing raw BigQuery query:\n{query}")
        query_job = client.query(query)
        results = query_job.result()  # Waits for job to complete.
        if query_job.num_dml_affected_rows is not None:
            affected_rows = query_job.num_dml_affected_rows
        else:
            affected_rows = results.total_rows
        logger.info(f"Query completed. Rows affected/processed: {affected_rows}")
        return True, affected_rows
    except Exception as e:
        logger.error(f"Error executing raw BigQuery query: {e}")
        raise