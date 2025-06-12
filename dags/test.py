import os
from google.cloud import bigquery
from google.cloud import secretmanager
import json
from config import SECRET_ID

def read_secret(config) -> str:
    path = f"projects/{SECRET_ID}/secrets/{config}/versions/latest"
    secret_client = secretmanager.SecretManagerServiceClient()
    response = secret_client.access_secret_version(request={"name": path})
    return response.payload.data.decode("UTF-8")

def test_bigquery():
    service_account_key = "airflow-service-account-key"
    service_key_info = json.loads(read_secret(service_account_key))
    client = bigquery.Client.from_service_account_info(service_key_info)

    bigquery_config = "airflow-bigquery-config"
    bigquery_config_info = json.loads(read_secret(bigquery_config))
    
    query = f"SELECT * from {bigquery_config_info["GCP_PROJECT_ID"]}.{bigquery_config_info["BQ_DATASET"]}.{bigquery_config_info["BQ_TABLE"]} LIMIT 50"
    query_job = client.query(query)
    for row in query_job:
        print(f"Test value: {row.name}")

if __name__ == "__main__":
    test_bigquery()
    print("BigQuery test completed successfully.")