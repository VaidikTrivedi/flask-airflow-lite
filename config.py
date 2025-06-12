import os

# Google Cloud Storage bucket for metadata and logs
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "your-airflow-lite-bucket")

# Basic Auth credentials
BASIC_AUTH_USERNAME = os.environ.get("BASIC_AUTH_USERNAME", "admin")
BASIC_AUTH_PASSWORD = os.environ.get("BASIC_AUTH_PASSWORD", "password")

# Path to BigQuery service account key file
# This should be mounted as a Cloud Run Secret
BIGQUERY_SERVICE_ACCOUNT_KEY_PATH = os.environ.get(
    "BIGQUERY_SERVICE_ACCOUNT_KEY_PATH",
    "/etc/secrets/service_account.json" # Default path if mounted as a secret
)

# BigQuery Project ID (optional, if not inferred from credentials)
BIGQUERY_PROJECT_ID = os.environ.get("BIGQUERY_PROJECT_ID")

SECRET_ID = os.environ.get("SECRET_ID", "secret_id")  

# Maximum number of parallel tasks to run within a single DAG run
MAX_PARALLEL_TASKS = 4