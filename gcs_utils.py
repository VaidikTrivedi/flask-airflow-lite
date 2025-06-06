from google.cloud import storage
import json
import logging

logger = logging.getLogger(__name__)

# Initialize GCS client globally (will reuse connection)
service_key_path = 'service_account.json'
gcs_client = storage.Client.from_service_account_json(service_key_path)

def _get_all_buckets():
    """Helper to get all buckets in the GCS project."""
    try:
        buckets = gcs_client.list_buckets()
        return [bucket.name for bucket in buckets]
    except Exception as e:
        logger.error(f"Error listing buckets: {e}")
        return []

def _get_blob(bucket_name, blob_name):
    """Helper to get a blob object."""
    bucket = gcs_client.get_bucket(bucket_name)
    return bucket.blob(blob_name)

def upload_json_to_gcs(bucket_name, blob_name, data):
    """Uploads a dictionary as a JSON file to GCS."""
    blob = _get_blob(bucket_name, blob_name)
    try:
        blob.upload_from_string(json.dumps(data, indent=4), content_type='application/json')
        logger.info(f"Uploaded JSON to gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f"Error uploading JSON to GCS {blob_name}: {e}")
        raise

def download_json_from_gcs(bucket_name, blob_name):
    """Downloads a JSON file from GCS and returns its content as a dictionary."""
    blob = _get_blob(bucket_name, blob_name)
    if not blob.exists():
        logger.warning(f"Blob gs://{bucket_name}/{blob_name} does not exist.")
        return None
    try:
        content = blob.download_as_text()
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error downloading or parsing JSON from GCS {blob_name}: {e}")
        return None

def upload_text_to_gcs(bucket_name, blob_name, text_content):
    """Uploads plain text content to GCS."""
    blob = _get_blob(bucket_name, blob_name)
    try:
        blob.upload_from_string(text_content, content_type='text/plain')
        logger.info(f"Uploaded text to gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logger.error(f"Error uploading text to GCS {blob_name}: {e}")
        raise

def download_text_from_gcs(bucket_name, blob_name):
    """Downloads text content from GCS."""
    blob = _get_blob(bucket_name, blob_name)
    if not blob.exists():
        logger.warning(f"Blob gs://{bucket_name}/{blob_name} does not exist.")
        return None
    try:
        content = blob.download_as_text()
        return content
    except Exception as e:
        logger.error(f"Error downloading text from GCS {blob_name}: {e}")
        return None

def list_blobs_in_prefix(bucket_name, prefix):
    """Lists all blobs in a given prefix."""
    bucket = gcs_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]