import os
from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
import logging
from datetime import datetime, timezone

from config import GCS_BUCKET_NAME, BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD
from dag_definitions import get_all_dag_ids, get_dag_by_id, ALL_DAGS
from dag_runner import DAGExecutor, get_dag_run_metadata, get_dag_run_log, get_all_dag_runs_summary
from gcs_utils import list_blobs_in_prefix, download_json_from_gcs

app = Flask(__name__)
auth = HTTPBasicAuth()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Basic Auth Users
users = {
    BASIC_AUTH_USERNAME: generate_password_hash(BASIC_AUTH_PASSWORD)
}

@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username

# --- UI Routes ---
@app.route('/')
@auth.login_required
def index():
    """Dashboard: Lists all DAGs and their latest status."""
    dag_ids = get_all_dag_ids()
    dags_data = []

    for dag_id in dag_ids:
        latest_run_status = "N/A"
        latest_run_id = None
        latest_run_start = "N/A"

        # Try to find the latest run by listing all metadata files and sorting
        prefix = f"dag_runs/{dag_id}/"
        # List all metadata files under the DAG's prefix to find the latest
        run_metadata_paths = [
            blob_name for blob_name in list_blobs_in_prefix(GCS_BUCKET_NAME, prefix)
            if blob_name.endswith('/metadata.json')
        ]
        
        # Extract run_ids and sort them (assuming UUIDs or timestamps in IDs can be sorted)
        # More robust would be to parse datetime from metadata.json of each run
        run_ids_with_paths = {}
        for path in run_metadata_paths:
            parts = path.split('/')
            if len(parts) >= 3:
                run_ids_with_paths[parts[2]] = path # parts[2] is run_id

        if run_ids_with_paths:
            # TODO: you'd download each metadata.json, get actual start_time, then sort.
            latest_run_id_key = sorted(run_ids_with_paths.keys(), reverse=False)[0]
            latest_metadata_path = run_ids_with_paths[latest_run_id_key]
            
            latest_run_data = download_json_from_gcs(GCS_BUCKET_NAME, latest_metadata_path)
            if latest_run_data:
                latest_run_status = latest_run_data.get("status", "UNKNOWN")
                latest_run_id = latest_run_data.get("run_id")
                latest_run_start = latest_run_data.get("start_time", "N/A")

        dags_data.append({
            "dag_id": dag_id,
            "latest_run_status": latest_run_status,
            "latest_run_id": latest_run_id,
            "latest_run_start": latest_run_start
        })
    
    return render_template('index.html', dags=dags_data)

@app.route('/dags/<string:dag_id>')
@auth.login_required
def dag_detail(dag_id):
    """Shows run history for a specific DAG."""
    dag = get_dag_by_id(dag_id)
    if not dag:
        return f"DAG with ID '{dag_id}' not found", 404
    
    runs = get_all_dag_runs_summary(dag_id)
    return render_template('dag_detail.html', dag_id=dag_id, runs=runs)

@app.route('/dags/<string:dag_id>/runs/<string:run_id>')
@auth.login_required
def run_detail(dag_id, run_id):
    """Shows details of a specific DAG run."""
    run_metadata = get_dag_run_metadata(dag_id, run_id)
    if not run_metadata:
        return "DAG Run not found or metadata corrupted.", 404

    # Provide GCS bucket name to template for direct log links if needed
    return render_template('run_detail.html',
                           dag_id=dag_id,
                           run_id=run_id,
                           run_metadata=run_metadata,
                           gcs_bucket_name=GCS_BUCKET_NAME)

@app.route('/dags/<string:dag_id>/runs/<string:run_id>/tasks/<string:task_id>/log')
@auth.login_required
def get_task_log(dag_id, run_id, task_id):
    """Retrieves logs for a specific task instance."""
    log_content = get_dag_run_log(dag_id, run_id, task_id)
    if log_content is None:
        return "Task log not found.", 404
    
    return render_template('log_viewer.html', log_content=log_content,
                           dag_id=dag_id, run_id=run_id, task_id=task_id)


# --- API Endpoints ---
@app.route('/api/v1/dags/<string:dag_id>/trigger', methods=['POST'])
@auth.login_required
def trigger_dag(dag_id):
    """API endpoint to trigger a DAG run."""
    dag = get_dag_by_id(dag_id)
    if not dag:
        # Render an error page here
        return render_template(
            "trigger_result.html",
            dag_id=dag_id,
            status="DAG Not Found",
            run_id=None,
            detail_url="#"
        ), 404

    try:
        executor = DAGExecutor(dag)
        run_id = executor.start_dag_run() # Starts the DAG run in a background thread
        return render_template(
            "trigger_result.html",
            dag_id=dag_id,
            status="Triggered and running in background",
            run_id=run_id,
            detail_url=url_for('run_detail', dag_id=dag_id, run_id=run_id, _external=True)
        ), 200
    except Exception as e:
        logger.exception(f"Error triggering DAG {dag_id}: {e}")
        return jsonify({"message": f"Error triggering DAG '{dag_id}': {str(e)}"}), 500

@app.route('/health')
def health_check():
    return jsonify({"status": "healthy"}), 200

if __name__ == '__main__':
    # TODO: Remove it before deployment
    import debugpy
    debugpy.listen(("0.0.0.0", 5678))
    print("Waiting for debugger to attach...")
    debugpy.wait_for_client()  # Wait for the debugger to attach
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)), use_reloader=False)  # Disable reloader to avoid multiple debugpy instances