import datetime
import uuid
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from typing import List, Dict, Optional

from dag_definitions import DAG, Task
from gcs_utils import upload_json_to_gcs, download_json_from_gcs, upload_text_to_gcs, list_blobs_in_prefix, download_text_from_gcs
from bigquery_utils import execute_bigquery_query
from config import GCS_BUCKET_NAME, MAX_PARALLEL_TASKS, BIGQUERY_PROJECT_ID

logger = logging.getLogger(__name__)

class DAGExecutor:
    def __init__(self, dag: DAG):
        self.dag = dag
        self.run_id = str(uuid.uuid4())
        self.base_gcs_path = f"dag_runs/{self.dag.dag_id}/{self.run_id}"
        self.metadata_gcs_path = f"{self.base_gcs_path}/metadata.json"
        self.task_log_gcs_prefix = f"{self.base_gcs_path}/logs"
        self.executor = ThreadPoolExecutor(max_workers=MAX_PARALLEL_TASKS)
        self.futures = {} # To keep track of running tasks

    def _get_initial_run_metadata(self):
        """Generates initial metadata for a DAG run."""
        task_instances = []
        for task in self.dag.tasks:
            task_instances.append({
                "task_id": task.task_id,
                "status": "QUEUED",
                "start_time": None,
                "end_time": None,
                "log_file_path": f"{self.task_log_gcs_prefix}/{task.task_id}.log",
                "depends_on": task.depends_on
            })
        return {
            "dag_id": self.dag.dag_id,
            "run_id": self.run_id,
            "start_time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "end_time": None,
            "status": "RUNNING", # Initial status when execution starts
            "task_instances": task_instances
        }

    def _update_run_metadata_in_gcs(self, metadata: dict):
        """Updates the DAG run metadata JSON file in GCS."""
        upload_json_to_gcs(GCS_BUCKET_NAME, self.metadata_gcs_path, metadata)

    def _get_run_metadata_from_gcs(self):
        """Downloads the current DAG run metadata from GCS."""
        return download_json_from_gcs(GCS_BUCKET_NAME, self.metadata_gcs_path)

    def _execute_task(self, task_instance_id: int):
        """Executes a single task and updates its status/logs in GCS."""
        current_metadata = self._get_run_metadata_from_gcs()
        if not current_metadata:
            logger.error(f"Could not retrieve metadata for run {self.run_id} to execute task {task_instance_id}")
            return

        task_instance = current_metadata["task_instances"][task_instance_id]
        task = self.dag.get_task(task_instance["task_id"])

        task_instance["status"] = "RUNNING"
        task_instance["start_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        self._update_run_metadata_in_gcs(current_metadata)
        logger.info(f"Task {task.task_id} of run {self.run_id} started.")

        log_content = ""
        try:
            success, result_or_error = execute_bigquery_query(task.bigquery_query, project_id=BIGQUERY_PROJECT_ID)
            if success:
                task_instance["status"] = "SUCCESS"
                log_content = f"Task {task.task_id} completed successfully.\nRows affected/processed: {result_or_error}"
                logger.info(f"Task {task.task_id} of run {self.run_id} succeeded.")
            else:
                task_instance["status"] = "FAILED"
                log_content = f"Task {task.task_id} failed.\nError: {result_or_error}"
                logger.error(f"Task {task.task_id} of run {self.run_id} failed: {result_or_error}")

        except Exception as e:
            task_instance["status"] = "FAILED"
            log_content = f"Task {task.task_id} failed with unhandled exception: {e}"
            logger.exception(f"Unhandled exception during task {task.task_id} execution in run {self.run_id}")
        finally:
            task_instance["end_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            self._update_run_metadata_in_gcs(current_metadata)
            upload_text_to_gcs(GCS_BUCKET_NAME, task_instance["log_file_path"], log_content)

    def _monitor_dag_run(self):
        """Monitors the DAG run and schedules tasks based on dependencies."""
        metadata = self._get_initial_run_metadata()
        self._update_run_metadata_in_gcs(metadata) # Initial status to GCS
        logger.info(f"Initiated DAG run {self.run_id} for DAG {self.dag.dag_id}. Starting monitoring.")

        # Map task_id to its index in task_instances list for easier lookup
        task_id_to_idx = {t["task_id"]: i for i, t in enumerate(metadata["task_instances"])}

        while True:
            current_metadata = self._get_run_metadata_from_gcs()
            if not current_metadata:
                logger.error(f"Failed to retrieve metadata for run {self.run_id} during monitoring.")
                break # Exit if metadata cannot be loaded

            all_tasks_completed = True
            run_failed = False

            # Identify runnable tasks and update states
            for idx, task_instance in enumerate(current_metadata["task_instances"]):
                task_status = task_instance["status"]
                
                if task_status == "FAILED":
                    run_failed = True
                    all_tasks_completed = True # Stop further processing, mark DAG as failed
                    break # Break from task loop

                if task_status not in ["SUCCESS", "FAILED"]:
                    all_tasks_completed = False # At least one task is not yet completed

                    # Check dependencies
                    dependencies_met = True
                    for dep_task_id in task_instance["depends_on"]:
                        dep_idx = task_id_to_idx[dep_task_id]
                        dep_status = current_metadata["task_instances"][dep_idx]["status"]
                        if dep_status != "SUCCESS":
                            dependencies_met = False
                            break

                    # Schedule if ready and not running/finished
                    if dependencies_met and task_status == "QUEUED":
                        if task_instance["task_id"] not in self.futures or self.futures[task_instance["task_id"]].done():
                            logger.info(f"Scheduling task {task_instance['task_id']} for run {self.run_id}")
                            future = self.executor.submit(self._execute_task, idx)
                            self.futures[task_instance["task_id"]] = future
                            task_instance["status"] = "PENDING" # Mark as submitted, awaiting execution
                            self._update_run_metadata_in_gcs(current_metadata) # Update immediately

            if run_failed:
                current_metadata["status"] = "FAILED"
                current_metadata["end_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                self._update_run_metadata_in_gcs(current_metadata)
                logger.error(f"DAG run {self.run_id} failed due to task failure.")
                break # Exit monitoring loop

            if all_tasks_completed:
                current_metadata["status"] = "SUCCESS"
                current_metadata["end_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                self._update_run_metadata_in_gcs(current_metadata)
                logger.info(f"DAG run {self.run_id} completed successfully.")
                break # Exit monitoring loop

            time.sleep(5) # Poll every 5 seconds for task status updates

        self.executor.shutdown(wait=True) # Ensure all submitted tasks complete

    def start_dag_run(self):
        """Starts the DAG run in a separate thread."""
        logger.info(f"Starting DAG run {self.run_id} for DAG {self.dag.dag_id} in background thread.")
        # We start the monitoring in a new thread so the main request can return
        threading.Thread(target=self._monitor_dag_run, daemon=True).start()
        return self.run_id

def get_dag_run_metadata(dag_id: str, run_id: str) -> dict | None:
    """Retrieves a specific DAG run's metadata from GCS."""
    path = f"dag_runs/{dag_id}/{run_id}/metadata.json"
    return download_json_from_gcs(GCS_BUCKET_NAME, path)

def get_dag_run_log(dag_id: str, run_id: str, task_id: str) -> str | None:
    """Retrieves a specific task's log from GCS."""
    path = f"dag_runs/{dag_id}/{run_id}/logs/{task_id}.log"
    return download_text_from_gcs(GCS_BUCKET_NAME, path)

def get_all_dag_runs_summary(dag_id: str) -> List[Dict]:
    """Lists all runs for a given DAG by listing GCS prefixes."""
    prefix = f"dag_runs/{dag_id}/"
    run_folders = set()
    
    # List all objects under the DAG's prefix and extract run_id
    for blob_name in list_blobs_in_prefix(GCS_BUCKET_NAME, prefix):
        parts = blob_name.split('/')
        if len(parts) >= 3 and parts[0] == 'dag_runs' and parts[1] == dag_id:
            run_folders.add(parts[2]) # parts[2] should be the run_id

    runs_summary = []
    for run_id in sorted(list(run_folders), reverse=True): # Sort by run_id (likely creation order)
        metadata_path = f"dag_runs/{dag_id}/{run_id}/metadata.json"
        metadata = download_json_from_gcs(GCS_BUCKET_NAME, metadata_path)
        if metadata:
            runs_summary.append({
                "run_id": metadata["run_id"],
                "status": metadata.get("status", "UNKNOWN"),
                "start_time": metadata.get("start_time", "N/A"),
                "end_time": metadata.get("end_time", "N/A")
            })
    return runs_summary