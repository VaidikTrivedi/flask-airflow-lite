import os
import uuid
import datetime
from typing import List, Dict, Callable

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
DATASET = os.environ.get("BQ_DATASET", "your_dataset")
TABLE = os.environ.get("BQ_TABLE", "your_table")

class Task:
    def __init__(self, task_id: str, bigquery_query: str, depends_on: List[str] = None): # type: ignore
        self.task_id = task_id
        self.bigquery_query = bigquery_query
        self.depends_on = depends_on if depends_on is not None else []

    def to_dict(self):
        return {
            "task_id": self.task_id,
            "bigquery_query": self.bigquery_query,
            "depends_on": self.depends_on
        }

class DAG:
    def __init__(self, dag_id: str, schedule_interval: str = None, tasks: List[Task] = None):
        self.dag_id = dag_id
        self.schedule_interval = schedule_interval # Placeholder for future scheduling logic
        self.tasks = tasks if tasks is not None else []
        self._task_map = {task.task_id: task for task in self.tasks}

        # Basic dependency validation
        for task in self.tasks:
            for dep in task.depends_on:
                if dep not in self._task_map:
                    raise ValueError(f"Task '{task.task_id}' depends on non-existent task '{dep}' in DAG '{dag_id}'")

    def get_task(self, task_id: str) -> Task:
        return self._task_map.get(task_id)

    def to_dict(self):
        return {
            "dag_id": self.dag_id,
            "schedule_interval": self.schedule_interval,
            "tasks": [task.to_dict() for task in self.tasks]
        }

# --- Define Your DAGs Here ---
# Example 1: Sequential DAG
dag_etl_pipeline = DAG(
    dag_id="etl_pipeline_test",
    tasks=[
        Task(
            task_id="extract_data",
            bigquery_query="SELECT count(*) FROM `bigquery-public-data.stackoverflow.posts_questions` WHERE creation_date > '2023-01-01'",
        )
    ]
)

# Example 2: Parallel Tasks DAG
dag_marketing_reports = DAG(
    dag_id="marketing_reports",
    tasks=[
        Task(
            task_id="fetch_sales_data",
            bigquery_query="SELECT count(*) FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` WHERE totals.visits > 1"
        ),
        Task(
            task_id="fetch_ad_campaign_data",
            bigquery_query="SELECT count(*) FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` WHERE device.deviceCategory = 'desktop'"
        ),
        Task(
            task_id="generate_summary_report",
            bigquery_query="SELECT count(*) FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801`",
            depends_on=["fetch_sales_data", "fetch_ad_campaign_data"]
        )
    ]
)

# Register all DAGs for the application to discover
ALL_DAGS: Dict[str, DAG] = {
    dag.dag_id: dag for dag in [
        dag_etl_pipeline,
        # dag_marketing_reports,
        # Add more DAGs here
    ]
}

def get_dag_by_id(dag_id: str):
    return ALL_DAGS.get(dag_id)

def get_all_dag_ids() -> List[str]:
    return list(ALL_DAGS.keys())



# Example 1: Sequential DAG
# dag_etl_pipeline = DAG(
#     dag_id="etl_pipeline",
#     tasks=[
#         Task(
#             task_id="extract_data",
#             bigquery_query=f"SELECT * from {PROJECT_ID}.{DATASET}.{TABLE} LIMIT 50",
#         ),
#         # Task(
#         #     task_id="transform_data",
#         #     bigquery_query="SELECT count(*) FROM `bigquery-public-data.stackoverflow.posts_answers` WHERE creation_date > '2023-01-01'",
#         #     depends_on=["extract_data"]
#         # )
#     ]
# )