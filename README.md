##FLask Airflow Lite
I'm trying to create a similar application to airflow which can run on stateless platform like Google Cloud Run

###Motivation:
Deploy airflow (or similar funcationality) to Google Cloud Run

###Proposed Solution
Use Google Cloud Storage to maintain state of the application, which includes: DAG status, Logs and History
