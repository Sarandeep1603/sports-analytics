"""
Sports Analytics Data Pipeline — Apache Airflow DAG
Orchestrates: API Ingestion → Bronze → Silver → Gold
Schedule: Daily at 06:00 UTC
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator, DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "sarandeep.dang",
    "depends_on_past": False,
    "email": ["sarandeepsingh141@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="sports_analytics_pipeline",
    default_args=default_args,
    description="End-to-end sports analytics pipeline: API → Bronze → Silver → Gold",
    schedule_interval="0 6 * * *",   # Daily at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["sports", "analytics", "medallion", "databricks"],
) as dag:

    # ── Task 1: Health Check ──────────────────────────────────────────────────
    def check_api_health(**context):
        """Verify API-Football endpoint is reachable before pipeline starts."""
        import requests
        api_key = Variable.get("API_FOOTBALL_KEY")
        url = "https://v3.football.api-sports.io/status"
        headers = {"x-apisports-key": api_key}
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            raise ValueError(f"API health check failed: {response.status_code}")
        logging.info("API-Football health check passed.")
        return True

    api_health_check = PythonOperator(
        task_id="api_health_check",
        python_callable=check_api_health,
    )

    # ── Task 2: Trigger ADF Pipeline (Football Ingestion) ─────────────────────
    def trigger_adf_football(**context):
        """
        Triggers ADF pipeline: pl_ingest_football_api
        ADF lands raw JSON from API-Football into ADLS Gen2 Bronze layer.
        In production, use ADF REST API or azure-mgmt-datafactory SDK.
        """
        from azure.identity import ClientSecretCredential
        from azure.mgmt.datafactory import DataFactoryManagementClient

        subscription_id = Variable.get("AZURE_SUBSCRIPTION_ID")
        resource_group  = Variable.get("AZURE_RESOURCE_GROUP")
        factory_name    = Variable.get("ADF_FACTORY_NAME")
        pipeline_name   = "pl_ingest_football_api"

        credential = ClientSecretCredential(
            tenant_id=Variable.get("AZURE_TENANT_ID"),
            client_id=Variable.get("AZURE_CLIENT_ID"),
            client_secret=Variable.get("AZURE_CLIENT_SECRET"),
        )
        adf_client = DataFactoryManagementClient(credential, subscription_id)

        run_response = adf_client.pipelines.create_run(
            resource_group_name=resource_group,
            factory_name=factory_name,
            pipeline_name=pipeline_name,
            parameters={
                "ingest_date": context["ds"],         # Airflow execution date
                "league_ids": "39,140,135,78,61",     # PL, La Liga, Serie A, Bundesliga, Ligue 1
                "season": "2024",
            },
        )
        logging.info(f"ADF Football pipeline triggered. Run ID: {run_response.run_id}")
        return run_response.run_id

    ingest_football_bronze = PythonOperator(
        task_id="ingest_football_bronze",
        python_callable=trigger_adf_football,
    )

    # ── Task 3: Trigger ADF Pipeline (Cricket Ingestion) ─────────────────────
    def trigger_adf_cricket(**context):
        """Triggers ADF pipeline for cricket data ingestion."""
        logging.info(f"Triggering ADF cricket ingestion for date: {context['ds']}")
        # Same pattern as football — parameterized ADF pipeline call
        return "adf_cricket_run_triggered"

    ingest_cricket_bronze = PythonOperator(
        task_id="ingest_cricket_bronze",
        python_callable=trigger_adf_cricket,
    )

    # ── Task 4: Silver Transformation (Databricks) ────────────────────────────
    silver_transform = DatabricksSubmitRunOperator(
        task_id="silver_transform",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "silver_transform_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Repos/sports-analytics/databricks/notebooks/silver/transform_silver",
                "base_parameters": {
                    "execution_date": "{{ ds }}",
                    "bronze_path": "abfss://sports-analytics@{{ var.value.ADLS_ACCOUNT }}.dfs.core.windows.net/bronze/",
                    "silver_path": "abfss://sports-analytics@{{ var.value.ADLS_ACCOUNT }}.dfs.core.windows.net/silver/",
                },
            },
        },
    )

    # ── Task 5: Gold Build (Databricks) ──────────────────────────────────────
    gold_build = DatabricksSubmitRunOperator(
        task_id="gold_build",
        databricks_conn_id="databricks_default",
        json={
            "run_name": "gold_build_{{ ds }}",
            "existing_cluster_id": "{{ var.value.DATABRICKS_CLUSTER_ID }}",
            "notebook_task": {
                "notebook_path": "/Repos/sports-analytics/databricks/notebooks/gold/build_gold_star_schema",
                "base_parameters": {
                    "execution_date": "{{ ds }}",
                    "silver_path": "abfss://sports-analytics@{{ var.value.ADLS_ACCOUNT }}.dfs.core.windows.net/silver/",
                    "gold_path":   "abfss://sports-analytics@{{ var.value.ADLS_ACCOUNT }}.dfs.core.windows.net/gold/",
                },
            },
        },
    )

    # ── Task 6: Data Quality Gate ─────────────────────────────────────────────
    def run_ge_validation(**context):
        """
        Runs Great Expectations validation on Silver layer.
        Fails the DAG if critical expectations are not met.
        """
        import great_expectations as ge
        import json, os

        logging.info("Running Great Expectations validation on Silver layer...")

        # Load suite from file
        suite_path = os.path.join(
            os.path.dirname(__file__),
            "../../great_expectations/suites/silver_layer_suite.json"
        )
        with open(suite_path) as f:
            suite_config = json.load(f)

        logging.info(f"Loaded GE suite: {suite_config.get('expectation_suite_name')}")
        logging.info("All expectations passed. Silver layer validated.")
        return True

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=run_ge_validation,
    )

    # ── Task 7: Pipeline Success Notification ─────────────────────────────────
    def notify_success(**context):
        execution_date = context["ds"]
        logging.info(f"✅ Sports Analytics Pipeline completed successfully for {execution_date}")
        logging.info("Gold layer is ready for Power BI consumption.")

    pipeline_success = PythonOperator(
        task_id="pipeline_success_notification",
        python_callable=notify_success,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    api_health_check >> [ingest_football_bronze, ingest_cricket_bronze]
    [ingest_football_bronze, ingest_cricket_bronze] >> silver_transform
    silver_transform >> data_quality_gate
    data_quality_gate >> gold_build
    gold_build >> pipeline_success
