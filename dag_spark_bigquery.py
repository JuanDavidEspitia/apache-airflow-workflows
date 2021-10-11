import os
from datetime import datetime, timedelta
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateWorkflowTemplateOperator,
    DataprocDeleteClusterOperator,
    DataprocInstantiateWorkflowTemplateOperator,
    DataprocSubmitJobOperator,
    DataprocUpdateClusterOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bdb-gcp-de-cds")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "bdb-gcp-cds-example-cluster-composer-ingest")
REGION = os.environ.get("GCP_LOCATION", "us-east1")
ZONE = os.environ.get("GCP_REGION", "us-east1-d")
#METADATA = {"spark-bigquery-connector-version": "0.21.1"}
METADATA = [("bigquery-connector-version", "1.2.0"), ("spark-bigquery-connector-version", "0.21.0")]

CLUSTER_CONFIG = {
    "gce_cluster_config": {
        "metadata": {"bigquery-connector-version": "1.2.0", "spark-bigquery-connector-version": "0.21.0"},
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 512},
    },
    "initialization_actions": [
        {"executable_file": "gs://goog-dataproc-initialization-actions-us-east1/connectors/connectors.sh", "execution_timeout": {'seconds': 60}}
    ],
}

BIGQUERY_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://bdb-gcp-cds-dev/ProyectoCDS/Scripts/Scala/SparkBigQueryConnector-assembly-0.1.jar"],
        "main_class": "com.google.cloud.Shakespeare",
    },
}

default_args = {
    'owner': 'bdb_airflow',
    'start_date': datetime(2021, 9, 23),
    'catchup_by_default': False,
    'retry_delay': timedelta(minutes=10),
    'retries': 1
}

with models.DAG("bdb_example_dataproc_ingest",
                default_args=default_args,
                schedule_interval=timedelta(days=1)) as dag:

    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        delete_on_error=True,
        init_actions_uris=["gs://goog-dataproc-initialization-actions-us-east1/connectors/connectors.sh"],
        init_action_timeout="10m",
        metadata=METADATA

    )

    bigquery_task = DataprocSubmitJobOperator(
        task_id="bigquery_task",
        job=BIGQUERY_JOB,
        location=REGION,
        project_id=PROJECT_ID
    )

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]

    create_cluster >> bigquery_task >> delete_cluster