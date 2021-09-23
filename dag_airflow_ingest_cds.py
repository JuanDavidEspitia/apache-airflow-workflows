#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html#create-a-cluster
# https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_modules/airflow/providers/google/cloud/example_dags/example_dataproc.html
"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""

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
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.utils.dates import days_ago



PROJECT_ID = models.Variable.get('gcp_project')
CLUSTER_NAME = models.Variable.get('gcp_cluster_name')
REGION = models.Variable.get('gcp_region')
ZONE = models.Variable.get('gcp_zone')

#PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "bdb-gcp-de-cds")
#CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME", "example-cluster-composer-bdb-gcp-ingest")
#REGION = os.environ.get("GCP_LOCATION", "us-east1")
#ZONE = os.environ.get("GCP_REGION", "us-east1-d")


# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
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
}

# [START how_to_cloud_dataproc_spark_config]
SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "jar_file_uris": ["gs://bdb-gcp-cds-dev/ProyectoCDS/Scripts/Scala/JobSparkDataproc-assembly-0.0.1.jar"],
        "main_class": "net.bancodebogota.DemoIngest",
        "args": ["gs://bdb-gcp-cds-dev/ProyectoCDS/Data/Customers/", "gs://bdb-gcp-cds-qa/Data/Bronze/", "|"],
    },
}

# 'schedule_interval': '@daily',
# 'schedule_interval': '@hourly',

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 9, 23),
    'catchup_by_default': False,
    'retry_delay': timedelta(minutes=2),
    'retries': 1
}

with models.DAG("example_gcp_dataproc",
                default_args=default_args,
                schedule_interval='*/15 * * * *') as dag:

    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task",
        job=SPARK_JOB, 
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

    create_cluster >> spark_task >> delete_cluster

