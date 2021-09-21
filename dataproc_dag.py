# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# =============================================================================

from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator, DataProcSparkOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.strptime("2018-08-01 00:00:00", "%Y-%m-%d %H:%M:%S"),
    'email': ['email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=60),
}

dag = DAG('dataproc_example_dag',
          schedule_interval=timedelta(days=1),
          default_args=default_args)

with dag:
    CLUSTER_NAME = 'example-{{ds_nodash}}'
    JOB_NAME = '{{task.task_id}}-{{ds_nodash}}'

    def ensure_cluster_exists():
        cluster = DataProcHook().get_conn().projects().regions().clusters().get(
            projectId=Variable.get('project'),
            region=Variable.get('region'),
            clusterName=CLUSTER_NAME
        ).execute(num_retries=5)
        if cluster is None or len(cluster) == 0 or 'clusterName' not in cluster:
            return 'create_cluster'
        else:
            return 'run_job'

    start = BranchPythonOperator(
        task_id='start',
        provide_context=True,
        python_callable=ensure_cluster_exists,
    )

    create_cluster = DataprocClusterCreateOperator(
        task_id='create_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=Variable.get('project'),
        num_workers=2,
        master_disk_size=50,
        worker_disk_size=50,
        image_version='preview',
        internal_ip_only=True,
        tags=['dataproc'],
        labels={'dataproc-cluster': CLUSTER_NAME},
        zone=Variable.get('zone'),
        subnetwork_uri='projects/{}/region/{}/subnetworks/{}'.format(
            Variable.get('project'),
            Variable.get('region'),
            Variable.get('subnet')),
        service_account=Variable.get('serviceAccount'),
    )

    class PatchedDataProcSparkOperator(DataProcSparkOperator):
        """
        Workaround for DataProcSparkOperator.execute()
        not passing project_id to DataProcHook
        """
        def __init__(self, project_id=None, *args, **kwargs):
            self.project_id = project_id
            super(PatchedDataProcSparkOperator, self).__init__(*args, **kwargs)

        def execute(self, context):
            hook = DataProcHook(gcp_conn_id=self.gcp_conn_id,
                                delegate_to=self.delegate_to)
            job = hook.create_job_template(self.task_id, self.cluster_name, "sparkJob",
                                           self.dataproc_properties)
            job.set_main(self.main_jar, self.main_class)
            job.add_args(self.arguments)
            job.add_jar_file_uris(self.dataproc_jars)
            job.add_archive_uris(self.archives)
            job.add_file_uris(self.files)
            job.set_job_name(self.job_name)
            hook.submit(self.project_id, job.build(), self.region)

    run_job = PatchedDataProcSparkOperator(
        task_id='run_job',
        project_id=Variable.get('project'),
        main_class='com.google.cloud.example.BigQueryConnectorExample',
        arguments=[
            '{{ds_nodash}}',
            Variable.get('project'),
            Variable.get('dataset'),
            Variable.get('table'),
            Variable.get('bucket'),
            Variable.get('prefix')
        ],
        job_name=JOB_NAME,
        cluster_name=CLUSTER_NAME,
        dataproc_spark_jars=['gs://{}{}'.format(Variable.get('bucket'), Variable.get('jarPrefix'))]
    )

    delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_cluster',
        cluster_name=CLUSTER_NAME,
        project_id=Variable.get('project')
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    start >> create_cluster >> run_job
    start >> run_job
    run_job >> delete_cluster >> end
