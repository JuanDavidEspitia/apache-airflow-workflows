"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta , datetime
from airflow.operators.python_operator import PythonOperator

def print_world():
   print('Hello world')


default_args = {
    'owner': 'bdb_airflow',
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('dag_today_every_5_minutes',
    default_args=default_args,
    description='Dag execute ever x minutes',
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20))


print_hello = BashOperator(task_id='print_hello',
        bash_command='gnome-terminal')


sleep = BashOperator(task_id='sleep',
        bash_command='sleep 5')


print_world = PythonOperator(task_id='print_world',
        python_callable=print_world)

print_hello >> sleep >> print_world