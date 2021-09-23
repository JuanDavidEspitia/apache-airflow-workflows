"""
Example Airflow DAG that show how to use various Dataproc
operators to manage a cluster and submit jobs.
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt
from airflow.operators.python_operator import PythonOperator

def print_world():
   print('Hello world')


default_args = {
    'owner': 'bhanuprakash',
    'depends_on_past': False,
    'start_date': dt.datetime(2021, 9, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG('dag_today_every_5_minutes',
    default_args=default_args,
    schedule_interval= '*/5 * * * *'
    ) as dag:


    print_hello = BashOperator(task_id='print_hello',
        bash_command='gnome-terminal')


    sleep = BashOperator(task_id='sleep',
        bash_command='sleep 5')


    print_world = PythonOperator(task_id='print_world',
        python_callable=print_world)

print_hello >> sleep >> print_world