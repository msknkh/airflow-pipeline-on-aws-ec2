from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from bitcoin_etl import run_bitcoin_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['muskan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'bitcoin_dag',
    default_args=default_args, 
    description='My first etl code'
)

run_etl = PythonOperator(
    task_id='complete_bitcoin_etl',
    python_callable=run_bitcoin_etl,
    dag=dag,
)

run_etl