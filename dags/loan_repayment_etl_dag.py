from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sacredfrank@gmail.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'loan_repayment_etl_dag',
    default_args=default_args,
    description='Run loan repayment ETL monthly',
    schedule_interval='@monthly',  # can be changed to cron
    start_date=days_ago(1),
    catchup=False,
    tags=['loan', 'spark', 'ETL'],
) as dag:

    run_etl = BashOperator(
        task_id='run_spark_etl_pipeline',
        bash_command='spark-submit /workspaces/Loan-Repayment/pipeline.py'
    )

    run_etl
