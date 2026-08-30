from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='Automated Spark to Postgres Data Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'postgres', 'data-engineering'],
) as dag:

    start_pipeline = EmptyOperator(
        task_id='start_pipeline'
    )

    # Submit job to Spark Master from within Airflow
    run_spark_job = BashOperator(
        task_id='submit_spark_transformation',
        bash_command='spark-submit --master spark://spark-master:7077 --packages org.postgresql:postgresql:42.7.3 /opt/spark/jobs/transform.py'
    )

    end_pipeline = EmptyOperator(
        task_id='end_pipeline'
    )

    # Define task dependencies
    start_pipeline >> run_spark_job >> end_pipeline