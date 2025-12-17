from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id='parser_task_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 0 * * *',
    catchup=False,
) as dag:

    @task
    def sample_task():
        print("Hello from sample_task!")

    @task
    def another_task():
        print("Hello from another_task!")

    sample_task() >> another_task()
