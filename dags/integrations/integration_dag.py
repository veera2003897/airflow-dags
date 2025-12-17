from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, Airflow!")

with DAG(
    dag_id="integration",
    dag_display_name = 'Integration',
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=hello_world
    )
