from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator


def sample_dag():
    for i in range(10):
        print(i)

def sample_dag2():
    print('HELLO MAPRECRUIT')

with DAG(
    dag_id = 'parser_',
    dag_display_name= 'PARSERS',
    schedule= '0 0 * * *',
    start_date=datetime(2020, 1, 1),

) as dag:

    task1 = PythonOperator(
        python_callable=sample_dag,
        task_id='print_1-10',
    )

    task2 = PythonOperator(
        python_callable=sample_dag2,
    )

    task1 >> task2