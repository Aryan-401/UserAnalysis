from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Aryan-401',
    'start_date': datetime(2023, 10, 25, 10, 00)
}


def stream_data():
    import json
    import requests

    response = requests.get('https://randomuser.me/api/')
    response = response.json()['results'][0]
    return response

with DAG(task_id = 'user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'streaming_data_from_API',
        python_callable=stream_data  # TODO: Make Function
    )
