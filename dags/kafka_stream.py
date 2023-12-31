from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import uuid
import logging
default_args = {
    'owner': 'Aryan-401',
    'start_date': datetime(2023, 10, 25, 10, 00)
}


def get_data():
    import requests

    response = requests.get('https://randomuser.me/api/')
    response = response.json()['results'][0]
    return response


def format_data(res):
    data = {}
    location = res['location']
    # data['id'] = uuid.uuid4()
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    import json
    from kafka import KafkaProducer
    from time import sleep, time

    producer = KafkaProducer(bootstrap_servers = ['broker:29092'], max_block_ms = 5000)
    current_time = time() + 60
    while True:
        if time() > current_time:
            break
        try:
            response = get_data()
            response = format_data(response)
            response = json.dumps(response).encode('utf-8')

            producer.send('users_created', response)
        except Exception as e:
            logging.error(f"An Error Occured: {e}")
            continue

with DAG(dag_id = 'user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'streaming_data_from_API',
        python_callable=stream_data
    )

# stream_data()  # Testing