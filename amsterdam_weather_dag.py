from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def get_amsterdam_temperature():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.3676,
        "longitude": 4.9041,
        "current_weather": True
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    temperature = data["current_weather"]["temperature"]
    print(f"The current temperature in Amsterdam is {temperature}°C")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='amsterdam_weather_dag',
    default_args=default_args,
    description='Fetches and prints the current temperature in Amsterdam',
    schedule_interval=None,  # Trigger manually
    catchup=False,
    tags=['weather', 'test'],
) as dag:

    print_weather = PythonOperator(
        task_id='get_amsterdam_temperature',
        python_callable=get_amsterdam_temperature,
    )

