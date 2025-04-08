from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def fetch_amsterdam_weather():
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 52.3676,
        "longitude": 4.9041,
        "current_weather": True,
        "hourly": "precipitation_probability",
        "timezone": "Europe/Amsterdam"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()

    # Extract temperature and current time
    temp = data["current_weather"]["temperature"]
    current_time = data["current_weather"]["time"]

    # Round down to the hour
    parsed_time = datetime.fromisoformat(current_time)
    hourly_time = parsed_time.replace(minute=0, second=0, microsecond=0).isoformat()

    # Try to match rounded hour in hourly forecast
    try:
        time_index = data["hourly"]["time"].index(hourly_time)
        rain_prob = data["hourly"]["precipitation_probability"][time_index]
    except ValueError:
        rain_prob = "N/A"

    # Pretty output
    print("============================================")
    print("📍 Weather Report for: Amsterdam")
    print(f"🕒 Time: {current_time} (rounded: {hourly_time})")
    print(f"🌡️ Temperature: {temp}°C")
    print(f"🌧️ Rain Probability: {rain_prob}%")
    print("============================================")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

with DAG(
    dag_id='amsterdam_weather_dag',
    default_args=default_args,
    description='Fetches and prints temperature and rain probability in Amsterdam',
    schedule_interval=None,  # Trigger manually
    catchup=False,
    tags=['weather', 'amsterdam', 'test'],
) as dag:

    print_weather = PythonOperator(
        task_id='print_amsterdam_weather',
        python_callable=fetch_amsterdam_weather,
    )
