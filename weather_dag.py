from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd

def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return temp_in_celsius

def transform_load_data(task_instance):                                 
    data = task_instance.xcom_pull(task_ids="extract_weather_data")     
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = round(kelvin_to_celsius(data["main"]["temp"]),2)
    feels_like_farenheit= round(kelvin_to_celsius(data["main"]["feels_like"]),2)
    min_temp_farenheit = round(kelvin_to_celsius(data["main"]["temp_min"]),2)
    max_temp_farenheit = round(kelvin_to_celsius(data["main"]["temp_max"]),2)
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,                           # Creating a dictionary.
                        "Description": weather_description,
                        "Temperature (C)": temp_farenheit,
                        "Feels Like (C)": feels_like_farenheit,
                        "Minimun Temp (C)":min_temp_farenheit,
                        "Maximum Temp (C)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)       # Converted into a Dataframe.
    aws_credentials = {"key": "xxx", "secret": "xxx", "token": "xxx"}     #Connect to S3 amazon.

    now = datetime.now()                                
    dt_string = now.strftime("%d%m%Y%H%M%S")            # File name to include the time of running the script.
    dt_string = 'current_weather_data_lisbon_' + dt_string      # File name.
    df_data.to_csv(f"s3://weather-api-airflow-joao-pedro-yml/{dt_string}.csv", index=False, storage_options=aws_credentials)        # Create file csv and save it in S3 Amazon.

default_args = {
    'owner': 'airflow', 
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),         # When do you want the airflow to start running?
    'email': ['myemail@domain.com'],            
    'email_on_failure': False,                  # Should an email be sent if the pipeline fails?
    'email_on_retry': False,                    # Should an email be sent if the pipeline tries to retry? 
    'retries': 2,                               # How many times should try to retry after failure.
    'retry_delay': timedelta(minutes=2)         # Minutes between each retry.
}

with DAG('weather_dag',                         # Airflow schedule and monitors the pipeline
        default_args=default_args,
        schedule_interval = '@daily',           # It's going to run daily. At midnight.
        catchup=False) as dag:                  # When catchup is enabled, Airflow will schedule and execute the DAG runs for those past intervals to ensure that all the historical data is processed.

        is_weather_api_ready = HttpSensor(      # Check task: HttpSensor checks if API is ready, to not break the pipeline.
        task_id ='is_weather_api_ready',        # This need to be unique.
        http_conn_id='weathermap_api',          # Connection created in Apache Airflow to connect to API OpenWeatherMap.
        endpoint='/data/2.5/weather?q=Lisbon&appid=8589a23fe01f42ecedb5475e1108f820'    #API call
        ) 

        extract_weather_data = SimpleHttpOperator(      # Extract task: Get method to extract the data. 
        task_id = 'extract_weather_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=Lisbon&appid=8589a23fe01f42ecedb5475e1108f820',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),     # Uses json library to load data.
        log_response=True
        ) 

        transform_load_weather_data = PythonOperator(       # Transform and load task: Get method to extract the data.            
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )                                      

        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data        # order of tasks.