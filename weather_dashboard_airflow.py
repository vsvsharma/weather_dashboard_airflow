#Import necessary libraries
import requests
import psycopg2
import pandas as pd
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

# Define essential information like api_key, locations, API base URL

api_key = "XXXXXXXXXXXX"
location = ["Amaravati", "Itanagar", "Dispur", "Patna", "Raipur", "Panaji",
            "Gandhinagar", "Shimla", "Srinagar", "Ranchi", "Bengaluru",
            "Trivandrum", "Bhopal", "Mumbai", "Imphal", "Shillong", "Aizawl",
            "Kohima", "Bhubaneswar", "Chandigarh", "Jaipur", "Gangtok", "Chennai",
            "Hyderabad", "Agartala", "Lucknow", "Dehradun", "Kolkata"]

base_url = "http://api.weatherapi.com/v1/current.json"

#Database connection details

host = "localhost"
user = "postgres"
password = "postgres"
dbname = "weatherDB_airflow"
conn = psycopg2.connect(
    host=host, user=user, password=password, dbname=dbname
)
cursor = conn.cursor()

# Task to extract weather data for specified locations

def extract_data_task(location, **kwargs):
    """
    weather data for the particular locations are fetched
    with the help of different parameters like:
    1) API_key , 2) q-> location and 
    3) aqi-> which is already declared 'yes' 
    4)response.get of API will be used to connect database in which 
    base_url and above declared paramters will be passed.
    """
    weather_data_list = []
    for loc in location:
        params = {
            "key": api_key,
            "q": loc,
            "aqi": "yes"
        }
        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            weather_data = response.json()
            weather_data_list.append(weather_data)
        else:
            print(f"Failed to fetch data for {loc}. Status code: {response.status_code}")

    # extracted weather data list is pushed into xcom so that 
    # it can be acessed in the other functions.

    kwargs['ti'].xcom_push(key="weather_data", value=weather_data_list)

def process_location_data(ti):
    """
    In this function, location data is extracted from the whole weather data that was
    pushed into the xcom in the exraction data process.
    in this city iso codes are declared to so that 
    cities can be plotted on a map in the dashboard that will be built later on.

    """

    city_iso_codes = {
        'Amaravati': 'IN-AP', 'Itanagar': 'IN-AR', 'Dispur': 'IN-AS', 'Patna': 'IN-BR',
        'Raipur': 'IN-CG', 'Panaji': 'IN-GA', 'Gandhinagar': 'IN-GJ', 'Shimla': 'IN-HP',
        'Srinagar': 'IN-JK', 'Ranchi': 'IN-JH', 'Bengaluru': 'IN-KA', 'Trivandrum': 'IN-KL',
        'Bhopal': 'IN-MP', 'Mumbai': 'IN-MH', 'Imphal': 'IN-MN', 'Shillong': 'IN-ML',
        'Aizawl': 'IN-MZ', 'Kohima': 'IN-NL', 'Bhubaneswar': 'IN-OD', 'Chandigarh': 'IN-CH',
        'Jaipur': 'IN-RJ', 'Gangtok': 'IN-SK', 'Chennai': 'IN-TN', 'Hyderabad': 'IN-TS',
        'Agartala': 'IN-TR', 'Lucknow': 'IN-UP', 'Dehradun': 'IN-UK', 'Kolkata': 'IN-WB'
    }

    weather_data_list = ti.xcom_pull(task_ids="extract_data_task", key="weather_data")
    
    #Here data is being pulled from the xcom and data engineering is being done ie.
    #unwanted data is being removed.
    
    location_list = []
    for w in weather_data_list:
        location_dict = w.get("location")
        location_dict.pop('localtime_epoch', None)
        location_dict.pop('localtime', None)

        city_names = location_dict.get('name', '')
        iso_code = city_iso_codes.get(city_names, '')
        location_dict['iso_code'] = iso_code
        location_list.append(location_dict)
    
    # after all process data is being converted in the form of DataFrame 
    # which will be pushed to xcom
        
    location_df = pd.DataFrame(location_list)
    ti.xcom_push(key="location_df", value=location_df)

def process_current_data(ti):
    """
    In this function, current data is extracted from the whole weather data that was
    pushed into the xcom in the exraction data process.
    """
    weather_data_list = ti.xcom_pull(task_ids="extract_data_task", key="weather_data")

    # Here data is being pulled from the xcom and data engineering is being done ie.
    # unwanted data is being removed.
    
    current_list = []
    for c in weather_data_list:
        name_value = c['location']['name']
        current_dict = c.get("current")
        current_dict['name'] = name_value
        current_dict.pop('last_updated_epoch', None)

        condition_text = current_dict.get('condition', {}).get('text', '')
        current_dict['condition'] = condition_text

        current_dict.pop('air_quality', None)
        current_list.append(current_dict)

    # after all process data is being converted in the form of DataFrame 
    # which will be pushed to xcom

    current_df = pd.DataFrame(current_list)
    ti.xcom_push(key="current_df", value=current_df)

def process_air_quality(ti):
    """
    In this function, air quality data is extracted from the whole weather data that was
    pushed into the xcom in the exraction data process.

    """
    
    weather_data_list = ti.xcom_pull(task_ids="extract_data_task", key="weather_data")

    # Here data is being pulled from the xcom and data engineering is being done ie.
    # unwanted data is being removed.

    air_quality_list = []
    for a in weather_data_list:
        name_value1 = a['location']['name']
        name_value2 = a['current']['last_updated']
        air_dict = a.get("current", {}).get('air_quality', {})
        air_dict['name'] = name_value1
        air_dict['last_updated'] = name_value2

        air_dict.pop('us-epa-index', None)
        air_dict.pop('gb-defra-index', None)

        air_quality_list.append(air_dict)
    
    # after all process data is being converted in the form of DataFrame 
    # which will be pushed to xcom

    air_quality_df = pd.DataFrame(air_quality_list)
    ti.xcom_push(key="air_df", value=air_quality_df)

def insert_into_DB(ti,table_name, xcom_key, key, **kwargs):
    """
    All the dataframes that were pushed earlier in the xcom,
    will the fetched and then it will be pushed into the DataBase
    """
    
    df = ti.xcom_pull(task_ids=xcom_key, key=key)
    columns = ",".join(df.columns)
    value = ",".join(["%s" for _ in df.columns])

    df = df.where(pd.notna(df), None)
    data_values = [tuple(row) for _, row in df.iterrows()]
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({value})" #SQL query to insert into the columns
    cursor.executemany(query, data_values)

    conn.commit()
    print(f'Data inserted successfully into: {table_name}')

# Define the DAG 

dag = DAG('Weather_dashboard_data', start_date=datetime(2023, 1, 1),
          schedule_interval='@daily', catchup=False)

#Define Operators
"""
Operators are defined to call the different functions and task Ids are defined respectively.
If there are some arguments then they are also dclared with help of op_kwargs ie.
operator keyword argument.
"""
extract_data_operator = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data_task,
    provide_context=True,
    op_kwargs={'location': location},
    dag=dag
)

process_location_operator = PythonOperator(
    task_id='process_location',
    python_callable=process_location_data,
    provide_context=True,
    dag=dag
)

process_current_operator = PythonOperator(
    task_id='process_current',
    python_callable=process_current_data,
    provide_context=True,
    dag=dag
)

process_air_operator = PythonOperator(
    task_id='process_air',
    python_callable=process_air_quality,
    provide_context=True,
    dag=dag
)

insert_location_operator = PythonOperator(
    task_id='insert_location_data',
    python_callable=insert_into_DB,
    provide_context=True,
    op_kwargs={'table_name': 'location', 'xcom_key': 'process_location', 'key': 'location_df'},
    dag=dag
)

insert_current_operator = PythonOperator(
    task_id='insert_current_data',
    python_callable=insert_into_DB,
    provide_context=True,
    op_kwargs={'table_name': 'current', 'xcom_key': 'process_current', 'key': 'current_df'},
    dag=dag
)

insert_air_operator = PythonOperator(
    task_id='insert_air_data',
    python_callable=insert_into_DB,
    provide_context=True,
    op_kwargs={'table_name': 'air_quality', 'xcom_key': 'process_air', 'key': 'air_df'},
    dag=dag
)

#Set the task dependencies
"""
here task dependencies are defined to make that clear which operator will execute first 
and then other dependent operator respectively each after another.
"""
extract_data_operator >> process_location_operator
extract_data_operator >> process_current_operator
extract_data_operator >> process_air_operator

process_location_operator >> insert_location_operator
process_current_operator >> insert_current_operator
process_air_operator >> insert_air_operator





