from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import os
import requests

# Example solution to exercise number 3

def get_sunset_data(latitude, longitude):
    # API URL
    url = 'https://api.sunrise-sunset.org/json?'

    res = requests.get(url+'lat={0}&lng={1}.csv'.format(latitude, longitude))



# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'email_on_success': True,
    'email': 'kenten@astronomer.io',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG('sunset_data',
         start_date=datetime(2020, 12, 1),
         max_active_runs=1,
         schedule_interval='0 1/12 * * *',
         default_args=default_args,
         catchup=False  # enable if you don't want historical dag runs to run
         ) as dag:


    seattle = PythonOperator(
        task_id='seattle_sunset',
        python_callable=get_sunset_data,
        op_kwargs={'latitude': 47.606, 'longitude': 122.3321}
    )

    hawaii = PythonOperator(
        task_id='hawaii_sunset',
        python_callable=get_sunset_data,
        op_kwargs={'latitude': 20.798, 'longitude': 156.3319}
    )