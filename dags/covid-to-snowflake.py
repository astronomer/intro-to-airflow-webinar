from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import os
import requests

S3_CONN_ID = 'astro-s3-workshop'
BUCKET = 'astro-workshop-bucket'

def upload_to_s3(endpoint, date):
    # Instanstiate
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # Base URL
    url = 'https://covidtracking.com/api/v1/states/'
    
    # Grab data
    res = requests.get(url+'{0}/{1}.csv'.format(endpoint, date))

    # Take string, upload to S3 using predefined method
    s3_hook.load_string(res.text, '{0}_{1}.csv'.format(endpoint, date), bucket_name=BUCKET, replace=True)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


endpoints = ['ca', 'co', 'wa', 'or']
date = '{{ yesterday_ds_nodash }}'

with DAG('covid_data_s3_to_snowflake',
         start_date=datetime(2020, 6, 1),
         max_active_runs=1,
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:

    t0 = DummyOperator(task_id='start') 

    pivot_data = SnowflakeOperator(
        task_id='call_pivot_sproc',
        snowflake_conn_id='snowflake',
        sql='call pivot_state_data();',
        role='KENTEND',
        schema='SANDBOX_KENTEND'
    ) 

    for endpoint in endpoints:
        generate_files = PythonOperator(
            task_id='generate_file_{0}'.format(endpoint),
            python_callable=upload_to_s3,
            op_kwargs={'endpoint': endpoint, 'date': date}
        )

        snowflake = S3ToSnowflakeOperator(
            task_id='upload_{0}_snowflake'.format(endpoint),
            s3_keys=['{0}_{1}.csv'.format(endpoint, date)],
            stage='covid_stage',
            table='STATE_DATA',
            schema='SANDBOX_KENTEND',
            file_format='covid_csv',
            role='KENTEND',
            snowflake_conn_id='snowflake'
        )

        t0 >> generate_files >> snowflake >> pivot_data