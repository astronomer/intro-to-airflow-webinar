from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('paramaterized_query',
         start_date=datetime(2020, 6, 1),
         max_active_runs=3,
         schedule_interval='@daily',
         default_args=default_args,
         template_searchpath='/usr/local/airflow/include',
         catchup=False
         ) as dag:

         opr_param_query = SnowflakeOperator(
             task_id='param_query',
             snowflake_conn_id='snowflake',
             sql='param-query.sql'
         )