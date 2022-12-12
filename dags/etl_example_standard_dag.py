from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import random
import os

# Hello!

# constants
MY_LIMIT = os.environ['MY_ENV_VAR']

def _extract(upper_limit, ti):
    """Picks a random number between 10 and the upper limit"""

    upper_limit_int = int(upper_limit)
    random_number = random.randint(10,upper_limit_int)

    ti.xcom_push(key="extracted_val", value=random_number)

def _transform(ti):
    """Multiplies a number by 23.""" 

    extracted_val = ti.xcom_pull(task_ids="extract", key="extracted_val")
    transformed_val = extracted_val * 23

    ti.xcom_push(key="transformed_val", value=transformed_val)


def _load(ti):
    """Writes the result to a file."""

    transformed_val = ti.xcom_pull(task_ids="transform", key="transformed_val")

    f = open("/usr/local/airflow/include/storing_results.txt", "w")
    f.write(f"Result: {transformed_val}! ")
    f.close()

with DAG(
    dag_id="ETL_example_standard_dag",
    start_date=datetime(2022,7,1),
    schedule_interval=None,
    catchup=False
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract,
        op_kwargs={"upper_limit": MY_LIMIT}
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform
    )

    load = PythonOperator(
        task_id="load",
        python_callable=_load
    )

    extract >> transform >> load