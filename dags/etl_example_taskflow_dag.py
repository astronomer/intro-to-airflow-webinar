from airflow import DAG
from airflow.decorators import task

from datetime import datetime
import random
import os

# constants
MY_LIMIT = os.environ['MY_ENV_VAR']

with DAG(
    dag_id="ETL_example_TaskFlowAPI_dag",
    start_date=datetime(2022,7,1),
    schedule_interval=None,
    catchup=False
) as dag:

    @task()
    def extract(upper_limit):
        """Picks a random number between 10 and the upper limit"""

        upper_limit_int = int(upper_limit)
        random_number = random.randint(10,upper_limit_int)

        return random_number

    @task()
    def transform(extracted_val):
        """Multiplies a number by 23."""

        transformed_val = extracted_val * 23

        return transformed_val

    @task()
    def load(transformed_val):
        """Writes the result to a file."""

        f = open("/usr/local/airflow/include/storing_results.txt", "w")
        f.write(f"Result: {transformed_val}! ")
        f.close()

    extracted = extract(MY_LIMIT)

    load(transform(extracted))
