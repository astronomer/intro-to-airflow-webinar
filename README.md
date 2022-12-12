# intro-to-airflow-webinar
This repo contains demo DAGs covered in Astronomer's Intro-to-Airflow webinar.

## Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://docs.astronomer.io/astro/cli/overview)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
 
## Example DAGs

This repo contains 6 example DAGs to help you get started with Airflow:

- `example-dag-basic.py`: contains 3 basic Python tasks defined with the TaskFlow API to show DAG syntax.
- `example-dag-advanced.py`: contains a DAG with different operators that demonstrates complex branching and task groups.
- `dependencies_example.py`: contains a DAG with BashOperators to show implementing complex task dependencies.
- `etl_example_standard_dag.py`: contains a basic ETL workflow implementing using standard PythonOperators and demonstrating passing data between tasks.
- `etl_example_taskflow_dag.py`: contains a basic ETL workflow implemented using TaskFlow API Python decorators.
- `data_quality_example_dag.py`: contains Common SQL provider tasks which complete data quality checks on a Snowflake database.

## Exercises

The `exercises.md` file in the root of this directory contains exercises to help you get more comfortable with Airflow and build on the examples in this repo.

## How to learn more

For more instruction on getting started with Airflow and working with these examples, we recommend the following resources:

- [Intro to Airflow webinar](https://www.astronomer.io/events/webinars/airflow-101-dec-2022/) walks through this repo in detail and answers audience questions.
- [Astronomer Webinars](https://www.astronomer.io/events/webinars/) contains all of our webinar and LIVE events on various Airflow topics.
- [Astronomer Learn](https://docs.astronomer.io/learn) contains concept docs and tutorials for basic through advanced use cases.
