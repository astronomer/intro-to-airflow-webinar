# intro-to-airflow-webinar
This repo contains demo DAGs covered in Astronomer's Intro-to-Airflow webinar.

## Getting Started
The easiest way to run these example DAGs is to use the Astronomer CLI to get an Airflow instance up and running locally:

 1. [Install the Astronomer CLI](https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart)
 2. Clone this repo somewhere locally and navigate to it in your terminal
 3. Initialize an Astronomer project by running `astro dev init`
 4. Start Airflow locally by running `astro dev start`
 5. Navigate to localhost:8080 in your browser and you should see the tutorial DAGs there
 
Note that to use the `adf_great_expectations` DAG you would need Azure Data Factory and Azure Blob Storage resources and connections set up, as well as SMTP configured for the `send_email` task.
