# Intro to Airflow Exercises

This document contains a few exercises for getting started with Airflow locally.

## 1. Get Airflow Running Locally

Install the Astronomer CLI, initialize an astro project (`astro dev init`), and start Airflow locally using Docker (`astro dev start`). For more detailed instructions, head [here](https://docs.astronomer.io/astro/cli/overview).

## 2. Update the Example DAG

Try making a couple of updates to the `example-dag-basic.py` DAG.

 - Add another `BashOperator` task that is dependent on `t3`. Check the Graph View 
 - Change the DAG's schedule to daily at midnight
 - Change the `PythonOperator` tasks to require 2 retries


## 3. Create Your Own DAG

Now let's try creating your own DAG! For this exercise, the purpose of the DAG will be to retrieve data from an API endpoint and save it locally before sending an email on completion. Your DAG should have the following functionality:

 - Retrieve data about the current sunrise and sunset times from from https://sunrise-sunset.org/api. Make two requests, one for your location (lat/long), and one for the location of a place you would like to travel to. Save them both to separate local CSVs
 - Send an email notification if both API calls were successful 

Additionally, configure your DAG to have the following properties:

 - Email notifications enabled for task failures and retries
 - Two retries per task with a one minute delay in between
 - A schedule of twice per day at the hours of your choosing


## 4. Working with Providers

Building off of the DAG you created in Step 3, let's say instead of saving the data locally we want to save the data to a CSV file on S3 (Note, if you have an S3 bucket handy you can use it here, but it's not necessary. You can still go through the steps without running the final DAG successfully.)

 - Install the `apache-airflow-providers-amazon` package in your local Airflow environment. For more info on provider packages, check out the [Astronomer Registry](https://registry.astronomer.io/)
 - Using the AWS provider package, add functionality to your DAG to save the CSV to S3. There are multiple ways you could do this, and all of them are valid! Hint: you could add logic to your existing tasks with a hook, or add a separate task using an operator.
 - Add an S3 connection to your Airflow environment that will be referenced in your DAG. It's okay if you don't have an actual S3 bucket to connect to, you can just add dummy information.
