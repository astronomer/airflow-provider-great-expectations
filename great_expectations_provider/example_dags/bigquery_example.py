"""
A DAG that demonstrates implementation of the GreatExpectationsBigQueryOperator. 

Note: you wil need to reference the necessary data assets and expectations suites in your project. You can find samples available in the provider source directory.

Steps to run:

1. Download the Astronomer CLI and initialize a project: https://www.astronomer.io/docs/cloud/stable/develop/cli-quickstart

2. Add airflow-provider-great-expectations to your `requirements.txt` file.

3. Place this file in the `/dags` folder of your Astro project.

3. Add your Great Expectations context and data to the `include` directory of your project. You can copy the template context and data in the provider source if you'd prefer to start from our boilerplate.
https://github.com/great-expectations/airflow-provider-great-expectations

4. Be sure to flip the `enable_xcom_pickling` value in your airflow.cfg to true. You can do this by adding `ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True` to your Dockerfile.

5. If you're running a checkpoint task against a new data source, be sure change the path to the data directory in great_expectations/checkpoint/*.yml

6. Add a bigquery connection and title it `my_bigquery_conn_id`.

Note: You'll need to set the `ge_root_dir` path, `data_file` path, and data paths in your checkpoints if you are running this in a bespoke operating environment.
"""


import logging
import os
import airflow
from airflow import DAG
from great_expectations_provider.operators.great_expectations_bigquery import GreatExpectationsBigQueryOperator

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_great_expectations_dag',
    default_args=default_args
)


bq_task = GreatExpectationsBigQueryOperator(
    task_id='bq_task',
    gcp_project='my_project',
    gcs_bucket='my_bucket',
    # GE will use a folder "my_bucket/expectations"
    gcs_expectations_prefix='expectations',
    # GE will use a folder "my_bucket/validations"
    gcs_validations_prefix='validations',
    gcs_datadocs_prefix='data_docs',  # GE will use a folder "my_bucket/data_docs"
    # GE will look for a file my_bucket/expectations/taxi/demo.json
    expectation_suite_name='taxi.demo',
    table='my_table_in_bigquery',
    bq_dataset_name='my_dataset',
    bigquery_conn_id='my_bigquery_conn_id',
    send_alert_email=False,
    email_to='your@email.com',
    dag=dag
)
