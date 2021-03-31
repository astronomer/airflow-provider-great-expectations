"""
A DAG that demonstrates implementation of the GreatExpectationsBigQueryOperator. 

Note: you wil need to reference the necessary data assets and expectations suites in your project. You can find samples available in the provider source directory. At the moment, you will also need to add a connection titled `my_bigquery_conn_id` to get this DAG to import into your Airflow environment.

To view steps on running this DAG, check out the Provider Readme: https://github.com/great-expectations/airflow-provider-great-expectations#examples
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
    dag_id='example_great_expectations_bq_dag',
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
