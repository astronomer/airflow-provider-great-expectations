# A sample DAG to show some functionality of the GE operator. Steps to run:
#
# 1. You'll first need to install this operator:
# `pip install great_expectations_airflow`
#
# 2. Make sure airflow is installed and your dags_folder is configured to point to this directory.
#
# 3. When running a checkpoint task, change the path to the data directory in great_expectations/checkpoint/*.yml
#
# 4. You can then test-run a single task in this DAG using:
# Airflow v1.x: `airflow test example_great_expectations_dag ge_batch_kwargs_pass 2020-01-01`
# Airflow v2.x: `airflow tasks test example_great_expectations_dag ge_batch_kwargs_pass 2020-01-01`
#
# Note: The tasks that don't set an explicit data_context_root_dir need to be run from within
# this examples directory, otherwise GE won't know where to find the data context.

import os
import airflow
from airflow import DAG
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from great_expectations_provider.operators.great_expectations_bigquery import GreatExpectationsBigQueryOperator

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_great_expectations_dag',
    default_args=default_args
)

# This runs an expectation suite against a data asset that passes the tests
data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/yellow_tripdata_sample_2019-01.csv')
ge_batch_kwargs_pass = GreatExpectationsOperator(
    task_id='ge_batch_kwargs_pass',
    expectation_suite_name='taxi.demo',
    batch_kwargs={
        'path': data_file,
        'datasource': 'data__dir'
    },
    dag=dag
)

# This runs an expectation suite against a data asset that passes the tests
ge_batch_kwargs_list_pass = GreatExpectationsOperator(
    task_id='ge_batch_kwargs_list_pass',
    assets_to_validate=[
        {
            'batch_kwargs': {
                'path': data_file,
                'datasource': 'data__dir'
            },
            'expectation_suite_name': 'taxi.demo'
        }
    ],
    dag=dag
)

# This runs a checkpoint that will pass. Make sure the checkpoint yml file has the correct path to the data file.
ge_checkpoint_pass = GreatExpectationsOperator(
    task_id='ge_checkpoint_pass',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.pass.chk',
    dag=dag
)

# This runs a checkpoint that will fail. Make sure the checkpoint yml file has the correct path to the data file.
ge_checkpoint_fail = GreatExpectationsOperator(
    task_id='ge_checkpoint_fail',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.fail.chk',
    dag=dag
)

# This runs a checkpoint that will fail, but we set a flag to exit the task successfully.
# Make sure the checkpoint yml file has the correct path to the data file.
ge_checkpoint_fail_but_continue = GreatExpectationsOperator(
    task_id='ge_checkpoint_fail_but_continue',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.fail.chk',
    fail_task_on_validation_failure=False,
    dag=dag
)

# This runs a checkpoint and passes in a root dir.
# Make sure the checkpoint yml file has the correct path to the data file.
ge_root_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'great_expectations')
ge_checkpoint_pass_root_dir = GreatExpectationsOperator(
    task_id='ge_checkpoint_pass_root_dir',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.pass.chk',
    data_context_root_dir=ge_root_dir,
    dag=dag
)

# This is an example for the BigQuery operator which connects to BigQuery as a Datasource
# and uses Google Cloud Storage for the Expectation, Validation, and Data Docs stores.
# This example will require a BigQuery connection with the name "my_bigquery_conn_id" to be configured in
# Airflow, see the instructions here: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
# NOTE: This is a minimal working example. Check out the operator docstrings for more configuration options.
bq_task = GreatExpectationsBigQueryOperator(
    task_id='bq_task',
    gcp_project='my_project',
    gcs_bucket='my_bucket',
    gcs_expectations_prefix='expectations',  # GE will use a folder "my_bucket/expectations"
    gcs_validations_prefix='validations',  # GE will use a folder "my_bucket/validations"
    gcs_datadocs_prefix='data_docs',  # GE will use a folder "my_bucket/data_docs"
    expectation_suite_name='taxi.demo',  # GE will look for a file my_bucket/expectations/taxi/demo.json
    table='my_table_in_bigquery',
    bq_dataset_name='my_dataset',
    bigquery_conn_id='my_bigquery_conn_id',
    send_alert_email=False,
    dag=dag
)

# This runs an expectation suite against a data asset that passes the tests
# and automatically generates the Great Expectations data docs at your local path
# under great_expectations/data_docs
data_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data/yellow_tripdata_sample_2019-01.csv')
ge_batch_kwargs_pass = GreatExpectationsOperator(
    task_id='ge_batch_kwargs_pass',
    expectation_suite_name='taxi.demo',
    generate_local_datadocs=True,
    batch_kwargs={
        'path': data_file,
        'datasource': 'data__dir'
    },
    dag=dag
)

# TODO: Add an example for creating an in-memory data context using a dictionary config
