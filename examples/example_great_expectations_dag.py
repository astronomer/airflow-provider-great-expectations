# A sample DAG to show some functionality of the GE operator
# You'll first need to install this operator:
# `pip install -e .` (from the root directory of this repo)
# You can then test-run a single task using:
# `airflow test example_great_expectations_dag ge_checkpoint_pass 2020-01-01`
# Note: The tasks that don't set an explicit data_context_root_dir need to be run from within
# this examples directory, otherwise GE won't know where to find the data context

import os
import airflow
from airflow import DAG
from great_expectations_airflow.operators.great_expectations import GreatExpectationsOperator

default_args = {
    "owner": "Airflow",
    "start_date": airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id='example_great_expectations_dag',
    default_args=default_args
)

# This runs a checkpoint that will pass
ge_checkpoint_pass = GreatExpectationsOperator(
    task_id='ge_checkpoint_pass',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.pass.chk',
    dag=dag
)

# This runs a checkpoint that will fail
ge_checkpoint_fail = GreatExpectationsOperator(
    task_id='ge_checkpoint_fail',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.fail.chk',
    dag=dag
)

# This runs a checkpoint that will fail, but we set a flag to exit the task successfully
ge_checkpoint_fail_but_continue = GreatExpectationsOperator(
    task_id='ge_checkpoint_fail_but_continue',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.fail.chk',
    fail_task_on_validation_failure=False,
    dag=dag
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

# This runs a checkpoint and passes in a root dir
ge_root_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'great_expectations')
ge_checkpoint_pass_root_dir = GreatExpectationsOperator(
    task_id='ge_checkpoint_pass_root_dir',
    run_name='ge_airflow_run',
    checkpoint_name='taxi.pass.chk',
    data_context_root_dir=ge_root_dir,
    dag=dag
)

# TODO: example for creating an in-memory data context using a dictionary config

# Just a little dependency here to run a couple of tasks in sequence, this isn't meaningful in any way
ge_checkpoint_pass >> ge_checkpoint_fail_but_continue
