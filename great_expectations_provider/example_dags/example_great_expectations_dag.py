"""
A DAG that demonstrates use of the operators in this provider package.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.models.baseoperator import chain

from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from include.great_expectations.object_configs.example_checkpoint_config import (
    example_checkpoint_config,
)
from include.great_expectations.object_configs.example_data_context_config import (
    example_data_context_config,
)
from include.great_expectations.object_configs.example_runtime_batch_request_for_plugin_expectation import (
    runtime_batch_request,
)

base_path = Path(__file__).parents[2]
data_dir = base_path / "include" / "data"
data_file = data_dir / "yellow_tripdata_sample_2019-01.csv"

ge_root_dir = str(base_path / "include" / "great_expectations")


with DAG(
    dag_id="example_great_expectations_dag",
    start_date=datetime(2021, 12, 15),
    catchup=False,
    schedule_interval=None,
) as dag:
    ge_data_context_root_dir_with_checkpoint_name_pass = GreatExpectationsOperator(
        task_id="ge_data_context_root_dir_with_checkpoint_name_pass",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.pass.chk",
    )

    ge_data_context_root_dir_with_checkpoint_name_fail_validation_and_not_task = GreatExpectationsOperator(
        task_id="ge_data_context_root_dir_with_checkpoint_name_fail_validation_and_not_task",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.fail.chk",
        fail_task_on_validation_failure=False,
    )

    ge_checkpoint_kwargs_substitute_batch_request_fails_validation_but_not_task = GreatExpectationsOperator(
        task_id="ge_checkpoint_kwargs_substitute_batch_request_fails_validation_but_not_task",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.pass.chk",
        checkpoint_kwargs={"expectation_suite_name": "taxi.demo_fail"},
        fail_task_on_validation_failure=False,
    )

    ge_data_context_config_with_checkpoint_config_pass = GreatExpectationsOperator(
        task_id="ge_data_context_config_with_checkpoint_config_pass",
        data_context_config=example_data_context_config,
        checkpoint_config=example_checkpoint_config,
    )

    ge_checkpoint_fails_and_runs_callback = GreatExpectationsOperator(
        task_id="ge_checkpoint_fails_and_runs_callback",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.fail.chk",
        fail_task_on_validation_failure=False,
        validation_failure_callback=(lambda x: print("Callback successfully run", x)),
    )

    ge_data_context_root_dir_with_checkpoint_name_using_custom_expectation_pass = GreatExpectationsOperator(
        task_id="ge_data_context_root_dir_with_checkpoint_name_using_custom_expectation_pass",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="plugin_expectation_checkpoint.chk",
        checkpoint_kwargs={"validations": [{"batch_request": runtime_batch_request}]},
    )

    ge_data_context_root_directory_no_checkpoint_pass = GreatExpectationsOperator(
        task_id="ge_data_context_root_directory_no_checkpoint_pass",
        data_context_root_dir=ge_root_dir,
        dataframe_to_validate=pd.read_csv(
            filepath_or_buffer=data_file,
            header=1,
            parse_dates=True,
            infer_datetime_format=True,
        ),
        expectation_suite_name="taxi.demo",
        data_asset_name="taxi_dataframe",
        execution_engine="PandasExecutionEngine",
    )

    ge_data_context_config_no_checkpoint_pass = GreatExpectationsOperator(
        task_id="ge_data_context_config_no_checkpoint_pass",
        data_context_config=example_data_context_config,
        dataframe_to_validate=pd.read_csv(
            filepath_or_buffer=data_file,
            header=1,
            parse_dates=True,
            infer_datetime_format=True,
        ),
        expectation_suite_name="taxi.demo",
        data_asset_name="taxi_dataframe",
        execution_engine="PandasExecutionEngine",
    )


chain(
    ge_data_context_root_dir_with_checkpoint_name_pass,
    ge_data_context_root_dir_with_checkpoint_name_fail_validation_and_not_task,
    ge_checkpoint_kwargs_substitute_batch_request_fails_validation_but_not_task,
    ge_data_context_config_with_checkpoint_config_pass,
    ge_checkpoint_fails_and_runs_callback,
    ge_data_context_root_dir_with_checkpoint_name_using_custom_expectation_pass,
    ge_data_context_root_directory_no_checkpoint_pass,
)
