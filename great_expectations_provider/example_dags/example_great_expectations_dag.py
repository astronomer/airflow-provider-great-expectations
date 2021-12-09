"""
A DAG that demonstrates use of the operators in this provider package.
"""

import os
from pathlib import Path

import airflow
from airflow import DAG
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (CheckpointConfig,
                                                        DataContextConfig)

from great_expectations_provider.operators.great_expectations import \
    GreatExpectationsOperator

default_args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(1)}

dag = DAG(dag_id="example_great_expectations_dag", default_args=default_args)


base_path = Path(__file__).parents[2]
data_dir = os.path.join(base_path, "include", "data")

ge_root_dir = os.path.join(base_path, "include", "great_expectations")

data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_datasource": {
                "module_name": "great_expectations.datasource",
                "data_connectors": {
                    "default_inferred_data_connector_name": {
                        "default_regex": {
                            "group_names": ["data_asset_name"],
                            "pattern": "(.*)",
                        },
                        "base_directory": data_dir,
                        "module_name": "great_expectations.datasource.data_connector",
                        "class_name": "InferredAssetFilesystemDataConnector",
                    },
                    "default_runtime_data_connector_name": {
                        "batch_identifiers": ["default_identifier_name"],
                        "module_name": "great_expectations.datasource.data_connector",
                        "class_name": "RuntimeDataConnector",
                    },
                },
                "execution_engine": {
                    "module_name": "great_expectations.execution_engine",
                    "class_name": "PandasExecutionEngine",
                },
                "class_name": "Datasource",
            }
        },
        "config_variables_file_path": os.path.join(
            ge_root_dir, "uncommitted", "config_variables.yml"
        ),
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "expectations"),
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(
                        ge_root_dir, "uncommitted", "validations"
                    ),
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory": os.path.join(ge_root_dir, "checkpoints"),
                },
            },
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "checkpoint_store_name": "checkpoint_store",
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(
                        ge_root_dir, "uncommitted", "data_docs", "local_site"
                    ),
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        "anonymous_usage_statistics": {
            "data_context_id": "abcdabcd-1111-2222-3333-abcdabcdabcd",
            "enabled": False,
        },
        "notebooks": None,
        "concurrency": {"enabled": False},
    }
)

checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "taxi.demo",
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": "yellow_tripdata_sample_2019-01.csv",
                    "data_connector_query": {"index": -1},
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)

passing_batch_request = BatchRequest(
    **{
        "datasource_name": "my_datasource",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": "yellow_tripdata_sample_2019-01.csv",
        "data_connector_query": {"index": -1},
    }
)

ge_data_context_root_dir_with_checkpoint_name_pass = GreatExpectationsOperator(
    task_id="ge_data_context_root_dir_with_checkpoint_name_pass",
    data_context_root_dir=ge_root_dir,
    checkpoint_name="taxi.pass.chk",
    dag=dag,
)

ge_data_context_root_dir_with_checkpoint_name_fail_validation_and_not_task = GreatExpectationsOperator(
    task_id="ge_data_context_root_dir_with_checkpoint_name_fail_validation_and_not_task",
    data_context_root_dir=ge_root_dir,
    checkpoint_name="taxi.fail.chk",
    fail_task_on_validation_failure=False,
    dag=dag,
)

ge_checkpoint_kwargs_substitute_batch_request_fails_validation_but_not_task = GreatExpectationsOperator(
    task_id="ge_checkpoint_kwargs_substitute_batch_request_fails_validation_but_not_task",
    data_context_root_dir=ge_root_dir,
    checkpoint_name="taxi.pass.chk",
    checkpoint_kwargs={"expectation_suite_name": "taxi.demo_fail"},
    fail_task_on_validation_failure=False,
    dag=dag,
)

ge_data_context_config_with_checkpoint_config_pass = GreatExpectationsOperator(
    task_id="ge_data_context_config_with_checkpoint_config_pass",
    data_context_config=data_context_config,
    checkpoint_config=checkpoint_config,
    dag=dag,
)


ge_checkpoint_fails_and_runs_callback = GreatExpectationsOperator(
    task_id="ge_checkpoint_fails_and_runs_callback",
    data_context_root_dir=ge_root_dir,
    checkpoint_name="taxi.fail.chk",
    fail_task_on_validation_failure=False,
    validation_failure_callback=(lambda x: print("Callback successfully run", x)),
    dag=dag,
)

(
    ge_data_context_root_dir_with_checkpoint_name_pass
    >> ge_data_context_root_dir_with_checkpoint_name_fail_validation_and_not_task
    >> ge_checkpoint_kwargs_substitute_batch_request_fails_validation_but_not_task
    >> ge_data_context_config_with_checkpoint_config_pass
    >> ge_checkpoint_fails_and_runs_callback
)
