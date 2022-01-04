"""
Unit test module to test Great Expectations Operators.

Requirements can be installed in, for instance, a virtual environment, with::

    pip install -r requirements.txt`

and tests can be run with, for instance::

    pytest -p no:warnings
"""

import logging
import os
import unittest.mock as mock
from contextlib import contextmanager
from pathlib import Path

import pytest
from airflow.exceptions import AirflowException
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (CheckpointConfig,
                                                        DataContextConfig)
from great_expectations.exceptions.exceptions import CheckpointNotFoundError

from great_expectations_provider.operators.great_expectations import \
    GreatExpectationsOperator

logger = logging.getLogger(__name__)

# Set relative paths for Great Expectations directory and sample data
base_path = Path(__file__).parents[2]
data_dir = os.path.join(base_path, "include", "data")

ge_root_dir = os.path.join(base_path, "include", "great_expectations")

@pytest.fixture()
def in_memory_data_context_config():
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
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
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

    return data_context_config


@pytest.fixture
def in_memory_checkpoint_config():
    checkpoint_config = CheckpointConfig(
        **{
            "name": "taxi.pass.from_config",
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
    return checkpoint_config


def test_great_expectations_operator__context_root_dir_and_checkpoint_name_pass():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.pass.chk",
    )
    result = operator.execute(context={})
    logger.info(result)
    assert result["success"]


def test_great_expectations_operator__data_context_config_and_checkpoint_name_pass(
    in_memory_data_context_config,
):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        checkpoint_name="taxi.pass.chk",
    )
    result = operator.execute(context={})
    logger.info(result)
    assert result["success"]


def test_great_expectations_operator__data_context_config_and_checkpoint_config_pass(
    in_memory_data_context_config, in_memory_checkpoint_config
):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        checkpoint_config=in_memory_checkpoint_config,
    )
    result = operator.execute(context={})
    logger.info(result)
    assert result["success"]


def test_great_expectations_operator__checkpoint_config_with_substituted_batch_request_works_and_fails(
    in_memory_data_context_config, in_memory_checkpoint_config
):
    failing_batch_request = BatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "yellow_tripdata_sample_2019-02.csv",
            "data_connector_query": {"index": -1},
        }
    )

    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        checkpoint_config=in_memory_checkpoint_config,
        checkpoint_kwargs={"validations": [{"batch_request": failing_batch_request}]},
        fail_task_on_validation_failure=False,
    )
    result = operator.execute(context={})  # should fail the suite
    logger.info(result)
    assert result["success"] is False


def test_great_expectations_operator__checkpoint_config_with_substituted_expectation_suite_works_and_fails(
    in_memory_data_context_config, in_memory_checkpoint_config
):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        checkpoint_config=in_memory_checkpoint_config,
        checkpoint_kwargs={"expectation_suite_name": "taxi.demo_fail"},
        fail_task_on_validation_failure=False,
    )
    result = operator.execute(context={})  # should fail the suite
    logger.info(result)
    assert result["success"] is False


def test_great_expectations_operator__raises_error_without_data_context():
    with pytest.raises(ValueError):
        operator = GreatExpectationsOperator(
            task_id="task_id", checkpoint_name="taxi.pass.chk"
        )


def test_great_expectations_operator__raises_error_with_data_context_root_dir_and_data_context_config(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        operator = GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_context_root_dir=ge_root_dir,
            checkpoint_name="taxi.pass.chk",
        )


def test_great_expectations_operator__raises_error_without_checkpoint(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        operator = GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
        )


def test_great_expectations_operator__raises_error_with_checkpoint_name_and_checkpoint_config(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        operator = GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_context_root_dir=ge_root_dir,
            checkpoint_name="taxi.pass.chk",
        )
        

def test_great_expectations_operator__invalid_checkpoint_name():
    with pytest.raises(CheckpointNotFoundError):
        operator = GreatExpectationsOperator(
            task_id="task_id",
            checkpoint_name="invalid-checkpoint.name",
            data_context_root_dir=ge_root_dir,
        )


def test_great_expectations_operator__validation_failure_raises_exc():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.fail.chk",
    )
    with pytest.raises(AirflowException):
        operator.execute(context={})


def test_great_expectations_operator__validation_failure_logs_warning(caplog):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.fail.chk",
        fail_task_on_validation_failure=False,
    )
    operator._log = logging.getLogger("my_test_logger")
    caplog.set_level(level="WARNING", logger="my_test_logger")
    caplog.clear()
    result = operator.execute(context={})
    assert result["success"] is False
    assert ("my_test_logger", logging.WARNING) in (
        (r.name, r.levelno) for r in caplog.records
    )


def test_great_expectations_operator__validation_failure_callback():
    my_callback = mock.MagicMock()
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.fail.chk",
        fail_task_on_validation_failure=False,
        validation_failure_callback=my_callback,
    )
    result = operator.execute(context={})
    assert result["success"] is False
    my_callback.assert_called_once_with(result)


def test_great_expectations_operator__return_json_dict():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.pass.chk",
        return_json_dict=True
    )
    result = operator.execute(context={})
    logger.info(result)
    assert isinstance(result, dict)
    assert result["success"]