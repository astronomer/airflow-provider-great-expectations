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
from pathlib import Path

import pandas as pd
import pytest
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
)
from great_expectations.datasource import Datasource
from great_expectations.exceptions.exceptions import CheckpointNotFoundError

from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

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
            "config_variables_file_path": os.path.join(ge_root_dir, "uncommitted", "config_variables.yml"),
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
                        "base_directory": os.path.join(ge_root_dir, "uncommitted", "validations"),
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
                        "base_directory": os.path.join(ge_root_dir, "uncommitted", "data_docs", "local_site"),
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


@pytest.fixture()
def constructed_sql_runtime_datasource():
    return {
        "name": "sqlite_conn_runtime_sql_datasource",
        "id": None,
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": "sqlite:///host",
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "module_name": "great_expectations.datasource.data_connector",
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["query_string", "airflow_run_id"],
            },
        },
    }


@pytest.fixture()
def constructed_sql_configured_datasource():
    return {
        "name": "sqlite_conn_configured_sql_datasource",
        "id": None,
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": "sqlite:///host",
        },
        "data_connectors": {
            "default_configured_asset_sql_data_connector": {
                "module_name": "great_expectations.datasource.data_connector",
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "my_sqlite_table": {
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "class_name": "Asset",
                        "schema_name": "my_schema",
                        "batch_identifiers": ["airflow_run_id"],
                    },
                },
            },
        },
    }


@pytest.fixture()
def mock_airflow_conn():
    conn = mock.Mock(conn_id="sqlite_conn", schema="my_schema", host="host", conn_type="sqlite")
    return conn


@pytest.fixture()
def runtime_sql_operator(in_memory_data_context_config):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="my_sqlite_table",
        query_to_validate="select * from my_table limit 10",
        conn_id="sqlite_conn",
        expectation_suite_name="taxi.demo",
    )
    return operator


@pytest.fixture()
def configured_sql_operator(in_memory_data_context_config):
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="my_sqlite_table",
        conn_id="sqlite_conn",
        expectation_suite_name="taxi.demo",
        run_name="my_run",
    )
    return operator


def test_great_expectations_operator__assert_template_fields_exist():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.pass.chk",
    )
    assert "run_name" in operator.template_fields
    assert "conn_id" in operator.template_fields
    assert "data_context_root_dir" in operator.template_fields
    assert "checkpoint_name" in operator.template_fields
    assert "checkpoint_kwargs" in operator.template_fields
    assert "query_to_validate" in operator.template_fields


def test_great_expectations_operator__assert_template_ext_exist():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="taxi.pass.chk",
    )
    for ext in operator.template_ext:
        assert ext == ".sql"


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


def test_checkpoint_config_and_checkpoint_kwargs_substituted_batch_request_works_and_fails(
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
        GreatExpectationsOperator(task_id="task_id", checkpoint_name="taxi.pass.chk")


def test_great_expectations_operator__raises_error_with_data_context_root_dir_and_data_context_config(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_context_root_dir=ge_root_dir,
            checkpoint_name="taxi.pass.chk",
        )


def test_great_expectations_operator__raises_error_without_checkpoint(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
        )


def test_great_expectations_operator__raises_error_with_checkpoint_name_and_checkpoint_config(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_context_root_dir=ge_root_dir,
            checkpoint_name="taxi.pass.chk",
        )


def test_great_expectations_operator__raises_error_with_dataframe_and_query(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_asset_name="test_runtime_data_asset",
            dataframe_to_validate=pd.DataFrame({}),
            query_to_validate="SELECT * FROM db;",
        )


def test_great_expectations_operator__raises_error_with_dataframe_and_conn_id(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_asset_name="test_runtime_data_asset",
            dataframe_to_validate=pd.DataFrame({}),
            conn_id="sqlite",
        )


def test_great_expectations_operator__raises_error_with_query_and_no_conn_id(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            data_asset_name="test_runtime_data_asset",
            query_to_validate="SELECT * FROM db;",
        )


def test_great_expectations_operator__raises_error_with_runtime_datasource_no_data_asset_name(
    in_memory_data_context_config,
):
    with pytest.raises(ValueError):
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            query_to_validate="SELECT * FROM db;",
        )
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            dataframe_to_validate=pd.DataFrame({}),
        )
        GreatExpectationsOperator(
            task_id="task_id",
            data_context_config=in_memory_data_context_config,
            conn_id="sqlite",
        )


def test_great_expectations_operator__invalid_checkpoint_name():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        checkpoint_name="invalid-checkpoint.name",
        data_context_root_dir=ge_root_dir,
    )
    with pytest.raises(CheckpointNotFoundError):
        operator.execute(context={})


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
    assert ("my_test_logger", logging.WARNING) in ((r.name, r.levelno) for r in caplog.records)


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
        return_json_dict=True,
    )
    result = operator.execute(context={})
    logger.info(result)
    assert isinstance(result, dict)
    assert result["success"]


def test_great_expectations_operator__custom_expectation_plugin():
    import sys

    sys.path.append(ge_root_dir)

    from object_configs.example_runtime_batch_request_for_plugin_expectation import (
        runtime_batch_request,
    )
    from plugins.expectations.expect_column_values_to_be_alphabetical import (  # noqa: F401
        ExpectColumnValuesToBeAlphabetical,
    )

    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_root_dir=ge_root_dir,
        checkpoint_name="plugin_expectation_checkpoint.chk",
        checkpoint_kwargs={"validations": [{"batch_request": runtime_batch_request}]},
    )
    result = operator.execute(context={})
    logger.info(result)
    assert result["success"]


def test_great_expectations_operator__works_with_simple_checkpoint_and_checkpoint_kwargs(
    in_memory_data_context_config, in_memory_checkpoint_config
):
    batch_request = BatchRequest(
        **{
            "datasource_name": "my_datasource",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "yellow_tripdata_sample_2019-01.csv",
            "data_connector_query": {"index": -1},
        }
    )

    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        checkpoint_name="simple.chk",
        checkpoint_kwargs={"validations": [{"batch_request": batch_request, "expectation_suite_name": "taxi.demo"}]},
    )
    result = operator.execute(context={})  # should fail the suite
    logger.info(result)
    assert result["success"]


def test_great_expectations_operator__validate_pandas_dataframe_with_no_datasource_pass(
    in_memory_data_context_config,
):
    df = pd.read_csv(f"{data_dir}/yellow_tripdata_sample_2019-01.csv")

    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_dataframe",
        dataframe_to_validate=df,
        expectation_suite_name="taxi.demo",
        execution_engine="PandasExecutionEngine",
        fail_task_on_validation_failure=False,
    )
    assert operator.is_dataframe
    result = operator.execute(context={})

    assert result["success"]


def test_great_expectations_operator__validate_pandas_dataframe_with_no_datasource_fail(
    in_memory_data_context_config,
):
    smaller_df = pd.read_csv(f"{data_dir}/yellow_tripdata_sample_2019-01.csv").truncate(after=8000)

    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_dataframe",
        dataframe_to_validate=smaller_df,
        expectation_suite_name="taxi.demo",
        execution_engine="PandasExecutionEngine",
        fail_task_on_validation_failure=False,
    )
    result = operator.execute(context={})

    assert not result["success"]


def test_build_configured_sql_datasource_config_from_conn_id(
    in_memory_data_context_config,
    constructed_sql_configured_datasource,
    mock_airflow_conn,
    configured_sql_operator,
    monkeypatch,
):
    configured_sql_operator.conn = mock_airflow_conn
    monkeypatch.setattr(configured_sql_operator, "conn", mock_airflow_conn)
    monkeypatch.setattr(configured_sql_operator, "schema", mock_airflow_conn.schema)
    constructed_datasource = configured_sql_operator.build_configured_sql_datasource_config_from_conn_id()

    assert isinstance(constructed_datasource, Datasource)
    assert constructed_datasource.config == constructed_sql_configured_datasource


def test_build_runtime_sql_datasource_config_from_conn_id(
    in_memory_data_context_config,
    constructed_sql_runtime_datasource,
    mock_airflow_conn,
    runtime_sql_operator,
    monkeypatch,
):
    runtime_sql_operator.conn = mock_airflow_conn
    monkeypatch.setattr(runtime_sql_operator, "conn", mock_airflow_conn)

    constructed_datasource = runtime_sql_operator.build_runtime_sql_datasource_config_from_conn_id()

    assert isinstance(constructed_datasource, Datasource)

    assert constructed_datasource.config == constructed_sql_runtime_datasource


def test_build_configured_sql_datasource_batch_request(configured_sql_operator, mock_airflow_conn, monkeypatch):
    configured_sql_operator.conn = mock_airflow_conn
    monkeypatch.setattr(configured_sql_operator, "conn", mock_airflow_conn)
    batch_request = configured_sql_operator.build_configured_sql_datasource_batch_request()

    assert isinstance(batch_request, BatchRequest)

    assert batch_request.to_json_dict() == {
        "datasource_name": "sqlite_conn_configured_sql_datasource",
        "data_connector_name": "default_configured_asset_sql_data_connector",
        "data_asset_name": "my_sqlite_table",
        "batch_spec_passthrough": None,
        "data_connector_query": None,
        "limit": None,
    }


def test_build_runtime_sql_datasource_batch_request(runtime_sql_operator, mock_airflow_conn, monkeypatch):
    runtime_sql_operator.conn = mock_airflow_conn
    monkeypatch.setattr(runtime_sql_operator, "conn", mock_airflow_conn)
    batch_request = runtime_sql_operator.build_runtime_sql_datasource_batch_request()

    assert isinstance(batch_request, RuntimeBatchRequest)

    assert batch_request.to_json_dict() == {
        "datasource_name": "sqlite_conn_runtime_sql_datasource",
        "data_connector_name": "default_runtime_data_connector",
        "data_asset_name": "my_sqlite_table",
        "batch_spec_passthrough": None,
        "runtime_parameters": {"query": "select * from my_table limit 10"},
        "batch_identifiers": {
            "airflow_run_id": "{{ task_instance_key_str }}",
            "query_string": "select * from my_table limit 10",
        },
    }


def test_build_runtime_pandas_datasource_batch_request(mock_airflow_conn, monkeypatch):
    df = pd.read_csv(f"{data_dir}/yellow_tripdata_sample_2019-01.csv")

    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_dataframe",
        dataframe_to_validate=df,
        expectation_suite_name="taxi.demo",
        execution_engine="PandasExecutionEngine",
        fail_task_on_validation_failure=False,
    )

    batch_request = operator.build_runtime_datasource_batch_request()
    assert batch_request.to_json_dict() == {
        "datasource_name": "test_dataframe_runtime_datasource",
        "data_connector_name": "default_runtime_connector",
        "data_asset_name": "test_dataframe",
        "batch_spec_passthrough": None,
        "runtime_parameters": {"batch_data": "<class 'pandas.core.frame.DataFrame'>"},
        "batch_identifiers": {
            "airflow_run_id": "{{ task_instance_key_str }}",
        },
    }


def test_build_default_checkpoint_config(configured_sql_operator):
    checkpoint_config = configured_sql_operator.build_default_checkpoint_config()
    assert isinstance(checkpoint_config, dict)
    assert checkpoint_config == {
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
        "class_name": "Checkpoint",
        "config_version": 1.0,
        "expectation_suite_name": "taxi.demo",
        "module_name": "great_expectations.checkpoint",
        "run_name_template": "my_run",
        "batch_request": {},
        "evaluation_parameters": {},
        "profilers": [],
        "runtime_configuration": {},
        "validations": [],
    }


def test_great_expectations_operator__make_connection_string_redshift():
    test_conn_str = "postgresql+psycopg2://user:password@connection:5439/schema"
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="redshift_default",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="redshift_default",
        conn_type="redshift",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_postgres():
    test_conn_str = "postgresql+psycopg2://user:password@connection:5439/schema"
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="postgres_default",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="postgres_default",
        conn_type="postgres",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_mysql():
    test_conn_str = "mysql://user:password@connection:5439/schema"
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="mysql_default",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="mysql_default",
        conn_type="mysql",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_mssql():
    test_conn_str = "mssql+pyodbc://user:password@connection:5439/schema"
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="mssql_default",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="mssql_default",
        conn_type="mssql",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_snowflake():
    test_conn_str = (
        "snowflake://user:password@account.region-east-1/"
        "database/schema?warehouse=warehouse&role=role&private_key_file=/path/to/key.p8"
    )
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="snowflake_default",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="snowflake_default",
        conn_type="snowflake",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
        extra={
            "extra__snowflake__role": "role",
            "extra__snowflake__warehouse": "warehouse",
            "extra__snowflake__database": "database",
            "extra__snowflake__region": "region-east-1",
            "extra__snowflake__account": "account",
            "extra__snowflake__private_key_file": "/path/to/key.p8",
        },
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_sqlite():
    test_conn_str = "sqlite:///host"
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="mssql_default",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="sqlite_default",
        conn_type="sqlite",
        host="host",
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_schema_parameter():
    test_conn_str = (
        "snowflake://user:password@account.region-east-1/database/test_schema_parameter?warehouse=warehouse&role=role"
    )
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_schema.test_table",
        conn_id="snowflake_default",
        expectation_suite_name="suite",
        schema="test_schema_parameter",
    )
    operator.conn = Connection(
        conn_id="snowflake_default",
        conn_type="snowflake",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
        extra={
            "extra__snowflake__role": "role",
            "extra__snowflake__warehouse": "warehouse",
            "extra__snowflake__database": "database",
            "extra__snowflake__region": "region-east-1",
            "extra__snowflake__account": "account",
        },
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str


def test_great_expectations_operator__make_connection_string_data_asset_name_schema_parse():
    test_conn_str = (
        "snowflake://user:password@account.region-east-1/database/test_schema?warehouse=warehouse&role=role"
    )
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_schema.test_table",
        conn_id="snowflake_default",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="snowflake_default",
        conn_type="snowflake",
        host="connection",
        login="user",
        password="password",
        port=5439,
        extra={
            "extra__snowflake__role": "role",
            "extra__snowflake__warehouse": "warehouse",
            "extra__snowflake__database": "database",
            "extra__snowflake__region": "region-east-1",
            "extra__snowflake__account": "account",
        },
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str
    assert operator.data_asset_name == "test_table"


def test_great_expectations_operator__build_configured_sql_datasource_config_from_conn_id_uses_schema_override():
    test_conn_str = "sqlite:///host"
    datasource_config = {
        "name": "sqlite_default_configured_sql_datasource",
        "id": None,
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": test_conn_str,
        },
        "data_connectors": {
            "default_configured_asset_sql_data_connector": {
                "module_name": "great_expectations.datasource.data_connector",
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "test_table": {
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "class_name": "Asset",
                        "schema_name": "test_schema",
                        "batch_identifiers": ["airflow_run_id"],
                    },
                },
            },
        },
    }
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="default_schema.test_table",
        conn_id="sqlite_default",
        expectation_suite_name="suite",
        schema="test_schema",
    )
    operator.conn = Connection(
        conn_id="sqlite_default",
        conn_type="sqlite",
        host="host",
        login="user",
        password="password",
        schema="wrong_schema",
    )
    operator.conn_type = operator.conn.conn_type
    assert operator.make_connection_string() == test_conn_str
    assert operator.build_configured_sql_datasource_config_from_conn_id().config == datasource_config

    constructed_datasource = operator.build_configured_sql_datasource_config_from_conn_id()

    assert isinstance(constructed_datasource, Datasource)
    assert constructed_datasource.config == datasource_config


def test_great_expectations_operator__make_connection_string_raise_error():
    operator = GreatExpectationsOperator(
        task_id="task_id",
        data_context_config=in_memory_data_context_config,
        data_asset_name="test_runtime_data_asset",
        conn_id="unsupported_conn",
        query_to_validate="SELECT * FROM db;",
        expectation_suite_name="suite",
    )
    operator.conn = Connection(
        conn_id="unsupported_conn",
        conn_type="unsupported_conn",
        host="connection",
        login="user",
        password="password",
        schema="schema",
        port=5439,
    )
    operator.conn_type = operator.conn.conn_type
    with pytest.raises(ValueError):
        operator.make_connection_string()
