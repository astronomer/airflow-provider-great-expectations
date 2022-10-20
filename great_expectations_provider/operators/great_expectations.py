#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import os
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import great_expectations as ge
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
)
from great_expectations.data_context.util import instantiate_class_from_config
from pandas import DataFrame

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GreatExpectationsRunLink(BaseOperatorLink):
    """Constructs a link to Great Expectations data docs site."""

    name = "Data Docs"

    def get_link(self, operator, *, kwargs) -> str:
        return operator.data_context.get_docs_sites_urls()


class GreatExpectationsOperator(BaseOperator):
    """
    An operator to leverage Great Expectations as a task in your Airflow DAG.

    Current list of expectations types:
    https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html

    How to create expectations files:
    https://docs.greatexpectations.io/en/latest/guides/tutorials/how_to_create_expectations.html

    :param run_name: Identifies the validation run (defaults to timestamp if not specified)
    :type run_name: Optional[str]
    :param conn_id: The name of a connection in Airflow
    :type conn_id: Optional[str]
    :param execution_engine: The execution engine to use when running Great Expectations
    :type execution_engine: Optional[str]
    :param expectation_suite_name: Name of the expectation suite to run if using a default Checkpoint
    :type expectation_suite_name: Optional[str]
    :param data_asset_name: The name of the table or dataframe that the default Data Context will load and default
        Checkpoint will run over
    :type data_asset_name: Optional[str]
    :param data_context_root_dir: Path of the great_expectations directory
    :type data_context_root_dir: Optional[str]
    :param data_context_config: A great_expectations `DataContextConfig` object
    :type data_context_config: Optional[DataContextConfig]
    :param dataframe_to_validate: A pandas dataframe to validate
    :type dataframe_to_validate: Optional[str]
    :param query_to_validate: A SQL query to validate
    :type query_to_validate: Optional[str]
    :param checkpoint_name: A Checkpoint name to use for validation
    :type checkpoint_name: Optional[str]
    :param checkpoint_config: A great_expectations `CheckpointConfig` object to use for validation
    :type checkpoint_config: Optional[CheckpointConfig]
    :param checkpoint_kwargs: A dictionary whose keys match the parameters of CheckpointConfig which can be used to
        update and populate the Operator's Checkpoint at runtime
    :type checkpoint_kwargs: Optional[Dict]
    :param validation_failure_callback: Called when the Great Expectations validation fails
    :type validation_failure_callback: Callable[[CheckpointResult], None]
    :param fail_task_on_validation_failure: Fail the Airflow task if the Great Expectation validation fails
    :type fail_task_on_validation_failure: bool
    :param return_json_dict: If True, returns a json-serializable dictionary instead of a CheckpointResult object
    :type return_json_dict: bool
    :param use_open_lineage: If True (default), creates an OpenLineage action if an OpenLineage environment is found
    :type use_open_lineage: bool
    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"

    operator_extra_links = (GreatExpectationsRunLink(),)

    def __init__(
        self,
        run_name: Optional[str] = None,
        conn_id: Optional[str] = None,
        execution_engine: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = None,
        data_context_config: Optional[DataContextConfig] = None,
        dataframe_to_validate: Optional[DataFrame] = None, # should we allow a Spark DataFrame as well?
        query_to_validate: Optional[str] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        checkpoint_kwargs: Optional[Dict[str, Any]] = None,
        validation_failure_callback: Optional[Callable[[CheckpointResult], None]] = None,
        fail_task_on_validation_failure: bool = True,
        return_json_dict: bool = False,
        use_open_lineage: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.data_asset_name: Optional[str] = data_asset_name
        self.run_name: Optional[str] = run_name
        self.conn_id: Optional[str] = conn_id
        self.execution_engine: Optional[str] = (
            execution_engine if execution_engine else "PandasExecutionEngine"
        )
        self.expectation_suite_name: Optional[str] = expectation_suite_name
        self.data_context_root_dir: Optional[Union[str, bytes, os.PathLike[Any]]] = data_context_root_dir
        self.data_context_config: DataContextConfig = data_context_config
        self.dataframe_to_validate: Optional[DataFrame] = dataframe_to_validate
        self.query_to_validate: Optional[str] = query_to_validate
        self.checkpoint_name: Optional[str] = checkpoint_name
        self.checkpoint_config: Union[CheckpointConfig, Dict[Any, Any]] = (
            checkpoint_config if checkpoint_config else {}
        )
        self.checkpoint_kwargs: Optional[Dict[str, Any]] = checkpoint_kwargs
        self.fail_task_on_validation_failure: Optional[bool] = fail_task_on_validation_failure
        self.validation_failure_callback: Optional[Callable[[CheckpointResult], None]] = validation_failure_callback
        self.return_json_dict: bool = return_json_dict
        self.use_open_lineage = use_open_lineage

        if self.dataframe_to_validate and self.query_to_validate:
            raise ValueError(
                "Exactly one, or neither, of dataframe_to_validate or query_to_validate may be specified."
            )

        # Check that only one of the arguments is passed to set a data context
        if not (bool(self.data_context_root_dir) ^ bool(self.data_context_config)):
            raise ValueError("Exactly one of data_context_root_dir or data_context_config must be specified.")

        if self.dataframe_to_validate and self.conn_id:
            raise ValueError(
                "Exactly one, or neither, of dataframe_to_validate or conn_id may be specified. If neither is"
                " specified, the data_context_root_dir is used to find the data source."
            )

        if self.query_to_validate and not self.conn_id:
            raise ValueError(
                "A conn_id must be specified when query_to_validate is specified."
            )

        # A data asset name is also used to determine if a runtime env will be used; if it is not passed in,
        # then the data asset name is assumed to be configured in the data context passed in.
        if (self.dataframe_to_validate or self.dataframe_to_validate or self.conn_id) and not self.data_asset_name:
            raise ValueError("A data_asset_name must be specified with a runtime_data_source or conn_id.")

        # Check that at most one of the arguments is passed to set a checkpoint
        if self.checkpoint_name and self.checkpoint_config:
            raise ValueError(
                "Exactly one, or neither, of checkpoint_name or checkpoint_config may be specified. If neither is"
                " specified, the default Checkpoint is used."
            )

        if not (self.checkpoint_name or self.checkpoint_config) and not self.expectation_suite_name:
            raise ValueError(
                "An expectation_suite_name must be supplied if neither checkpoint_name nor checkpoint_config are."
            )

        if type(self.checkpoint_config) == CheckpointConfig:
            self.checkpoint_config = self.checkpoint_config.to_json_dict()

    def make_connection_string(self) -> str:
        """Builds connection strings based off existing Airflow connections. Only supports necessary extras."""
        uri_string = ""
        if not self.conn:
            raise ValueError("No conn passed to operator.")
        if self.conn_type in ("redshift", "postgres", "mysql", "mssql"):
            odbc_connector = ""
            if self.conn_type in ("redshift", "postgres"):
                odbc_connector = "postgresql+psycopg2"
            elif self.conn_type == "mysql":
                odbc_connector = "mysql"
            else:
                odbc_connector = "mssql+pyodbc"
            uri_string = f"{odbc_connector}://{self.conn.login}:{self.conn.password}@{self.conn.host}:{self.conn.port}/{self.conn.schema}"  # noqa
        elif self.conn_type == "snowflake":
            uri_string = f"snowflake://{self.conn.login}:{self.conn.password}@{self.conn.extra_dejson['extra__snowflake__account']}.{self.conn.extra_dejson['extra__snowflake__region']}/{self.conn.extra_dejson['extra__snowflake__database']}/{self.conn.schema}?warehouse={self.conn.extra_dejson['extra__snowflake__warehouse']}&role={self.conn.extra_dejson['extra__snowflake__role']}"  # noqa
        elif self.conn_type == "gcpbigquery":
            uri_string = f"{self.conn.host}{self.conn.schema}"
        elif self.conn_type == "sqlite":
            uri_string = f"sqlite:///{self.conn.host}"
        # TODO: Add Athena and Trino support if possible
        else:
            raise ValueError(f"Conn type: {self.conn_type} is not supported.")
        return uri_string


    def build_configured_sql_datasource_config_from_conn_id(self):
        datasource_config = {
            "name": f"{self.conn.name}_datasource",
            "module_name": "great_expectations.datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": self.make_connection_string(),
            },
            "data_connectors": {
                "default_configured_asset_sql_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "class_name": "ConfiguredAssetSqlDataConnector",
                    "assets": {
                        f"{self.data_asset_name}": {
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "class_name": "Asset",
                            "schema_name": f"{self.conn.schema}",
                            "batch_identifiers": ["airflow_run_id"]
                        },
                    },
                },
            },
        }

        return datasource_config

    def build_configured_sql_datasource_batch_request(self):
        batch_request = {
            "datasource_name": f"{self.conn.name}_datasource",
            "data_connector_name": "default_configured_asset_sql_data_connector",
            "data_asset_name": f"{self.data_asset_name}",
            "batch_identifiers": {"airflow_run_id": f"{{ task_instance_key_str }}"}
        }
        return BatchRequest(**batch_request)

    def build_runtime_sql_datasource_config_from_conn_id(self):
        datasource_config = {
            "name": f"{self.conn.name}_datasource",
            "module_name": "great_expectations.datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": self.make_connection_string(),
            },
            "data_connectors": {
                "default_runtime_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": [
                        "query_string",
                        "airflow_run_id"
                    ]
                },
            },
        }

        return datasource_config

    def build_runtime_sql_datasource_batch_request(self):
        batch_request = {
            "datasource_name": f"{self.conn.name}_datasource",
            "data_connector_name": "default_runtime_data_connector",
            "data_asset_name": f"{self.data_asset_name}",
            "runtime_parameters": {"query": f"{self.query_to_validate}"},
            "batch_identifiers": {"query_string": f"{self.query_to_validate}", "airflow_run_id": f"{{ task_instance_key_str }}"}, 
        }
        return RuntimeBatchRequest(**batch_request)


    def build_runtime_pandas_datasource(self):
        datasource_config = {
            "name": f"{self.conn.name}_datasource",
            "module_name": "great_expectations.datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": f"{self.execution_engine}",
            },
            "data_connectors": {
                "default_runtime_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": [
                        "airflow_run_id"
                    ]
                },
            },
        }

        return datasource_config

    def build_runtime_pandas_datasource_batch_request(self):
        batch_request = {
            "datasource_name": f"{self.conn.name}_datasource",
            "data_connector_name": "default_runtime_data_connector",
            "data_asset_name": f"{self.data_asset_name}",
            "runtime_parameters": {"batch_data": self.dataframe_to_validate},
            "batch_identifiers": {"airflow_run_id": f"{{ task_instance_key_str }}"},
        }
        return RuntimeBatchRequest(**batch_request)

    def build_runtime_datasources(self) -> tuple([Dict[str, Any]]):
        """Builds datasources at runtime based on Airflow connections or for use with a dataframe."""
        if self.dataframe_to_validate is not None:
            datasource_config = self.build_runtime_pandas_datasource()
            batch_request = self.build_runtime_pandas_datasource_batch_request()

        elif self.query_to_validate:
            datasource_config = self.build_runtime_sql_datasource_config_from_conn_id()
            batch_request = self.build_runtime_sql_datasource_batch_request()

        elif self.conn:
            datasource_config = self.build_configured_sql_datasource_config_from_conn_id()
            batch_request = self.build_configured_sql_datasource_batch_request()

        else:
            raise ValueError("Unrecognized, or lack of, runtime or conn_id datasource passed.")
        return datasource_config, batch_request


    def build_runtime_env(self) -> tuple([Dict[str, Any]]):
        """Builds the runtime_environment dict that overwrites all other configs."""
        runtime_env: Dict[str, Any] = {}
        runtime_env["datasources"], batch_request = self.build_runtime_datasources()
        return runtime_env, batch_request


    def build_default_action_list(self) -> List[Dict[str, Any]]:
        """Builds a default action list for a default checkpoint."""
        action_list = [
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
        ]

        if (
            os.getenv("AIRFLOW__LINEAGE__BACKEND") == "openlineage.lineage_backend.OpenLineageBackend"
            and self.use_open_lineage
        ):
            self.log.info(
                "Found OpenLineage Connection, automatically connecting... "
                "\nThis behavior may be turned off by setting use_open_lineage to False."
            )
            openlineage_host = os.getenv("OPENLINEAGE_URL")
            openlineage_api_key = os.getenv("OPENLINEAGE_API_KEY")
            openlineage_namespace = os.getenv("OPENLINEAGE_NAMESPACE")
            if not (openlineage_host and openlineage_api_key and openlineage_namespace):
                raise ValueError(
                    "Could not find one of OpenLineage host, API Key, or Namespace environment variables."
                    f"\nHost: {openlineage_host}\nAPI Key: {openlineage_api_key}\nNamespace: {openlineage_namespace}"
                )
            action_list.append(
                {
                    "name": "open_lineage",
                    "action": {
                        "class_name": "OpenLineageValidationAction",
                        "module_name": "openlineage.common.provider.great_expectations",
                        "openlineage_host": openlineage_host,
                        "openlineage_apiKey": openlineage_api_key,
                        "openlineage_namespace": openlineage_namespace,
                        "job_name": f"validate_{self.task_id}",
                    },
                }
            )
        return action_list

    def build_default_checkpoint_config(self) -> CheckpointConfig:
        """Builds a default checkpoint with default values."""
        self.run_name = (
            self.run_name
            if self.run_name
            else f"{self.task_id}_{datetime.now().strftime('%Y-%m-%d::%H:%M:%S')}"
        )
        return CheckpointConfig(
            name=self.checkpoint_name,
            config_version=1.0,
            template_name=None,
            module_name="great_expectations.checkpoint",
            class_name="Checkpoint",
            run_name_template=self.run_name,
            expectation_suite_name=self.expectation_suite_name,
            batch_request=None,
            action_list=self.build_default_action_list(),
            evaluation_parameters={},
            runtime_configuration={},
            validations=None,
            profilers=[],
            ge_cloud_id=None,
            expectation_suite_ge_cloud_id=None,
        )

    def execute(self, context: "Context") -> Union[CheckpointResult, Dict[str, Any]]:
        """
        Determines whether a checkpoint exists or need to be built, then
        runs the resulting checkpoint.
        """
        self.log.info("Running validation with Great Expectations...")
        self.conn = BaseHook.get_connection(self.conn_id) if self.conn_id else None
        self.conn_type = self.conn.conn_type if self.conn else None

        self.log.info("Instantiating Data Context...")
        runtime_env, batch_request = self.build_runtime_env() if self.data_asset_name else ({}, {})
        if self.data_context_root_dir:
            self.data_context = ge.data_context.DataContext(
                context_root_dir=self.data_context_root_dir, runtime_environment=runtime_env
            )
        else:
            self.data_context = BaseDataContext(
                project_config=self.data_context_config, runtime_environment=runtime_env
            )

        self.log.info("Creating Checkpoint...")
        self.checkpoint: Checkpoint
        if self.checkpoint_name:
            self.checkpoint = self.data_context.get_checkpoint(name=self.checkpoint_name)
        elif self.checkpoint_config:
            self.checkpoint = instantiate_class_from_config(
                config=self.checkpoint_config,
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": "great_expectations.checkpoint"},
            )
        else:
            self.checkpoint_name = f"{self.data_asset_name}.{self.expectation_suite_name}.chk"
            self.checkpoint = instantiate_class_from_config(
                config=self.build_default_checkpoint_config().to_json_dict(),
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": "great_expectations.checkpoint"},
            )

        self.log.info("Running Checkpoint...")

        if batch_request:
            result = self.checkpoint.run(batch_request=batch_request)
        elif self.checkpoint_kwargs:
            result = self.checkpoint.run(**self.checkpoint_kwargs)
        else:
            result = self.checkpoint.run()

        self.log.info("GE Checkpoint Run Result:\n%s", result)
        self.handle_result(result)

        if self.return_json_dict:
            return result.to_json_dict()

        return result

    def handle_result(self, result: CheckpointResult) -> None:
        """Handle the given validation result.

        If the validation failed, this method will:

        - call `validation_failure_callback`, if set
        - raise an `airflow.exceptions.AirflowException`, if
          `fail_task_on_validation_failure` is `True`, otherwise, log a warning
          message

        If the validation succeeded, this method will simply log an info message.

        :param result: The validation result
        :type result: CheckpointResult
        """
        if not result["success"]:
            if self.validation_failure_callback:
                self.validation_failure_callback(result)
            if self.fail_task_on_validation_failure:
                statistics = result.statistics
                results = result.results
                raise AirflowException(
                    "Validation with Great Expectations failed.\n"
                    f"Stats: {str(statistics)}\n"
                    f"Results: {str(results)}"
                )
            else:
                self.log.warning(
                    "Validation with Great Expectations failed. "
                    "Continuing DAG execution because "
                    "fail_task_on_validation_failure is set to False."
                )
        else:
            self.log.info("Validation with Great Expectations successful.")
