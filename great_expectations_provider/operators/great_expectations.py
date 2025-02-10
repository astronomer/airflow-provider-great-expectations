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
import warnings
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Union

import great_expectations as ge
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, BaseOperatorLink, Connection, XCom
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
)
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    DataContextConfig,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.new_datasource import Datasource
from great_expectations.util import deep_filter_properties_iterable
from pandas import DataFrame

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GreatExpectationsDataDocsLink(BaseOperatorLink):
    """
    This class is deprecated and will be removed in an upcoming release where operators supporting GX Core 1.x will be
    added.

    Constructs a link to Great Expectations data docs site.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        warnings.warn(
            (
                "This class is deprecated and will be removed in an upcoming release where operators supporting "
                "GX Core 1.x will be added."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)

    @property
    def name(self):
        return "Great Expectations Data Docs"

    def get_link(self, operator, *, ti_key) -> str:
        if ti_key is not None:
            return XCom.get_value(key="data_docs_url", ti_key=ti_key)
        return (
            XCom.get_one(
                dag_id=ti_key.dag_id,
                task_id=ti_key.task_id,
                run_id=ti_key.run_id,
                key="data_docs_url",
            )
            or ""
        )


class GreatExpectationsOperator(BaseOperator):
    """
    This class is deprecated and will be removed in an upcoming release where operators supporting GX Core 1.x will be
    added.

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
    :param schema: If provided, overwrites the default schema provided by the connection
    :type schema: Optional[str]
    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"
    template_fields = (
        "run_name",
        "conn_id",
        "data_context_root_dir",
        "checkpoint_name",
        "checkpoint_kwargs",
        "query_to_validate",
        "schema",
    )
    template_ext = (".sql",)
    operator_extra_links = (GreatExpectationsDataDocsLink(),)

    def __init__(
        self,
        run_name: Optional[str] = None,
        conn_id: Optional[str] = None,
        execution_engine: Optional[str] = None,
        expectation_suite_name: Optional[str] = None,
        data_asset_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = None,
        data_context_config: Optional[DataContextConfig] = None,
        dataframe_to_validate: Optional[DataFrame] = None,  # should we allow a Spark DataFrame as well?
        query_to_validate: Optional[str] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        checkpoint_kwargs: Optional[Dict[str, Any]] = None,
        validation_failure_callback: Optional[Callable[[CheckpointResult], None]] = None,
        fail_task_on_validation_failure: bool = True,
        return_json_dict: bool = False,
        use_open_lineage: bool = True,
        schema: Optional[str] = None,
        runtime_environment: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs,
    ) -> None:
        warnings.warn(
            (
                "This class is deprecated and will be removed in an upcoming release where operators supporting "
                "GX Core 1.x will be added."
            ),
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)

        self.data_asset_name: Optional[str] = data_asset_name
        self.run_name: Optional[str] = run_name
        self.conn_id: Optional[str] = conn_id
        self.execution_engine: Optional[str] = execution_engine
        self.expectation_suite_name: Optional[str] = expectation_suite_name
        self.data_context_root_dir: Optional[Union[str, bytes, os.PathLike[Any]]] = data_context_root_dir
        self.data_context_config: Optional[DataContextConfig] = data_context_config
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
        self.is_dataframe = True if self.dataframe_to_validate is not None else False
        self.datasource: Optional[Datasource] = None
        self.batch_request: Optional[BatchRequestBase] = None
        self.runtime_environment: Optional[Dict[str, Any]] = runtime_environment
        self.schema = schema
        self.kwargs = kwargs

        if self.is_dataframe and self.query_to_validate:
            raise ValueError(
                "Exactly one, or neither, of dataframe_to_validate or query_to_validate may be specified."
            )

        # Check that only one of the arguments is passed to set a data context
        if not (bool(self.data_context_root_dir) ^ bool(self.data_context_config)):
            raise ValueError("Exactly one of data_context_root_dir or data_context_config must be specified.")

        if self.is_dataframe and self.conn_id:
            raise ValueError(
                "Exactly one, or neither, of dataframe_to_validate or conn_id may be specified. If neither is"
                " specified, the data_context_root_dir is used to find the data source."
            )

        if self.query_to_validate and not self.conn_id:
            raise ValueError("A conn_id must be specified when query_to_validate is specified.")

        # A data asset name is also used to determine if a runtime env will be used; if it is not passed in,
        # then the data asset name is assumed to be configured in the data context passed in.
        if (self.is_dataframe or self.query_to_validate or self.conn_id) and not self.data_asset_name:
            raise ValueError("A data_asset_name must be specified with a runtime_data_source or conn_id.")

        # If a dataframe is specified, the execution engine must be specified as well
        if self.is_dataframe and not self.execution_engine:
            raise ValueError("An execution_engine must be specified if a dataframe is passed.")

        # Check that at most one of the arguments is passed to set a checkpoint
        if self.checkpoint_name and self.checkpoint_config:
            raise ValueError(
                "Exactly one, or neither, of checkpoint_name or checkpoint_config may be specified. If neither is"
                " specified, the default Checkpoint is used."
            )

        if not (self.checkpoint_name or self.checkpoint_config) and not self.expectation_suite_name:
            raise ValueError(
                "An expectation_suite_name must be specified if neither checkpoint_name nor checkpoint_config are."
            )

        # Check that when a data asset name is passed, a valid conn_id or dataframe_to_validate is passed as well
        # so the appropriate custom data assets can be generated
        if self.data_asset_name and not (self.is_dataframe or self.conn_id):
            raise ValueError(
                "When a data_asset_name is specified, a dataframe_to_validate or conn_id must also be specified"
                " to generate the data asset."
            )

        if isinstance(self.checkpoint_config, CheckpointConfig):
            self.checkpoint_config = deep_filter_properties_iterable(properties=self.checkpoint_config.to_dict())

        # If a schema is passed as part of the data_asset_name, use that schema
        if self.data_asset_name and "." in self.data_asset_name:
            # Assume data_asset_name is in the form "SCHEMA.TABLE"
            # Schema parameter always takes priority
            asset_list = self.data_asset_name.split(".")
            self.schema = self.schema or asset_list[0]
            # Update data_asset_name to be only the table
            self.data_asset_name = asset_list[1]

    def make_connection_configuration(self) -> Dict[str, str]:
        """Builds connection strings based off existing Airflow connections. Only supports necessary extras."""
        uri_string = ""
        driver = ""
        if not self.conn:
            raise ValueError(f"Connections does not exist in Airflow for conn_id: {self.conn_id}")
        self.schema = self.schema or self.conn.schema
        conn_type = self.conn.conn_type
        if conn_type in ("redshift", "mysql", "mssql"):
            odbc_connector = ""
            if conn_type in ("redshift"):
                odbc_connector = "postgresql+psycopg2"
                database_name = self.schema
            elif conn_type == "mysql":
                odbc_connector = "mysql"
                database_name = self.schema
            elif conn_type == "mssql":
                odbc_connector = "mssql+pyodbc"
                ms_driver = self.conn.extra_dejson.get("driver") or "ODBC Driver 17 for SQL Server"
                driver = f"?driver={ms_driver}"
                database_name = self.conn.schema or "master"
            else:
                raise ValueError(f"Conn type: {conn_type} is not supported.")
            uri_string = (
                f"{odbc_connector}://{self.conn.login}:{self.conn.password}@"
                f"{self.conn.host}:{self.conn.port}/{database_name}{driver}"
            )
        elif conn_type == "postgres":
            # the schema parameter in the postgres connection is the database name
            if self.conn.schema:
                postgres_database = self.conn.schema
                odbc_connector = "postgresql+psycopg2"
                uri_string = f"{odbc_connector}://{self.conn.login}:{self.conn.password}@{self.conn.host}:{self.conn.port}/{postgres_database}"  # noqa
            else:
                raise ValueError(
                    "Specify the name of the database in the schema parameter of the Postgres connection. See: https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html"  # noqa
                )
        elif conn_type == "snowflake":
            try:
                return self.build_snowflake_connection_config_from_hook()
            except ImportError:
                self.log.warning(
                    (
                        "Snowflake provider package could not be imported, "
                        "attempting to build connection uri from %s "
                        "Snowflake provider package is required for key-based auth, "
                        "see: https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html"
                    ),
                    self.conn,
                )

            snowflake_account = (
                self.conn.extra_dejson.get("account") or self.conn.extra_dejson["extra__snowflake__account"]
            )
            snowflake_region = self.conn.extra_dejson.get("region") or self.conn.extra_dejson.get(
                "extra__snowflake__region"
            )  # Snowflake region can be None for us-west-2
            snowflake_database = (
                self.conn.extra_dejson.get("database") or self.conn.extra_dejson["extra__snowflake__database"]
            )
            snowflake_warehouse = (
                self.conn.extra_dejson.get("warehouse") or self.conn.extra_dejson["extra__snowflake__warehouse"]
            )
            snowflake_role = self.conn.extra_dejson.get("role") or self.conn.extra_dejson["extra__snowflake__role"]

            if snowflake_region:
                uri_string = f"snowflake://{self.conn.login}:{self.conn.password}@{snowflake_account}.{snowflake_region}/{snowflake_database}/{self.schema}?warehouse={snowflake_warehouse}&role={snowflake_role}"  # noqa
            else:
                uri_string = f"snowflake://{self.conn.login}:{self.conn.password}@{snowflake_account}/{snowflake_database}/{self.schema}?warehouse={snowflake_warehouse}&role={snowflake_role}"  # noqa

        elif conn_type == "gcpbigquery":
            uri_string = f"{self.conn.host}{self.schema}"
        elif conn_type == "sqlite":
            uri_string = f"sqlite:///{self.conn.host}"
        elif conn_type == "aws":
            # TODO: Check which AWS resource is being used based on the hook. This is difficult because
            # we don't have access to a specific hook.
            athena_db = self.schema or self.params.get("database")
            s3_path = self.params.get("s3_path")
            region = self.params.get("region")
            if not s3_path:
                raise ValueError("No s3_path given in params.")
            if not region:
                raise ValueError("No region given in params.")
            if athena_db:
                uri_string = f"awsathena+rest://@athena.{region}.amazonaws.com/{athena_db}?s3_staging_dir={s3_path}"
            else:
                uri_string = f"awsathena+rest://@athena.{region}.amazonaws.com/?s3_staging_dir={s3_path}"
            # TODO: Add other AWS sources here as needed
        # TODO: Add and Trino support (if possible)
        else:
            raise ValueError(f"Conn type: {conn_type} is not supported.")
        return {"connection_string": uri_string}

    def build_snowflake_connection_config_from_hook(self) -> Dict[str, str]:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)

        # Support the operator overriding the schema
        # which is necessary for temp tables.
        hook.schema = self.schema or hook.schema

        conn = hook.get_connection(self.conn_id)
        engine = hook.get_sqlalchemy_engine()

        url = engine.url.render_as_string(hide_password=False)

        private_key_file = conn.extra_dejson.get("extra__snowflake__private_key_file") or conn.extra_dejson.get(
            "private_key_file"
        )

        if private_key_file:
            private_key_pem = Path(private_key_file).read_bytes()

            passphrase = None
            if conn.password:
                passphrase = conn.password.strip().encode()

            p_key = serialization.load_pem_private_key(private_key_pem, password=passphrase, backend=default_backend())

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            return {
                # Unfortunately GE uses deepcopy when instantiating the SqlAlchemyExecutionEngine
                # which uses pickle and SAEngine is not pickleable.
                # "engine": engine,
                "url": url,
                "connect_args": {
                    "private_key": pkb,
                },
            }

        return {"url": url}

    def build_configured_sql_datasource_config_from_conn_id(
        self,
    ) -> Datasource:
        create_temp_table = self.conn.extra_dejson.get("create_temp_table", True)

        datasource_config = {
            "name": f"{self.conn.conn_id}_configured_sql_datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "SqlAlchemyExecutionEngine",
                "create_temp_table": create_temp_table,
                **self.make_connection_configuration(),
            },
            "data_connectors": {
                "default_configured_asset_sql_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "class_name": "ConfiguredAssetSqlDataConnector",
                    "assets": {
                        f"{self.data_asset_name}": {
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "class_name": "Asset",
                            "schema_name": f"{self.schema}",
                            "batch_identifiers": ["airflow_run_id"],
                        },
                    },
                },
            },
        }
        return Datasource(**datasource_config)

    def build_configured_sql_datasource_batch_request(self):
        batch_request = {
            "datasource_name": f"{self.conn.conn_id}_configured_sql_datasource",
            "data_connector_name": "default_configured_asset_sql_data_connector",
            "data_asset_name": f"{self.data_asset_name}",
        }
        return BatchRequest(**batch_request)

    def build_runtime_sql_datasource_config_from_conn_id(
        self,
    ) -> Datasource:
        create_temp_table = self.conn.extra_dejson.get("create_temp_table", True)

        datasource_config = {
            "name": f"{self.conn.conn_id}_runtime_sql_datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "SqlAlchemyExecutionEngine",
                "create_temp_table": create_temp_table,
                **self.make_connection_configuration(),
            },
            "data_connectors": {
                "default_runtime_data_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["query_string", "airflow_run_id"],
                },
            },
            "data_context_root_directory": self.data_context_root_dir,
        }
        return Datasource(**datasource_config)

    def build_runtime_sql_datasource_batch_request(self):
        batch_request = {
            "datasource_name": f"{self.conn.conn_id}_runtime_sql_datasource",
            "data_connector_name": "default_runtime_data_connector",
            "data_asset_name": f"{self.data_asset_name}",
            "runtime_parameters": {"query": f"{self.query_to_validate}"},
            "batch_identifiers": {
                "query_string": f"{self.query_to_validate}",
                "airflow_run_id": "{{ task_instance_key_str }}",
            },
        }
        return RuntimeBatchRequest(**batch_request)

    def build_runtime_datasource(self) -> Datasource:
        datasource_config = {
            "name": f"{self.data_asset_name}_runtime_datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": f"{self.execution_engine}",
            },
            "data_connectors": {
                "default_runtime_connector": {
                    "module_name": "great_expectations.datasource.data_connector",
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["airflow_run_id"],
                },
            },
            "data_context_root_directory": self.data_context_root_dir,
        }
        return Datasource(**datasource_config)

    def build_runtime_datasource_batch_request(self):
        batch_request = {
            "datasource_name": f"{self.data_asset_name}_runtime_datasource",
            "data_connector_name": "default_runtime_connector",
            "data_asset_name": f"{self.data_asset_name}",
            "runtime_parameters": {"batch_data": self.dataframe_to_validate},
            "batch_identifiers": {"airflow_run_id": "{{ task_instance_key_str }}"},
        }
        return RuntimeBatchRequest(**batch_request)

    def build_runtime_datasources(self):
        """Builds datasources at runtime based on Airflow connections or for use with a dataframe."""
        self.conn = BaseHook.get_connection(self.conn_id) if self.conn_id else None
        batch_request = None
        if self.is_dataframe:
            self.datasource = self.build_runtime_datasource()
            batch_request = self.build_runtime_datasource_batch_request()
        elif isinstance(self.conn, Connection):
            if self.query_to_validate:
                self.datasource = self.build_runtime_sql_datasource_config_from_conn_id()
                batch_request = self.build_runtime_sql_datasource_batch_request()
            elif self.conn:
                self.datasource = self.build_configured_sql_datasource_config_from_conn_id()
                batch_request = self.build_configured_sql_datasource_batch_request()
            else:
                raise ValueError("Unrecognized, or lack of, runtime query or Airflow connection passed.")
        if not self.checkpoint_kwargs:
            self.batch_request = batch_request

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
                "(This behavior may be turned off by setting use_open_lineage to False.)"
            )
            openlineage_host = os.getenv("OPENLINEAGE_URL")
            openlineage_api_key = os.getenv("OPENLINEAGE_API_KEY")
            openlineage_namespace = os.getenv("OPENLINEAGE_NAMESPACE")
            if not (openlineage_host and openlineage_api_key and openlineage_namespace):
                raise ValueError(
                    "Could not find one of OpenLineage host, API Key, or Namespace environment variables."
                    f"\nHost: {openlineage_host}\nAPI Key: *****\nNamespace: {openlineage_namespace}"
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

    def build_default_checkpoint_config(self):
        """Builds a default checkpoint with default values."""
        self.run_name = self.run_name or f"{self.task_id}_{datetime.now().strftime('%Y-%m-%d::%H:%M:%S')}"
        checkpoint_config = CheckpointConfig(
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
        ).to_json_dict()
        filtered_config = deep_filter_properties_iterable(properties=checkpoint_config)

        return filtered_config

    def execute(self, context: "Context") -> Union[CheckpointResult, Dict[str, Any]]:
        """
        Determines whether a checkpoint exists or need to be built, then
        runs the resulting checkpoint.
        """
        self.log.info("Running validation with Great Expectations...")

        self.log.info("Instantiating Data Context...")
        if self.data_asset_name:
            self.build_runtime_datasources()
        if self.data_context_root_dir:
            self.data_context = ge.data_context.FileDataContext(
                context_root_dir=self.data_context_root_dir,
                runtime_environment=self.runtime_environment,
            )
        else:
            self.data_context = BaseDataContext(project_config=self.data_context_config)
        if self.datasource:
            # Add the datasource after the data context is created because in the case of
            # loading from a file, we'd have to write the datasource to file, and we want
            # this to be a temporary datasource only used at runtime.
            self.data_context.datasources[self.datasource.name] = self.datasource

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
            self.checkpoint_config = self.build_default_checkpoint_config()
            self.checkpoint = instantiate_class_from_config(
                config=self.checkpoint_config,
                runtime_environment={"data_context": self.data_context},
                config_defaults={"module_name": "great_expectations.checkpoint"},
            )

        self.log.info("Running Checkpoint...")

        if self.batch_request:
            result = self.checkpoint.run(batch_request=self.batch_request)
        elif self.checkpoint_kwargs:
            result = self.checkpoint.run(**self.checkpoint_kwargs)
        else:
            result = self.checkpoint.run()

        data_docs_site = self.data_context.get_docs_sites_urls()[0]["site_url"]
        try:
            context["ti"].xcom_push(key="data_docs_url", value=data_docs_site)
        except KeyError:
            self.log.debug("Could not push data_docs_url to XCom.")
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
                result_list = []
                for _, value in result.run_results.items():
                    result_information = {}
                    result_information["statistics"] = value["validation_result"].statistics
                    result_information["expectation_suite_name"] = value["validation_result"].meta[
                        "expectation_suite_name"
                    ]
                    result_information["batch_definition"] = value["validation_result"].meta["active_batch_definition"]
                    result_list.append(result_information)
                    result_list.append("\n")

                if len(result_list) < 3:
                    result_list = result_list[0]

                raise AirflowException("Validation with Great Expectations failed.\n" f"Results\n {result_list}")
            else:
                self.log.warning(
                    "Validation with Great Expectations failed. "
                    "Continuing DAG execution because "
                    "fail_task_on_validation_failure is set to False."
                )
        else:
            self.log.info("Validation with Great Expectations successful.")
