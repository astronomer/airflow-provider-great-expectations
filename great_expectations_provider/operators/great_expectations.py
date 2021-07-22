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

import datetime
import os
from typing import Any, Callable, Dict, List, Optional, Union
import uuid

import airflow
from airflow.exceptions import AirflowException

if airflow.__version__ > "2.0":
    from airflow.hooks.base import BaseHook
else:
    from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import great_expectations as ge
from great_expectations.checkpoint import LegacyCheckpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context.types.base import (
    DataContextConfig,
    GCSStoreBackendDefaults,
)
from great_expectations.data_context import BaseDataContext


class GreatExpectationsOperator(BaseOperator):
    """
    An operator to leverage Great Expectations as a task in your Airflow DAG.

    Current list of expectations types:
    https://docs.greatexpectations.io/en/latest/reference/glossary_of_expectations.html

    How to create expectations files:
    https://docs.greatexpectations.io/en/latest/guides/tutorials/how_to_create_expectations.html

    :param run_name: Identifies the validation run (defaults to timestamp if not specified)
    :type run_name: Optional[str]
    :param data_context_root_dir: Path of the great_expectations directory
    :type data_context_root_dir: Optional[str]
    :param data_context: A great_expectations `DataContext` object
    :type data_context: Optional[BaseDataContext]
    :param expectation_suite_name: The name of the Expectation Suite to use for validation
    :type expectation_suite_name: Optional[str]
    :param batch_kwargs: The batch_kwargs to use for validation
    :type batch_kwargs: Optional[dict]
    :param assets_to_validate: A list of dictionaries of batch_kwargs + Expectation Suites to use for validation
    :type assets_to_validate: Optional[list[dict]]
    :param checkpoint_name: A Checkpoint name to use for validation
    :type checkpoint_name: Optional[str]
    :param validation_operator_name: name of a Great Expectations validation operator, defaults to action_list_operator
    :type validation_operator_name: Optional[str]
    :param fail_task_on_validation_failure: Fail the Airflow task if the Great Expectation validation fails
    :type fail_task_on_validation_failure: Optiopnal[bool]
    :param validation_failure_callback: Called when the Great Expectations validation fails
    :type validation_failure_callback: Callable[[CheckpointResult], None]
    :param **kwargs: kwargs
    :type **kwargs: Optional[dict]
    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"
    template_fields = (
        "checkpoint_name",
        "batch_kwargs",
        "assets_to_validate",
        "data_context_root_dir",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        run_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = None,
        data_context: Optional[BaseDataContext] = None,
        expectation_suite_name: Optional[str] = None,
        batch_kwargs: Optional[Dict] = None,
        assets_to_validate: Optional[List[Dict]] = None,
        checkpoint_name: Optional[str] = None,
        validation_operator_name: Optional[str] = None,
        fail_task_on_validation_failure: Optional[bool] = True,
        validation_failure_callback: Optional[
            Callable[[CheckpointResult], None]
        ] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.run_name: Optional[str] = run_name

        # Check that only one of the arguments is passed to set a data context (or none)
        if data_context_root_dir and data_context:
            raise ValueError(
                "Only one of data_context_root_dir or data_context can be specified."
            )

        self.data_context_root_dir: Optional[str] = data_context_root_dir
        self.data_context: Optional[BaseDataContext] = data_context

        # Check that only the correct args to validate are passed
        # this doesn't cover the case where only one of expectation_suite_name or batch_kwargs is specified
        # along with one of the others, but I'm ok with just giving precedence to the correct one
        if (
            sum(
                bool(x)
                for x in [
                    (expectation_suite_name and batch_kwargs),
                    assets_to_validate,
                    checkpoint_name,
                ]
            )
            != 1
        ):
            raise ValueError(
                "Exactly one of expectation_suite_name + batch_kwargs, "
                "assets_to_validate, or checkpoint_name is required to run validation."
            )

        self.expectation_suite_name: Optional[str] = expectation_suite_name
        self.batch_kwargs: Optional[Dict] = batch_kwargs
        self.assets_to_validate: Optional[List[Dict]] = assets_to_validate
        self.checkpoint_name: Optional[str] = checkpoint_name
        self.validation_operator_name: Optional[str] = validation_operator_name
        self.fail_task_on_validation_failure = fail_task_on_validation_failure
        self.validation_failure_callback = validation_failure_callback

    def create_data_context(self) -> BaseDataContext:
        """Create and return the :class:`~ge.data_context.DataContext` to be used
        during validation.

        Subclasses should override this to provide custom logic around creating a
        `DataContext`. This is called at task execution time, which defers connecting
        to the meta database and allows for the use of templated variables.
        """
        if self.data_context_root_dir:
            return ge.data_context.DataContext(
                context_root_dir=self.data_context_root_dir
            )
        else:
            return ge.data_context.DataContext()

    def execute(self, context: Any) -> CheckpointResult:
        self.log.info("Ensuring data context exists...")
        if not self.data_context:
            self.log.info("Data context does not exist, creating now.")
            self.data_context: Optional[BaseDataContext] = self.create_data_context()

        self.log.info("Running validation with Great Expectations...")
        batches_to_validate = []

        if self.batch_kwargs and self.expectation_suite_name:
            batch = {
                "batch_kwargs": self.batch_kwargs,
                "expectation_suite_names": [self.expectation_suite_name],
            }
            batches_to_validate.append(batch)

        elif self.checkpoint_name:
            checkpoint = self.data_context.get_checkpoint(self.checkpoint_name)

            for batch in checkpoint.batches:

                batch_kwargs = batch["batch_kwargs"]
                for suite_name in batch["expectation_suite_names"]:
                    batch = {
                        "batch_kwargs": batch_kwargs,
                        "expectation_suite_names": [suite_name],
                    }
                    batches_to_validate.append(batch)

        elif self.assets_to_validate:
            for asset in self.assets_to_validate:
                batch = {
                    "batch_kwargs": asset["batch_kwargs"],
                    "expectation_suite_names": [asset["expectation_suite_name"]],
                }
                batches_to_validate.append(batch)

        result = LegacyCheckpoint(
            name="_temp_checkpoint",
            data_context=self.data_context,
            validation_operator_name=self.validation_operator_name,
            batches=batches_to_validate,
        ).run(run_name=self.run_name)

        self.handle_result(result)

        return result

    def handle_result(self, result: CheckpointResult) -> None:
        """Handle the given validation result.

        If the validation failed, this method will:

        - call :attr:`~validation_failure_callback`, if set
        - raise an :exc:`airflow.exceptions.AirflowException`, if
          :attr:`~fail_task_on_validation_failure` is `True`, otherwise, log a warning
          message

        If the validation succeeded, this method will simply log an info message.

        :param result: The validation result
        :type result: CheckpointResult
        """
        if not result["success"]:
            if self.validation_failure_callback:
                self.validation_failure_callback(result)
            if self.fail_task_on_validation_failure:
                raise AirflowException("Validation with Great Expectations failed.")
            else:
                self.log.warning(
                    "Validation with Great Expectations failed. "
                    "Continuing DAG execution because "
                    "fail_task_on_validation_failure is set to False."
                )
        else:
            self.log.info("Validation with Great Expectations successful.")


class GreatExpectationsBigQueryOperator(GreatExpectationsOperator):
    """
    An operator that allows you to use Great Expectations to validate data Expectations
    against a BigQuery table or the result of a SQL query.

    The Expectations need to be stored in a JSON file sitting in an accessible GCS
    bucket. The validation results are output to GCS in both JSON and HTML formats.

    :param gcp_project: The GCP project of the bucket holding the Great Expectations
        artifacts.
    :type gcp_project: str
    :param gcs_bucket: GCS bucket holding the Great Expectations artifacts.
    :type gcs_bucket: str
    :param gcs_expectations_prefix: GCS prefix where the Expectations file can be
        found. For example, "ge/expectations".
    :type gcs_expectations_prefix: str
    :param gcs_validations_prefix:  GCS prefix where the validation output files should
        be saved. For example, "ge/expectations".
    :type gcs_validations_prefix: str
    :param gcs_datadocs_prefix:  GCS prefix where the validation datadocs files should
        be saved. For example, "ge/expectations".
    :type gcs_datadocs_prefix: str
    :param query: The SQL query that defines the set of data to be validated. If the
        query parameter is filled in then the `table` parameter cannot be.
    :type query: Optional[str]
    :param table: The name of the BigQuery table with the data to be validated. If the
        table parameter is filled in then the `query` parameter cannot be.
    :type table: Optional[str]
    :param bq_dataset_name:  The name of the BigQuery data set where any temp tables
        will be created that are needed as part of the GE validation process.
    :type bq_dataset_name: str
    :param bigquery_conn_id: ID of the connection with the credentials info needed to
        connect to BigQuery.
    :type bigquery_conn_id: str

    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"
    template_fields = GreatExpectationsOperator.template_fields + (
        "bq_dataset_name",
        "gcp_project",
        "gcs_bucket",
    )

    @apply_defaults
    def __init__(
        self,
        *,
        gcp_project: str,
        gcs_bucket: str,
        gcs_expectations_prefix: str,
        gcs_validations_prefix: str,
        gcs_datadocs_prefix: str,
        query: Optional[str] = None,
        table: Optional[str] = None,
        bq_dataset_name: str,
        bigquery_conn_id: str = "bigquery_default",
        **kwargs
    ):
        self.query: Optional[str] = query
        self.table: Optional[str] = table
        self.bigquery_conn_id = bigquery_conn_id
        self.bq_dataset_name = bq_dataset_name
        self.gcp_project = gcp_project
        self.gcs_bucket = gcs_bucket
        self.gcs_expectations_prefix = gcs_expectations_prefix
        self.gcs_validations_prefix = gcs_validations_prefix
        self.gcs_datadocs_prefix = gcs_datadocs_prefix
        super().__init__(batch_kwargs=self.get_batch_kwargs(), **kwargs)

    def create_data_context(self) -> BaseDataContext:
        """Create and return the `DataContext` with a BigQuery `DataSource`."""
        # Get the credentials information for the BigQuery data source from the BigQuery
        # Airflow connection
        conn = BaseHook.get_connection(self.bigquery_conn_id)
        connection_json = conn.extra_dejson
        credentials_path = connection_json.get("extra__google_cloud_platform__key_path")
        data_context_config = DataContextConfig(
            config_version=2,
            datasources={
                "bq_datasource": {
                    "credentials": {
                        "url": "bigquery://"
                        + self.gcp_project
                        + "/"
                        + self.bq_dataset_name
                        + "?credentials_path="
                        + credentials_path
                    },
                    "class_name": "SqlAlchemyDatasource",
                    "module_name": "great_expectations.datasource",
                    "data_asset_type": {
                        "module_name": "great_expectations.dataset",
                        "class_name": "SqlAlchemyDataset",
                    },
                }
            },
            store_backend_defaults=GCSStoreBackendDefaults(
                default_bucket_name=self.gcs_bucket,
                default_project_name=self.gcp_project,
                validations_store_prefix=self.gcs_validations_prefix,
                expectations_store_prefix=self.gcs_expectations_prefix,
                data_docs_prefix=self.gcs_datadocs_prefix,
            ),
        )

        return BaseDataContext(project_config=data_context_config)

    def get_batch_kwargs(self) -> Dict:
        # Tell GE where to fetch the batch of data to be validated.
        batch_kwargs = {
            "datasource": "bq_datasource",
        }

        # Check that only one of the arguments is passed to set a data context (or none)
        if self.query and self.table:
            raise ValueError("Only one of query or table can be specified.")
        if self.query:
            batch_kwargs["query"] = self.query
            batch_kwargs["data_asset_name"] = self.bq_dataset_name
            batch_kwargs["bigquery_temp_table"] = self.get_temp_table_name(
                "ge_" + datetime.datetime.now().strftime("%Y%m%d") + "_", 10
            )
        elif self.table:
            batch_kwargs["table"] = self.table
            batch_kwargs["data_asset_name"] = self.bq_dataset_name

        self.log.info("batch_kwargs: " + str(batch_kwargs))

        return batch_kwargs

    def get_temp_table_name(
        self, desired_prefix: str, desired_length_of_random_portion: int
    ) -> str:
        random_string = str(uuid.uuid4().hex)
        random_portion_of_name = random_string[:desired_length_of_random_portion]
        full_name = desired_prefix + random_portion_of_name
        self.log.info("Generated name for temporary table: %s", full_name)
        return full_name
