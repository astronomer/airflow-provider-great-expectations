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
from great_expectations.checkpoint import LegacyCheckpoint, Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.data_context.types.base import (
    DataContextConfig,
    GCSStoreBackendDefaults, CheckpointConfig,
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

    def __init__(
        self,
        *,
        run_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = None,
        data_context_config: DataContextConfig = None,
        # expectation_suite_name: Optional[str] = None,
        # batch_kwargs: Optional[Dict] = None,
        # batch_request: Optional[Dict] = None,
        # assets_to_validate: Optional[List[Dict]] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        checkpoint_kwargs: Optional[Dict] = None,
        # validation_operator_name: Optional[str] = None,
        fail_task_on_validation_failure: Optional[bool] = True,
        validation_failure_callback: Optional[
            Callable[[CheckpointResult], None]
        ] = None,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.run_name: Optional[str] = run_name
        self.data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = data_context_root_dir
        self.data_context_config: DataContextConfig = data_context_config
        self.checkpoint_name: Optional[str] = checkpoint_name
        self.checkpoint_config: Optional[CheckpointConfig] = checkpoint_config
        self.checkpoint_kwargs: Optional[dict] = checkpoint_kwargs
        self.fail_task_on_validation_failure: Optional[bool] = fail_task_on_validation_failure
        self.validation_failure_callback: Optional[Callable[[CheckpointResult], None]] = validation_failure_callback

        # Check that only one of the arguments is passed to set a data context
        if not bool(self.data_context_root_dir) ^ bool(self.data_context_config):
            raise ValueError("Exactly one of data_context_root_dir or data_context_config must be specified.")

        # Check that only one of the arguments is passed to set a checkpoint
        if not bool(self.checkpoint_name) ^ bool(self.checkpoint_config):
            raise ValueError("Exactly one of checkpoint_name or checkpoint_config must be specified.")

        # Instantiate the Data Context
        self.log.info("Ensuring data context is valid...")
        if data_context_root_dir:
            self.data_context: BaseDataContext = ge.data_context.DataContext(
                context_root_dir=self.data_context_root_dir
            )
        else:
            self.data_context: BaseDataContext = BaseDataContext(project_config=self.data_context_config)

        # Instantiate the Checkpoint
        if self.checkpoint_name:
            if self.checkpoint_name not in self.data_context.list_checkpoints():
                raise ValueError(f"Checkpoint of name {self.checkpoint_name} not found in the supplied data_context. "
                                 f"Please ensure that a Checkpoint of the given name exists and try again.")
            self.checkpoint: Checkpoint = self.data_context.get_checkpoint(name=self.checkpoint_name)
        else:
            try:
                self.checkpoint: Checkpoint = Checkpoint(
                    data_context=self.data_context,
                    **self.checkpoint_config
                )

            except:
                raise ValueError("Unable to instantiate a Checkpoint with the included configuration. Please ensure "
                                 "that your Checkpoint configuration is correct.")


    def execute(self, context: Any) -> CheckpointResult:
        self.log.info("Running validation with Great Expectations...")

        if self.checkpoint_kwargs:
            result = self.checkpoint.run(**self.checkpoint_kwargs)


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
