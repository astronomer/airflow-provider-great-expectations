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
from typing import Any, Callable, Dict, Optional, Union

import great_expectations as ge
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import \
    CheckpointResult
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (CheckpointConfig,
                                                        DataContextConfig)


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
    :param data_context_config: A great_expectations `DataContextConfig` object
    :type data_context_config: Optional[DataContextConfig]
    :param checkpoint_name: A Checkpoint name to use for validation
    :type checkpoint_name: Optional[str]
    :param checkpoint_config: A great_expectations `CheckpointConfig` object to use for validation
    :type checkpoint_config: Optional[CheckpointConfig]
    :param checkpoint_kwargs: A dictionary whose keys match the parameters of CheckpointConfig which can be used to update and populate the Operator's Checkpoint at runtime
    :type checkpoint_kwargs: Optional[Dict]
    :param fail_task_on_validation_failure: Fail the Airflow task if the Great Expectation validation fails
    :type fail_task_on_validation_failure: Optiopnal[bool]
    :param validation_failure_callback: Called when the Great Expectations validation fails
    :type validation_failure_callback: Callable[[CheckpointResult], None]
    :param return_json_dict: If True, returns a json-serializable dictionary instead of a CheckpointResult object
    :type return_json_dict: bool
    :param **kwargs: kwargs
    :type **kwargs: Optional[dict]
    """

    ui_color = "#AFEEEE"
    ui_fgcolor = "#000000"
    template_fields = (
        "data_context_root_dir",
        "checkpoint_name",
        "checkpoint_kwargs",
    )

    def __init__(
        self,
        *,
        run_name: Optional[str] = None,
        data_context_root_dir: Optional[Union[str, bytes, os.PathLike]] = None,
        data_context_config: Optional[DataContextConfig] = None,
        checkpoint_name: Optional[str] = None,
        checkpoint_config: Optional[CheckpointConfig] = None,
        checkpoint_kwargs: Optional[Dict] = None,
        fail_task_on_validation_failure: Optional[bool] = True,
        validation_failure_callback: Optional[
            Callable[[CheckpointResult], None]
        ] = None,
        return_json_dict: bool = False,
        **kwargs
    ):
        super().__init__(**kwargs)

        self.run_name: Optional[str] = run_name
        self.data_context_root_dir: Optional[
            Union[str, bytes, os.PathLike]
        ] = data_context_root_dir
        self.data_context_config: DataContextConfig = data_context_config
        self.checkpoint_name: Optional[str] = checkpoint_name
        self.checkpoint_config: Optional[CheckpointConfig] = checkpoint_config or {}
        self.checkpoint_kwargs: Optional[dict] = checkpoint_kwargs
        self.fail_task_on_validation_failure: Optional[
            bool
        ] = fail_task_on_validation_failure
        self.validation_failure_callback: Optional[
            Callable[[CheckpointResult], None]
        ] = validation_failure_callback
        self.return_json_dict: bool = return_json_dict

        # Check that only one of the arguments is passed to set a data context
        if not bool(self.data_context_root_dir) ^ bool(self.data_context_config):
            raise ValueError(
                "Exactly one of data_context_root_dir or data_context_config must be specified."
            )

        # Check that only one of the arguments is passed to set a checkpoint
        if not bool(self.checkpoint_name) ^ bool(self.checkpoint_config):
            raise ValueError(
                "Exactly one of checkpoint_name or checkpoint_config must be specified."
            )

        # Instantiate the Data Context
        self.log.info("Ensuring data context is valid...")
        if data_context_root_dir:
            self.data_context: BaseDataContext = ge.data_context.DataContext(
                context_root_dir=self.data_context_root_dir
            )
        else:
            self.data_context: BaseDataContext = BaseDataContext(
                project_config=self.data_context_config
            )

        # Instantiate the Checkpoint
        self.checkpoint: Checkpoint
        if self.checkpoint_name:
            self.checkpoint = self.data_context.get_checkpoint(
                name=self.checkpoint_name
            )
        else:
            self.checkpoint = Checkpoint(
                data_context=self.data_context, **self.checkpoint_config.to_json_dict()
            )

    def execute(self, context: Any) -> [CheckpointResult, dict]:
        self.log.info("Running validation with Great Expectations...")

        if self.checkpoint_kwargs:
            result = self.checkpoint.run(**self.checkpoint_kwargs)

        else:
            result = self.checkpoint.run()

        self.handle_result(result)

        if self.return_json_dict:
            return result.to_json_dict()

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
