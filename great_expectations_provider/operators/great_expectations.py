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

import logging

from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
import great_expectations as ge
from great_expectations.checkpoint import LegacyCheckpoint

log = logging.getLogger(__name__)


class GreatExpectationsOperator(BaseOperator):
    """
    An operator to leverage Great Expectations as a task in your Airflow DAG.

    :param run_name: Identifies the validation run (defaults to timestamp if not specified)
    :type run_name: Optional[str]
    :param data_context_root_dir: Path of the great_expectations directory
    :type data_context_root_dir: str
    :param data_contex: A great_expectations DataContext object
    :type data_contex: dict
    :param expectation_suite_name: The name of the Expectation Suite to use for validation
    :type expectation_suite_name: str
    :param batch_kwargs: The batch_kwargs to use for validation
    :type batch_kwargs: dict
    :param assets_to_validate: A list of dictionaries of batch_kwargs + Expectation Suites to use for validation
    :type assets_to_validate: iterable
    :param checkpoint_name: A Checkpoint name to use for validation
    :type checkpoint_name: str
    :param fail_task_on_validation_failure: Fail the Airflow task if the Great Expectation validation fails
    :type fail_task_on_validation_failure: bool
    :param validation_operator_name: name of a Great Expectations validation operator, defaults to action_list_operator
    :type validation_operator_name: Optional[str]
    :param **kwargs: kwargs
    :type **kwargs: Optional[dict]
    """
    ui_color = '#AFEEEE'
    ui_fgcolor = '#000000'
    template_fields = ('checkpoint_name', 'batch_kwargs', 'assets_to_validate')

    @apply_defaults
    def __init__(self,
                 *,
                 run_name=None,
                 data_context_root_dir=None,
                 data_context=None,
                 expectation_suite_name=None,
                 batch_kwargs=None,
                 assets_to_validate=None,
                 checkpoint_name=None,
                 fail_task_on_validation_failure=True,
                 on_failure_callback=None,
                 **kwargs
                 ):
        super().__init__(**kwargs)

        self.run_name = run_name

        # Check that only one of the arguments is passed to set a data context (or none)
        if data_context_root_dir and data_context:
            raise ValueError("Only one of data_context_root_dir or data_context can be specified.")

        if data_context:
            self.data_context = data_context
        elif data_context_root_dir:
            self.data_context = ge.data_context.DataContext(data_context_root_dir)
        else:
            self.data_context = ge.data_context.DataContext()

        # Check that only the correct args to validate are passed
        # this doesn't cover the case where only one of expectation_suite_name or batch_kwargs is specified
        # along with one of the others, but I'm ok with just giving precedence to the correct one
        if sum(bool(x) for x in [(expectation_suite_name and batch_kwargs), assets_to_validate, checkpoint_name]) != 1:
            raise ValueError("Exactly one of expectation_suite_name + batch_kwargs, assets_to_validate, \
             or checkpoint_name is required to run validation.")

        self.expectation_suite_name = expectation_suite_name
        self.batch_kwargs = batch_kwargs
        self.assets_to_validate = assets_to_validate
        self.checkpoint_name = checkpoint_name

        self.fail_task_on_validation_failure = fail_task_on_validation_failure
        self.on_failure_callback = on_failure_callback

    def execute(self, context):
        log.info("Running validation with Great Expectations...")
        batches_to_validate = []

        if self.batch_kwargs and self.expectation_suite_name:
            batch = {"batch_kwargs": self.batch_kwargs, "expectation_suite_names": [self.expectation_suite_name]}
            batches_to_validate.append(batch)

        elif self.checkpoint_name:
            checkpoint = self.data_context.get_checkpoint(self.checkpoint_name)

            for batch in checkpoint.batches:

                batch_kwargs = batch["batch_kwargs"]
                for suite_name in batch["expectation_suite_names"]:
                    batch = {"batch_kwargs": batch_kwargs, "expectation_suite_names": [suite_name]}
                    batches_to_validate.append(batch)

        elif self.assets_to_validate:
            for asset in self.assets_to_validate:
                batch = {
                    "batch_kwargs": asset["batch_kwargs"],
                    "expectation_suite_names": [asset["expectation_suite_name"]]
                }
                batches_to_validate.append(batch)

        results = LegacyCheckpoint(
            name="_temp_checkpoint",
            data_context=self.data_context,
            batches=batches_to_validate,
        ).run()

        if not results["success"]:
            if self.fail_task_on_validation_failure:
                if self.on_failure_callback is None:
                    raise AirflowException("Validation with Great Expectations failed.")
                else:
                    self.on_failure_callback(results)
            else:
                log.warning("Validation with Great Expectations failed. Continuing DAG execution because "
                            "fail_task_on_validation_failure is set to False.")
        else:
            log.info("Validation with Great Expectations successful.")

        return results
