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

log = logging.getLogger(__name__)


class GreatExpectationsOperator(BaseOperator):
    ui_color = '#AFEEEE'
    ui_fgcolor = '#000000'
    template_fields = ['checkpoint_name', 'batch_kwargs', 'assets_to_validate']

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
                 validation_operator_name="action_list_operator",
                 **kwargs
                 ):
        """
        Args:
            run_name: Optional run_name to identify the validation run (defaults to timestamp if not specified)
            data_context_root_dir: Path of the great_expectations directory
            data_context: A great_expectations DataContext object
            expectation_suite_name: The name of the Expectation Suite to use for validation
            batch_kwargs: The batch_kwargs to use for validation
            assets_to_validate: A list of dictionaries of batch_kwargs + expectation suites to use for validation
            checkpoint_name: A Checkpoint name to use for validation
            fail_task_on_validation_failure: Fail the Airflow task if the Great Expectation validation fails
            validation_operator_name: Optional name of a Great Expectations validation operator, defaults to
            action_list_operator
            **kwargs: Optional kwargs
        """
        super().__init__(**kwargs)

        self.run_name = run_name

        # Check that only one of the arguments is passed to set a data context (or none)
        if data_context_root_dir and data_context:
            raise ValueError("Only one of data_context_root_dir or data_context can be specified.")

        if data_context:
            self.data_context = data_context
        elif data_context_root_dir:
            self.data_context = ge.DataContext(data_context_root_dir)
        else:
            self.data_context = ge.DataContext()

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

        self.validation_operator_name = validation_operator_name

    def execute(self, context):
        log.info("Running validation with Great Expectations...")

        batches_to_validate = []
        validation_operator_name = self.validation_operator_name

        if self.batch_kwargs and self.expectation_suite_name:
            batch = self.data_context.get_batch(self.batch_kwargs, self.expectation_suite_name)
            batches_to_validate.append(batch)

        elif self.checkpoint_name:
            checkpoint = self.data_context.get_checkpoint(self.checkpoint_name)
            validation_operator_name = checkpoint["validation_operator_name"]

            for batch in checkpoint["batches"]:
                batch_kwargs = batch["batch_kwargs"]
                for suite_name in batch["expectation_suite_names"]:
                    suite = self.data_context.get_expectation_suite(suite_name)
                    batch = self.data_context.get_batch(batch_kwargs, suite)
                    batches_to_validate.append(batch)

        elif self.assets_to_validate:
            for asset in self.assets_to_validate:
                batch = self.data_context.get_batch(
                    asset["batch_kwargs"],
                    asset["expectation_suite_name"]
                )
                batches_to_validate.append(batch)

        results = self.data_context.run_validation_operator(
            validation_operator_name,
            assets_to_validate=batches_to_validate,
            run_name=self.run_name
        )

        if not results["success"]:
            if self.fail_task_on_validation_failure:
                raise AirflowException("Validation with Great Expectations failed.")
            else:
                log.warning("Validation with Great Expectations failed. Continuing DAG execution because "
                            "fail_task_on_validation_failure is set to False.")
        else:
            log.info("Validation with Great Expectations successful.")
