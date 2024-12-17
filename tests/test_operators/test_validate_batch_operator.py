import json

from great_expectations.data_context import AbstractDataContext
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations_provider.operators.validate_batch import ValidateBatchOperator
import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations.core import (
    ExpectationValidationResult,
    ExpectationSuiteValidationResult,
)


class TestValidateBatchOperator:
    def test_expectation(self):
        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["a", "b", "c"]})
        expect = ExpectColumnValuesToBeInSet(
            column=column_name, value_set=["a", "b", "c", "d", "e"]
        )

        validate_batch = ValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=configure_ephemeral_batch_definition,
            expect=expect,
            batch_parameters={"dataframe": df},
        )

        # act
        result = validate_batch.execute(context={})

        # assert
        deserialized_result = ExpectationValidationResult(**result)
        assert deserialized_result.success

    def test_expectation_suite(self):
        # arrange
        def configure_ephemeral_batch_definition(
            context: AbstractDataContext,
        ) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["a", "b", "c"]})
        expect = ExpectationSuite(
            name="test suite",
            expectations=[
                ExpectColumnValuesToBeInSet(
                    column=column_name, value_set=["a", "b", "c", "d", "e"]
                ),
            ],
        )

        validate_batch = ValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=configure_ephemeral_batch_definition,
            expect=expect,
            batch_parameters={"dataframe": df},
        )

        # act
        result = validate_batch.execute(context={})

        # assert
        json.dumps(result)  # result must be json serializable
        assert result["success"] is True
