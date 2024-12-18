import json

import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
from great_expectations.core import (
    ExpectationValidationResult,
    ExpectationSuiteValidationResult,
)


class TestValidateDataFrameOperator:
    def test_expectation(self):
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name, value_set=["a", "b", "c", "d", "e"]
        )

        validate_batch = GXValidateDataFrameOperator(
            task_id="validate_batch_success",
            configure_dataframe=configure_dataframe,
            expect=expect,
        )

        # act
        result = validate_batch.execute(context={})

        # assert
        json.dumps(result)  # result must be json serializable
        deserialized_result = ExpectationValidationResult(**result)
        assert deserialized_result.success

    def test_expectation_suite(self):
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectationSuite(
            name="test suite",
            expectations=[
                ExpectColumnValuesToBeInSet(
                    column=column_name, value_set=["a", "b", "c", "d", "e"]
                ),
            ],
        )

        validate_batch = GXValidateDataFrameOperator(
            task_id="validate_batch_success",
            configure_dataframe=configure_dataframe,
            expect=expect,
        )

        # act
        result = validate_batch.execute(context={})

        # assert
        json.dumps(result)  # result must be json serializable
        assert result["success"] is True
