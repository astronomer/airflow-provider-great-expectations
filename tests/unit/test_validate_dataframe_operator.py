import json
from typing import Literal

import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet

import pytest
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
from great_expectations.core import (
    ExpectationValidationResult,
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

    @pytest.mark.parametrize(
        "result_format,expected_result",
        [
            pytest.param("BOOLEAN_ONLY", {}),
            pytest.param(
                "BASIC",
                {
                    "element_count": 3,
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "partial_unexpected_list": [],
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "unexpected_percent_total": 0.0,
                },
            ),
            pytest.param(
                "SUMMARY",
                {
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_counts": [],
                    "partial_unexpected_index_list": [],
                },
            ),
            pytest.param(
                "COMPLETE",
                {
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0,
                    "partial_unexpected_counts": [],
                    "partial_unexpected_index_list": [],
                    "unexpected_list": [],
                    "unexpected_index_list": [],
                    "unexpected_index_query": "df.filter(items=[], axis=0)",
                },
            ),
        ],
    )
    def test_result_format(
        self,
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
        expected_result: dict,
    ):
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name, value_set=["a", "b", "c", "d", "e"]
        )

        validate_batch = GXValidateDataFrameOperator(
            task_id="test-result-format",
            configure_dataframe=configure_dataframe,
            expect=expect,
            result_format=result_format,
        )

        # act
        result = validate_batch.execute(context={})

        # assert
        assert result["result"] == expected_result
