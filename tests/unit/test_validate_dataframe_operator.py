import json
from typing import Literal
from unittest.mock import Mock

import pandas as pd
import pytest
from great_expectations import ExpectationSuite
from great_expectations.core import (
    ExpectationValidationResult,
)
from great_expectations.expectations import Expectation, ExpectColumnValuesToBeInSet
from pytest_mock import MockerFixture

from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)


class TestValidateDataFrameOperator:
    def test_expectation(self) -> None:
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
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

    def test_expectation_suite(self) -> None:
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectationSuite(
            name="test suite",
            expectations=[
                ExpectColumnValuesToBeInSet(
                    column=column_name,
                    value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
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
            pytest.param("BOOLEAN_ONLY", {}, id="boolean"),
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
                id="basic",
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
                id="summary",
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
                id="complete",
            ),
        ],
    )
    def test_result_format(
        self,
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
        expected_result: dict,
    ) -> None:
        # arrange
        column_name = "col_A"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )

        validate_df = GXValidateDataFrameOperator(
            task_id="test-result-format",
            configure_dataframe=configure_dataframe,
            expect=expect,
            result_format=result_format,
        )

        # act
        result = validate_df.execute(context={})

        # assert
        # check the result of the first (only) expectation
        assert result["expectations"][0]["result"] == expected_result

    def test_context_type_ephemeral(self, mock_gx: Mock):
        """Expect that param context_type creates an EphemeralDataContext."""
        # arrange
        context_type: Literal["ephemeral"] = "ephemeral"

        validate_batch = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(),
            expect=Mock(),
            context_type=context_type,
        )

        # act
        validate_batch.execute(context={})

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type)

    def test_context_type_cloud(self, mock_gx: Mock):
        """Expect that param context_type creates a CloudDataContext."""
        # arrange
        context_type: Literal["cloud"] = "cloud"

        validate_batch = GXValidateDataFrameOperator(
            task_id="validate_df_success",
            configure_dataframe=Mock(),
            expect=Mock(),
            context_type=context_type,
        )

        # act
        validate_batch.execute(context={})

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type)
