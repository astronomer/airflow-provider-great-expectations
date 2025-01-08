import json
from typing import Literal
from unittest.mock import Mock
import pytest
from great_expectations.data_context import AbstractDataContext
from great_expectations.core.batch_definition import BatchDefinition

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
)
from pytest_mock import MockerFixture
from great_expectations.core import (
    ExpectationValidationResult,
)
from airflow.operators.python import PythonOperator


class TestValidateBatchOperator:
    def test_expectation(self):
        """Expect that an Expectation can be used as an `expect` parameter to generate a result."""

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

        validate_batch = GXValidateBatchOperator(
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
        """Expect that an ExpectationSuite can be used as an `expect` parameter to generate a result."""

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

        validate_batch = GXValidateBatchOperator(
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
    ):
        """Expect that valid values of param result_format alter result as expected."""

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

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=configure_ephemeral_batch_definition,
            expect=expect,
            batch_parameters={"dataframe": df},
            result_format=result_format,
        )

        # act
        result = validate_batch.execute(context={})

        # assert
        assert result["result"] == expected_result

    def test_context_type_ephemeral(self, mocker: MockerFixture):
        """Expect that param context_type creates an EphemeralDataContext."""
        # arrange
        context_type = "ephemeral"
        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: Mock(),
            expect=Mock(),
            batch_parameters={"dataframe": Mock()},
            context_type=context_type,
        )

        # act
        validate_batch.execute(context={})

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type)

    def test_context_type_cloud(self, mocker: MockerFixture):
        """Expect that param context_type creates a CloudDataContext."""
        # arrange
        context_type = "cloud"
        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: Mock(),
            expect=Mock(),
            batch_parameters={"dataframe": Mock()},
            context_type=context_type,
        )

        # act
        validate_batch.execute(context={})

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type)

    def test_batch_parameters(self):
        """Expect that param batch_parameters is passed to BatchDefinition.get_batch"""
        # arrange
        mock_batch_definition = Mock()
        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }

        validate_batch = GXValidateBatchOperator(
            task_id="validate_batch_success",
            configure_batch_definition=lambda context: mock_batch_definition,
            expect=Mock(),
            batch_parameters=batch_parameters,
            context_type="ephemeral",
        )

        # act
        validate_batch.execute(context={})

        # assert
        mock_batch_definition.get_batch.assert_called_once_with(
            batch_parameters=batch_parameters
        )
