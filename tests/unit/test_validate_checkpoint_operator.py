import json
from typing import Literal
from unittest.mock import Mock
import pytest
from great_expectations import ValidationDefinition
from great_expectations import Checkpoint
from pytest_mock import MockerFixture

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from great_expectations.data_context import AbstractDataContext, FileDataContext
import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet


class TestValidateCheckpointOperator:
    def test_validate_dataframe(self):
        # arrange
        column_name = "col_A"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            # setup data source, asset, batch definition
            batch_definition = (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )
            # setup expectation suite
            expectation_suite = context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name,
                            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
                        ),
                    ],
                )
            )
            # setup validation definition
            validation_definition = context.validation_definitions.add(
                ValidationDefinition(
                    name="test validation definition",
                    data=batch_definition,
                    suite=expectation_suite,
                )
            )
            # setup checkpoint
            checkpoint = context.checkpoints.add(
                Checkpoint(
                    name="test checkpoint",
                    validation_definitions=[validation_definition],
                )
            )
            return checkpoint

        df = pd.DataFrame({column_name: ["a", "b", "c"]})

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": df},
        )

        # act
        serialized_result = validate_cloud_checkpoint.execute(context={})

        # assert
        assert serialized_result["success"]
        json.dumps(serialized_result)  # result must be json serializable

    def test_context_type_ephemeral(self, mocker: MockerFixture):
        """Expect that param context_type creates an EphemeralDataContext."""
        # arrange
        context_type: Literal["ephemeral"] = "ephemeral"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            return Mock()

        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type)

    def test_context_type_cloud(self, mocker: MockerFixture):
        """Expect that param context_type creates a CloudDataContext."""
        # arrange
        context_type: Literal["cloud"] = "cloud"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            return Mock()

        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type)

    def test_context_type_filesystem(self, mocker: MockerFixture):
        """Expect that param context_type defers creation of data context to user."""
        # arrange
        context_type: Literal["file"] = "file"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            return Mock()

        def configure_file_data_context() -> FileDataContext:
            return Mock()

        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
            configure_file_data_context=configure_file_data_context,
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        mock_gx.get_context.assert_not_called()

    def test_context_type_filesystem_requires_configure_file_data_context(
        self, mocker: MockerFixture
    ):
        """Expect that param context_type requires the configure_file_data_context parameter."""
        # arrange
        context_type: Literal["file"] = "file"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            return Mock()

        # act/assert
        with pytest.raises(ValueError, match="configure_file_data_context"):
            GXValidateCheckpointOperator(
                task_id="validate_checkpoint_success",
                configure_checkpoint=configure_checkpoint,
                context_type=context_type,
                configure_file_data_context=None,  # must be defined
            )

    def test_batch_parameters(self):
        """Expect that param batch_parameters is passed to Checkpoint.run"""
        # arrange
        mock_checkpoint = Mock()

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            return mock_checkpoint

        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters=batch_parameters,
            context_type="ephemeral",
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        mock_checkpoint.run.assert_called_once_with(batch_parameters=batch_parameters)
