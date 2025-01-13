import json
from typing import Generator, Literal
from unittest.mock import Mock

import pandas as pd
import pytest
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition
from great_expectations.data_context import AbstractDataContext, FileDataContext
from great_expectations.expectations import ExpectColumnValuesToBeInSet
from pytest_mock import MockerFixture

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)

pytestmark = pytest.mark.unit


class TestValidateCheckpointOperator:
    def test_validate_dataframe(self) -> None:
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
            task_id="validate_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": df},
        )

        # act
        serialized_result = validate_cloud_checkpoint.execute(context={})

        # assert
        assert serialized_result["success"]
        json.dumps(serialized_result)  # result must be json serializable

    def test_context_type_ephemeral(self, mocker: MockerFixture) -> None:
        """Expect that param context_type creates an EphemeralDataContext."""
        # arrange
        context_type: Literal["ephemeral"] = "ephemeral"
        configure_checkpoint = Mock()
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

    def test_context_type_cloud(self, mocker: MockerFixture) -> None:
        """Expect that param context_type creates a CloudDataContext."""
        # arrange
        context_type: Literal["cloud"] = "cloud"
        configure_checkpoint = Mock()
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

    def test_context_type_filesystem(self, mocker: MockerFixture) -> None:
        """Expect that param context_type defers creation of data context to user."""
        # arrange
        context_type: Literal["file"] = "file"
        configure_checkpoint = Mock()
        configure_file_data_context = Mock()
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

    def test_user_configured_context_is_passed_to_configure_checkpoint(self) -> None:
        """Expect that if a user configures a file context, it gets passed to the configure_checkpoint function."""
        # arrange
        context_type: Literal["file"] = "file"
        configure_checkpoint = Mock()
        configure_file_data_context = Mock()
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
            configure_file_data_context=configure_file_data_context,
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        configure_checkpoint.assert_called_once_with(
            configure_file_data_context.return_value
        )

    def test_context_type_filesystem_requires_configure_file_data_context(self) -> None:
        """Expect that param context_type requires the configure_file_data_context parameter."""
        # arrange
        context_type: Literal["file"] = "file"
        configure_checkpoint = Mock()

        # act/assert
        with pytest.raises(ValueError, match="configure_file_data_context"):
            GXValidateCheckpointOperator(
                task_id="validate_checkpoint_success",
                configure_checkpoint=configure_checkpoint,
                context_type=context_type,
                configure_file_data_context=None,  # must be defined
            )

    def test_batch_parameters(self) -> None:
        """Expect that param batch_parameters is passed to Checkpoint.run. This
        also confirms that we run the Checkpoint returned by configure_checkpoint."""
        # arrange
        mock_checkpoint = Mock()
        configure_checkpoint = Mock()
        configure_checkpoint.return_value = mock_checkpoint
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

    def test_configure_file_data_context_with_without_generator(self) -> None:
        """Expect that configure_file_data_context can just return a DataContext"""
        # arrange
        mock_context = Mock(spec=AbstractDataContext)
        setup = Mock()
        teardown = Mock()
        configure_checkpoint = Mock()

        def configure_file_data_context() -> FileDataContext:
            setup()
            return mock_context

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type="file",
            configure_file_data_context=configure_file_data_context,
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        setup.assert_called_once()
        configure_checkpoint.assert_called_once_with(mock_context)
        teardown.assert_not_called()

    def test_configure_file_data_context_with_generator(self) -> None:
        """Expect that configure_file_data_context can return a generator that yeidls a DataContext."""
        # arrange
        mock_context = Mock(spec=AbstractDataContext)
        setup = Mock()
        teardown = Mock()
        configure_checkpoint = Mock()

        def configure_file_data_context() -> Generator[FileDataContext, None, None]:
            setup()
            yield mock_context
            teardown()

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type="file",
            configure_file_data_context=configure_file_data_context,
        )

        # act
        validate_checkpoint.execute(context={})

        # assert
        setup.assert_called_once()
        configure_checkpoint.assert_called_once_with(mock_context)
        teardown.assert_called_once()
