import pytest

import pandas as pd

from typing import Callable
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context import AbstractDataContext, FileDataContext
from pathlib import Path

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from integration.conftest import rand_name


class TestValidateCheckpointOperator:
    """Test cases for GXValidateCheckpointOperator with different context types."""

    COL_NAME = "my_column"

    @pytest.fixture
    def data_frame(self) -> pd.DataFrame:
        """Sample data frame for test cases"""
        return pd.DataFrame({self.COL_NAME: [1, 2, 3, 4, 5]})

    @pytest.fixture
    def configure_checkpoint(self) -> Callable[[AbstractDataContext], gx.Checkpoint]:
        """Configure an arbitrary checkpoint to a given context.

        This will pass for the data_frame fixture.
        """

        def _configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            batch_definition = (
                context.data_sources.add_pandas(name=rand_name())
                .add_dataframe_asset(rand_name())
                .add_batch_definition_whole_dataframe(rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column=self.COL_NAME,
                            min_value=0,
                            max_value=100,
                        )
                    ],
                )
            )
            validation_definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=batch_definition,
                    suite=suite,
                )
            )
            checkpoint = context.checkpoints.add(
                gx.Checkpoint(
                    name=rand_name(), validation_definitions=[validation_definition]
                )
            )

            return checkpoint

        return _configure_checkpoint

    @pytest.fixture
    def configure_checkpoint_with_cleanup(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
        ensure_checkpoint_cleanup: Callable[[str], None],
        ensure_validation_definition_cleanup: Callable[[str], None],
        ensure_suite_cleanup: Callable[[str], None],
        ensure_data_source_cleanup: Callable[[str], None],
    ) -> Callable[[AbstractDataContext], gx.Checkpoint]:
        """Configure an arbitrary checkpoint to a given cloud context.

        The models will be cleaned up when the test ends
        This will pass for the data_frame fixture.
        """

        def _configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            # get the checkpoint
            checkpoint = configure_checkpoint(context)

            # ensure cleanup after the test ends
            ensure_checkpoint_cleanup(checkpoint.name)
            for vd in checkpoint.validation_definitions:
                ensure_validation_definition_cleanup(vd.name)
                ensure_suite_cleanup(vd.suite.name)
                ensure_data_source_cleanup(vd.data.data_asset.datasource.name)

            # return the checkpoint for use in test
            return checkpoint

        return _configure_checkpoint

    def test_with_cloud_context(
        self,
        configure_checkpoint_with_cleanup: Callable[
            [AbstractDataContext], gx.Checkpoint
        ],
        data_frame: pd.DataFrame,
    ) -> None:
        """Ensure GXValidateCheckpointOperator works with cloud contexts."""

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            context_type="cloud",
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint_with_cleanup,
            batch_parameters={"dataframe": data_frame},
        )

        result = validate_cloud_checkpoint.execute(context={})

        assert result["success"] is True

    def test_with_file_context(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
        tmp_path: Path,
        data_frame: pd.DataFrame,
    ) -> None:
        """Ensure GXValidateCheckpointOperator works with file contexts."""

        def configure_context() -> FileDataContext:
            return gx.get_context(mode="file", project_root_dir=tmp_path)

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            configure_file_data_context=configure_context,
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": data_frame},
        )

        result = validate_cloud_checkpoint.execute(context={})

        assert result["success"] is True

    def test_with_ephemeral_context(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
        data_frame: pd.DataFrame,
    ) -> None:
        """Ensure GXValidateCheckpointOperator works with ephemeral contexts."""

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": data_frame},
        )

        result = validate_cloud_checkpoint.execute(context={})

        assert result["success"] is True

    def test_postgres_data_source(
        self,
        table_name: str,
        load_postgres_data: Callable[[list[dict]], None],
        postgres_connection_string: str,
    ) -> None:
        load_postgres_data(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ]
        )

        def configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            bd = (
                context.data_sources.add_postgres(
                    name=rand_name(),
                    connection_string=postgres_connection_string,
                )
                .add_table_asset(name=rand_name(), table_name=table_name)
                .add_batch_definition_whole_table(name=rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column="age",
                            min_value=0,
                            max_value=100,
                        ),
                        gxe.ExpectTableRowCountToEqual(value=2),
                    ],
                )
            )
            vd = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=bd,
                    suite=suite,
                )
            )
            return context.checkpoints.add(
                gx.Checkpoint(name=rand_name(), validation_definitions=[vd])
            )

        validate_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
        )

        result = validate_checkpoint.execute(context={})

        assert result["success"] is True

    def test_filesystem_data_source(
        self,
        load_csv_data: Callable[[Path, list[dict]], None],
        tmp_path: Path,
    ) -> None:
        data_location = tmp_path / "data.csv"
        load_csv_data(
            data_location,
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ],
        )

        def configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            bd = (
                context.data_sources.add_pandas(name=rand_name())
                .add_csv_asset(name=rand_name(), filepath_or_buffer=data_location)
                .add_batch_definition_whole_dataframe(name=rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column="age",
                            min_value=0,
                            max_value=100,
                        ),
                        gxe.ExpectTableRowCountToEqual(value=2),
                    ],
                )
            )
            vd = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=bd,
                    suite=suite,
                )
            )
            return context.checkpoints.add(
                gx.Checkpoint(name=rand_name(), validation_definitions=[vd])
            )

        validate_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
        )

        result = validate_checkpoint.execute(context={})

        assert result["success"] is True
