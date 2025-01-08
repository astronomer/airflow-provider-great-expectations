from dataclasses import dataclass
import pytest
import random
import string

import pandas as pd

from typing import Callable, Generator
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context import AbstractDataContext

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)


@dataclass(frozen=True)
class CheckpointCloudState:
    """Dataclass to hold what we need to clean up after the test."""

    checkpoint_name: str
    datasource_name: str
    suite_name: str
    validation_definition_name: str


def rand_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))


class TestValidateCheckpointOperator:
    COL_NAME = "my_column"

    @pytest.fixture
    def configure_checkpoint(
        self,
    ) -> Generator[Callable[[AbstractDataContext], gx.Checkpoint], None, None]:
        to_cleanup: set[CheckpointCloudState] = set()

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
            validation_definitions = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=batch_definition,
                    suite=suite,
                )
            )
            checkpoint = context.checkpoints.add(
                gx.Checkpoint(
                    name=rand_name(), validation_definitions=[validation_definitions]
                )
            )

            to_cleanup.add(
                CheckpointCloudState(
                    checkpoint_name=checkpoint.name,
                    datasource_name=batch_definition.data_asset.datasource.name,
                    suite_name=suite.name,
                    validation_definition_name=validation_definitions.name,
                )
            )
            return checkpoint

        yield _configure_checkpoint

        cleanup_context = gx.get_context(mode="cloud")

        for to_clean in to_cleanup:
            cleanup_context.checkpoints.delete(to_clean.checkpoint_name)
            cleanup_context.validation_definitions.delete(
                to_clean.validation_definition_name
            )
            cleanup_context.data_sources.delete(to_clean.datasource_name)
            cleanup_context.suites.delete(to_clean.suite_name)

    def test_validate_checkpoint_data_frame_cloud(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
    ) -> None:
        df = pd.DataFrame({self.COL_NAME: [1, 2, 3, 4, 5]})
        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            context_type="cloud",
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": df},
        )

        result = validate_cloud_checkpoint.execute(context={})

        assert result["success"] is True
