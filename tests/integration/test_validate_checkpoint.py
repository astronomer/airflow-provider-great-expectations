import pytest
import random
import string

import pandas as pd

from typing import Callable
import great_expectations as gx
import great_expectations.expectations as gxe
from great_expectations.data_context import AbstractDataContext

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)


def rand_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))


class TestValidateCheckpointOperator:
    COL_NAME = "my_column"

    @pytest.fixture
    def configure_checkpoint(
        self,
        ensure_checkpoint_cleanup: Callable[[str], None],
        ensure_validation_definition_cleanup: Callable[[str], None],
        ensure_suite_cleanup: Callable[[str], None],
        ensure_data_source_cleanup: Callable[[str], None],
    ) -> Callable[[AbstractDataContext], gx.Checkpoint]:
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

            ensure_checkpoint_cleanup(checkpoint.name)
            ensure_validation_definition_cleanup(validation_definition.name)
            ensure_suite_cleanup(suite.name)
            ensure_data_source_cleanup(batch_definition.data_asset.datasource.name)

            return checkpoint

        return _configure_checkpoint

    def test_validate_checkpoint_with_cloud(
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

    def test_postgres(
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
                .add_table_asset(table_name)
                .add_batch_definition_whole_table(rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column="age",
                            min_value=0,
                            max_value=100,
                        )
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
            context_type="cloud",
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
        )

        result = validate_checkpoint.execute(context={})

        assert result["success"] is True
