import random
import string
from typing import Callable

import great_expectations as gx
import pandas as pd
import pytest
from great_expectations import expectations as gxe
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from integration.conftest import rand_name


class TestValidateBatchOperator:
    COL_NAME = "my_column"

    def test_with_cloud_context(self, ensure_data_source_cleanup) -> None:
        task_id = f"validate_batch_cloud_integration_test_{rand_name()}"
        ensure_data_source_cleanup(task_id)
        dataframe = pd.DataFrame({self.COL_NAME: ["a", "b", "c"]})
        expect = gxe.ExpectColumnValuesToBeInSet(
            column=self.COL_NAME,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )
        batch_parameters = {"dataframe": dataframe}

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name=task_id)
                .add_dataframe_asset(task_id)
                .add_batch_definition_whole_dataframe(task_id)
            )

        validate_cloud_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            expect=expect,
            batch_parameters=batch_parameters,
            context_type="cloud",
        )

        result = validate_cloud_batch.execute(context={})

        assert result["success"] is True

    def test_validate_dataframe(self):
        task_id = f"validate_batch_dataframe_integration_test_{rand_name()}"
        dataframe = pd.DataFrame({self.COL_NAME: ["a", "b", "c"]})
        expect = gxe.ExpectColumnValuesToBeInSet(
            column=self.COL_NAME,
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )
        batch_parameters = {"dataframe": dataframe}

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_pandas(name=task_id)
                .add_dataframe_asset(task_id)
                .add_batch_definition_whole_dataframe(task_id)
            )

        validate_cloud_batch = GXValidateBatchOperator(
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            expect=expect,
            batch_parameters=batch_parameters,
            context_type="ephemeral",
        )

        result = validate_cloud_batch.execute(context={})

        assert result["success"] is True

    def test_validate_csv(self): ...

    def test_validate_sql(
        self,
        table_name: str,
        load_postgres_data: Callable[[list[dict]], None],
        postgres_connection_string: str,
    ) -> None:
        task_id = f"validate_batch_sql_integration_test_{rand_name()}"
        load_postgres_data(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ]
        )

        def configure_batch_definition(context: AbstractDataContext) -> BatchDefinition:
            return (
                context.data_sources.add_postgres(
                    name=task_id,
                    connection_string=postgres_connection_string,
                )
                .add_table_asset(
                    name=task_id,
                    table_name=table_name,
                )
                .add_batch_definition_whole_table(task_id)
            )

        expect = gxe.ExpectColumnValuesToBeBetween(
            column="age",
            min_value=0,
            max_value=100,
        )

        validate_batch = GXValidateBatchOperator(
            context_type="ephemeral",
            task_id=task_id,
            configure_batch_definition=configure_batch_definition,
            expect=expect,
        )

        result = validate_batch.execute(context={})

        assert result["success"] is True
