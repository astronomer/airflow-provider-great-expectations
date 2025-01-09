from typing import Callable
import random
import string
import pytest
import great_expectations as gx
from great_expectations import expectations as gxe
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch
import pandas as pd

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from integration.conftest import rand_name
from great_expectations.core.batch_definition import BatchDefinition


class TestValidateBatchOperator:
    COL_NAME = "my_column"

    def test_validate_dataframe(self, ensure_data_source_cleanup) -> None:
        task_id = f"validate_batch_dataframe_integration_test_{rand_name()}"
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

    def test_validate_csv(self): ...

    def test_validate_sql(self): ...
