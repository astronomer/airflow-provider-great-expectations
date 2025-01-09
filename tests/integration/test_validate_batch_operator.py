from typing import Callable
import random
import string
import pytest
import great_expectations as gx
from great_expectations import expectations as gxe
from great_expectations.data_context import AbstractDataContext
from great_expectations.datasource.fluent.interfaces import Batch


def rand_name() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=10))


class TestValidateBatchOperator:
    COL_NAME = "my_column"

    def test_validate_dataframe(self):
        ...

    def test_validate_csv(self):
        ...

    def test_validate_sql(self):
        ...


    @pytest.fixture
    def configure_batch(
        self,
        ensure_data_source_cleanup: Callable[[str], None],
    ) -> Callable[[AbstractDataContext], gx.Batch]:
        def _configure_batch(context: AbstractDataContext) -> gx.Checkpoint:
            batch_definition = (
                context.data_sources.add_pandas(name=rand_name())
                .add_dataframe_asset(rand_name())
                .add_batch_definition_whole_dataframe(rand_name())
            )


            ensure_data_source_cleanup(batch_definition.data_asset.datasource.name)

            return batch_definition.get_batch()

        return _configure_batch

