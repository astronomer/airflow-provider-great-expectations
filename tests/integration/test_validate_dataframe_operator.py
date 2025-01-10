from typing import Callable

import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
from integration.conftest import is_valid_gx_cloud_url, rand_name


class TestGXValidateDataFrameOperator:
    def test_validate_dataframe_with_cloud(
        self,
        ensure_data_source_cleanup: Callable[[str], None],
        ensure_suite_cleanup: Callable[[str], None],
        ensure_validation_definition_cleanup: Callable[[str], None],
    ) -> None:
        # arrange
        column_name = "col_A"
        task_id = f"test_validate_dataframe_with_cloud_{rand_name()}"

        def configure_dataframe() -> pd.DataFrame:
            return pd.DataFrame({column_name: ["a", "b", "c"]})

        expect = ExpectationSuite(
            name=task_id,
            expectations=[
                ExpectColumnValuesToBeInSet(
                    column=column_name,
                    value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
                ),
            ],
        )
        ensure_data_source_cleanup(task_id)
        ensure_suite_cleanup(task_id)
        ensure_validation_definition_cleanup(task_id)

        validate_df = GXValidateDataFrameOperator(
            context_type="cloud",
            task_id=task_id,
            configure_dataframe=configure_dataframe,
            expect=expect,
        )

        # act
        result = validate_df.execute(context={})

        # assert
        assert result["success"] is True
        assert is_valid_gx_cloud_url(result["result_url"])
