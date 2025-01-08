from __future__ import annotations

from typing import Callable, Literal, TYPE_CHECKING

from airflow.models import BaseOperator


if TYPE_CHECKING:
    from great_expectations.expectations import Expectation
    from great_expectations import ExpectationSuite
    from airflow.utils.context import Context
    from pandas import DataFrame


class GXValidateDataFrameOperator(BaseOperator):
    # todo: update signature to allow Spark DataFrame
    def __init__(
        self,
        configure_dataframe: Callable[[], DataFrame],
        expect: Expectation | ExpectationSuite,
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
        | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.dataframe = configure_dataframe()
        self.expect = expect
        self.result_format = result_format

    def execute(self, context: Context) -> dict:
        import great_expectations as gx

        gx_context = gx.get_context(mode="ephemeral")
        batch = (
            gx_context.data_sources.add_pandas(name=self.task_id)
            .add_dataframe_asset(name=self.task_id)
            .add_batch_definition_whole_dataframe(name=self.task_id)
            .get_batch(
                batch_parameters={
                    "dataframe": self.dataframe,
                }
            )
        )
        if self.result_format:
            result = batch.validate(
                expect=self.expect, result_format=self.result_format
            )
        else:
            result = batch.validate(expect=self.expect)
        return result.describe_dict()
