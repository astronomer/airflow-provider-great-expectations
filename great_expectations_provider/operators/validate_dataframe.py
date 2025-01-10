from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal

from airflow.models import BaseOperator

if TYPE_CHECKING:
    import pyspark.sql as pyspark
    from great_expectations.expectations import Expectation
    from great_expectations import ExpectationSuite
    from great_expectations.data_context import AbstractDataContext
    from airflow.utils.context import Context
    from great_expectations import ExpectationSuite
    from great_expectations.expectations import Expectation
    from pandas import DataFrame
    from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame


class GXValidateDataFrameOperator(BaseOperator):
    def __init__(
        self,
        configure_dataframe: Callable[
            [], DataFrame | pyspark.DataFrame | SparkConnectDataFrame
        ],
        expect: Expectation | ExpectationSuite,
        context_type: Literal["ephemeral", "cloud"] = "ephemeral",
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
        | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.context_type = context_type
        self.dataframe = configure_dataframe()
        self.expect = expect
        self.result_format = result_format

    def execute(self, context: Context) -> dict:
        import great_expectations as gx

        gx_context: AbstractDataContext = gx.get_context(mode=self.context_type)
        batch = (
            gx_context.data_sources.add_spark(name=self.task_id)
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
