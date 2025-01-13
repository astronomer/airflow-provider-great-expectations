from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal

from airflow.models import BaseOperator
from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame

from great_expectations_provider.common.gx_context_actions import (
    run_validation_definition,
)

if TYPE_CHECKING:
    import pyspark.sql as pyspark
    from pandas import DataFrame
    from airflow.utils.context import Context
    from great_expectations import ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation


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

        gx_context = gx.get_context(mode=self.context_type)
        if isinstance(self.dataframe, DataFrame):
            batch_definition = self._get_pandas_batch_definition(gx_context)
        elif self.dataframe.__class__.__name__ == "DataFrame":
            # if it's not pandas, but the classname is Dataframe, we assume spark
            batch_definition = self._get_spark_batch_definition(gx_context)
        else:
            raise ValueError(
                f"Unsupported dataframe type: {type(self.dataframe).__name__}"
            )

        batch_parameters = {
            "dataframe": self.dataframe,
        }
        result = run_validation_definition(
            task_id=self.task_id,
            expect=self.expect,
            batch_definition=batch_definition,
            result_format=self.result_format,
            batch_parameters=batch_parameters,
            gx_context=gx_context,
        )
        return result.describe_dict()

    def _get_spark_batch_definition(
        self, gx_context: AbstractDataContext
    ) -> BatchDefinition:
        return (
            gx_context.data_sources.add_or_update_spark(name=self.task_id)
            .add_dataframe_asset(name=self.task_id)
            .add_batch_definition_whole_dataframe(name=self.task_id)
        )

    def _get_pandas_batch_definition(
        self, gx_context: AbstractDataContext
    ) -> BatchDefinition:
        return (
            gx_context.data_sources.add_or_update_pandas(name=self.task_id)
            .add_dataframe_asset(name=self.task_id)
            .add_batch_definition_whole_dataframe(name=self.task_id)
        )
