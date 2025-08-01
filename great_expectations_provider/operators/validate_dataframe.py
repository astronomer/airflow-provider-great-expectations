from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Literal, Union

from airflow.models import BaseOperator
from great_expectations.datasource.fluent import PandasDatasource, SparkDatasource

from great_expectations_provider.common.errors import GXValidationFailed
from great_expectations_provider.common.gx_context_actions import (
    load_data_context,
    run_validation_definition,
)
from great_expectations_provider.exceptions.exceptions import (
    ExistingDataSourceTypeMismatch,
)
from great_expectations_provider.hooks.gx_cloud import GXCloudHook

if TYPE_CHECKING:
    import pyspark.sql as pyspark
    from airflow.utils.context import Context
    from great_expectations import ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation
    from pandas import DataFrame
    from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame


class GXValidateDataFrameOperator(BaseOperator):
    """
    An operator to use Great Expectations to validate Expectations against a DataFrame in your Airflow DAG.

    Args:
        task_id: Airflow task ID. Alphanumeric name used in the Airflow UI and to name components in GX Cloud.
        configure_dataframe: A callable which returns the DataFrame to be validated.
        expect: An Expectation or ExpectationSuite to validate against the DataFrame. Available Expectations can
            be found at https://greatexpectations.io/expectations.
        result_format: control the verbosity of returned Validation Results. Possible values are
            "BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE". Defaults to "SUMMARY". See
            https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format
            for more information.
        context_type: accepts `ephemeral` or `cloud` to set the DataContext used by the Operator.
            Defaults to `ephemeral`, which does not persist results between runs.
            To save and view Validation Results in GX Cloud, use `cloud` and include
            GX Cloud credentials in your environment.
    """

    def __init__(
        self,
        configure_dataframe: Callable[
            [], DataFrame | pyspark.DataFrame | SparkConnectDataFrame
        ],
        expect: Expectation | ExpectationSuite,
        context_type: Literal["ephemeral", "cloud"] = "ephemeral",
        result_format: (
            Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"] | None
        ) = None,
        conn_id: Union[str, None] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.context_type = context_type
        self.dataframe = configure_dataframe()
        self.expect = expect
        self.result_format = result_format
        self.conn_id = conn_id

    def execute(self, context: Context) -> None:
        from pandas import DataFrame

        if self.conn_id:
            gx_cloud_config = GXCloudHook(gx_cloud_conn_id=self.conn_id).get_conn()
        else:
            gx_cloud_config = None
        gx_context = load_data_context(
            gx_cloud_config=gx_cloud_config, context_type=self.context_type
        )

        if isinstance(self.dataframe, DataFrame):
            batch_definition = self._get_pandas_batch_definition(gx_context)
        elif type(self.dataframe).__name__ == "DataFrame":
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
        result_dict = result.describe_dict()
        context["ti"].xcom_push(key="return_value", value=result_dict)
        if not result.success:
            raise GXValidationFailed(result_dict, self.task_id)

    def _get_spark_batch_definition(
        self, gx_context: AbstractDataContext
    ) -> BatchDefinition:
        name = self.task_id
        try:
            data_source = gx_context.data_sources.get(name=name)
            if not isinstance(data_source, SparkDatasource):
                raise ExistingDataSourceTypeMismatch(
                    expected_type=SparkDatasource,
                    actual_type=type(data_source),
                    name=name,
                )
        except KeyError:
            data_source = gx_context.data_sources.add_spark(name=name)

        try:
            asset = data_source.get_asset(name=name)
        except LookupError:
            asset = data_source.add_dataframe_asset(name=name)

        try:
            batch_definition = asset.get_batch_definition(name=name)
        except KeyError:
            batch_definition = asset.add_batch_definition_whole_dataframe(name=name)

        return batch_definition

    def _get_pandas_batch_definition(
        self, gx_context: AbstractDataContext
    ) -> BatchDefinition:
        name = self.task_id
        try:
            data_source = gx_context.data_sources.get(name=name)
            if not isinstance(data_source, PandasDatasource):
                raise ExistingDataSourceTypeMismatch(
                    expected_type=PandasDatasource,
                    actual_type=type(data_source),
                    name=name,
                )
        except KeyError:
            data_source = gx_context.data_sources.add_pandas(name=name)

        try:
            asset = data_source.get_asset(name=name)
        except LookupError:
            asset = data_source.add_dataframe_asset(name=name)

        try:
            batch_definition = asset.get_batch_definition(name=name)
        except KeyError:
            batch_definition = asset.add_batch_definition_whole_dataframe(name=name)

        return batch_definition
