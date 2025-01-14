from typing import Callable

import pandas as pd
import pytest
from typing import TYPE_CHECKING
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
from integration.conftest import is_valid_gx_cloud_url, rand_name

if TYPE_CHECKING:
    import pyspark.sql as pyspark
    from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame
    from pyspark.sql.connect.session import SparkSession as SparkConnectSession


class TestGXValidateDataFrameOperator:
    @pytest.mark.integration
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

    @pytest.mark.spark_integration
    def test_spark(self, spark_session: pyspark.SparkSession) -> None:
        column_name = "col_A"
        task_id = f"test_spark_{rand_name()}"

        def configure_dataframe() -> pyspark.DataFrame:
            data_frame = spark_session.createDataFrame(
                pd.DataFrame({column_name: ["a", "b", "c"]})
            )
            assert isinstance(data_frame, pyspark.DataFrame)
            return data_frame

        validate_df = GXValidateDataFrameOperator(
            task_id=task_id,
            configure_dataframe=configure_dataframe,
            expect=ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c", "d", "e"],
            ),
        )

        # act
        result = validate_df.execute(context={})

        # assert
        assert result["success"]

    @pytest.mark.spark_connect_integration
    def test_spark_connect(self, spark_connect_session: SparkConnectSession) -> None:
        column_name = "col_A"
        task_id = f"test_spark_{rand_name()}"

        def configure_dataframe() -> SparkConnectDataFrame:
            data_frame = spark_connect_session.createDataFrame(
                pd.DataFrame({column_name: ["a", "b", "c"]})
            )
            assert isinstance(data_frame, SparkConnectDataFrame)
            return data_frame

        validate_df = GXValidateDataFrameOperator(
            task_id=task_id,
            configure_dataframe=configure_dataframe,
            expect=ExpectColumnValuesToBeInSet(
                column=column_name,
                value_set=["a", "b", "c", "d", "e"],
            ),
        )

        # act
        result = validate_df.execute(context={})

        # assert
        assert result["success"]


@pytest.fixture
def spark_session() -> pyspark.SparkSession:
    import pyspark.sql as pyspark

    session = pyspark.SparkSession.builder.getOrCreate()
    assert isinstance(session, pyspark.SparkSession)
    return session


@pytest.fixture
def spark_connect_session() -> SparkConnectSession:
    import pyspark.sql as pyspark
    from pyspark.sql.connect.session import SparkSession as SparkConnectSession

    session = pyspark.SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
    assert isinstance(session, SparkConnectSession)
    return session
