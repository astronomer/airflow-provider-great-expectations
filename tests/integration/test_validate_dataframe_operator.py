from __future__ import annotations

from typing import TYPE_CHECKING, Callable
from unittest.mock import Mock

import pandas as pd
import pytest
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet

from great_expectations_provider.common.errors import GXValidationFailed
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)
from tests.integration.conftest import is_valid_gx_cloud_url, rand_name

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from pyspark.sql import SparkSession
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
        mock_ti = Mock()

        # act
        validate_df.execute(context={"ti": mock_ti})

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True
        assert is_valid_gx_cloud_url(pushed_result["result_url"])

    @pytest.mark.integration
    def test_multiple_runs(
        self,
        ensure_data_source_cleanup: Callable[[str], None],
        ensure_suite_cleanup: Callable[[str], None],
        ensure_validation_definition_cleanup: Callable[[str], None],
    ) -> None:
        """Test to ensure we don't error when running multiple times.

        This validates that both the add_* and update_* code paths work."""
        # arrange
        column_name = "col_A"
        task_id = f"test_validate_dataframe_multiple_{rand_name()}"

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
        mock_ti_a = Mock()
        context_a: Context = {"ti": mock_ti_a}  # type: ignore[typeddict-item]
        mock_ti_b = Mock()
        context_b: Context = {"ti": mock_ti_b}  # type: ignore[typeddict-item]

        # act
        validate_df.execute(context=context_a)
        validate_df.execute(context=context_b)

        # assert
        pushed_result_a = mock_ti_a.xcom_push.call_args[1]["value"]
        pushed_result_b = mock_ti_b.xcom_push.call_args[1]["value"]
        assert pushed_result_a["success"] is True
        assert pushed_result_b["success"] is True

    @pytest.mark.spark_integration
    def test_spark(self, spark_session: SparkSession) -> None:
        import pyspark.sql as pyspark

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
        mock_ti = Mock()

        # act
        validate_df.execute(context={"ti": mock_ti})

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"]

    @pytest.mark.spark_connect_integration
    def test_spark_connect(self, spark_connect_session: SparkConnectSession) -> None:
        from pyspark.sql.connect.dataframe import DataFrame as SparkConnectDataFrame

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
        mock_ti = Mock()

        # act
        validate_df.execute(context={"ti": mock_ti})

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"]

    @pytest.mark.integration
    def test_validation_failure_raises_exception(self) -> None:
        """Test that validation failure raises GXValidationFailed exception."""
        column_name = "col_A"
        task_id = f"test_validate_dataframe_failure_{rand_name()}"

        def configure_dataframe() -> pd.DataFrame:
            # Create data that will fail validation
            return pd.DataFrame(
                {column_name: ["x", "y", "z"]}
            )  # values NOT in expected set

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c"],  # different values to cause failure
        )

        validate_df = GXValidateDataFrameOperator(
            task_id=task_id,
            configure_dataframe=configure_dataframe,
            expect=expect,
            context_type="ephemeral",
        )

        # act & assert
        mock_ti = Mock()
        with pytest.raises(GXValidationFailed):
            validate_df.execute(context={"ti": mock_ti})

    @pytest.mark.integration
    def test_validation_failure_xcom_contains_result(self) -> None:
        """Test that when validation fails and exception is raised, xcom still contains the failed result."""
        column_name = "col_A"
        task_id = f"test_validate_dataframe_failure_xcom_{rand_name()}"

        def configure_dataframe() -> pd.DataFrame:
            # Create data that will fail validation
            return pd.DataFrame(
                {column_name: ["x", "y", "z"]}
            )  # values NOT in expected set

        expect = ExpectColumnValuesToBeInSet(
            column=column_name,
            value_set=["a", "b", "c"],  # different values to cause failure
        )

        validate_df = GXValidateDataFrameOperator(
            task_id=task_id,
            configure_dataframe=configure_dataframe,
            expect=expect,
            context_type="ephemeral",
        )
        mock_ti = Mock()

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_df.execute(context={"ti": mock_ti})

        # Verify that xcom_push was called with the failed validation result
        mock_ti.xcom_push.assert_called_once()
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["key"] == "return_value"
        result = call_args[1]["value"]
        assert result["success"] is False


@pytest.fixture
def spark_session() -> SparkSession:
    import pyspark.sql as pyspark

    session = pyspark.SparkSession.builder.getOrCreate()
    assert isinstance(session, pyspark.SparkSession)
    return session


@pytest.fixture
def spark_connect_session() -> SparkConnectSession:
    import pyspark.sql as pyspark
    from pyspark.sql.connect.session import SparkSession as SparkConnectSession

    session = pyspark.SparkSession.builder.remote("sc://localhost:15002").getOrCreate()  # type: ignore[attr-defined]
    assert isinstance(session, SparkConnectSession)
    return session
