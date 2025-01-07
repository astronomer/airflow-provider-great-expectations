from __future__ import annotations
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from airflow import DAG
from airflow.models.baseoperator import chain

from airflow.decorators import task
from great_expectations import ExpectationSuite, ValidationDefinition, Checkpoint
import great_expectations.expectations as gxe

from great_expectations_provider.operators.validate_batch import GXValidateBatchOperator
from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from great_expectations_provider.operators.validate_dataframe import (
    GXValidateDataFrameOperator,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations import Checkpoint

base_path = Path(__file__).parents[2]
data_dir = base_path / "include" / "data"
data_file = data_dir / "yellow_tripdata_sample_2019-01.csv"


# configuration functions
def configure_pandas_batch_definition(context: AbstractDataContext) -> BatchDefinition:
    """This function takes a GX Context and returns a BatchDefinition that
    can load our CSV files from the data directory."""
    data_source = context.data_sources.add_pandas_filesystem(
        name="Extract Data Source",
        base_directory=data_dir,
    )
    asset = data_source.add_csv_asset(name="Extract CSV Asset")
    batch_definition = asset.add_batch_definition_monthly(
        name="Extract Batch Definition",
        regex="yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
    )
    return batch_definition


def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
    """This function takes a GX Context and returns a Checkpoint that
    can load our CSV files from the data directory, validate them
    against an ExpectationSuite, and run Actions."""
    # setup data source, asset, batch definition
    batch_definition = (
        context.data_sources.add_pandas_filesystem(
            name="Load Datasource", base_directory=data_dir
        )
        .add_csv_asset("Load Asset")
        .add_batch_definition_monthly(
            name="Load Batch Definition",
            regex="yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2}).csv",
        )
    )
    # setup expectation suite
    expectation_suite = context.suites.add(
        ExpectationSuite(
            name="Load ExpectationSuite",
            expectations=[
                gxe.ExpectTableRowCountToBeBetween(
                    min_value=9000,
                    max_value=11000,
                ),
                gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
                gxe.ExpectColumnValuesToBeBetween(
                    column="passenger_count", min_value=1, max_value=6
                ),
            ],
        )
    )
    # setup validation definition
    validation_definition = context.validation_definitions.add(
        ValidationDefinition(
            name="Load Validation Definition",
            data=batch_definition,
            suite=expectation_suite,
        )
    )
    # setup checkpoint
    checkpoint = context.checkpoints.add(
        Checkpoint(
            name="Load Checkpoint",
            validation_definitions=[validation_definition],
            actions=[],
        )
    )
    return checkpoint


# Batch Parameters will also be available as DAG params, to be consumed directly by the
# operator through the context. Users can still provide batch_parameters on operator init
# (critical for validating data frames), but batch_parameters provided as DAG params should take precedence.
# To demo validation failure, use FAILURE_MONTH as a batch parameter instead of SUCCESS_MONTH
SUCCESS_MONTH = "01"
FAILURE_MONTH = "02"
batch_parameters = {"year": "2019", "month": SUCCESS_MONTH}


expectation_suite = ExpectationSuite(
    name="Taxi Data Expectations",
    expectations=[
        gxe.ExpectTableRowCountToBeBetween(
            min_value=9000,
            max_value=11000,
        ),
        gxe.ExpectColumnValuesToNotBeNull(column="vendor_id"),
        gxe.ExpectColumnValuesToBeBetween(
            column="passenger_count", min_value=1, max_value=6
        ),
    ],
)


with DAG(
    dag_id="gx_provider_example_dag",
) as dag:
    # define a consistent set of expectations we'll use throughout the pipeline

    validate_extract = GXValidateBatchOperator(
        task_id="validate_extract",
        configure_batch_definition=configure_pandas_batch_definition,
        expect=expectation_suite,
        batch_parameters=batch_parameters,
    )

    @task.short_circuit()
    def check_validate_extract(task_instance):
        result = task_instance.xcom_pull(task_ids="validate_extract")
        return result.get("success")

    validate_transform = GXValidateDataFrameOperator(
        task_id="validate_transform",
        configure_dataframe=lambda: pd.read_csv(data_file),
        expect=expectation_suite,
    )

    @task.short_circuit()
    def check_validate_transform(task_instance):
        result = task_instance.xcom_pull(task_ids="validate_transform")
        return result.get("success")

    validate_load = GXValidateCheckpointOperator(
        task_id="validate_load",
        configure_checkpoint=configure_checkpoint,
        batch_parameters=batch_parameters,
    )

    @task.short_circuit()
    def check_validate_load(task_instance):
        result = task_instance.xcom_pull(task_ids="validate_load")
        return result.get("success")

    chain(
        validate_extract,
        check_validate_extract(),
        validate_transform,
        check_validate_transform(),
        validate_load,
        check_validate_load(),
    )
