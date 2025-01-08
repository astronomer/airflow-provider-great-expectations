import json
from great_expectations import ValidationDefinition
from great_expectations import Checkpoint

from great_expectations_provider.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)
from great_expectations.data_context import AbstractDataContext
import pandas as pd
from great_expectations import ExpectationSuite
from great_expectations.expectations import ExpectColumnValuesToBeInSet


class TestValidateCheckpointOperator:
    def test_validate_dataframe(self):
        # arrange
        column_name = "col_A"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            # setup data source, asset, batch definition
            batch_definition = (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )
            # setup expectation suite
            expectation_suite = context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name,
                            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
                        ),
                    ],
                )
            )
            # setup validation definition
            validation_definition = context.validation_definitions.add(
                ValidationDefinition(
                    name="test validation definition",
                    data=batch_definition,
                    suite=expectation_suite,
                )
            )
            # setup checkpoint
            checkpoint = context.checkpoints.add(
                Checkpoint(
                    name="test checkpoint",
                    validation_definitions=[validation_definition],
                )
            )
            return checkpoint

        df = pd.DataFrame({column_name: ["a", "b", "c"]})

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_cloud_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": df},
        )

        # act
        serialized_result = validate_cloud_checkpoint.execute(context={})

        # assert
        assert serialized_result["success"]
        json.dumps(serialized_result)  # result must be json serializable
