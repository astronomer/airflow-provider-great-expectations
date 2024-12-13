import pandas as pd
from great_expectations.data_context import AbstractDataContext
from great_expectations.core.batch_definition import BatchDefinition
from great_expectations.expectations import ExpectColumnValuesToBeInSet
from great_expectations_provider.operators.validate_batch import ValidateBatchOperator


def test_validate_batch_operator_success():
    # arrange
    def configure_ephemeral_batch_definition(
        context: AbstractDataContext,
    ) -> BatchDefinition:
        return (
            context.data_sources.add_pandas(name="test datasource")
            .add_dataframe_asset("test asset")
            .add_batch_definition_whole_dataframe("test batch def")
        )

    column_name = "col_A"
    df = pd.DataFrame({column_name: ["a", "b", "c"]})
    expect = ExpectColumnValuesToBeInSet(
        column=column_name, value_set=["a", "b", "c", "d", "e"]
    )

    validate_batch = ValidateBatchOperator(
        task_id="validate_batch_success",
        configure_batch_definition=configure_ephemeral_batch_definition,
        expect=expect,
        batch_parameters={"dataframe": df},
    )
    result = validate_batch.execute(context={})
    assert result["success"]
