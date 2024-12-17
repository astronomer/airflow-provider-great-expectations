from __future__ import annotations
from typing import Callable, Literal, TYPE_CHECKING

from airflow.models import BaseOperator


if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.core.batch import BatchParameters
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.expectations import Expectation
    from great_expectations import ExpectationSuite
    from airflow.utils.context import Context


class ValidateBatchOperator(BaseOperator):
    def __init__(
        self,
        configure_batch_definition: Callable[[AbstractDataContext], BatchDefinition],
        expect: Expectation | ExpectationSuite,
        batch_parameters: BatchParameters | None = None,
        context_type: Literal["ephemeral", "cloud"] = "ephemeral",
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
        | None = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        if batch_parameters is None:
            self.batch_parameters = {}
        else:
            self.batch_parameters = batch_parameters
        self.context_type = context_type
        self.configure_batch_definition = configure_batch_definition
        self.expect = expect
        self.result_format = result_format

    def execute(self, context: Context) -> dict:
        import great_expectations as gx

        gx_context = gx.get_context(mode=self.context_type)
        batch_definition = self.configure_batch_definition(gx_context)
        batch = batch_definition.get_batch(batch_parameters=self.batch_parameters)
        if self.result_format:
            result = batch.validate(
                expect=self.expect, result_format=self.result_format
            )
        else:
            result = batch.validate(expect=self.expect)
        return result.to_json_dict()
