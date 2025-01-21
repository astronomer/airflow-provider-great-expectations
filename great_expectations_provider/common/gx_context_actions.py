from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from great_expectations import ExpectationSuite
    from great_expectations.core.batch_definition import BatchDefinition
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.expectations import Expectation


def run_validation_definition(
    task_id: str,
    expect: Expectation | ExpectationSuite,
    batch_definition: BatchDefinition,
    result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
    | None,
    batch_parameters: dict,
    gx_context: AbstractDataContext,
) -> ExpectationSuiteValidationResult:
    """Given a BatchDefinition and an Expectation or ExpectationSuite, ensure a
    ValidationDefinition and run it."""
    import great_expectations as gx

    if isinstance(expect, gx.expectations.Expectation):
        suite = gx.ExpectationSuite(name=task_id, expectations=[expect])
    else:
        suite = expect
    validation_definition = gx_context.validation_definitions.add_or_update(
        validation=gx.ValidationDefinition(
            name=task_id,
            suite=suite,
            data=batch_definition,
        ),
    )
    if result_format:
        result = validation_definition.run(
            batch_parameters=batch_parameters,
            result_format=result_format,
        )
    else:
        result = validation_definition.run(
            batch_parameters=batch_parameters,
        )
    return result
