from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.utils.context import Context
    from great_expectations.checkpoint.checkpoint import CheckpointDescriptionDict


def _extract_failed_types_from_expectations(expectations: Any) -> set[str]:
    """Helper function to extract failed expectation types from a list of expectations."""
    failed_types: set[str] = set()

    if not isinstance(expectations, list):
        return failed_types

    for expectation in expectations:
        if (
            isinstance(expectation, dict)
            and expectation.get("success") is False
            and "expectation_type" in expectation
        ):
            failed_types.add(expectation["expectation_type"])

    return failed_types


def extract_validation_failure_context(
    validation_result_dict: dict[str, Any] | CheckpointDescriptionDict, task_id: str
) -> dict[str, Any]:
    """
    Extract relevant context from a failed validation result for inclusion in exception.

    Args:
        validation_result_dict: The result.describe_dict() from a Great Expectations validation
        task_id: The Airflow task ID for xcom reference

    Returns:
        Dictionary containing failure context including xcom location, statistics,
        and list of failed expectation types (max 10, unique)
    """
    context: dict[str, Any] = {
        "xcom_location": f"Task '{task_id}' -> XCom key 'return_value'",
        "statistics": None,
        "failed_expectation_types": [],
    }

    # Extract statistics if present - use get() for safe access
    context["statistics"] = validation_result_dict.get("statistics")

    failed_types: set[str] = set()  # Use set to ensure uniqueness

    # Check for expectations directly in the result (ExpectationSuiteValidationResult format)
    expectations = validation_result_dict.get("expectations")
    failed_types.update(_extract_failed_types_from_expectations(expectations))

    # Check for validation_results containing expectations (CheckpointResult format)
    validation_results = validation_result_dict.get("validation_results")
    if isinstance(validation_results, list):
        for validation_result in validation_results:
            if isinstance(validation_result, dict):
                expectations = validation_result.get("expectations")
                failed_types.update(
                    _extract_failed_types_from_expectations(expectations)
                )

    # Limit to first 10 unique types, sorted alphabetically
    limited_failed_types = sorted(list(failed_types))[:10]
    context["failed_expectation_types"] = limited_failed_types
    return context


class GXValidationFailed(Exception):
    """Great Expectations data validation failed.

    This exception includes detailed context about the validation failure,
    including statistics and information about which expectations failed.

    Attributes:
        xcom_location: Location of the full validation result in Airflow XCom
        statistics: Validation statistics if available
        failed_expectation_types: List of expectation types that failed (max 10, unique)
        context: Full context dictionary
    """

    def __init__(
        self,
        validation_result_dict: dict[str, Any]
        | CheckpointDescriptionDict
        | None = None,
        task_id: str | None = None,
        message: str | None = None,
    ):
        if validation_result_dict and task_id:
            self.context = extract_validation_failure_context(
                validation_result_dict, task_id
            )
            self.xcom_location = self.context["xcom_location"]
            self.statistics = self.context["statistics"]
            self.failed_expectation_types = self.context["failed_expectation_types"]

            # Build detailed error message
            if not message:
                message = self._build_error_message()
        else:
            self.context = {}
            self.xcom_location = None
            self.statistics = None
            self.failed_expectation_types = []
            if not message:
                message = "Great Expectations data validation failed. See the task xcom for the failing ValidationResult."

        super().__init__(message)

    def _build_error_message(self) -> str:
        """Build a detailed error message from the validation context."""
        lines = ["Great Expectations data validation failed."]

        lines.append(f"Full validation result available at: {self.xcom_location}")

        if self.statistics:
            lines.append("Statistics:")
            for key, value in self.statistics.items():
                lines.append(f"  {key}: {value}")

        if self.failed_expectation_types:
            expectation_count = len(self.failed_expectation_types)
            if expectation_count <= 10:
                lines.append(f"Failed expectation types ({expectation_count}):")
            else:
                lines.append(
                    f"Failed expectation types (showing first 10 of {expectation_count}):"
                )
            for exp_type in self.failed_expectation_types:
                lines.append(f"  - {exp_type}")

        return "\n".join(lines)
