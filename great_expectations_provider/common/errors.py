class GXValidationFailed(Exception):
    """Great Expectations data validation failed.

    See the task xcom for the failing ValidationResult.
    """

    ...
