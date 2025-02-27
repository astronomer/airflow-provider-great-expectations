class ExistingDataSourceTypeMismatch(Exception):
    def __init__(self, expected_type: type, actual_type: type, name: str) -> None:
        message = f"Error getting datasource '{name}'; expected type {expected_type.__name__}, but got {actual_type.__name__}."
        super().__init__(message)
