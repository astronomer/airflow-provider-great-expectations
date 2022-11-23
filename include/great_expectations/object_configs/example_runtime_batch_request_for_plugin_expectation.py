import pandas as pd
from great_expectations.core.batch import RuntimeBatchRequest

df = pd.DataFrame(
    {
        "fruits": ["apple", "banana", "cherry", "date"],
        "animals": ["zebra", "yak", "xylo", "walrus"],
        "places": ["house", "school", "park", "store"],
    }
)

runtime_batch_request = RuntimeBatchRequest(
    **{
        "datasource_name": "my_datasource",
        "data_connector_name": "default_runtime_data_connector_name",
        "data_asset_name": "my_alphabetical_dataframe",
        "runtime_parameters": {"batch_data": df},
        "batch_identifiers": {"default_identifier_name": "default_identifier"},
    }
)
