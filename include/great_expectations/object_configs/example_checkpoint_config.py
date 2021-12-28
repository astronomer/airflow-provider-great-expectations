from great_expectations.data_context.types.base import CheckpointConfig

example_checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "taxi.demo",
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": "yellow_tripdata_sample_2019-01.csv",
                    "data_connector_query": {"index": -1},
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)
