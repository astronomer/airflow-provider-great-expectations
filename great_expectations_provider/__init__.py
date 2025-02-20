from great_expectations_provider.operators.constants import VERSION

__version__ = VERSION


def get_provider_info():
    return {
        "package-name": "airflow-provider-great-expectations",
        "name": "Great Expectations Provider",
        "description": "An Apache Airflow provider for Great Expectations.",
        "versions": [__version__],
        "connection_types": [
            {
                "connection_type": "gx_cloud",
                "hook_class_name": "great_expectations_provider.hooks.gx_cloud:GXCloudConnection",
                "hook_name": "Great Expectations Cloud",
            }
        ],
    }
