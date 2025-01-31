# Getting started

[Great Expectations](https://greatexpectations.io/) (GX) is a framework for describing data using expressive tests and then validating that the data meets test criteria. [Astronomer](https://www.astronomer.io/) maintains the Great Expectations Airflow Provider to give users a convenient method for running validations directly from their DAGs. The Great Expectations Airflow Provider has three Operators to choose from, which vary in the amount of configuration they require and flexibility they provide.

- `GXValidateDataFrameOperator` 
- `GXValidateBatchOperator` 
- `GXValidateCheckpointOperator` 


## Operator use cases

When deciding which Operator best fits your use case, consider the location of the data you are validating, whether or not you need external alerts or actions to be triggered by the Operator, and what Data Context you will use which depends on whether or not you need to view how results change over time.

- If your data is in memory as a Spark or Pandas DataFrame, we recommend using the `GXValidateDataFrameOperator`. This option requires only a DataFrame and your Expectations to create a validation result.
- If your data is not in memory, we recommend configuring GX to connect to it by defining a BatchDefinition with the `GXValidateBatchOperator`. This option requires a BatchDefinition and your Expectations to create a validation result.
- If you want to [trigger actions](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/create_a_checkpoint_with_actions) based on validation results, use the `GXValidateCheckpointOperator`. This option supports all features of GX Core so it requires the most configuration - you have to define a  Checkpoint, BatchDefinition, ExpectationSuite, and ValidationDefinition to get validation results.

The Operators vary in which [Data Contexts](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) they support. All 3 Operators support Ephemeral and GX Cloud Data Contexts. Only the `GXValidateCheckpointOperator` supports the File Data Context.

- If the results are used only within the Airflow DAG by other tasks, we recommend using an Ephemeral Data Context. The serialized ValidationResult will be available within the DAG as the task result, but not persisted externally for viewing the results across multiple runs. All 3 Operators support the Ephemeral Data Context.

- To persist and view results outside of Airflow, we recommend using a Cloud Data Context. ValidationResults are automatically visible in the GX Cloud UI when using a Cloud Data Context, and the task result contains a link to the stored validation result. All 3 Operators support the Cloud Data Context.

- If you want to manage ValidationResults yourself, use a File Data Context. With this option, ValidationResults can be viewed in [Data Docs](https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_data_docs/). Only the `GXValidateCheckpointOperator` supports the File Data Context.

## Prerequisites

- Python version 3.9 to 3.12
- Great Expectations v1.3.3+
- Apache Airflow® 2.1.0+

## Assumed knowledge

To get the most out of this getting started guide, make sure you have an understanding of:

- The basics of Great Expectations. See [Try GX Core](https://docs.greatexpectations.io/docs/core/introduction/try_gx/).
- Airflow fundamentals, such as writing DAGs and defining tasks. See [Get started with Apache Airflow](https://www.astronomer.io/docs/learn/get-started-with-airflow/).
- Airflow operators. See [Operators 101](https://www.astronomer.io/docs/learn/what-is-an-operator/).
- Airflow connections. See [Managing your Connections in Apache Airflow](https://www.astronomer.io/docs/learn/connections/).

## Install the provider and dependencies

1. Install the provider

   ```bash
   pip install airflow-provider-great-expectations 
   ```
2. (Optional) Install additional dependencies for the data sources you’ll use. For example, to install the optional Snowflake dependency, use the following command:

   ```bash
   pip install "airflow-provider-great-expectations[snowflake]"
   ```
   The following backends are supported as optional dependencies:
      - `athena`
      - `azure`
      - `bigquery`
      - `gcp`
      - `mssql`
      - `postgresql`
      - `s3`
      - `snowflake`
      - `spark`

## Configure an Operator

After deciding [which Operator best fits your use case](#operator-use-cases), follow the Operator-specific instructions below to configure it.

### Data Frame Operator

1. Import the operator.

    ```python
    from great_expectations_provider.operators.validate_dataframe import (
        GXValidateDataFrameOperator,
    )
    ```

2. Instantiate the Operator with required and optional parameters.

    ```python
    my_validation = GXValidateDataFrameOperator(
        task_id="my_validation",
        configure_dataframe=lambda: pd.read_csv(my_data_file),
        expect=my_expectation_suite,
    )
    ```

    - `task_id`: alphanumeric display name for the task in the Airflow UI.
    - `configure_dataframe`: function that returns a DataFrame to pass data to the Operator.
    - `expect`: either a [single Expectation](https://docs.greatexpectations.io/docs/core/define_expectations/create_an_expectation) or an [Expectation Suite](https://docs.greatexpectations.io/docs/core/define_expectations/organize_expectation_suites) to validate against your data. 
    - (optional) `result_format`: dictionary that sets the [verbosity of returned Validation Results](https://docs.greatexpectations.io/docs/core/trigger_actions_based_on_results/choose_a_result_format/). Defaults to `SUMMARY`.
    - (optional) `context_type`: accepts `ephemeral` or `cloud` to set the [Data Context](https://docs.greatexpectations.io/docs/core/set_up_a_gx_environment/create_a_data_context) used by the Operator. Defaults to `ephemeral`, which does not persist configuration between runs. Use `cloud` and complete the additional Cloud configuration below to view Validation Results in GX Cloud.
    
3. If you are using a cloud Data Context, set the following Airflow variables.

    - GX_CLOUD_ACCESS_TOKEN
    - GX_CLOUD_ORGANIZATION_ID

    Create a [free GX Cloud account](https://app.greatexpectations.io/) to get your [Cloud credentials](https://docs.greatexpectations.io/docs/cloud/connect/connect_python#get-your-user-access-token-and-organization-id).

    
For more detail, explore this [end-to-end code sample](https://github.com/joshua-stauffer/airflow-provider-great-expectations/blob/v1_gx_provider/great_expectations_provider/example_dags/example_great_expectations_dag.py#L134-L138)

### Batch Operator



### Checkpoint Operator

